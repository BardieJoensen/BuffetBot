#!/usr/bin/env python3
"""
Phase C — Bulk Load

One-time script to populate the BuffettBot v2 database from scratch.

Steps:
    1. Sync conviction list → universe table
    2. Build three-pool universe (conviction + S&P 500 + Finviz)
    3. Fetch fundamentals via yfinance for all universe stocks
    4. Compute percentile quality scores, save to DB
    5. Run Haiku pre-screen on top N by quality score (conviction list first)
    6. Run Sonnet deep analysis on Haiku passes (within budget cap)
    7. Assign S/A/B/C tiers, log to tier_history
    8. Print verification report

Usage:
    cd /home/bardie/git-repos/BuffetBot/buffett-bot
    python3 scripts/bulk_load.py [options]

Options:
    --dry-run           Skip all writes and LLM calls; show what would happen
    --conviction-only   Only process the conviction list (skip S&P 500 / Finviz)
    --skip-llm          Only run fundamentals + quality scores; skip all LLM calls
    --haiku-limit N     Max Haiku calls this run (default: 400)
    --sonnet-limit N    Max Sonnet calls this run (default: 80)
    --db-path PATH      Override the default database path
    --config-path PATH  Override the default YAML config path
"""

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

# ── Bootstrap sys.path so imports work from scripts/ ───────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database import Database
from src.quality_scorer import compute_quality_scores
from src.screener import StockScreener, load_criteria_from_yaml
from src.tier_engine import assign_tier, staged_entry_suggestion
from src.universe_builder import (
    UniverseStock,
    build_universe,
    get_conviction_tickers,
    get_tickers,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bulk_load")

# Default paths (relative to project root)
DEFAULT_CONVICTION_YAML = Path("config/conviction_list.yaml")
DEFAULT_CACHE_DIR = Path("data/cache")
DEFAULT_DB_PATH = Path("data/buffett_bot_v2.db")
DEFAULT_CONFIG_PATH = Path("config/screening_criteria.yaml")

# Approximate Claude API costs (USD) — batch API rates (50% off real-time)
# Real-time: Haiku ~$0.002, Sonnet ~$0.05. Batch: half price, no latency SLA.
HAIKU_COST_USD = 0.001  # per quick-screen call (batch)
SONNET_COST_USD = 0.025  # per deep-analysis call (batch)


# ─── Utility helpers ────────────────────────────────────────────────────────


def _build_company_summary(ticker: str, data: dict, notes: str = "") -> str:
    """
    Build a textual company summary from yfinance fundamentals data.

    This is the "filing text" passed to Haiku / Sonnet when no actual 10-K
    is available.  The LLMs use it to assess business quality and moat.
    """
    lines = [
        f"TICKER: {ticker}",
        f"NAME: {data.get('name', ticker)}",
        f"SECTOR: {data.get('sector', 'Unknown')}",
        f"INDUSTRY: {data.get('industry', 'Unknown')}",
    ]

    market_cap = data.get("market_cap") or 0
    if market_cap:
        lines.append(f"MARKET CAP: ${market_cap / 1e9:.1f}B")

    lines.append("")
    lines.append("=== KEY FINANCIAL METRICS ===")

    if data.get("price"):
        lines.append(f"Current Price: ${data['price']:.2f}")
    if data.get("pe_ratio"):
        lines.append(f"P/E Ratio: {data['pe_ratio']:.1f}x")
    if data.get("roe") is not None:
        lines.append(f"Return on Equity (ROE): {data['roe']:.1%}")

    # roic may be in the flat dict or inside data["historical"]
    roic = data.get("roic") or (data.get("historical") or {}).get("roic")
    if roic is not None:
        lines.append(f"Return on Invested Capital (ROIC): {roic:.1%}")

    if data.get("operating_margin") is not None:
        lines.append(f"Operating Margin: {data['operating_margin']:.1%}")

    fcfy = data.get("real_fcf_yield") or data.get("fcf_yield")
    if fcfy is not None:
        lines.append(f"FCF Yield (SBC-adjusted): {fcfy:.1%}")

    de = data.get("debt_equity")
    if de is not None:
        # yfinance returns debt/equity as a percentage (e.g. 45 → 0.45× ratio)
        ratio = de / 100.0 if de > 5 else de  # guard against already-ratio values
        lines.append(f"Debt/Equity: {ratio:.2f}×")

    if data.get("revenue_growth") is not None:
        lines.append(f"Revenue Growth (YoY): {data['revenue_growth']:.1%}")

    rev_cagr = data.get("revenue_cagr") or (data.get("historical") or {}).get("revenue_cagr")
    if rev_cagr is not None:
        lines.append(f"Revenue CAGR (multi-year): {rev_cagr:.1%}")

    earnings_q = data.get("earnings_quality")
    if earnings_q is not None:
        lines.append(f"Earnings Quality (FCF/NI): {earnings_q:.2f}×")

    if notes:
        lines += ["", "=== ANALYST NOTES ===", notes]

    return "\n".join(lines)


def _priority_list(
    conviction_tickers: list[str],
    quality_scores: dict,  # {ticker: QualityScore}
    all_tickers: list[str],
) -> list[str]:
    """
    Build the priority list for Haiku / Sonnet batching.

    Order:
        1. Conviction tickers (always first — highest priority)
           within conviction: sort by quality score descending (unscored last)
        2. Remaining universe tickers sorted by quality score descending
    """
    conviction_set = set(conviction_tickers)

    def _score(t: str) -> float:
        qs = quality_scores.get(t)
        return qs.score if qs else 0.0

    conviction_sorted = sorted(conviction_tickers, key=_score, reverse=True)

    rest = [t for t in all_tickers if t not in conviction_set]
    rest_sorted = sorted(rest, key=_score, reverse=True)

    return conviction_sorted + rest_sorted


# ─── Step functions ─────────────────────────────────────────────────────────


def step1_sync_universe(
    db: Database,
    universe: list[UniverseStock],
    dry_run: bool,
) -> None:
    """Upsert all universe stocks into the database universe table."""
    logger.info("── Step 1: Syncing universe to database ──")
    if dry_run:
        logger.info("[DRY-RUN] Would upsert %d stocks into universe table", len(universe))
        return
    for stock in universe:
        db.upsert_universe_stock(stock.ticker, source=stock.source)
    logger.info("Synced %d stocks into universe table", len(universe))


def step2_fetch_fundamentals(
    screener: StockScreener,
    universe: list[UniverseStock],
    conviction_tickers: list[str],
    db: Database,
    dry_run: bool,
) -> list:
    """
    Fetch yfinance fundamentals for all universe tickers.

    Conviction stocks bypass ALL hard filters (industry checks, market cap
    ceiling) so that stocks like TROW (Asset Management & Custody Banks)
    are not accidentally excluded by the industry keyword filter.

    S&P 500 constituents bypass only the max_market_cap ceiling so that
    large caps (AAPL, MSFT, etc.) pass even though they exceed $10B.

    Returns list[ScreenedStock] — the screener's output (already enriched
    with historical trend metrics from multiple years of financials).
    """
    logger.info("── Step 2: Fetching fundamentals from yfinance ──")
    tickers = get_tickers(universe)
    criteria = load_criteria_from_yaml()
    # Conviction stocks bypass ALL hard filters (including industry checks
    # that would misclassify e.g. TROW as an "asset management vehicle").
    force = set(conviction_tickers)
    # S&P 500 constituents bypass only the max-market-cap ceiling so that
    # large caps like AAPL ($3T+) are not rejected by the $10B hard limit.
    force_large = {s.ticker for s in universe if s.source == "sp500_filter"}

    screened = screener.screen_tickers(tickers, criteria, force_include=force, force_large_cap=force_large)

    logger.info(
        "Screened %d stocks from %d universe tickers (%d skipped hard filters)",
        len(screened),
        len(tickers),
        len(tickers) - len(screened),
    )

    if dry_run:
        logger.info("[DRY-RUN] Would save %d fundamentals snapshots", len(screened))
        return screened

    today = date.today().isoformat()
    source_map = {s.ticker: s.source for s in universe}

    for stock in screened:
        db.save_fundamentals(stock.symbol, stock.to_dict(), as_of_date=today)
        db.upsert_universe_stock(
            stock.symbol,
            company_name=stock.name,
            sector=stock.sector,
            market_cap=stock.market_cap,
            cap_category=stock.cap_category,
            source=source_map.get(stock.symbol, "finviz_screen"),
        )

    logger.info("Saved fundamentals for %d stocks", len(screened))
    return screened


def step3_quality_scores(
    screened: list,
    db: Database,
    dry_run: bool,
) -> dict:
    """
    Compute percentile quality scores from the screened universe.

    Returns {ticker: QualityScore}.
    """
    logger.info("── Step 3: Computing quality scores ──")
    # compute_quality_scores expects .ticker attr or dict with "ticker" key.
    # ScreenedStock uses .symbol — build compatible dicts.
    compat = [{**s.to_dict(), "ticker": s.symbol} for s in screened]
    scores = compute_quality_scores(compat)

    if not scores:
        logger.warning("No quality scores computed — empty screened list?")
        return {}

    top5 = sorted(scores.values(), key=lambda q: q.score, reverse=True)[:5]
    logger.info(
        "Top 5 quality scores: %s",
        ", ".join(f"{q.ticker}={q.score:.1f}" for q in top5),
    )

    if dry_run:
        logger.info("[DRY-RUN] Would update quality scores for %d tickers", len(scores))
        return scores

    for ticker, qs in scores.items():
        db.update_quality_score(ticker, qs.score)

    logger.info("Updated quality scores for %d tickers", len(scores))
    return scores


def step4_haiku_batch(
    analyzer,
    priority: list[str],
    data_map: dict,
    conviction_notes: dict[str, str],
    db: Database,
    dry_run: bool,
    limit: int,
) -> dict[str, dict]:
    """
    Haiku quick-screen using the Batch API (50% discount vs real-time).

    Collects all summaries upfront, submits a single batch request, then
    waits for completion.  This is strictly better than sequential calls for
    a bulk load where latency does not matter.

    Skips tickers with a valid (non-expired) cached Haiku result.
    Returns {ticker: haiku_result}.
    """
    logger.info("── Step 4: Haiku pre-screen via Batch API (limit=%d) ──", limit)

    candidates = priority[:limit]
    results: dict[str, dict] = {}
    to_screen: list[tuple[str, str]] = []  # (ticker, summary) pairs for the batch

    for ticker in candidates:
        existing = db.get_latest_haiku(ticker)
        if existing and existing.get("expires_at"):
            expires = datetime.fromisoformat(existing["expires_at"])
            if expires > datetime.now():
                results[ticker] = existing
                continue

        data = data_map.get(ticker)
        if data is None:
            logger.debug("No fundamentals for %s — skipping Haiku", ticker)
            continue

        notes = conviction_notes.get(ticker, "")
        to_screen.append((ticker, _build_company_summary(ticker, data, notes)))

    cached_count = len(results)
    logger.info(
        "Haiku: %d cached, %d to screen, %d no data",
        cached_count,
        len(to_screen),
        len(candidates) - cached_count - len(to_screen),
    )

    if not to_screen:
        logger.info("All Haiku results cached — no API calls needed")
        return results

    if dry_run:
        logger.info("[DRY-RUN] Would submit Haiku batch with %d requests", len(to_screen))
        for ticker, _ in to_screen:
            results[ticker] = {"symbol": ticker, "worth_analysis": True, "reason": "dry-run"}
        return results

    # Submit all at once via batch API
    batch_results = analyzer.batch_quick_screen(to_screen)

    for result in batch_results:
        ticker = result.get("symbol", "")
        if not ticker:
            continue
        results[ticker] = result
        db.save_haiku_result(
            ticker,
            passed=result.get("worth_analysis", False),
            moat_estimate=_moat_hint_to_label(result.get("moat_hint", 0)),
            summary=result.get("reason", ""),
        )

    passed = sum(1 for r in results.values() if r.get("worth_analysis") or r.get("passed"))
    logger.info(
        "Haiku complete: %d API calls, %d cached, %d/%d passed",
        len(to_screen),
        cached_count,
        passed,
        len(results),
    )
    return results


def step5_sonnet_batch(
    analyzer,
    haiku_results: dict[str, dict],
    priority: list[str],
    conviction_tickers: list[str],
    data_map: dict,
    conviction_notes: dict[str, str],
    db: Database,
    dry_run: bool,
    limit: int,
) -> int:
    """
    Sonnet deep analysis using the Batch API (50% discount vs real-time).

    Conviction tickers always get Sonnet (they bypass the Haiku gate).
    All eligible tickers are submitted in a single batch, then results are
    processed together — no per-stock polling or sleeping.

    Skips tickers with a valid (non-expired) existing deep analysis.
    Logs tier assignments to tier_history.

    Returns the number of API calls made (excludes cached results).
    """
    logger.info("── Step 5: Sonnet deep analysis via Batch API (limit=%d) ──", limit)

    conviction_set = set(conviction_tickers)
    haiku_passes = set(t for t, r in haiku_results.items() if r.get("worth_analysis") or r.get("passed"))
    candidates = [t for t in priority if t in conviction_set or t in haiku_passes][:limit]

    to_analyze: list[dict] = []
    skipped = 0

    for ticker in candidates:
        existing = db.get_latest_deep_analysis(ticker)
        if existing and existing.get("expires_at"):
            expires = datetime.fromisoformat(existing["expires_at"])
            if expires > datetime.now():
                skipped += 1
                continue

        data = data_map.get(ticker)
        if data is None:
            logger.debug("No fundamentals for %s — skipping Sonnet", ticker)
            continue

        notes = conviction_notes.get(ticker, "")
        to_analyze.append(
            {
                "symbol": ticker,
                "company_name": data.get("name", ticker),
                "filing_text": _build_company_summary(ticker, data, notes),
                "sector": data.get("sector", ""),
            }
        )

    logger.info("Sonnet: %d to analyze, %d cached", len(to_analyze), skipped)

    if not to_analyze:
        logger.info("All Sonnet results cached — no API calls needed")
        return 0

    if dry_run:
        for stock in to_analyze:
            logger.info("[DRY-RUN] Would Sonnet-analyze %s (%s)", stock["symbol"], stock["company_name"])
        return len(to_analyze)

    # Submit all at once via batch API
    analyses = analyzer.batch_analyze_companies(to_analyze)

    for analysis in analyses:
        ticker = analysis.symbol
        tier_assignment = assign_tier(analysis)

        db.save_deep_analysis(
            ticker,
            tier=tier_assignment.tier,
            conviction=analysis.conviction,
            moat_rating=analysis.moat_rating.value.upper(),
            moat_sources=analysis.moat_sources,
            fair_value=((analysis.estimated_fair_value_low or 0) + (analysis.estimated_fair_value_high or 0)) / 2
            or None,
            target_entry=analysis.target_entry_price,
            investment_thesis=analysis.summary,
            key_risks=analysis.key_risks,
            thesis_breakers=analysis.thesis_risks,
        )

        db.log_tier_change(
            ticker,
            new_tier=tier_assignment.tier,
            old_tier=None,
            trigger="bulk_load",
            reason=tier_assignment.tier_reason,
        )

        if tier_assignment.tier in ("S", "A", "B"):
            entries = staged_entry_suggestion(
                analysis.target_entry_price,
                tier_assignment.tier,
            )
            db.upsert_price_alert(
                ticker,
                tier=tier_assignment.tier,
                target_entry=analysis.target_entry_price,
                staged_entries=entries,
                last_price=analysis.current_price,
                gap_pct=tier_assignment.price_gap_pct,
            )

        logger.info(
            "%s → Tier %s (%s conviction, gap=%.0f%%)",
            ticker,
            tier_assignment.tier,
            analysis.conviction,
            (tier_assignment.price_gap_pct or 0) * 100,
        )

    logger.info("Sonnet complete: %d analyzed, %d cached", len(analyses), skipped)
    return len(analyses)


def step6_report(
    db: Database,
    *,
    dry_run: bool = False,
    universe_stocks: Optional[list] = None,
    screened_stocks: Optional[list] = None,
    quality_scores: Optional[dict] = None,
    priority: Optional[list] = None,
    haiku_calls: int = 0,
    sonnet_calls: int = 0,
    total_cost: float = 0.0,
) -> None:
    """Print a verification report of what was loaded (or would be in dry-run)."""
    logger.info("── Step 6: Verification report ──")

    print("\n" + "=" * 60)
    if dry_run:
        print("  BULK LOAD DRY-RUN REPORT (nothing was written)")
    else:
        print("  BULK LOAD VERIFICATION REPORT")
    print("  " + datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("=" * 60)

    if dry_run and universe_stocks is not None:
        # Show in-memory counts — nothing in DB yet
        by_source: dict[str, int] = {}
        for s in universe_stocks:
            src = getattr(s, "source", "unknown")
            by_source[src] = by_source.get(src, 0) + 1

        print(f"\nUniverse (would load): {len(universe_stocks)} stocks")
        for src, n in sorted(by_source.items()):
            print(f"  {src:20s}: {n:4d}")

        screened_n = len(screened_stocks) if screened_stocks else 0
        print(f"\nFundamentals (would fetch): {screened_n} stocks passed hard filters")

        if quality_scores:
            print(f"Quality scores (would compute): {len(quality_scores)} stocks")
            top10 = sorted(quality_scores.items(), key=lambda x: x[1].score, reverse=True)[:10]
            print("\nTop 10 quality scores (projected):")
            for ticker, qs in top10:
                print(f"  {ticker:8s}  {qs.score:5.1f}")

        if priority:
            print(f"\nPriority queue: {len(priority)} tickers")

        print(f"\nHaiku (would screen):  {haiku_calls} calls  (~${haiku_calls * HAIKU_COST_USD:.2f})")
        print(f"Sonnet (would analyze): {sonnet_calls} calls  (~${sonnet_calls * SONNET_COST_USD:.2f})")
        print(f"Estimated total cost:   ~${total_cost:.2f}")
    else:
        # Normal mode: read from DB
        universe = db.get_universe()
        by_source_db: dict[str, int] = {}
        by_tier: dict[str, int] = {}

        for stock in universe:
            src = stock.get("source", "unknown")
            by_source_db[src] = by_source_db.get(src, 0) + 1

        for stock in universe:
            da = db.get_latest_deep_analysis(stock["ticker"])
            if da:
                tier = da.get("tier", "?")
                by_tier[tier] = by_tier.get(tier, 0) + 1

        print(f"\nUniverse: {len(universe)} total stocks")
        for src, n in sorted(by_source_db.items()):
            print(f"  {src:20s}: {n:4d}")

        if by_tier:
            print(f"\nTier distribution ({sum(by_tier.values())} analyzed):")
            for tier in ["S", "A", "B", "C"]:
                n = by_tier.get(tier, 0)
                if n:
                    print(f"  Tier {tier}: {n:4d}")

        scored = [s for s in universe if s.get("quality_score") is not None]
        scored.sort(key=lambda s: s["quality_score"], reverse=True)
        if scored:
            print("\nTop 10 quality scores:")
            for s in scored[:10]:
                print(
                    f"  {s['ticker']:8s}  {s['quality_score']:5.1f}  "
                    f"{(s.get('sector') or '')[:25]:<25}  [{s.get('source', '')}]"
                )

        for cap_type in ("weekly_news_haiku", "weekly_news_sonnet"):
            status = db.get_budget_status(cap_type)
            if status:
                print(f"\n{cap_type}: {status['calls_used']}/{status['max_calls']} used")

    print()


# ─── Helpers ────────────────────────────────────────────────────────────────


def _moat_hint_to_label(moat_hint: int) -> str:
    """Convert Haiku's 1-5 moat score to a moat label."""
    if moat_hint >= 4:
        return "WIDE"
    if moat_hint >= 3:
        return "NARROW"
    return "NONE"


def _screened_to_data_map(screened: list) -> dict[str, dict]:
    """Build {ticker: data_dict} from a list of ScreenedStock objects."""
    return {s.symbol: s.to_dict() for s in screened}


# ─── CLI ────────────────────────────────────────────────────────────────────


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="BuffettBot Phase C — Bulk Load",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--dry-run", action="store_true", help="Show what would happen; no writes")
    p.add_argument("--conviction-only", action="store_true", help="Only process conviction list")
    p.add_argument("--skip-llm", action="store_true", help="Skip Haiku + Sonnet")
    p.add_argument("--haiku-limit", type=int, default=400, metavar="N")
    p.add_argument("--sonnet-limit", type=int, default=80, metavar="N")
    p.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    p.add_argument("--config-path", type=Path, default=DEFAULT_CONFIG_PATH)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if args.dry_run:
        logger.info("=== DRY-RUN MODE — no database writes, no LLM calls ===")

    # ── Initialise ─────────────────────────────────────────────────────────
    db = Database(args.db_path)
    run_id = db.start_run("bulk_load")
    screener = StockScreener()

    conviction_yaml = DEFAULT_CONVICTION_YAML
    cache_dir = DEFAULT_CACHE_DIR

    # ── Build universe ─────────────────────────────────────────────────────
    logger.info("── Building three-pool universe ──")
    if args.conviction_only:
        from src.universe_builder import _read_conviction_pool

        universe = _read_conviction_pool(conviction_yaml)
        logger.info("Conviction-only mode: %d stocks", len(universe))
    else:
        universe = build_universe(conviction_yaml, cache_dir, db if not args.dry_run else None)
        logger.info("Full universe: %d stocks", len(universe))

    conviction_tickers = get_conviction_tickers(universe)

    # Load conviction notes for LLM context
    conviction_notes: dict[str, str] = {}
    import yaml

    if conviction_yaml.exists():
        with open(conviction_yaml) as f:
            raw = yaml.safe_load(f)
        for entry in raw.get("stocks", []):
            t = entry.get("ticker", "").strip().upper()
            if t:
                conviction_notes[t] = entry.get("notes", "")

    # ── Step 1: Sync universe to DB ────────────────────────────────────────
    step1_sync_universe(db, universe, args.dry_run)

    # ── Step 2: Fetch fundamentals ─────────────────────────────────────────
    screened = step2_fetch_fundamentals(screener, universe, conviction_tickers, db, args.dry_run)

    # Build data map for LLM steps
    data_map = _screened_to_data_map(screened)

    # ── Step 3: Quality scores ─────────────────────────────────────────────
    quality_scores = step3_quality_scores(screened, db, args.dry_run)

    # ── Priority list (conviction first, then by quality score) ───────────
    all_tickers_screened = [s.symbol for s in screened]
    priority = _priority_list(conviction_tickers, quality_scores, all_tickers_screened)
    logger.info(
        "Priority queue: %d tickers (%d conviction, %d other)",
        len(priority),
        len(conviction_tickers),
        len(priority) - len(conviction_tickers),
    )

    haiku_results: dict[str, dict] = {}
    haiku_calls = 0
    sonnet_calls = 0
    total_cost = 0.0

    if not args.skip_llm:
        # ── Step 4: Haiku batch ───────────────────────────────────────────
        try:
            from src.analyzer import CompanyAnalyzer

            analyzer = CompanyAnalyzer()
        except Exception as exc:
            logger.error("Could not initialise CompanyAnalyzer: %s", exc)
            logger.error("Set ANTHROPIC_API_KEY or use --skip-llm")
            db.complete_run(run_id)
            return 1

        haiku_results = step4_haiku_batch(
            analyzer,
            priority,
            data_map,
            conviction_notes,
            db,
            args.dry_run,
            args.haiku_limit,
        )
        # Count only the results that came from API calls (not pre-cached)
        haiku_calls = sum(
            1
            for r in haiku_results.values()
            if not (r.get("expires_at") and r.get("screened_at"))  # cached rows have both
        )
        total_cost += haiku_calls * HAIKU_COST_USD

        # ── Step 5: Sonnet batch ──────────────────────────────────────────
        sonnet_calls = step5_sonnet_batch(
            analyzer,
            haiku_results,
            priority,
            conviction_tickers,
            data_map,
            conviction_notes,
            db,
            args.dry_run,
            args.sonnet_limit,
        )
        total_cost += sonnet_calls * SONNET_COST_USD

    # ── Step 6: Report ─────────────────────────────────────────────────────
    step6_report(
        db,
        dry_run=args.dry_run,
        universe_stocks=universe,
        screened_stocks=screened,
        quality_scores=quality_scores,
        priority=priority,
        haiku_calls=haiku_calls,
        sonnet_calls=sonnet_calls,
        total_cost=total_cost,
    )

    # ── Close run log ──────────────────────────────────────────────────────
    db.complete_run(
        run_id,
        stocks_screened=len(screened),
        haiku_calls=haiku_calls,
        sonnet_calls=sonnet_calls,
        total_cost_usd=total_cost,
    )

    logger.info(
        "Bulk load complete. Estimated cost: $%.2f  (haiku=%d × $%.3f, sonnet=%d × $%.2f)",
        total_cost,
        haiku_calls,
        HAIKU_COST_USD,
        sonnet_calls,
        SONNET_COST_USD,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
