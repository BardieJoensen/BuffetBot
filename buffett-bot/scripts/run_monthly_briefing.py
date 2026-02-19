#!/usr/bin/env python3
"""
Monthly Briefing Runner (v2.1 — Coverage Campaign)

Orchestrates the full pipeline:
1. Check market temperature
2. Screen stocks by quality (not valuation)
3. Load registry, check campaign status
4. Detect bubble stocks to avoid
5. Haiku pre-screen (campaign-aware: only new stocks)
6. Sonnet deep analysis on best candidates
7. Fetch supplementary valuations
8. Tier engine: assign tiers + update registry
9. Portfolio check
10. Opus second opinion on Tier 1 picks
11. Generate tiered briefing with campaign progress
12. Send notifications

COST WARNING:
This script calls the Claude API which costs money!
- Each deep analysis costs ~$0.03-0.05 (Sonnet)
- Haiku pre-screen costs ~$0.002 per stock (~100 per run)
- 10 analyses = ~$0.30-0.50 per run
- Full campaign (~6 runs to cover ~585 stocks): ~$4-5 total

Run manually:  docker compose run --rm buffett-bot
Auto-schedule: scheduler.py runs this on 1st of each month (if enabled)
"""

import json
import logging
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analyzer import CompanyAnalyzer, set_cache_dir
from src.benchmark import fetch_benchmark_data, set_benchmark_cache_dir
from src.briefing import BriefingGenerator, StockBriefing
from src.bubble_detector import BubbleDetector, get_market_temperature
from src.config import config
from src.notifications import NotificationManager
from src.portfolio import PortfolioTracker, calculate_position_size
from src.registry import Registry
from src.screener import StockScreener, load_criteria_from_yaml
from src.tier_engine import (
    assign_tier,
    compute_movements,
    load_previous_watchlist,
    save_watchlist_state,
)
from src.valuation import AggregatedValuation, ValuationAggregator

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_cached_watchlist(cache_path: Path) -> list[dict]:
    """Load watchlist from cache if recent enough"""
    paths_to_check = [cache_path, Path(tempfile.gettempdir()) / "buffett-bot-watchlist.json"]

    for path in paths_to_check:
        if path.exists():
            try:
                data = json.loads(path.read_text())
                cached_date = datetime.fromisoformat(data.get("generated_at", "2000-01-01"))
                age_days = (datetime.now() - cached_date).days
                if age_days < 7:
                    logger.info(f"Using cached watchlist from {path} ({age_days} days old)")
                    return data.get("stocks", [])
            except Exception as e:
                logger.warning(f"Error reading cache from {path}: {e}")
    return []


def save_watchlist(stocks: list, cache_path: Path):
    """Save watchlist to cache"""
    data = {
        "generated_at": datetime.now().isoformat(),
        "stocks": [s.to_dict() if hasattr(s, "to_dict") else s for s in stocks],
    }
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(data, indent=2))
        logger.info(f"Saved {len(stocks)} stocks to watchlist cache")
    except PermissionError:
        fallback_path = Path(tempfile.gettempdir()) / "buffett-bot-watchlist.json"
        fallback_path.write_text(json.dumps(data, indent=2))
        logger.warning(f"Permission denied for {cache_path}, saved to {fallback_path}")


def fetch_company_summary(symbol: str) -> str:
    """Fetch company description for LLM analysis using yfinance"""
    import yfinance as yf

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info

        name = info.get("longName") or info.get("shortName") or symbol
        sector = info.get("sector", "Unknown")
        industry = info.get("industry", "Unknown")
        description = info.get("longBusinessSummary", "No description available.")
        market_cap = info.get("marketCap", 0)
        price = info.get("regularMarketPrice") or info.get("currentPrice") or 0
        beta = info.get("beta", "N/A")
        low_52 = info.get("fiftyTwoWeekLow")
        high_52 = info.get("fiftyTwoWeekHigh")
        range_52 = f"${low_52:.2f} - ${high_52:.2f}" if low_52 and high_52 else "N/A"
        employees = info.get("fullTimeEmployees", "Unknown")

        return f"""
Company: {name}
Sector: {sector}
Industry: {industry}
Employees: {employees}

Description:
{description}

Key Financials:
- Market Cap: ${market_cap:,.0f}
- Price: ${price:.2f}
- Beta: {beta}
- 52-Week Range: {range_52}
"""
    except Exception as e:
        logger.warning(f"Error fetching company summary for {symbol}: {e}")
        return f"Company: {symbol}. Unable to fetch details."


def run_monthly_briefing(max_analyses: int = 10, use_cache: bool = True, send_notifications: bool = True):
    """
    Run the full monthly briefing pipeline (v2.1 — coverage campaign).

    Margin of safety is configured via MARGIN_OF_SAFETY_PCT env var (default 25%).

    Args:
        max_analyses: Maximum number of stocks to analyze with Claude (costs ~$0.05 each)
        use_cache: Use cached analyses to avoid re-running (recommended)
        send_notifications: Send results via configured notification channels
    """

    # Hard limit on analyses to prevent runaway costs
    max_analyses = min(max_analyses, 15)

    logger.info("=" * 60)
    logger.info("STARTING WATCHLIST UPDATE (v2.1 — Coverage Campaign)")
    logger.info("=" * 60)
    logger.info("")
    logger.info("COST ESTIMATE:")
    logger.info(f"   Max analyses: {max_analyses} stocks")
    logger.info(f"   Haiku batch:  up to {config.haiku_batch_size} stocks (~${config.haiku_batch_size * 0.002:.2f})")
    logger.info(f"   Est. cost: ~${max_analyses * 0.05 + config.haiku_batch_size * 0.002:.2f}")
    logger.info(f"   Cache enabled: {use_cache} (reuses analyses < 30 days old)")
    logger.info("")

    # Set up data directory with fallback for permission issues
    data_dir = Path("./data")
    try:
        data_dir.mkdir(exist_ok=True)
        test_file = data_dir / ".write_test"
        test_file.write_text("test")
        test_file.unlink()
    except PermissionError:
        data_dir = Path(tempfile.gettempdir()) / "buffett-bot-data"
        data_dir.mkdir(exist_ok=True)
        logger.warning(f"Using fallback data dir: {data_dir}")

    set_cache_dir(data_dir / "analyses")
    set_benchmark_cache_dir(data_dir / "benchmark")
    watchlist_cache = data_dir / "watchlist_cache.json"

    # ─────────────────────────────────────────────────────────────
    # Step 1: Market Temperature
    # ─────────────────────────────────────────────────────────────
    logger.info("\n[1/11] CHECKING MARKET REGIME...")

    market_temp = get_market_temperature()
    logger.info(f"Market: {market_temp.get('temperature')} - {market_temp.get('interpretation', '')[:50]}...")

    benchmark_symbol = config.benchmark_symbol
    logger.info(f"Fetching benchmark data for {benchmark_symbol}...")
    benchmark_data = fetch_benchmark_data(benchmark_symbol)
    bm_pe = benchmark_data.get("pe_ratio")
    bm_ytd = benchmark_data.get("ytd_return")
    logger.info(f"Benchmark {benchmark_symbol}: P/E={bm_pe}, YTD={f'{bm_ytd:+.1%}' if bm_ytd is not None else 'N/A'}")

    # ─────────────────────────────────────────────────────────────
    # Step 2: Screen Stocks by Quality
    # ─────────────────────────────────────────────────────────────
    logger.info("\n[2/11] SCREENING STOCKS BY QUALITY...")

    cached_stocks = load_cached_watchlist(watchlist_cache) if use_cache else []

    screened_lookup = {}

    if cached_stocks:
        symbols = [s["symbol"] for s in cached_stocks]
        logger.info(f"Using {len(symbols)} stocks from cache")
    else:
        try:
            screener = StockScreener()
            criteria = load_criteria_from_yaml()

            candidates = screener.screen(criteria)
            logger.info(f"Initial screen: {len(candidates)} candidates")
        except ValueError as e:
            logger.error(f"\nSCREENING FAILED: {e}\n")
            logger.error("TROUBLESHOOTING:")
            logger.error("  1. Check your internet connection")
            logger.error("  2. yfinance may be temporarily unavailable - try again later")
            logger.error("  3. Try relaxing screening criteria in config/screening_criteria.yaml\n")
            return

        # Keep ALL candidates (registry handles prioritization across runs)
        symbols = [c.symbol for c in candidates]
        screened_lookup = {c.symbol: c for c in candidates}

        save_watchlist(candidates, watchlist_cache)

    all_screened_symbols = symbols.copy()

    if not symbols:
        logger.warning("No stocks passed screening. Exiting.")
        return

    # ─────────────────────────────────────────────────────────────
    # Step 3: Load Registry & Check Campaign
    # ─────────────────────────────────────────────────────────────
    logger.info("\n[3/11] LOADING REGISTRY...")

    registry = Registry(data_dir)

    if registry.should_start_new_campaign(len(all_screened_symbols)):
        new_id = registry.start_new_campaign()
        logger.info(f"Started new campaign: {new_id} (previous covered >90% of universe)")
    else:
        logger.info(f"Continuing campaign: {registry.campaign['campaign_id']}")

    progress = registry.get_campaign_progress(len(all_screened_symbols))
    logger.info(
        f"Campaign progress: {progress['haiku_screened']}/{progress['universe_size']} screened "
        f"({progress['coverage_pct']:.0%}), {progress['deeply_analyzed']} analyzed, "
        f"{progress['total_studied_all_time']} in registry"
    )

    # ─────────────────────────────────────────────────────────────
    # Step 4: Detect Bubbles
    # ─────────────────────────────────────────────────────────────
    logger.info("\n[4/11] SCANNING FOR BUBBLE STOCKS...")

    bubble_detector = BubbleDetector()
    bubble_warnings = bubble_detector.scan_for_bubbles()

    logger.info(f"Found {len(bubble_warnings)} potential bubble stocks")

    # ─────────────────────────────────────────────────────────────
    # Step 5: Haiku Pre-Screen (campaign-aware)
    # ─────────────────────────────────────────────────────────────
    unstudied = registry.get_unstudied_symbols(all_screened_symbols)
    previously_passed = registry.get_unanalyzed_haiku_passed()
    haiku_batch_size = config.haiku_batch_size
    batch_to_screen = unstudied[:haiku_batch_size]

    logger.info("\n[5/11] HAIKU PRE-SCREEN...")
    logger.info(f"   Universe: {len(all_screened_symbols)}, Unstudied: {len(unstudied)}, Batch: {len(batch_to_screen)}")
    logger.info(f"   Previously passed (backlog): {len(previously_passed)}")
    logger.info(f"   Cost: ~${len(batch_to_screen) * 0.002:.2f} (Haiku)")

    analyzer = CompanyAnalyzer()
    haiku_results = []

    use_batch = config.use_batch_api

    if batch_to_screen:
        if use_batch:
            logger.info("   Using Batch API (50% discount)...")
            stocks_for_screen = []
            for sym in batch_to_screen:
                filing_text = fetch_company_summary(sym)
                stocks_for_screen.append((sym, filing_text))

            batch_results = analyzer.batch_quick_screen(stocks_for_screen)
            for result in batch_results:
                haiku_results.append(result)
                logger.info(
                    f"  {result['symbol']}: moat={result['moat_hint']}, quality={result['quality_hint']} - {result['reason'][:60]}"
                )
        else:
            for sym in batch_to_screen:
                try:
                    filing_text = fetch_company_summary(sym)
                    result = analyzer.quick_screen(sym, filing_text)
                    haiku_results.append(result)
                    logger.info(
                        f"  {sym}: moat={result['moat_hint']}, quality={result['quality_hint']} - {result['reason'][:60]}"
                    )
                except Exception as ex:
                    logger.warning(f"  {sym}: Haiku screen failed: {ex}")
                    haiku_results.append({"symbol": sym, "worth_analysis": True, "moat_hint": 3, "quality_hint": 3})

        # Record results in registry
        registry.mark_haiku_screened(batch_to_screen, haiku_results, min_score=config.haiku_min_score)
    else:
        logger.info("   No new stocks to Haiku-screen this run")

    # Combine newly passed + backlog, sort by Haiku score, take top for analysis
    newly_passed = [
        r["symbol"]
        for r in haiku_results
        if (r.get("moat_hint", 0) + r.get("quality_hint", 0)) >= config.haiku_min_score
    ]
    haiku_result_map = {r["symbol"]: r for r in haiku_results}

    # Merge: previously passed backlog + newly passed
    all_passed = list(dict.fromkeys(previously_passed + newly_passed))

    # Sort all passed by Haiku score (newly screened have scores, backlog gets default)
    def _haiku_sort_key(sym: str) -> int:
        r = haiku_result_map.get(sym)
        if r:
            return r.get("moat_hint", 0) + r.get("quality_hint", 0)
        return 6  # default for backlog (they passed threshold)

    all_passed.sort(key=_haiku_sort_key, reverse=True)
    top_for_analysis = all_passed[:max_analyses]

    logger.info(
        f"   Haiku: {len(newly_passed)} newly passed, {len(previously_passed)} backlog"
        f" -> {len(top_for_analysis)} for deep analysis"
    )

    # ─────────────────────────────────────────────────────────────
    # Step 6: LLM Analysis (COSTS MONEY - uses Claude API)
    # ─────────────────────────────────────────────────────────────
    num_to_analyze = len(top_for_analysis)
    logger.info(f"\n[6/11] RUNNING LLM ANALYSIS ON TOP {num_to_analyze} CANDIDATES...")
    logger.info("   Cached analyses (<30 days old) will be reused to save costs")

    from src.analyzer import get_cached_analysis

    pre_cached = sum(1 for sym in top_for_analysis if get_cached_analysis(sym, 30))
    if pre_cached > 0:
        logger.info(f"   Found {pre_cached} cached analyses (will save ~${pre_cached * 0.05:.2f})")

    analyses = {}  # symbol -> analysis
    analyzed_symbols = []

    if use_batch:
        logger.info("   Using Batch API (50% discount)...")
        stocks_for_analysis = []
        for sym in top_for_analysis:
            filing_text = fetch_company_summary(sym)
            sc = screened_lookup.get(sym)
            company_name = sc.name if sc else sym
            sector = sc.sector if sc and hasattr(sc, "sector") else ""
            stocks_for_analysis.append(
                {
                    "symbol": sym,
                    "company_name": company_name,
                    "filing_text": filing_text,
                    "sector": sector or "",
                }
            )

        analysis_list = analyzer.batch_analyze_companies(stocks_for_analysis)
        for a in analysis_list:
            analyses[a.symbol] = a
            analyzed_symbols.append(a.symbol)
    else:
        for sym in top_for_analysis:
            try:
                logger.info(f"Analyzing {sym}...")
                filing_text = fetch_company_summary(sym)
                sc = screened_lookup.get(sym)
                company_name = sc.name if sc else sym
                sector = sc.sector if sc and hasattr(sc, "sector") else ""
                analysis = analyzer.analyze_company(
                    symbol=sym,
                    company_name=company_name,
                    filing_text=filing_text,
                    use_cache=use_cache,
                    sector=sector or "",
                )
                analyses[sym] = analysis
                analyzed_symbols.append(sym)
            except Exception as ex:
                logger.error(f"Error analyzing {sym}: {ex}")
                continue

    # Mark analyzed in registry
    for sym in analyzed_symbols:
        registry.mark_analyzed(sym)

    logger.info(f"   Completed {len(analyses)} analyses")

    # ─────────────────────────────────────────────────────────────
    # Step 7: Fetch Supplementary Valuations
    # ─────────────────────────────────────────────────────────────
    logger.info(f"\n[7/11] FETCHING SUPPLEMENTARY VALUATIONS FOR {len(analyzed_symbols)} STOCKS...")

    aggregator = ValuationAggregator()
    valuation_lookup = {}
    for sym in analyzed_symbols:
        try:
            val = aggregator.get_valuation(sym)
            valuation_lookup[sym] = val
        except Exception as ex:
            logger.warning(f"Valuation fetch failed for {sym}: {ex}")

    logger.info(f"Got valuations for {len(valuation_lookup)} stocks")

    # ─────────────────────────────────────────────────────────────
    # Step 8: Tier Engine — assign tiers + update registry
    # ─────────────────────────────────────────────────────────────
    logger.info("\n[8/11] ASSIGNING TIERS...")

    tier_assignments = {}
    for sym, analysis in analyses.items():
        sc = screened_lookup.get(sym)
        screener_score = sc.score if sc else 0.0
        screener_confidence = sc.score_confidence if sc else 0.0
        ext_val = valuation_lookup.get(sym)

        tier = assign_tier(analysis, screener_score=screener_score, external_valuation=ext_val)
        tier_assignments[sym] = tier
        logger.info(f"  {sym}: Tier {tier.tier} — {tier.tier_reason}")

        # Update registry
        registry.add_study(
            symbol=sym,
            analysis=analysis,
            tier_assignment=tier,
            haiku_result=haiku_result_map.get(sym),
            score=screener_score,
            confidence=screener_confidence,
        )

    # Include Tier 1/2 entries from registry that aren't freshly analyzed
    registry_tier12 = registry.get_tier_entries([1, 2])
    for sym, entry in registry_tier12.items():
        if sym in tier_assignments:
            continue  # Already have fresh analysis
        # Reconstruct a TierAssignment from stored data
        from src.tier_engine import TierAssignment

        age_days = 0
        try:
            analyzed_at = datetime.fromisoformat(entry["analyzed_at"])
            age_days = (datetime.now() - analyzed_at).days
        except (KeyError, ValueError):
            pass

        stale_note = f" [analyzed {age_days}d ago]" if age_days > 0 else ""

        # Lightweight price fetch to recalculate gap to target
        stored_price = entry.get("current_price_at_analysis")
        target = entry.get("target_entry_price")
        current_price = stored_price
        price_stale = False
        try:
            import yfinance as yf

            ticker = yf.Ticker(sym)
            live_price = ticker.info.get("regularMarketPrice") or ticker.info.get("currentPrice")
            if live_price and live_price > 0:
                current_price = live_price
            else:
                price_stale = True
        except Exception:
            price_stale = True

        if price_stale and stored_price:
            stale_note += " [stale price]"

        # Recalculate price gap
        price_gap_pct = None
        approaching = False
        if current_price and target and target > 0:
            price_gap_pct = (current_price - target) / target
            approaching = 0 < price_gap_pct <= config.tier1_proximity_alert_pct

        tier_assignments[sym] = TierAssignment(
            symbol=sym,
            tier=entry["tier"],
            quality_level="high" if entry["tier"] <= 2 else "moderate",
            tier_reason=entry.get("tier_reason", "") + stale_note,
            target_entry_price=target,
            current_price=current_price,
            price_gap_pct=price_gap_pct,
            approaching_target=approaching,
        )

    # Compute movements from previous state
    previous_state = load_previous_watchlist(data_dir)
    movements = compute_movements(tier_assignments, previous_state)
    if movements:
        logger.info(f"  Movement log: {len(movements)} changes")
        for m in movements:
            logger.info(f"    [{m.change_type.upper()}] {m.symbol}: {m.detail}")

    # Save current state for next run
    save_watchlist_state(data_dir, tier_assignments)

    # Save registry
    registry.save()
    logger.info(f"Registry saved ({len(registry.get_all_studied())} total studies)")

    # ─────────────────────────────────────────────────────────────
    # Step 9: Portfolio Check
    # ─────────────────────────────────────────────────────────────
    logger.info("\n[9/11] CHECKING PORTFOLIO STATUS...")

    from src.paper_trader import PaperTrader, set_trade_log_dir

    set_trade_log_dir(data_dir)
    trader = PaperTrader()
    portfolio_summary = {}
    if trader.is_enabled():
        portfolio_summary = trader.get_portfolio_summary()
        if portfolio_summary.get("position_count", 0) > 0:
            logger.info("Using Alpaca paper trading positions for portfolio status")

    if not portfolio_summary or portfolio_summary.get("position_count", 0) == 0:
        portfolio_tracker = PortfolioTracker(data_dir=str(data_dir))
        portfolio_summary = portfolio_tracker.get_portfolio_summary()

    portfolio_value = config.portfolio_value
    current_positions = portfolio_summary.get("position_count", 0)

    logger.info(f"Portfolio: {current_positions} positions, ${portfolio_summary.get('current_value', 0):,.0f} value")

    # ─────────────────────────────────────────────────────────────
    # Step 9.5: Paper trades for Tier 1 picks
    # ─────────────────────────────────────────────────────────────
    tier1_symbols = [sym for sym, t in tier_assignments.items() if t.tier == 1]

    if trader.is_enabled() and tier1_symbols:
        logger.info(f"\n[9.5/11] EXECUTING PAPER TRADES FOR {len(tier1_symbols)} TIER 1 PICKS...")
        for sym in tier1_symbols:
            analysis_for_trade = analyses.get(sym)
            conv = getattr(analysis_for_trade, "conviction_level", "MEDIUM") if analysis_for_trade else "MEDIUM"
            sizing = calculate_position_size(
                portfolio_value=portfolio_value,
                conviction=conv,
                current_positions=current_positions,
            )
            amount = sizing.get("recommended_amount", 0) if isinstance(sizing, dict) else 0
            if amount > 0:
                trader.buy(sym, amount)
    else:
        logger.info("\n[9.5/11] PAPER TRADING SKIPPED (no Tier 1 picks or Alpaca not configured)")

    # ─────────────────────────────────────────────────────────────
    # Step 10: Opus Second Opinion on Tier 1 picks
    # ─────────────────────────────────────────────────────────────
    use_opus = config.use_opus_second_opinion
    opus_opinions = {}

    if use_opus and tier1_symbols:
        logger.info(f"\n[10/11] OPUS SECOND OPINION ON {len(tier1_symbols)} TIER 1 PICKS...")
        logger.info(f"   Cost: ~${len(tier1_symbols) * 0.30:.2f} (Opus)")

        for sym in tier1_symbols[:5]:
            try:
                filing_text = fetch_company_summary(sym)
                analysis = analyses[sym]
                company_name = getattr(analysis, "company_name", sym) or sym
                opus_result = analyzer.opus_second_opinion(
                    symbol=sym,
                    company_name=company_name,
                    filing_text=filing_text,
                    sonnet_analysis=analysis,
                    use_cache=use_cache,
                )
                opus_opinions[sym] = opus_result
                logger.info(
                    f"  {sym}: {opus_result.get('agreement')} (Opus conviction: {opus_result.get('opus_conviction')})"
                )
            except Exception as ex:
                logger.error(f"  Opus second opinion failed for {sym}: {ex}")
    else:
        reason = "no Tier 1 picks" if not tier1_symbols else "USE_OPUS_SECOND_OPINION != true"
        logger.info(f"\n[10/11] OPUS SECOND OPINION SKIPPED ({reason})")

    # ─────────────────────────────────────────────────────────────
    # Step 11: Generate Tiered Briefing
    # ─────────────────────────────────────────────────────────────
    logger.info("\n[11/11] GENERATING TIERED BRIEFING...")

    # Build StockBriefing objects for each analyzed stock + registry Tier 1/2
    briefings = []
    briefing_symbols = set()

    # Fresh analyses first
    for sym, analysis in analyses.items():
        tier_or_none = tier_assignments.get(sym)
        sc = screened_lookup.get(sym)
        ext_val = valuation_lookup.get(sym)

        if not tier_or_none or tier_or_none.tier == 0:
            continue

        tier = tier_or_none

        # Position sizing for Tier 1
        conv = getattr(analysis, "conviction_level", "MEDIUM")
        position_size = None
        if tier.tier == 1:
            position_size = calculate_position_size(
                portfolio_value=portfolio_value,
                conviction=conv,
                current_positions=current_positions,
            )

        if not ext_val:
            ext_val = AggregatedValuation(
                symbol=sym,
                current_price=tier.current_price or 0,
                estimates=[],
            )

        briefing = StockBriefing(
            symbol=sym,
            company_name=getattr(analysis, "company_name", sym) or sym,
            current_price=tier.current_price or ext_val.current_price,
            market_cap=sc.market_cap if sc else 0,
            pe_ratio=sc.pe_ratio if sc else None,
            debt_equity=sc.debt_equity if sc else None,
            roe=sc.roe if sc else None,
            revenue_growth=sc.revenue_growth if sc else None,
            valuation=ext_val,
            analysis=analysis,
            tier=tier.tier,
            tier_reason=tier.tier_reason,
            target_entry_price=tier.target_entry_price,
            price_gap_pct=tier.price_gap_pct,
            approaching_target=tier.approaching_target,
            position_size=position_size,
            fcf_yield=sc.fcf_yield if sc else None,
            earnings_quality=sc.earnings_quality if sc else None,
            payout_ratio=sc.payout_ratio if sc else None,
            operating_margin=sc.operating_margin if sc else None,
            opus_opinion=opus_opinions.get(sym),
        )
        briefings.append(briefing)
        briefing_symbols.add(sym)

    # Registry Tier 1/2 entries not already in briefings
    for sym, entry in registry_tier12.items():
        if sym in briefing_symbols:
            continue
        tier_info = tier_assignments.get(sym)
        if not tier_info or tier_info.tier == 0:
            continue

        # Build minimal briefing from registry data
        analysis_dict = entry.get("analysis", {})
        ext_val = AggregatedValuation(
            symbol=sym,
            current_price=entry.get("current_price_at_analysis") or 0,
            estimates=[],
        )

        # Use a SimpleNamespace as a duck-typed analysis for display
        from types import SimpleNamespace

        mock_analysis = SimpleNamespace(
            **{
                "company_name": entry.get("company_name", sym),
                "moat_rating": None,
                "conviction_level": analysis_dict.get("conviction", "MEDIUM"),
                "investment_thesis": analysis_dict.get("summary", ""),
                "key_risks": analysis_dict.get("key_risks", []),
                "thesis_risks": analysis_dict.get("thesis_risks", []),
                "management_rating": None,
                "moat_sources": [],
                "to_dict": lambda: analysis_dict,
            }
        )

        briefing = StockBriefing(
            symbol=sym,
            company_name=entry.get("company_name", sym),
            current_price=entry.get("current_price_at_analysis") or 0,
            market_cap=0,
            pe_ratio=None,
            debt_equity=None,
            roe=None,
            revenue_growth=None,
            valuation=ext_val,
            analysis=mock_analysis,
            tier=tier_info.tier,
            tier_reason=tier_info.tier_reason,
            target_entry_price=tier_info.target_entry_price,
            price_gap_pct=tier_info.price_gap_pct,
            approaching_target=tier_info.approaching_target,
        )
        briefings.append(briefing)
        briefing_symbols.add(sym)

    # Radar = unanalyzed Haiku-passed stocks from registry
    radar_stocks = registry.get_unanalyzed_haiku_passed()[:30]

    # Campaign progress for briefing
    campaign_progress = registry.get_campaign_progress(len(all_screened_symbols))
    stale = registry.get_stale_symbols(config.analysis_max_age_days)
    campaign_progress["stale_symbols"] = stale
    campaign_progress["max_age_days"] = config.analysis_max_age_days

    # Performance metrics from portfolio tracker
    perf = portfolio_summary.get("performance", {})
    performance_metrics = {
        "total_trades": perf.get("total_trades", 0),
        "winning_trades": perf.get("winning_trades", 0),
        "losing_trades": perf.get("losing_trades", 0),
        "win_rate": perf.get("win_rate", 0),
    }

    generator = BriefingGenerator(output_dir=str(data_dir / "briefings"))
    briefing_text = generator.generate_briefing(
        briefings=briefings,
        portfolio_summary=portfolio_summary,
        market_temp=market_temp,
        bubble_warnings=bubble_warnings,
        radar_stocks=radar_stocks,
        performance_metrics=performance_metrics,
        benchmark_data=benchmark_data,
        movements=movements,
        campaign_progress=campaign_progress,
    )

    # Read the generated HTML for email delivery
    html_content = None
    if hasattr(generator, "html_path") and generator.html_path.exists():
        html_content = generator.html_path.read_text()
        logger.info(f"HTML briefing: {generator.html_path}")

    # ─────────────────────────────────────────────────────────────
    # Send Notifications
    # ─────────────────────────────────────────────────────────────
    if send_notifications:
        logger.info("\nSENDING NOTIFICATIONS...")

        notifier = NotificationManager()
        results = notifier.send_briefing(briefing_text, html_content=html_content)

        for channel, success in results.items():
            status = "OK" if success else "FAIL"
            logger.info(f"  [{status}] {channel}")
    else:
        logger.info("\nNOTIFICATIONS SKIPPED")

    # ─────────────────────────────────────────────────────────────
    # Summary
    # ─────────────────────────────────────────────────────────────
    tier1_count = sum(1 for b in briefings if b.tier == 1)
    tier2_count = sum(1 for b in briefings if b.tier == 2)
    tier3_count = sum(1 for b in briefings if b.tier == 3)
    approaching_count = sum(1 for b in briefings if b.approaching_target)

    logger.info("\n" + "=" * 60)
    logger.info("WATCHLIST UPDATE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Market Temperature: {market_temp.get('temperature')}")
    logger.info(f"Stocks Analyzed:    {len(briefings)}")
    logger.info(f"Tier 1 (Buy Zone):  {tier1_count}")
    logger.info(f"Tier 2 (Watchlist): {tier2_count}")
    logger.info(f"Tier 3 (Monitor):   {tier3_count}")
    logger.info(f"Approaching Target: {approaching_count}")
    logger.info(f"Movements:          {len(movements)}")
    logger.info(f"Bubble Warnings:    {len(bubble_warnings)}")
    logger.info(f"Radar:              {len(radar_stocks)}")
    logger.info(
        f"Campaign:           {campaign_progress['campaign_id']} "
        f"({campaign_progress['coverage_pct']:.0%} coverage, "
        f"{campaign_progress['total_studied_all_time']} total studies)"
    )
    logger.info(f"\nBriefing saved to: {data_dir / 'briefings'}")
    if html_content:
        logger.info(f"HTML report:        {generator.html_path}")

    return briefings


if __name__ == "__main__":
    load_dotenv()

    # Check for required API keys
    required_keys = ["ANTHROPIC_API_KEY"]
    missing = [k for k in required_keys if not os.getenv(k)]

    if missing:
        logger.error(f"Missing required API keys: {missing}")
        logger.error("Please set them in .env file")
        sys.exit(1)

    # Run the briefing
    run_monthly_briefing(
        max_analyses=config.max_deep_analyses,
        send_notifications=True,
    )
