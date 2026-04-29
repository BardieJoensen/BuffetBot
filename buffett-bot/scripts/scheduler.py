#!/usr/bin/env python3
"""
Scheduler Module

Runs automated jobs on a schedule:

AUTOMATIC (free):
- Weekly screen:    Every Friday at 17:00 (yfinance, free)
- Daily check:      Every day at 08:00 (yfinance, free)

AUTOMATIC (cheap, needs API keys):
- Weekly auto-trade: Every Friday at 18:00 (Haiku ~$0.10-0.20/week)
- Monthly briefing:  1st of month at 09:00 (Sonnet ~$0.30-0.50/run)

Kill switch: set AUTO_TRADE_ENABLED=false in .env to disable
auto-trading without removing Alpaca keys.

Set MONTHLY_BRIEFING_ENABLED=false to disable auto-briefing
(you can still run manually: docker compose run --rm buffett-bot).
"""

import logging
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

import schedule
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Approximate batch API costs (50% off real-time)
_HAIKU_COST_USD = 0.001  # per Haiku quick-screen call
_SONNET_COST_USD = 0.025  # per Sonnet deep-analysis call


def _moat_label(moat_hint: int) -> str:
    """Convert Haiku moat score (1–5) to WIDE / NARROW / NONE."""
    if moat_hint >= 4:
        return "WIDE"
    if moat_hint >= 3:
        return "NARROW"
    return "NONE"


def _build_db_summary(ticker: str, db) -> str:
    """
    Build a company summary from DB universe + fundamentals data.

    Used by wednesday_haiku_batch and friday_sonnet_batch to supply
    context to the LLM without making fresh yfinance calls.
    Monday's fundamentals refresh ensures this data is ≤4 days old.
    """
    u = db.get_universe_stock(ticker)
    f = db.get_latest_fundamentals(ticker)

    lines = [f"TICKER: {ticker}"]
    if u:
        if u.get("company_name"):
            lines.append(f"NAME: {u['company_name']}")
        if u.get("sector"):
            lines.append(f"SECTOR: {u['sector']}")
        mc = u.get("market_cap")
        if mc:
            lines.append(f"MARKET CAP: ${mc / 1e9:.1f}B")

    if f:
        lines.append("")
        lines.append("=== KEY FINANCIAL METRICS ===")
        if f.get("price"):
            lines.append(f"Current Price: ${f['price']:.2f}")
        if f.get("pe_ratio"):
            lines.append(f"P/E Ratio: {f['pe_ratio']:.1f}x")
        if f.get("roe") is not None:
            lines.append(f"Return on Equity (ROE): {f['roe']:.1%}")
        if f.get("roic") is not None:
            lines.append(f"Return on Invested Capital (ROIC): {f['roic']:.1%}")
        if f.get("operating_margin") is not None:
            lines.append(f"Operating Margin: {f['operating_margin']:.1%}")
        if f.get("fcf_yield") is not None:
            lines.append(f"FCF Yield (SBC-adj.): {f['fcf_yield']:.1%}")
        if f.get("debt_equity") is not None:
            lines.append(f"Debt/Equity: {f['debt_equity']:.2f}x")
        if f.get("revenue_growth") is not None:
            lines.append(f"Revenue Growth (YoY): {f['revenue_growth']:.1%}")

    return "\n".join(lines)


def weekly_screen():
    """
    Run stock screening weekly to update watchlist.

    This is FREE - only uses yfinance (no API key).
    Does NOT call Claude API.
    """
    logger.info("=" * 50)
    logger.info("WEEKLY SCREEN - FREE (yfinance only)")
    logger.info("=" * 50)

    try:
        import json

        from src.screener import StockScreener, load_criteria_from_yaml
        from src.valuation import screen_for_undervalued

        screener = StockScreener()
        criteria = load_criteria_from_yaml()
        candidates = screener.screen(criteria)

        logger.info(f"Screened {len(candidates)} candidates")

        # Get valuations for top candidates (uses yfinance + Finnhub, no Claude)
        symbols = [c.symbol for c in candidates[:50]]
        valuations = screen_for_undervalued(symbols, min_margin_of_safety=0.10)

        # Save to watchlist
        data_dir = Path("./data")
        try:
            data_dir.mkdir(exist_ok=True)
        except PermissionError:
            data_dir = Path(tempfile.gettempdir()) / "buffett-bot-data"
            data_dir.mkdir(exist_ok=True)

        watchlist = {"updated_at": datetime.now().isoformat(), "stocks": [v.to_dict() for v in valuations]}

        watchlist_file = data_dir / "watchlist.json"
        watchlist_file.write_text(json.dumps(watchlist, indent=2))

        logger.info(f"Weekly screen complete. {len(valuations)} stocks on watchlist.")
        logger.info(f"Saved to {watchlist_file}")

        # Log top picks
        if valuations:
            logger.info("\nTop undervalued stocks:")
            for v in valuations[:5]:
                logger.info(f"  {v.symbol}: {v.margin_of_safety:.1%} margin of safety")

    except Exception as e:
        logger.error(f"Weekly screen failed: {e}")


def daily_watchlist_check():
    """
    Quick daily check of watchlist prices.

    This is FREE - only uses yfinance.
    Does NOT call Claude API.
    """
    logger.info("Daily watchlist price check...")

    watchlist_path = Path("./data/watchlist.json")
    if not watchlist_path.exists():
        watchlist_path = Path(tempfile.gettempdir()) / "buffett-bot-data" / "watchlist.json"

    if not watchlist_path.exists():
        logger.info("No watchlist found. Run weekly_screen first.")
        return

    try:
        import json

        import yfinance as yf

        watchlist = json.loads(watchlist_path.read_text())
        stocks = watchlist.get("stocks", [])

        if not stocks:
            logger.info("Watchlist is empty.")
            return

        logger.info(f"Checking {len(stocks)} stocks...")

        alerts = []
        for stock in stocks[:10]:  # Only check top 10
            symbol = stock.get("symbol")
            fair_value = stock.get("average_fair_value", 0)

            try:
                ticker = yf.Ticker(symbol)
                current_price = ticker.info.get("currentPrice", 0)

                if current_price and fair_value:
                    margin = (fair_value - current_price) / fair_value
                    if margin > 0.30:  # 30%+ margin of safety
                        alerts.append(f"{symbol}: ${current_price:.2f} (margin: {margin:.1%})")
            except Exception as e:
                logger.debug(f"Price check failed for {symbol}: {e}")

            time.sleep(0.1)

        if alerts:
            logger.info("\n🔔 ALERTS - Stocks with >30% margin of safety:")
            for alert in alerts:
                logger.info(f"  {alert}")
        else:
            logger.info("No stocks currently at >30% margin of safety.")

    except Exception as e:
        logger.error(f"Daily check failed: {e}")


def weekly_auto_trade():
    """
    Weekly auto-trade job using Haiku pre-screening.

    Runs Friday at 18:00 (after market close at 16:00 ET).
    Orders placed now will queue for Monday open via Alpaca.
    Cost: ~$0.10-0.20/week (Haiku only, no Sonnet).

    - Loads watchlist from weekly_screen
    - Runs Haiku quick-screen on top candidates
    - Fetches valuations, determines recommendations
    - Executes paper buys for BUY signals
    - Checks existing positions: sells if stock has risen to near fair value
      (margin of safety < 5% = take-profit, the stock is no longer undervalued)
    """
    logger.info("=" * 50)
    logger.info("WEEKLY AUTO-TRADE (Haiku + Alpaca paper)")
    logger.info("=" * 50)

    try:
        from src.paper_trader import PaperTrader

        # Check kill switch
        if not PaperTrader.auto_trade_enabled():
            logger.info("AUTO_TRADE_ENABLED=false — skipping auto-trade")
            return

        trader = PaperTrader()
        if not trader.is_enabled():
            logger.info("Alpaca not configured — skipping auto-trade")
            return

        import json

        from src.analyzer import CompanyAnalyzer
        from src.valuation import ValuationAggregator, screen_for_undervalued

        # Load watchlist from weekly_screen
        watchlist_path = Path("./data/watchlist.json")
        if not watchlist_path.exists():
            watchlist_path = Path(tempfile.gettempdir()) / "buffett-bot-data" / "watchlist.json"
        if not watchlist_path.exists():
            logger.info("No watchlist found — run weekly_screen first")
            return

        watchlist = json.loads(watchlist_path.read_text())
        stocks = watchlist.get("stocks", [])
        if not stocks:
            logger.info("Watchlist is empty")
            return

        # Get top undervalued symbols
        symbols = [s.get("symbol") for s in stocks[:30] if s.get("symbol")]
        valuations = screen_for_undervalued(symbols, min_margin_of_safety=0.10)

        if not valuations:
            logger.info("No undervalued stocks found")
            return

        # Haiku quick-screen on top candidates
        analyzer = CompanyAnalyzer()
        import yfinance as yf

        haiku_results = []
        for val in valuations[:20]:
            try:
                ticker = yf.Ticker(val.symbol)
                desc = ticker.info.get("longBusinessSummary", f"Company: {val.symbol}")
                result = analyzer.quick_screen(val.symbol, desc)
                result["valuation"] = val
                haiku_results.append(result)
            except Exception as e:
                logger.warning(f"Haiku screen failed for {val.symbol}: {e}")

        # Sort by quality and buy top picks
        haiku_results.sort(key=lambda r: r["moat_hint"] + r["quality_hint"], reverse=True)

        min_margin = config.margin_of_safety_pct
        portfolio_value = config.portfolio_value
        current_positions = len(trader.get_positions())
        max_positions = 10

        for result in haiku_results[:5]:
            val = result["valuation"]
            if not result["worth_analysis"]:
                continue
            if val.margin_of_safety is None or val.margin_of_safety < min_margin:
                continue
            if current_positions >= max_positions:
                logger.info("Max positions reached — stopping buys")
                break

            # Simple position sizing: equal weight
            amount = portfolio_value * 0.10  # 10% per position
            order = trader.buy(val.symbol, amount)
            if order:
                current_positions += 1
                logger.info(f"Bought {val.symbol}: ${amount:,.0f}")

        # Check existing positions for take-profit sells.
        # Margin of safety = (fair_value - price) / fair_value.
        # When margin drops below 5%, the stock has risen to near fair value
        # — it's no longer undervalued, so we take profit and free up capital.
        positions = trader.get_positions()
        if positions:
            logger.info(f"\nChecking {len(positions)} existing positions for sell signals...")
            aggregator = ValuationAggregator()

            for pos in positions:
                symbol = pos["symbol"]
                try:
                    val = aggregator.get_valuation(symbol)
                    if val and val.margin_of_safety is not None and val.margin_of_safety < 0.05:
                        trader.sell(
                            symbol,
                            reason=f"Take profit: margin of safety {val.margin_of_safety:.1%} "
                            f"(stock near fair value ${val.average_fair_value:.2f})",
                        )
                except Exception as e:
                    logger.warning(f"Error checking {symbol}: {e}")

        logger.info("Weekly auto-trade complete")

    except Exception as e:
        logger.error(f"Weekly auto-trade failed: {e}")


def monthly_briefing():
    """
    Run the full monthly briefing pipeline automatically.

    Scheduled for the 1st of each month at 09:00.
    Cost: ~$0.30-0.50 per run (Sonnet deep analyses, cached for 30 days).

    Disable with MONTHLY_BRIEFING_ENABLED=false in .env.
    """
    # Only run on the 1st of the month
    if datetime.now().day != 1:
        return

    if not config.monthly_briefing_enabled:
        logger.info("MONTHLY_BRIEFING_ENABLED=false — skipping auto-briefing")
        return

    logger.info("=" * 50)
    logger.info("MONTHLY BRIEFING (automatic)")
    logger.info("=" * 50)

    try:
        from scripts.run_briefing import main as run_db_briefing

        run_db_briefing(send_notifications=True)

    except Exception as e:
        logger.error(f"Monthly briefing failed: {e}")

        # Try to notify about the failure
        try:
            from src.notifications import NotificationManager

            notifier = NotificationManager()
            notifier.send_alert("SYSTEM", f"Monthly briefing failed: {e}")
        except Exception as notify_err:
            logger.debug(f"Failed to send failure notification: {notify_err}")


def monday_maintenance():
    """
    Monday 02:00 — weekly housekeeping before market open.

    Steps (run in order so each builds on the previous):
    1. Reset weekly budget caps (fresh allowance for the week)
    2. Refresh fundamentals + quality scores for all universe stocks
    3. Update price_alerts last_price / gap_pct from yfinance
    4. Sync paper positions from Alpaca (if configured)
    5. Data retention cleanup

    This is FREE except for the yfinance calls (no LLM usage).
    Wednesday and Friday jobs rely on the fresh DB data written here.
    """
    logger.info("=" * 50)
    logger.info("MONDAY MAINTENANCE")
    logger.info("=" * 50)

    try:
        from datetime import date as _date

        import yfinance as yf

        from src.database import Database
        from src.quality_scorer import compute_quality_scores
        from src.screener import StockScreener, load_criteria_from_yaml

        db = Database()

        # 1. Reset budget caps — fresh allowance for the whole week
        db.reset_weekly_budgets()
        logger.info("Weekly budget caps reset")

        # 2. Refresh fundamentals for all universe stocks
        universe = db.get_universe()
        tickers = [s["ticker"] for s in universe]
        conviction_tickers = {s["ticker"] for s in universe if s.get("source") == "conviction"}

        if tickers:
            screener = StockScreener()
            criteria = load_criteria_from_yaml()
            sp500_tickers = {s["ticker"] for s in universe if s.get("source") == "sp500_filter"}
            screened = screener.screen_tickers(
                tickers, criteria, force_include=conviction_tickers, force_large_cap=sp500_tickers
            )

            today = _date.today().isoformat()
            source_map = {s["ticker"]: s.get("source", "finviz_screen") for s in universe}
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
            logger.info("Refreshed fundamentals for %d/%d universe stocks", len(screened), len(tickers))

            # Recompute quality scores from fresh data
            compat = [{**s.to_dict(), "ticker": s.symbol} for s in screened]
            scores = compute_quality_scores(compat)
            for sym, qs in scores.items():
                db.update_quality_score(sym, qs.score)
            logger.info("Recomputed quality scores for %d stocks", len(scores))

        # 3. Update price alert last_price / gap_pct
        alerts = db.get_price_alerts()
        updated = 0
        for alert in alerts:
            symbol = alert["ticker"]
            target_entry = alert.get("target_entry")
            try:
                current_price = yf.Ticker(symbol).fast_info.last_price
                if current_price and target_entry:
                    gap_pct = (current_price - target_entry) / target_entry
                    db.upsert_price_alert(
                        symbol,
                        tier=alert["tier"],
                        target_entry=target_entry,
                        staged_entries=alert.get("staged_entries"),
                        last_price=current_price,
                        gap_pct=gap_pct,
                        alert_triggered=bool(alert.get("alert_triggered")),
                    )
                    updated += 1
            except Exception as exc:
                logger.debug("Price update failed for %s: %s", symbol, exc)
        logger.info("Updated %d/%d price alerts", updated, len(alerts))

        # 4. Sync paper positions from Alpaca
        try:
            from src.paper_trader import PaperTrader

            trader = PaperTrader()
            if trader.is_enabled():
                positions = trader.get_positions()
                for pos in positions:
                    da = db.get_latest_deep_analysis(pos["symbol"])
                    tier_at_entry = da.get("tier", "C") if da else "C"
                    db.upsert_paper_position(
                        pos["symbol"],
                        tier_at_entry=tier_at_entry,
                        current_price=pos.get("current_price"),
                        current_value=pos.get("market_value"),
                        gain_loss_pct=pos.get("unrealized_plpc"),
                        shares=pos.get("qty"),
                    )
                logger.info("Synced %d paper positions from Alpaca", len(positions))
            else:
                logger.info("Alpaca not configured — skipping paper position sync")
        except Exception as exc:
            logger.warning("Paper position sync failed: %s", exc)

        # 5. Data retention cleanup
        deleted = db.run_retention_cleanup()
        logger.info(
            "Retention cleanup: %s",
            ", ".join(f"{k}={v}" for k, v in deleted.items()),
        )

        logger.info("Monday maintenance complete")

    except Exception as e:
        logger.error("Monday maintenance failed: %s", e)


def wednesday_haiku_batch():
    """
    Wednesday 07:00 — Haiku pre-screening via batch API.

    Screens top 30 unscreened tickers (no valid Haiku result) and top 20
    with expiring results (< 30 days to expiry).  Respects the
    weekly_haiku_screen budget cap (50 calls/week).  Uses the Anthropic
    batch API for 50% cost savings vs real-time calls.

    Cost: up to $0.05 (50 × $0.001).

    Reads fundamentals from the DB populated by Monday's refresh — no
    fresh yfinance calls needed.
    """
    logger.info("=" * 50)
    logger.info("WEDNESDAY HAIKU BATCH")
    logger.info("=" * 50)

    try:
        from src.analyzer import CompanyAnalyzer
        from src.database import Database

        db = Database()

        # Gather candidates: unscreened first (higher priority), then expiring
        unscreened = db.get_unscreened_tickers(limit=30)
        expiring = db.get_expiring_haiku_tickers(within_days=30, limit=20)

        # Deduplicate while preserving priority order (unscreened first)
        seen: set[str] = set()
        candidates: list[str] = []
        for t in unscreened + expiring:
            if t not in seen:
                seen.add(t)
                candidates.append(t)

        if not candidates:
            logger.info("No Haiku candidates — universe fully screened")
            return

        # Reserve budget up front (atomic check-and-reserve)
        allowed = db.spend_batch("weekly_haiku_screen", len(candidates))
        if allowed == 0:
            logger.info("weekly_haiku_screen budget exhausted — skipping Wednesday Haiku batch")
            return

        candidates = candidates[:allowed]
        logger.info(
            "Haiku batch: %d unscreened + %d expiring → %d candidates (%d budget-allowed)",
            len(unscreened),
            len(expiring),
            len(seen),
            allowed,
        )

        # Build summaries from DB (Monday refresh provides up-to-date data)
        to_screen: list[tuple[str, str]] = [(ticker, _build_db_summary(ticker, db)) for ticker in candidates]

        run_id = db.start_run("wednesday_haiku")
        analyzer = CompanyAnalyzer()
        batch_results = analyzer.batch_quick_screen(to_screen)

        for result in batch_results:
            symbol = result.get("symbol", "")
            if not symbol:
                continue
            db.save_haiku_result(
                symbol,
                passed=result.get("worth_analysis", False),
                moat_estimate=_moat_label(result.get("moat_hint", 0)),
                summary=result.get("reason", ""),
            )

        passed = sum(1 for r in batch_results if r.get("worth_analysis"))
        db.complete_run(
            run_id,
            stocks_screened=len(batch_results),
            haiku_calls=len(batch_results),
            total_cost_usd=len(batch_results) * _HAIKU_COST_USD,
        )
        logger.info("Wednesday Haiku batch complete: %d screened, %d passed", len(batch_results), passed)

    except Exception as e:
        logger.error("Wednesday Haiku batch failed: %s", e)


def friday_sonnet_batch():
    """
    Friday 07:00 — Sonnet deep-dive on new Haiku passes via batch API.

    Analyzes up to 5 tickers whose Haiku pre-screen passed but have no
    valid (non-expired) deep analysis.  Runs before the existing Friday
    17:00 weekly_screen job so the watchlist is primed with fresh tiers.
    Respects the weekly_sonnet_analysis budget cap (10 calls/week).

    Cost: up to $0.125 (5 × $0.025).
    """
    logger.info("=" * 50)
    logger.info("FRIDAY SONNET BATCH")
    logger.info("=" * 50)

    try:
        from src.analyzer import CompanyAnalyzer
        from src.database import Database
        from src.tier_engine import assign_tier, staged_entry_suggestion
        from src.valuation import ValuationAggregator

        db = Database()

        candidates = db.get_haiku_passes_without_analysis(limit=5)
        if not candidates:
            logger.info("No Haiku passes waiting for Sonnet analysis")
            return

        allowed = db.spend_batch("weekly_sonnet_analysis", len(candidates))
        if allowed == 0:
            logger.info("weekly_sonnet_analysis budget exhausted — skipping Friday Sonnet batch")
            return

        candidates = candidates[:allowed]
        logger.info("Sonnet batch: %d candidates (%d budget-allowed)", len(candidates), allowed)

        to_analyze: list[dict] = []
        for ticker in candidates:
            u = db.get_universe_stock(ticker)
            to_analyze.append(
                {
                    "symbol": ticker,
                    "company_name": (u or {}).get("company_name", ticker),
                    "filing_text": _build_db_summary(ticker, db),
                    "sector": (u or {}).get("sector", ""),
                }
            )

        run_id = db.start_run("friday_sonnet")
        analyzer = CompanyAnalyzer()
        analyses = analyzer.batch_analyze_companies(to_analyze)

        aggregator = ValuationAggregator()

        for analysis in analyses:
            ticker = analysis.symbol
            old_da = db.get_latest_deep_analysis(ticker)
            old_tier = old_da.get("tier") if old_da else None

            external_val = None
            if analysis.target_entry_price is None:
                try:
                    external_val = aggregator.get_valuation(ticker)
                except Exception as exc:
                    logger.debug("External valuation failed for %s: %s", ticker, exc)

            tier_assignment = assign_tier(analysis, external_valuation=external_val)
            resolved_target = tier_assignment.target_entry_price
            resolved_price = tier_assignment.current_price or analysis.current_price

            db.save_deep_analysis(
                ticker,
                tier=tier_assignment.tier,
                conviction=analysis.conviction,
                moat_rating=analysis.moat_rating.value.upper(),
                moat_sources=analysis.moat_sources,
                fair_value=((analysis.estimated_fair_value_low or 0) + (analysis.estimated_fair_value_high or 0)) / 2
                or None,
                target_entry=resolved_target,
                investment_thesis=analysis.summary,
                key_risks=analysis.key_risks,
                thesis_breakers=analysis.thesis_risks,
            )

            db.log_tier_change(
                ticker,
                new_tier=tier_assignment.tier,
                old_tier=old_tier,
                trigger="scheduled",
                reason=tier_assignment.tier_reason,
            )

            if tier_assignment.tier in ("S", "A", "B"):
                entries = staged_entry_suggestion(resolved_target, tier_assignment.tier) if resolved_target else []
                db.upsert_price_alert(
                    ticker,
                    tier=tier_assignment.tier,
                    target_entry=resolved_target,
                    staged_entries=entries,
                    last_price=resolved_price,
                    gap_pct=tier_assignment.price_gap_pct,
                )

            logger.info(
                "%s → Tier %s (%s conviction, gap=%.0f%%)",
                ticker,
                tier_assignment.tier,
                analysis.conviction,
                (tier_assignment.price_gap_pct or 0) * 100,
            )

        db.complete_run(
            run_id,
            stocks_screened=len(analyses),
            sonnet_calls=len(analyses),
            total_cost_usd=len(analyses) * _SONNET_COST_USD,
        )
        logger.info("Friday Sonnet batch complete: %d analyzed", len(analyses))

    except Exception as e:
        logger.error("Friday Sonnet batch failed: %s", e)


def daily_news_monitor():
    """
    Daily 20:00 — news pipeline for S/A/B tier stocks.

    Fetches company news from Finnhub, runs keyword filter, Haiku materiality
    check, and optionally Sonnet re-analysis when red flags are detected.

    Budget caps:
        weekly_news_haiku:  50 calls/week (~$0.05)
        weekly_news_sonnet: 10 calls/week (~$0.25)

    Kill switch: job is silently skipped if FINNHUB_API_KEY is not set.
    """
    logger.info("=" * 50)
    logger.info("DAILY NEWS MONITOR")
    logger.info("=" * 50)

    try:
        from src.analyzer import CompanyAnalyzer
        from src.database import Database
        from src.news_fetcher import FinnhubNewsFetcher, run_news_pipeline

        db = Database()
        fetcher = FinnhubNewsFetcher()

        if not fetcher.api_key:
            logger.info("FINNHUB_API_KEY not set — skipping daily news monitor")
            return

        analyzer = CompanyAnalyzer()

        notifier = None
        try:
            from src.notifications import NotificationManager

            notifier = NotificationManager()
        except Exception:
            pass

        stats = run_news_pipeline(db, analyzer, fetcher, notifier=notifier)
        logger.info(
            "Daily news monitor complete: %d ticker(s) checked, %d Haiku, %d Sonnet",
            stats["tickers_checked"],
            stats["haiku_calls"],
            stats["sonnet_calls"],
        )

    except Exception as e:
        logger.error("Daily news monitor failed: %s", e)


def run_scheduler():
    """Start the scheduler"""

    auto_trade = config.auto_trade_enabled
    auto_briefing = config.monthly_briefing_enabled

    logger.info("=" * 60)
    logger.info("BUFFETT BOT SCHEDULER")
    logger.info("=" * 60)
    logger.info("")
    logger.info("SCHEDULED JOBS:")
    logger.info("  - Monday maintenance:  Every Monday at 02:00   (yfinance, free)")
    logger.info("  - Wednesday Haiku:     Every Wednesday at 07:00 (Haiku batch, ~$0.05/wk)")
    logger.info("  - Friday Sonnet:       Every Friday at 07:00   (Sonnet batch, ~$0.13/wk)")
    logger.info("  - Weekly screen:       Every Friday at 17:00   (yfinance, free)")
    logger.info(
        f"  - Weekly auto-trade:   Every Friday at 18:00   (Haiku ~$0.10-0.20) [{'ON' if auto_trade else 'OFF'}]"
    )
    logger.info("  - Daily check:         Every day at 08:00      (yfinance, free)")
    logger.info("  - Daily news monitor:  Every day at 20:00      (Haiku+Sonnet, ~$0.30/wk max)")
    logger.info(
        f"  - Monthly briefing:    1st of month at 09:00   (Sonnet ~$0.50) [{'ON' if auto_briefing else 'OFF'}]"
    )
    logger.info("")
    logger.info("BUDGET CAPS (weekly, reset Monday 02:00):")
    logger.info("  weekly_haiku_screen:    50 calls/week ($0.05 max)")
    logger.info("  weekly_sonnet_analysis: 10 calls/week ($0.25 max)")
    logger.info("  weekly_news_haiku:      50 calls/week ($0.05 max)")
    logger.info("  weekly_news_sonnet:     10 calls/week ($0.25 max)")
    logger.info("")
    logger.info("KILL SWITCHES (in .env):")
    logger.info(f"  AUTO_TRADE_ENABLED={auto_trade}        — disable weekly auto-trading")
    logger.info(f"  MONTHLY_BRIEFING_ENABLED={auto_briefing} — disable monthly briefing")
    logger.info("")
    logger.info("MANUAL TRIGGER:")
    logger.info("  docker compose run --rm buffett-bot")
    logger.info("")
    logger.info("TRADE LOG: data/trade_log.json")
    logger.info("=" * 60)
    logger.info("")

    # Phase D: weekly cadence (DB-based)
    schedule.every().monday.at("02:00").do(monday_maintenance)
    schedule.every().wednesday.at("07:00").do(wednesday_haiku_batch)
    schedule.every().friday.at("07:00").do(friday_sonnet_batch)

    # Phase E: daily news pipeline (DB-based, requires FINNHUB_API_KEY)
    schedule.every().day.at("20:00").do(daily_news_monitor)

    # Legacy free operations (watchlist.json-based)
    schedule.every().friday.at("17:00").do(weekly_screen)
    schedule.every().day.at("08:00").do(daily_watchlist_check)

    # Paid operations (have their own kill switches)
    schedule.every().friday.at("18:00").do(weekly_auto_trade)
    schedule.every().day.at("09:00").do(monthly_briefing)  # Only actually runs on the 1st

    logger.info("Scheduler running. Press Ctrl+C to stop.")
    logger.info(f"Next scheduled job: {schedule.next_run()}")
    logger.info("")

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


if __name__ == "__main__":
    load_dotenv()
    run_scheduler()
