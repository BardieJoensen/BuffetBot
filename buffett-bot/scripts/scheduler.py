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
import time
from datetime import datetime
from pathlib import Path

import schedule
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


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
            data_dir = Path("/tmp/buffett-bot-data")
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
        watchlist_path = Path("/tmp/buffett-bot-data/watchlist.json")

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
            except Exception:
                pass

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
            watchlist_path = Path("/tmp/buffett-bot-data/watchlist.json")
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
        from scripts.run_monthly_briefing import run_monthly_briefing

        run_monthly_briefing(
            max_analyses=config.max_deep_analyses,
            send_notifications=True,
        )

    except Exception as e:
        logger.error(f"Monthly briefing failed: {e}")

        # Try to notify about the failure
        try:
            from src.notifications import NotificationManager

            notifier = NotificationManager()
            notifier.send_alert("SYSTEM", f"Monthly briefing failed: {e}")
        except Exception:
            pass


def run_scheduler():
    """Start the scheduler"""

    auto_trade = config.auto_trade_enabled
    auto_briefing = config.monthly_briefing_enabled

    logger.info("=" * 60)
    logger.info("BUFFETT BOT SCHEDULER")
    logger.info("=" * 60)
    logger.info("")
    logger.info("SCHEDULED JOBS:")
    logger.info("  - Weekly screen:     Every Friday at 17:00 (yfinance, free)")
    logger.info(f"  - Weekly auto-trade: Every Friday at 18:00 (Haiku ~$0.10-0.20) [{'ON' if auto_trade else 'OFF'}]")
    logger.info("  - Daily check:       Every day at 08:00 (yfinance, free)")
    logger.info(f"  - Monthly briefing:  1st of month at 09:00 (Sonnet ~$0.50) [{'ON' if auto_briefing else 'OFF'}]")
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

    # Free operations
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
