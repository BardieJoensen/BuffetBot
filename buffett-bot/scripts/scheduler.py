#!/usr/bin/env python3
"""
Scheduler Module - SAFE MODE

This scheduler ONLY runs lightweight, free operations automatically.
Expensive Claude API operations require MANUAL triggering.

AUTOMATIC (free/cheap):
- Weekly stock screening using yfinance (no API cost)
- Daily news fetching using Finnhub (free tier)

MANUAL ONLY (costs money):
- Full monthly briefing with Claude analysis
- Use: docker compose run --rm buffett-bot

This prevents accidental API costs from scheduler bugs.
"""

import os
import sys
import time
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import schedule

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
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
        from src.screener import StockScreener, ScreeningCriteria
        from src.valuation import screen_for_undervalued
        import json

        screener = StockScreener()
        candidates = screener.screen(ScreeningCriteria())

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

        watchlist = {
            "updated_at": datetime.now().isoformat(),
            "stocks": [v.to_dict() for v in valuations]
        }

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
            fair_value = stock.get("fair_value", 0)

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


def run_scheduler():
    """Start the scheduler - SAFE MODE"""

    logger.info("=" * 60)
    logger.info("BUFFETT BOT SCHEDULER - SAFE MODE")
    logger.info("=" * 60)
    logger.info("")
    logger.info("AUTOMATIC (free operations only):")
    logger.info("  - Weekly screen:   Every Sunday at 18:00 (yfinance)")
    logger.info("  - Daily check:     Every day at 08:00 (yfinance)")
    logger.info("")
    logger.info("MANUAL ONLY (requires explicit trigger):")
    logger.info("  - Full briefing:   docker compose run --rm buffett-bot")
    logger.info("")
    logger.info("This prevents accidental Claude API costs.")
    logger.info("=" * 60)
    logger.info("")

    # Schedule ONLY free operations
    schedule.every().sunday.at("18:00").do(weekly_screen)
    schedule.every().day.at("08:00").do(daily_watchlist_check)

    # NO automatic monthly briefing - that costs money!
    # User must manually run: docker compose run --rm buffett-bot

    logger.info("Scheduler running. Press Ctrl+C to stop.")
    logger.info(f"Next scheduled job: {schedule.next_run()}")
    logger.info("")

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


if __name__ == "__main__":
    load_dotenv()
    run_scheduler()
