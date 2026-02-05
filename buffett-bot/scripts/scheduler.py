#!/usr/bin/env python3
"""
Scheduler Module

Runs the briefing pipeline on a schedule:
- Weekly: Stock screening + valuation check
- Monthly: Full deep analysis + briefing generation
- Daily: News monitoring for portfolio holdings (optional)

Uses the 'schedule' library for simple scheduling.
For production, consider using cron or Airflow.
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
    """Run stock screening weekly to update watchlist"""
    logger.info("Running weekly screen...")
    
    try:
        from src.screener import StockScreener, ScreeningCriteria
        from src.valuation import screen_for_undervalued
        import json
        
        screener = StockScreener()
        candidates = screener.screen(ScreeningCriteria())
        
        # Get valuations for top candidates
        symbols = [c.symbol for c in candidates[:50]]
        valuations = screen_for_undervalued(symbols, min_margin_of_safety=0.10)
        
        # Save to watchlist
        data_dir = Path("./data")
        data_dir.mkdir(exist_ok=True)
        
        watchlist = {
            "updated_at": datetime.now().isoformat(),
            "stocks": [v.to_dict() for v in valuations]
        }
        
        (data_dir / "watchlist.json").write_text(json.dumps(watchlist, indent=2))
        logger.info(f"Weekly screen complete. {len(valuations)} stocks on watchlist.")
        
    except Exception as e:
        logger.error(f"Weekly screen failed: {e}")


def monthly_briefing():
    """Run full monthly briefing generation"""
    logger.info("Running monthly briefing...")
    
    try:
        from scripts.run_monthly_briefing import run_monthly_briefing
        run_monthly_briefing(use_cache=False)
        logger.info("Monthly briefing complete.")
        
    except Exception as e:
        logger.error(f"Monthly briefing failed: {e}")


def daily_news_check():
    """Check news for portfolio holdings (if portfolio exists)"""
    logger.info("Running daily news check...")
    
    portfolio_path = Path("./data/portfolio.json")
    if not portfolio_path.exists():
        logger.info("No portfolio found. Skipping news check.")
        return
    
    try:
        import json
        from src.analyzer import CompanyAnalyzer
        
        portfolio = json.loads(portfolio_path.read_text())
        holdings = portfolio.get("holdings", [])
        
        if not holdings:
            logger.info("Portfolio is empty. Skipping news check.")
            return
        
        # Would implement news fetching and analysis here
        # For now, just log
        logger.info(f"Would check news for {len(holdings)} holdings")
        
    except Exception as e:
        logger.error(f"Daily news check failed: {e}")


def run_scheduler():
    """Start the scheduler"""
    
    logger.info("=" * 50)
    logger.info("BUFFETT BOT SCHEDULER STARTED")
    logger.info("=" * 50)
    logger.info("")
    logger.info("Schedule:")
    logger.info("  - Weekly screen:    Every Sunday at 18:00")
    logger.info("  - Monthly briefing: 1st of month at 19:00")
    logger.info("  - News check:       Daily at 08:00 (if portfolio exists)")
    logger.info("")
    
    # Schedule jobs
    schedule.every().sunday.at("18:00").do(weekly_screen)
    schedule.every().day.at("08:00").do(daily_news_check)
    
    # Monthly briefing on the 1st
    def monthly_check():
        if datetime.now().day == 1:
            monthly_briefing()
    
    schedule.every().day.at("19:00").do(monthly_check)
    
    # Run immediately on startup for testing
    if os.getenv("RUN_ON_START", "false").lower() == "true":
        logger.info("RUN_ON_START enabled. Running initial screen...")
        weekly_screen()
    
    # Main loop
    logger.info("Scheduler running. Press Ctrl+C to stop.")
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


if __name__ == "__main__":
    load_dotenv()
    run_scheduler()
