#!/usr/bin/env python3
"""
Monthly Briefing Runner

Orchestrates the full pipeline:
1. Check market temperature
2. Screen stocks with value criteria
3. Detect bubble stocks to avoid
4. Fetch valuations for candidates
5. Run LLM analysis on top picks
6. Calculate position sizing
7. Generate comprehensive briefing
8. Send notifications

COST WARNING:
This script calls the Claude API which costs money!
- Each deep analysis costs ~$0.03-0.05 (Sonnet)
- Haiku pre-screen costs ~$0.002 per stock
- 10 analyses = ~$0.30-0.50 per run
- Analyses are cached for 30 days to avoid re-running

Run manually:  docker compose run --rm buffett-bot
Auto-schedule: scheduler.py runs this on 1st of each month (if enabled)
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analyzer import CompanyAnalyzer, set_cache_dir
from src.briefing import BriefingGenerator, StockBriefing, determine_recommendation
from src.bubble_detector import BubbleDetector, get_market_temperature
from src.notifications import NotificationManager
from src.portfolio import PortfolioTracker, calculate_position_size
from src.screener import StockScreener, load_criteria_from_yaml
from src.valuation import screen_for_undervalued

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_cached_watchlist(cache_path: Path) -> list[dict]:
    """Load watchlist from cache if recent enough"""
    # Check both primary and fallback locations
    paths_to_check = [cache_path, Path("/tmp/buffett-bot-watchlist.json")]

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
        # Fall back to /tmp if data directory isn't writable
        fallback_path = Path("/tmp/buffett-bot-watchlist.json")
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


def run_monthly_briefing(
    max_analyses: int = 10, min_margin_of_safety: float = 0.20, use_cache: bool = True, send_notifications: bool = True
):
    """
    Run the full monthly briefing pipeline.

    Args:
        max_analyses: Maximum number of stocks to analyze with Claude (costs ~$0.05 each)
        min_margin_of_safety: Minimum margin of safety to consider (0.20 = 20%)
        use_cache: Use cached analyses to avoid re-running (recommended)
        send_notifications: Send results via configured notification channels
    """

    # Hard limit on analyses to prevent runaway costs
    max_analyses = min(max_analyses, 15)

    logger.info("═" * 60)
    logger.info("STARTING MONTHLY BRIEFING GENERATION")
    logger.info("═" * 60)
    logger.info("")
    logger.info("💰 COST ESTIMATE:")
    logger.info(f"   Max analyses: {max_analyses} stocks")
    logger.info(f"   Est. cost: ~${max_analyses * 0.05:.2f} (Sonnet)")
    logger.info(f"   Cache enabled: {use_cache} (reuses analyses < 30 days old)")
    logger.info("")

    # Set up data directory with fallback for permission issues
    data_dir = Path("./data")
    try:
        data_dir.mkdir(exist_ok=True)
        # Test write permission
        test_file = data_dir / ".write_test"
        test_file.write_text("test")
        test_file.unlink()
    except PermissionError:
        data_dir = Path("/tmp/buffett-bot-data")
        data_dir.mkdir(exist_ok=True)
        logger.warning(f"Using fallback data dir: {data_dir}")

    set_cache_dir(data_dir / "analyses")
    watchlist_cache = data_dir / "watchlist_cache.json"

    # ─────────────────────────────────────────────────────────────
    # Step 1: Market Temperature
    # ─────────────────────────────────────────────────────────────
    logger.info("\n[1/8] CHECKING MARKET TEMPERATURE...")

    market_temp = get_market_temperature()
    logger.info(f"Market: {market_temp.get('temperature')} - {market_temp.get('interpretation', '')[:50]}...")

    # ─────────────────────────────────────────────────────────────
    # Step 2: Screen Stocks
    # ─────────────────────────────────────────────────────────────
    logger.info("\n[2/8] SCREENING STOCKS...")

    cached_stocks = load_cached_watchlist(watchlist_cache) if use_cache else []

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
            logger.error(f"\n❌ SCREENING FAILED: {e}\n")
            logger.error("🔧 TROUBLESHOOTING:")
            logger.error("  1. Check your internet connection")
            logger.error("  2. yfinance may be temporarily unavailable - try again later")
            logger.error("  3. Try relaxing screening criteria in config/screening_criteria.yaml\n")
            return

        if len(candidates) > 100:
            candidates = candidates[:100]

        candidates = screener.apply_detailed_filters(candidates, criteria)
        symbols = [c.symbol for c in candidates]

        save_watchlist(candidates, watchlist_cache)

    # Store all screened symbols for "radar"
    all_screened_symbols = symbols.copy()

    if not symbols:
        logger.warning("No stocks passed screening. Exiting.")
        return

    # ─────────────────────────────────────────────────────────────
    # Step 3: Detect Bubbles
    # ─────────────────────────────────────────────────────────────
    logger.info("\n[3/8] SCANNING FOR BUBBLE STOCKS...")

    bubble_detector = BubbleDetector()
    bubble_warnings = bubble_detector.scan_for_bubbles()  # Scans trending stocks

    logger.info(f"Found {len(bubble_warnings)} potential bubble stocks")

    # ─────────────────────────────────────────────────────────────
    # Step 4: Get Valuations
    # ─────────────────────────────────────────────────────────────
    logger.info(f"\n[4/8] FETCHING VALUATIONS FOR {min(50, len(symbols))} STOCKS...")

    valuations = screen_for_undervalued(symbols[:50], min_margin_of_safety=min_margin_of_safety * 0.5)

    logger.info(f"Found {len(valuations)} potentially undervalued stocks")

    if not valuations:
        logger.warning("No undervalued stocks found. Lowering threshold...")
        valuations = screen_for_undervalued(symbols[:30], min_margin_of_safety=0.05)

    # ─────────────────────────────────────────────────────────────
    # Step 5: Portfolio Check
    # ─────────────────────────────────────────────────────────────
    logger.info("\n[5/8] CHECKING PORTFOLIO STATUS...")

    portfolio_tracker = PortfolioTracker(data_dir=str(data_dir))
    portfolio_summary = portfolio_tracker.get_portfolio_summary()

    portfolio_value = float(os.getenv("PORTFOLIO_VALUE", 50000))
    current_positions = portfolio_summary.get("position_count", 0)

    logger.info(f"Portfolio: {current_positions} positions, ${portfolio_summary.get('current_value', 0):,.0f} value")

    # ─────────────────────────────────────────────────────────────
    # Step 5.5: Haiku Pre-Screen (cheap filter before expensive Sonnet)
    # ─────────────────────────────────────────────────────────────
    haiku_candidates = min(30, len(valuations))
    logger.info(f"\n[5.5/9] HAIKU PRE-SCREEN ON TOP {haiku_candidates} UNDERVALUED STOCKS...")
    logger.info(f"   💡 Cost: ~${haiku_candidates * 0.002:.2f} (Haiku, 25x cheaper than Sonnet)")

    analyzer = CompanyAnalyzer()
    haiku_results = []

    for val in valuations[:haiku_candidates]:
        try:
            filing_text = fetch_company_summary(val.symbol)
            result = analyzer.quick_screen(val.symbol, filing_text)
            result["valuation"] = val
            haiku_results.append(result)
            logger.info(
                f"  {val.symbol}: moat={result['moat_hint']}, quality={result['quality_hint']} - {result['reason'][:60]}"
            )
        except Exception as e:
            logger.warning(f"  {val.symbol}: Haiku screen failed: {e}")
            # Fail open — include it
            haiku_results.append(
                {"symbol": val.symbol, "worth_analysis": True, "moat_hint": 3, "quality_hint": 3, "valuation": val}
            )

    # Sort by combined moat + quality score, take top max_analyses for Sonnet
    haiku_results.sort(key=lambda r: r["moat_hint"] + r["quality_hint"], reverse=True)
    top_for_analysis = [r["valuation"] for r in haiku_results if r["worth_analysis"]][:max_analyses]

    logger.info(f"   ✓ Haiku selected {len(top_for_analysis)} stocks for deep analysis (from {haiku_candidates})")

    # ─────────────────────────────────────────────────────────────
    # Step 6: LLM Analysis (COSTS MONEY - uses Claude API)
    # ─────────────────────────────────────────────────────────────
    num_to_analyze = len(top_for_analysis)
    logger.info(f"\n[6/9] RUNNING LLM ANALYSIS ON TOP {num_to_analyze} CANDIDATES...")
    logger.info("   💡 Cached analyses (<30 days old) will be reused to save costs")

    # Check how many are already cached
    from src.analyzer import get_cached_analysis

    pre_cached = sum(1 for v in top_for_analysis if get_cached_analysis(v.symbol, 30))
    if pre_cached > 0:
        logger.info(f"   ✓ Found {pre_cached} cached analyses (will save ~${pre_cached * 0.05:.2f})")

    briefings = []
    analyzed_symbols = []

    for val in top_for_analysis:
        try:
            logger.info(f"Analyzing {val.symbol}...")

            filing_text = fetch_company_summary(val.symbol)

            # analyze_company will use cache if available (default)
            analysis = analyzer.analyze_company(
                symbol=val.symbol,
                company_name=val.symbol,
                filing_text=filing_text,
                use_cache=use_cache,  # Pass through cache setting
            )

            # Determine recommendation
            recommendation = determine_recommendation(val, analysis, min_margin_of_safety)

            # Calculate position sizing
            position_size = calculate_position_size(
                portfolio_value=portfolio_value,
                conviction=analysis.conviction_level,
                current_positions=current_positions,
            )

            briefing = StockBriefing(
                symbol=val.symbol,
                company_name=analysis.company_name or val.symbol,
                current_price=val.current_price,
                market_cap=0,
                pe_ratio=None,
                debt_equity=None,
                roe=None,
                revenue_growth=None,
                valuation=val,
                analysis=analysis,
                recommendation=recommendation,
                position_size=position_size,
            )

            briefings.append(briefing)
            analyzed_symbols.append(val.symbol)

        except Exception as e:
            logger.error(f"Error analyzing {val.symbol}: {e}")
            continue

    logger.info(f"   ✓ Completed {len(briefings)} analyses")

    # ─────────────────────────────────────────────────────────────
    # Step 6.5: Execute paper trades for BUY recommendations
    # ─────────────────────────────────────────────────────────────
    from src.paper_trader import PaperTrader, set_trade_log_dir

    set_trade_log_dir(data_dir)
    trader = PaperTrader()
    if trader.is_enabled():
        logger.info("\n[6.5/9] EXECUTING PAPER TRADES...")
        for briefing in briefings:
            if briefing.recommendation == "BUY":
                amount = (
                    briefing.position_size.get("recommended_amount", 0)
                    if isinstance(briefing.position_size, dict)
                    else 0
                )
                if amount > 0:
                    trader.buy(briefing.symbol, amount)
    else:
        logger.info("\n[6.5/9] PAPER TRADING SKIPPED (Alpaca not configured)")

    # ─────────────────────────────────────────────────────────────
    # Step 7: Generate Briefing
    # ─────────────────────────────────────────────────────────────
    logger.info("\n[7/9] GENERATING BRIEFING DOCUMENT...")

    # Radar = screened but not analyzed
    radar_stocks = [s for s in all_screened_symbols if s not in analyzed_symbols][:30]

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
    )

    # Read the generated HTML for email delivery
    html_content = None
    if hasattr(generator, "html_path") and generator.html_path.exists():
        html_content = generator.html_path.read_text()
        logger.info(f"HTML briefing: {generator.html_path}")

    # ─────────────────────────────────────────────────────────────
    # Step 8: Send Notifications
    # ─────────────────────────────────────────────────────────────
    if send_notifications:
        logger.info("\n[8/9] SENDING NOTIFICATIONS...")

        notifier = NotificationManager()
        results = notifier.send_briefing(briefing_text, html_content=html_content)

        for channel, success in results.items():
            status = "✓" if success else "✗"
            logger.info(f"  {status} {channel}")
    else:
        logger.info("\n[8/9] NOTIFICATIONS SKIPPED")

    # ─────────────────────────────────────────────────────────────
    # Summary
    # ─────────────────────────────────────────────────────────────
    buy_count = sum(1 for b in briefings if b.recommendation == "BUY")
    watch_count = sum(1 for b in briefings if b.recommendation == "WATCHLIST")

    logger.info("\n" + "═" * 60)
    logger.info("BRIEFING COMPLETE")
    logger.info("═" * 60)
    logger.info(f"Market Temperature: {market_temp.get('temperature')}")
    logger.info(f"Stocks Analyzed:    {len(briefings)}")
    logger.info(f"Buy Candidates:     {buy_count}")
    logger.info(f"Watchlist:          {watch_count}")
    logger.info(f"Bubble Warnings:    {len(bubble_warnings)}")
    logger.info(f"Radar:              {len(radar_stocks)}")
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
        max_analyses=int(os.getenv("MAX_DEEP_ANALYSES", 10)),
        min_margin_of_safety=float(os.getenv("MIN_MARGIN_OF_SAFETY", 0.20)),
        send_notifications=True,
    )
