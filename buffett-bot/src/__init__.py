"""
Buffett Bot - Value Investing Research Assistant (v2.0)

Modules:
- universe: Dynamic stock universe (Finviz/Wikipedia/fallback)
- screener: Quality-first stock screening with sector-aware scoring
- valuation: Aggregate fair value estimates
- analyzer: LLM qualitative analysis (Haiku/Sonnet/Opus)
- tier_engine: Tiered watchlist assignment and staged entry
- briefing: Generate tiered investment reports
- portfolio: ASK portfolio management with concentration controls
- bubble_detector: Market regime classification (Euphoria→Crisis)
- notifications: Email/Telegram/ntfy.sh/Discord alerts
- paper_trader: Alpaca paper trading integration
- benchmark: Benchmark comparison (SPY default)
- backtest: Forward validation and quality-return correlation
"""

__version__ = "2.0.0"
