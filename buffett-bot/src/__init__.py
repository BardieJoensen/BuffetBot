"""
Buffett Bot - Value Investing Research Assistant (v2.1 — Phase A)

Modules:
- universe: Dynamic stock universe (Finviz/Wikipedia/fallback)
- screener: Quality-first stock screening with sector-aware scoring
- valuation: Aggregate fair value estimates
- analyzer: LLM qualitative analysis (Haiku/Sonnet/Opus)
- tier_engine: S/A/B/C tiered watchlist assignment and staged entry
- database: SQLite persistence (WAL mode, 10 tables, budget caps)
- quality_scorer: Percentile-based composite quality scoring
- universe_builder: Three-pool universe (conviction + S&P 500 + Finviz)
- briefing: Generate tiered investment reports
- portfolio: ASK portfolio management with concentration controls
- bubble_detector: Market regime classification (Euphoria→Crisis)
- notifications: Email/Telegram/ntfy.sh/Discord alerts
- paper_trader: Alpaca paper trading integration
- benchmark: Benchmark comparison (SPY default)
- backtest: Forward validation and quality-return correlation
"""

__version__ = "2.1.0"
