"""
Centralized configuration for BuffettBot.

All behavioral env vars are read once at import time.
API keys/credentials stay in their respective modules.
"""

import os
from dataclasses import dataclass
from typing import Optional


def _optional_float(name: str) -> Optional[float]:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


@dataclass(frozen=True)
class Config:
    """Application configuration loaded from environment variables."""

    # Portfolio
    max_positions: int = int(os.getenv("MAX_POSITIONS", "8"))
    portfolio_value: float = float(os.getenv("PORTFOLIO_VALUE", "50000"))
    ask_contribution_limit_dkk: int = int(os.getenv("ASK_CONTRIBUTION_LIMIT", "135900"))

    # Tier thresholds (stored as decimals)
    margin_of_safety_pct: float = float(os.getenv("MARGIN_OF_SAFETY_PCT", "25")) / 100
    tier1_proximity_alert_pct: float = float(os.getenv("TIER1_PROXIMITY_ALERT_PCT", "10")) / 100

    # Position sizing
    max_position_pct: float = float(os.getenv("MAX_POSITION_PCT", "0.15"))

    # API behavior
    use_batch_api: bool = os.getenv("USE_BATCH_API", "true").lower() == "true"
    use_opus_second_opinion: bool = os.getenv("USE_OPUS_SECOND_OPINION", "false").lower() == "true"
    benchmark_symbol: str = os.getenv("BENCHMARK_SYMBOL", "SPY")
    max_deep_analyses: int = int(os.getenv("MAX_DEEP_ANALYSES", "10"))

    # SEC EDGAR 10-K ingestion (Phase 2). SEC fair-access requires a descriptive
    # User-Agent with a contact email or requests are blocked. Empty → EDGAR is
    # skipped and deep analyses fall back to the yfinance-derived summary.
    edgar_user_agent: str = os.getenv("EDGAR_USER_AGENT", "")

    # FX bridge (Phase 5). Manual USD→DKK rate (DKK per 1 USD) for offline use
    # and tests; unset → live daily fetch via yfinance.
    usddkk_override: Optional[float] = _optional_float("USDDKK_OVERRIDE")

    # Coverage campaign
    haiku_batch_size: int = int(os.getenv("HAIKU_BATCH_SIZE", "100"))
    haiku_min_score: int = int(os.getenv("HAIKU_MIN_SCORE", "5"))
    analysis_max_age_days: int = int(os.getenv("ANALYSIS_MAX_AGE_DAYS", "180"))

    # Automation kill switches
    auto_trade_enabled: bool = os.getenv("AUTO_TRADE_ENABLED", "true").lower() != "false"
    monthly_briefing_enabled: bool = os.getenv("MONTHLY_BRIEFING_ENABLED", "true").lower() != "false"


config = Config()
