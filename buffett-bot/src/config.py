"""
Centralized configuration for BuffettBot.

All behavioral env vars are read once at import time.
API keys/credentials stay in their respective modules.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Load local configuration before any dataclass defaults are evaluated.  Docker
# injects env_file values before Python starts, while direct ``python -m``
# invocations rely on this call.  load_dotenv never overwrites an already-set
# process variable, so deployment-provided values remain authoritative.
load_dotenv(Path.cwd() / ".env")


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

    # Persistence
    database_path: Path = Path(os.getenv("DATABASE_PATH", "./data/buffett_bot_v2.db"))

    # Tier thresholds (stored as decimals)
    margin_of_safety_pct: float = float(os.getenv("MARGIN_OF_SAFETY_PCT", "25")) / 100
    tier1_proximity_alert_pct: float = float(os.getenv("TIER1_PROXIMITY_ALERT_PCT", "10")) / 100

    # Position sizing
    max_position_pct: float = float(os.getenv("MAX_POSITION_PCT", "0.15"))

    # Regime-driven deployment targets (Phase C). Target fraction of equity
    # invested for each bubble_detector.classify_market_regime() regime —
    # formalizes its existing deployment_guidance strings into numbers so
    # weekly_auto_trade deploys toward a target instead of sitting on cash
    # whenever nothing clears the old 25% margin-of-safety hard gate.
    deploy_target_euphoria: float = float(os.getenv("DEPLOY_TARGET_EUPHORIA", "65")) / 100
    deploy_target_overvalued: float = float(os.getenv("DEPLOY_TARGET_OVERVALUED", "80")) / 100
    deploy_target_fair_value: float = float(os.getenv("DEPLOY_TARGET_FAIR_VALUE", "90")) / 100
    deploy_target_correction: float = float(os.getenv("DEPLOY_TARGET_CORRECTION", "97")) / 100
    deploy_target_crisis: float = float(os.getenv("DEPLOY_TARGET_CRISIS", "100")) / 100

    # Quality ceiling (Phase C): margin_of_safety demotes from a pass/fail gate
    # to a ranking/sizing tilt, but a name trading more than this far above its
    # fair value is still skipped rather than bought at any price.
    quality_ceiling_pct: float = float(os.getenv("QUALITY_CEILING_PCT", "12.5")) / 100

    # Minimum dollar size for a deployment buy/consideration — below this a
    # gap or remaining allocation is treated as "close enough", avoiding dust
    # trades and infinite tiny-order loops.
    min_trade_usd: float = float(os.getenv("MIN_TRADE_USD", "250"))

    # Quarantine of non-tradable holdings. A delisted position keeps showing up
    # in Alpaca's /v2/account equity at a frozen mark long after it stops being
    # tradable (their own portfolio-history endpoint drops it), which inflates
    # equity, oversizes every subsequent buy, and burns a position slot. The bot
    # excludes such holdings from equity/sizing/slots/sells and alerts instead —
    # it cannot sell them, so resolution is manual and the alert has to repeat
    # without becoming daily noise.
    quarantine_alerts_enabled: bool = os.getenv("QUARANTINE_ALERTS_ENABLED", "true").lower() != "false"
    quarantine_realert_days: int = int(os.getenv("QUARANTINE_REALERT_DAYS", "7"))
    # A delisting is permanent but a trading halt is not, so tradability is
    # cached with a TTL rather than pinned — an un-halted name recovers on its
    # own instead of needing a container restart.
    asset_status_cache_hours: int = int(os.getenv("ASSET_STATUS_CACHE_HOURS", "6"))

    # Daily health check. A new or changed problem alerts immediately; an
    # unchanged one repeats on this cadence rather than nagging daily.
    health_checks_enabled: bool = os.getenv("HEALTH_CHECKS_ENABLED", "true").lower() != "false"
    health_realert_days: int = int(os.getenv("HEALTH_REALERT_DAYS", "7"))

    # API behavior
    use_batch_api: bool = os.getenv("USE_BATCH_API", "true").lower() == "true"
    use_opus_second_opinion: bool = os.getenv("USE_OPUS_SECOND_OPINION", "false").lower() == "true"
    benchmark_symbol: str = os.getenv("BENCHMARK_SYMBOL", "SPY")
    max_deep_analyses: int = int(os.getenv("MAX_DEEP_ANALYSES", "10"))

    # Claude models. Env-overridable so a bad model can be rolled back from
    # .env without rebuilding the image.
    #   deep  — company analysis (Sonnet tier)
    #   light — news monitoring and quick screens (Haiku tier, ~20x cheaper)
    #   opus  — optional contrarian second opinion, off by default
    model_deep: str = os.getenv("ANTHROPIC_MODEL_DEEP", "claude-sonnet-5")
    model_light: str = os.getenv("ANTHROPIC_MODEL_LIGHT", "claude-haiku-4-5")
    model_opus: str = os.getenv("ANTHROPIC_MODEL_OPUS", "claude-opus-5")

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
    # C-tier verdicts expire far sooner than the rest. A C rating is the one
    # that ejects a stock from consideration entirely, and the re-analysis
    # queue skips anything holding a non-expired analysis — so at the normal
    # 180 days a single bad verdict was an effectively permanent exile. Of 74
    # stocks that reached C, none ever recovered.
    c_tier_analysis_days: int = int(os.getenv("C_TIER_ANALYSIS_DAYS", "30"))

    # Automation kill switches. Paid work and order submission are opt-in: a
    # missing or incomplete .env must leave the unattended scheduler harmless.
    auto_trade_enabled: bool = os.getenv("AUTO_TRADE_ENABLED", "false").lower() == "true"
    briefing_paper_trades_enabled: bool = os.getenv("BRIEFING_PAPER_TRADES_ENABLED", "false").lower() == "true"
    monthly_briefing_enabled: bool = os.getenv("MONTHLY_BRIEFING_ENABLED", "false").lower() == "true"
    wednesday_haiku_enabled: bool = os.getenv("WEDNESDAY_HAIKU_ENABLED", "false").lower() == "true"
    friday_sonnet_enabled: bool = os.getenv("FRIDAY_SONNET_ENABLED", "false").lower() == "true"
    daily_news_analysis_enabled: bool = os.getenv("DAILY_NEWS_ANALYSIS_ENABLED", "false").lower() == "true"


config = Config()
