"""
USD → DKK FX Bridge — Phase 5

Provides the USD/DKK rate so portfolio values, returns, and dividend estimates
can be reported in DKK — the currency that actually lands in Bardie's ASK
account. ASK *constraints* (contribution limit, 17% dividend tax) were already
DKK-aware, but position values and returns were still USD.

Rate source: yfinance ticker "DKK=X" (DKK per 1 USD), cached daily. A manual
USDDKK_OVERRIDE (config) wins when set — useful offline and in tests. DKK is
pegged to EUR, so the rate is stable; a stale-cache or override fallback is fine.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.config import config

logger = logging.getLogger(__name__)

FX_SYMBOL = "DKK=X"  # USD/DKK: DKK per 1 USD
_cache_dir = Path("data/fx")
CACHE_TTL_HOURS = 24


def set_fx_cache_dir(path: Path) -> None:
    """Override the FX cache directory (used in tests)."""
    global _cache_dir
    _cache_dir = Path(path)


def _cache_path() -> Path:
    return _cache_dir / "usddkk.json"


def _read_cache(*, allow_stale: bool = False) -> Optional[float]:
    path = _cache_path()
    if not path.exists():
        return None
    try:
        blob = json.loads(path.read_text())
        rate = float(blob["rate"])
        fetched = datetime.fromisoformat(blob.get("fetched_at", "2000-01-01"))
        age_h = (datetime.now() - fetched).total_seconds() / 3600
        if allow_stale or age_h < CACHE_TTL_HOURS:
            return rate
    except Exception:
        pass
    return None


def _write_cache(rate: float) -> None:
    try:
        _cache_dir.mkdir(parents=True, exist_ok=True)
        _cache_path().write_text(json.dumps({"rate": rate, "fetched_at": datetime.now().isoformat()}))
    except Exception as e:
        logger.warning(f"Could not cache USD/DKK rate: {e}")


def _fetch_live_rate() -> Optional[float]:
    """Fetch the current USD/DKK close from yfinance, or None on failure."""
    try:
        import yfinance as yf

        hist = yf.Ticker(FX_SYMBOL).history(period="5d")
        if len(hist) >= 1:
            rate = float(hist["Close"].iloc[-1])
            if rate > 0:
                return rate
    except Exception as e:
        logger.warning(f"Failed to fetch USD/DKK rate: {e}")
    return None


def get_usd_dkk_rate() -> Optional[float]:
    """
    DKK per 1 USD. Resolution order: manual override → fresh daily cache →
    live yfinance fetch → stale cache. Returns None only if every source fails
    (then callers should omit DKK rather than fabricate it).
    """
    if config.usddkk_override is not None:
        return config.usddkk_override

    cached = _read_cache()
    if cached is not None:
        return cached

    rate = _fetch_live_rate()
    if rate is not None:
        _write_cache(rate)
        return rate

    # Last resort: a stale cache beats no number at all (DKK is EUR-pegged).
    return _read_cache(allow_stale=True)


def get_cached_usd_dkk_rate() -> Optional[float]:
    """
    Override or last-known cached rate (fresh or stale), but NEVER a live fetch.
    For offline contexts like the DB briefing that must not make network calls;
    the cache is kept warm by the online portfolio/maintenance paths.
    """
    if config.usddkk_override is not None:
        return config.usddkk_override
    return _read_cache(allow_stale=True)


def usd_to_dkk(amount_usd: Optional[float], rate: Optional[float] = None) -> Optional[float]:
    """Convert a USD amount to DKK. Returns None if the amount or rate is missing."""
    if amount_usd is None:
        return None
    if rate is None:
        rate = get_usd_dkk_rate()
    if rate is None:
        return None
    return amount_usd * rate
