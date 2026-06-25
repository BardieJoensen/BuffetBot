"""
Insider Buying Signal — Phase 3

A per-stock insider-conviction signal derived from SEC Form 4 data via Finnhub's
`stock/insider-transactions` endpoint. bubble_detector.py already pulls this
feed, but only as a market-froth signal (insider *selling*); the buying side
never reached the quality score. Cluster insider *buying* (multiple insiders
making open-market purchases) is one of the few signals with real academic
support and is philosophically on-point — skin in the game.

Signal: net distinct insider **buyers** minus distinct **sellers** (open-market
transactions, SEC code P/S) over a trailing window. Counting distinct people
rather than transactions or dollars makes it a cluster proxy that a single large
outlier trade can't dominate.

Finnhub's free tier is rate-limited, so results are cached on disk with a short
TTL and callers are expected to fetch only for stocks that already reach scoring.
No key (or no coverage) → None, so the stock is simply excluded from the insider
percentile ranking rather than penalized.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)

FINNHUB_URL = "https://finnhub.io/api/v1/stock/insider-transactions"
_cache_dir = Path("data/insider")

CACHE_TTL_DAYS = 7
DEFAULT_WINDOW_MONTHS = 6
DEFAULT_FETCH_LIMIT = 50  # cap live fetches per run (Finnhub free tier)
_REQUEST_DELAY_S = 0.2


def set_insider_cache_dir(path: Path) -> None:
    """Override the insider cache directory (used in tests)."""
    global _cache_dir
    _cache_dir = Path(path)


def _finnhub_key(explicit: Optional[str] = None) -> Optional[str]:
    return explicit or os.getenv("FINNHUB_API_KEY")


def _cache_path(symbol: str) -> Path:
    return _cache_dir / f"{symbol.upper()}.json"


def _load_cached(symbol: str) -> Optional[list]:
    path = _cache_path(symbol)
    if not path.exists():
        return None
    try:
        blob = json.loads(path.read_text())
        fetched = datetime.fromisoformat(blob.get("fetched_at", "2000-01-01"))
        if (datetime.now() - fetched).days < CACHE_TTL_DAYS:
            return blob.get("data", [])
    except Exception:
        pass
    return None


def _save_cache(symbol: str, data: list) -> None:
    try:
        _cache_dir.mkdir(parents=True, exist_ok=True)
        _cache_path(symbol).write_text(
            json.dumps({"symbol": symbol.upper(), "fetched_at": datetime.now().isoformat(), "data": data})
        )
    except Exception as e:
        logger.warning(f"Could not cache insider data for {symbol}: {e}")


def _fetch_transactions(symbol: str, key: str) -> Optional[list]:
    """Fetch raw insider transactions from Finnhub, or None on failure."""
    try:
        time.sleep(_REQUEST_DELAY_S)
        resp = requests.get(FINNHUB_URL, params={"symbol": symbol, "token": key}, timeout=10)
        if resp.status_code == 200:
            return resp.json().get("data", []) or []
        logger.debug(f"Finnhub insider {symbol} returned {resp.status_code}")
    except Exception as e:
        logger.debug(f"Finnhub insider error for {symbol}: {e}")
    return None


def _net_buyers(transactions: list, window_months: int) -> float:
    """
    Net distinct insider buyers minus distinct sellers over the trailing window.

    Direction is taken from the SEC transaction code: 'P' = open-market purchase,
    'S' = open-market sale (the meaningful, conviction-bearing trades). Awards,
    option exercises, gifts, and tax withholdings (codes A/M/G/F …) are ignored.
    """
    cutoff = (datetime.now() - timedelta(days=int(window_months * 30.44))).date().isoformat()
    buyers: set[str] = set()
    sellers: set[str] = set()
    for t in transactions:
        when = t.get("transactionDate") or t.get("filingDate") or ""
        if when and when < cutoff:
            continue
        name = (t.get("name") or "").strip().lower()
        if not name:
            continue
        code = str(t.get("transactionCode") or t.get("transactionType") or "").upper()
        if code == "P":
            buyers.add(name)
        elif code == "S":
            sellers.add(name)
    return float(len(buyers) - len(sellers))


def get_insider_buying_signal(
    symbol: str,
    *,
    window_months: int = DEFAULT_WINDOW_MONTHS,
    finnhub_key: Optional[str] = None,
    use_cache: bool = True,
) -> Optional[float]:
    """
    Net distinct insider buyers minus sellers over the trailing window. Positive
    means a net buying cluster. Returns None when there is no coverage (no API
    key, no transactions, or a fetch error) so the caller excludes the stock from
    the insider percentile ranking.
    """
    key = _finnhub_key(finnhub_key)
    if not key:
        return None

    data = _load_cached(symbol) if use_cache else None
    if data is None:
        data = _fetch_transactions(symbol, key)
        if data is None:
            return None
        _save_cache(symbol, data)

    if not data:
        return None
    return _net_buyers(data, window_months)


def fetch_insider_signals(
    symbols: list[str],
    *,
    window_months: int = DEFAULT_WINDOW_MONTHS,
    limit: int = DEFAULT_FETCH_LIMIT,
    finnhub_key: Optional[str] = None,
) -> dict[str, float]:
    """
    Fetch insider buying signals for up to `limit` symbols (rate-limit guard).
    Returns {symbol: signal} only for symbols with coverage; callers should treat
    missing symbols as None (no data). No-op returning {} when no key is set.
    """
    key = _finnhub_key(finnhub_key)
    if not key:
        return {}
    out: dict[str, float] = {}
    for symbol in symbols[:limit]:
        sig = get_insider_buying_signal(symbol, window_months=window_months, finnhub_key=key)
        if sig is not None:
            out[symbol] = sig
    return out
