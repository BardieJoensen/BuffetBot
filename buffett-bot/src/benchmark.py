"""
Benchmark Comparison Module

Fetches benchmark index data (default SPY) for comparing picks against
the overall market. Uses yfinance for price/metric data with 24h caching.
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Cache directory (same pattern as screener)
_cache_dir = Path("data/benchmark")


def set_benchmark_cache_dir(path: Path):
    """Override the benchmark cache directory."""
    global _cache_dir
    _cache_dir = path


def fetch_benchmark_data(symbol: str = "SPY") -> dict:
    """
    Fetch benchmark index data using yfinance.

    Args:
        symbol: Benchmark ticker (default SPY for S&P 500 ETF)

    Returns:
        dict with symbol, name, current_price, pe_ratio, ytd_return,
        one_year_return, dividend_yield, 52w_high, 52w_low
    """
    import yfinance as yf

    # Check 24h cache
    cached = _get_cached_benchmark(symbol)
    if cached:
        return cached

    logger.info(f"Fetching benchmark data for {symbol}...")

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info

        current_price = info.get("regularMarketPrice") or info.get("currentPrice") or 0
        pe_ratio = info.get("trailingPE")
        # yfinance may return dividendYield as a whole number (e.g. 1.05 = 1.05%)
        raw_yield = info.get("dividendYield")
        dividend_yield = raw_yield / 100 if raw_yield is not None and raw_yield > 1 else raw_yield
        high_52w = info.get("fiftyTwoWeekHigh")
        low_52w = info.get("fiftyTwoWeekLow")
        name = info.get("longName") or info.get("shortName") or symbol

        # Calculate YTD and 1Y returns from price history
        ytd_return = _calculate_ytd_return(ticker)
        one_year_return = _calculate_1y_return(ticker)

        result = {
            "symbol": symbol,
            "name": name,
            "current_price": current_price,
            "pe_ratio": pe_ratio,
            "ytd_return": ytd_return,
            "one_year_return": one_year_return,
            "dividend_yield": dividend_yield,
            "52w_high": high_52w,
            "52w_low": low_52w,
            "fetched_at": datetime.now().isoformat(),
        }

        # Cache result
        _save_benchmark_cache(symbol, result)

        return result

    except Exception as e:
        logger.warning(f"Error fetching benchmark data for {symbol}: {e}")
        return {
            "symbol": symbol,
            "name": symbol,
            "current_price": 0,
            "pe_ratio": None,
            "ytd_return": None,
            "one_year_return": None,
            "dividend_yield": None,
            "52w_high": None,
            "52w_low": None,
            "fetched_at": datetime.now().isoformat(),
        }


# Calendar days of padding around the requested window when fetching bars.
# Must exceed the longest run of consecutive non-trading days (a holiday
# adjoining a weekend), or a boundary date can fail to resolve to any bar.
_BRACKET_PAD_DAYS = 10


def fetch_benchmark_return(start_date: str, end_date: Optional[str] = None, symbol: str = "SPY") -> Optional[float]:
    """
    Total price return of the benchmark over the hold window [start, end].

    Used by the decision journal to compute per-trade alpha. Dates are ISO
    strings (YYYY-MM-DD); end_date defaults to today. Returns the fractional
    return (e.g. 0.08 for +8%), or None if the window cannot be resolved.

    Resolves each boundary to the last close **on or before** that date rather
    than slicing yfinance's window directly. Two reasons, both of which
    corrupted the journal in practice:

      - yfinance treats `end` as exclusive, so passing the exit date measured
        a window one bar short of the real hold. A 2026-07-03..07-10 hold was
        scored over 07-06..07-09, understating the benchmark by ~1.4pp and
        overstating that trade's alpha by the same amount.
      - a boundary can land on a weekend or market holiday, where no bar
        exists at all. A position exited on 2026-07-03 (July 4th observed)
        produced fewer than two bars, so the old `len(hist) >= 2` guard
        returned None and the trade was journalled with no alpha whatsoever.

    Both failed silently, which is why the journal looked populated while
    being partly empty and partly wrong.
    """
    import yfinance as yf

    if not start_date:
        return None
    start = start_date[:10]
    end = (end_date or datetime.now().strftime("%Y-%m-%d"))[:10]
    if end < start:
        logger.warning("Benchmark window ends before it starts [%s..%s] — skipping", start, end)
        return None

    try:
        # Pad both ends so each boundary is guaranteed a bar to resolve
        # against even across a long holiday weekend.
        fetch_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=_BRACKET_PAD_DAYS)).strftime("%Y-%m-%d")
        fetch_end = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=_BRACKET_PAD_DAYS)).strftime("%Y-%m-%d")

        hist = yf.Ticker(symbol).history(start=fetch_start, end=fetch_end)
        if hist.empty:
            logger.warning("No %s history for benchmark window [%s..%s]", symbol, start, end)
            return None

        # Drop NaN closes: yfinance emits a placeholder bar for the current
        # session before it settles, and letting one through propagates NaN
        # into benchmark_return and silently into alpha.
        bars = [
            (d.strftime("%Y-%m-%d"), float(c))
            for d, c in zip(hist.index, hist["Close"])
            if c == c  # NaN is the only value that fails this
        ]
        if not bars:
            logger.warning("No usable %s closes for benchmark window [%s..%s]", symbol, start, end)
            return None

        def close_on_or_before(target: str) -> Optional[float]:
            found = None
            for d, c in bars:
                if d <= target:
                    found = c
                else:
                    break
            return found

        first_close = close_on_or_before(start)
        last_close = close_on_or_before(end)

        if first_close is None or last_close is None:
            logger.warning("Could not bracket benchmark window [%s..%s] for %s", start, end, symbol)
            return None
        if first_close <= 0:
            return None
        # Both boundaries landing on the same bar means the window contained no
        # completed trading day (a same-day round trip, or entry and exit
        # straddling only a holiday). The benchmark genuinely did not move.
        return (last_close - first_close) / first_close
    except Exception as e:
        logger.warning(f"Error calculating benchmark return for {symbol} [{start}..{end}]: {e}")
    return None


def _calculate_ytd_return(ticker) -> Optional[float]:
    """Calculate year-to-date return from price history."""
    try:
        now = datetime.now()
        start_of_year = datetime(now.year, 1, 1)
        hist = ticker.history(start=start_of_year.strftime("%Y-%m-%d"))
        if len(hist) >= 2:
            first_close = hist["Close"].iloc[0]
            last_close = hist["Close"].iloc[-1]
            if first_close > 0:
                return (last_close - first_close) / first_close
    except Exception as e:
        logger.warning(f"Error calculating YTD return: {e}")
    return None


def _calculate_1y_return(ticker) -> Optional[float]:
    """Calculate 1-year return from price history."""
    try:
        hist = ticker.history(period="1y")
        if len(hist) >= 2:
            first_close = hist["Close"].iloc[0]
            last_close = hist["Close"].iloc[-1]
            if first_close > 0:
                return (last_close - first_close) / first_close
    except Exception as e:
        logger.warning(f"Error calculating 1Y return: {e}")
    return None


def _get_cached_benchmark(symbol: str) -> Optional[dict]:
    """Return cached benchmark data if less than 24h old."""
    cache_file = _cache_dir / f"{symbol}_benchmark.json"
    if cache_file.exists():
        try:
            data = json.loads(cache_file.read_text())
            fetched = datetime.fromisoformat(data.get("fetched_at", "2000-01-01"))
            age_hours = (datetime.now() - fetched).total_seconds() / 3600
            if age_hours < 24:
                logger.info(f"Using cached benchmark data for {symbol} ({age_hours:.1f}h old)")
                return data
        except Exception as e:
            logger.warning(f"Error reading benchmark cache: {e}")
    return None


def _save_benchmark_cache(symbol: str, data: dict):
    """Save benchmark data to cache."""
    try:
        _cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = _cache_dir / f"{symbol}_benchmark.json"
        cache_file.write_text(json.dumps(data, indent=2))
    except Exception as e:
        logger.warning(f"Failed to cache benchmark data: {e}")
