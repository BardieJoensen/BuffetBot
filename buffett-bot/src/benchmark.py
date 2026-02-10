"""
Benchmark Comparison Module

Fetches benchmark index data (default SPY) for comparing picks against
the overall market. Uses yfinance for price/metric data with 24h caching.
"""

import json
import logging
from datetime import datetime
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
