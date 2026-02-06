"""
Stock Screener Module

Filters stocks based on Buffett-style value criteria.
Returns candidates that pass quantitative filters for further analysis.

NOTE: Uses yfinance (completely free, no API key) for all stock data.
"""

import json
import time
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timedelta
from pathlib import Path
import logging
import yfinance as yf

from src.universe import get_stock_universe, set_cache_dir as set_universe_cache_dir

logger = logging.getLogger(__name__)

# Cache directory for stock data
CACHE_DIR = Path(__file__).parent.parent / "data" / "cache"


@dataclass
class ScreeningCriteria:
    """Value investing screening criteria"""
    min_market_cap: float = 300_000_000      # $300M minimum
    max_market_cap: float = 10_000_000_000   # $10B maximum
    max_pe_ratio: float = 20.0               # Not overvalued
    max_debt_equity: float = 0.5             # Conservative debt
    min_roe: float = 0.12                    # 12% return on equity
    min_revenue_growth: float = 0.05         # 5% growth
    min_current_ratio: float = 1.5           # Can pay short-term debts


@dataclass
class ScreenedStock:
    """A stock that passed screening criteria"""
    symbol: str
    name: str
    market_cap: float
    pe_ratio: Optional[float]
    debt_equity: Optional[float]
    roe: Optional[float]
    revenue_growth: Optional[float]
    sector: str
    industry: str
    screened_at: datetime
    price: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "name": self.name,
            "market_cap": self.market_cap,
            "pe_ratio": self.pe_ratio,
            "debt_equity": self.debt_equity,
            "roe": self.roe,
            "revenue_growth": self.revenue_growth,
            "sector": self.sector,
            "industry": self.industry,
            "screened_at": self.screened_at.isoformat(),
            "price": self.price
        }


class StockScreener:
    """
    Screens stocks using yfinance (completely free, no API key).

    Strategy:
    1. Get dynamic universe from Finviz/Wikipedia/fallback (see universe.py)
    2. Fetch detailed data via yfinance
    3. Filter locally by market cap, P/E, fundamentals
    """

    BATCH_SIZE = 20  # Process in smaller batches for reliability
    CACHE_HOURS = 24  # Cache stock data for 24 hours

    def __init__(self):
        self.cache_dir = CACHE_DIR
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            self.cache_dir = Path("/tmp/buffett-bot-cache")
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            logger.warning(f"Using fallback cache dir: {self.cache_dir}")
        # Share the resolved cache dir with the universe module
        set_universe_cache_dir(self.cache_dir)

    def _get_cached_data(self, symbol: str) -> Optional[dict]:
        """Load cached stock data if fresh"""
        cache_file = self.cache_dir / f"{symbol}.json"

        if not cache_file.exists():
            return None

        mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
        if datetime.now() - mtime > timedelta(hours=self.CACHE_HOURS):
            return None

        try:
            with open(cache_file) as f:
                return json.load(f)
        except Exception:
            return None

    def _save_cached_data(self, symbol: str, data: dict):
        """Save stock data to cache"""
        cache_file = self.cache_dir / f"{symbol}.json"
        try:
            with open(cache_file, "w") as f:
                json.dump(data, f)
        except Exception as e:
            logger.warning(f"Failed to cache {symbol}: {e}")

    def _fetch_stock_data(self, symbol: str) -> Optional[dict]:
        """
        Fetch stock data from yfinance.
        Returns dict with price, market cap, P/E, sector, etc.
        """
        # Check cache first
        cached = self._get_cached_data(symbol)
        if cached:
            return cached

        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            if not info or info.get("regularMarketPrice") is None:
                return None

            data = {
                "symbol": symbol,
                "name": info.get("longName") or info.get("shortName") or symbol,
                "quote_type": info.get("quoteType", "EQUITY"),
                "price": info.get("regularMarketPrice") or info.get("currentPrice"),
                "market_cap": info.get("marketCap", 0),
                "pe_ratio": info.get("trailingPE") or info.get("forwardPE"),
                "debt_equity": info.get("debtToEquity"),
                "roe": info.get("returnOnEquity"),
                "revenue_growth": info.get("revenueGrowth"),
                "current_ratio": info.get("currentRatio"),
                "sector": info.get("sector", "Unknown"),
                "industry": info.get("industry", "Unknown"),
                "beta": info.get("beta"),
                "dividend_yield": info.get("dividendYield"),
                "profit_margin": info.get("profitMargins"),
                "52_week_high": info.get("fiftyTwoWeekHigh"),
                "52_week_low": info.get("fiftyTwoWeekLow"),
            }

            # Cache the data
            self._save_cached_data(symbol, data)

            return data

        except Exception as e:
            logger.debug(f"Error fetching {symbol}: {e}")
            return None

    def screen(self, criteria: Optional[ScreeningCriteria] = None) -> list[ScreenedStock]:
        """
        Run stock screen with given criteria using yfinance.

        Returns list of stocks passing all filters.
        """
        criteria = criteria or ScreeningCriteria()

        universe = get_stock_universe()

        logger.info(f"Running screen with criteria: market_cap={criteria.min_market_cap:,.0f}-{criteria.max_market_cap:,.0f}")
        logger.info(f"Stock universe: {len(universe)} stocks")
        logger.info("Fetching data from yfinance (free, no API key)...")

        candidates = []
        processed = 0
        errors = 0

        for symbol in universe:
            processed += 1

            if processed % 20 == 0:
                logger.info(f"Progress: {processed}/{len(universe)} stocks processed...")

            data = self._fetch_stock_data(symbol)

            # Small delay to be respectful to Yahoo Finance (no official rate limit but be nice)
            time.sleep(0.1)

            if data is None:
                errors += 1
                continue

            # Skip non-equity securities (closed-end funds, ETFs, etc.)
            quote_type = data.get("quote_type", "EQUITY")
            if quote_type != "EQUITY":
                continue

            # Skip closed-end funds and asset management vehicles
            industry = (data.get("industry") or "").lower()
            if any(term in industry for term in [
                "closed-end fund", "asset management", "shell companies",
                "exchange traded fund",
            ]):
                continue

            # Apply filters
            market_cap = data.get("market_cap", 0) or 0
            if market_cap < criteria.min_market_cap or market_cap > criteria.max_market_cap:
                continue

            price = data.get("price", 0)
            if not price or price < 5:
                continue

            pe = data.get("pe_ratio")
            if pe is not None and (pe <= 0 or pe > criteria.max_pe_ratio):
                continue

            # Optional: filter by debt/equity if available
            de = data.get("debt_equity")
            if de is not None and de > criteria.max_debt_equity * 100:  # yfinance returns as percentage
                continue

            candidates.append(ScreenedStock(
                symbol=symbol,
                name=data.get("name", symbol),
                market_cap=market_cap,
                pe_ratio=pe,
                debt_equity=de / 100 if de else None,  # Convert to ratio
                roe=data.get("roe"),
                revenue_growth=data.get("revenue_growth"),
                sector=data.get("sector", "Unknown"),
                industry=data.get("industry", "Unknown"),
                screened_at=datetime.now(),
                price=price
            ))

        logger.info(f"Processed {processed} stocks, {errors} errors")
        logger.info(f"After filtering: {len(candidates)} candidates")

        return candidates

    def get_detailed_metrics(self, symbol: str) -> dict:
        """
        Fetch detailed metrics for a single stock.
        Uses yfinance which provides comprehensive data.
        """
        data = self._fetch_stock_data(symbol)
        return data or {}

    def apply_detailed_filters(
        self,
        candidates: list[ScreenedStock],
        criteria: ScreeningCriteria
    ) -> list[ScreenedStock]:
        """
        Apply additional filters using detailed metrics.

        With yfinance, most data is already fetched in screen(),
        but this can apply stricter filters.
        """
        filtered = []

        for stock in candidates:
            # Check ROE
            if stock.roe is not None and stock.roe < criteria.min_roe:
                logger.debug(f"{stock.symbol}: ROE {stock.roe:.2%} below threshold")
                continue

            # Check Debt/Equity
            if stock.debt_equity is not None and stock.debt_equity > criteria.max_debt_equity:
                logger.debug(f"{stock.symbol}: D/E {stock.debt_equity:.2f} above threshold")
                continue

            filtered.append(stock)

        logger.info(f"After detailed filtering: {len(filtered)} stocks")
        return filtered


def run_screen(apply_detailed: bool = False) -> list[ScreenedStock]:
    """
    Convenience function to run a full screen.

    Args:
        apply_detailed: If True, apply additional fundamental filters
    """
    screener = StockScreener()
    criteria = ScreeningCriteria()

    candidates = screener.screen(criteria)

    if apply_detailed and candidates:
        candidates = screener.apply_detailed_filters(candidates, criteria)

    return candidates


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    stocks = run_screen(apply_detailed=True)
    print(f"\nFound {len(stocks)} candidates:\n")

    for stock in stocks[:10]:
        print(f"  {stock.symbol}: {stock.name}")
        print(f"    Market Cap: ${stock.market_cap:,.0f}")
        print(f"    Price: ${stock.price:.2f}")
        print(f"    P/E: {stock.pe_ratio}")
        print(f"    Sector: {stock.sector}")
        print()
