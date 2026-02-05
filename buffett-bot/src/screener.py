"""
Stock Screener Module

Filters stocks based on Buffett-style value criteria using FMP API.
Returns candidates that pass quantitative filters for further analysis.

NOTE: This version uses FREE FMP endpoints only. The /stock-screener endpoint
requires a paid plan, so we use /stock/list + batched /quote calls instead.
"""

import os
import requests
import json
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timedelta
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# Cache directory for stock list (reduces API calls)
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
    country: str = "US"                      # US stocks only for now
    exchanges: list = None                   # Major exchanges

    def __post_init__(self):
        if self.exchanges is None:
            self.exchanges = ["NASDAQ", "NYSE"]


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
    Screens stocks using Financial Modeling Prep API (FREE TIER).

    Uses batched requests to stay within 250 calls/day limit.
    Strategy:
    1. Fetch stock list (cached, 1 call/week)
    2. Batch fetch quotes for market cap filtering (batches of 50)
    3. Filter locally by market cap, P/E, sector
    4. Fetch detailed metrics only for candidates
    """

    BASE_URL = "https://financialmodelingprep.com/api/v3"
    BATCH_SIZE = 50  # FMP allows batching multiple symbols
    CACHE_DAYS = 7   # How long to cache stock list

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("FMP_API_KEY")
        if not self.api_key:
            raise ValueError("FMP_API_KEY not found in environment")
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _get_cached_stock_list(self) -> Optional[list[dict]]:
        """Load stock list from cache if fresh enough"""
        cache_file = CACHE_DIR / "stock_list.json"

        if not cache_file.exists():
            return None

        # Check if cache is fresh
        mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
        if datetime.now() - mtime > timedelta(days=self.CACHE_DAYS):
            logger.info("Stock list cache expired")
            return None

        with open(cache_file) as f:
            data = json.load(f)
            logger.info(f"Loaded {len(data)} stocks from cache")
            return data

    def _save_stock_list_cache(self, stocks: list[dict]):
        """Save stock list to cache"""
        cache_file = CACHE_DIR / "stock_list.json"
        with open(cache_file, "w") as f:
            json.dump(stocks, f)
        logger.info(f"Cached {len(stocks)} stocks")

    def _fetch_stock_list(self) -> list[dict]:
        """
        Fetch all tradeable stocks from FMP (FREE endpoint).
        Returns basic info: symbol, name, exchange.
        """
        # Try cache first
        cached = self._get_cached_stock_list()
        if cached:
            return cached

        logger.info("Fetching fresh stock list from FMP...")

        response = requests.get(
            f"{self.BASE_URL}/stock/list",
            params={"apikey": self.api_key}
        )

        if response.status_code == 403:
            raise ValueError(
                "FMP API returned 403 Forbidden. Check your API key. "
                "If this persists, the stock/list endpoint may require authentication."
            )

        response.raise_for_status()
        stocks = response.json()

        # Cache for future use
        self._save_stock_list_cache(stocks)

        return stocks

    def _fetch_quotes_batch(self, symbols: list[str]) -> list[dict]:
        """
        Fetch quote data for multiple symbols in one call (FREE endpoint).
        Returns: price, marketCap, pe, eps, sector, industry, etc.
        """
        if not symbols:
            return []

        # Join symbols for batch request
        symbols_str = ",".join(symbols)

        response = requests.get(
            f"{self.BASE_URL}/quote/{symbols_str}",
            params={"apikey": self.api_key}
        )

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 403:
            logger.error("FMP quote endpoint returned 403 - API key issue")
            return []
        else:
            logger.warning(f"Quote batch failed with status {response.status_code}")
            return []

    def _fetch_profiles_batch(self, symbols: list[str]) -> list[dict]:
        """
        Fetch company profiles for multiple symbols (FREE endpoint).
        Includes: sector, industry, market cap, description.
        """
        if not symbols:
            return []

        symbols_str = ",".join(symbols)

        response = requests.get(
            f"{self.BASE_URL}/profile/{symbols_str}",
            params={"apikey": self.api_key}
        )

        if response.status_code == 200:
            return response.json()
        else:
            logger.warning(f"Profile batch failed with status {response.status_code}")
            return []

    def screen(self, criteria: Optional[ScreeningCriteria] = None) -> list[ScreenedStock]:
        """
        Run stock screen with given criteria using FREE FMP endpoints.

        Uses a multi-stage approach to minimize API calls:
        1. Get full stock list (cached)
        2. Filter by exchange locally
        3. Batch fetch quotes for remaining stocks
        4. Filter by market cap, P/E locally

        Returns list of stocks passing all filters.
        """
        criteria = criteria or ScreeningCriteria()

        logger.info(f"Running screen with criteria: market_cap={criteria.min_market_cap:,.0f}-{criteria.max_market_cap:,.0f}")

        # Stage 1: Get stock list (usually cached)
        all_stocks = self._fetch_stock_list()
        logger.info(f"Total stocks in universe: {len(all_stocks)}")

        # Stage 2: Filter by exchange locally
        us_exchanges = set(criteria.exchanges)
        filtered_symbols = []

        for stock in all_stocks:
            exchange = stock.get("exchangeShortName", "") or stock.get("exchange", "")
            symbol = stock.get("symbol", "")

            # Skip if not on target exchange
            if exchange not in us_exchanges:
                continue

            # Skip penny stock symbols (often have special characters)
            if not symbol or "." in symbol or "-" in symbol or len(symbol) > 5:
                continue

            # Skip ETFs, funds, warrants
            stock_type = stock.get("type", "")
            if stock_type and stock_type.lower() not in ["stock", "cs", ""]:
                continue

            filtered_symbols.append(symbol)

        logger.info(f"After exchange filter: {len(filtered_symbols)} stocks")

        # Stage 3: Batch fetch quotes for market cap filtering
        # This is the main API cost - we batch to minimize calls
        candidates = []
        total_batches = (len(filtered_symbols) + self.BATCH_SIZE - 1) // self.BATCH_SIZE

        logger.info(f"Fetching quotes in {total_batches} batches...")

        for i in range(0, len(filtered_symbols), self.BATCH_SIZE):
            batch_symbols = filtered_symbols[i:i + self.BATCH_SIZE]
            batch_num = i // self.BATCH_SIZE + 1

            if batch_num % 10 == 0:
                logger.info(f"Processing batch {batch_num}/{total_batches}...")

            quotes = self._fetch_quotes_batch(batch_symbols)

            for quote in quotes:
                # Extract data
                symbol = quote.get("symbol")
                market_cap = quote.get("marketCap", 0) or 0
                pe = quote.get("pe")
                price = quote.get("price", 0)
                name = quote.get("name", "Unknown")

                # Filter by market cap
                if market_cap < criteria.min_market_cap or market_cap > criteria.max_market_cap:
                    continue

                # Filter by price (avoid penny stocks)
                if price is None or price < 5:
                    continue

                # Filter by P/E if available
                if pe is not None:
                    if pe <= 0 or pe > criteria.max_pe_ratio:
                        continue

                candidates.append(ScreenedStock(
                    symbol=symbol,
                    name=name,
                    market_cap=market_cap,
                    pe_ratio=pe,
                    debt_equity=None,
                    roe=None,
                    revenue_growth=None,
                    sector=quote.get("sector", "Unknown") or "Unknown",
                    industry=quote.get("industry", "Unknown") or "Unknown",
                    screened_at=datetime.now(),
                    price=price
                ))

        logger.info(f"After market cap + P/E filtering: {len(candidates)} candidates")
        return candidates
    
    def get_detailed_metrics(self, symbol: str) -> dict:
        """
        Fetch detailed financial metrics for a single stock.
        
        Used to apply additional filters that require separate API calls.
        """
        metrics = {}
        
        # Key metrics endpoint
        response = requests.get(
            f"{self.BASE_URL}/key-metrics-ttm/{symbol}",
            params={"apikey": self.api_key}
        )
        if response.status_code == 200:
            data = response.json()
            if data:
                metrics.update(data[0])
        
        # Ratios endpoint
        response = requests.get(
            f"{self.BASE_URL}/ratios-ttm/{symbol}",
            params={"apikey": self.api_key}
        )
        if response.status_code == 200:
            data = response.json()
            if data:
                metrics.update(data[0])
        
        return metrics
    
    def apply_detailed_filters(
        self, 
        candidates: list[ScreenedStock], 
        criteria: ScreeningCriteria
    ) -> list[ScreenedStock]:
        """
        Apply filters that require per-stock API calls.
        
        WARNING: This uses multiple API calls per stock.
        Use sparingly to stay within free tier limits.
        """
        filtered = []
        
        for stock in candidates:
            logger.debug(f"Fetching detailed metrics for {stock.symbol}")
            
            metrics = self.get_detailed_metrics(stock.symbol)
            
            # Check ROE
            roe = metrics.get("returnOnEquityTTM") or metrics.get("roeTTM")
            if roe is not None:
                stock.roe = roe
                if roe < criteria.min_roe:
                    logger.debug(f"{stock.symbol}: ROE {roe:.2%} below threshold")
                    continue
            
            # Check Debt/Equity
            de = metrics.get("debtEquityRatioTTM") or metrics.get("debtToEquityTTM")
            if de is not None:
                stock.debt_equity = de
                if de > criteria.max_debt_equity:
                    logger.debug(f"{stock.symbol}: D/E {de:.2f} above threshold")
                    continue
            
            # Check Current Ratio
            current_ratio = metrics.get("currentRatioTTM")
            if current_ratio is not None and current_ratio < criteria.min_current_ratio:
                logger.debug(f"{stock.symbol}: Current ratio {current_ratio:.2f} below threshold")
                continue
            
            filtered.append(stock)
        
        logger.info(f"After detailed filtering: {len(filtered)} stocks")
        return filtered


def run_screen(apply_detailed: bool = False) -> list[ScreenedStock]:
    """
    Convenience function to run a full screen.
    
    Args:
        apply_detailed: If True, fetch detailed metrics (uses more API calls)
    """
    screener = StockScreener()
    criteria = ScreeningCriteria()
    
    candidates = screener.screen(criteria)
    
    if apply_detailed and candidates:
        # Limit to top candidates to save API calls
        candidates = candidates[:50]
        candidates = screener.apply_detailed_filters(candidates, criteria)
    
    return candidates


if __name__ == "__main__":
    # Test the screener
    from dotenv import load_dotenv
    load_dotenv()
    
    logging.basicConfig(level=logging.INFO)
    
    stocks = run_screen(apply_detailed=False)
    print(f"\nFound {len(stocks)} candidates:\n")
    
    for stock in stocks[:10]:
        print(f"  {stock.symbol}: {stock.name}")
        print(f"    Market Cap: ${stock.market_cap:,.0f}")
        print(f"    P/E: {stock.pe_ratio}")
        print(f"    Sector: {stock.sector}")
        print()
