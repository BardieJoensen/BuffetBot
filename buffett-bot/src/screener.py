"""
Stock Screener Module

Filters stocks based on Buffett-style value criteria using FMP API.
Returns candidates that pass quantitative filters for further analysis.
"""

import os
import requests
from dataclasses import dataclass
from typing import Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


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
    exchange: str = "NASDAQ,NYSE"            # Major exchanges


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
            "screened_at": self.screened_at.isoformat()
        }


class StockScreener:
    """
    Screens stocks using Financial Modeling Prep API.
    
    Free tier: 250 requests/day
    """
    
    BASE_URL = "https://financialmodelingprep.com/api/v3"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("FMP_API_KEY")
        if not self.api_key:
            raise ValueError("FMP_API_KEY not found in environment")
    
    def screen(self, criteria: Optional[ScreeningCriteria] = None) -> list[ScreenedStock]:
        """
        Run stock screen with given criteria.
        
        Returns list of stocks passing all filters.
        """
        criteria = criteria or ScreeningCriteria()
        
        logger.info(f"Running screen with criteria: market_cap={criteria.min_market_cap}-{criteria.max_market_cap}")
        
        # FMP stock screener endpoint
        params = {
            "apikey": self.api_key,
            "marketCapMoreThan": int(criteria.min_market_cap),
            "marketCapLowerThan": int(criteria.max_market_cap),
            "priceMoreThan": 5,  # Avoid penny stocks
            "isActivelyTrading": True,
            "country": criteria.country,
            "exchange": criteria.exchange,
            "limit": 1000
        }
        
        response = requests.get(f"{self.BASE_URL}/stock-screener", params=params)
        response.raise_for_status()
        
        raw_stocks = response.json()
        logger.info(f"Initial screen returned {len(raw_stocks)} stocks")
        
        # Apply additional filters that FMP screener doesn't support directly
        candidates = []
        for stock in raw_stocks:
            # Skip if missing critical data
            if not stock.get("symbol"):
                continue
            
            # Filter by P/E (if available)
            pe = stock.get("pe")
            if pe is not None and (pe <= 0 or pe > criteria.max_pe_ratio):
                continue
            
            # Filter by beta (optional - avoid extremely volatile)
            beta = stock.get("beta")
            if beta is not None and beta > 2.0:
                continue
            
            candidates.append(ScreenedStock(
                symbol=stock["symbol"],
                name=stock.get("companyName", "Unknown"),
                market_cap=stock.get("marketCap", 0),
                pe_ratio=pe,
                debt_equity=None,  # Fetched separately in fundamentals
                roe=None,          # Fetched separately
                revenue_growth=None,
                sector=stock.get("sector", "Unknown"),
                industry=stock.get("industry", "Unknown"),
                screened_at=datetime.now()
            ))
        
        logger.info(f"After filtering: {len(candidates)} candidates")
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
