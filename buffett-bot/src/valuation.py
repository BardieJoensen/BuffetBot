"""
Valuation Module

Aggregates fair value estimates from multiple external sources.
Does NOT calculate intrinsic value (LLMs are bad at this).
Instead, fetches estimates from services that specialize in valuation.
"""

import os
import requests
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class ValuationEstimate:
    """A single fair value estimate from one source"""
    source: str
    fair_value: float
    methodology: str  # DCF, relative, etc.
    date: datetime
    confidence: str = "unknown"  # high, medium, low


@dataclass 
class AggregatedValuation:
    """Combined valuation assessment for a stock"""
    symbol: str
    current_price: float
    estimates: list[ValuationEstimate] = field(default_factory=list)
    
    @property
    def average_fair_value(self) -> Optional[float]:
        if not self.estimates:
            return None
        return sum(e.fair_value for e in self.estimates) / len(self.estimates)
    
    @property
    def margin_of_safety(self) -> Optional[float]:
        """
        Margin of safety = (Fair Value - Current Price) / Fair Value
        
        Positive = undervalued (good)
        Negative = overvalued (avoid)
        """
        avg = self.average_fair_value
        if avg is None or avg == 0:
            return None
        return (avg - self.current_price) / avg
    
    @property
    def upside_potential(self) -> Optional[float]:
        """Potential upside as percentage"""
        avg = self.average_fair_value
        if avg is None or self.current_price == 0:
            return None
        return (avg - self.current_price) / self.current_price
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "current_price": self.current_price,
            "average_fair_value": self.average_fair_value,
            "margin_of_safety": self.margin_of_safety,
            "upside_potential": self.upside_potential,
            "estimates": [
                {
                    "source": e.source,
                    "fair_value": e.fair_value,
                    "methodology": e.methodology
                }
                for e in self.estimates
            ]
        }


class ValuationAggregator:
    """
    Fetches and aggregates fair value estimates from multiple sources.
    
    Available sources:
    - FMP: Analyst price targets (free)
    - Finnhub: Analyst recommendations (free)
    - Simple multiples: Calculated from sector averages (free)
    
    Premium sources (require subscription):
    - GuruFocus: DCF valuation
    - Simply Wall St: Fair value estimate
    - Morningstar: Fair value estimate
    """
    
    def __init__(
        self, 
        fmp_key: Optional[str] = None,
        finnhub_key: Optional[str] = None
    ):
        self.fmp_key = fmp_key or os.getenv("FMP_API_KEY")
        self.finnhub_key = finnhub_key or os.getenv("FINNHUB_API_KEY")
    
    def get_valuation(self, symbol: str) -> AggregatedValuation:
        """
        Get aggregated valuation for a stock from all available sources.
        """
        # First get current price
        current_price = self._get_current_price(symbol)
        
        valuation = AggregatedValuation(
            symbol=symbol,
            current_price=current_price,
            estimates=[]
        )
        
        # Fetch from each source
        estimates = []
        
        # 1. FMP Analyst Price Targets
        fmp_estimate = self._get_fmp_price_target(symbol)
        if fmp_estimate:
            estimates.append(fmp_estimate)
        
        # 2. Finnhub Price Target
        finnhub_estimate = self._get_finnhub_price_target(symbol)
        if finnhub_estimate:
            estimates.append(finnhub_estimate)
        
        # 3. FMP DCF Value (their calculated fair value)
        dcf_estimate = self._get_fmp_dcf(symbol)
        if dcf_estimate:
            estimates.append(dcf_estimate)
        
        # 4. Simple P/E based valuation
        pe_estimate = self._calculate_pe_based_value(symbol)
        if pe_estimate:
            estimates.append(pe_estimate)
        
        valuation.estimates = estimates
        
        logger.info(
            f"{symbol}: Price=${current_price:.2f}, "
            f"Avg Fair Value=${valuation.average_fair_value or 0:.2f}, "
            f"Margin of Safety={valuation.margin_of_safety or 0:.1%}"
        )
        
        return valuation
    
    def _get_current_price(self, symbol: str) -> float:
        """Fetch current stock price"""
        if not self.fmp_key:
            return 0.0
        
        response = requests.get(
            f"https://financialmodelingprep.com/api/v3/quote-short/{symbol}",
            params={"apikey": self.fmp_key}
        )
        
        if response.status_code == 200:
            data = response.json()
            if data:
                return data[0].get("price", 0.0)
        
        return 0.0
    
    def _get_fmp_price_target(self, symbol: str) -> Optional[ValuationEstimate]:
        """
        Get analyst consensus price target from FMP.
        
        This is the average of Wall Street analyst targets.
        """
        if not self.fmp_key:
            return None
        
        response = requests.get(
            f"https://financialmodelingprep.com/api/v4/price-target-consensus/{symbol}",
            params={"apikey": self.fmp_key}
        )
        
        if response.status_code == 200:
            data = response.json()
            if data:
                target = data[0].get("targetConsensus")
                if target and target > 0:
                    return ValuationEstimate(
                        source="FMP Analyst Consensus",
                        fair_value=target,
                        methodology="Analyst Price Targets",
                        date=datetime.now(),
                        confidence="medium"
                    )
        
        return None
    
    def _get_finnhub_price_target(self, symbol: str) -> Optional[ValuationEstimate]:
        """Get price target from Finnhub"""
        if not self.finnhub_key:
            return None
        
        response = requests.get(
            "https://finnhub.io/api/v1/stock/price-target",
            params={"symbol": symbol, "token": self.finnhub_key}
        )
        
        if response.status_code == 200:
            data = response.json()
            target = data.get("targetMean") or data.get("targetMedian")
            if target and target > 0:
                return ValuationEstimate(
                    source="Finnhub Analyst Consensus",
                    fair_value=target,
                    methodology="Analyst Price Targets",
                    date=datetime.now(),
                    confidence="medium"
                )
        
        return None
    
    def _get_fmp_dcf(self, symbol: str) -> Optional[ValuationEstimate]:
        """
        Get FMP's calculated DCF value.
        
        Note: This is FMP's model, not ours. We just fetch their estimate.
        """
        if not self.fmp_key:
            return None
        
        response = requests.get(
            f"https://financialmodelingprep.com/api/v3/discounted-cash-flow/{symbol}",
            params={"apikey": self.fmp_key}
        )
        
        if response.status_code == 200:
            data = response.json()
            if data:
                dcf = data[0].get("dcf")
                if dcf and dcf > 0:
                    return ValuationEstimate(
                        source="FMP DCF Model",
                        fair_value=dcf,
                        methodology="Discounted Cash Flow",
                        date=datetime.now(),
                        confidence="low"  # Automated DCF, use with caution
                    )
        
        return None
    
    def _calculate_pe_based_value(self, symbol: str) -> Optional[ValuationEstimate]:
        """
        Simple P/E based fair value calculation.
        
        This is basic: Fair Value = EPS × Industry Average P/E
        
        Not sophisticated, but provides another reference point.
        """
        if not self.fmp_key:
            return None
        
        # Get company EPS and sector
        response = requests.get(
            f"https://financialmodelingprep.com/api/v3/quote/{symbol}",
            params={"apikey": self.fmp_key}
        )
        
        if response.status_code != 200:
            return None
        
        data = response.json()
        if not data:
            return None
        
        stock = data[0]
        eps = stock.get("eps")
        current_pe = stock.get("pe")
        
        if not eps or eps <= 0:
            return None
        
        # Use a conservative "fair" P/E of 15 (market historical average)
        # Could be enhanced to use sector-specific averages
        fair_pe = 15
        
        # If current P/E is very low, the stock might be cheap
        # If current P/E is high, using average P/E gives lower fair value
        fair_value = eps * fair_pe
        
        if fair_value > 0:
            return ValuationEstimate(
                source="P/E Multiple (Conservative)",
                fair_value=fair_value,
                methodology=f"EPS × Fair P/E (15)",
                date=datetime.now(),
                confidence="low"
            )
        
        return None


def get_valuation(symbol: str) -> AggregatedValuation:
    """Convenience function to get valuation for a symbol"""
    aggregator = ValuationAggregator()
    return aggregator.get_valuation(symbol)


def screen_for_undervalued(
    symbols: list[str], 
    min_margin_of_safety: float = 0.20
) -> list[AggregatedValuation]:
    """
    Screen a list of symbols for undervalued stocks.
    
    Args:
        symbols: List of stock symbols to check
        min_margin_of_safety: Minimum required margin (0.20 = 20%)
    
    Returns:
        List of undervalued stocks, sorted by margin of safety
    """
    aggregator = ValuationAggregator()
    undervalued = []
    
    for symbol in symbols:
        try:
            valuation = aggregator.get_valuation(symbol)
            
            if valuation.margin_of_safety and valuation.margin_of_safety >= min_margin_of_safety:
                undervalued.append(valuation)
                
        except Exception as e:
            logger.warning(f"Error getting valuation for {symbol}: {e}")
            continue
    
    # Sort by margin of safety (highest first)
    undervalued.sort(key=lambda v: v.margin_of_safety or 0, reverse=True)
    
    return undervalued


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    logging.basicConfig(level=logging.INFO)
    
    # Test with a known stock
    test_symbols = ["AAPL", "MSFT", "JNJ"]
    
    for symbol in test_symbols:
        val = get_valuation(symbol)
        print(f"\n{symbol}:")
        print(f"  Current Price: ${val.current_price:.2f}")
        print(f"  Average Fair Value: ${val.average_fair_value or 0:.2f}")
        print(f"  Margin of Safety: {val.margin_of_safety or 0:.1%}")
        print(f"  Estimates from {len(val.estimates)} sources:")
        for est in val.estimates:
            print(f"    - {est.source}: ${est.fair_value:.2f}")
