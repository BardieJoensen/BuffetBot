"""
Valuation Module

Aggregates fair value estimates from multiple external sources.
Does NOT calculate intrinsic value (LLMs are bad at this).
Instead, fetches estimates from services that specialize in valuation.

NOTE: Uses yfinance (free) and Finnhub (free tier).
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import requests
import yfinance as yf

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
                {"source": e.source, "fair_value": e.fair_value, "methodology": e.methodology} for e in self.estimates
            ],
        }


class ValuationAggregator:
    """
    Fetches and aggregates fair value estimates from multiple sources.

    Available sources (all free):
    - yfinance: Current price, analyst targets, fundamentals
    - Finnhub: Analyst price targets (free tier: 60 req/min)
    - Simple P/E multiples: Calculated from EPS × fair P/E
    """

    def __init__(self, finnhub_key: Optional[str] = None):
        self.finnhub_key = finnhub_key or os.getenv("FINNHUB_API_KEY")

    def get_valuation(self, symbol: str) -> AggregatedValuation:
        """
        Get aggregated valuation for a stock from all available sources.
        """
        # Get stock data from yfinance
        ticker = yf.Ticker(symbol)
        info = ticker.info

        current_price = info.get("regularMarketPrice") or info.get("currentPrice") or 0

        valuation = AggregatedValuation(symbol=symbol, current_price=current_price, estimates=[])

        # Fetch from each source
        estimates = []

        # 1. yfinance analyst targets
        yf_estimate = self._get_yfinance_target(info)
        if yf_estimate:
            estimates.append(yf_estimate)

        # 2. Finnhub Price Target
        finnhub_estimate = self._get_finnhub_price_target(symbol)
        if finnhub_estimate:
            estimates.append(finnhub_estimate)

        # 3. Simple P/E based valuation
        pe_estimate = self._calculate_pe_based_value(info)
        if pe_estimate:
            estimates.append(pe_estimate)

        # 4. Graham Number (conservative intrinsic value)
        graham_estimate = self._calculate_graham_number(info)
        if graham_estimate:
            estimates.append(graham_estimate)

        valuation.estimates = estimates

        if current_price > 0:
            logger.info(
                f"{symbol}: Price=${current_price:.2f}, "
                f"Avg Fair Value=${valuation.average_fair_value or 0:.2f}, "
                f"Margin of Safety={valuation.margin_of_safety or 0:.1%}"
            )
        else:
            logger.warning(f"{symbol}: Could not fetch current price")

        return valuation

    def _get_yfinance_target(self, info: dict) -> Optional[ValuationEstimate]:
        """
        Get analyst target price from yfinance.
        """
        target = info.get("targetMeanPrice") or info.get("targetMedianPrice")

        if target and target > 0:
            return ValuationEstimate(
                source="Yahoo Finance Analyst Target",
                fair_value=target,
                methodology="Analyst Price Targets",
                date=datetime.now(),
                confidence="medium",
            )

        return None

    def _get_finnhub_price_target(self, symbol: str) -> Optional[ValuationEstimate]:
        """Get price target from Finnhub (free tier: 60 req/min)"""
        if not self.finnhub_key:
            return None

        try:
            response = requests.get(
                "https://finnhub.io/api/v1/stock/price-target",
                params={"symbol": symbol, "token": self.finnhub_key},
                timeout=10,
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
                        confidence="medium",
                    )
        except Exception as e:
            logger.debug(f"Finnhub error for {symbol}: {e}")

        return None

    def _calculate_pe_based_value(self, info: dict) -> Optional[ValuationEstimate]:
        """
        Simple P/E based fair value calculation.

        Fair Value = EPS × Fair P/E (using 15 as market average)
        """
        eps = info.get("trailingEps") or info.get("forwardEps")

        if not eps or eps <= 0:
            return None

        # Use a conservative "fair" P/E of 15 (market historical average)
        fair_pe = 15
        fair_value = eps * fair_pe

        if fair_value > 0:
            return ValuationEstimate(
                source="P/E Multiple (Conservative)",
                fair_value=fair_value,
                methodology=f"EPS (${eps:.2f}) × Fair P/E (15)",
                date=datetime.now(),
                confidence="low",
            )

        return None

    def _calculate_graham_number(self, info: dict) -> Optional[ValuationEstimate]:
        """
        Calculate Graham Number - a conservative intrinsic value estimate.

        Graham Number = √(22.5 × EPS × Book Value per Share)

        This is Benjamin Graham's formula for finding a maximum fair price.
        """
        eps = info.get("trailingEps")
        book_value = info.get("bookValue")

        if not eps or eps <= 0 or not book_value or book_value <= 0:
            return None

        import math

        graham_number = math.sqrt(22.5 * eps * book_value)

        if graham_number > 0:
            return ValuationEstimate(
                source="Graham Number",
                fair_value=graham_number,
                methodology="√(22.5 × EPS × Book Value)",
                date=datetime.now(),
                confidence="medium",  # Time-tested conservative formula
            )

        return None


def get_valuation(symbol: str) -> AggregatedValuation:
    """Convenience function to get valuation for a symbol"""
    aggregator = ValuationAggregator()
    return aggregator.get_valuation(symbol)


def screen_for_undervalued(symbols: list[str], min_margin_of_safety: float = 0.20) -> list[AggregatedValuation]:
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
