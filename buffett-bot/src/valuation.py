"""
Valuation Module

Aggregates fair value estimates from multiple external sources.
Does NOT calculate intrinsic value (LLMs are bad at this).
Instead, fetches estimates from services that specialize in valuation.

NOTE: Uses yfinance (free) and Finnhub (free tier).
"""

import logging
import math
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
        weight_map = {"high": 1.5, "medium": 1.0, "low": 0.5}
        total_weighted = sum(e.fair_value * weight_map.get(e.confidence, 1.0) for e in self.estimates)
        total_weight = sum(weight_map.get(e.confidence, 1.0) for e in self.estimates)
        return total_weighted / total_weight if total_weight > 0 else None

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

        # 5. DCF (10yr two-phase, forward-looking)
        dcf_estimate = self._calculate_dcf_fair_value(info, ticker)
        if dcf_estimate:
            estimates.append(dcf_estimate)

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

        Fair Value = EPS × Fair P/E (using 18 as adjusted market average)
        """
        eps = info.get("trailingEps") or info.get("forwardEps")

        if not eps or eps <= 0:
            return None

        # Use an 18x fair P/E — better reflects quality businesses (was 15, Graham-era)
        fair_pe = 18
        fair_value = eps * fair_pe

        if fair_value > 0:
            return ValuationEstimate(
                source="P/E Multiple (Conservative)",
                fair_value=fair_value,
                methodology=f"EPS (${eps:.2f}) × Fair P/E (18)",
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

        graham_number = math.sqrt(22.5 * eps * book_value)

        if graham_number > 0:
            # Downgrade confidence for asset-light companies: high P/B means book value
            # is disconnected from business value (e.g. Visa, MSFT trade at 10-40x book).
            # Graham Number systematically undervalues these — reduce its weight.
            price = info.get("currentPrice") or info.get("regularMarketPrice")
            pb_ratio = price / book_value if (price and book_value > 0) else None
            confidence = "low" if (pb_ratio is not None and pb_ratio > 5.0) else "medium"

            return ValuationEstimate(
                source="Graham Number",
                fair_value=graham_number,
                methodology="√(22.5 × EPS × Book Value)",
                date=datetime.now(),
                confidence=confidence,
            )

        return None

    def _calculate_dcf_fair_value(self, info: dict, ticker: yf.Ticker) -> Optional[ValuationEstimate]:
        """
        Simple 10-year two-phase DCF using Real FCF (OCF − CapEx − SBC).

        Phase 1 (years 1–5): base growth capped at 15%
        Phase 2 (years 6–10): base growth capped at 8%
        Terminal: 12× Year-10 FCF
        Discount rate: 10% flat

        Growth rate = min(FCF CAGR, Revenue CAGR) — conservative anchor.
        Falls back to 0% growth if neither is available.
        """
        shares = info.get("sharesOutstanding")
        if not shares or shares <= 0:
            return None

        try:
            cashflow = ticker.cashflow
            if cashflow is None or cashflow.empty:
                return None

            # --- Real FCF (latest year): OCF − CapEx − SBC ---
            ocf = None
            for label in ["Operating Cash Flow", "Cash Flow From Continuing Operating Activities"]:
                if label in cashflow.index:
                    ocf = float(cashflow.loc[label].iloc[0])
                    break

            if ocf is None:
                return None

            capex = 0.0
            for label in ["Capital Expenditure", "Capital Expenditures"]:
                if label in cashflow.index:
                    capex = abs(float(cashflow.loc[label].iloc[0]))
                    break

            sbc = 0.0
            for label in ["Stock Based Compensation", "Share Based Compensation", "ShareBasedCompensation"]:
                if label in cashflow.index:
                    sbc = abs(float(cashflow.loc[label].iloc[0]))
                    break

            real_fcf = ocf - capex - sbc
            if real_fcf <= 0:
                return None  # No DCF for loss-making or FCF-negative companies

            # --- FCF CAGR from cashflow history ---
            fcf_cagr: Optional[float] = None
            try:
                n_cols = len(cashflow.columns)
                if n_cols >= 2:
                    ocf_row = None
                    for label in ["Operating Cash Flow", "Cash Flow From Continuing Operating Activities"]:
                        if label in cashflow.index:
                            ocf_row = cashflow.loc[label].dropna()
                            break

                    capex_row = None
                    for label in ["Capital Expenditure", "Capital Expenditures"]:
                        if label in cashflow.index:
                            capex_row = cashflow.loc[label].dropna()
                            break

                    if ocf_row is not None and capex_row is not None:
                        common = ocf_row.index.intersection(capex_row.index)
                        if len(common) >= 2:
                            latest = float(ocf_row[common[0]]) - abs(float(capex_row[common[0]]))
                            earliest = float(ocf_row[common[-1]]) - abs(float(capex_row[common[-1]]))
                            years = len(common) - 1
                            if earliest > 0 and latest > 0 and years > 0:
                                fcf_cagr = (latest / earliest) ** (1.0 / years) - 1.0
            except Exception:
                pass

            # --- Revenue CAGR from financials ---
            revenue_cagr: Optional[float] = None
            try:
                financials = ticker.financials
                if financials is not None and not financials.empty and len(financials.columns) >= 2:
                    rev_row = None
                    for label in ["Total Revenue", "Revenue"]:
                        if label in financials.index:
                            rev_row = financials.loc[label].astype(float)
                            break
                    if rev_row is not None:
                        latest_rev = rev_row.iloc[0]
                        earliest_rev = rev_row.iloc[-1]
                        years = len(rev_row) - 1
                        if earliest_rev > 0 and latest_rev > 0 and years > 0:
                            revenue_cagr = (latest_rev / earliest_rev) ** (1.0 / years) - 1.0
            except Exception:
                pass

            # --- Growth rate: conservative (lower of the two) ---
            candidates = [g for g in [fcf_cagr, revenue_cagr] if g is not None]
            base_growth = min(candidates) if candidates else 0.0

            phase1_growth = max(0.0, min(base_growth, 0.15))
            phase2_growth = max(0.0, min(base_growth, 0.08))

            # --- Project and discount ---
            total_pv = 0.0
            projected_fcf = real_fcf
            discount_rate = 0.10
            terminal_multiple = 12

            for year in range(1, 11):
                growth = phase1_growth if year <= 5 else phase2_growth
                projected_fcf *= 1 + growth
                total_pv += projected_fcf / ((1 + discount_rate) ** year)

            terminal_pv = (projected_fcf * terminal_multiple) / ((1 + discount_rate) ** 10)
            fair_value_per_share = (total_pv + terminal_pv) / shares

            if fair_value_per_share <= 0:
                return None

            methodology = (
                f"10yr DCF: Real FCF ${real_fcf / 1e9:.1f}B, "
                f"g={phase1_growth:.0%}/{phase2_growth:.0%}, "
                f"10% discount, 12× terminal"
            )
            return ValuationEstimate(
                source="DCF (10yr Owner Earnings)",
                fair_value=fair_value_per_share,
                methodology=methodology,
                date=datetime.now(),
                confidence="medium",
            )

        except Exception as e:
            logger.debug(f"DCF calculation failed: {e}")
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
