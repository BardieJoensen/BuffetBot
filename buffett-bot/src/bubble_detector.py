"""
Bubble Detector Module

Identifies potentially overvalued stocks to avoid.
NOT for shorting - just a "stay away" list.

Signals:
- Extreme P/E with slowing growth
- Negative earnings + high valuation
- Insider selling clusters
- Revenue decline + price increase
- Excessive debt accumulation

NOTE: Uses yfinance (free) and Finnhub (free tier).
"""

import os
import requests
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import logging
import yfinance as yf

logger = logging.getLogger(__name__)


# Popular/trending stocks to check for bubbles
TRENDING_STOCKS = [
    "TSLA", "NVDA", "PLTR", "AMD", "COIN", "HOOD", "MSTR", "RIOT",
    "SQ", "SHOP", "SNOW", "CRWD", "NET", "DDOG", "ZS", "OKTA",
    "RBLX", "U", "ABNB", "DASH", "UBER", "LYFT", "RIVN", "LCID"
]


@dataclass
class BubbleWarning:
    """A stock flagged as potentially overvalued"""
    symbol: str
    company_name: str
    current_price: float

    # Warning signals
    signals: list[str]
    signal_count: int

    # Key metrics that triggered warning
    pe_ratio: Optional[float] = None
    revenue_growth: Optional[float] = None
    insider_selling: Optional[str] = None
    debt_change: Optional[float] = None

    # Risk level
    risk_level: str = "MEDIUM"  # HIGH, MEDIUM

    # Explanation
    summary: str = ""

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "company_name": self.company_name,
            "current_price": self.current_price,
            "signals": self.signals,
            "signal_count": self.signal_count,
            "risk_level": self.risk_level,
            "metrics": {
                "pe_ratio": self.pe_ratio,
                "revenue_growth": self.revenue_growth,
                "insider_selling": self.insider_selling,
                "debt_change": self.debt_change
            },
            "summary": self.summary
        }


class BubbleDetector:
    """
    Scans for overvalued stocks to avoid.

    This is NOT for shorting. It's a warning list of stocks
    that look dangerously overvalued.

    Uses yfinance (free) and Finnhub (free tier).
    """

    def __init__(self, finnhub_key: Optional[str] = None):
        self.finnhub_key = finnhub_key or os.getenv("FINNHUB_API_KEY")

    def scan_for_bubbles(self, symbols: Optional[list[str]] = None) -> list[BubbleWarning]:
        """
        Scan stocks for bubble characteristics.

        If no symbols provided, scans popular/trending stocks.
        """

        if symbols is None:
            symbols = TRENDING_STOCKS

        warnings = []

        for symbol in symbols:
            try:
                warning = self._analyze_stock(symbol)
                if warning and warning.signal_count >= 2:
                    warnings.append(warning)
            except Exception as e:
                logger.debug(f"Error analyzing {symbol}: {e}")
                continue

        # Sort by signal count (most dangerous first)
        warnings.sort(key=lambda w: w.signal_count, reverse=True)

        return warnings

    def _analyze_stock(self, symbol: str) -> Optional[BubbleWarning]:
        """Analyze a single stock for bubble signals using yfinance"""

        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
        except Exception as e:
            logger.debug(f"Failed to fetch {symbol}: {e}")
            return None

        if not info:
            return None

        signals = []

        price = info.get("regularMarketPrice") or info.get("currentPrice") or 0
        pe = info.get("trailingPE") or info.get("forwardPE")
        name = info.get("longName") or info.get("shortName") or symbol
        market_cap = info.get("marketCap", 0)
        revenue_growth = info.get("revenueGrowth")
        debt_equity = info.get("debtToEquity")
        ps_ratio = info.get("priceToSalesTrailing12Months")

        pe_ratio = pe
        insider_selling = None
        debt_change = None

        # Signal 1: Extreme P/E (>50) with weak growth
        if pe and pe > 50:
            rev_growth = revenue_growth or 0

            if rev_growth < 0.20:  # Less than 20% growth
                signals.append(f"P/E of {pe:.0f} with only {rev_growth:.0%} revenue growth")

        # Signal 2: Negative earnings but high market cap
        if pe and pe < 0:
            if market_cap > 10_000_000_000:  # >$10B
                signals.append(f"No earnings (negative P/E) with ${market_cap/1e9:.0f}B market cap")

        # Signal 3: P/E > 100 (extreme speculation)
        if pe and pe > 100:
            signals.append(f"Extreme P/E of {pe:.0f} - priced for perfection")

        # Signal 4: Revenue declining but stock up
        if revenue_growth is not None:
            fifty_two_week_change = info.get("52WeekChange", 0)

            if revenue_growth < -0.05 and fifty_two_week_change > 0.20:
                signals.append(f"Revenue down {revenue_growth:.0%} but stock up {fifty_two_week_change:.0%} YoY")

        # Signal 5: Insider selling cluster (via Finnhub)
        insider_data = self._get_insider_activity(symbol)
        if insider_data:
            net_insider = insider_data.get("net_transactions", 0)
            insider_selling = insider_data.get("summary", "")

            if net_insider < -5:  # More than 5 net sells
                signals.append(f"Heavy insider selling: {insider_selling}")

        # Signal 6: Debt spiking
        if debt_equity and debt_equity > 200:  # yfinance returns as percentage
            debt_change = debt_equity / 100
            signals.append(f"High debt/equity of {debt_equity/100:.1f}")

        # Signal 7: Price-to-Sales extreme
        if ps_ratio and ps_ratio > 20:
            signals.append(f"Price/Sales of {ps_ratio:.0f} - extremely speculative")

        # Signal 8: Price far above analyst target
        target_price = info.get("targetMeanPrice")
        if target_price and price > 0:
            if price > target_price * 1.3:  # 30% above target
                signals.append(f"Price ${price:.0f} is {((price/target_price)-1)*100:.0f}% above analyst target ${target_price:.0f}")

        if not signals:
            return None

        # Determine risk level
        risk_level = "HIGH" if len(signals) >= 3 else "MEDIUM"

        # Create summary
        summary = f"⚠️ {len(signals)} warning signals detected. "
        if pe and pe > 50:
            summary += "Valuation appears disconnected from fundamentals. "
        if insider_selling:
            summary += "Insiders are selling. "

        return BubbleWarning(
            symbol=symbol,
            company_name=name,
            current_price=price,
            signals=signals,
            signal_count=len(signals),
            pe_ratio=pe_ratio,
            revenue_growth=revenue_growth,
            insider_selling=insider_selling,
            debt_change=debt_change,
            risk_level=risk_level,
            summary=summary
        )

    def _get_insider_activity(self, symbol: str) -> Optional[dict]:
        """Fetch insider trading activity from Finnhub"""

        if not self.finnhub_key:
            return None

        try:
            response = requests.get(
                "https://finnhub.io/api/v1/stock/insider-transactions",
                params={"symbol": symbol, "token": self.finnhub_key},
                timeout=10
            )

            if response.status_code == 200:
                data = response.json().get("data", [])

                if data:
                    # Count buys vs sells in recent transactions
                    buys = sum(1 for t in data[:20] if t.get("transactionType") == "P")
                    sells = sum(1 for t in data[:20] if t.get("transactionType") == "S")

                    return {
                        "net_transactions": buys - sells,
                        "buys": buys,
                        "sells": sells,
                        "summary": f"{sells} sells, {buys} buys recently"
                    }
        except Exception as e:
            logger.debug(f"Finnhub insider error for {symbol}: {e}")

        return None


def get_market_temperature() -> dict:
    """
    Assess overall market valuation level using yfinance.

    Returns a "temperature" reading:
    - HOT: Market expensive, few bargains
    - WARM: Market fairly valued
    - COOL: Market cheap, many opportunities
    """

    try:
        # Get S&P 500 data as proxy for market valuation
        spy = yf.Ticker("SPY")
        info = spy.info

        # SPY doesn't have direct P/E, estimate from price/earnings
        price = info.get("regularMarketPrice") or info.get("previousClose") or 0

        # Get VOO (Vanguard S&P 500) which sometimes has better data
        voo = yf.Ticker("VOO")
        voo_info = voo.info
        pe_ratio = voo_info.get("trailingPE") or info.get("trailingPE")

        # Alternatively, use SPY's trailing PE if available
        if not pe_ratio:
            # Rough estimate: S&P 500 historical average is ~20-25
            # We'll use the Shiller PE proxy or just return unknown
            pe_ratio = None

    except Exception as e:
        logger.debug(f"Error fetching market data: {e}")
        pe_ratio = None
        price = 0

    # Historical S&P 500 P/E averages:
    # <15: Cheap
    # 15-20: Fair
    # 20-25: Expensive
    # >25: Very expensive

    if pe_ratio is None:
        temperature = "UNKNOWN"
        interpretation = "Could not fetch market data"
    elif pe_ratio < 15:
        temperature = "COLD"
        interpretation = f"Market P/E of {pe_ratio:.1f} is below historical average. Good time to find bargains."
    elif pe_ratio < 20:
        temperature = "COOL"
        interpretation = f"Market P/E of {pe_ratio:.1f} is near fair value. Selective opportunities exist."
    elif pe_ratio < 25:
        temperature = "WARM"
        interpretation = f"Market P/E of {pe_ratio:.1f} is above average. Be selective, demand margin of safety."
    else:
        temperature = "HOT"
        interpretation = f"Market P/E of {pe_ratio:.1f} is elevated. Few bargains, consider holding cash."

    return {
        "temperature": temperature,
        "market_pe": pe_ratio,
        "interpretation": interpretation,
        "checked_at": datetime.now().isoformat()
    }


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    logging.basicConfig(level=logging.INFO)

    # Test market temperature
    temp = get_market_temperature()
    print(f"Market Temperature: {temp['temperature']}")
    print(f"Interpretation: {temp['interpretation']}")

    # Test bubble detection
    detector = BubbleDetector()

    # Test on some known expensive stocks
    warnings = detector.scan_for_bubbles(["TSLA", "NVDA", "PLTR"])

    print(f"\nBubble Warnings ({len(warnings)}):")
    for w in warnings:
        print(f"\n{w.symbol}: {w.risk_level} RISK")
        print(f"  Signals: {w.signal_count}")
        for signal in w.signals:
            print(f"    - {signal}")
