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
"""

import os
import requests
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import logging

logger = logging.getLogger(__name__)


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
    """
    
    def __init__(self, fmp_key: Optional[str] = None, finnhub_key: Optional[str] = None):
        self.fmp_key = fmp_key or os.getenv("FMP_API_KEY")
        self.finnhub_key = finnhub_key or os.getenv("FINNHUB_API_KEY")
    
    def scan_for_bubbles(self, symbols: Optional[list[str]] = None) -> list[BubbleWarning]:
        """
        Scan stocks for bubble characteristics.
        
        If no symbols provided, scans popular/trending stocks.
        """
        
        if symbols is None:
            symbols = self._get_trending_stocks()
        
        warnings = []
        
        for symbol in symbols:
            try:
                warning = self._analyze_stock(symbol)
                if warning and warning.signal_count >= 2:
                    warnings.append(warning)
            except Exception as e:
                logger.error(f"Error analyzing {symbol}: {e}")
                continue
        
        # Sort by signal count (most dangerous first)
        warnings.sort(key=lambda w: w.signal_count, reverse=True)
        
        return warnings
    
    def _get_trending_stocks(self) -> list[str]:
        """Get list of popular/hyped stocks to check"""
        
        # Start with most active stocks
        if not self.fmp_key:
            return []
        
        response = requests.get(
            "https://financialmodelingprep.com/api/v3/stock_market/actives",
            params={"apikey": self.fmp_key}
        )
        
        if response.status_code == 200:
            data = response.json()
            return [stock["symbol"] for stock in data[:30]]
        
        return []
    
    def _analyze_stock(self, symbol: str) -> Optional[BubbleWarning]:
        """Analyze a single stock for bubble signals"""
        
        if not self.fmp_key:
            return None
        
        signals = []
        
        # Fetch quote data
        quote = self._get_quote(symbol)
        if not quote:
            return None
        
        price = quote.get("price", 0)
        pe = quote.get("pe")
        name = quote.get("name", symbol)
        
        # Fetch additional metrics
        metrics = self._get_metrics(symbol)
        growth = self._get_growth(symbol)
        insider_data = self._get_insider_activity(symbol)
        
        pe_ratio = pe
        revenue_growth = None
        insider_selling = None
        debt_change = None
        
        # Signal 1: Extreme P/E (>50) with weak growth
        if pe and pe > 50:
            rev_growth = growth.get("revenueGrowth", 0) if growth else 0
            revenue_growth = rev_growth
            
            if rev_growth < 0.20:  # Less than 20% growth
                signals.append(f"P/E of {pe:.0f} with only {rev_growth:.0%} revenue growth")
        
        # Signal 2: Negative earnings but high market cap
        if pe and pe < 0:
            market_cap = quote.get("marketCap", 0)
            if market_cap > 10_000_000_000:  # >$10B
                signals.append(f"No earnings (negative P/E) with ${market_cap/1e9:.0f}B market cap")
        
        # Signal 3: P/E > 100 (extreme speculation)
        if pe and pe > 100:
            signals.append(f"Extreme P/E of {pe:.0f} - priced for perfection")
        
        # Signal 4: Revenue declining but price up
        if growth:
            rev_growth = growth.get("revenueGrowth", 0)
            revenue_growth = rev_growth
            price_change = quote.get("changesPercentage", 0)
            
            if rev_growth < -0.05 and price_change > 20:
                signals.append(f"Revenue down {rev_growth:.0%} but stock up {price_change:.0f}%")
        
        # Signal 5: Insider selling cluster
        if insider_data:
            net_insider = insider_data.get("net_transactions", 0)
            insider_selling = insider_data.get("summary", "")
            
            if net_insider < -5:  # More than 5 net sells
                signals.append(f"Heavy insider selling: {insider_selling}")
        
        # Signal 6: Debt spiking
        if metrics:
            debt_equity = metrics.get("debtToEquityTTM", 0)
            if debt_equity > 2:
                debt_change = debt_equity
                signals.append(f"High debt/equity of {debt_equity:.1f}")
        
        # Signal 7: Price-to-Sales extreme
        if metrics:
            ps_ratio = metrics.get("priceToSalesRatioTTM", 0)
            if ps_ratio > 20:
                signals.append(f"Price/Sales of {ps_ratio:.0f} - extremely speculative")
        
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
    
    def _get_quote(self, symbol: str) -> Optional[dict]:
        """Fetch stock quote"""
        response = requests.get(
            f"https://financialmodelingprep.com/api/v3/quote/{symbol}",
            params={"apikey": self.fmp_key}
        )
        
        if response.status_code == 200:
            data = response.json()
            return data[0] if data else None
        return None
    
    def _get_metrics(self, symbol: str) -> Optional[dict]:
        """Fetch key metrics"""
        response = requests.get(
            f"https://financialmodelingprep.com/api/v3/ratios-ttm/{symbol}",
            params={"apikey": self.fmp_key}
        )
        
        if response.status_code == 200:
            data = response.json()
            return data[0] if data else None
        return None
    
    def _get_growth(self, symbol: str) -> Optional[dict]:
        """Fetch growth metrics"""
        response = requests.get(
            f"https://financialmodelingprep.com/api/v3/financial-growth/{symbol}",
            params={"apikey": self.fmp_key, "limit": 1}
        )
        
        if response.status_code == 200:
            data = response.json()
            return data[0] if data else None
        return None
    
    def _get_insider_activity(self, symbol: str) -> Optional[dict]:
        """Fetch insider trading activity"""
        
        # Try Finnhub for insider data
        if self.finnhub_key:
            response = requests.get(
                "https://finnhub.io/api/v1/stock/insider-transactions",
                params={"symbol": symbol, "token": self.finnhub_key}
            )
            
            if response.status_code == 200:
                data = response.json().get("data", [])
                
                if data:
                    # Count buys vs sells in last 90 days
                    buys = sum(1 for t in data[:20] if t.get("transactionType") == "P")
                    sells = sum(1 for t in data[:20] if t.get("transactionType") == "S")
                    
                    return {
                        "net_transactions": buys - sells,
                        "buys": buys,
                        "sells": sells,
                        "summary": f"{sells} sells, {buys} buys recently"
                    }
        
        return None


def get_market_temperature(fmp_key: Optional[str] = None) -> dict:
    """
    Assess overall market valuation level.
    
    Returns a "temperature" reading:
    - HOT: Market expensive, few bargains
    - WARM: Market fairly valued
    - COOL: Market cheap, many opportunities
    """
    
    fmp_key = fmp_key or os.getenv("FMP_API_KEY")
    if not fmp_key:
        return {"temperature": "UNKNOWN", "message": "No API key"}
    
    # Get S&P 500 P/E as proxy for market valuation
    response = requests.get(
        "https://financialmodelingprep.com/api/v3/quote/%5EGSPC",
        params={"apikey": fmp_key}
    )
    
    market_pe = None
    if response.status_code == 200:
        data = response.json()
        if data:
            market_pe = data[0].get("pe")
    
    # Historical S&P 500 P/E averages:
    # <15: Cheap
    # 15-20: Fair
    # 20-25: Expensive
    # >25: Very expensive
    
    if market_pe is None:
        temperature = "UNKNOWN"
        interpretation = "Could not fetch market data"
    elif market_pe < 15:
        temperature = "COLD"
        interpretation = f"Market P/E of {market_pe:.1f} is below historical average. Good time to find bargains."
    elif market_pe < 20:
        temperature = "COOL"
        interpretation = f"Market P/E of {market_pe:.1f} is near fair value. Selective opportunities exist."
    elif market_pe < 25:
        temperature = "WARM"
        interpretation = f"Market P/E of {market_pe:.1f} is above average. Be selective, demand margin of safety."
    else:
        temperature = "HOT"
        interpretation = f"Market P/E of {market_pe:.1f} is elevated. Few bargains, consider holding cash."
    
    return {
        "temperature": temperature,
        "market_pe": market_pe,
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
