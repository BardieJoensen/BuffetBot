"""
Bubble Detector & Market Regime Classifier

v2.0 — Two responsibilities:

1. **Market Regime Classification** — Classifies current market conditions:
   - Euphoria/Bubble: extreme overvaluation, high speculation → watchlist only
   - Overvalued: above historical averages → selective Tier 1 deployment only
   - Fair Value: normal conditions → deploy on Tier 1 picks
   - Correction: 10-20% drawdown → cross-reference Tier 2 for new Tier 1 entries
   - Crisis: 20%+ drawdown, fear elevated → maximum deployment mode

2. **Bubble Stock Detection** — Identifies overvalued individual stocks to avoid.

Uses yfinance (free) and Finnhub (free tier, optional).
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import requests
import yfinance as yf

logger = logging.getLogger(__name__)


# Popular/trending stocks to check for bubbles
TRENDING_STOCKS = [
    "TSLA", "NVDA", "PLTR", "AMD", "COIN", "HOOD", "MSTR", "RIOT",
    "SQ", "SHOP", "SNOW", "CRWD", "NET", "DDOG", "ZS", "OKTA",
    "RBLX", "U", "ABNB", "DASH", "UBER", "LYFT", "RIVN", "LCID",
]

# Market regime thresholds
REGIME_THRESHOLDS = {
    "pe_euphoria": 30,       # S&P 500 P/E above this = euphoria
    "pe_overvalued": 23,     # Above this = overvalued
    "pe_fair_high": 20,      # Fair value ceiling
    "pe_fair_low": 15,       # Fair value floor
    "vix_fear": 30,          # VIX above this = elevated fear
    "vix_extreme_fear": 40,  # VIX above this = crisis fear
    "vix_complacency": 12,   # VIX below this = complacency (euphoria signal)
    "drawdown_correction": -0.10,  # 10% below peak
    "drawdown_crisis": -0.20,      # 20% below peak
    "ma200_euphoria": 0.15,        # 15%+ above 200-day MA
    "ma200_correction": -0.05,     # 5%+ below 200-day MA
}


@dataclass
class MarketRegime:
    """Market regime classification result."""

    regime: str  # "euphoria", "overvalued", "fair_value", "correction", "crisis"
    confidence: str  # "high", "moderate", "low"
    interpretation: str
    deployment_guidance: str

    # Underlying signals
    market_pe: Optional[float] = None
    vix: Optional[float] = None
    drawdown_from_peak: Optional[float] = None
    distance_from_200ma: Optional[float] = None

    # Legacy compatibility
    temperature: str = "UNKNOWN"

    signals: list[str] = field(default_factory=list)
    checked_at: str = ""

    def to_dict(self) -> dict:
        return {
            "regime": self.regime,
            "temperature": self.temperature,
            "confidence": self.confidence,
            "interpretation": self.interpretation,
            "deployment_guidance": self.deployment_guidance,
            "market_pe": self.market_pe,
            "vix": self.vix,
            "drawdown_from_peak": self.drawdown_from_peak,
            "distance_from_200ma": self.distance_from_200ma,
            "signals": self.signals,
            "checked_at": self.checked_at,
        }


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
                "debt_change": self.debt_change,
            },
            "summary": self.summary,
        }


# ─────────────────────────────────────────────────────────────
# Market Regime Classifier
# ─────────────────────────────────────────────────────────────


def classify_market_regime() -> MarketRegime:
    """
    Classify current market conditions into a regime.

    Uses multiple signals:
    - S&P 500 P/E ratio
    - VIX (fear index)
    - S&P 500 drawdown from 52-week high
    - S&P 500 distance from 200-day moving average

    Returns MarketRegime with regime, confidence, and deployment guidance.
    """
    signals = []
    regime_votes = []  # Each signal votes for a regime

    market_pe = _fetch_market_pe()
    vix = _fetch_vix()
    drawdown, distance_200ma = _fetch_spy_technicals()

    # Signal 1: Market P/E
    if market_pe is not None:
        if market_pe >= REGIME_THRESHOLDS["pe_euphoria"]:
            signals.append(f"Market P/E {market_pe:.1f} is in euphoria territory (>{REGIME_THRESHOLDS['pe_euphoria']})")
            regime_votes.append("euphoria")
        elif market_pe >= REGIME_THRESHOLDS["pe_overvalued"]:
            signals.append(f"Market P/E {market_pe:.1f} is above historical average")
            regime_votes.append("overvalued")
        elif market_pe >= REGIME_THRESHOLDS["pe_fair_low"]:
            signals.append(f"Market P/E {market_pe:.1f} is in fair value range")
            regime_votes.append("fair_value")
        else:
            signals.append(f"Market P/E {market_pe:.1f} is below historical average — cheap")
            regime_votes.append("correction")

    # Signal 2: VIX
    if vix is not None:
        if vix >= REGIME_THRESHOLDS["vix_extreme_fear"]:
            signals.append(f"VIX at {vix:.1f} — extreme fear (crisis indicator)")
            regime_votes.append("crisis")
        elif vix >= REGIME_THRESHOLDS["vix_fear"]:
            signals.append(f"VIX at {vix:.1f} — elevated fear")
            regime_votes.append("correction")
        elif vix <= REGIME_THRESHOLDS["vix_complacency"]:
            signals.append(f"VIX at {vix:.1f} — low volatility, possible complacency")
            regime_votes.append("euphoria")
        else:
            signals.append(f"VIX at {vix:.1f} — normal range")
            regime_votes.append("fair_value")

    # Signal 3: Drawdown from 52-week high
    if drawdown is not None:
        if drawdown <= REGIME_THRESHOLDS["drawdown_crisis"]:
            signals.append(f"S&P 500 drawdown {drawdown:.1%} from peak — bear market territory")
            regime_votes.append("crisis")
        elif drawdown <= REGIME_THRESHOLDS["drawdown_correction"]:
            signals.append(f"S&P 500 drawdown {drawdown:.1%} from peak — correction")
            regime_votes.append("correction")
        elif drawdown >= 0:
            signals.append(f"S&P 500 near 52-week high ({drawdown:+.1%})")
            regime_votes.append("overvalued")

    # Signal 4: Distance from 200-day MA
    if distance_200ma is not None:
        if distance_200ma >= REGIME_THRESHOLDS["ma200_euphoria"]:
            signals.append(f"S&P 500 is {distance_200ma:+.1%} above 200-day MA — extended")
            regime_votes.append("euphoria")
        elif distance_200ma <= REGIME_THRESHOLDS["ma200_correction"]:
            signals.append(f"S&P 500 is {distance_200ma:+.1%} below 200-day MA — weakness")
            regime_votes.append("correction")
        else:
            regime_votes.append("fair_value")

    # Determine regime by majority vote
    if not regime_votes:
        regime = "fair_value"
        confidence = "low"
    else:
        from collections import Counter
        vote_counts = Counter(regime_votes)
        regime, top_count = vote_counts.most_common(1)[0]
        total_votes = len(regime_votes)

        if top_count >= total_votes * 0.75:
            confidence = "high"
        elif top_count >= total_votes * 0.5:
            confidence = "moderate"
        else:
            confidence = "low"

    # Map regime to interpretation and deployment guidance
    regime_info = _get_regime_info(regime)

    # Map to legacy temperature for backward compatibility
    temp_map = {
        "euphoria": "HOT",
        "overvalued": "WARM",
        "fair_value": "COOL",
        "correction": "COOL",
        "crisis": "COLD",
    }

    return MarketRegime(
        regime=regime,
        confidence=confidence,
        interpretation=regime_info["interpretation"],
        deployment_guidance=regime_info["guidance"],
        market_pe=market_pe,
        vix=vix,
        drawdown_from_peak=drawdown,
        distance_from_200ma=distance_200ma,
        temperature=temp_map.get(regime, "UNKNOWN"),
        signals=signals,
        checked_at=datetime.now().isoformat(),
    )


def _get_regime_info(regime: str) -> dict:
    """Get human-readable interpretation and deployment guidance for a regime."""
    info = {
        "euphoria": {
            "interpretation": "Market in euphoria territory. Extreme valuations and complacency signals.",
            "guidance": "Patience. Quality watchlist building phase only. No new deployments.",
        },
        "overvalued": {
            "interpretation": "Market above historical averages but not extreme.",
            "guidance": "Selective deployment on Tier 1 picks only. Demand higher margin of safety.",
        },
        "fair_value": {
            "interpretation": "Market near fair value. Normal conditions.",
            "guidance": "Deploy on Tier 1 picks with standard margin of safety.",
        },
        "correction": {
            "interpretation": "Market correction underway. Opportunities developing.",
            "guidance": "Opportunity developing. Cross-reference Tier 2 watchlist for new Tier 1 entries.",
        },
        "crisis": {
            "interpretation": "Significant market decline. Fear elevated. Historical buying opportunity.",
            "guidance": "Deployment window open. Prioritize highest conviction Tier 1 picks with staged entry.",
        },
    }
    return info.get(regime, info["fair_value"])


def _fetch_market_pe() -> Optional[float]:
    """Fetch S&P 500 P/E ratio."""
    try:
        # Try VOO first (better P/E data), then SPY
        for ticker_symbol in ["VOO", "SPY"]:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            pe = info.get("trailingPE")
            if pe and pe > 0:
                return float(pe)
    except Exception as e:
        logger.debug(f"Error fetching market P/E: {e}")
    return None


def _fetch_vix() -> Optional[float]:
    """Fetch current VIX level."""
    try:
        vix = yf.Ticker("^VIX")
        info = vix.info
        price = info.get("regularMarketPrice") or info.get("previousClose")
        if price:
            return float(price)
    except Exception as e:
        logger.debug(f"Error fetching VIX: {e}")
    return None


def _fetch_spy_technicals() -> tuple[Optional[float], Optional[float]]:
    """
    Fetch S&P 500 technical indicators.

    Returns:
        (drawdown_from_peak, distance_from_200ma)
        Both as decimal fractions (e.g., -0.10 = 10% drawdown).
    """
    try:
        spy = yf.Ticker("SPY")
        hist = spy.history(period="1y")

        if hist.empty or len(hist) < 50:
            return None, None

        current_price = hist["Close"].iloc[-1]

        # Drawdown from 52-week high
        peak = hist["Close"].max()
        drawdown = (current_price - peak) / peak if peak > 0 else None

        # Distance from 200-day MA (use what we have, up to 1 year)
        ma_200 = hist["Close"].rolling(window=min(200, len(hist))).mean().iloc[-1]
        distance_200ma = (current_price - ma_200) / ma_200 if ma_200 > 0 else None

        return drawdown, distance_200ma

    except Exception as e:
        logger.debug(f"Error fetching SPY technicals: {e}")
        return None, None


# ─────────────────────────────────────────────────────────────
# Legacy API — backward compatible
# ─────────────────────────────────────────────────────────────


def get_market_temperature() -> dict:
    """
    Assess overall market valuation level.

    Returns dict with temperature, market_pe, interpretation for backward
    compatibility. Internally uses the new regime classifier.
    """
    regime = classify_market_regime()
    return {
        "temperature": regime.temperature,
        "market_pe": regime.market_pe,
        "interpretation": regime.interpretation,
        "checked_at": regime.checked_at,
        # v2 additions (backward compatible — old consumers ignore these)
        "regime": regime.regime,
        "regime_confidence": regime.confidence,
        "deployment_guidance": regime.deployment_guidance,
        "vix": regime.vix,
        "drawdown_from_peak": regime.drawdown_from_peak,
        "distance_from_200ma": regime.distance_from_200ma,
        "signals": regime.signals,
    }


# ─────────────────────────────────────────────────────────────
# Bubble Detector (individual stocks)
# ─────────────────────────────────────────────────────────────


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
                signals.append(f"No earnings (negative P/E) with ${market_cap / 1e9:.0f}B market cap")

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
            signals.append(f"High debt/equity of {debt_equity / 100:.1f}")

        # Signal 7: Price-to-Sales extreme
        if ps_ratio and ps_ratio > 20:
            signals.append(f"Price/Sales of {ps_ratio:.0f} - extremely speculative")

        # Signal 8: Price far above analyst target
        target_price = info.get("targetMeanPrice")
        if target_price and price > 0:
            if price > target_price * 1.3:  # 30% above target
                signals.append(
                    f"Price ${price:.0f} is {((price / target_price) - 1) * 100:.0f}% above analyst target ${target_price:.0f}"
                )

        if not signals:
            return None

        # Determine risk level
        risk_level = "HIGH" if len(signals) >= 3 else "MEDIUM"

        # Create summary
        summary = f"{len(signals)} warning signals detected. "
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
            summary=summary,
        )

    def _get_insider_activity(self, symbol: str) -> Optional[dict]:
        """Fetch insider trading activity from Finnhub"""

        if not self.finnhub_key:
            return None

        try:
            response = requests.get(
                "https://finnhub.io/api/v1/stock/insider-transactions",
                params={"symbol": symbol, "token": self.finnhub_key},
                timeout=10,
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
                        "summary": f"{sells} sells, {buys} buys recently",
                    }
        except Exception as e:
            logger.debug(f"Finnhub insider error for {symbol}: {e}")

        return None


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    logging.basicConfig(level=logging.INFO)

    # Test market regime
    print("=== Market Regime Classification ===")
    regime = classify_market_regime()
    print(f"Regime: {regime.regime} (confidence: {regime.confidence})")
    print(f"Temperature: {regime.temperature}")
    print(f"Interpretation: {regime.interpretation}")
    print(f"Deployment: {regime.deployment_guidance}")
    if regime.market_pe:
        print(f"Market P/E: {regime.market_pe:.1f}")
    if regime.vix:
        print(f"VIX: {regime.vix:.1f}")
    if regime.drawdown_from_peak is not None:
        print(f"Drawdown: {regime.drawdown_from_peak:.1%}")
    if regime.distance_from_200ma is not None:
        print(f"Distance from 200-day MA: {regime.distance_from_200ma:+.1%}")
    print("\nSignals:")
    for s in regime.signals:
        print(f"  - {s}")

    # Test legacy API
    print("\n=== Legacy API ===")
    temp = get_market_temperature()
    print(f"Temperature: {temp['temperature']}")

    # Test bubble detection
    print("\n=== Bubble Detection ===")
    detector = BubbleDetector()
    warnings = detector.scan_for_bubbles(["TSLA", "NVDA", "PLTR"])

    print(f"Bubble Warnings ({len(warnings)}):")
    for w in warnings:
        print(f"\n{w.symbol}: {w.risk_level} RISK")
        print(f"  Signals: {w.signal_count}")
        for signal in w.signals:
            print(f"    - {signal}")
