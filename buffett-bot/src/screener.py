"""
Stock Screener Module

Filters stocks based on Buffett-style quality criteria.
Returns candidates that pass quantitative filters for further analysis.

NOTE: Uses yfinance (completely free, no API key) for all stock data.

v2.0 — De-weighted valuation, added trend-based quality metrics from historical
financials, and sector-specific scoring overrides.
"""

import json
import logging
import random
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import yaml
import yfinance as yf

from src.universe import get_stock_universe
from src.universe import set_cache_dir as set_universe_cache_dir

logger = logging.getLogger(__name__)

# Cache directory for stock data
CACHE_DIR = Path(__file__).parent.parent / "data" / "cache"

# Default YAML config path
DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "config" / "screening_criteria.yaml"


@dataclass
class ScoringRule:
    """A single scored metric rule"""

    ideal: float
    weight: float = 1.0
    min: Optional[float] = None  # Zero score below this (for "higher is better" metrics)
    max: Optional[float] = None  # Zero score above this (for "lower is better" metrics)


@dataclass
class ScreeningCriteria:
    """Value investing screening criteria"""

    min_market_cap: float = 300_000_000  # $300M minimum
    max_market_cap: float = 500_000_000_000  # $500B maximum
    min_price: float = 5.0  # Minimum stock price
    scoring: dict[str, ScoringRule] = field(default_factory=dict)
    sector_overrides: dict[str, dict[str, ScoringRule]] = field(default_factory=dict)
    top_n: int = 100  # How many top-scoring stocks to keep


def load_criteria_from_yaml(config_path: Optional[Path] = None) -> ScreeningCriteria:
    """
    Load screening criteria from YAML config file.

    Falls back to hardcoded defaults if the file is missing or has parse errors.
    """
    path = config_path or DEFAULT_CONFIG_PATH

    try:
        with open(path) as f:
            config = yaml.safe_load(f)

        screening = config.get("screening", {})

        # Parse scoring rules
        scoring = {}
        scoring_raw = screening.get("scoring", {})
        for metric, rule_data in scoring_raw.items():
            scoring[metric] = ScoringRule(
                ideal=float(rule_data.get("ideal", 0)),
                weight=float(rule_data.get("weight", 1.0)),
                min=float(rule_data["min"]) if "min" in rule_data else None,
                max=float(rule_data["max"]) if "max" in rule_data else None,
            )

        # Parse sector overrides — each override inherits from base scoring
        sector_overrides: dict[str, dict[str, ScoringRule]] = {}
        overrides_raw = screening.get("sector_overrides", {})
        for sector_name, sector_rules in overrides_raw.items():
            sector_scoring = {}
            for metric, rule_data in sector_rules.items():
                sector_scoring[metric] = ScoringRule(
                    ideal=float(rule_data.get("ideal", 0)),
                    weight=float(rule_data.get("weight", 1.0)),
                    min=float(rule_data["min"]) if "min" in rule_data else None,
                    max=float(rule_data["max"]) if "max" in rule_data else None,
                )
            sector_overrides[sector_name] = sector_scoring

        defaults = ScreeningCriteria()
        return ScreeningCriteria(
            min_market_cap=float(screening.get("min_market_cap", defaults.min_market_cap)),
            max_market_cap=float(screening.get("max_market_cap", defaults.max_market_cap)),
            min_price=float(screening.get("min_price", defaults.min_price)),
            scoring=scoring,
            sector_overrides=sector_overrides,
            top_n=int(screening.get("top_n", defaults.top_n)),
        )

    except FileNotFoundError:
        logger.warning(f"Config file not found at {path}, using hardcoded defaults")
        return ScreeningCriteria()
    except Exception as e:
        logger.warning(f"Error parsing config file {path}: {e}, using hardcoded defaults")
        return ScreeningCriteria()


def score_stock(data: dict, criteria: ScreeningCriteria, sector: str = "") -> tuple[float, float]:
    """
    Score a stock based on how close each metric is to the ideal value.

    Each metric gets a 0-1 score, multiplied by its weight.
    Returns (total_score, score_confidence) where confidence =
    scored_weight / total_possible_weight.
    """
    if not criteria.scoring:
        return 0.0, 0.0

    # Merge base scoring with sector overrides
    effective_scoring = dict(criteria.scoring)
    if sector and sector in criteria.sector_overrides:
        for metric, rule in criteria.sector_overrides[sector].items():
            effective_scoring[metric] = rule

    total_score = 0.0
    scored_weight = 0.0
    total_possible_weight = 0.0

    valid_metrics = {
        "pe_ratio",
        "debt_equity",
        "roe",
        "revenue_growth",
        "current_ratio",
        "fcf_yield",
        "earnings_quality",
        "payout_ratio",
        "operating_margin",
        "roe_consistency",
        "roic",
        "margin_stability",
        "earnings_consistency",
        "revenue_cagr",
        "fcf_consistency",
    }

    for metric_name, rule in effective_scoring.items():
        if metric_name not in valid_metrics:
            continue

        total_possible_weight += rule.weight

        value = data.get(metric_name)
        if value is None:
            continue

        # For debt_equity, yfinance returns as percentage - normalize
        if metric_name == "debt_equity" and value > 5:
            value = value / 100.0

        score = _compute_metric_score(value, rule)
        total_score += score * rule.weight
        scored_weight += rule.weight

    confidence = scored_weight / total_possible_weight if total_possible_weight > 0 else 0.0
    return total_score, confidence


def _compute_metric_score(value: float, rule: ScoringRule) -> float:
    """
    Compute 0-1 score for a single metric.

    - At ideal value: score = 1.0
    - At min/max boundary: score = 0.0
    - Linear interpolation between
    """
    # "Lower is better" metrics (have a max, e.g., PE, debt)
    if rule.max is not None and rule.min is None:
        if value <= rule.ideal:
            return 1.0
        if value >= rule.max:
            return 0.0
        # Linear decay from ideal to max
        return 1.0 - (value - rule.ideal) / (rule.max - rule.ideal)

    # "Higher is better" metrics (have a min, e.g., ROE, growth)
    if rule.min is not None and rule.max is None:
        if value >= rule.ideal:
            return 1.0
        if value <= rule.min:
            return 0.0
        # Linear rise from min to ideal
        return (value - rule.min) / (rule.ideal - rule.min)

    # Metrics with both min and max (e.g., current_ratio)
    if rule.min is not None and rule.max is not None:
        if value >= rule.ideal:
            return 1.0 if value <= rule.max else max(0.0, 1.0 - (value - rule.max) / rule.max)
        if value <= rule.min:
            return 0.0
        return (value - rule.min) / (rule.ideal - rule.min)

    # No boundaries defined, just check if at ideal
    return 1.0 if value == rule.ideal else 0.5


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
    score: float = 0.0
    score_confidence: float = 0.0
    fcf_yield: Optional[float] = None
    earnings_quality: Optional[float] = None
    payout_ratio: Optional[float] = None
    operating_margin: Optional[float] = None
    roe_consistency: Optional[float] = None
    roic: Optional[float] = None
    margin_stability: Optional[float] = None
    earnings_consistency: Optional[float] = None
    revenue_cagr: Optional[float] = None
    fcf_consistency: Optional[float] = None
    current_ratio: Optional[float] = None

    @property
    def effective_score(self) -> float:
        """Score weighted by data confidence — used for tiebreaking."""
        return self.score * self.score_confidence

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
            "price": self.price,
            "score": self.score,
            "score_confidence": self.score_confidence,
            "fcf_yield": self.fcf_yield,
            "earnings_quality": self.earnings_quality,
            "payout_ratio": self.payout_ratio,
            "operating_margin": self.operating_margin,
            "roe_consistency": self.roe_consistency,
            "roic": self.roic,
            "margin_stability": self.margin_stability,
            "earnings_consistency": self.earnings_consistency,
            "revenue_cagr": self.revenue_cagr,
            "fcf_consistency": self.fcf_consistency,
            "current_ratio": self.current_ratio,
        }


class StockScreener:
    """
    Screens stocks using yfinance (completely free, no API key).

    Strategy:
    1. Get dynamic universe from Finviz/Wikipedia/fallback (see universe.py)
    2. Fetch detailed data via yfinance
    3. Apply hard filters (market cap, price, quote type, industry)
    4. Fetch historical financials for trend metrics
    5. Score remaining stocks (with sector overrides) and return top N
    """

    CACHE_HOURS = 24  # Cache stock data for 24 hours

    def __init__(self):
        self.cache_dir = CACHE_DIR
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            self.cache_dir = Path(tempfile.gettempdir()) / "buffett-bot-cache"
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
        Fetch stock data from yfinance (network call, no cache check).
        Returns dict with price, market cap, P/E, sector, etc.
        """
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
                "free_cashflow": info.get("freeCashflow"),
                "operating_cashflow": info.get("operatingCashflow"),
                "net_income": info.get("netIncomeToCommon"),
                "payout_ratio": info.get("payoutRatio"),
                "operating_margin": info.get("operatingMargins"),
            }

            # Derived metrics
            free_cashflow = data.get("free_cashflow")
            net_income = data.get("net_income")

            market_cap = data.get("market_cap")
            if free_cashflow and market_cap:
                data["fcf_yield"] = free_cashflow / market_cap
            else:
                data["fcf_yield"] = None

            if free_cashflow and net_income and net_income > 0:
                data["earnings_quality"] = free_cashflow / net_income
            else:
                data["earnings_quality"] = None

            # Cache the data
            self._save_cached_data(symbol, data)

            return data

        except Exception as e:
            logger.debug(f"Error fetching {symbol}: {e}")
            return None

    def _fetch_historical_data(self, symbol: str, ticker: yf.Ticker) -> dict:
        """
        Fetch historical financials and compute trend-based quality metrics.

        Uses ticker.financials, ticker.balance_sheet, ticker.cashflow.
        Each metric is wrapped in try/except for graceful degradation —
        if yfinance labels differ across versions, metrics degrade individually.

        Returns dict with metric values and _historical_years count.
        """
        result: dict = {"_historical_years": 0}

        try:
            financials = ticker.financials
            balance_sheet = ticker.balance_sheet
            cashflow = ticker.cashflow
        except Exception as e:
            logger.debug(f"Failed to fetch historical data for {symbol}: {e}")
            return result

        # Check we have data
        if financials is None or financials.empty:
            return result

        n_years = len(financials.columns)
        result["_historical_years"] = n_years

        if n_years < 2:
            return result

        # --- ROE Consistency: std(Net Income / Stockholders Equity) ---
        try:
            net_income_row = None
            for label in ["Net Income", "Net Income Common Stockholders"]:
                if label in financials.index:
                    net_income_row = financials.loc[label]
                    break

            equity_row = None
            if balance_sheet is not None and not balance_sheet.empty:
                for label in [
                    "Stockholders Equity",
                    "Total Stockholder Equity",
                    "Stockholders' Equity",
                    "Common Stock Equity",
                ]:
                    if label in balance_sheet.index:
                        equity_row = balance_sheet.loc[label]
                        break

            if net_income_row is not None and equity_row is not None:
                # Align columns
                common_cols = net_income_row.index.intersection(equity_row.index)
                if len(common_cols) >= 2:
                    ni = net_income_row[common_cols].astype(float)
                    eq = equity_row[common_cols].astype(float)
                    roe_series = ni / eq
                    roe_series = roe_series.replace([np.inf, -np.inf], np.nan).dropna()
                    if len(roe_series) >= 2:
                        result["roe_consistency"] = float(roe_series.std())
        except Exception as e:
            logger.debug(f"{symbol} roe_consistency failed: {e}")

        # --- ROIC: Net Income / (Stockholders Equity + Total Debt) for latest year ---
        try:
            ni_val = None
            for label in ["Net Income", "Net Income Common Stockholders"]:
                if label in financials.index:
                    ni_val = float(financials.loc[label].iloc[0])
                    break

            eq_val = None
            if balance_sheet is not None and not balance_sheet.empty:
                for label in [
                    "Stockholders Equity",
                    "Total Stockholder Equity",
                    "Stockholders' Equity",
                    "Common Stock Equity",
                ]:
                    if label in balance_sheet.index:
                        eq_val = float(balance_sheet.loc[label].iloc[0])
                        break

            debt_val = 0.0
            if balance_sheet is not None and not balance_sheet.empty:
                for label in ["Total Debt", "Long Term Debt", "Long Term Debt And Capital Lease Obligation"]:
                    if label in balance_sheet.index:
                        debt_val = float(balance_sheet.loc[label].iloc[0])
                        break

            if ni_val is not None and eq_val is not None:
                invested_capital = eq_val + debt_val
                if invested_capital > 0:
                    result["roic"] = ni_val / invested_capital
        except Exception as e:
            logger.debug(f"{symbol} roic failed: {e}")

        # --- Margin Stability: std(Operating Income / Total Revenue) ---
        try:
            op_income_row = None
            for label in ["Operating Income", "Operating Revenue"]:
                if label in financials.index:
                    op_income_row = financials.loc[label]
                    break

            revenue_row = None
            for label in ["Total Revenue", "Revenue"]:
                if label in financials.index:
                    revenue_row = financials.loc[label]
                    break

            if op_income_row is not None and revenue_row is not None:
                common_cols = op_income_row.index.intersection(revenue_row.index)
                if len(common_cols) >= 2:
                    op = op_income_row[common_cols].astype(float)
                    rev = revenue_row[common_cols].astype(float)
                    margin_series = op / rev
                    margin_series = margin_series.replace([np.inf, -np.inf], np.nan).dropna()
                    if len(margin_series) >= 2:
                        result["margin_stability"] = float(margin_series.std())
        except Exception as e:
            logger.debug(f"{symbol} margin_stability failed: {e}")

        # --- Earnings Consistency: count of years with positive earnings growth ---
        try:
            ni_row = None
            for label in ["Net Income", "Net Income Common Stockholders"]:
                if label in financials.index:
                    ni_row = financials.loc[label]
                    break

            if ni_row is not None and len(ni_row) >= 2:
                ni_vals = ni_row.astype(float).values
                # financials columns are newest-first, reverse for chronological
                ni_vals = ni_vals[::-1]
                growth_years = sum(1 for i in range(1, len(ni_vals)) if ni_vals[i] > ni_vals[i - 1])
                result["earnings_consistency"] = float(growth_years)
        except Exception as e:
            logger.debug(f"{symbol} earnings_consistency failed: {e}")

        # --- Revenue CAGR: (latest_rev / earliest_rev) ^ (1/years) - 1 ---
        try:
            rev_row = None
            for label in ["Total Revenue", "Revenue"]:
                if label in financials.index:
                    rev_row = financials.loc[label]
                    break

            if rev_row is not None and len(rev_row) >= 2:
                rev_vals = rev_row.astype(float).values
                # newest first — iloc[0] is latest, iloc[-1] is earliest
                latest_rev = rev_vals[0]
                earliest_rev = rev_vals[-1]
                years = len(rev_vals) - 1
                if earliest_rev > 0 and latest_rev > 0 and years > 0:
                    result["revenue_cagr"] = float((latest_rev / earliest_rev) ** (1.0 / years) - 1.0)
        except Exception as e:
            logger.debug(f"{symbol} revenue_cagr failed: {e}")

        # --- FCF Consistency: std(Free Cash Flow / Net Income) ---
        try:
            fcf_row = None
            if cashflow is not None and not cashflow.empty:
                for label in ["Free Cash Flow", "FreeCashFlow"]:
                    if label in cashflow.index:
                        fcf_row = cashflow.loc[label]
                        break

            ni_row2 = None
            for label in ["Net Income", "Net Income Common Stockholders"]:
                if label in financials.index:
                    ni_row2 = financials.loc[label]
                    break

            if fcf_row is not None and ni_row2 is not None:
                common_cols = fcf_row.index.intersection(ni_row2.index)
                if len(common_cols) >= 2:
                    fcf = fcf_row[common_cols].astype(float)
                    ni = ni_row2[common_cols].astype(float)
                    # Only where net income is positive
                    mask = ni > 0
                    if mask.sum() >= 2:
                        ratio = fcf[mask] / ni[mask]
                        ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()
                        if len(ratio) >= 2:
                            result["fcf_consistency"] = float(ratio.std())
        except Exception as e:
            logger.debug(f"{symbol} fcf_consistency failed: {e}")

        return result

    def screen(self, criteria: Optional[ScreeningCriteria] = None) -> list[ScreenedStock]:
        """
        Run stock screen with given criteria using yfinance.

        Hard filters: market cap, price, quote_type, industry.
        Scoring: all passing stocks are scored and sorted; top N returned.
        """
        criteria = criteria or ScreeningCriteria()

        universe = get_stock_universe()

        logger.info(
            f"Running screen with criteria: market_cap={criteria.min_market_cap:,.0f}-{criteria.max_market_cap:,.0f}"
        )
        logger.info(f"Stock universe: {len(universe)} stocks")
        logger.info("Fetching data from yfinance (free, no API key)...")

        candidates = []
        processed = 0
        errors = 0

        for symbol in universe:
            processed += 1

            if processed % 20 == 0:
                logger.info(f"Progress: {processed}/{len(universe)} stocks processed...")

            cached = self._get_cached_data(symbol)
            data: Optional[dict] = None
            if cached:
                data = cached
            else:
                data = self._fetch_stock_data(symbol)
                # Small delay to be respectful to Yahoo Finance
                time.sleep(0.1)

            if data is None:
                errors += 1
                continue

            # === Hard filters (must pass) ===

            # Skip non-equity securities (closed-end funds, ETFs, etc.)
            quote_type = data.get("quote_type", "EQUITY")
            if quote_type != "EQUITY":
                continue

            # Skip closed-end funds and asset management vehicles
            industry = (data.get("industry") or "").lower()
            if any(
                term in industry
                for term in [
                    "closed-end fund",
                    "asset management",
                    "shell companies",
                    "exchange traded fund",
                ]
            ):
                continue

            # Market cap filter
            market_cap = data.get("market_cap", 0) or 0
            if market_cap < criteria.min_market_cap or market_cap > criteria.max_market_cap:
                continue

            # Minimum price filter
            price = data.get("price", 0)
            if not price or price < criteria.min_price:
                continue

            # Negative P/E means losses — skip
            pe = data.get("pe_ratio")
            if pe is not None and pe <= 0:
                continue

            # === Fetch historical data for trend metrics ===
            if "historical" not in data:
                try:
                    ticker = yf.Ticker(symbol)
                    historical = self._fetch_historical_data(symbol, ticker)
                    data["historical"] = historical
                    # Save enriched data back to cache
                    self._save_cached_data(symbol, data)
                except Exception as e:
                    logger.debug(f"Historical data fetch failed for {symbol}: {e}")
                    data["historical"] = {"_historical_years": 0}

            # Merge historical metrics into flat data dict for scoring
            historical = data.get("historical", {})
            for metric in [
                "roe_consistency",
                "roic",
                "margin_stability",
                "earnings_consistency",
                "revenue_cagr",
                "fcf_consistency",
            ]:
                if metric not in data and metric in historical:
                    data[metric] = historical[metric]

            # === Score the stock ===
            sector = data.get("sector", "Unknown")
            stock_score, score_confidence = score_stock(data, criteria, sector)

            de = data.get("debt_equity")

            candidates.append(
                ScreenedStock(
                    symbol=symbol,
                    name=data.get("name", symbol),
                    market_cap=market_cap,
                    pe_ratio=pe,
                    debt_equity=de / 100 if de else None,  # Convert to ratio
                    roe=data.get("roe"),
                    revenue_growth=data.get("revenue_growth"),
                    sector=sector,
                    industry=data.get("industry", "Unknown"),
                    screened_at=datetime.now(),
                    price=price,
                    score=stock_score,
                    score_confidence=score_confidence,
                    fcf_yield=data.get("fcf_yield"),
                    earnings_quality=data.get("earnings_quality"),
                    payout_ratio=data.get("payout_ratio"),
                    operating_margin=data.get("operating_margin"),
                    roe_consistency=data.get("roe_consistency"),
                    roic=data.get("roic"),
                    margin_stability=data.get("margin_stability"),
                    earnings_consistency=data.get("earnings_consistency"),
                    revenue_cagr=data.get("revenue_cagr"),
                    fcf_consistency=data.get("fcf_consistency"),
                    current_ratio=data.get("current_ratio"),
                )
            )

        logger.info(f"Processed {processed} stocks, {errors} errors")
        logger.info(f"After hard filters: {len(candidates)} candidates")

        # Sort by effective score (score × confidence), banded to 0.5 precision.
        # Within bands: prefer larger market cap, then randomize to avoid
        # alphabetical bias from stable sort preserving Finviz order.
        if criteria.scoring:
            seed = random.randint(0, 2**31)  # nosec B311 — tiebreaker, not security
            logger.info(f"Sort tiebreaker seed: {seed} (for reproducibility)")
            rng = random.Random(seed)  # nosec B311
            candidates.sort(
                key=lambda s: (
                    -(round(s.effective_score * 2) / 2),
                    -s.market_cap,
                    rng.random(),
                )
            )
            top_n = criteria.top_n
            if len(candidates) > top_n:
                logger.info(f"Keeping top {top_n} by score (from {len(candidates)})")
                candidates = candidates[:top_n]

        logger.info(f"Returning {len(candidates)} candidates")

        return candidates

    def get_detailed_metrics(self, symbol: str) -> dict:
        """
        Fetch detailed metrics for a single stock.
        Uses yfinance which provides comprehensive data.
        """
        data = self._get_cached_data(symbol) or self._fetch_stock_data(symbol)
        return data or {}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    screener = StockScreener()
    criteria = load_criteria_from_yaml()
    stocks = screener.screen(criteria)
    print(f"\nFound {len(stocks)} candidates:\n")

    for stock in stocks[:10]:
        print(f"  {stock.symbol}: {stock.name} (score: {stock.score:.2f}, confidence: {stock.score_confidence:.2f})")
        print(f"    Market Cap: ${stock.market_cap:,.0f}")
        print(f"    Price: ${stock.price:.2f}")
        print(f"    P/E: {stock.pe_ratio}")
        print(f"    Sector: {stock.sector}")
        if stock.roic is not None:
            print(f"    ROIC: {stock.roic:.1%}")
        if stock.revenue_cagr is not None:
            print(f"    Revenue CAGR: {stock.revenue_cagr:.1%}")
        print()
