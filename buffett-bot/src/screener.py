"""
Stock Screener Module

Filters stocks based on Buffett-style value criteria.
Returns candidates that pass quantitative filters for further analysis.

NOTE: Uses yfinance (completely free, no API key) for all stock data.
"""

import json
import time
import yaml
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timedelta
from pathlib import Path
import logging
import yfinance as yf

from src.universe import get_stock_universe, set_cache_dir as set_universe_cache_dir

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
    min_market_cap: float = 300_000_000      # $300M minimum
    max_market_cap: float = 500_000_000_000   # $500B maximum
    max_pe_ratio: float = 20.0               # Not overvalued
    max_debt_equity: float = 0.5             # Conservative debt
    min_roe: float = 0.12                    # 12% return on equity
    min_revenue_growth: float = 0.05         # 5% growth
    min_current_ratio: float = 1.5           # Can pay short-term debts
    min_price: float = 5.0                   # Minimum stock price
    scoring: dict[str, ScoringRule] = field(default_factory=dict)
    top_n: int = 100                         # How many top-scoring stocks to keep


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

        defaults = ScreeningCriteria()
        return ScreeningCriteria(
            min_market_cap=float(screening.get("min_market_cap", defaults.min_market_cap)),
            max_market_cap=float(screening.get("max_market_cap", defaults.max_market_cap)),
            max_pe_ratio=float(screening.get("max_pe_ratio", defaults.max_pe_ratio)),
            max_debt_equity=float(screening.get("max_debt_equity", defaults.max_debt_equity)),
            min_roe=float(screening.get("min_roe", defaults.min_roe)),
            min_revenue_growth=float(screening.get("min_revenue_growth", defaults.min_revenue_growth)),
            min_current_ratio=float(screening.get("min_current_ratio", defaults.min_current_ratio)),
            min_price=float(screening.get("min_price", defaults.min_price)),
            scoring=scoring,
            top_n=int(screening.get("top_n", defaults.top_n)),
        )

    except FileNotFoundError:
        logger.warning(f"Config file not found at {path}, using hardcoded defaults")
        return ScreeningCriteria()
    except Exception as e:
        logger.warning(f"Error parsing config file {path}: {e}, using hardcoded defaults")
        return ScreeningCriteria()


def score_stock(data: dict, criteria: ScreeningCriteria) -> float:
    """
    Score a stock based on how close each metric is to the ideal value.

    Each metric gets a 0-1 score, multiplied by its weight.
    Returns the total weighted score.
    """
    if not criteria.scoring:
        return 0.0

    total_score = 0.0

    valid_metrics = {"pe_ratio", "debt_equity", "roe", "revenue_growth", "current_ratio"}

    for metric_name, rule in criteria.scoring.items():
        if metric_name not in valid_metrics:
            continue

        value = data.get(metric_name)
        if value is None:
            continue

        # For debt_equity, yfinance returns as percentage - normalize
        if metric_name == "debt_equity" and value > 5:
            value = value / 100.0

        score = _compute_metric_score(value, rule)
        total_score += score * rule.weight

    return total_score


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
            "score": self.score
        }


class StockScreener:
    """
    Screens stocks using yfinance (completely free, no API key).

    Strategy:
    1. Get dynamic universe from Finviz/Wikipedia/fallback (see universe.py)
    2. Fetch detailed data via yfinance
    3. Apply hard filters (market cap, price, quote type, industry)
    4. Score remaining stocks and return top N
    """

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

        Hard filters: market cap, price, quote_type, industry.
        Scoring: all passing stocks are scored and sorted; top N returned.
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

            cached = self._get_cached_data(symbol)
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
            if any(term in industry for term in [
                "closed-end fund", "asset management", "shell companies",
                "exchange traded fund",
            ]):
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

            # === Score the stock ===
            stock_score = score_stock(data, criteria)

            de = data.get("debt_equity")

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
                price=price,
                score=stock_score
            ))

        logger.info(f"Processed {processed} stocks, {errors} errors")
        logger.info(f"After hard filters: {len(candidates)} candidates")

        # Sort by score descending and return top N
        if criteria.scoring:
            candidates.sort(key=lambda s: s.score, reverse=True)
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

    def apply_detailed_filters(
        self,
        candidates: list[ScreenedStock],
        criteria: ScreeningCriteria
    ) -> list[ScreenedStock]:
        """Kept for backward compatibility. Scoring in screen() handles ranking."""
        return candidates


def run_screen(apply_detailed: bool = False) -> list[ScreenedStock]:
    """
    Convenience function to run a full screen.

    Args:
        apply_detailed: If True, apply additional fundamental filters
    """
    screener = StockScreener()
    criteria = load_criteria_from_yaml()

    candidates = screener.screen(criteria)

    if apply_detailed and candidates:
        candidates = screener.apply_detailed_filters(candidates, criteria)

    return candidates


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    stocks = run_screen(apply_detailed=True)
    print(f"\nFound {len(stocks)} candidates:\n")

    for stock in stocks[:10]:
        print(f"  {stock.symbol}: {stock.name} (score: {stock.score:.2f})")
        print(f"    Market Cap: ${stock.market_cap:,.0f}")
        print(f"    Price: ${stock.price:.2f}")
        print(f"    P/E: {stock.pe_ratio}")
        print(f"    Sector: {stock.sector}")
        print()
