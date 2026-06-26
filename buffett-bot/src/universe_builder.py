"""
Universe Builder — Three-Pool Stock Universe

Builds the screened universe by merging three pools in priority order:

    Pool 1 — Conviction (source='conviction')
        Hand-curated wide-moat businesses from config/conviction_list.yaml.
        Always included regardless of quality score.
        Priority 0: analyzed first every run.

    Pool 2 — S&P 500 Large Cap (source='sp500_filter')
        All S&P 500 constituents fetched from Wikipedia.
        Cap category: 'large' ($10B+).
        Catches quality large caps not on the conviction list.

    Pool 3 — Finviz Small/Mid Discovery (source='finviz_screen')
        Broad market from Finviz screener: US, $300M+, liquid.
        Cap category: 'mid' or 'small'.

De-duplication priority: conviction > sp500_filter > finviz_screen.
A ticker on both the conviction list and S&P 500 keeps source='conviction'.

Usage:
    from src.universe_builder import build_universe
    stocks = build_universe(
        conviction_yaml=Path("config/conviction_list.yaml"),
        cache_dir=Path("data/cache"),
    )
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

# Market-cap boundaries
LARGE_CAP_THRESHOLD = 10_000_000_000  # $10B
MID_CAP_THRESHOLD = 2_000_000_000  # $2B

# Cache TTL for the S&P 500 constituent list
SP500_CACHE_DAYS = 7

# Source priority for de-duplication (lower number wins)
SOURCE_PRIORITY = {
    "conviction": 0,
    "sp500_filter": 1,
    "finviz_screen": 2,
}


# ─── Data Structures ──────────────────────────────────────────────────────


@dataclass
class UniverseStock:
    """A single stock entry in the merged universe."""

    ticker: str
    source: str  # 'conviction', 'sp500_filter', 'finviz_screen'
    notes: str = ""  # human notes from conviction list, empty for other pools

    def __eq__(self, other) -> bool:
        return isinstance(other, UniverseStock) and self.ticker == other.ticker

    def __hash__(self) -> int:
        return hash(self.ticker)


# ─── Cap Category ─────────────────────────────────────────────────────────


def get_cap_category(market_cap: Optional[float]) -> str:
    """
    Classify market cap into 'large', 'mid', or 'small'.

    Boundaries:
        large: >= $10B
        mid:   >= $2B  and < $10B
        small: >= $300M and < $2B   (anything below is micro-cap, filtered out)
    """
    if not market_cap or market_cap <= 0:
        return "unknown"
    if market_cap >= LARGE_CAP_THRESHOLD:
        return "large"
    if market_cap >= MID_CAP_THRESHOLD:
        return "mid"
    return "small"


# ─── Pool 1: Conviction List ──────────────────────────────────────────────


def _read_conviction_pool(yaml_path: Path) -> list[UniverseStock]:
    """
    Load conviction list from YAML and return as UniverseStock entries.

    Handles missing file gracefully — returns empty list with a warning.
    """
    if not yaml_path.exists():
        logger.warning("Conviction list not found at %s — skipping conviction pool", yaml_path)
        return []

    try:
        with open(yaml_path) as f:
            data = yaml.safe_load(f)
    except Exception as exc:
        logger.error("Failed to parse conviction list %s: %s", yaml_path, exc)
        return []

    stocks = []
    for entry in data.get("stocks", []):
        ticker = entry.get("ticker", "").strip().upper()
        if not ticker:
            continue
        stocks.append(
            UniverseStock(
                ticker=ticker,
                source="conviction",
                notes=entry.get("notes", ""),
            )
        )

    logger.info("Conviction pool: %d stocks loaded from %s", len(stocks), yaml_path)
    return stocks


# ─── Pool 2: S&P 500 Large Cap ────────────────────────────────────────────


def _fetch_sp500_pool(cache_dir: Path) -> list[UniverseStock]:
    """
    Fetch S&P 500 constituents from Wikipedia (cached 7 days).

    Returns all S&P 500 stocks tagged as source='sp500_filter'.
    Market cap filtering ($10B+) happens in the screener when yfinance
    data is fetched — we can't know the exact cap without an API call here.

    Falls back to a curated list of ~40 known S&P 500 large-cap quality
    businesses if Wikipedia is unreachable.
    """
    cache_file = cache_dir / "sp500_universe.json"

    # Try cache first
    cached = _load_sp500_cache(cache_file)
    if cached is not None:
        return cached

    # Try Wikipedia
    tickers = _fetch_sp500_from_wikipedia()
    if tickers and len(tickers) >= 100:
        stocks = [UniverseStock(ticker=t, source="sp500_filter") for t in tickers]
        _save_sp500_cache(cache_file, tickers)
        logger.info("S&P 500 pool: %d stocks from Wikipedia", len(stocks))
        return stocks

    # Fallback: curated list of wide-moat S&P 500 quality names
    logger.warning("Wikipedia S&P 500 fetch failed — using curated large-cap fallback")
    return _sp500_curated_fallback()


def _fetch_sp500_from_wikipedia() -> Optional[list[str]]:
    """Fetch S&P 500 constituent tickers from Wikipedia table."""
    try:
        import io

        import pandas as pd
        import requests

        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        headers = {"User-Agent": "BuffettBot/1.0 (https://github.com/BardieJoensen/BuffetBot; investment research bot)"}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        html = io.StringIO(resp.text)
        tables = pd.read_html(html, attrs={"id": "constituents"})
        if not tables:
            tables = pd.read_html(io.StringIO(resp.text))
        df = tables[0]

        # Find the ticker column — Wikipedia uses 'Symbol' or 'Ticker symbol'
        ticker_col = None
        for col in ["Symbol", "Ticker symbol", "Ticker"]:
            if col in df.columns:
                ticker_col = col
                break

        if ticker_col is None:
            logger.warning("Could not find ticker column in S&P 500 table. Columns: %s", df.columns.tolist())
            return None

        tickers = [
            str(t).strip().replace(".", "-")  # BRK.B → BRK-B for yfinance
            for t in df[ticker_col].tolist()
            if pd.notna(t) and str(t).strip()
        ]
        logger.info("Wikipedia S&P 500: %d tickers fetched", len(tickers))
        return tickers

    except ImportError:
        logger.warning("pandas not available for S&P 500 Wikipedia fetch")
        return None
    except Exception as exc:
        logger.warning("S&P 500 Wikipedia fetch failed: %s", exc)
        return None


def _sp500_curated_fallback() -> list[UniverseStock]:
    """
    Curated list of high-quality S&P 500 large caps (~105 names).
    Used only when Wikipedia is unreachable.
    Broad sector coverage so the fallback is a credible standalone source.
    """
    tickers = [
        # Mega-cap tech
        "MSFT",
        "AAPL",
        "NVDA",
        "GOOGL",
        "META",
        "AMZN",
        # Software / SaaS
        "ADBE",
        "CRM",
        "NOW",
        "WDAY",
        "ANSS",
        "CDNS",
        "MSCI",
        "INTU",
        # Semiconductors
        "AVGO",
        "TXN",
        "AMAT",
        "KLAC",
        "LRCX",
        "MCHP",
        # Healthcare — pharma / biotech
        "LLY",
        "JNJ",
        "MRK",
        "PFE",
        "ABBV",
        "AMGN",
        "GILD",
        "REGN",
        # Healthcare — devices / tools
        "ABT",
        "MDT",
        "BSX",
        "SYK",
        "EW",
        "IDXX",
        "DHR",
        "TMO",
        "A",
        "ZBH",
        "HOLX",
        # Consumer staples
        "PG",
        "KO",
        "PEP",
        "PM",
        "MO",
        "KMB",
        "WMT",
        "COST",
        "CL",
        "CHD",
        "CLX",
        "K",
        "GIS",
        "CAG",
        # Consumer discretionary
        "MCD",
        "SBUX",
        "NKE",
        "ORLY",
        "AZO",
        "BKNG",
        # Financials — banks / payments
        "JPM",
        "BAC",
        "WFC",
        "V",
        "MA",
        # Financials — data / exchanges
        "SPGI",
        "MCO",
        "ICE",
        "CME",
        "BLK",
        # Financials — insurance
        "CB",
        "TRV",
        "AFL",
        "PRU",
        "MET",
        "ALL",
        # Industrials — defence
        "LMT",
        "RTX",
        "NOC",
        "GD",
        # Industrials — diversified
        "HON",
        "MMM",
        "EMR",
        "ROK",
        "DOV",
        "PH",
        "FAST",
        "UPS",
        "CSX",
        "NSC",
        # Energy
        "XOM",
        "CVX",
        "COP",
        # Utilities
        "NEE",
        "AEP",
        "SO",
        # Materials
        "APD",
        "SHW",
        "ECL",
        "PPG",
        # Communication services
        "NFLX",
        "DIS",
        # REITs / Infrastructure
        "PLD",
        "AMT",
        "CCI",
        "EQIX",
    ]
    return [UniverseStock(ticker=t, source="sp500_filter") for t in tickers]


def _load_sp500_cache(cache_file: Path) -> Optional[list[UniverseStock]]:
    """Load S&P 500 universe from cache if fresh."""
    if not cache_file.exists():
        return None
    try:
        data = json.loads(cache_file.read_text())
        cached_at = datetime.fromisoformat(data["cached_at"])
        if datetime.now() - cached_at > timedelta(days=SP500_CACHE_DAYS):
            logger.info("S&P 500 cache expired — will refresh")
            return None
        tickers = data.get("tickers", [])
        logger.info("S&P 500 pool: %d stocks from cache (%s)", len(tickers), cached_at.strftime("%Y-%m-%d"))
        return [UniverseStock(ticker=t, source="sp500_filter") for t in tickers]
    except Exception as exc:
        logger.warning("Failed to load S&P 500 cache: %s", exc)
        return None


def _save_sp500_cache(cache_file: Path, tickers: list[str]) -> None:
    """Save S&P 500 tickers to cache."""
    try:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(
            json.dumps(
                {
                    "tickers": tickers,
                    "cached_at": datetime.now().isoformat(),
                    "count": len(tickers),
                    "source": "wikipedia",
                },
                indent=2,
            )
        )
    except Exception as exc:
        logger.warning("Failed to save S&P 500 cache: %s", exc)


# ─── Pool 3: Finviz Small/Mid ─────────────────────────────────────────────


def _fetch_finviz_pool(cache_dir: Path) -> list[UniverseStock]:
    """
    Fetch small/mid-cap candidates from Finviz (existing universe module).

    Reuses the existing `get_stock_universe()` which already has its own
    7-day cache and Wikipedia S&P 600 fallback.
    """
    try:
        from src.universe import get_stock_universe, set_cache_dir

        set_cache_dir(cache_dir)
        tickers = get_stock_universe()
        stocks = [UniverseStock(ticker=t, source="finviz_screen") for t in tickers]
        logger.info("Finviz pool: %d stocks from small/mid-cap screen", len(stocks))
        return stocks
    except Exception as exc:
        logger.warning("Finviz pool fetch failed: %s", exc)
        return []


# ─── Merger ───────────────────────────────────────────────────────────────


def _merge_pools(*pools: list[UniverseStock]) -> list[UniverseStock]:
    """
    Merge multiple pools, keeping the highest-priority source for each ticker.

    SOURCE_PRIORITY defines the winner when a ticker appears in multiple pools:
        conviction (0) > sp500_filter (1) > finviz_screen (2)
    """
    seen: dict[str, UniverseStock] = {}

    for pool in pools:
        for stock in pool:
            if stock.ticker not in seen:
                seen[stock.ticker] = stock
            else:
                existing = seen[stock.ticker]
                existing_priority = SOURCE_PRIORITY.get(existing.source, 99)
                new_priority = SOURCE_PRIORITY.get(stock.source, 99)
                if new_priority < existing_priority:
                    # New entry is higher priority — replace, but keep notes if upgrading
                    seen[stock.ticker] = UniverseStock(
                        ticker=stock.ticker,
                        source=stock.source,
                        notes=stock.notes or existing.notes,
                    )

    return list(seen.values())


# ─── Main Entry Point ─────────────────────────────────────────────────────


def build_universe(
    conviction_yaml: Path,
    cache_dir: Path,
    db=None,  # Optional[Database] — if provided, syncs conviction tickers into universe table
) -> list[UniverseStock]:
    """
    Build the merged three-pool universe.

    Returns stocks in a stable order:
        1. Conviction list (always first — highest priority)
        2. S&P 500 stocks not on the conviction list
        3. Finviz stocks not in either of the above

    If a Database instance is provided, syncs conviction tickers into the
    universe table so they're tracked for priority analysis scheduling.

    Args:
        conviction_yaml: Path to config/conviction_list.yaml
        cache_dir: Path to data/cache/ directory
        db: Optional Database instance for syncing

    Returns:
        list[UniverseStock] — all unique stocks with source tagged
    """
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Fetch all three pools
    conviction_pool = _read_conviction_pool(conviction_yaml)
    sp500_pool = _fetch_sp500_pool(cache_dir)
    finviz_pool = _fetch_finviz_pool(cache_dir)

    # Merge with priority: conviction > sp500 > finviz
    merged = _merge_pools(conviction_pool, sp500_pool, finviz_pool)

    # Stable ordering: conviction first (preserves priority intent)
    priority = {s.ticker: SOURCE_PRIORITY.get(s.source, 99) for s in merged}
    merged.sort(key=lambda s: (priority.get(s.ticker, 99), s.ticker))

    # Log pool breakdown
    source_counts: dict[str, int] = {}
    for s in merged:
        source_counts[s.source] = source_counts.get(s.source, 0) + 1
    logger.info(
        "Universe built: %d total stocks — %s",
        len(merged),
        ", ".join(f"{src}={n}" for src, n in sorted(source_counts.items())),
    )

    # Optionally sync conviction tickers into the database
    if db is not None:
        try:
            for stock in conviction_pool:
                db.upsert_universe_stock(stock.ticker, source="conviction")
            logger.info("Synced %d conviction tickers into database", len(conviction_pool))
        except Exception as exc:
            logger.warning("Failed to sync conviction tickers to database: %s", exc)

    return merged


def get_tickers(universe: list[UniverseStock]) -> list[str]:
    """Extract just the ticker symbols from a universe list."""
    return [s.ticker for s in universe]


def get_conviction_tickers(universe: list[UniverseStock]) -> list[str]:
    """Return only conviction-source tickers."""
    return [s.ticker for s in universe if s.source == "conviction"]
