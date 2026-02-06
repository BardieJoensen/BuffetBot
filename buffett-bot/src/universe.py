"""
Stock Universe Module

Dynamically fetches stock candidates from multiple sources:
1. Finviz screener (primary) - broad US market
2. Wikipedia S&P 600 (fallback) - ~600 small caps
3. Curated list (last resort) - ~185 stocks

Results are cached for 7 days to avoid hammering external sources.
"""

import json
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# Default cache settings
_cache_dir = Path("data/cache")
UNIVERSE_CACHE_DAYS = 7

# Sector caps to ensure diversification
MAX_PER_SECTOR = 100


def set_cache_dir(path: Path):
    """Override the universe cache directory (e.g. for permission fallback)"""
    global _cache_dir
    _cache_dir = path


def _universe_cache_file() -> Path:
    return _cache_dir / "stock_universe.json"


def get_stock_universe(force_refresh: bool = False) -> list[str]:
    """
    Get list of stock tickers to screen.

    Tries sources in order:
    1. Cache (if fresh)
    2. Finviz screener
    3. Wikipedia S&P 600
    4. Curated fallback list

    Returns:
        List of ticker symbols
    """
    # Check cache first
    if not force_refresh:
        cached = _load_cached_universe()
        if cached:
            logger.info(f"Using cached universe ({len(cached)} stocks, {_cache_age_days():.1f} days old)")
            return cached

    # Try Finviz first (widest net)
    universe = _fetch_finviz_universe()

    if universe and len(universe) >= 50:
        logger.info(f"Finviz universe: {len(universe)} stocks")
        _save_universe_cache(universe, source="finviz")
        return universe

    # Fallback to Wikipedia S&P 600
    logger.warning("Finviz failed or returned too few results, trying Wikipedia S&P 600")
    universe = _fetch_sp600_from_wikipedia()

    if universe and len(universe) >= 50:
        logger.info(f"Wikipedia S&P 600 universe: {len(universe)} stocks")
        _save_universe_cache(universe, source="wikipedia")
        return universe

    # Last resort: curated list
    logger.warning("All dynamic sources failed, using curated fallback list")
    return _get_curated_fallback()


def _fetch_finviz_universe() -> Optional[list[str]]:
    """
    Fetch candidates from Finviz screener.

    Uses loose filters to cast a wide net - strict filtering
    happens locally with yfinance data.
    """
    try:
        from finvizfinance.screener.overview import Overview

        foverview = Overview()

        # Loose pre-filters (server-side)
        filters_dict = {
            'Market Cap.': '+Small (over $300mln)',  # $300M+
            'Price': 'Over $5',                       # No penny stocks
            'Average Volume': 'Over 100K',            # Liquid
            'P/E': 'Under 30',                        # Loose - strict locally
            'Country': 'USA',                         # US stocks only
        }

        foverview.set_filter(filters_dict=filters_dict)
        df = foverview.screener_view()

        if df is None or df.empty:
            logger.warning("Finviz returned empty results")
            return None

        # Extract tickers
        tickers = df['Ticker'].tolist()

        # Apply sector caps to ensure diversification
        if 'Sector' in df.columns:
            tickers = _apply_sector_caps(df)

        logger.info(f"Finviz returned {len(tickers)} stocks after sector caps")
        return tickers

    except ImportError:
        logger.warning("finvizfinance not installed, skipping Finviz source")
        return None
    except Exception as e:
        logger.warning(f"Finviz screener failed: {e}")
        return None


def _fetch_sp600_from_wikipedia() -> Optional[list[str]]:
    """
    Fetch S&P 600 Small Cap constituents from Wikipedia.

    This list is maintained and updates when the index rebalances.
    """
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"

        tables = pd.read_html(url)

        # The constituents table is usually the first one
        df = tables[0]

        # Find the ticker column
        ticker_col = None
        for col in ['Symbol', 'Ticker', 'Ticker symbol']:
            if col in df.columns:
                ticker_col = col
                break

        if ticker_col is None:
            logger.error(f"Could not find ticker column. Columns: {df.columns.tolist()}")
            return None

        tickers = df[ticker_col].tolist()

        # Clean up tickers (remove any NaN, whitespace)
        tickers = [str(t).strip() for t in tickers if pd.notna(t) and str(t).strip()]

        logger.info(f"Wikipedia S&P 600 returned {len(tickers)} stocks")
        return tickers

    except Exception as e:
        logger.warning(f"Wikipedia S&P 600 fetch failed: {e}")
        return None


def _apply_sector_caps(df: pd.DataFrame) -> list[str]:
    """
    Apply per-sector caps to ensure diversification.

    Without this, you might get 200 regional banks and 5 tech stocks.
    """
    capped_tickers = []
    sector_counts = {}

    for _, row in df.iterrows():
        sector = row.get('Sector', 'Unknown')
        ticker = row['Ticker']

        current_count = sector_counts.get(sector, 0)

        if current_count < MAX_PER_SECTOR:
            capped_tickers.append(ticker)
            sector_counts[sector] = current_count + 1

    logger.info(f"Sector distribution: {sector_counts}")

    return capped_tickers


def _get_curated_fallback() -> list[str]:
    """
    Fallback curated list - only used if dynamic sources fail.
    """
    return [
        # Technology - Semiconductors
        "LSCC", "DIOD", "SLAB", "POWI", "AOSL", "AMBA", "SITM", "CRUS", "FORM", "MTSI",
        "SMCI", "CRDO", "AEHR", "RMBS", "HIMX", "PLAB", "ICHR", "ACLS", "COHU", "KLIC",

        # Technology - Software
        "ALRM", "APPF", "BAND", "BRZE", "DOCN", "ESTC", "FSLY", "GTLB", "JAMF", "QLYS",
        "SMAR", "TENB", "NCNO", "EVBG",

        # Healthcare - Biotech/Medical Devices
        "ABCL", "ACAD", "ALKS", "ARVN", "AXSM", "BCRX", "BMRN", "EXAS", "GMED", "HOLX",
        "INCY", "INSM", "IONS", "JAZZ", "LGND", "MASI", "NVCR", "RARE", "SRPT", "UTHR",
        "VCYT", "XENE", "MEDP", "ITCI", "CORT", "HALO", "RVMD", "NBIX",

        # Industrials
        "AEIS", "AGCO", "ALG", "ASTE", "BWXT", "CMC", "ENS", "GGG", "GVA", "HUBB",
        "KBR", "LDOS", "MLI", "NVT", "PRIM", "RBC", "TRN", "VMI", "WCC", "WSC",
        "POWL", "ROAD", "STRL", "DY", "MTZ", "BLDR", "UFPI", "TREX", "ATKR", "GNRC",
        "AAON", "LECO", "MIDD",

        # Consumer - Retail/Restaurants
        "BJRI", "BOOT", "CAKE", "DIN", "EAT", "FIZZ", "HIBB", "PLAY", "PLNT", "SHAK",
        "TXRH", "WING", "LULU", "DECK", "CROX", "SKX", "DKS",

        # Financials
        "ALLY", "AX", "CADE", "EWBC", "FHN", "GBCI", "HBAN", "IBOC", "NWBI", "ONB",
        "PNFP", "SBCF", "SFBS", "SNV", "TFIN", "UBSI", "VLY", "WAL", "LPLA", "PIPR",
        "IBKR", "MKTX", "VIRT", "CACC", "SLM", "ENVA", "OMF", "LC", "UPST", "SOFI",

        # Energy & Materials
        "AROC", "BCPC", "CEIX", "CNX", "CTRA", "FANG", "HLX", "HP", "KOS", "MTDR",
        "OVV", "PARR", "RRC", "SM", "SWN", "CLF", "STLD", "NUE", "RS", "ATI", "AA",

        # REITs
        "AIRC", "BRX", "COLD", "CPT", "CUZ", "DEI", "EGP", "FR", "GTY", "HIW",
        "IIPR", "KRC", "LSI", "NNN", "OHI", "ROIC", "STAG", "SBRA", "VTR", "LTC",
    ]


# === Cache Management ===

def _load_cached_universe() -> Optional[list[str]]:
    """Load universe from cache if fresh enough"""
    cache_file = _universe_cache_file()

    if not cache_file.exists():
        return None

    try:
        data = json.loads(cache_file.read_text())

        cached_at = datetime.fromisoformat(data.get('cached_at', '2000-01-01'))
        age = datetime.now() - cached_at

        if age > timedelta(days=UNIVERSE_CACHE_DAYS):
            logger.info(f"Universe cache expired ({age.days} days old)")
            return None

        return data.get('tickers', [])

    except Exception as e:
        logger.warning(f"Failed to load universe cache: {e}")
        return None


def _save_universe_cache(tickers: list[str], source: str):
    """Save universe to cache"""
    try:
        _cache_dir.mkdir(parents=True, exist_ok=True)

        data = {
            'tickers': tickers,
            'source': source,
            'cached_at': datetime.now().isoformat(),
            'count': len(tickers)
        }

        _universe_cache_file().write_text(json.dumps(data, indent=2))
        logger.info(f"Cached {len(tickers)} tickers from {source}")

    except Exception as e:
        logger.warning(f"Failed to save universe cache: {e}")


def _cache_age_days() -> float:
    """Get age of cache in days"""
    cache_file = _universe_cache_file()

    if not cache_file.exists():
        return float('inf')

    try:
        data = json.loads(cache_file.read_text())
        cached_at = datetime.fromisoformat(data.get('cached_at', '2000-01-01'))
        return (datetime.now() - cached_at).total_seconds() / 86400
    except Exception:
        return float('inf')


def refresh_universe():
    """Force refresh the universe cache"""
    logger.info("Forcing universe refresh...")
    return get_stock_universe(force_refresh=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("Fetching stock universe...")
    universe = get_stock_universe(force_refresh=True)

    print(f"\nTotal stocks: {len(universe)}")
    print(f"First 20: {universe[:20]}")
    print(f"Last 20: {universe[-20:]}")
