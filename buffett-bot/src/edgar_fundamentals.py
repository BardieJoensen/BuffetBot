"""
Point-in-Time Fundamentals via SEC EDGAR companyfacts — Phase 2.5

Builds a *free* point-in-time historical fundamentals store from SEC EDGAR's
XBRL companyfacts API. Each fact carries its `filed` date — the moment it became
public — which is exactly what a look-ahead-free backtest needs. This lets
src/backtest.py score stocks on the numbers that were actually known at each
historical date, instead of today's (restated, hindsight-tainted) financials.

Extends Phase 2: reuses edgar_fetcher's ticker→CIK map, fair-access User-Agent,
and throttled GET. No new env var — set EDGAR_USER_AGENT (see edgar_fetcher).

Design:
- For each canonical field we consume, an ordered fallback list of XBRL tags;
  the first tag that resolves wins (companies tag the same concept differently).
- Point-in-time selection keeps the ORIGINALLY-FILED value per (concept, period,
  form): the earliest `filed` date, so a later restatement never leaks backward.

Limitations (scope, not blockers):
- XBRL coverage begins ~2009, so history is ~15 years, not decades.
- v1 studies the *current* universe's filings, so survivorship bias remains at
  the universe-selection level (delisted filers' facts persist in EDGAR, but
  reconstructing the true historical investable set is heavier future work).
- Foreign filers (20-F) and non-XBRL gaps resolve to "no data" — never fabricated.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from src import edgar_fetcher

logger = logging.getLogger(__name__)

COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
COMPANYFACTS_TTL_DAYS = 90

# Canonical field → ordered XBRL tags (first that resolves wins). This small,
# finite mapping is what makes the project tractable — we are not rebuilding a
# vendor's 200-field surface, just what the screener/valuation actually use.
CONCEPT_MAP: dict[str, list[str]] = {
    "revenue": [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet",
    ],
    "net_income": ["NetIncomeLoss"],
    "operating_income": ["OperatingIncomeLoss"],
    "ocf": [
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    ],
    "capex": [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
    ],
    "depreciation": [
        "DepreciationDepletionAndAmortization",
        "DepreciationAmortizationAndAccretionNet",
        "DepreciationAndAmortization",
    ],
    "sbc": ["ShareBasedCompensation"],
    "equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ],
    "long_term_debt": ["LongTermDebtNoncurrent", "LongTermDebt"],
    "short_term_debt": ["LongTermDebtCurrent", "DebtCurrent", "ShortTermBorrowings"],
    "shares_diluted": ["WeightedAverageNumberOfDilutedSharesOutstanding"],
    "shares_outstanding": ["CommonStockSharesOutstanding", "EntityCommonStockSharesOutstanding"],
    "eps_diluted": ["EarningsPerShareDiluted"],
    "total_assets": ["Assets"],
    "total_liabilities": ["Liabilities"],
}

# Expected XBRL unit per concept (companyfacts groups observations by unit).
_SHARE_CONCEPTS = {"shares_diluted", "shares_outstanding"}


def _expected_unit(concept: str) -> str:
    if concept in _SHARE_CONCEPTS:
        return "shares"
    if concept == "eps_diluted":
        return "USD/shares"
    return "USD"


def _companyfacts_cache_path(cik: str):
    return edgar_fetcher._cache_dir / f"companyfacts_{cik}.json"


def fetch_companyfacts(cik: str, *, use_cache: bool = True) -> Optional[dict]:
    """Fetch (and cache, 90d) the companyfacts JSON for a CIK, or None on failure."""
    if not edgar_fetcher.is_enabled():
        return None

    cache_path = _companyfacts_cache_path(cik)
    if use_cache and cache_path.exists():
        try:
            from datetime import datetime

            blob = json.loads(cache_path.read_text())
            fetched = datetime.fromisoformat(blob.get("_fetched_at", "2000-01-01"))
            if (datetime.now() - fetched).days < COMPANYFACTS_TTL_DAYS:
                return blob.get("facts_doc")
        except Exception:
            pass

    resp = edgar_fetcher._get(COMPANYFACTS_URL.format(cik=cik))
    if resp is None:
        return None
    try:
        doc = resp.json()
    except ValueError:
        return None

    try:
        from datetime import datetime

        edgar_fetcher._cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps({"_fetched_at": datetime.now().isoformat(), "facts_doc": doc}))
    except Exception as e:
        logger.warning(f"Could not cache companyfacts for CIK {cik}: {e}")
    return doc


def _observations_for_tag(facts_doc: dict, tag: str, unit: str) -> Optional[list]:
    """Return the raw observation list for an XBRL tag/unit, searching namespaces."""
    facts = facts_doc.get("facts", {})
    for namespace in ("us-gaap", "dei", "ifrs-full"):
        concept_obj = facts.get(namespace, {}).get(tag)
        if concept_obj:
            units = concept_obj.get("units", {})
            if unit in units:
                return units[unit]
    return None


def _originally_filed(observations: list) -> dict[tuple, dict]:
    """
    Reduce observations to the originally-filed value per (period_end, form):
    the one with the earliest `filed` date. Returns {(end, form): obs}.
    """
    best: dict[tuple, dict] = {}
    for obs in observations:
        end = obs.get("end")
        form = obs.get("form")
        filed = obs.get("filed")
        if not end or not filed or obs.get("val") is None:
            continue
        key = (end, form)
        if key not in best or filed < best[key].get("filed", "9999"):
            best[key] = obs
    return best


def build_pit_records(ticker: str, cik: str, facts_doc: dict) -> list[dict]:
    """
    Turn a companyfacts document into originally-filed pit_fundamentals rows for
    every canonical concept that resolves. Returns a list ready for
    Database.save_pit_fundamentals().
    """
    records: list[dict] = []
    for concept, tags in CONCEPT_MAP.items():
        unit = _expected_unit(concept)

        # Merge observations across ALL tags in the fallback list, not just the
        # first that resolves: companies migrate tags over time (e.g. Apple moved
        # Revenues → RevenueFromContractWithCustomerExcludingAssessedTax in 2019),
        # so any single tag yields a truncated series. Per (period, form) keep the
        # earliest-filed value across tags; earlier tags in the list win on ties.
        merged: dict[tuple, dict] = {}
        for tag in tags:
            obs_list = _observations_for_tag(facts_doc, tag, unit)
            if not obs_list:
                continue
            for key, obs in _originally_filed(obs_list).items():
                if key not in merged or obs.get("filed", "9999") < merged[key].get("filed", "9999"):
                    merged[key] = obs
        if not merged:
            logger.debug(f"{ticker}: no XBRL data for concept '{concept}'")
            continue

        for (end, form), obs in merged.items():
            records.append(
                {
                    "ticker": ticker.upper(),
                    "cik": cik,
                    "concept": concept,
                    "period_end": end,
                    "fiscal_year": obs.get("fy"),
                    "fiscal_period": obs.get("fp"),
                    "form": form,
                    "value": float(obs["val"]),
                    "filed_date": obs.get("filed"),
                    "accession": obs.get("accn"),
                }
            )
    return records


def load_ticker_fundamentals(ticker: str, db, *, use_cache: bool = True) -> int:
    """
    Fetch a ticker's companyfacts and persist its point-in-time fundamentals.
    Returns the number of rows inserted (0 if no CIK / no data / EDGAR disabled).
    """
    if not edgar_fetcher.is_enabled():
        logger.info("EDGAR disabled (no EDGAR_USER_AGENT) — skipping PIT fundamentals")
        return 0
    cik = edgar_fetcher.get_cik(ticker)
    if not cik:
        logger.info(f"PIT: no CIK for {ticker} (foreign filer or unlisted)")
        return 0
    facts_doc = fetch_companyfacts(cik, use_cache=use_cache)
    if not facts_doc:
        return 0
    records = build_pit_records(ticker, cik, facts_doc)
    return db.save_pit_fundamentals(records)


def load_universe_fundamentals(tickers: list[str], db, *, limit: Optional[int] = None) -> dict[str, int]:
    """
    Populate point-in-time fundamentals for many tickers. Returns {ticker: rows}.
    Per-company calls (with edgar_fetcher's built-in courtesy throttle) are fine
    for the current ~850-ticker universe; switch to the bulk companyfacts.zip
    only if the universe expands toward the whole market.
    """
    out: dict[str, int] = {}
    for ticker in tickers[: (limit if limit is not None else len(tickers))]:
        try:
            out[ticker] = load_ticker_fundamentals(ticker, db)
        except Exception as e:
            logger.warning(f"PIT load failed for {ticker}: {e}")
            out[ticker] = 0
    return out
