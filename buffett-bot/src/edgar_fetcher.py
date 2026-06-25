"""
SEC EDGAR 10-K Fetcher — Phase 2

Fetches real 10-K filing text (Item 1 Business / Item 1A Risk Factors /
Item 7 MD&A) from SEC EDGAR and folds it into the deep-analysis prompt's
`filing_text`. Previously `filing_text` only ever carried a yfinance-derived
numeric summary; now it carries actual filing prose, which is where the
qualitative moat/management/risk signal lives.

Only the deep-analysis tier (~6-10 stocks reaching Sonnet) should use this —
never the whole universe (SEC rate limits + token cost).

SEC fair-access policy requires a descriptive User-Agent containing a contact
email (config.edgar_user_agent / EDGAR_USER_AGENT). With it unset, every public
function degrades to a no-op so callers transparently keep their existing
summary — EDGAR never breaks a deep-analysis run.
"""

from __future__ import annotations

import html as html_lib
import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

from src.config import config

logger = logging.getLogger(__name__)

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
ARCHIVES_DOC_URL = "https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc}/{doc}"

_cache_dir = Path("data/edgar")

TICKER_MAP_TTL_DAYS = 30
FILING_TTL_DAYS = 90  # 10-Ks are annual

# Per-section cap stored in the cache (generous; the final prompt budget is
# applied later in augment_filing_text, which can re-trim without re-fetching).
_RAW_SECTION_CAP = 8000

# Default total prompt budget — matches CompanyAnalyzer.MAX_FILING_CHARS.
DEFAULT_MAX_CHARS = 15000
# Share of the available budget given to each section (Risk + MD&A carry the
# most qualitative signal, so they get the lion's share).
_SECTION_WEIGHTS = {"risk_factors": 0.40, "mda": 0.40, "business": 0.20}

_REQUEST_DELAY_S = 0.2  # courtesy throttle (SEC asks for <10 req/s)


def set_edgar_cache_dir(path: Path) -> None:
    """Override the EDGAR cache directory (used in tests)."""
    global _cache_dir
    _cache_dir = Path(path)


def is_enabled() -> bool:
    """EDGAR is usable only when a fair-access User-Agent is configured."""
    return bool(config.edgar_user_agent.strip())


def _headers() -> dict:
    return {"User-Agent": config.edgar_user_agent, "Accept-Encoding": "gzip, deflate"}


def _get(url: str) -> Optional[requests.Response]:
    """GET with the SEC fair-access UA and a courtesy throttle."""
    try:
        time.sleep(_REQUEST_DELAY_S)
        resp = requests.get(url, headers=_headers(), timeout=30)
        if resp.status_code == 200:
            return resp
        logger.warning(f"EDGAR GET {url} returned {resp.status_code}")
    except Exception as e:
        logger.warning(f"EDGAR GET {url} failed: {e}")
    return None


# ─── Ticker → CIK ──────────────────────────────────────────────────────────


def _ticker_map_path() -> Path:
    return _cache_dir / "company_tickers.json"


def _load_ticker_map() -> Optional[dict]:
    """Return {TICKER: zero-padded-CIK}, refreshing the cache if stale."""
    path = _ticker_map_path()
    fresh = False
    if path.exists():
        age_days = (datetime.now().timestamp() - path.stat().st_mtime) / 86400
        fresh = age_days < TICKER_MAP_TTL_DAYS

    raw = None
    if fresh:
        try:
            raw = json.loads(path.read_text())
        except Exception:
            raw = None

    if raw is None:
        resp = _get(COMPANY_TICKERS_URL)
        if resp is None:
            # Fall back to a stale cache if we have one.
            if path.exists():
                try:
                    raw = json.loads(path.read_text())
                except Exception:
                    return None
            else:
                return None
        else:
            raw = resp.json()
            try:
                _cache_dir.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(raw))
            except Exception as e:
                logger.warning(f"Could not cache company_tickers.json: {e}")

    # company_tickers.json is keyed by arbitrary index: {"0": {"cik_str", "ticker", "title"}}
    mapping = {}
    for entry in raw.values():
        ticker = str(entry.get("ticker", "")).upper()
        cik = entry.get("cik_str")
        if ticker and cik is not None:
            mapping[ticker] = f"{int(cik):010d}"
    return mapping


def get_cik(ticker: str) -> Optional[str]:
    """Return the zero-padded 10-digit CIK for a ticker, or None."""
    mapping = _load_ticker_map()
    if not mapping:
        return None
    return mapping.get(ticker.upper())


# ─── Latest 10-K location ──────────────────────────────────────────────────


def _latest_10k_doc(cik: str) -> Optional[tuple[str, str]]:
    """
    Return (accession_no_dashes, primary_document) for the most recent 10-K,
    or None. Reads the company submissions feed.
    """
    resp = _get(SUBMISSIONS_URL.format(cik=cik))
    if resp is None:
        return None
    try:
        recent = resp.json()["filings"]["recent"]
        forms = recent["form"]
        accns = recent["accessionNumber"]
        docs = recent["primaryDocument"]
    except (KeyError, ValueError) as e:
        logger.warning(f"Unexpected submissions shape for CIK {cik}: {e}")
        return None

    for form, accn, doc in zip(forms, accns, docs):
        if form == "10-K" and doc:
            return accn.replace("-", ""), doc
    return None


def _fetch_filing_html(cik: str, accession: str, doc: str) -> Optional[str]:
    url = ARCHIVES_DOC_URL.format(cik_int=int(cik), acc=accession, doc=doc)
    resp = _get(url)
    return resp.text if resp is not None else None


# ─── HTML → text → sections ────────────────────────────────────────────────


def _html_to_text(raw: str) -> str:
    """Strip a filing's HTML to whitespace-normalized plain text."""
    raw = re.sub(r"(?is)<(script|style).*?</\1>", " ", raw)
    raw = re.sub(r"(?s)<[^>]+>", " ", raw)
    raw = html_lib.unescape(raw)
    raw = re.sub(r"\s+", " ", raw)
    return raw.strip()


def _trim(text: str, max_chars: int) -> str:
    """Trim to max_chars on a word boundary, appending an ellipsis if cut."""
    text = text.strip()
    if len(text) <= max_chars:
        return text
    cut = text[:max_chars]
    sp = cut.rfind(" ")
    if sp > max_chars * 0.6:
        cut = cut[:sp]
    return cut.rstrip() + " …"


# Item header patterns (matched against lowercased, whitespace-collapsed text).
# End boundaries are anchored to their section *titles*, not just the bare item
# number — otherwise an inline cross-reference like "see Part II, Item 8" would
# be mistaken for the next section header and truncate the slice (real 10-Ks are
# full of such references, especially in MD&A).
_RE_BUSINESS = re.compile(r"item\s*1[\.\:\)]?\s+business\b")
_RE_RISK = re.compile(r"item\s*1a[\.\:\)]?\s+risk\s+factors\b")
_RE_ITEM_1B = re.compile(r"item\s*1b[\.\:\)]?\s+unresolved")
_RE_ITEM_2 = re.compile(r"item\s*2[\.\:\)]?\s+propert")
_RE_MDA = re.compile(r"item\s*7[\.\:\)]?\s+management")
_RE_ITEM_7A = re.compile(r"item\s*7a[\.\:\)]?\s+quantitative")
_RE_ITEM_8 = re.compile(r"item\s*8[\.\:\)]?\s+financial\s+statement")

# An end boundary closer than this to the section start is treated as a
# cross-reference, not the real next-section header.
_MIN_SECTION_CHARS = 800


def _slice_section(text_lower: str, text: str, start_re, end_res, cap: int) -> Optional[str]:
    """
    Slice the section beginning at the LAST match of start_re (the real section,
    not the table-of-contents entry that appears first) and ending at the first
    end-boundary that is far enough past it to be a genuine section break. Falls
    back to a fixed window if no boundary is found.
    """
    starts = list(start_re.finditer(text_lower))
    if not starts:
        return None
    start = starts[-1].start()

    end = None
    for end_re in end_res:
        m = end_re.search(text_lower, start + _MIN_SECTION_CHARS)
        if m and (end is None or m.start() < end):
            end = m.start()
    if end is None:
        end = min(start + cap * 2, len(text))

    section = text[start:end].strip()
    if len(section) < 200:
        return None
    return _trim(section, cap)


def _extract_sections(text: str) -> dict:
    """Extract Business / Risk Factors / MD&A from filing plain text."""
    low = text.lower()
    return {
        "business": _slice_section(low, text, _RE_BUSINESS, [_RE_RISK], _RAW_SECTION_CAP),
        "risk_factors": _slice_section(low, text, _RE_RISK, [_RE_ITEM_1B, _RE_ITEM_2], _RAW_SECTION_CAP),
        "mda": _slice_section(low, text, _RE_MDA, [_RE_ITEM_7A, _RE_ITEM_8], _RAW_SECTION_CAP),
    }


# ─── Public API ──────────────────────────────────────────────────────────────


def _filing_cache_path(ticker: str) -> Path:
    return _cache_dir / f"{ticker.upper()}_10k.json"


def fetch_10k_sections(ticker: str, *, use_cache: bool = True) -> Optional[dict]:
    """
    Return {"business", "risk_factors", "mda"} extracted from the ticker's most
    recent 10-K (values may be None per section), or None if EDGAR is disabled,
    the ticker has no 10-K (e.g. foreign filer), or the fetch fails.

    Results are cached per ticker for FILING_TTL_DAYS.
    """
    if not is_enabled():
        return None

    cache_path = _filing_cache_path(ticker)
    if use_cache and cache_path.exists():
        try:
            data = json.loads(cache_path.read_text())
            fetched = datetime.fromisoformat(data.get("fetched_at", "2000-01-01"))
            if (datetime.now() - fetched).days < FILING_TTL_DAYS:
                return data.get("sections")
        except Exception:
            pass

    cik = get_cik(ticker)
    if not cik:
        logger.info(f"EDGAR: no CIK for {ticker} (foreign filer or unlisted) — skipping")
        return None

    loc = _latest_10k_doc(cik)
    if not loc:
        logger.info(f"EDGAR: no 10-K found for {ticker} (CIK {cik})")
        return None

    accession, doc = loc
    html = _fetch_filing_html(cik, accession, doc)
    if not html:
        return None

    sections = _extract_sections(_html_to_text(html))
    if not any(sections.values()):
        logger.info(f"EDGAR: could not extract sections for {ticker} — unusual 10-K layout")
        return None

    try:
        _cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps(
                {
                    "ticker": ticker.upper(),
                    "cik": cik,
                    "accession": accession,
                    "fetched_at": datetime.now().isoformat(),
                    "sections": sections,
                }
            )
        )
    except Exception as e:
        logger.warning(f"Could not cache 10-K sections for {ticker}: {e}")

    return sections


def augment_filing_text(ticker: str, base_summary: str, max_chars: int = DEFAULT_MAX_CHARS) -> str:
    """
    Append real 10-K excerpts to a base (numeric) summary, budgeted to fit
    `max_chars` total. Returns `base_summary` unchanged if EDGAR is disabled or
    no filing text is available — so callers can use this unconditionally.
    """
    sections = None
    try:
        sections = fetch_10k_sections(ticker)
    except Exception as e:
        logger.warning(f"EDGAR augment failed for {ticker}: {e}")
    if not sections or not any(sections.values()):
        return base_summary

    header = "\n\n=== SEC 10-K EXCERPTS (most recent annual filing) ===\n"
    overhead = len(header) + 90  # section sub-headers
    available = max(0, max_chars - len(base_summary) - overhead)
    if available < 600:  # not enough room to add anything meaningful
        return base_summary

    labels = [
        ("business", "Item 1 — Business"),
        ("risk_factors", "Item 1A — Risk Factors"),
        ("mda", "Item 7 — Management's Discussion & Analysis"),
    ]
    parts = [base_summary, header.rstrip()]
    for key, label in labels:
        body = sections.get(key)
        if not body:
            continue
        budget = int(available * _SECTION_WEIGHTS[key])
        if budget < 200:
            continue
        parts.append(f"\n--- {label} ---")
        parts.append(_trim(body, budget))

    if len(parts) <= 2:  # nothing actually added
        return base_summary
    return "\n".join(parts)
