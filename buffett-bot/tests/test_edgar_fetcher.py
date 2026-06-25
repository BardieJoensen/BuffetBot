"""
Tests for src/edgar_fetcher.py — SEC EDGAR 10-K ingestion (Phase 2).

The HTTP layer (`_get`) and the configured User-Agent (`config`) are mocked, so
these tests never touch the network. Covers CIK lookup, 10-K location, HTML→text,
section extraction (including skipping the table-of-contents entries), the prompt
budget in augment_filing_text, and graceful degradation when EDGAR is disabled.
"""

from types import SimpleNamespace

import pytest

import src.edgar_fetcher as ef

# A synthetic 10-K: a table of contents (item headers appear FIRST here) followed
# by the real sections. The extractor must pick the real sections, not the TOC.
_BUSINESS_BODY = "We design and sell wonderful widgets with a durable cost advantage. " * 20
_RISK_BODY = "Our business faces competition, regulation, and supply concentration risks. " * 20
_MDA_BODY = "Revenue grew on volume and pricing while margins expanded year over year. " * 20

_SAMPLE_10K_HTML = f"""
<html><head><style>.x{{color:red}}</style></head><body>
<p>TABLE OF CONTENTS</p>
<p>Item 1. Business .............. 4</p>
<p>Item 1A. Risk Factors ......... 12</p>
<p>Item 7. Management's Discussion and Analysis .... 40</p>
<div>&nbsp;page break&nbsp;</div>
<h2>Item 1. Business</h2>
<p>{_BUSINESS_BODY}</p>
<h2>Item 1A. Risk Factors</h2>
<p>{_RISK_BODY}</p>
<h2>Item 1B. Unresolved Staff Comments</h2>
<p>None.</p>
<h2>Item 2. Properties</h2>
<p>We lease offices.</p>
<h2>Item 7. Management's Discussion and Analysis of Financial Condition</h2>
<p>{_MDA_BODY}</p>
<h2>Item 7A. Quantitative and Qualitative Disclosures</h2>
<p>Interest rate risk.</p>
<h2>Item 8. Financial Statements</h2>
</body></html>
"""

_COMPANY_TICKERS = {
    "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
    "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft Corp"},
}

_SUBMISSIONS = {
    "filings": {
        "recent": {
            "form": ["8-K", "10-K", "10-Q"],
            "accessionNumber": ["0000-x", "0000320193-24-000123", "0000-y"],
            "primaryDocument": ["a.htm", "aapl-10k.htm", "q.htm"],
        }
    }
}


class _FakeResp:
    def __init__(self, *, json_data=None, text=""):
        self.status_code = 200
        self._json = json_data
        self.text = text

    def json(self):
        return self._json


@pytest.fixture
def enabled(monkeypatch, tmp_path):
    """Enable EDGAR with a fake UA and an isolated cache dir."""
    monkeypatch.setattr(ef, "config", SimpleNamespace(edgar_user_agent="BuffettBot test@example.com"))
    ef.set_edgar_cache_dir(tmp_path / "edgar")
    yield


def _route(url):
    """Map a URL to a canned response for the mocked _get."""
    if "company_tickers.json" in url:
        return _FakeResp(json_data=_COMPANY_TICKERS)
    if "/submissions/" in url:
        return _FakeResp(json_data=_SUBMISSIONS)
    if "/Archives/" in url:
        return _FakeResp(text=_SAMPLE_10K_HTML)
    return None


# ─── enablement ──────────────────────────────────────────────────────────────


def test_disabled_by_default(monkeypatch, tmp_path):
    monkeypatch.setattr(ef, "config", SimpleNamespace(edgar_user_agent=""))
    assert ef.is_enabled() is False
    assert ef.fetch_10k_sections("AAPL") is None
    # augment is a transparent no-op when disabled
    assert ef.augment_filing_text("AAPL", "BASE SUMMARY") == "BASE SUMMARY"


def test_enabled_flag(enabled):
    assert ef.is_enabled() is True


# ─── HTML → text, sections, trim ──────────────────────────────────────────────


def test_html_to_text_strips_tags_and_entities():
    txt = ef._html_to_text("<p>Hello&nbsp;<b>world</b></p><script>ignore()</script>")
    assert "Hello world" in txt
    assert "<" not in txt and "ignore" not in txt


def test_extract_sections_picks_real_not_toc():
    sections = ef._extract_sections(ef._html_to_text(_SAMPLE_10K_HTML))
    assert sections["business"] and "wonderful widgets" in sections["business"]
    assert sections["risk_factors"] and "competition" in sections["risk_factors"]
    assert sections["mda"] and "margins expanded" in sections["mda"]
    # Business must stop before Risk Factors (no bleed-through).
    assert "competition, regulation" not in sections["business"]
    # Risk must stop at Item 1B (Properties text not included).
    assert "We lease offices" not in sections["risk_factors"]


def test_trim_word_boundary():
    out = ef._trim("alpha beta gamma delta", 12)
    assert out.endswith("…")
    assert "gamma" not in out  # cut before the word that overflows


# ─── CIK + 10-K location ──────────────────────────────────────────────────────


def test_get_cik(enabled, monkeypatch):
    monkeypatch.setattr(ef, "_get", _route)
    assert ef.get_cik("AAPL") == "0000320193"
    assert ef.get_cik("aapl") == "0000320193"  # case-insensitive
    assert ef.get_cik("ZZZZ") is None


def test_latest_10k_doc(enabled, monkeypatch):
    monkeypatch.setattr(ef, "_get", _route)
    accession, doc = ef._latest_10k_doc("0000320193")
    assert accession == "000032019324000123"  # dashes stripped
    assert doc == "aapl-10k.htm"


# ─── fetch_10k_sections ───────────────────────────────────────────────────────


def test_fetch_sections_happy_path_and_cache(enabled, monkeypatch):
    calls = {"n": 0}

    def counting_route(url):
        calls["n"] += 1
        return _route(url)

    monkeypatch.setattr(ef, "_get", counting_route)
    sections = ef.fetch_10k_sections("AAPL")
    assert sections and "wonderful widgets" in sections["business"]
    first_calls = calls["n"]

    # Second call hits the on-disk cache → no further HTTP.
    sections2 = ef.fetch_10k_sections("AAPL")
    assert sections2 == sections
    assert calls["n"] == first_calls


def test_fetch_sections_no_cik_returns_none(enabled, monkeypatch):
    monkeypatch.setattr(ef, "_get", _route)
    assert ef.fetch_10k_sections("ZZZZ") is None


def test_fetch_sections_network_failure_returns_none(enabled, monkeypatch):
    monkeypatch.setattr(ef, "_get", lambda url: None)
    assert ef.fetch_10k_sections("AAPL") is None


# ─── augment_filing_text ──────────────────────────────────────────────────────


def test_augment_appends_within_budget(enabled, monkeypatch):
    monkeypatch.setattr(ef, "_get", _route)
    base = "TICKER: AAPL\nSECTOR: Tech"
    out = ef.augment_filing_text("AAPL", base, max_chars=5000)
    assert out.startswith(base)
    assert "SEC 10-K EXCERPTS" in out
    assert "Item 1A — Risk Factors" in out
    assert len(out) <= 5000


def test_augment_falls_back_on_failure(enabled, monkeypatch):
    monkeypatch.setattr(ef, "_get", lambda url: None)
    assert ef.augment_filing_text("AAPL", "BASE") == "BASE"
