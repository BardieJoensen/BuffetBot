"""
Tests for src/edgar_fundamentals.py — point-in-time fundamentals (Phase 2.5).

No network: companyfacts JSON is supplied in-memory and edgar_fetcher's CIK /
enablement hooks are monkeypatched. Covers concept resolution (ordered fallback),
originally-filed selection (restatements don't leak backward), record building,
the DB round-trip, and the as-of (look-ahead-free) accessor.
"""

from types import SimpleNamespace

import pytest

import src.edgar_fetcher as edgar_fetcher
import src.edgar_fundamentals as ef
from src.database import Database

# Synthetic companyfacts: Revenues has a 2021 restatement filed in 2022 — the
# originally-filed 2021 value must win. SalesRevenueNet is a fallback that should
# be ignored because the primary "Revenues" tag resolves first.
FACTS = {
    "cik": 320193,
    "entityName": "Apple Inc.",
    "facts": {
        "us-gaap": {
            "Revenues": {
                "units": {
                    "USD": [
                        {
                            "end": "2022-09-24",
                            "val": 394328,
                            "fy": 2022,
                            "fp": "FY",
                            "form": "10-K",
                            "filed": "2022-10-28",
                            "accn": "a2",
                        },
                        {
                            "end": "2021-09-25",
                            "val": 365817,
                            "fy": 2021,
                            "fp": "FY",
                            "form": "10-K",
                            "filed": "2021-10-29",
                            "accn": "a1",
                        },
                        {
                            "end": "2021-09-25",
                            "val": 999999,
                            "fy": 2021,
                            "fp": "FY",
                            "form": "10-K",
                            "filed": "2022-10-28",
                            "accn": "a2",
                        },  # restatement
                    ]
                }
            },
            "NetIncomeLoss": {
                "units": {
                    "USD": [
                        {
                            "end": "2022-09-24",
                            "val": 99803,
                            "fy": 2022,
                            "fp": "FY",
                            "form": "10-K",
                            "filed": "2022-10-28",
                            "accn": "a2",
                        },
                    ]
                }
            },
            "SalesRevenueNet": {
                "units": {
                    "USD": [
                        {
                            "end": "2020-09-26",
                            "val": 274515,
                            "fy": 2020,
                            "fp": "FY",
                            "form": "10-K",
                            "filed": "2020-10-30",
                            "accn": "a0",
                        },
                    ]
                }
            },
        }
    },
}


@pytest.fixture
def db(tmp_path):
    return Database(db_path=tmp_path / "test.db")


# ─── pure helpers ─────────────────────────────────────────────────────────────


def test_expected_unit():
    assert ef._expected_unit("revenue") == "USD"
    assert ef._expected_unit("eps_diluted") == "USD/shares"
    assert ef._expected_unit("shares_diluted") == "shares"


def test_observations_for_tag_searches_namespaces():
    obs = ef._observations_for_tag(FACTS, "Revenues", "USD")
    assert obs is not None and len(obs) == 3
    assert ef._observations_for_tag(FACTS, "DoesNotExist", "USD") is None


def test_originally_filed_keeps_earliest():
    obs = ef._observations_for_tag(FACTS, "Revenues", "USD")
    best = ef._originally_filed(obs)
    # 2021 period: earliest filed (2021-10-29) value kept, not the 2022 restatement.
    assert best[("2021-09-25", "10-K")]["val"] == 365817
    assert ("2022-09-24", "10-K") in best


def _obs(start, end, val, form, filed="2020-01-01", **extra):
    return {"start": start, "end": end, "val": val, "form": form, "filed": filed, "fy": 2019, "fp": "FY", **extra}


class TestSpanFilter:
    """
    Facts are keyed by (end, form) only, so a 10-K's Q4 fact (same `end` as the
    annual fact) or a 10-Q year-to-date fact could win on list order alone.
    _originally_filed must drop facts whose duration doesn't match the form.
    """

    def test_10k_annual_beats_q4_regardless_of_order(self):
        # Q4 fact listed FIRST — the bug would let it win by insertion order.
        obs = [
            _obs("2018-07-01", "2018-09-29", 14125, "10-K", filed="2018-11-05"),
            _obs("2017-10-01", "2018-09-29", 59531, "10-K", filed="2018-11-05"),
        ]
        best = ef._originally_filed(obs)
        assert best[("2018-09-29", "10-K")]["val"] == 59531

    def test_10q_ytd_rejected_quarter_kept(self):
        obs = [
            _obs("2019-01-01", "2019-09-30", 900, "10-Q", filed="2019-11-01"),  # 9-month YTD
            _obs("2019-07-01", "2019-09-30", 300, "10-Q", filed="2019-11-01"),  # the quarter
        ]
        best = ef._originally_filed(obs)
        assert best[("2019-09-30", "10-Q")]["val"] == 300

    def test_instant_facts_without_start_still_load(self):
        obs = [{"end": "2019-09-28", "val": 4443236000, "form": "10-K", "filed": "2019-10-31"}]
        best = ef._originally_filed(obs)
        assert best[("2019-09-28", "10-K")]["val"] == 4443236000

    def test_52_week_fiscal_year_accepted(self):
        # 2018-09-30 .. 2019-09-28 is a 52-week (363/364-day) year
        obs = [_obs("2018-09-30", "2019-09-28", 55256, "10-K", filed="2019-10-31")]
        best = ef._originally_filed(obs)
        assert best[("2019-09-28", "10-K")]["val"] == 55256

    def test_calendar_year_and_53_week_year_accepted(self):
        assert ef._span_ok(_obs("2019-01-01", "2019-12-31", 1, "10-K"))
        assert ef._span_ok(_obs("2018-12-30", "2020-01-04", 1, "10-K"))  # 371 days

    def test_unknown_form_keeps_old_behaviour(self):
        assert ef._span_ok(_obs("2019-07-01", "2019-09-30", 1, "8-K"))

    def test_bad_dates_rejected(self):
        assert not ef._span_ok({"start": "nope", "end": "2019-09-30", "form": "10-K"})


# ─── build_pit_records ────────────────────────────────────────────────────────


def test_build_records_merges_tags_and_keeps_originally_filed():
    records = ef.build_pit_records("AAPL", "0000320193", FACTS)
    by_concept: dict[str, list] = {}
    for r in records:
        by_concept.setdefault(r["concept"], []).append(r)

    assert set(by_concept) == {"revenue", "net_income"}  # only resolvable concepts
    # Tags are MERGED across the fallback list: SalesRevenueNet's 2020 period
    # fills a gap not covered by the primary "Revenues" tag (real-world tag
    # migration). The 2021 restatement is still discarded (originally-filed wins).
    rev_periods = {r["period_end"]: r["value"] for r in by_concept["revenue"]}
    assert rev_periods == {"2022-09-24": 394328, "2021-09-25": 365817, "2020-09-26": 274515}


# ─── DB round-trip + as-of ────────────────────────────────────────────────────


class TestPitDatabase:
    def _load(self, db):
        records = ef.build_pit_records("AAPL", "0000320193", FACTS)
        return db.save_pit_fundamentals(records)

    def test_save_and_series(self, db):
        n = self._load(db)
        assert n == 4  # 3 revenue periods (merged tags) + 1 net_income
        series = db.get_pit_concept_series("AAPL", "revenue")
        assert [r["value"] for r in series] == [274515, 365817, 394328]  # oldest first

    def test_save_is_idempotent(self, db):
        self._load(db)
        assert self._load(db) == 0  # INSERT OR IGNORE on re-load

    def test_asof_excludes_not_yet_filed(self, db):
        self._load(db)
        # At 2022-01-01 only the 2021 10-K (filed 2021-10-29) is public.
        known = db.get_pit_fundamentals_asof("AAPL", "2022-01-01")
        assert known["revenue"] == 365817
        assert "net_income" not in known  # 2022 figure filed 2022-10-28, not yet public

    def test_asof_takes_latest_known_period(self, db):
        self._load(db)
        known = db.get_pit_fundamentals_asof("AAPL", "2023-01-01")
        assert known["revenue"] == 394328  # latest period public by then
        assert known["net_income"] == 99803


# ─── load_ticker_fundamentals (mocked CIK + fetch) ──────────────────────────


def test_load_ticker_fundamentals(db, monkeypatch):
    monkeypatch.setattr(edgar_fetcher, "config", SimpleNamespace(edgar_user_agent="BuffettBot t@e.com"))
    monkeypatch.setattr(edgar_fetcher, "get_cik", lambda t: "0000320193")
    monkeypatch.setattr(ef, "fetch_companyfacts", lambda cik, use_cache=True: FACTS)

    n = ef.load_ticker_fundamentals("AAPL", db)
    assert n == 4
    assert "AAPL" in db.get_pit_tickers()


def test_load_ticker_disabled_returns_zero(db, monkeypatch):
    monkeypatch.setattr(edgar_fetcher, "config", SimpleNamespace(edgar_user_agent=""))
    assert ef.load_ticker_fundamentals("AAPL", db) == 0
