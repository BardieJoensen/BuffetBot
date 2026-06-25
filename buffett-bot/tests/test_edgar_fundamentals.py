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
