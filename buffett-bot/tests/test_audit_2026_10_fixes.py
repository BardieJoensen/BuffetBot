"""
Regression tests for the fixes that followed the 2026-10-19 audit
(docs/research/BUFFETTBOT_AUDIT.md). Each test names the audit item it pins.
"""

import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

_anthropic_mock = MagicMock()
for _pkg in ("schedule", "dotenv", "anthropic", "anthropic.types", "anthropic.lib", "anthropic.lib.streaming"):
    sys.modules.setdefault(_pkg, _anthropic_mock)

from src.database import Database, _open  # noqa: E402
from src.screener import StockScreener, load_criteria_from_yaml  # noqa: E402


@pytest.fixture
def db(tmp_path):
    return Database(db_path=tmp_path / "test.db")


def _mock_data(symbol, market_cap=10e9):
    return {
        "symbol": symbol,
        "name": f"{symbol} Corp",
        "quote_type": "EQUITY",
        "price": 100.0,
        "market_cap": market_cap,
        "pe_ratio": 20.0,
        "debt_equity": 0.3,
        "roe": 0.18,
        "revenue_growth": 0.10,
        "current_ratio": 1.5,
        "sector": "Technology",
        "industry": "Software",
        "beta": 1.0,
        "dividend_yield": None,
        "profit_margin": 0.2,
        "52_week_high": 120.0,
        "52_week_low": 80.0,
        "free_cashflow": 2e9,
        "operating_cashflow": 3e9,
        "net_income": 2.5e9,
        "payout_ratio": 0.25,
        "operating_margin": 0.25,
        "avg_volume": 5_000_000,
        "fcf_yield": 0.04,
        "earnings_quality": 0.9,
        "real_fcf_yield": 0.04,
        "sbc_ratio": 0.01,
    }


# ─── A1 / L1: Monday persists every scored name ───────────────────────────────


class TestScreenTickersTopN:
    def _screen(self, tickers, **kw):
        screener = StockScreener()
        criteria = load_criteria_from_yaml()  # truncation only runs when scoring rules exist
        criteria.top_n = 2
        with (
            patch.object(screener, "_fetch_stock_data", side_effect=lambda s: _mock_data(s)),
            patch.object(screener, "_get_cached_data", return_value=None),
            patch.object(screener, "_fetch_historical_data", return_value={}),
        ):
            return screener.screen_tickers(tickers, criteria, **kw)

    def test_default_keeps_criteria_top_n(self):
        assert len(self._screen(["A", "B", "C", "D"])) == 2

    def test_zero_returns_every_passing_name(self):
        assert len(self._screen(["A", "B", "C", "D"], top_n=0)) == 4

    def test_explicit_override(self):
        assert len(self._screen(["A", "B", "C", "D"], top_n=3)) == 3


class TestClearQualityScores:
    def test_clears_only_names_not_rescored(self, db):
        db.upsert_universe_stock("FRESH", source="finviz_screen", quality_score=55.0)
        db.upsert_universe_stock("STALE", source="finviz_screen", quality_score=98.5)
        db.upsert_universe_stock("NEVER", source="finviz_screen")
        cleared = db.clear_quality_scores_except(["FRESH"])
        assert cleared == 1
        assert db.get_universe_stock("FRESH")["quality_score"] == pytest.approx(55.0)
        assert db.get_universe_stock("STALE")["quality_score"] is None
        assert db.get_universe_stock("NEVER")["quality_score"] is None


# ─── B3 / L5: hard filters reach the paid queues ──────────────────────────────


class TestQueuesSkipUnscoredNames:
    def _haiku(self, db, ticker, passed=True, days=180):
        with _open(db.path) as conn:
            conn.execute(
                "INSERT INTO haiku_screens (ticker, screened_at, passed, moat_estimate, summary, expires_at)"
                " VALUES (?, ?, ?, 'WIDE', 's', ?)",
                (ticker, datetime.now().isoformat(), int(passed), (datetime.now() + timedelta(days=days)).isoformat()),
            )

    def test_unscreened_queue(self, db):
        db.upsert_universe_stock("SCORED", source="finviz_screen", quality_score=60.0)
        db.upsert_universe_stock("TOOHARD", source="finviz_screen")  # failed hard filters: no score
        q = db.get_unscreened_tickers()
        assert "SCORED" in q and "TOOHARD" not in q

    def test_expiring_queue(self, db):
        db.upsert_universe_stock("SCORED", source="finviz_screen", quality_score=60.0)
        db.upsert_universe_stock("TOOHARD", source="finviz_screen")
        self._haiku(db, "SCORED", days=10)
        self._haiku(db, "TOOHARD", days=10)
        q = db.get_expiring_haiku_tickers(within_days=30)
        assert "SCORED" in q and "TOOHARD" not in q

    def test_sonnet_queue(self, db):
        db.upsert_universe_stock("SCORED", source="finviz_screen", quality_score=60.0)
        db.upsert_universe_stock("TOOHARD", source="finviz_screen")
        self._haiku(db, "SCORED")
        self._haiku(db, "TOOHARD")
        q = db.get_haiku_passes_without_analysis(limit=10)
        assert "SCORED" in q and "TOOHARD" not in q


# ─── A2: the prompt carries the as-of date ────────────────────────────────────


class TestDbSummaryAsOf:
    def test_as_of_date_is_printed(self, db):
        from scripts.scheduler import _build_db_summary

        db.upsert_universe_stock("AAPL", company_name="Apple", sector="Technology", source="sp500_filter")
        db.save_fundamentals("AAPL", {"price": 180.0, "roe": 1.2}, as_of_date="2026-06-29")
        summary = _build_db_summary("AAPL", db)
        assert "as of 2026-06-29" in summary
        assert "Current Price" not in summary


# ─── A6: NaN oldest statement column ──────────────────────────────────────────


class TestTrendMetricsIgnoreNaNColumn:
    def _ticker(self, revenue, net_income):
        cols = pd.to_datetime(["2025-12-31", "2024-12-31", "2023-12-31", "2022-12-31", "2021-12-31"])
        fin = pd.DataFrame(
            {c: [r, n] for c, r, n in zip(cols, revenue, net_income)}, index=["Total Revenue", "Net Income"]
        )
        t = MagicMock()
        t.financials = fin
        t.balance_sheet = pd.DataFrame()
        t.cashflow = pd.DataFrame()
        return t

    def test_revenue_cagr_and_consistency_skip_nan(self):
        screener = StockScreener()
        t = self._ticker([200.0, 180.0, 150.0, 100.0, np.nan], [20.0, 18.0, 15.0, 10.0, np.nan])
        out = screener._fetch_historical_data("X", t)
        assert out["revenue_cagr"] == pytest.approx((200 / 100) ** (1 / 3) - 1)
        assert out["earnings_consistency"] == 3.0


# ─── A4 / L2: universe validation and the DB-driven weekly screen ─────────────


class TestUniverseValidation:
    def test_doubled_first_letter_list_is_rejected(self):
        from src.universe import looks_like_valid_universe

        assert not looks_like_valid_universe(["AAAPL", "AABBV", "AABNB", "UUSLM", "ZZ"])

    def test_normal_list_is_accepted(self):
        from src.universe import looks_like_valid_universe

        assert looks_like_valid_universe(["AAPL", "ABBV", "BRK-B", "CWEN-A", "BF.B", "AA", "KKR"])

    def test_garbage_is_rejected(self):
        from src.universe import looks_like_valid_universe

        assert not looks_like_valid_universe([])
        assert not looks_like_valid_universe(["<a href>", "123", "aapl", None, "MSFT"])

    def test_malformed_cache_is_ignored(self, tmp_path):
        import json

        from src import universe

        universe.set_cache_dir(tmp_path)
        (tmp_path / "stock_universe.json").write_text(
            json.dumps({"tickers": ["AAAPL", "AABBV", "AABNB"], "cached_at": datetime.now().isoformat()})
        )
        assert universe._load_cached_universe() is None


class TestWeeklyScreenUsesDbUniverse:
    def test_screens_db_universe_with_force_sets(self, db):
        from scripts.scheduler import _screen_db_universe

        for i in range(60):
            db.upsert_universe_stock(f"T{i:02d}", source="finviz_screen")
        db.upsert_universe_stock("V", source="conviction")
        db.upsert_universe_stock("AAPL", source="sp500_filter")
        screener = MagicMock()
        screener.screen_tickers.return_value = ["screened"]
        with patch("src.database.Database", return_value=db):
            out = _screen_db_universe(screener, criteria="crit")
        assert out == ["screened"]
        args, kwargs = screener.screen_tickers.call_args
        assert "V" in args[0] and "AAPL" in args[0] and len(args[0]) == 62
        assert kwargs["force_include"] == {"V"} and kwargs["force_large_cap"] == {"AAPL"}

    def test_falls_back_when_db_universe_is_small(self, db):
        from scripts.scheduler import _screen_db_universe

        db.upsert_universe_stock("ONLY", source="finviz_screen")
        with patch("src.database.Database", return_value=db):
            assert _screen_db_universe(MagicMock(), criteria="crit") is None
