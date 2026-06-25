"""
Tests for the point-in-time backtest in src/backtest.py (Phase 2.5).

Prices are injected (yfinance is never called) and pit_fundamentals is populated
in a temp DB, so the look-ahead-free path runs fully offline. Also covers the
as-known fundamentals → quality-metric derivation.
"""

import pandas as pd
import pytest

import src.backtest as backtest
from src.database import Database


@pytest.fixture
def db(tmp_path):
    return Database(db_path=tmp_path / "test.db")


def _pit_records(
    ticker, *, net_income, equity, revenue, op_income, ocf, capex, shares, period_end="2019-12-31", filed="2020-02-15"
):
    base = dict(
        cik="0",
        period_end=period_end,
        fiscal_year=2019,
        fiscal_period="FY",
        form="10-K",
        filed_date=filed,
        accession="a",
    )
    vals = {
        "net_income": net_income,
        "equity": equity,
        "revenue": revenue,
        "operating_income": op_income,
        "ocf": ocf,
        "capex": capex,
        "shares_diluted": shares,
    }
    return [{**base, "ticker": ticker, "concept": c, "value": float(v)} for c, v in vals.items()]


def _price_series(annual_return: float, start: float = 100.0) -> pd.Series:
    idx = pd.date_range("2019-01-01", "2021-12-31", freq="D")
    # Compound daily toward the target 1-year return.
    daily = (1 + annual_return) ** (1 / 365) - 1
    values = [start * (1 + daily) ** i for i in range(len(idx))]
    return pd.Series(values, index=idx)


class TestDeriveMetrics:
    def test_full_derivation(self):
        known = {
            "net_income": 100,
            "equity": 1000,
            "revenue": 2000,
            "operating_income": 300,
            "ocf": 250,
            "capex": 50,
            "shares_diluted": 10,
            "long_term_debt": 200,
        }
        m = backtest._derive_quality_metrics(known, price=50.0)
        assert m["roe"] == pytest.approx(0.10)  # 100/1000
        assert m["roic"] == pytest.approx(100 / 1200)  # ni/(equity+debt)
        assert m["operating_margin"] == pytest.approx(0.15)  # 300/2000
        assert m["debt_equity"] == pytest.approx(0.20)  # 200/1000
        # market cap = 50 × 10 = 500; fcf = 250-50 = 200 → 0.40
        assert m["real_fcf_yield"] == pytest.approx(0.40)

    def test_returns_none_when_nothing_derivable(self):
        assert backtest._derive_quality_metrics({}, price=50.0) is None


class TestPointInTimeBacktest:
    def _populate(self, db):
        # Six stocks: higher net income (→ higher quality) paired with higher
        # forward return, so the study should see a positive premium.
        specs = {
            "AAA": (300, 0.40),
            "BBB": (250, 0.30),
            "CCC": (200, 0.20),
            "DDD": (150, 0.10),
            "EEE": (100, 0.00),
            "FFF": (50, -0.10),
        }
        prices = {}
        for t, (ni, ret) in specs.items():
            db.save_pit_fundamentals(
                _pit_records(
                    t, net_income=ni, equity=1000, revenue=2000, op_income=ni * 2, ocf=ni + 100, capex=50, shares=10
                )
            )
            prices[t] = _price_series(ret)
        return prices

    def test_runs_offline_and_produces_observations(self, db, monkeypatch):
        prices = self._populate(db)
        monkeypatch.setattr(backtest, "_load_prices", lambda sym, cache: prices.get(sym))

        result = backtest.run_point_in_time_backtest(list(prices.keys()), db, ["2020-06-01"], forward_months=12)
        obs = result["observations"]
        assert len(obs) == 6
        assert all(0.0 <= o["score"] <= 100.0 for o in obs)
        assert all(isinstance(o["return_1y"], float) for o in obs)

        summary = result["summary"]
        assert summary["basis"].startswith("point-in-time")
        assert summary["total_observations"] == 6
        assert "limitations" in summary
        # Highest-quality names should not underperform the lowest on average.
        assert summary["quality_premium"] > 0

    def test_excludes_stocks_without_asof_data(self, db, monkeypatch):
        prices = self._populate(db)
        # A stock whose only filing postdates the rebalance date → excluded.
        db.save_pit_fundamentals(
            _pit_records(
                "LATE",
                net_income=999,
                equity=1000,
                revenue=2000,
                op_income=300,
                ocf=200,
                capex=50,
                shares=10,
                filed="2021-01-01",
            )
        )
        prices["LATE"] = _price_series(0.5)
        monkeypatch.setattr(backtest, "_load_prices", lambda sym, cache: prices.get(sym))

        result = backtest.run_point_in_time_backtest(list(prices.keys()), db, ["2020-06-01"], forward_months=12)
        assert {o["symbol"] for o in result["observations"]} == {"AAA", "BBB", "CCC", "DDD", "EEE", "FFF"}
