"""
Tests for the DCF valuation methods in src/valuation.py — focused on Phase 4
(owner-earnings DCF). The projection helper is deterministic and unit-tested
directly; the dual-estimate method is exercised with a fake yfinance ticker
(in-memory pandas frames), so no network is required.
"""

from datetime import datetime

import pandas as pd
import pytest

from src.valuation import AggregatedValuation, ValuationAggregator, ValuationEstimate


@pytest.fixture
def agg():
    return ValuationAggregator(finnhub_key=None)


class _FakeTicker:
    """Minimal yfinance.Ticker stand-in exposing .cashflow / .financials."""

    def __init__(self, cashflow: pd.DataFrame, financials: pd.DataFrame):
        self.cashflow = cashflow
        self.financials = financials


def _frames(*, ocf, capex, sbc, dep, revenue):
    """Two-column (flat YoY → 0% growth) cashflow + financials frames."""
    cols = ["2024", "2023"]
    cashflow = pd.DataFrame(
        {
            c: {
                "Operating Cash Flow": ocf,
                "Capital Expenditure": -abs(capex),  # yfinance reports capex negative
                "Stock Based Compensation": sbc,
                "Depreciation And Amortization": dep,
            }
            for c in cols
        }
    )
    financials = pd.DataFrame({c: {"Total Revenue": revenue} for c in cols})
    return _FakeTicker(cashflow, financials)


# ─── _project_dcf (deterministic, known-input) ──────────────────────────────


class TestProjectDcf:
    def test_known_value_zero_growth(self, agg):
        # base=100, 0% growth, 10% discount, 12× terminal, 10 shares.
        # PV(annuity) = 100 × (1-1.1^-10)/0.1 = 614.46
        # terminal    = 100 × 12 / 1.1^10      = 462.65
        # per share   = (614.46 + 462.65) / 10 = 107.71
        fv = agg._project_dcf(100.0, 0.0, 0.0, 10)
        assert fv == pytest.approx(107.71, abs=0.1)

    def test_higher_base_gives_higher_value(self, agg):
        assert agg._project_dcf(160.0, 0.0, 0.0, 100) > agg._project_dcf(140.0, 0.0, 0.0, 100)


# ─── _calculate_dcf_estimates (owner earnings ≥ real FCF) ───────────────────


class TestDcfEstimates:
    def test_owner_earnings_ge_real_fcf(self, agg):
        # capex 50 > D&A 30 → maintenance capex 30 → owner earnings > real FCF.
        ticker = _frames(ocf=200, capex=50, sbc=10, dep=30, revenue=1000)
        ests = agg._calculate_dcf_estimates({"sharesOutstanding": 100}, ticker)
        by_source = {e.source: e.fair_value for e in ests}

        assert "DCF (10yr Real FCF)" in by_source
        assert "DCF (10yr Owner Earnings)" in by_source
        # real_fcf = 200-50-10 = 140; owner = 200-30-10 = 160 → owner FV higher
        assert by_source["DCF (10yr Owner Earnings)"] > by_source["DCF (10yr Real FCF)"]

    def test_owner_earnings_omitted_when_da_exceeds_capex(self, agg):
        # D&A 80 ≥ capex 50 → maintenance capex == capex → no distinct estimate.
        ticker = _frames(ocf=200, capex=50, sbc=10, dep=80, revenue=1000)
        ests = agg._calculate_dcf_estimates({"sharesOutstanding": 100}, ticker)
        sources = {e.source for e in ests}
        assert sources == {"DCF (10yr Real FCF)"}

    def test_no_estimate_when_real_fcf_negative(self, agg):
        # OCF below capex+sbc → real FCF ≤ 0 → no DCF at all (conservative).
        ticker = _frames(ocf=40, capex=50, sbc=10, dep=30, revenue=1000)
        assert agg._calculate_dcf_estimates({"sharesOutstanding": 100}, ticker) == []

    def test_no_shares_returns_empty(self, agg):
        ticker = _frames(ocf=200, capex=50, sbc=10, dep=30, revenue=1000)
        assert agg._calculate_dcf_estimates({"sharesOutstanding": 0}, ticker) == []


class TestActionablePrices:
    @staticmethod
    def _valuation(price):
        return AggregatedValuation(
            symbol="TEST",
            current_price=price,
            estimates=[ValuationEstimate("test", 100.0, "test", datetime.now(), "high")],
        )

    @pytest.mark.parametrize("price", [0, -1, float("nan"), float("inf")])
    def test_invalid_price_has_no_margin_of_safety(self, price):
        valuation = self._valuation(price)

        assert valuation.has_valid_price is False
        assert valuation.margin_of_safety is None
        assert valuation.upside_potential is None

    def test_positive_price_remains_actionable(self):
        valuation = self._valuation(80.0)

        assert valuation.has_valid_price is True
        assert valuation.margin_of_safety == pytest.approx(0.20)


# ─── Analyst-consensus de-duplication ───────────────────────────────────────


class TestAnalystTargetMerge:
    """
    yfinance and Finnhub both report the same sell-side consensus. Keeping them
    as two medium-weight estimates double-counted 12-month price targets inside
    "fair value"; both present must collapse into one estimate at the mean.
    """

    @staticmethod
    def _est(source, value):
        return ValuationEstimate(source, value, "Analyst Price Targets", datetime.now(), "medium")

    def test_both_sources_collapse_to_one_mean_estimate(self):
        merged = ValuationAggregator._merge_analyst_targets(self._est("yf", 100.0), self._est("finnhub", 120.0))
        assert merged is not None
        assert merged.fair_value == pytest.approx(110.0)
        assert merged.confidence == "medium"
        assert "Yahoo" in merged.source and "Finnhub" in merged.source

    def test_single_source_passes_through(self):
        only_yf = self._est("yf", 100.0)
        assert ValuationAggregator._merge_analyst_targets(only_yf, None) is only_yf
        only_fh = self._est("finnhub", 90.0)
        assert ValuationAggregator._merge_analyst_targets(None, only_fh) is only_fh
        assert ValuationAggregator._merge_analyst_targets(None, None) is None

    def test_get_valuation_keeps_one_analyst_estimate(self, agg, monkeypatch):
        monkeypatch.setattr(agg, "_get_yfinance_target", lambda info: self._est("yf", 100.0))
        monkeypatch.setattr(agg, "_get_finnhub_price_target", lambda symbol: self._est("finnhub", 120.0))
        monkeypatch.setattr(agg, "_calculate_pe_based_value", lambda info: None)
        monkeypatch.setattr(agg, "_calculate_graham_number", lambda info: None)
        monkeypatch.setattr(agg, "_calculate_dcf_estimates", lambda info, ticker: [])

        class _T:
            info = {"regularMarketPrice": 80.0}

        monkeypatch.setattr("src.valuation.yf.Ticker", lambda symbol: _T())
        valuation = agg.get_valuation("TEST")
        analyst = [e for e in valuation.estimates if "Analyst" in e.source]
        assert len(analyst) == 1
        assert valuation.average_fair_value == pytest.approx(110.0)
