"""
Tests for the DCF valuation methods in src/valuation.py — focused on Phase 4
(owner-earnings DCF). The projection helper is deterministic and unit-tested
directly; the dual-estimate method is exercised with a fake yfinance ticker
(in-memory pandas frames), so no network is required.
"""

import pandas as pd
import pytest

from src.valuation import ValuationAggregator


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
