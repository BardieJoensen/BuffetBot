"""
Tests for the Phase 5 DKK bridge in src/portfolio.py. A fixed USD/DKK rate is
injected (per the plan) by monkeypatching fx.get_usd_dkk_rate, and price updates
are stubbed so the summaries compute offline.
"""

from datetime import date

import pytest

import src.portfolio as portfolio
from src.portfolio import SECTOR_OVERRIDES, PortfolioTracker, Position, _resolve_sector


class TestResolveSector:
    def test_prefers_sector_then_disp_then_key(self):
        assert _resolve_sector({"sector": "Technology"}) == "Technology"
        assert _resolve_sector({"sectorDisp": "Healthcare"}) == "Healthcare"
        assert _resolve_sector({"sectorKey": "financial-services"}) == "Financial Services"

    def test_manual_override_when_yfinance_empty(self):
        # AL (Air Lease) 404s on Yahoo → empty info → override fills it.
        assert _resolve_sector({}, "AL") == SECTOR_OVERRIDES["AL"] == "Industrials"

    def test_none_when_nothing_resolves(self):
        assert _resolve_sector({}, "ZZZ") is None


@pytest.fixture
def tracker(tmp_path, monkeypatch):
    t = PortfolioTracker(data_dir=str(tmp_path))
    monkeypatch.setattr(t, "update_prices", lambda: None)  # no network
    monkeypatch.setattr(portfolio.fx, "get_usd_dkk_rate", lambda: 7.0)  # fixed rate
    return t


def _position(symbol, current_value, dividend_yield=None):
    p = Position(
        symbol=symbol,
        shares=10,
        cost_basis=100.0,
        purchase_date=date.today(),
        thesis="test",
    )
    p.current_value = current_value
    p.dividend_yield = dividend_yield
    return p


class TestDividendSummaryDkk:
    def test_dkk_and_after_tax(self, tracker):
        tracker.positions = [_position("AAA", 10000.0, dividend_yield=0.03)]
        summ = tracker.get_dividend_summary()
        # 10000 × 3% = 300 USD → ×7 = 2100 DKK → ×0.83 after 17% tax
        assert summ["estimated_annual_usd"] == pytest.approx(300.0)
        assert summ["estimated_annual_dkk"] == pytest.approx(2100.0)
        assert summ["estimated_annual_dkk_after_tax"] == pytest.approx(2100.0 * 0.83)
        assert summ["usd_dkk_rate"] == 7.0
        assert summ["positions"][0]["est_annual_dkk"] == pytest.approx(2100.0)

    def test_empty_portfolio_has_dkk_keys(self, tracker):
        tracker.positions = []
        summ = tracker.get_dividend_summary()
        assert summ["estimated_annual_dkk"] == 0
        assert summ["ask_tax_rate"] == 0.17


class TestPortfolioSummaryDkk:
    def test_values_converted(self, tracker):
        tracker.positions = [_position("AAA", 10000.0)]
        summ = tracker.get_portfolio_summary()
        rate = summ["usd_dkk_rate"]
        assert rate == 7.0
        assert summ["current_value_dkk"] == pytest.approx(summ["current_value"] * rate)
        assert summ["total_invested_dkk"] == pytest.approx(summ["total_invested"] * rate)
        assert summ["total_gain_loss_dkk"] == pytest.approx(summ["total_gain_loss"] * rate)
        # USD fields retained
        assert "current_value" in summ and "total_gain_loss_pct" in summ
