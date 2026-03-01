"""
Tests for scripts/bulk_load.py — Phase C Bulk Load

We test the pure-Python helpers that contain logic:
  - _build_company_summary()
  - _priority_list()
  - _moat_hint_to_label()
  - _screened_to_data_map()
  - screen_tickers() — new StockScreener method (mocked)
  - parse_args() — CLI argument parsing
  - step3_quality_scores() — DB interaction (in-memory DB)

No yfinance or Anthropic API calls are made.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ── Bootstrap path ──────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))
sys.path.insert(0, str(_PROJECT_ROOT / "scripts"))

from bulk_load import (
    _build_company_summary,
    _moat_hint_to_label,
    _priority_list,
    _screened_to_data_map,
    parse_args,
    step3_quality_scores,
    step4_haiku_batch,
    step6_report,
)
from src.database import Database
from src.screener import ScreeningCriteria, ScreenedStock, StockScreener
from src.quality_scorer import QualityScore
from datetime import datetime


# ─── Fixtures ───────────────────────────────────────────────────────────────

def _make_screened_stock(symbol: str, **kwargs) -> ScreenedStock:
    defaults = dict(
        symbol=symbol,
        name=f"{symbol} Inc",
        market_cap=10_000_000_000,
        pe_ratio=20.0,
        debt_equity=0.3,
        roe=0.18,
        revenue_growth=0.10,
        sector="Technology",
        industry="Software",
        screened_at=datetime.now(),
        price=100.0,
        score=5.0,
        score_confidence=0.8,
        roic=0.20,
        operating_margin=0.25,
        real_fcf_yield=0.05,
        cap_category="large",
    )
    defaults.update(kwargs)
    return ScreenedStock(**defaults)


def _make_db(tmp_path) -> Database:
    return Database(tmp_path / "test.db")


# ─── _build_company_summary ──────────────────────────────────────────────────

class TestBuildCompanySummary:
    def test_basic_output_is_non_empty(self):
        data = {
            "name": "Acme Corp",
            "sector": "Technology",
            "industry": "Software",
            "market_cap": 5_000_000_000,
            "price": 150.0,
            "pe_ratio": 25.0,
            "roe": 0.20,
        }
        result = _build_company_summary("ACME", data)
        assert result
        assert "ACME" in result
        assert "Acme Corp" in result

    def test_includes_sector_and_industry(self):
        data = {"sector": "Healthcare", "industry": "Biotech"}
        result = _build_company_summary("TST", data)
        assert "Healthcare" in result
        assert "Biotech" in result

    def test_includes_key_metrics(self):
        data = {
            "roe": 0.25,
            "roic": 0.30,
            "operating_margin": 0.35,
            "revenue_growth": 0.12,
            "real_fcf_yield": 0.06,
        }
        result = _build_company_summary("TST", data)
        assert "25.0%" in result    # roe
        assert "30.0%" in result    # roic
        assert "35.0%" in result    # margin
        assert "12.0%" in result    # revenue growth
        assert "6.0%" in result     # fcf yield

    def test_includes_conviction_notes(self):
        data = {}
        result = _build_company_summary("V", data, notes="Payments duopoly")
        assert "Payments duopoly" in result
        assert "ANALYST NOTES" in result

    def test_omits_analyst_notes_section_when_no_notes(self):
        data = {}
        result = _build_company_summary("V", data, notes="")
        assert "ANALYST NOTES" not in result

    def test_handles_missing_data_gracefully(self):
        """Empty data dict must not raise."""
        result = _build_company_summary("EMPTY", {})
        assert "EMPTY" in result
        assert result  # non-empty

    def test_market_cap_formatted_in_billions(self):
        data = {"market_cap": 3_500_000_000_000}  # $3.5T
        result = _build_company_summary("AAPL", data)
        assert "3500" in result or "3,500" in result or "3.5" in result

    def test_debt_equity_large_value_divided(self):
        """yfinance returns debt/equity as e.g. 45 (percent) not 0.45."""
        data = {"debt_equity": 45}
        result = _build_company_summary("TST", data)
        # Should show ~0.45× not 45×
        assert "0.45" in result

    def test_uses_real_fcf_yield_over_fcf_yield(self):
        data = {"real_fcf_yield": 0.07, "fcf_yield": 0.09}
        result = _build_company_summary("TST", data)
        assert "7.0%" in result
        # raw fcf_yield (9%) should NOT appear since real_fcf_yield takes priority
        assert "9.0%" not in result

    def test_falls_back_to_fcf_yield_when_real_missing(self):
        data = {"fcf_yield": 0.09}
        result = _build_company_summary("TST", data)
        assert "9.0%" in result

    def test_roic_from_historical_dict(self):
        """ROIC is sometimes only in data['historical']."""
        data = {"historical": {"roic": 0.22}}
        result = _build_company_summary("TST", data)
        assert "22.0%" in result


# ─── _priority_list ──────────────────────────────────────────────────────────

class TestPriorityList:
    def _make_score(self, ticker: str, score: float) -> QualityScore:
        return QualityScore(
            ticker=ticker,
            score=score,
            roic_pct=None, roe_pct=None, fcf_yield_pct=None,
            operating_margin_pct=None, low_debt_pct=None,
            data_coverage=1.0,
        )

    def test_conviction_tickers_come_first(self):
        conviction = ["COST", "V"]
        scores = {
            "COST": self._make_score("COST", 70.0),
            "V": self._make_score("V", 80.0),
            "AAAA": self._make_score("AAAA", 90.0),  # higher score but not conviction
            "BBBB": self._make_score("BBBB", 85.0),
        }
        all_tickers = ["COST", "V", "AAAA", "BBBB"]
        result = _priority_list(conviction, scores, all_tickers)

        # First two must be conviction tickers
        assert set(result[:2]) == {"COST", "V"}
        # AAAA and BBBB come after, even though their scores are higher
        assert result.index("AAAA") > result.index("V")
        assert result.index("BBBB") > result.index("COST")

    def test_conviction_sorted_by_quality_within_group(self):
        conviction = ["COST", "V"]
        scores = {
            "COST": self._make_score("COST", 70.0),
            "V": self._make_score("V", 90.0),  # V has higher score
        }
        result = _priority_list(conviction, scores, ["COST", "V"])
        assert result[0] == "V"
        assert result[1] == "COST"

    def test_non_conviction_sorted_by_quality(self):
        conviction = []
        scores = {
            "LOW": self._make_score("LOW", 30.0),
            "HIGH": self._make_score("HIGH", 80.0),
            "MID": self._make_score("MID", 55.0),
        }
        result = _priority_list(conviction, scores, ["LOW", "HIGH", "MID"])
        assert result == ["HIGH", "MID", "LOW"]

    def test_tickers_not_in_scores_get_zero(self):
        """Tickers with no quality score don't crash — they sort last."""
        conviction = ["CONV"]
        scores = {}   # empty
        all_tickers = ["CONV", "NODATA"]
        result = _priority_list(conviction, scores, all_tickers)
        assert result[0] == "CONV"   # conviction first
        assert "NODATA" in result

    def test_empty_universe(self):
        result = _priority_list([], {}, [])
        assert result == []

    def test_conviction_only_universe(self):
        conviction = ["V", "MA"]
        scores = {"V": self._make_score("V", 80), "MA": self._make_score("MA", 75)}
        result = _priority_list(conviction, scores, ["V", "MA"])
        assert len(result) == 2
        assert set(result) == {"V", "MA"}


# ─── _moat_hint_to_label ─────────────────────────────────────────────────────

class TestMoatHintToLabel:
    def test_5_is_wide(self):
        assert _moat_hint_to_label(5) == "WIDE"

    def test_4_is_wide(self):
        assert _moat_hint_to_label(4) == "WIDE"

    def test_3_is_narrow(self):
        assert _moat_hint_to_label(3) == "NARROW"

    def test_2_is_none(self):
        assert _moat_hint_to_label(2) == "NONE"

    def test_0_is_none(self):
        assert _moat_hint_to_label(0) == "NONE"


# ─── _screened_to_data_map ───────────────────────────────────────────────────

class TestScreenedToDataMap:
    def test_returns_dict_keyed_by_symbol(self):
        stocks = [_make_screened_stock("V"), _make_screened_stock("COST")]
        result = _screened_to_data_map(stocks)
        assert set(result.keys()) == {"V", "COST"}

    def test_values_are_dicts(self):
        stocks = [_make_screened_stock("MA")]
        result = _screened_to_data_map(stocks)
        assert isinstance(result["MA"], dict)
        assert result["MA"]["symbol"] == "MA"

    def test_empty_list(self):
        assert _screened_to_data_map([]) == {}


# ─── parse_args ──────────────────────────────────────────────────────────────

class TestParseArgs:
    def test_defaults(self):
        args = parse_args([])
        assert args.dry_run is False
        assert args.conviction_only is False
        assert args.skip_llm is False
        assert args.haiku_limit == 400
        assert args.sonnet_limit == 80

    def test_dry_run_flag(self):
        args = parse_args(["--dry-run"])
        assert args.dry_run is True

    def test_conviction_only(self):
        args = parse_args(["--conviction-only"])
        assert args.conviction_only is True

    def test_skip_llm(self):
        args = parse_args(["--skip-llm"])
        assert args.skip_llm is True

    def test_haiku_limit(self):
        args = parse_args(["--haiku-limit", "50"])
        assert args.haiku_limit == 50

    def test_sonnet_limit(self):
        args = parse_args(["--sonnet-limit", "10"])
        assert args.sonnet_limit == 10

    def test_db_path_override(self, tmp_path):
        p = tmp_path / "custom.db"
        args = parse_args(["--db-path", str(p)])
        assert args.db_path == p


# ─── step3_quality_scores ────────────────────────────────────────────────────

class TestStep3QualityScores:
    def test_scores_saved_to_db(self, tmp_path):
        db = _make_db(tmp_path)
        db.upsert_universe_stock("AAPL", source="conviction")
        db.upsert_universe_stock("MSFT", source="conviction")

        stocks = [
            _make_screened_stock("AAPL", roic=0.35, roe=0.30, operating_margin=0.30, real_fcf_yield=0.06, debt_equity=0.2),
            _make_screened_stock("MSFT", roic=0.25, roe=0.20, operating_margin=0.20, real_fcf_yield=0.04, debt_equity=0.4),
        ]

        scores = step3_quality_scores(stocks, db, dry_run=False)

        assert "AAPL" in scores
        assert "MSFT" in scores
        # AAPL has higher metrics → should score higher
        assert scores["AAPL"].score >= scores["MSFT"].score

        # Verify saved in DB
        universe = db.get_universe()
        aapl = next(s for s in universe if s["ticker"] == "AAPL")
        msft = next(s for s in universe if s["ticker"] == "MSFT")
        assert aapl["quality_score"] is not None
        assert msft["quality_score"] is not None
        assert aapl["quality_score"] >= msft["quality_score"]

    def test_dry_run_does_not_write_scores(self, tmp_path):
        db = _make_db(tmp_path)
        db.upsert_universe_stock("V", source="conviction")

        stocks = [_make_screened_stock("V", roic=0.40)]
        step3_quality_scores(stocks, dry_run=True, db=db)

        universe = db.get_universe()
        v_row = next(s for s in universe if s["ticker"] == "V")
        # quality_score should still be NULL — dry_run wrote nothing
        assert v_row["quality_score"] is None

    def test_empty_screened_returns_empty_dict(self, tmp_path):
        db = _make_db(tmp_path)
        result = step3_quality_scores([], db, dry_run=False)
        assert result == {}


# ─── StockScreener.screen_tickers ───────────────────────────────────────────

class TestScreenTickers:
    """Tests for the new screen_tickers() method added to StockScreener."""

    def _make_mock_data(self, symbol: str, market_cap: float = 10e9) -> dict:
        """Return minimal yfinance-style data dict that passes hard filters."""
        return {
            "symbol": symbol,
            "name": f"{symbol} Corp",
            "quote_type": "EQUITY",
            "price": 100.0,
            "market_cap": market_cap,
            "pe_ratio": 20.0,
            "debt_equity": 30.0,
            "roe": 0.18,
            "revenue_growth": 0.10,
            "current_ratio": 1.5,
            "sector": "Technology",
            "industry": "Software",
            "beta": 1.0,
            "dividend_yield": None,
            "profit_margin": 0.20,
            "52_week_high": 120.0,
            "52_week_low": 80.0,
            "free_cashflow": 2_000_000_000,
            "operating_cashflow": 3_000_000_000,
            "net_income": 2_500_000_000,
            "payout_ratio": 0.25,
            "operating_margin": 0.25,
            "avg_volume": 5_000_000,
            "fcf_yield": 0.04,
            "earnings_quality": 0.9,
            "real_fcf_yield": 0.04,
            "sbc_ratio": 0.01,
        }

    def test_accepts_explicit_ticker_list(self, tmp_path):
        """screen_tickers() should use provided tickers not Finviz universe."""
        screener = StockScreener()
        mock_data = self._make_mock_data("COST")

        with patch.object(screener, "_fetch_stock_data", return_value=mock_data), \
             patch.object(screener, "_get_cached_data", return_value=None), \
             patch.object(screener, "_fetch_historical_data", return_value={}):
            result = screener.screen_tickers(["COST"], ScreeningCriteria())

        assert any(s.symbol == "COST" for s in result)

    def test_force_include_bypasses_max_market_cap(self):
        """Mega-cap stocks in force_include should not be filtered out."""
        screener = StockScreener()
        mega_cap_data = self._make_mock_data("AAPL", market_cap=3_500_000_000_000)  # $3.5T

        criteria = ScreeningCriteria(max_market_cap=500_000_000_000)  # $500B limit

        with patch.object(screener, "_fetch_stock_data", return_value=mega_cap_data), \
             patch.object(screener, "_get_cached_data", return_value=None), \
             patch.object(screener, "_fetch_historical_data", return_value={}):

            # Without force_include: AAPL filtered out
            result_no_force = screener.screen_tickers(["AAPL"], criteria)
            assert not any(s.symbol == "AAPL" for s in result_no_force)

            # With force_include: AAPL passes
            result_with_force = screener.screen_tickers(
                ["AAPL"], criteria, force_include={"AAPL"}
            )
            assert any(s.symbol == "AAPL" for s in result_with_force)

    def test_non_force_tickers_still_filtered(self):
        """force_include should not affect other tickers."""
        screener = StockScreener()

        def _side_effect(symbol):
            return self._make_mock_data(symbol, market_cap=3_500_000_000_000)

        criteria = ScreeningCriteria(max_market_cap=500_000_000_000)

        with patch.object(screener, "_fetch_stock_data", side_effect=_side_effect), \
             patch.object(screener, "_get_cached_data", return_value=None), \
             patch.object(screener, "_fetch_historical_data", return_value={}):
            result = screener.screen_tickers(
                ["AAPL", "MSFT"],
                criteria,
                force_include={"AAPL"},   # only AAPL forced; MSFT should be filtered
            )

        assert any(s.symbol == "AAPL" for s in result)
        assert not any(s.symbol == "MSFT" for s in result)

    def test_screen_delegates_to_screen_tickers(self):
        """screen() should still work (now delegates to screen_tickers)."""
        screener = StockScreener()
        mock_data = self._make_mock_data("V")

        with patch("src.screener.get_stock_universe", return_value=["V"]), \
             patch.object(screener, "_fetch_stock_data", return_value=mock_data), \
             patch.object(screener, "_get_cached_data", return_value=None), \
             patch.object(screener, "_fetch_historical_data", return_value={}):
            result = screener.screen(ScreeningCriteria())

        assert any(s.symbol == "V" for s in result)

    def test_returns_empty_for_empty_ticker_list(self):
        screener = StockScreener()
        result = screener.screen_tickers([])
        assert result == []

    def test_cap_category_set_on_screened_stock(self):
        """Large-cap stocks should have cap_category='large'."""
        screener = StockScreener()
        mock_data = self._make_mock_data("COST", market_cap=50_000_000_000)  # $50B → large

        with patch.object(screener, "_fetch_stock_data", return_value=mock_data), \
             patch.object(screener, "_get_cached_data", return_value=None), \
             patch.object(screener, "_fetch_historical_data", return_value={}):
            result = screener.screen_tickers(["COST"], ScreeningCriteria())

        assert result
        assert result[0].cap_category == "large"


# ─── step4_haiku_batch (dry-run only) ────────────────────────────────────────

class TestStep4HaikuBatchDryRun:
    def test_dry_run_does_not_call_analyzer(self, tmp_path):
        db = _make_db(tmp_path)
        db.upsert_universe_stock("V", source="conviction")

        mock_analyzer = MagicMock()
        data_map = {"V": {"name": "Visa", "sector": "Financials"}}

        result = step4_haiku_batch(
            mock_analyzer,
            priority=["V"],
            data_map=data_map,
            conviction_notes={"V": "Payment duopoly"},
            db=db,
            dry_run=True,
            limit=10,
        )

        mock_analyzer.quick_screen.assert_not_called()
        assert "V" in result

    def test_skips_tickers_with_no_data(self, tmp_path):
        db = _make_db(tmp_path)
        mock_analyzer = MagicMock()

        result = step4_haiku_batch(
            mock_analyzer,
            priority=["NODATA"],
            data_map={},   # empty
            conviction_notes={},
            db=db,
            dry_run=True,
            limit=10,
        )
        assert "NODATA" not in result

    def test_respects_limit(self, tmp_path):
        db = _make_db(tmp_path)
        mock_analyzer = MagicMock()
        data_map = {t: {"name": t} for t in ["A", "B", "C", "D", "E"]}

        result = step4_haiku_batch(
            mock_analyzer,
            priority=["A", "B", "C", "D", "E"],
            data_map=data_map,
            conviction_notes={},
            db=db,
            dry_run=True,
            limit=3,
        )
        assert len(result) == 3


# ─── step6_report (smoke test) ───────────────────────────────────────────────

class TestStep6Report:
    def test_runs_without_error(self, tmp_path, capsys):
        db = _make_db(tmp_path)
        db.upsert_universe_stock("V", source="conviction", quality_score=80.0)
        db.upsert_universe_stock("COST", source="sp500_filter", quality_score=75.0)

        step6_report(db)   # should not raise

        captured = capsys.readouterr()
        assert "Universe" in captured.out or "universe" in captured.out.lower()
