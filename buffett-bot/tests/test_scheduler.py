"""
Tests for Phase D scheduler additions.

Covers:
  - Database query methods added for scheduling
    (spend_batch, get_universe_stock, get_latest_fundamentals,
     get_unscreened_tickers, get_expiring_haiku_tickers,
     get_haiku_passes_without_analysis)
  - Scheduler helper functions (_moat_label, _build_db_summary)
  - Scheduler job functions with mocked external dependencies
    (monday_maintenance, wednesday_haiku_batch, friday_sonnet_batch)

External dependencies (yfinance, Anthropic API, Alpaca) are always mocked.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ── Bootstrap path ──────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))
sys.path.insert(0, str(_PROJECT_ROOT / "scripts"))

# Stub container-only packages so scheduler.py and src.analyzer can be
# imported on the host test runner.  These live in the Docker image but
# are not installed when running pytest locally.
# anthropic needs sub-module stubs too (src/analyzer.py imports anthropic.types).
_anthropic_mock = MagicMock()
for _pkg in (
    "schedule", "dotenv",
    "anthropic", "anthropic.types", "anthropic.lib", "anthropic.lib.streaming",
):
    sys.modules.setdefault(_pkg, _anthropic_mock)

from src.database import Database


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def db(tmp_path):
    """Fresh Database with WAL mode for each test."""
    return Database(db_path=tmp_path / "test.db")


def _upsert_stock(db, ticker, quality_score=70.0, source="finviz_screen"):
    db.upsert_universe_stock(ticker, source=source, quality_score=quality_score)


def _save_haiku(db, ticker, passed=True, expires_delta_days=180):
    """Insert a haiku_screens row with a controlled expiry."""
    now = datetime.now()
    expires = (now + timedelta(days=expires_delta_days)).isoformat()
    import sqlite3
    from src.database import _open
    with _open(db.path) as conn:
        conn.execute(
            """
            INSERT INTO haiku_screens (ticker, screened_at, passed, expires_at)
            VALUES (?, ?, ?, ?)
            """,
            (ticker, now.isoformat(), int(passed), expires),
        )


def _save_deep_analysis(db, ticker, tier="B", expires_delta_days=180):
    db.save_deep_analysis(ticker, tier=tier, expires_days=expires_delta_days)


# ─── spend_batch ─────────────────────────────────────────────────────────────


class TestSpendBatch:
    def test_reserves_n_slots(self, db):
        result = db.spend_batch("weekly_haiku_screen", 5)
        assert result == 5

        status = db.get_budget_status("weekly_haiku_screen")
        assert status["calls_used"] == 5

    def test_partial_reserve_when_budget_nearly_exhausted(self, db):
        # Drain all but 3 slots
        cap = db.get_budget_status("weekly_haiku_screen")
        remaining = cap["max_calls"] - 3
        db.spend_batch("weekly_haiku_screen", remaining)

        # Requesting 10 should only give back 3
        result = db.spend_batch("weekly_haiku_screen", 10)
        assert result == 3

    def test_exhausted_returns_zero(self, db):
        cap = db.get_budget_status("weekly_haiku_screen")
        db.spend_batch("weekly_haiku_screen", cap["max_calls"])

        result = db.spend_batch("weekly_haiku_screen", 1)
        assert result == 0

    def test_unknown_type_returns_zero(self, db):
        result = db.spend_batch("nonexistent_cap_type", 5)
        assert result == 0

    def test_new_cap_types_seeded(self, db):
        haiku_status = db.get_budget_status("weekly_haiku_screen")
        sonnet_status = db.get_budget_status("weekly_sonnet_analysis")

        assert haiku_status["max_calls"] == 50
        assert sonnet_status["max_calls"] == 10
        assert haiku_status["calls_used"] == 0
        assert sonnet_status["calls_used"] == 0

    def test_reset_clears_spend_batch_usage(self, db):
        db.spend_batch("weekly_haiku_screen", 20)
        db.reset_weekly_budgets()

        status = db.get_budget_status("weekly_haiku_screen")
        assert status["calls_used"] == 0


# ─── get_universe_stock ───────────────────────────────────────────────────────


class TestGetUniverseStock:
    def test_returns_dict_for_known_ticker(self, db):
        _upsert_stock(db, "AAPL", quality_score=90.0, source="conviction")
        row = db.get_universe_stock("AAPL")
        assert row is not None
        assert row["ticker"] == "AAPL"
        assert row["source"] == "conviction"
        assert row["quality_score"] == pytest.approx(90.0)

    def test_returns_none_for_unknown_ticker(self, db):
        assert db.get_universe_stock("ZZZZ") is None

    def test_returns_most_recent_after_upsert(self, db):
        _upsert_stock(db, "MSFT", quality_score=70.0)
        db.upsert_universe_stock("MSFT", source="finviz_screen", quality_score=85.0)
        row = db.get_universe_stock("MSFT")
        assert row["quality_score"] == pytest.approx(85.0)


# ─── get_latest_fundamentals ─────────────────────────────────────────────────


class TestGetLatestFundamentals:
    def test_returns_none_when_no_fundamentals(self, db):
        _upsert_stock(db, "AAPL")
        assert db.get_latest_fundamentals("AAPL") is None

    def test_returns_fundamentals_dict(self, db):
        _upsert_stock(db, "AAPL")
        db.save_fundamentals("AAPL", {"price": 195.0, "roe": 0.17}, as_of_date="2026-03-03")
        row = db.get_latest_fundamentals("AAPL")
        assert row is not None
        assert row["price"] == pytest.approx(195.0)
        assert row["roe"] == pytest.approx(0.17)

    def test_returns_most_recent_row(self, db):
        _upsert_stock(db, "AAPL")
        db.save_fundamentals("AAPL", {"price": 180.0}, as_of_date="2026-01-01")
        db.save_fundamentals("AAPL", {"price": 195.0}, as_of_date="2026-03-03")

        row = db.get_latest_fundamentals("AAPL")
        assert row["price"] == pytest.approx(195.0)
        assert row["date"] == "2026-03-03"


# ─── get_unscreened_tickers ──────────────────────────────────────────────────


class TestGetUnscreenedTickers:
    def test_returns_ticker_with_no_haiku(self, db):
        _upsert_stock(db, "AAPL", quality_score=90.0)
        result = db.get_unscreened_tickers()
        assert "AAPL" in result

    def test_excludes_ticker_with_valid_haiku(self, db):
        _upsert_stock(db, "AAPL")
        _save_haiku(db, "AAPL", expires_delta_days=180)
        result = db.get_unscreened_tickers()
        assert "AAPL" not in result

    def test_includes_ticker_with_expired_haiku(self, db):
        _upsert_stock(db, "AAPL")
        _save_haiku(db, "AAPL", expires_delta_days=-1)  # expired yesterday
        result = db.get_unscreened_tickers()
        assert "AAPL" in result

    def test_ranked_by_quality_score_descending(self, db):
        _upsert_stock(db, "LOW_Q", quality_score=30.0)
        _upsert_stock(db, "HIGH_Q", quality_score=90.0)
        result = db.get_unscreened_tickers()
        assert result.index("HIGH_Q") < result.index("LOW_Q")

    def test_limit_respected(self, db):
        for i in range(10):
            _upsert_stock(db, f"TICK{i:02d}", quality_score=float(i))
        result = db.get_unscreened_tickers(limit=3)
        assert len(result) == 3

    def test_excludes_out_of_universe_stocks(self, db):
        _upsert_stock(db, "AAPL")
        import sqlite3
        from src.database import _open
        with _open(db.path) as conn:
            conn.execute("UPDATE universe SET in_universe = 0 WHERE ticker = 'AAPL'")
        result = db.get_unscreened_tickers()
        assert "AAPL" not in result


# ─── get_expiring_haiku_tickers ──────────────────────────────────────────────


class TestGetExpiringHaikuTickers:
    def test_returns_ticker_expiring_within_window(self, db):
        _upsert_stock(db, "AAPL")
        _save_haiku(db, "AAPL", expires_delta_days=20)  # expires in 20 days
        result = db.get_expiring_haiku_tickers(within_days=30)
        assert "AAPL" in result

    def test_excludes_ticker_not_yet_expiring(self, db):
        _upsert_stock(db, "AAPL")
        _save_haiku(db, "AAPL", expires_delta_days=60)  # expires in 60 days
        result = db.get_expiring_haiku_tickers(within_days=30)
        assert "AAPL" not in result

    def test_excludes_already_expired_ticker(self, db):
        # Already expired → belongs in get_unscreened_tickers, not here
        _upsert_stock(db, "AAPL")
        _save_haiku(db, "AAPL", expires_delta_days=-5)
        result = db.get_expiring_haiku_tickers(within_days=30)
        assert "AAPL" not in result

    def test_limit_respected(self, db):
        for i in range(10):
            _upsert_stock(db, f"TICK{i:02d}")
            _save_haiku(db, f"TICK{i:02d}", expires_delta_days=10)
        result = db.get_expiring_haiku_tickers(within_days=30, limit=4)
        assert len(result) == 4


# ─── get_haiku_passes_without_analysis ───────────────────────────────────────


class TestGetHaikuPassesWithoutAnalysis:
    def test_returns_haiku_pass_without_deep_analysis(self, db):
        _upsert_stock(db, "AAPL")
        _save_haiku(db, "AAPL", passed=True)
        result = db.get_haiku_passes_without_analysis()
        assert "AAPL" in result

    def test_excludes_haiku_fail(self, db):
        _upsert_stock(db, "AAPL")
        _save_haiku(db, "AAPL", passed=False)
        result = db.get_haiku_passes_without_analysis()
        assert "AAPL" not in result

    def test_excludes_ticker_with_valid_deep_analysis(self, db):
        _upsert_stock(db, "AAPL")
        _save_haiku(db, "AAPL", passed=True)
        _save_deep_analysis(db, "AAPL", expires_delta_days=60)
        result = db.get_haiku_passes_without_analysis()
        assert "AAPL" not in result

    def test_includes_ticker_with_expired_deep_analysis(self, db):
        _upsert_stock(db, "AAPL")
        _save_haiku(db, "AAPL", passed=True)
        _save_deep_analysis(db, "AAPL", expires_delta_days=-1)  # expired
        result = db.get_haiku_passes_without_analysis()
        assert "AAPL" in result

    def test_excludes_expired_haiku(self, db):
        _upsert_stock(db, "AAPL")
        _save_haiku(db, "AAPL", passed=True, expires_delta_days=-1)  # expired haiku
        result = db.get_haiku_passes_without_analysis()
        assert "AAPL" not in result

    def test_limit_respected(self, db):
        for i in range(8):
            _upsert_stock(db, f"TICK{i:02d}")
            _save_haiku(db, f"TICK{i:02d}", passed=True)
        result = db.get_haiku_passes_without_analysis(limit=5)
        assert len(result) == 5


# ─── Scheduler helpers ────────────────────────────────────────────────────────


class TestMoatLabel:
    def test_wide(self):
        from scheduler import _moat_label
        assert _moat_label(4) == "WIDE"
        assert _moat_label(5) == "WIDE"

    def test_narrow(self):
        from scheduler import _moat_label
        assert _moat_label(3) == "NARROW"

    def test_none(self):
        from scheduler import _moat_label
        assert _moat_label(2) == "NONE"
        assert _moat_label(0) == "NONE"


class TestBuildDbSummary:
    def test_returns_nonempty_string_with_data(self, db):
        from scheduler import _build_db_summary
        _upsert_stock(db, "AAPL", quality_score=90.0)
        db.upsert_universe_stock(
            "AAPL", company_name="Apple Inc", sector="Technology",
            market_cap=3e12, source="conviction",
        )
        db.save_fundamentals("AAPL", {"price": 195.0, "roe": 0.17, "roic": 0.45}, as_of_date="2026-03-03")
        summary = _build_db_summary("AAPL", db)
        assert "AAPL" in summary
        assert "Apple Inc" in summary
        assert "Technology" in summary
        assert "195" in summary

    def test_returns_ticker_line_when_no_fundamentals(self, db):
        from scheduler import _build_db_summary
        _upsert_stock(db, "AAPL")
        summary = _build_db_summary("AAPL", db)
        assert "AAPL" in summary

    def test_returns_ticker_line_for_unknown_stock(self, db):
        from scheduler import _build_db_summary
        summary = _build_db_summary("ZZZZ", db)
        assert "ZZZZ" in summary


# ─── monday_maintenance ───────────────────────────────────────────────────────


class TestMondayMaintenance:
    def test_resets_weekly_budget_caps(self, tmp_path):
        """After maintenance, budget caps should be reset to 0."""
        from scheduler import monday_maintenance

        db = Database(tmp_path / "test.db")
        # Spend some budget to simulate prior usage
        db.spend_batch("weekly_haiku_screen", 15)
        assert db.get_budget_status("weekly_haiku_screen")["calls_used"] == 15

        with patch("src.database.Database", return_value=db), \
             patch("src.screener.StockScreener") as MockScreener, \
             patch("src.quality_scorer.compute_quality_scores", return_value={}), \
             patch("yfinance.Ticker"), \
             patch("src.paper_trader.PaperTrader") as MockTrader:
            MockScreener.return_value.screen_tickers.return_value = []
            MockTrader.return_value.is_enabled.return_value = False
            monday_maintenance()

        assert db.get_budget_status("weekly_haiku_screen")["calls_used"] == 0

    def test_syncs_paper_positions_when_alpaca_enabled(self, tmp_path):
        """Paper positions should be upserted when Alpaca is configured."""
        from scheduler import monday_maintenance

        db = Database(tmp_path / "test.db")
        _upsert_stock(db, "AAPL")

        mock_position = {
            "symbol": "AAPL",
            "current_price": 195.0,
            "market_value": 1950.0,
            "unrealized_plpc": 0.05,
            "qty": 10.0,
        }

        with patch("src.database.Database", return_value=db), \
             patch("src.screener.StockScreener") as MockScreener, \
             patch("src.quality_scorer.compute_quality_scores", return_value={}), \
             patch("yfinance.Ticker"), \
             patch("src.paper_trader.PaperTrader") as MockTrader:
            MockScreener.return_value.screen_tickers.return_value = []
            trader_instance = MockTrader.return_value
            trader_instance.is_enabled.return_value = True
            trader_instance.get_positions.return_value = [mock_position]
            monday_maintenance()

        positions = db.get_paper_positions()
        assert any(p["ticker"] == "AAPL" for p in positions)

    def test_skips_position_sync_when_alpaca_disabled(self, tmp_path):
        """No paper_positions rows should be written when Alpaca is off."""
        from scheduler import monday_maintenance

        db = Database(tmp_path / "test.db")

        with patch("src.database.Database", return_value=db), \
             patch("src.screener.StockScreener") as MockScreener, \
             patch("src.quality_scorer.compute_quality_scores", return_value={}), \
             patch("yfinance.Ticker"), \
             patch("src.paper_trader.PaperTrader") as MockTrader:
            MockScreener.return_value.screen_tickers.return_value = []
            MockTrader.return_value.is_enabled.return_value = False
            monday_maintenance()

        assert db.get_paper_positions() == []

    def test_does_not_raise_on_empty_universe(self, tmp_path):
        """maintenance should succeed even if the universe is empty."""
        from scheduler import monday_maintenance

        db = Database(tmp_path / "test.db")

        with patch("src.database.Database", return_value=db), \
             patch("src.screener.StockScreener") as MockScreener, \
             patch("src.quality_scorer.compute_quality_scores", return_value={}), \
             patch("yfinance.Ticker"), \
             patch("src.paper_trader.PaperTrader") as MockTrader:
            MockScreener.return_value.screen_tickers.return_value = []
            MockTrader.return_value.is_enabled.return_value = False
            monday_maintenance()  # should not raise


# ─── wednesday_haiku_batch ────────────────────────────────────────────────────


class TestWednesdayHaikuBatch:
    def test_skips_when_budget_exhausted(self, tmp_path):
        """batch_quick_screen must NOT be called when budget is at max."""
        from scheduler import wednesday_haiku_batch

        db = Database(tmp_path / "test.db")
        _upsert_stock(db, "AAPL")
        # Exhaust budget
        db.spend_batch("weekly_haiku_screen", db.get_budget_status("weekly_haiku_screen")["max_calls"])

        with patch("src.database.Database", return_value=db), \
             patch("src.analyzer.CompanyAnalyzer") as MockAnalyzer:
            wednesday_haiku_batch()
            MockAnalyzer.return_value.batch_quick_screen.assert_not_called()

    def test_skips_when_no_candidates(self, tmp_path):
        """If all universe stocks have valid Haiku results, nothing is submitted."""
        from scheduler import wednesday_haiku_batch

        db = Database(tmp_path / "test.db")
        _upsert_stock(db, "AAPL")
        _save_haiku(db, "AAPL", expires_delta_days=180)  # valid result

        with patch("src.database.Database", return_value=db), \
             patch("src.analyzer.CompanyAnalyzer") as MockAnalyzer:
            wednesday_haiku_batch()
            MockAnalyzer.return_value.batch_quick_screen.assert_not_called()

    def test_submits_batch_and_saves_results(self, tmp_path):
        """Unscreened ticker → batch submitted → result saved to haiku_screens."""
        from scheduler import wednesday_haiku_batch

        db = Database(tmp_path / "test.db")
        _upsert_stock(db, "AAPL", quality_score=85.0)
        db.save_fundamentals("AAPL", {"price": 195.0, "roe": 0.17}, as_of_date="2026-03-03")

        fake_result = [
            {"symbol": "AAPL", "worth_analysis": True, "moat_hint": 4, "reason": "Wide moat"}
        ]

        with patch("src.database.Database", return_value=db), \
             patch("src.analyzer.CompanyAnalyzer") as MockAnalyzer:
            MockAnalyzer.return_value.batch_quick_screen.return_value = fake_result
            wednesday_haiku_batch()

        haiku = db.get_latest_haiku("AAPL")
        assert haiku is not None
        assert haiku["passed"] == 1
        assert haiku["moat_estimate"] == "WIDE"

    def test_budget_is_consumed_by_batch_count(self, tmp_path):
        """Exactly N slots should be consumed for an N-ticker batch."""
        from scheduler import wednesday_haiku_batch

        db = Database(tmp_path / "test.db")
        for sym in ["AAPL", "MSFT", "GOOG"]:
            _upsert_stock(db, sym)
            db.save_fundamentals(sym, {"price": 100.0}, as_of_date="2026-03-03")

        fake_results = [
            {"symbol": s, "worth_analysis": True, "moat_hint": 3, "reason": "ok"}
            for s in ["AAPL", "MSFT", "GOOG"]
        ]

        with patch("src.database.Database", return_value=db), \
             patch("src.analyzer.CompanyAnalyzer") as MockAnalyzer:
            MockAnalyzer.return_value.batch_quick_screen.return_value = fake_results
            wednesday_haiku_batch()

        status = db.get_budget_status("weekly_haiku_screen")
        assert status["calls_used"] == 3

    def test_does_not_raise_on_api_error(self, tmp_path):
        """Job must not propagate exceptions; it logs and returns."""
        from scheduler import wednesday_haiku_batch

        db = Database(tmp_path / "test.db")
        _upsert_stock(db, "AAPL")

        with patch("src.database.Database", return_value=db), \
             patch("src.analyzer.CompanyAnalyzer") as MockAnalyzer:
            MockAnalyzer.return_value.batch_quick_screen.side_effect = RuntimeError("API down")
            wednesday_haiku_batch()  # must not raise


# ─── friday_sonnet_batch ──────────────────────────────────────────────────────


class TestFridaySonnetBatch:
    def test_skips_when_budget_exhausted(self, tmp_path):
        from scheduler import friday_sonnet_batch

        db = Database(tmp_path / "test.db")
        _upsert_stock(db, "AAPL")
        _save_haiku(db, "AAPL", passed=True)
        db.spend_batch("weekly_sonnet_analysis", db.get_budget_status("weekly_sonnet_analysis")["max_calls"])

        with patch("src.database.Database", return_value=db), \
             patch("src.analyzer.CompanyAnalyzer") as MockAnalyzer:
            friday_sonnet_batch()
            MockAnalyzer.return_value.batch_analyze_companies.assert_not_called()

    def test_skips_when_no_haiku_passes(self, tmp_path):
        from scheduler import friday_sonnet_batch

        db = Database(tmp_path / "test.db")
        _upsert_stock(db, "AAPL")
        _save_haiku(db, "AAPL", passed=False)  # failed Haiku

        with patch("src.database.Database", return_value=db), \
             patch("src.analyzer.CompanyAnalyzer") as MockAnalyzer:
            friday_sonnet_batch()
            MockAnalyzer.return_value.batch_analyze_companies.assert_not_called()

    def test_submits_batch_and_saves_tier(self, tmp_path):
        """Haiku pass → batch_analyze_companies called → tier saved."""
        from scheduler import friday_sonnet_batch

        db = Database(tmp_path / "test.db")
        db.upsert_universe_stock("AAPL", company_name="Apple Inc", sector="Technology",
                                 source="conviction", quality_score=90.0)
        _save_haiku(db, "AAPL", passed=True)

        # Mock AnalysisV2 object
        mock_analysis = MagicMock()
        mock_analysis.symbol = "AAPL"
        mock_analysis.conviction = "HIGH"
        mock_analysis.moat_rating.value = "WIDE"
        mock_analysis.moat_sources = ["brand", "switching_costs"]
        mock_analysis.estimated_fair_value_low = 200.0
        mock_analysis.estimated_fair_value_high = 250.0
        mock_analysis.target_entry_price = 190.0
        mock_analysis.current_price = 180.0
        mock_analysis.summary = "Exceptional business with durable moat"
        mock_analysis.key_risks = ["competition"]
        mock_analysis.thesis_risks = ["margin pressure"]

        # Mock TierAssignment
        mock_tier_assignment = MagicMock()
        mock_tier_assignment.tier = "A"
        mock_tier_assignment.tier_reason = "High quality, priced reasonably"
        mock_tier_assignment.price_gap_pct = -0.05

        with patch("src.database.Database", return_value=db), \
             patch("src.analyzer.CompanyAnalyzer") as MockAnalyzer, \
             patch("src.tier_engine.assign_tier", return_value=mock_tier_assignment), \
             patch("src.tier_engine.staged_entry_suggestion", return_value={"1/2": 190, "2/2": 180}):
            MockAnalyzer.return_value.batch_analyze_companies.return_value = [mock_analysis]
            friday_sonnet_batch()

        # Verify deep analysis saved
        da = db.get_latest_deep_analysis("AAPL")
        assert da is not None
        assert da["tier"] == "A"

        # Verify tier history logged
        history = db.get_tier_history("AAPL")
        assert len(history) >= 1
        assert history[0]["new_tier"] == "A"
        assert history[0]["trigger"] == "scheduled"

        # Verify price alert created (tier A → price alert)
        alerts = db.get_price_alerts()
        assert any(a["ticker"] == "AAPL" for a in alerts)

    def test_does_not_raise_on_api_error(self, tmp_path):
        from scheduler import friday_sonnet_batch

        db = Database(tmp_path / "test.db")
        _upsert_stock(db, "AAPL")
        _save_haiku(db, "AAPL", passed=True)

        with patch("src.database.Database", return_value=db), \
             patch("src.analyzer.CompanyAnalyzer") as MockAnalyzer:
            MockAnalyzer.return_value.batch_analyze_companies.side_effect = RuntimeError("API down")
            friday_sonnet_batch()  # must not raise

    def test_budget_consumed_by_actual_analyses(self, tmp_path):
        """Slots reserved == number of candidates submitted (up to limit=5)."""
        from scheduler import friday_sonnet_batch

        db = Database(tmp_path / "test.db")
        for sym in ["AAPL", "MSFT"]:
            db.upsert_universe_stock(sym, company_name=f"{sym} Inc", sector="Tech",
                                     source="conviction", quality_score=90.0)
            _save_haiku(db, sym, passed=True)

        mock_analyses = []
        for sym in ["AAPL", "MSFT"]:
            m = MagicMock()
            m.symbol = sym
            m.conviction = "MEDIUM"
            m.moat_rating.value = "NARROW"
            m.moat_sources = []
            m.estimated_fair_value_low = 100.0
            m.estimated_fair_value_high = 120.0
            m.target_entry_price = 95.0
            m.current_price = 100.0
            m.summary = "Decent business"
            m.key_risks = []
            m.thesis_risks = []
            mock_analyses.append(m)

        mock_tier = MagicMock()
        mock_tier.tier = "B"
        mock_tier.tier_reason = "Watch"
        mock_tier.price_gap_pct = 0.10

        with patch("src.database.Database", return_value=db), \
             patch("src.analyzer.CompanyAnalyzer") as MockAnalyzer, \
             patch("src.tier_engine.assign_tier", return_value=mock_tier), \
             patch("src.tier_engine.staged_entry_suggestion", return_value={}):
            MockAnalyzer.return_value.batch_analyze_companies.return_value = mock_analyses
            friday_sonnet_batch()

        status = db.get_budget_status("weekly_sonnet_analysis")
        assert status["calls_used"] == 2
