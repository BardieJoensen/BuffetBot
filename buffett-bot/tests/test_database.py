"""
Tests for src/database.py — SQLite schema and budget cap enforcement.

Critical path: can_spend() must be race-free. A bug here means the system
silently ignores budget limits and runs up unexpected API costs.

Also tests:
- WAL mode and pragmas are applied correctly
- Schema initializes cleanly and idempotently
- Data retention cleanup deletes the right rows
- Migration from legacy registry.json works
"""

import json
import sqlite3
import tempfile
import threading
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from src.database import Database, _apply_pragmas


# ─── Fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture
def db(tmp_path):
    """Fresh in-memory-backed Database for each test."""
    return Database(db_path=tmp_path / "test.db")


@pytest.fixture
def tmp_db_path(tmp_path):
    return tmp_path / "test.db"


# ─── Schema & Pragmas ─────────────────────────────────────────────────────


class TestSchema:
    def test_db_file_created(self, tmp_db_path):
        Database(db_path=tmp_db_path)
        assert tmp_db_path.exists()

    def test_init_is_idempotent(self, tmp_db_path):
        """Calling init twice must not raise (IF NOT EXISTS guards)."""
        Database(db_path=tmp_db_path)
        Database(db_path=tmp_db_path)  # should not raise

    def test_wal_mode_enabled(self, tmp_db_path):
        Database(db_path=tmp_db_path)
        conn = sqlite3.connect(str(tmp_db_path))
        _apply_pragmas(conn)
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        conn.close()
        assert mode == "wal"

    def test_foreign_keys_on(self, tmp_db_path):
        Database(db_path=tmp_db_path)
        conn = sqlite3.connect(str(tmp_db_path))
        _apply_pragmas(conn)
        fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
        conn.close()
        assert fk == 1

    def test_all_tables_created(self, db):
        conn = sqlite3.connect(str(db.path))
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        conn.close()
        expected = {
            "universe", "fundamentals", "haiku_screens", "deep_analyses",
            "price_alerts", "run_log", "news_events", "tier_history",
            "budget_caps", "paper_positions",
        }
        assert expected.issubset(tables)

    def test_budget_caps_seeded(self, db):
        """budget_caps table should have default rows after init."""
        status_sonnet = db.get_budget_status("weekly_news_sonnet")
        status_haiku = db.get_budget_status("weekly_news_haiku")
        assert status_sonnet["max_calls"] == 10
        assert status_haiku["max_calls"] == 50
        assert status_sonnet["calls_used"] == 0
        assert status_haiku["calls_used"] == 0


# ─── can_spend() Tests ────────────────────────────────────────────────────


class TestCanSpend:
    def test_allows_spend_under_limit(self, db):
        assert db.can_spend("weekly_news_sonnet") is True

    def test_increments_counter(self, db):
        db.can_spend("weekly_news_sonnet")
        status = db.get_budget_status("weekly_news_sonnet")
        assert status["calls_used"] == 1

    def test_allows_up_to_max(self, db):
        """Should allow exactly max_calls spends."""
        # default max for weekly_news_sonnet = 10
        for _ in range(10):
            assert db.can_spend("weekly_news_sonnet") is True

    def test_denies_at_limit(self, db):
        """11th call must be denied."""
        for _ in range(10):
            db.can_spend("weekly_news_sonnet")
        assert db.can_spend("weekly_news_sonnet") is False

    def test_haiku_limit_independent(self, db):
        """Haiku and Sonnet caps are independent."""
        # Exhaust Sonnet
        for _ in range(10):
            db.can_spend("weekly_news_sonnet")
        # Haiku should still work
        assert db.can_spend("weekly_news_haiku") is True

    def test_unknown_cap_returns_false(self, db):
        """Requesting a non-existent cap type must return False, not raise."""
        result = db.can_spend("nonexistent_cap")
        assert result is False

    def test_concurrent_calls_do_not_exceed_limit(self, tmp_db_path):
        """
        Concurrent threads must not collectively exceed max_calls.

        This validates that BEGIN IMMEDIATE makes the check-then-increment
        atomic even under thread contention.
        """
        db = Database(db_path=tmp_db_path)
        max_calls = db.get_budget_status("weekly_news_sonnet")["max_calls"]  # 10

        results = []
        lock = threading.Lock()

        def try_spend():
            allowed = db.can_spend("weekly_news_sonnet")
            with lock:
                results.append(allowed)

        threads = [threading.Thread(target=try_spend) for _ in range(30)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        allowed_count = sum(1 for r in results if r)
        assert allowed_count == max_calls, (
            f"Expected exactly {max_calls} allowed, got {allowed_count}"
        )

    def test_counter_at_limit_after_concurrent_calls(self, tmp_db_path):
        """After concurrent exhaustion, counter must equal max_calls (not more)."""
        db = Database(db_path=tmp_db_path)
        threads = [threading.Thread(target=lambda: db.can_spend("weekly_news_haiku"))
                   for _ in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        status = db.get_budget_status("weekly_news_haiku")
        assert status["calls_used"] == status["max_calls"]  # 50


# ─── reset_weekly_budgets() ───────────────────────────────────────────────


class TestResetBudgets:
    def test_resets_to_zero(self, db):
        for _ in range(5):
            db.can_spend("weekly_news_sonnet")
        db.reset_weekly_budgets()
        status = db.get_budget_status("weekly_news_sonnet")
        assert status["calls_used"] == 0

    def test_allows_spend_after_reset(self, db):
        for _ in range(10):
            db.can_spend("weekly_news_sonnet")
        db.reset_weekly_budgets()
        assert db.can_spend("weekly_news_sonnet") is True

    def test_resets_all_caps(self, db):
        db.can_spend("weekly_news_sonnet")
        db.can_spend("weekly_news_haiku")
        db.reset_weekly_budgets()
        assert db.get_budget_status("weekly_news_sonnet")["calls_used"] == 0
        assert db.get_budget_status("weekly_news_haiku")["calls_used"] == 0


# ─── Universe & Fundamentals ──────────────────────────────────────────────


class TestUniverse:
    def test_upsert_new_stock(self, db):
        db.upsert_universe_stock("AAPL", source="conviction", company_name="Apple Inc.")
        universe = db.get_universe()
        tickers = [u["ticker"] for u in universe]
        assert "AAPL" in tickers

    def test_upsert_updates_existing(self, db):
        db.upsert_universe_stock("AAPL", source="conviction", quality_score=50.0)
        db.upsert_universe_stock("AAPL", source="conviction", quality_score=75.0)
        universe = db.get_universe()
        aapl = next(u for u in universe if u["ticker"] == "AAPL")
        assert aapl["quality_score"] == 75.0

    def test_filter_by_source(self, db):
        db.upsert_universe_stock("V", source="conviction")
        db.upsert_universe_stock("ACME", source="finviz_screen")
        conviction_stocks = db.get_universe(source_filter="conviction")
        tickers = [u["ticker"] for u in conviction_stocks]
        assert "V" in tickers
        assert "ACME" not in tickers

    def test_update_quality_score(self, db):
        db.upsert_universe_stock("MSFT", source="sp500_filter", quality_score=60.0)
        db.update_quality_score("MSFT", 88.5)
        universe = db.get_universe()
        msft = next(u for u in universe if u["ticker"] == "MSFT")
        assert msft["quality_score"] == pytest.approx(88.5)

    def test_sorted_by_quality_score_desc(self, db):
        db.upsert_universe_stock("LOW", source="finviz_screen", quality_score=20.0)
        db.upsert_universe_stock("HIGH", source="finviz_screen", quality_score=90.0)
        db.upsert_universe_stock("MID", source="finviz_screen", quality_score=55.0)
        universe = db.get_universe()
        scores = [u["quality_score"] for u in universe]
        assert scores == sorted(scores, reverse=True)


# ─── Haiku & Deep Analysis ────────────────────────────────────────────────


class TestAnalysisStorage:
    def test_save_and_retrieve_haiku(self, db):
        db.save_haiku_result("AAPL", passed=True, moat_estimate="WIDE", summary="Strong moat")
        result = db.get_latest_haiku("AAPL")
        assert result is not None
        assert result["passed"] == 1
        assert result["moat_estimate"] == "WIDE"

    def test_haiku_latest_returns_most_recent(self, db):
        db.save_haiku_result("V", passed=False, summary="Weak")
        db.save_haiku_result("V", passed=True, summary="Strong on recheck")
        result = db.get_latest_haiku("V")
        assert result["passed"] == 1

    def test_save_and_retrieve_deep_analysis(self, db):
        db.save_deep_analysis(
            "COST",
            tier="S",
            conviction="HIGH",
            moat_rating="WIDE",
            moat_sources=["membership loyalty", "scale"],
            fair_value=650.0,
            target_entry=520.0,
            investment_thesis="Best retailer",
            key_risks=["competition", "margin pressure"],
        )
        result = db.get_latest_deep_analysis("COST")
        assert result is not None
        assert result["tier"] == "S"
        assert result["moat_sources"] == ["membership loyalty", "scale"]
        assert result["key_risks"] == ["competition", "margin pressure"]

    def test_no_analysis_returns_none(self, db):
        assert db.get_latest_deep_analysis("UNKNOWN") is None

    def test_expiring_analyses_detected(self, db):
        """An analysis that expires in 10 days should appear in get_expiring_analyses(30)."""
        db.save_deep_analysis("EXPIRING", tier="B", expires_days=10)
        expiring = db.get_expiring_analyses(within_days=30)
        assert "EXPIRING" in expiring

    def test_fresh_analysis_not_expiring(self, db):
        db.save_deep_analysis("FRESH", tier="A", expires_days=120)
        expiring = db.get_expiring_analyses(within_days=30)
        assert "FRESH" not in expiring


# ─── Tier History ─────────────────────────────────────────────────────────


class TestTierHistory:
    def test_log_initial_assignment(self, db):
        db.log_tier_change("AAPL", new_tier="B", trigger="bulk_load")
        history = db.get_tier_history("AAPL")
        assert len(history) == 1
        assert history[0]["new_tier"] == "B"
        assert history[0]["old_tier"] is None

    def test_log_tier_upgrade(self, db):
        db.log_tier_change("AAPL", new_tier="B", trigger="bulk_load")
        db.log_tier_change("AAPL", new_tier="A", old_tier="B", trigger="price_move")
        history = db.get_tier_history("AAPL")
        assert history[0]["new_tier"] == "A"   # most recent first
        assert history[0]["old_tier"] == "B"

    def test_history_is_immutable_append_only(self, db):
        db.log_tier_change("V", new_tier="S", trigger="scheduled")
        db.log_tier_change("V", new_tier="B", old_tier="S", trigger="news_event")
        history = db.get_tier_history("V")
        assert len(history) == 2


# ─── Run Log ─────────────────────────────────────────────────────────────


class TestRunLog:
    def test_start_and_complete_run(self, db):
        run_id = db.start_run("weekly_refresh")
        assert run_id is not None
        db.complete_run(run_id, haiku_calls=50, sonnet_calls=5, total_cost_usd=0.15)
        history = db.get_run_history(limit=1)
        assert history[0]["haiku_calls"] == 50
        assert history[0]["total_cost_usd"] == pytest.approx(0.15)
        assert history[0]["completed_at"] is not None


# ─── Data Retention ───────────────────────────────────────────────────────


class TestRetention:
    def test_old_fundamentals_pruned(self, db):
        """Sub-monthly fundamentals older than 2 years should be deleted."""
        db.upsert_universe_stock("TEST", source="finviz_screen")
        # Insert a non-1st-of-month row from 3 years ago
        old_date = (datetime.now() - timedelta(days=365 * 3)).strftime("%Y-%m-15")
        db.save_fundamentals("TEST", {"price": 100.0}, as_of_date=old_date)

        # Insert a recent row (should survive)
        db.save_fundamentals("TEST", {"price": 105.0})

        result = db.run_retention_cleanup()
        assert result["fundamentals"] >= 1

    def test_recent_fundamentals_preserved(self, db):
        """Recent fundamentals should not be deleted."""
        db.upsert_universe_stock("KEEP", source="finviz_screen")
        today = date.today().isoformat()
        db.save_fundamentals("KEEP", {"price": 200.0}, as_of_date=today)
        db.run_retention_cleanup()

        # Verify the recent record still exists by checking it's not in expiring
        conn = sqlite3.connect(str(db.path))
        count = conn.execute(
            "SELECT COUNT(*) FROM fundamentals WHERE ticker = 'KEEP'"
        ).fetchone()[0]
        conn.close()
        assert count == 1

    def test_old_haiku_screens_pruned(self, db):
        """Haiku screens with expires_at older than 1 year should be deleted."""
        # Insert a haiku result with a very short expiry
        db.save_haiku_result("OLD_SCREEN", passed=False, expires_days=1)
        # Manually backdate it in the DB
        conn = sqlite3.connect(str(db.path))
        old_ts = (datetime.now() - timedelta(days=400)).isoformat()
        conn.execute(
            "UPDATE haiku_screens SET expires_at = ? WHERE ticker = 'OLD_SCREEN'",
            (old_ts,),
        )
        conn.commit()
        conn.close()

        result = db.run_retention_cleanup()
        assert result["haiku_screens"] >= 1


# ─── Migration ────────────────────────────────────────────────────────────


class TestMigration:
    def _make_registry(self, tmp_path: Path) -> Path:
        registry = {
            "version": 1,
            "campaign": {
                "campaign_id": "2026-Q1",
                "started_at": "2026-01-01T00:00:00",
                "haiku_screened": ["AAON", "ADUS"],
                "haiku_passed": ["AAON"],
                "haiku_failed": {},
                "analyzed": ["AAON"],
            },
            "studies": {
                "AAON": {
                    "symbol": "AAON",
                    "company_name": "AAON Inc.",
                    "sector": "Industrials",
                    "tier": 2,
                    "tier_reason": "High quality but overpriced",
                    "target_entry_price": 75.0,
                    "current_price_at_analysis": 127.43,
                    "analyzed_at": "2026-02-22T10:00:00",
                    "screener_score": 0.65,
                    "analysis": {
                        "moat_rating": "narrow",
                        "conviction": "MEDIUM",
                        "investment_thesis": "Best HVAC company",
                        "key_risks": ["valuation"],
                        "thesis_risks": ["competition"],
                    },
                },
                "CASH": {
                    "symbol": "CASH",
                    "company_name": "Pathfinder Bancorp",
                    "sector": "Financials",
                    "tier": 1,
                    "tier_reason": "Wonderful at fair value",
                    "target_entry_price": 20.0,
                    "current_price_at_analysis": 18.50,
                    "analyzed_at": "2026-02-20T10:00:00",
                    "screener_score": 0.72,
                    "analysis": {
                        "moat_rating": "wide",
                        "conviction": "HIGH",
                        "investment_thesis": "Regional bank with strong moat",
                        "key_risks": [],
                        "thesis_risks": [],
                    },
                },
            },
        }
        path = tmp_path / "registry.json"
        path.write_text(json.dumps(registry))
        return path

    def test_migrates_study_count(self, db, tmp_path):
        registry_path = self._make_registry(tmp_path)
        count = db.migrate_from_registry(registry_path)
        assert count == 2

    def test_migrates_to_universe(self, db, tmp_path):
        registry_path = self._make_registry(tmp_path)
        db.migrate_from_registry(registry_path)
        universe = db.get_universe()
        tickers = [u["ticker"] for u in universe]
        assert "AAON" in tickers
        assert "CASH" in tickers

    def test_tier1_maps_to_s(self, db, tmp_path):
        """Old tier 1 (wonderful at fair value) → S."""
        registry_path = self._make_registry(tmp_path)
        db.migrate_from_registry(registry_path)
        analysis = db.get_latest_deep_analysis("CASH")
        assert analysis["tier"] == "S"

    def test_tier2_maps_to_b(self, db, tmp_path):
        """Old tier 2 (high quality but overpriced) → B."""
        registry_path = self._make_registry(tmp_path)
        db.migrate_from_registry(registry_path)
        analysis = db.get_latest_deep_analysis("AAON")
        assert analysis["tier"] == "B"

    def test_tier_history_logged(self, db, tmp_path):
        registry_path = self._make_registry(tmp_path)
        db.migrate_from_registry(registry_path)
        history = db.get_tier_history("AAON")
        assert len(history) >= 1
        assert history[0]["trigger"] == "bulk_load"

    def test_missing_registry_returns_zero(self, db, tmp_path):
        result = db.migrate_from_registry(tmp_path / "nonexistent.json")
        assert result == 0

    def test_empty_registry_returns_zero(self, db, tmp_path):
        empty = {"version": 1, "campaign": {}, "studies": {}}
        path = tmp_path / "registry.json"
        path.write_text(json.dumps(empty))
        result = db.migrate_from_registry(path)
        assert result == 0
