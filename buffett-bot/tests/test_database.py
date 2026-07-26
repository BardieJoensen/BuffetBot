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
import threading
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from src.database import BUDGET_CAPS_DEFAULTS, Database, _apply_pragmas, _open

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
            "universe",
            "fundamentals",
            "haiku_screens",
            "deep_analyses",
            "price_alerts",
            "run_log",
            "news_events",
            "tier_history",
            "budget_caps",
            "paper_positions",
            "decision_log",
            "closed_trades",
            "pit_fundamentals",
            "portfolio_snapshots",
            "regime_log",
            "income_events",
        }
        assert expected.issubset(tables)

    def test_budget_caps_seeded(self, db):
        """budget_caps table should have default rows after init."""
        # Assert against the constant rather than literals so tuning a cap
        # doesn't require editing this test.
        for cap_type, max_calls in BUDGET_CAPS_DEFAULTS:
            status = db.get_budget_status(cap_type)
            assert status["max_calls"] == max_calls
            assert status["calls_used"] == 0


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
        assert allowed_count == max_calls, f"Expected exactly {max_calls} allowed, got {allowed_count}"

    def test_counter_at_limit_after_concurrent_calls(self, tmp_db_path):
        """After concurrent exhaustion, counter must equal max_calls (not more)."""
        db = Database(db_path=tmp_db_path)
        max_calls = db.get_budget_status("weekly_news_haiku")["max_calls"]
        # Oversubscribe the cap so exhaustion is actually reached regardless of
        # how the cap is tuned.
        threads = [threading.Thread(target=lambda: db.can_spend("weekly_news_haiku")) for _ in range(max_calls + 50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        status = db.get_budget_status("weekly_news_haiku")
        assert status["calls_used"] == max_calls


# ─── set_trade_benchmark ──────────────────────────────────────────────────


class TestSetTradeBenchmark:
    """
    Alpha and reasoning_sound are both derived from benchmark_return, so
    repairing a benchmark has to re-derive both or the row goes internally
    inconsistent.
    """

    def _closed_trade(
        self,
        db,
        *,
        ticker="GIS",
        entry=37.198632,
        exit_=36.145,
        reason="Take profit: near fair value",
        fair_value=42.53,
    ):
        # fair_value drives `converged`, which _score_reasoning_soundness needs
        # alongside alpha — without it soundness is None regardless of alpha.
        buy_id = db.log_decision(
            ticker,
            "buy",
            tier="B",
            price=entry,
            shares=100.0,
            notional=entry * 100,
            order_id="b1",
            reason="entry",
            regime="fair_value",
            reasoning_snapshot={"fair_value": fair_value},
        )
        sell_id = db.log_decision(
            ticker,
            "sell",
            tier="B",
            price=exit_,
            shares=100.0,
            notional=exit_ * 100,
            order_id="s1",
            reason=reason,
            regime="fair_value",
            reasoning_snapshot={},
        )
        assert buy_id and sell_id
        return db.close_trade(ticker, exit_decision_id=sell_id, entry_price=entry, exit_price=exit_, shares=100.0)

    def test_sets_benchmark_and_derives_alpha(self, db):
        trade_id = self._closed_trade(db)
        assert db.set_trade_benchmark(trade_id, 0.013655) is True

        t = db.get_closed_trades(ticker="GIS")[0]
        assert t["benchmark_return"] == pytest.approx(0.013655)
        assert t["alpha"] == pytest.approx(t["realized_pl_pct"] - 0.013655)

    def test_repairs_a_previously_missing_benchmark(self, db):
        trade_id = self._closed_trade(db)
        assert db.get_closed_trades(ticker="GIS")[0]["alpha"] is None

        db.set_trade_benchmark(trade_id, 0.0)
        t = db.get_closed_trades(ticker="GIS")[0]
        assert t["alpha"] == pytest.approx(t["realized_pl_pct"])

    def test_recomputes_from_realized_not_from_prior_alpha(self, db):
        """Idempotency: re-applying the same benchmark must not drift."""
        trade_id = self._closed_trade(db)
        db.set_trade_benchmark(trade_id, 0.02)
        first = db.get_closed_trades(ticker="GIS")[0]["alpha"]
        db.set_trade_benchmark(trade_id, 0.02)
        assert db.get_closed_trades(ticker="GIS")[0]["alpha"] == pytest.approx(first)

    def test_rescoring_take_profit_soundness(self, db):
        """A take-profit that lost to the benchmark is not sound reasoning."""
        trade_id = self._closed_trade(db, reason="Take profit: near fair value")
        db.set_trade_benchmark(trade_id, 0.013655)

        t = db.get_closed_trades(ticker="GIS")[0]
        assert t["sell_category"] == "take_profit"
        assert t["alpha"] < 0
        assert t["reasoning_sound"] == 0

    def test_thesis_breaker_soundness_stays_unknown(self, db):
        """Judging a thesis breaker needs post-exit prices, so it stays None."""
        trade_id = self._closed_trade(db, ticker="AD", reason="Thesis breaker: downgraded to C-tier")
        db.set_trade_benchmark(trade_id, 0.0)

        t = db.get_closed_trades(ticker="AD")[0]
        assert t["sell_category"] == "thesis_breaker"
        assert t["alpha"] is not None
        assert t["reasoning_sound"] is None

    def test_none_benchmark_clears_alpha(self, db):
        trade_id = self._closed_trade(db)
        db.set_trade_benchmark(trade_id, 0.01)
        db.set_trade_benchmark(trade_id, None)

        t = db.get_closed_trades(ticker="GIS")[0]
        assert t["benchmark_return"] is None
        assert t["alpha"] is None

    def test_unknown_trade_id_returns_false(self, db):
        assert db.set_trade_benchmark(9999, 0.01) is False


# ─── paced_allowance ──────────────────────────────────────────────────────


class TestPacedAllowance:
    """
    A weekly pool spent by a daily job drains on day one. Pacing reserves a
    proportional share for later days, but — unlike a hard per-day cap — lets a
    busy day also consume everything earlier days left unused.
    """

    MONDAY = date(2026, 7, 20)
    WEDNESDAY = date(2026, 7, 22)
    SUNDAY = date(2026, 7, 26)

    def test_monday_gets_roughly_one_seventh(self, db):
        cap = db.get_budget_status("weekly_news_haiku")["max_calls"]
        allowance = db.paced_allowance("weekly_news_haiku", today=self.MONDAY)
        assert allowance == pytest.approx(cap / 7, abs=2)

    def test_later_days_accumulate_unused_budget(self, db):
        """Spending nothing on Mon/Tue must make Wednesday's share larger."""
        monday = db.paced_allowance("weekly_news_haiku", today=self.MONDAY)
        wednesday = db.paced_allowance("weekly_news_haiku", today=self.WEDNESDAY)
        assert wednesday > monday

    def test_final_day_may_spend_everything_left(self, db):
        cap = db.get_budget_status("weekly_news_haiku")["max_calls"]
        assert db.paced_allowance("weekly_news_haiku", today=self.SUNDAY) == cap

    def test_shrinks_as_the_pool_is_consumed(self, db):
        before = db.paced_allowance("weekly_news_haiku", today=self.WEDNESDAY)
        for _ in range(10):
            db.can_spend("weekly_news_haiku")
        after = db.paced_allowance("weekly_news_haiku", today=self.WEDNESDAY)
        assert after == before - 10

    def test_zero_when_pool_exhausted(self, db):
        cap = db.get_budget_status("weekly_news_haiku")["max_calls"]
        db.spend_batch("weekly_news_haiku", cap)
        assert db.paced_allowance("weekly_news_haiku", today=self.SUNDAY) == 0

    def test_never_negative_when_ahead_of_pace(self, db):
        """Monday overspend must not produce a negative allowance later."""
        db.spend_batch("weekly_news_haiku", 100)
        assert db.paced_allowance("weekly_news_haiku", today=self.MONDAY) == 0

    def test_unknown_cap_returns_zero(self, db):
        assert db.paced_allowance("nonexistent_cap") == 0


# ─── get_recent_headlines ─────────────────────────────────────────────────


class TestGetRecentHeadlines:
    def test_returns_logged_headlines(self, db):
        db.log_news_event("AAPL", "AAPL under SEC investigation")
        assert db.get_recent_headlines("AAPL") == {"AAPL under SEC investigation"}

    def test_scoped_per_ticker(self, db):
        db.log_news_event("AAPL", "AAPL news")
        db.log_news_event("MSFT", "MSFT news")
        assert db.get_recent_headlines("AAPL") == {"AAPL news"}

    def test_empty_for_unknown_ticker(self, db):
        assert db.get_recent_headlines("ZZZZ") == set()

    def test_excludes_headlines_outside_the_window(self, db):
        db.log_news_event("AAPL", "old story")
        with _open(db.path) as conn:
            conn.execute("UPDATE news_events SET detected_at = datetime('now', '-30 days')")
        assert db.get_recent_headlines("AAPL", days=7) == set()

    def test_deduplicates_repeats(self, db):
        db.log_news_event("AAPL", "same story")
        db.log_news_event("AAPL", "same story")
        assert db.get_recent_headlines("AAPL") == {"same story"}


# ─── Column migrations ────────────────────────────────────────────────────


class TestColumnMigrations:
    """
    CREATE TABLE IF NOT EXISTS is a no-op on an existing table, so a column
    added to SCHEMA_SQL never reaches a database that already exists. Without
    the explicit ALTER these tests pin, save_snapshot() would start raising
    "no such column" inside the scheduler's per-account try/except — which logs
    and continues, so snapshots would silently stop being written in production
    with no visible failure.
    """

    LEGACY_SNAPSHOTS_DDL = """
        CREATE TABLE portfolio_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id      TEXT NOT NULL,
            as_of           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            currency        TEXT NOT NULL,
            equity          REAL NOT NULL,
            cash            REAL NOT NULL,
            buying_power    REAL,
            invested_value  REAL,
            invested_pct    REAL,
            equity_dkk      REAL,
            positions       TEXT
        );
    """

    def _legacy_db(self, path):
        """Build a database with the pre-migration portfolio_snapshots shape."""
        conn = sqlite3.connect(str(path))
        conn.executescript(self.LEGACY_SNAPSHOTS_DDL)
        conn.execute(
            """
            INSERT INTO portfolio_snapshots
                (account_id, currency, equity, cash, equity_dkk)
            VALUES ('alpaca_paper', 'USD', 1000.0, 400.0, 7000.0)
            """
        )
        conn.commit()
        conn.close()

    def test_adds_columns_to_legacy_db(self, tmp_db_path):
        self._legacy_db(tmp_db_path)
        Database(db_path=tmp_db_path)

        conn = sqlite3.connect(str(tmp_db_path))
        cols = {r[1] for r in conn.execute("PRAGMA table_info(portfolio_snapshots)")}
        conn.close()
        assert "gross_equity" in cols
        assert "untradable_value" in cols

    def test_preserves_existing_rows(self, tmp_db_path):
        self._legacy_db(tmp_db_path)
        db = Database(db_path=tmp_db_path)

        snaps = db.get_snapshots("alpaca_paper")
        assert len(snaps) == 1
        assert snaps[0]["equity"] == 1000.0
        # New columns backfill as NULL, not zero — "unknown", not "none held".
        assert snaps[0]["gross_equity"] is None
        assert snaps[0]["untradable_value"] is None

    def test_is_idempotent(self, tmp_db_path):
        self._legacy_db(tmp_db_path)
        Database(db_path=tmp_db_path)
        Database(db_path=tmp_db_path)  # must not raise "duplicate column name"

        db = Database(db_path=tmp_db_path)
        assert len(db.get_snapshots("alpaca_paper")) == 1

    def test_raised_cap_reaches_existing_db(self, tmp_db_path):
        """
        budget_caps is seeded with INSERT OR IGNORE, so raising a cap in
        BUDGET_CAPS_DEFAULTS must be synced explicitly or the old ceiling
        persists forever on a live database.
        """
        db = Database(db_path=tmp_db_path)
        with _open(tmp_db_path) as conn:
            conn.execute("UPDATE budget_caps SET max_calls = 1 WHERE cap_type = 'weekly_news_haiku'")

        db = Database(db_path=tmp_db_path)  # re-init should re-sync the ceiling
        expected = dict(BUDGET_CAPS_DEFAULTS)["weekly_news_haiku"]
        assert db.get_budget_status("weekly_news_haiku")["max_calls"] == expected

    def test_cap_sync_preserves_usage(self, tmp_db_path):
        """Re-syncing the ceiling must not reset the week's spend."""
        db = Database(db_path=tmp_db_path)
        db.can_spend("weekly_news_haiku")
        db.can_spend("weekly_news_haiku")

        db = Database(db_path=tmp_db_path)
        assert db.get_budget_status("weekly_news_haiku")["calls_used"] == 2


# ─── Quarantined positions ────────────────────────────────────────────────


class TestQuarantinedPositions:
    def test_first_sighting_is_due_for_alert(self, db):
        assert db.record_quarantine("alpaca_paper", "AL", market_value=7242.03) is True

    def test_not_due_again_within_window(self, db):
        db.record_quarantine("alpaca_paper", "AL", realert_days=7)
        db.mark_quarantine_alerted("alpaca_paper", "AL")
        assert db.record_quarantine("alpaca_paper", "AL", realert_days=7) is False

    def test_due_again_after_window(self, db):
        db.record_quarantine("alpaca_paper", "AL", realert_days=7)
        db.mark_quarantine_alerted("alpaca_paper", "AL")
        with _open(db.path) as conn:
            conn.execute("UPDATE quarantined_positions SET last_alerted = datetime('now', '-8 days')")
        assert db.record_quarantine("alpaca_paper", "AL", realert_days=7) is True

    def test_stays_due_until_alert_is_marked(self, db):
        """A failed notification must be retried, not silently swallowed."""
        assert db.record_quarantine("alpaca_paper", "AL") is True
        assert db.record_quarantine("alpaca_paper", "AL") is True

    def test_upsert_refreshes_values(self, db):
        db.record_quarantine("alpaca_paper", "AL", shares=100.0, market_value=6500.0)
        db.record_quarantine("alpaca_paper", "AL", shares=111.4, market_value=7242.03, asset_status="inactive")
        rows = db.get_quarantined("alpaca_paper")
        assert len(rows) == 1
        assert rows[0]["market_value"] == 7242.03
        assert rows[0]["asset_status"] == "inactive"

    def test_resolve_marks_departed_positions(self, db):
        db.record_quarantine("alpaca_paper", "AL")
        resolved = db.resolve_quarantines("alpaca_paper", ["AGM", "CTSH"])
        assert resolved == 1
        assert db.get_quarantined("alpaca_paper") == []
        assert len(db.get_quarantined("alpaca_paper", include_resolved=True)) == 1

    def test_resolve_keeps_still_held_positions(self, db):
        db.record_quarantine("alpaca_paper", "AL")
        assert db.resolve_quarantines("alpaca_paper", ["AL", "AGM"]) == 0
        assert len(db.get_quarantined("alpaca_paper")) == 1

    def test_resolve_ignores_empty_ticker_list(self, db):
        """
        An empty broker response is indistinguishable from a failed call, so it
        must never resolve open quarantines.
        """
        db.record_quarantine("alpaca_paper", "AL")
        assert db.resolve_quarantines("alpaca_paper", []) == 0
        assert len(db.get_quarantined("alpaca_paper")) == 1

    def test_reappearance_reopens_the_episode(self, db):
        db.record_quarantine("alpaca_paper", "AL")
        db.resolve_quarantines("alpaca_paper", ["AGM"])
        db.record_quarantine("alpaca_paper", "AL")
        assert len(db.get_quarantined("alpaca_paper")) == 1

    def test_accounts_are_independent(self, db):
        db.record_quarantine("alpaca_paper", "AL")
        db.record_quarantine("nordnet", "AL")
        db.resolve_quarantines("alpaca_paper", ["AGM"])
        assert db.get_quarantined("alpaca_paper") == []
        assert len(db.get_quarantined("nordnet")) == 1

    def test_get_quarantined_across_all_accounts(self, db):
        db.record_quarantine("alpaca_paper", "AL")
        db.record_quarantine("nordnet", "XYZ")
        assert len(db.get_quarantined()) == 2


# ─── Snapshot quarantine columns ──────────────────────────────────────────


class TestSnapshotQuarantineColumns:
    def test_round_trips_gross_equity_and_untradable_value(self, db):
        db.save_snapshot(
            "alpaca_paper",
            currency="USD",
            equity=100132.85,
            cash=30880.40,
            gross_equity=107374.88,
            untradable_value=7242.03,
        )
        snap = db.get_snapshots("alpaca_paper")[0]
        assert snap["equity"] == 100132.85
        assert snap["gross_equity"] == 107374.88
        assert snap["untradable_value"] == 7242.03

    def test_columns_default_to_null_when_omitted(self, db):
        db.save_snapshot("alpaca_paper", currency="USD", equity=1000.0, cash=400.0)
        snap = db.get_snapshots("alpaca_paper")[0]
        assert snap["gross_equity"] is None
        assert snap["untradable_value"] is None


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
        assert history[0]["new_tier"] == "A"  # most recent first
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
        count = conn.execute("SELECT COUNT(*) FROM fundamentals WHERE ticker = 'KEEP'").fetchone()[0]
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


# ─── Tier lifecycle ───────────────────────────────────────────────────────


class TestWasDowngradedToC:
    """
    Distinguishes a real thesis breaker from a first-ever analysis landing on
    C. Getting this backwards produced same-week round trips: AD was bought
    unanalysed, rated C the next morning, sold that afternoon for -3.67%.
    """

    def test_false_for_a_first_ever_analysis(self, db):
        db.log_tier_change("NEWBUY", new_tier="C", old_tier=None, trigger="scheduled")
        assert db.was_downgraded_to_c("NEWBUY") is False

    def test_true_after_a_real_downgrade(self, db):
        db.log_tier_change("OLDCO", new_tier="B", old_tier=None, trigger="scheduled")
        db.log_tier_change("OLDCO", new_tier="C", old_tier="B", trigger="news_event")
        assert db.was_downgraded_to_c("OLDCO") is True

    def test_false_for_an_unknown_ticker(self, db):
        assert db.was_downgraded_to_c("NOPE") is False

    def test_false_when_never_reached_c(self, db):
        db.log_tier_change("GOODCO", new_tier="A", old_tier="B", trigger="scheduled")
        assert db.was_downgraded_to_c("GOODCO") is False

    def test_c_to_c_is_not_a_downgrade(self, db):
        db.log_tier_change("STUCK", new_tier="C", old_tier="C", trigger="scheduled")
        assert db.was_downgraded_to_c("STUCK") is False

    def test_stays_true_after_recovering(self, db):
        """History is history — a past downgrade remains a past downgrade."""
        db.log_tier_change("BOUNCE", new_tier="C", old_tier="B", trigger="news_event")
        db.log_tier_change("BOUNCE", new_tier="B", old_tier="C", trigger="scheduled")
        assert db.was_downgraded_to_c("BOUNCE") is True

    def test_scoped_per_ticker(self, db):
        db.log_tier_change("BAD", new_tier="C", old_tier="B", trigger="news_event")
        assert db.was_downgraded_to_c("OTHER") is False


class TestCTierAnalysisExpiry:
    """
    A C verdict ejects a stock from every downstream queue, and the Haiku queue
    skips anything holding a non-expired analysis. At the standard 180 days a
    single bad verdict was a six-month exile: 74 stocks reached C in five
    months and not one ever came back.
    """

    def _expiry_days(self, db, ticker):
        row = db.get_latest_deep_analysis(ticker)
        expires = datetime.fromisoformat(row["expires_at"])
        return (expires - datetime.now()).days

    def test_c_tier_expires_far_sooner(self, db):
        db.save_deep_analysis("BADCO", tier="C")
        assert self._expiry_days(db, "BADCO") < 60

    def test_other_tiers_keep_the_long_ttl(self, db):
        db.save_deep_analysis("GOODCO", tier="B")
        assert self._expiry_days(db, "GOODCO") > 150

    def test_an_explicitly_shorter_ttl_still_wins(self, db):
        """min(), not an override — a caller expiring something now must win."""
        db.save_deep_analysis("URGENT", tier="C", expires_days=1)
        assert self._expiry_days(db, "URGENT") <= 1

    def test_c_tier_reenters_the_queue_once_expired(self, db):
        """The whole point: an expired C verdict becomes re-analysable."""
        db.upsert_universe_stock("BADCO", source="finviz_screen", quality_score=80.0)
        with _open(db.path) as conn:
            conn.execute(
                """
                INSERT INTO haiku_screens (ticker, screened_at, passed, expires_at)
                VALUES ('BADCO', datetime('now'), 1, datetime('now', '+180 days'))
                """
            )
        db.save_deep_analysis("BADCO", tier="C")
        assert "BADCO" not in db.get_haiku_passes_without_analysis(limit=10)

        with _open(db.path) as conn:
            conn.execute("UPDATE deep_analyses SET expires_at = datetime('now', '-1 day')")
        assert "BADCO" in db.get_haiku_passes_without_analysis(limit=10)
