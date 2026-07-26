"""
Tests for scripts/backfill_c_tier_expiry.py.

The short-TTL rule in save_deep_analysis only applies to newly written
analyses, so every stock stranded before that change kept its 180-day expiry.
This script closes that gap. It rewrites live data, so the properties that
matter are: dry-run touches nothing, it only ever shortens, and re-running is
a no-op.
"""

from datetime import datetime, timedelta

import pytest

from scripts.backfill_c_tier_expiry import main
from src.database import Database, _open


def _analysis(db, ticker, tier, *, expires_in_days):
    db.save_deep_analysis(ticker, tier=tier)
    expiry = (datetime.now() + timedelta(days=expires_in_days)).isoformat()
    with _open(db.path) as conn:
        conn.execute("UPDATE deep_analyses SET expires_at = ? WHERE ticker = ?", (expiry, ticker))


def _expiry_days(db, ticker):
    row = db.get_latest_deep_analysis(ticker)
    return (datetime.fromisoformat(row["expires_at"]) - datetime.now()).days


@pytest.fixture
def db(tmp_path):
    d = Database(db_path=tmp_path / "test.db")
    _analysis(d, "AAPL", "C", expires_in_days=150)  # stranded pre-change
    _analysis(d, "MSFT", "B", expires_in_days=150)  # healthy, must not move
    return d


class TestDryRun:
    def test_changes_nothing(self, db, capsys):
        assert main(["--db", str(db.path)]) == 0
        assert _expiry_days(db, "AAPL") > 100
        assert "DRY RUN" in capsys.readouterr().out

    def test_lists_the_affected_ticker(self, db, capsys):
        main(["--db", str(db.path)])
        assert "AAPL" in capsys.readouterr().out


class TestApply:
    def test_shortens_c_tier(self, db):
        main(["--db", str(db.path), "--days", "30", "--apply"])
        assert _expiry_days(db, "AAPL") <= 30

    def test_leaves_other_tiers_alone(self, db):
        main(["--db", str(db.path), "--days", "30", "--apply"])
        assert _expiry_days(db, "MSFT") > 100

    def test_never_extends_an_already_short_expiry(self, db):
        """Only shortens — a caller that expired something sooner keeps it."""
        _analysis(db, "URGENT", "C", expires_in_days=2)
        main(["--db", str(db.path), "--days", "30", "--apply"])
        assert _expiry_days(db, "URGENT") <= 2

    def test_is_idempotent(self, db, capsys):
        main(["--db", str(db.path), "--days", "30", "--apply"])
        first = _expiry_days(db, "AAPL")
        capsys.readouterr()

        main(["--db", str(db.path), "--days", "30"])
        assert "nothing to change" in capsys.readouterr().out
        assert _expiry_days(db, "AAPL") == first

    def test_makes_the_stock_re_analysable(self, db):
        """
        The actual point: shortening the expiry must return the stock to the
        Sonnet queue once it lapses.
        """
        db.upsert_universe_stock("AAPL", source="conviction", quality_score=95.0)
        with _open(db.path) as conn:
            conn.execute(
                """
                INSERT INTO haiku_screens (ticker, screened_at, passed, expires_at)
                VALUES ('AAPL', datetime('now'), 1, datetime('now', '+180 days'))
                """
            )
        assert "AAPL" not in db.get_haiku_passes_without_analysis(limit=10)

        main(["--db", str(db.path), "--days", "30", "--apply"])
        with _open(db.path) as conn:  # simulate the 30 days elapsing
            conn.execute("UPDATE deep_analyses SET expires_at = datetime('now', '-1 day') WHERE ticker = 'AAPL'")

        assert "AAPL" in db.get_haiku_passes_without_analysis(limit=10)


class TestDegenerateInputs:
    def test_missing_database_errors(self, tmp_path):
        assert main(["--db", str(tmp_path / "nope.db")]) == 1

    def test_no_c_tier_analyses_exits_cleanly(self, tmp_path, capsys):
        d = Database(db_path=tmp_path / "empty.db")
        _analysis(d, "MSFT", "B", expires_in_days=150)
        assert main(["--db", str(d.path)]) == 0
        assert "nothing to change" in capsys.readouterr().out
