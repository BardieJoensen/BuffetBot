"""
Tests for scripts/backfill_trade_alpha.py.

Repairs closed trades journalled while fetch_benchmark_return was resolving
hold windows incorrectly. Rewrites a live journal, so the properties that
matter are: dry-run touches nothing, re-running is a no-op, and a failed
benchmark fetch does not wipe an existing value.
"""

from unittest.mock import patch

import pytest

from scripts.backfill_trade_alpha import main
from src.database import Database


def _closed_trade(db, ticker, *, entry=100.0, exit_=95.0, reason="Take profit: near fair value"):
    buy_id = db.log_decision(
        ticker,
        "buy",
        tier="B",
        price=entry,
        shares=10.0,
        notional=entry * 10,
        order_id="b",
        reason="entry",
        regime="fair_value",
        reasoning_snapshot={"fair_value": entry * 1.2},
    )
    sell_id = db.log_decision(
        ticker,
        "sell",
        tier="B",
        price=exit_,
        shares=10.0,
        notional=exit_ * 10,
        order_id="s",
        reason=reason,
        regime="fair_value",
        reasoning_snapshot={},
    )
    assert buy_id
    return db.close_trade(ticker, exit_decision_id=sell_id, entry_price=entry, exit_price=exit_, shares=10.0)


@pytest.fixture
def db(tmp_path):
    d = Database(db_path=tmp_path / "test.db")
    _closed_trade(d, "GIS")
    return d


def _bench(value):
    """Patch the benchmark fetch the script imported into its own namespace."""
    return patch("scripts.backfill_trade_alpha.fetch_benchmark_return", return_value=value)


class TestDryRun:
    def test_changes_nothing(self, db):
        before = db.get_closed_trades()
        with _bench(0.0137):
            assert main(["--db", str(db.path)]) == 0
        assert db.get_closed_trades() == before

    def test_reports_the_pending_change(self, db, capsys):
        with _bench(0.0137):
            main(["--db", str(db.path)])
        out = capsys.readouterr().out
        assert "DRY RUN" in out
        assert "GIS" in out


class TestApply:
    def test_writes_benchmark_and_alpha(self, db):
        with _bench(0.0137):
            main(["--db", str(db.path), "--apply"])

        t = db.get_closed_trades()[0]
        assert t["benchmark_return"] == pytest.approx(0.0137)
        assert t["alpha"] == pytest.approx(t["realized_pl_pct"] - 0.0137)

    def test_rescoring_reaches_reasoning_sound(self, db):
        with _bench(0.0137):
            main(["--db", str(db.path), "--apply"])

        # Lost money AND lost to the benchmark on a take-profit: not sound.
        assert db.get_closed_trades()[0]["reasoning_sound"] == 0

    def test_second_run_is_a_no_op(self, db, capsys):
        with _bench(0.0137):
            main(["--db", str(db.path), "--apply"])
            capsys.readouterr()
            main(["--db", str(db.path)])

        assert "nothing to change" in capsys.readouterr().out

    def test_repairs_a_trade_that_had_no_benchmark(self, db):
        assert db.get_closed_trades()[0]["alpha"] is None

        with _bench(0.0):
            main(["--db", str(db.path), "--apply"])

        t = db.get_closed_trades()[0]
        assert t["benchmark_return"] == pytest.approx(0.0)
        assert t["alpha"] == pytest.approx(t["realized_pl_pct"])

    def test_ticker_filter_limits_scope(self, db):
        _closed_trade(db, "AAPL")

        with _bench(0.0137):
            main(["--db", str(db.path), "--ticker", "GIS", "--apply"])

        by_ticker = {t["ticker"]: t for t in db.get_closed_trades()}
        assert by_ticker["GIS"]["benchmark_return"] is not None
        assert by_ticker["AAPL"]["benchmark_return"] is None


class TestFailureHandling:
    def test_unfetchable_benchmark_leaves_an_existing_value_alone(self, db):
        """A network failure must not erase a benchmark already on the row."""
        with _bench(0.0137):
            main(["--db", str(db.path), "--apply"])

        with _bench(None):
            main(["--db", str(db.path), "--apply"])

        assert db.get_closed_trades()[0]["benchmark_return"] == pytest.approx(0.0137)

    def test_missing_database_errors(self, tmp_path):
        assert main(["--db", str(tmp_path / "nope.db")]) == 1

    def test_no_closed_trades_exits_cleanly(self, tmp_path, capsys):
        Database(db_path=tmp_path / "empty.db")
        assert main(["--db", str(tmp_path / "empty.db")]) == 0
        assert "No closed trades" in capsys.readouterr().out
