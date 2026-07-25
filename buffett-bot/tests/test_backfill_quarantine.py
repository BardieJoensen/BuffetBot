"""
Tests for scripts/backfill_quarantine_snapshots.py.

This script rewrites historical equity in place on a live database, so the
properties that matter most are: dry-run touches nothing, the correction is
idempotent, and --revert is a true inverse.
"""

import pytest

from scripts.backfill_quarantine_snapshots import main
from src.database import Database


def _snapshot(db, *, as_of, equity, cash, al_price=65.0, al_value=7242.03, equity_dkk=None):
    """Append one snapshot holding AGM (tradable) plus AL (delisted)."""
    positions = [
        {
            "symbol": "AGM",
            "shares": 37.8,
            "avg_cost": 159.1,
            "price": 199.27,
            "market_value": 7542.02,
            "unrealized_pl": 1520.48,
            "unrealized_pl_pct": 0.25,
            "tier_at_entry": None,
        },
        {
            "symbol": "AL",
            "shares": 111.4,
            "avg_cost": 64.62,
            "price": al_price,
            "market_value": al_value,
            "unrealized_pl": 42.04,
            "unrealized_pl_pct": 0.006,
            "tier_at_entry": None,
        },
    ]
    snap_id = db.save_snapshot(
        "alpaca_paper",
        currency="USD",
        equity=equity,
        cash=cash,
        buying_power=equity * 3,
        invested_value=equity - cash,
        invested_pct=(equity - cash) / equity,
        equity_dkk=equity_dkk,
        positions=positions,
    )
    # save_snapshot stamps as_of itself; force a deterministic value.
    from src.database import _open

    with _open(db.path) as conn:
        conn.execute("UPDATE portfolio_snapshots SET as_of = ? WHERE id = ?", (as_of, snap_id))
    return snap_id


@pytest.fixture
def db(tmp_path):
    d = Database(db_path=tmp_path / "test.db")
    _snapshot(d, as_of="2026-07-01 22:00:00", equity=107101.22, cash=30880.40, equity_dkk=738998.42)
    _snapshot(d, as_of="2026-07-02 22:00:00", equity=107307.91, cash=30880.40, equity_dkk=740424.19)
    _snapshot(d, as_of="2026-07-03 22:00:00", equity=107374.88, cash=30880.40, equity_dkk=740884.67)
    return d


class TestDryRun:
    def test_changes_nothing(self, db, capsys):
        before = db.get_snapshots("alpaca_paper")
        assert main(["--db", str(db.path), "--symbols", "AL"]) == 0
        assert db.get_snapshots("alpaca_paper") == before
        assert "DRY RUN" in capsys.readouterr().out

    def test_reports_the_affected_symbol(self, db, capsys):
        main(["--db", str(db.path), "--symbols", "AL"])
        assert "AL" in capsys.readouterr().out


class TestApply:
    def test_corrects_equity(self, db):
        main(["--db", str(db.path), "--symbols", "AL", "--apply", "--no-backup"])

        snaps = db.get_snapshots("alpaca_paper")
        assert snaps[-1]["equity"] == pytest.approx(107374.88 - 7242.03)
        assert snaps[-1]["gross_equity"] == pytest.approx(107374.88)
        assert snaps[-1]["untradable_value"] == pytest.approx(7242.03)

    def test_recomputes_invested_figures(self, db):
        main(["--db", str(db.path), "--symbols", "AL", "--apply", "--no-backup"])

        snap = db.get_snapshots("alpaca_paper")[-1]
        expected_equity = 107374.88 - 7242.03
        assert snap["invested_value"] == pytest.approx(expected_equity - 30880.40)
        assert snap["invested_pct"] == pytest.approx((expected_equity - 30880.40) / expected_equity)

    def test_scales_dkk_rather_than_refetching_fx(self, db):
        """The rate in effect that day must be preserved."""
        main(["--db", str(db.path), "--symbols", "AL", "--apply", "--no-backup"])

        snap = db.get_snapshots("alpaca_paper")[-1]
        ratio = (107374.88 - 7242.03) / 107374.88
        assert snap["equity_dkk"] == pytest.approx(740884.67 * ratio)

    def test_annotates_positions_json(self, db):
        main(["--db", str(db.path), "--symbols", "AL", "--apply", "--no-backup"])

        positions = {p["symbol"]: p for p in db.get_snapshots("alpaca_paper")[-1]["positions"]}
        assert positions["AL"]["tradable"] is False
        assert positions["AL"]["asset_status"] == "inactive"
        assert positions["AGM"]["tradable"] is True

    def test_writes_a_backup_by_default(self, db, tmp_path):
        main(["--db", str(db.path), "--symbols", "AL", "--apply"])
        assert list(tmp_path.glob("test.db.bak-*"))

    def test_is_idempotent(self, db):
        main(["--db", str(db.path), "--symbols", "AL", "--apply", "--no-backup"])
        first = db.get_snapshots("alpaca_paper")

        main(["--db", str(db.path), "--symbols", "AL", "--apply", "--no-backup"])
        assert db.get_snapshots("alpaca_paper") == first

    def test_second_run_reports_nothing_to_change(self, db, capsys):
        main(["--db", str(db.path), "--symbols", "AL", "--apply", "--no-backup"])
        capsys.readouterr()

        main(["--db", str(db.path), "--symbols", "AL"])
        assert "already up to date" in capsys.readouterr().out


class TestRevert:
    def test_restores_original_equity(self, db):
        main(["--db", str(db.path), "--symbols", "AL", "--apply", "--no-backup"])
        main(["--db", str(db.path), "--revert", "--apply", "--no-backup"])

        snaps = db.get_snapshots("alpaca_paper")
        assert snaps[-1]["equity"] == pytest.approx(107374.88)
        assert snaps[-1]["gross_equity"] is None
        assert snaps[-1]["untradable_value"] is None

    def test_restores_dkk(self, db):
        main(["--db", str(db.path), "--symbols", "AL", "--apply", "--no-backup"])
        main(["--db", str(db.path), "--revert", "--apply", "--no-backup"])

        assert db.get_snapshots("alpaca_paper")[-1]["equity_dkk"] == pytest.approx(740884.67)


class TestFrozenPriceInference:
    def test_only_corrects_rows_after_the_price_froze(self, tmp_path):
        """A symbol that traded normally earlier must not be corrected then."""
        d = Database(db_path=tmp_path / "test.db")
        _snapshot(d, as_of="2026-06-01 22:00:00", equity=100_000.0, cash=30_000.0, al_price=70.0)
        _snapshot(d, as_of="2026-06-02 22:00:00", equity=100_000.0, cash=30_000.0, al_price=68.0)
        _snapshot(d, as_of="2026-06-03 22:00:00", equity=100_000.0, cash=30_000.0, al_price=65.0)
        _snapshot(d, as_of="2026-06-04 22:00:00", equity=100_000.0, cash=30_000.0, al_price=65.0)

        main(["--db", str(d.path), "--symbols", "AL", "--apply", "--no-backup"])

        snaps = d.get_snapshots("alpaca_paper")
        # Price last moved on 06-03, so only 06-04 is treated as frozen.
        assert [s["gross_equity"] is None for s in snaps] == [True, True, True, False]

    def test_from_always_corrects_every_row(self, tmp_path):
        d = Database(db_path=tmp_path / "test.db")
        _snapshot(d, as_of="2026-06-01 22:00:00", equity=100_000.0, cash=30_000.0, al_price=70.0)
        _snapshot(d, as_of="2026-06-02 22:00:00", equity=100_000.0, cash=30_000.0, al_price=65.0)

        main(["--db", str(d.path), "--symbols", "AL", "--from", "always", "--apply", "--no-backup"])

        assert all(s["gross_equity"] is not None for s in d.get_snapshots("alpaca_paper"))

    def test_flat_price_throughout_corrects_every_row(self, db):
        """The live AL case: $65.00 in every snapshot ever taken."""
        main(["--db", str(db.path), "--symbols", "AL", "--apply", "--no-backup"])
        assert all(s["gross_equity"] is not None for s in db.get_snapshots("alpaca_paper"))


class TestNoOpCases:
    def test_symbol_not_held_changes_nothing(self, db, capsys):
        assert main(["--db", str(db.path), "--symbols", "ZZZZ", "--apply", "--no-backup"]) == 0
        assert all(s["gross_equity"] is None for s in db.get_snapshots("alpaca_paper"))

    def test_unknown_account_exits_cleanly(self, db, capsys):
        assert main(["--db", str(db.path), "--account-id", "nordnet", "--symbols", "AL"]) == 0
        assert "No snapshots" in capsys.readouterr().out

    def test_missing_database_errors(self, tmp_path):
        assert main(["--db", str(tmp_path / "nope.db"), "--symbols", "AL"]) == 1
