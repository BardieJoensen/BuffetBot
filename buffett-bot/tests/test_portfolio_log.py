"""
Tests for the Phase B append-only logging layer: account snapshots, market
regime history, and cash/income events outside of buy/sell trades.

Covers:
- save_snapshot persistence and JSON round-trip of the positions blob
- get_snapshots ordering and the `since` lower bound
- get_equity_curve's minimal (as_of, equity, equity_dkk) projection
- log_regime persistence
- log_income_event persistence and event_type validation
"""

import pytest

from src.database import Database


@pytest.fixture
def db(tmp_path):
    return Database(db_path=tmp_path / "test.db")


# ─── save_snapshot / get_snapshots ─────────────────────────────────────────


class TestSaveSnapshot:
    def test_returns_row_id_and_persists(self, db):
        sid = db.save_snapshot(
            "alpaca_paper",
            currency="USD",
            equity=107608.95,
            cash=87681.73,
            buying_power=386245.45,
            invested_value=19927.22,
            invested_pct=0.1852,
            equity_dkk=742000.0,
            positions=[{"symbol": "AGM", "shares": 37.848}],
        )
        assert isinstance(sid, int) and sid > 0

        snaps = db.get_snapshots("alpaca_paper")
        assert len(snaps) == 1
        row = snaps[0]
        assert row["account_id"] == "alpaca_paper"
        assert row["currency"] == "USD"
        assert row["equity"] == 107608.95
        assert row["equity_dkk"] == 742000.0
        assert row["positions"] == [{"symbol": "AGM", "shares": 37.848}]

    def test_optional_fields_default_none(self, db):
        db.save_snapshot("alpaca_paper", currency="USD", equity=1000.0, cash=1000.0)
        row = db.get_snapshots("alpaca_paper")[0]
        assert row["buying_power"] is None
        assert row["equity_dkk"] is None
        assert row["positions"] is None

    def test_never_overwrites_prior_snapshot(self, db):
        db.save_snapshot("alpaca_paper", currency="USD", equity=1000.0, cash=500.0)
        db.save_snapshot("alpaca_paper", currency="USD", equity=1100.0, cash=500.0)
        snaps = db.get_snapshots("alpaca_paper")
        assert len(snaps) == 2
        assert [s["equity"] for s in snaps] == [1000.0, 1100.0]


class TestGetSnapshots:
    def test_empty_when_no_snapshots(self, db):
        assert db.get_snapshots("alpaca_paper") == []

    def test_scoped_to_account_id(self, db):
        db.save_snapshot("alpaca_paper", currency="USD", equity=1000.0, cash=500.0)
        db.save_snapshot("nordnet_ask", currency="DKK", equity=50000.0, cash=1000.0)
        assert len(db.get_snapshots("alpaca_paper")) == 1
        assert len(db.get_snapshots("nordnet_ask")) == 1

    def test_orders_oldest_first(self, db):
        db.save_snapshot("alpaca_paper", currency="USD", equity=100.0, cash=0.0)
        db.save_snapshot("alpaca_paper", currency="USD", equity=200.0, cash=0.0)
        db.save_snapshot("alpaca_paper", currency="USD", equity=300.0, cash=0.0)
        snaps = db.get_snapshots("alpaca_paper")
        assert [s["equity"] for s in snaps] == [100.0, 200.0, 300.0]

    def test_since_filters_lower_bound(self, db):
        with __import__("sqlite3").connect(str(db.path)) as conn:
            conn.execute(
                "INSERT INTO portfolio_snapshots (account_id, as_of, currency, equity, cash) "
                "VALUES ('alpaca_paper', '2026-01-01T00:00:00', 'USD', 100.0, 0.0)"
            )
            conn.execute(
                "INSERT INTO portfolio_snapshots (account_id, as_of, currency, equity, cash) "
                "VALUES ('alpaca_paper', '2026-06-01T00:00:00', 'USD', 200.0, 0.0)"
            )
            conn.commit()
        snaps = db.get_snapshots("alpaca_paper", since="2026-03-01")
        assert len(snaps) == 1
        assert snaps[0]["equity"] == 200.0


class TestGetEquityCurve:
    def test_empty_when_no_snapshots(self, db):
        assert db.get_equity_curve("alpaca_paper") == []

    def test_returns_minimal_projection_oldest_first(self, db):
        db.save_snapshot("alpaca_paper", currency="USD", equity=100.0, cash=0.0, equity_dkk=700.0)
        db.save_snapshot("alpaca_paper", currency="USD", equity=200.0, cash=0.0, equity_dkk=1400.0)
        curve = db.get_equity_curve("alpaca_paper")
        assert len(curve) == 2
        assert set(curve[0].keys()) == {"as_of", "equity", "equity_dkk"}
        assert [p["equity"] for p in curve] == [100.0, 200.0]
        assert [p["equity_dkk"] for p in curve] == [700.0, 1400.0]


# ─── log_regime ─────────────────────────────────────────────────────────────


class TestLogRegime:
    def test_returns_row_id_and_persists(self, db):
        rid = db.log_regime("fair_value", confidence="high", market_pe=22.5, vix=15.0)
        assert isinstance(rid, int) and rid > 0

        with __import__("sqlite3").connect(str(db.path)) as conn:
            conn.row_factory = __import__("sqlite3").Row
            row = conn.execute("SELECT * FROM regime_log").fetchone()
        assert row["regime"] == "fair_value"
        assert row["confidence"] == "high"
        assert row["market_pe"] == 22.5
        assert row["vix"] == 15.0

    def test_optional_fields_default_none(self, db):
        db.log_regime("bubble")
        with __import__("sqlite3").connect(str(db.path)) as conn:
            conn.row_factory = __import__("sqlite3").Row
            row = conn.execute("SELECT * FROM regime_log").fetchone()
        assert row["confidence"] is None
        assert row["market_pe"] is None
        assert row["vix"] is None


# ─── log_income_event ───────────────────────────────────────────────────────


class TestLogIncomeEvent:
    def test_returns_row_id_and_persists(self, db):
        eid = db.log_income_event(
            "alpaca_paper",
            event_date="2026-07-01",
            event_type="dividend",
            symbol="AGM",
            amount=12.34,
            currency="USD",
            withholding=1.85,
        )
        assert isinstance(eid, int) and eid > 0

        with __import__("sqlite3").connect(str(db.path)) as conn:
            conn.row_factory = __import__("sqlite3").Row
            row = conn.execute("SELECT * FROM income_events").fetchone()
        assert row["account_id"] == "alpaca_paper"
        assert row["event_type"] == "dividend"
        assert row["symbol"] == "AGM"
        assert row["amount"] == 12.34
        assert row["withholding"] == 1.85

    def test_symbol_optional_for_contribution(self, db):
        db.log_income_event(
            "nordnet_ask", event_date="2026-07-01", event_type="contribution", amount=5000.0, currency="DKK"
        )
        with __import__("sqlite3").connect(str(db.path)) as conn:
            conn.row_factory = __import__("sqlite3").Row
            row = conn.execute("SELECT * FROM income_events").fetchone()
        assert row["symbol"] is None
        assert row["withholding"] is None

    def test_rejects_bad_event_type(self, db):
        with pytest.raises(ValueError):
            db.log_income_event("alpaca_paper", event_date="2026-07-01", event_type="bonus", amount=1.0, currency="USD")
