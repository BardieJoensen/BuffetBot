"""
Tests for the Phase 1 decision journal & track record.

Covers:
- log_decision persistence and the buy/sell event log
- get_open_buy matching (and exclusion of already-closed buys)
- close_trade outcome math (realized P&L, alpha, convergence) and soundness verdict
- tier_performance aggregation
- the sell-reason classifier and soundness rubric helpers
"""

from datetime import date, datetime, timedelta

import pytest

from src.database import (
    Database,
    _classify_sell_reason,
    _score_reasoning_soundness,
)


@pytest.fixture
def db(tmp_path):
    return Database(db_path=tmp_path / "test.db")


# ─── log_decision ───────────────────────────────────────────────────────────


class TestLogDecision:
    def test_returns_row_id_and_persists(self, db):
        rid = db.log_decision(
            "AAPL",
            "buy",
            tier="A",
            notional=1000.0,
            order_id="ord-1",
            reason="undervalued",
            regime="fair_value",
            reasoning_snapshot={"fair_value": 200.0, "margin_of_safety": 0.2},
        )
        assert isinstance(rid, int) and rid > 0

        log = db.get_decision_log("AAPL")
        assert len(log) == 1
        row = log[0]
        assert row["action"] == "buy"
        assert row["tier"] == "A"
        assert row["notional"] == 1000.0
        assert row["regime"] == "fair_value"
        assert row["reasoning_snapshot"]["fair_value"] == 200.0

    def test_rejects_bad_action(self, db):
        with pytest.raises(ValueError):
            db.log_decision("AAPL", "hold")

    def test_get_decision_log_orders_newest_first(self, db):
        db.log_decision("AAPL", "buy", notional=1.0)
        db.log_decision("AAPL", "sell", price=10.0)
        log = db.get_decision_log("AAPL")
        assert [r["action"] for r in log] == ["sell", "buy"]


# ─── get_open_buy ─────────────────────────────────────────────────────────────


class TestOpenBuy:
    def test_none_when_no_buy(self, db):
        assert db.get_open_buy("MSFT") is None

    def test_returns_unclosed_buy(self, db):
        db.log_decision("MSFT", "buy", notional=500.0, reasoning_snapshot={"fair_value": 1.0})
        ob = db.get_open_buy("MSFT")
        assert ob is not None
        assert ob["action"] == "buy"
        assert ob["reasoning_snapshot"]["fair_value"] == 1.0

    def test_excludes_already_closed_buy(self, db):
        db.log_decision("MSFT", "buy", notional=500.0)
        exit_id = db.log_decision("MSFT", "sell", price=120.0, shares=5.0)
        db.close_trade(
            "MSFT",
            exit_decision_id=exit_id,
            entry_price=100.0,
            exit_price=120.0,
            shares=5.0,
        )
        # The only buy is now closed -> no open buy remains.
        assert db.get_open_buy("MSFT") is None


# ─── close_trade ──────────────────────────────────────────────────────────────


class TestCloseTrade:
    def test_no_open_buy_returns_none(self, db):
        eid = db.log_decision("NVDA", "sell", price=10.0, shares=1.0)
        assert db.close_trade("NVDA", exit_decision_id=eid, entry_price=9, exit_price=10, shares=1) is None

    def test_realized_pnl_and_pct(self, db):
        db.log_decision("AAPL", "buy", tier="A", reasoning_snapshot={"fair_value": 150.0})
        eid = db.log_decision("AAPL", "sell", price=120.0, shares=10.0, reason="Take profit: near fair value")
        cid = db.close_trade(
            "AAPL",
            exit_decision_id=eid,
            entry_price=100.0,
            exit_price=120.0,
            shares=10.0,
            benchmark_return=0.05,
        )
        assert cid is not None
        ct = db.get_closed_trades("AAPL")[0]
        assert ct["cost_basis"] == 1000.0
        assert ct["proceeds"] == 1200.0
        assert ct["realized_pl"] == 200.0
        assert ct["realized_pl_pct"] == pytest.approx(0.20)
        # alpha = 0.20 - 0.05
        assert ct["alpha"] == pytest.approx(0.15)
        # entry fair value 150 > entry 100 (predicted up); exit 120 > 100 (moved up) -> converged
        assert ct["converged"] == 1
        # take_profit + alpha>0 + converged -> sound
        assert ct["sell_category"] == "take_profit"
        assert ct["reasoning_sound"] == 1

    def test_take_profit_not_sound_when_underperforms(self, db):
        db.log_decision("AAPL", "buy", tier="A", reasoning_snapshot={"fair_value": 150.0})
        eid = db.log_decision("AAPL", "sell", reason="Take profit: near fair value")
        db.close_trade(
            "AAPL",
            exit_decision_id=eid,
            entry_price=100.0,
            exit_price=110.0,
            shares=1.0,
            benchmark_return=0.20,  # market did better -> negative alpha
        )
        ct = db.get_closed_trades("AAPL")[0]
        assert ct["alpha"] == pytest.approx(0.10 - 0.20)
        assert ct["reasoning_sound"] == 0

    def test_converged_false_when_price_moves_wrong_way(self, db):
        db.log_decision("XYZ", "buy", reasoning_snapshot={"fair_value": 150.0})
        eid = db.log_decision("XYZ", "sell", reason="manual exit")
        db.close_trade("XYZ", exit_decision_id=eid, entry_price=100.0, exit_price=90.0, shares=1.0)
        ct = db.get_closed_trades("XYZ")[0]
        # predicted up (150>100) but moved down (90<100)
        assert ct["converged"] == 0

    def test_hold_days_computed(self, db):
        # Buy 30 days ago.
        old = (datetime.now() - timedelta(days=30)).isoformat()
        db.log_decision("AAPL", "buy", reasoning_snapshot={"fair_value": 150.0})
        # Patch the buy's decided_at to be 30 days old.
        with __import__("sqlite3").connect(str(db.path)) as conn:
            conn.execute("UPDATE decision_log SET decided_at = ? WHERE ticker = 'AAPL'", (old,))
            conn.commit()
        eid = db.log_decision("AAPL", "sell", reason="Take profit")
        db.close_trade(
            "AAPL",
            exit_decision_id=eid,
            entry_price=100.0,
            exit_price=110.0,
            shares=1.0,
            exit_date=date.today().isoformat(),
        )
        ct = db.get_closed_trades("AAPL")[0]
        assert ct["hold_days"] == 30

    def test_thesis_breaker_soundness_is_unknown(self, db):
        db.log_decision("AAPL", "buy", reasoning_snapshot={"fair_value": 150.0})
        eid = db.log_decision("AAPL", "sell", reason="Thesis breaker triggered: moat eroded")
        db.close_trade(
            "AAPL",
            exit_decision_id=eid,
            entry_price=100.0,
            exit_price=80.0,
            shares=1.0,
            benchmark_return=0.0,
        )
        ct = db.get_closed_trades("AAPL")[0]
        assert ct["sell_category"] == "thesis_breaker"
        assert ct["reasoning_sound"] is None


# ─── tier_performance ─────────────────────────────────────────────────────────


class TestTierPerformance:
    def test_empty(self, db):
        assert db.tier_performance() == []

    def test_aggregates_by_tier(self, db):
        # Two A-tier closes: one winner, one loser.
        for ticker, exit_price, sound_bench in [("AAA", 120.0, 0.0), ("BBB", 90.0, 0.0)]:
            db.log_decision(ticker, "buy", tier="A", reasoning_snapshot={"fair_value": 150.0})
            eid = db.log_decision(ticker, "sell", reason="Take profit")
            db.close_trade(
                ticker,
                exit_decision_id=eid,
                entry_price=100.0,
                exit_price=exit_price,
                shares=1.0,
                benchmark_return=sound_bench,
            )
        perf = db.tier_performance()
        assert len(perf) == 1
        row = perf[0]
        assert row["tier"] == "A"
        assert row["n"] == 2
        # avg of +20% and -10% = +5%
        assert row["avg_realized_pct"] == pytest.approx(0.05)
        # one winner of two
        assert row["hit_rate"] == pytest.approx(0.5)


# ─── helpers ──────────────────────────────────────────────────────────────────


class TestHelpers:
    @pytest.mark.parametrize(
        "reason,expected",
        [
            ("Take profit: margin of safety 3%", "take_profit"),
            ("stock near fair value", "take_profit"),
            ("Thesis breaker: moat gone", "thesis_breaker"),
            ("stop loss hit", "stop"),
            ("manual override", "manual"),
            ("", "other"),
        ],
    )
    def test_classify_sell_reason(self, reason, expected):
        assert _classify_sell_reason(reason) == expected

    def test_soundness_take_profit(self):
        assert _score_reasoning_soundness("take_profit", 0.1, 1) == 1
        assert _score_reasoning_soundness("take_profit", -0.1, 1) == 0
        assert _score_reasoning_soundness("take_profit", 0.1, 0) == 0
        assert _score_reasoning_soundness("take_profit", None, 1) is None

    def test_soundness_other_categories_unknown(self):
        assert _score_reasoning_soundness("thesis_breaker", 0.1, 1) is None
        assert _score_reasoning_soundness("stop", -0.5, 0) is None
