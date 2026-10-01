"""
Briefing-path paper trades (run_monthly_briefing step 9.5).

Audit finding F7: these orders used to size off the static PORTFOLIO_VALUE,
skip the dust floor, and never reach the SQLite decision journal — so
reconciliation, the closed-trade track record and per-tier alpha could not
see them. These tests pin the corrected behaviour.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts.run_monthly_briefing import (
    _execute_paper_trades,
    _live_sizing_equity,
    _paper_trade_symbols,
)
from src.config import config
from src.tier_engine import Tier, TierAssignment
from src.valuation import AggregatedValuation, ValuationEstimate


def _tier(symbol: str, tier: Tier, gap: float = -0.1) -> TierAssignment:
    return TierAssignment(
        symbol=symbol,
        tier=tier,
        quality_level="wonderful",
        tier_reason="test",
        target_entry_price=100.0,
        current_price=100.0 * (1 + gap),
        price_gap_pct=gap,
    )


def _valuation(symbol: str, price: float = 90.0) -> AggregatedValuation:
    return AggregatedValuation(
        symbol=symbol,
        current_price=price,
        estimates=[ValuationEstimate(source="t", fair_value=120.0, methodology="m", date=None)],  # type: ignore[arg-type]
    )


def _analysis(conviction: str = "HIGH"):
    return SimpleNamespace(conviction_level=conviction)


def _trader(enabled: bool = True, account: dict | None = None, order: dict | None = None) -> MagicMock:
    trader = MagicMock()
    trader.is_enabled.return_value = enabled
    trader.get_account.return_value = account if account is not None else {"equity": 100_000.0, "cash": 50_000.0}
    trader.buy.return_value = order
    return trader


class TestPaperTradeSymbols:
    def test_only_fresh_s_a_at_or_below_target_with_valid_valuation(self):
        tiers = {
            "FRESH_S": _tier("FRESH_S", Tier.S),
            "FRESH_A": _tier("FRESH_A", Tier.A),
            "FRESH_B": _tier("FRESH_B", Tier.B),
            "ABOVE": _tier("ABOVE", Tier.S, gap=0.05),
            "STALE_S": _tier("STALE_S", Tier.S),
            "NO_VAL": _tier("NO_VAL", Tier.S),
            "NO_PRICE": _tier("NO_PRICE", Tier.S),
        }
        valuations = {s: _valuation(s) for s in tiers if s not in ("NO_VAL",)}
        valuations["NO_PRICE"] = AggregatedValuation(symbol="NO_PRICE", current_price=0.0, estimates=[])
        fresh = [s for s in tiers if s != "STALE_S"]

        assert _paper_trade_symbols(tiers, fresh, valuations) == ["FRESH_S", "FRESH_A"]


class TestLiveSizingEquity:
    def test_uses_live_equity_when_available(self):
        assert _live_sizing_equity(_trader(account={"equity": 99_857.06, "cash": 30_880.39})) == (99_857.06, True)

    def test_falls_back_when_trader_disabled(self):
        assert _live_sizing_equity(_trader(enabled=False)) == (config.portfolio_value, False)

    @pytest.mark.parametrize("account", [{"error": "down"}, {"equity": 0.0}, {"equity": float("nan")}, {"equity": "x"}])
    def test_falls_back_on_unusable_equity(self, account):
        assert _live_sizing_equity(_trader(account=account)) == (config.portfolio_value, False)

    def test_falls_back_when_account_query_raises(self):
        trader = _trader()
        trader.get_account.side_effect = RuntimeError("network")

        assert _live_sizing_equity(trader) == (config.portfolio_value, False)


class TestExecutePaperTrades:
    def _run(self, trader, db, symbols=("ACME",), **overrides):
        kwargs = dict(
            tier_assignments={s: _tier(s, Tier.S) for s in symbols},
            analyses={s: _analysis() for s in symbols},
            valuation_lookup={s: _valuation(s) for s in symbols},
            portfolio_value=100_000.0,
            current_positions=3,  # >=3 so HIGH conviction sizes at its base 20%
            regime="fair_value",
        )
        kwargs.update(overrides)
        return _execute_paper_trades(trader, db, list(symbols), **kwargs)

    def test_submitted_order_is_journaled_with_reasoning(self):
        order = {
            "symbol": "ACME",
            "order_id": "o-1",
            "status": "accepted",
            "filled_avg_price": None,
            "filled_qty": None,
        }
        trader = _trader(order=order)
        db = MagicMock()

        assert self._run(trader, db) == 1

        trader.buy.assert_called_once()
        symbol, amount = trader.buy.call_args.args
        assert symbol == "ACME"
        assert amount == pytest.approx(100_000.0 * 0.20)  # HIGH conviction = 20% of live equity

        db.log_decision.assert_called_once()
        args, kwargs = db.log_decision.call_args
        assert args == ("ACME", "buy")
        assert kwargs["account_id"] == "alpaca_paper"
        assert kwargs["tier"] == "S"
        assert kwargs["order_id"] == "o-1"
        assert kwargs["order_status"] == "accepted"
        assert kwargs["price"] is None and kwargs["shares"] is None
        assert kwargs["notional"] == pytest.approx(amount)
        assert kwargs["regime"] == "fair_value"
        assert kwargs["reason"] == "Monthly briefing paper trade"
        snapshot = kwargs["reasoning_snapshot"]
        assert snapshot["target_entry"] == 100.0
        assert snapshot["conviction"] == "HIGH"
        assert snapshot["fair_value"] == pytest.approx(120.0)
        assert snapshot["margin_of_safety"] == pytest.approx((120.0 - 90.0) / 120.0)

    def test_filled_order_records_fill_economics(self):
        order = {"symbol": "ACME", "order_id": "o-2", "status": "filled", "filled_avg_price": 91.5, "filled_qty": 10.0}
        db = MagicMock()

        self._run(_trader(order=order), db)

        kwargs = db.log_decision.call_args.kwargs
        assert kwargs["order_status"] == "filled"
        assert kwargs["price"] == 91.5
        assert kwargs["shares"] == 10.0

    def test_filled_status_without_economics_is_not_invented(self):
        order = {"symbol": "ACME", "order_id": "o-3", "status": "filled", "filled_avg_price": None, "filled_qty": None}
        db = MagicMock()

        self._run(_trader(order=order), db)

        kwargs = db.log_decision.call_args.kwargs
        assert kwargs["price"] is None and kwargs["shares"] is None

    def test_skipped_order_is_not_journaled(self):
        db = MagicMock()

        assert self._run(_trader(order=None), db) == 0
        db.log_decision.assert_not_called()

    def test_below_minimum_trade_is_not_submitted(self):
        trader = _trader(order={"order_id": "o", "status": "accepted"})
        db = MagicMock()

        # 20% of $1,000 = $200 < MIN_TRADE_USD (250 by default).
        assert self._run(trader, db, portfolio_value=1_000.0) == 0
        trader.buy.assert_not_called()
        db.log_decision.assert_not_called()

    def test_journal_failure_does_not_abort_remaining_trades(self):
        trader = _trader(order={"order_id": "o", "status": "accepted"})
        db = MagicMock()
        db.log_decision.side_effect = RuntimeError("disk full")

        assert self._run(trader, db, symbols=("ONE", "TWO")) == 2
        assert trader.buy.call_count == 2

    def test_position_count_advances_between_buys(self):
        trader = _trader(order={"order_id": "o", "status": "accepted"})
        db = MagicMock()

        # With current_positions at the max, sizing shrinks to 60% for the second buy.
        self._run(trader, db, symbols=("ONE", "TWO"), current_positions=config.max_positions - 1)

        first, second = (c.args[1] for c in trader.buy.call_args_list)
        assert first == pytest.approx(100_000.0 * 0.20)
        assert second == pytest.approx(100_000.0 * 0.20 * 0.6)
