"""Regression tests for broker-order safety and Alpaca SDK normalization."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from alpaca.trading.enums import OrderSide

from src.config import config
from src.paper_trader import BrokerReadError, PaperTrader


@pytest.fixture
def trader():
    instance = PaperTrader.__new__(PaperTrader)
    instance._enabled = True
    instance._client = None
    instance._trading_client = MagicMock()
    instance._notifier = MagicMock()
    instance._log_trade = MagicMock()
    instance._notify_trade = MagicMock()
    return instance


def _open_order(symbol="AAPL", side=OrderSide.BUY):
    return SimpleNamespace(
        symbol=symbol,
        side=side,
        notional="1000",
        qty=None,
        status="accepted",
        id="order-1",
    )


class TestBrokerReads:
    def test_open_order_side_uses_enum_value(self, trader):
        trader._trading_client.get_orders.return_value = [_open_order()]

        assert trader.get_open_orders()[0]["side"] == "buy"

    def test_position_read_failure_is_not_an_empty_portfolio(self, trader):
        trader._trading_client.get_all_positions.side_effect = RuntimeError("network down")

        with pytest.raises(BrokerReadError, match="positions"):
            trader.get_positions()

    def test_buy_aborts_when_position_state_is_unreadable(self, trader):
        trader.get_asset_status = MagicMock(return_value=(True, "active"))
        trader.get_positions = MagicMock(side_effect=BrokerReadError("network down"))

        assert trader.buy("AAPL", 1000.0) is None
        trader._trading_client.submit_order.assert_not_called()

    def test_non_finite_position_value_fails_closed(self, trader):
        trader.get_asset_status = MagicMock(return_value=(True, "active"))
        trader._trading_client.get_all_positions.return_value = [
            SimpleNamespace(
                symbol="AAPL",
                qty="10",
                market_value=float("nan"),
                avg_entry_price="100",
                current_price="110",
                unrealized_pl="100",
                unrealized_plpc="0.1",
            )
        ]

        with pytest.raises(BrokerReadError, match="positions"):
            trader.get_positions()

    def test_non_finite_account_value_is_an_error(self, trader):
        trader._trading_client.get_account.return_value = SimpleNamespace(
            equity="100000",
            cash="50000",
            buying_power=float("inf"),
            portfolio_value="100000",
        )

        assert "error" in trader.get_account()


class TestOrderIdempotency:
    def test_real_enum_pending_buy_blocks_duplicate(self, trader):
        trader.get_asset_status = MagicMock(return_value=(True, "active"))
        trader.get_positions = MagicMock(return_value=[])
        trader._trading_client.get_orders.return_value = [_open_order()]

        assert trader.buy("AAPL", 1000.0) is None
        trader._trading_client.submit_order.assert_not_called()

    def test_buy_sets_deterministic_client_order_id(self, trader):
        trader.get_asset_status = MagicMock(return_value=(True, "active"))
        trader.get_positions = MagicMock(return_value=[])
        trader.get_open_orders = MagicMock(return_value=[])
        trader.get_account = MagicMock(return_value={"portfolio_value": 100_000.0, "buying_power": 100_000.0})
        trader._trading_client.submit_order.return_value = SimpleNamespace(
            id="buy-1",
            status="accepted",
            filled_avg_price=None,
            filled_qty=None,
        )

        result = trader.buy("AAPL", 1000.0)

        assert result is not None
        request = trader._trading_client.submit_order.call_args.args[0]
        assert request.client_order_id.startswith("buffettbot-")
        assert request.client_order_id.endswith("-buy-AAPL")
        assert result["status"] == "accepted"

    def test_unknown_asset_state_blocks_buy(self, trader):
        trader.get_asset_status = MagicMock(return_value=(True, "unknown"))

        assert trader.buy("AAPL", 1000.0) is None
        trader._trading_client.submit_order.assert_not_called()

    def test_missing_buying_power_blocks_buy(self, trader):
        trader.get_asset_status = MagicMock(return_value=(True, "active"))
        trader.get_positions = MagicMock(return_value=[])
        trader.get_open_orders = MagicMock(return_value=[])
        trader.get_account = MagicMock(return_value={"portfolio_value": 100_000.0})

        assert trader.buy("AAPL", 1000.0) is None
        trader._trading_client.submit_order.assert_not_called()

    @pytest.mark.parametrize("limit", [0.0, -0.1, 1.1, float("nan"), float("inf")])
    def test_invalid_position_limit_blocks_buy(self, trader, monkeypatch, limit):
        monkeypatch.setattr(trader, "MAX_POSITION_PCT", limit)
        trader.get_asset_status = MagicMock(return_value=(True, "active"))
        trader.get_positions = MagicMock(return_value=[])
        trader.get_open_orders = MagicMock(return_value=[])
        trader.get_account = MagicMock(return_value={"portfolio_value": 100_000.0, "buying_power": 100_000.0})

        assert trader.buy("AAPL", 1000.0) is None
        trader._trading_client.submit_order.assert_not_called()

    def test_maximum_position_count_blocks_buy(self, trader):
        trader.get_asset_status = MagicMock(return_value=(True, "active"))
        trader.get_positions = MagicMock(
            return_value=[{"symbol": f"P{i}", "tradable": True} for i in range(config.max_positions)]
        )

        assert trader.buy("AAPL", 1000.0) is None
        trader._trading_client.submit_order.assert_not_called()

    @pytest.mark.parametrize("amount", [0.0, -1.0, float("nan"), float("inf")])
    def test_non_positive_or_non_finite_buy_is_rejected(self, trader, amount):
        assert trader.buy("AAPL", amount) is None
        trader._trading_client.submit_order.assert_not_called()


class TestPartialSell:
    def test_sell_uses_requested_trim_quantity(self, trader):
        trader.get_positions = MagicMock(
            return_value=[{"symbol": "AAPL", "qty": 10.0, "tradable": True, "asset_status": "active"}]
        )
        trader.get_open_orders = MagicMock(return_value=[])
        trader._trading_client.submit_order.return_value = SimpleNamespace(
            id="sell-1",
            status="accepted",
            filled_avg_price=None,
            filled_qty=None,
        )

        result = trader.sell("AAPL", reason="trim", quantity=2.5)

        assert result is not None
        request = trader._trading_client.submit_order.call_args.args[0]
        assert float(request.qty) == 2.5
        assert result["qty"] == 2.5

    def test_pending_sell_blocks_duplicate(self, trader):
        trader.get_positions = MagicMock(
            return_value=[{"symbol": "AAPL", "qty": 10.0, "tradable": True, "asset_status": "active"}]
        )
        trader.get_open_orders = MagicMock(return_value=[{"symbol": "AAPL", "side": "sell"}])

        assert trader.sell("AAPL") is None
        trader._trading_client.submit_order.assert_not_called()

    @pytest.mark.parametrize("quantity", [0.0, -1.0, float("nan"), float("inf")])
    def test_non_positive_or_non_finite_sell_is_rejected(self, trader, quantity):
        trader.get_positions = MagicMock(
            return_value=[{"symbol": "AAPL", "qty": 10.0, "tradable": True, "asset_status": "active"}]
        )
        trader.get_open_orders = MagicMock(return_value=[])

        assert trader.sell("AAPL", quantity=quantity) is None
        trader._trading_client.submit_order.assert_not_called()
