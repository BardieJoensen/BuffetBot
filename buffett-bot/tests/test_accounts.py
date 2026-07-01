"""
Tests for src/accounts/ — the broker-agnostic account interface.

AlpacaPaperAccount is a pure mapping layer over PaperTrader, so these tests
inject a mock PaperTrader and assert the dataclass mapping is correct —
no real Alpaca API calls are made.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.accounts import Account, get_accounts
from src.accounts.alpaca import AlpacaPaperAccount
from src.accounts.base import AccountState, PositionState


def _mock_trader(enabled=True):
    trader = MagicMock()
    trader.is_enabled.return_value = enabled
    trader.get_account.return_value = {
        "equity": 107608.95,
        "cash": 87681.73,
        "buying_power": 386245.45,
        "portfolio_value": 107608.95,
    }
    trader.get_positions.return_value = [
        {
            "symbol": "AGM",
            "qty": 37.848250233,
            "market_value": 7542.020824,
            "avg_entry_price": 159.096809,
            "current_price": 199.27,
            "unrealized_pl": 1520.484986,
            "unrealized_plpc": 0.25251,
        }
    ]
    return trader


class TestAlpacaPaperAccount:
    def test_satisfies_account_protocol(self):
        account = AlpacaPaperAccount(trader=_mock_trader())
        assert isinstance(account, Account)

    def test_is_enabled_delegates_to_trader(self):
        assert AlpacaPaperAccount(trader=_mock_trader(enabled=True)).is_enabled() is True
        assert AlpacaPaperAccount(trader=_mock_trader(enabled=False)).is_enabled() is False

    def test_get_state_maps_account_dict(self):
        account = AlpacaPaperAccount(trader=_mock_trader())
        state = account.get_state()

        assert isinstance(state, AccountState)
        assert state.account_id == "alpaca_paper"
        assert state.currency == "USD"
        assert state.equity == 107608.95
        assert state.cash == 87681.73
        assert state.buying_power == 386245.45
        assert state.invested_value == 107608.95 - 87681.73
        assert state.invested_pct == (107608.95 - 87681.73) / 107608.95

    def test_get_state_handles_zero_equity(self):
        trader = _mock_trader()
        trader.get_account.return_value = {"equity": 0.0, "cash": 0.0, "buying_power": 0.0}
        state = AlpacaPaperAccount(trader=trader).get_state()
        assert state.invested_pct == 0.0

    def test_get_state_raises_on_trader_error(self):
        # PaperTrader reports API failures as {"error": ...} rather than
        # raising — get_state must NOT map that into a fake equity=0 state.
        trader = _mock_trader()
        trader.get_account.return_value = {"error": "connection timed out"}
        with pytest.raises(RuntimeError, match="connection timed out"):
            AlpacaPaperAccount(trader=trader).get_state()

    def test_get_positions_maps_position_dicts(self):
        account = AlpacaPaperAccount(trader=_mock_trader())
        positions = account.get_positions()

        assert len(positions) == 1
        pos = positions[0]
        assert isinstance(pos, PositionState)
        assert pos.symbol == "AGM"
        assert pos.shares == 37.848250233
        assert pos.avg_cost == 159.096809
        assert pos.price == 199.27
        assert pos.market_value == 7542.020824
        assert pos.unrealized_pl == 1520.484986
        assert pos.unrealized_pl_pct == 0.25251
        assert pos.tier_at_entry is None

    def test_buy_delegates_to_trader(self):
        trader = _mock_trader()
        trader.buy.return_value = {"symbol": "AAPL", "side": "buy"}
        account = AlpacaPaperAccount(trader=trader)

        result = account.buy("AAPL", 1000.0)

        trader.buy.assert_called_once_with("AAPL", 1000.0)
        assert result == {"symbol": "AAPL", "side": "buy"}

    def test_sell_delegates_to_trader_with_reason(self):
        trader = _mock_trader()
        trader.sell.return_value = {"symbol": "AAPL", "side": "sell"}
        account = AlpacaPaperAccount(trader=trader)

        result = account.sell("AAPL", reason="thesis broke")

        trader.sell.assert_called_once_with("AAPL", reason="thesis broke")
        assert result == {"symbol": "AAPL", "side": "sell"}


class TestGetAccounts:
    def test_returns_alpaca_when_enabled(self):
        with patch("src.accounts.AlpacaPaperAccount") as MockAlpaca:
            MockAlpaca.return_value.is_enabled.return_value = True
            accounts = get_accounts()
        assert len(accounts) == 1
        assert accounts[0].is_enabled() is True

    def test_omits_alpaca_when_disabled(self):
        with patch("src.accounts.AlpacaPaperAccount") as MockAlpaca:
            MockAlpaca.return_value.is_enabled.return_value = False
            accounts = get_accounts()
        assert accounts == []
