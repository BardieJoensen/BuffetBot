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
    # Deliberately carries no "tradable" key: this fixture doubles as the
    # regression test that a position dict predating tradability annotation
    # still reads as tradable rather than quarantined.
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


def _mock_trader_with_untradable(enabled=True):
    """
    The live failure this whole mechanism exists for: AL (Air Lease) is
    delisted, but Alpaca still reports it in /v2/positions at a frozen $65.00
    mark and still counts it in /v2/account equity. Real numbers from the
    production paper account on 2026-07-24.
    """
    trader = _mock_trader(enabled=enabled)
    trader.get_account.return_value = {
        "equity": 107374.88,
        "cash": 30880.40,
        "buying_power": 386245.45,
        "portfolio_value": 107374.88,
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
            "tradable": True,
            "asset_status": "active",
        },
        {
            "symbol": "AL",
            "qty": 111.415815536,
            "market_value": 7242.03,
            "avg_entry_price": 64.622693,
            "current_price": 65.0,
            "unrealized_pl": 42.037967,
            "unrealized_plpc": 0.00584,
            "tradable": False,
            "asset_status": "inactive",
        },
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
        # Nothing quarantined: the correction must be an exact no-op.
        assert state.gross_equity == 107608.95
        assert state.untradable_value == 0.0

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
        # No "tradable" key in the source dict — must fail open.
        assert pos.tradable is True
        assert pos.asset_status is None


class TestQuarantineCorrection:
    """
    Alpaca keeps a delisted holding in /v2/account equity at a frozen mark, so
    the broker's number overstates the account. These pin the correction that
    turns $107,374.88 of reported equity into the $100,132.85 that Alpaca's own
    portfolio-history endpoint reports.
    """

    def test_excludes_untradable_from_equity(self):
        state = AlpacaPaperAccount(trader=_mock_trader_with_untradable()).get_state()

        assert state.gross_equity == 107374.88
        assert state.untradable_value == 7242.03
        assert state.equity == pytest.approx(100132.85)

    def test_invested_value_derives_from_corrected_equity(self):
        state = AlpacaPaperAccount(trader=_mock_trader_with_untradable()).get_state()

        assert state.invested_value == pytest.approx(100132.85 - 30880.40)
        assert state.invested_pct == pytest.approx((100132.85 - 30880.40) / 100132.85)

    def test_positions_carry_tradability(self):
        positions = AlpacaPaperAccount(trader=_mock_trader_with_untradable()).get_positions()
        by_symbol = {p.symbol: p for p in positions}

        # Annotated, not filtered — the sell path and duplicate-buy check both
        # rely on a quarantined holding still being reported as held.
        assert len(positions) == 2
        assert by_symbol["AGM"].tradable is True
        assert by_symbol["AL"].tradable is False
        assert by_symbol["AL"].asset_status == "inactive"

    def test_refuses_implausibly_large_correction(self):
        """
        A correction consuming most of the account is far more likely a bad
        asset-status read than reality. Applying it would collapse equity
        toward cash and fire the overweight-rotation branch on everything.
        """
        trader = _mock_trader()
        trader.get_account.return_value = {
            "equity": 10000.0,
            "cash": 1000.0,
            "buying_power": 20000.0,
        }
        trader.get_positions.return_value = [
            {
                "symbol": "XYZ",
                "qty": 1.0,
                "market_value": 9000.0,
                "avg_entry_price": 9000.0,
                "current_price": 9000.0,
                "unrealized_pl": 0.0,
                "unrealized_plpc": 0.0,
                "tradable": False,
                "asset_status": "inactive",
            }
        ]
        state = AlpacaPaperAccount(trader=trader).get_state()

        assert state.equity == 10000.0
        assert state.untradable_value == 0.0

    def test_position_still_flagged_when_correction_refused(self):
        """The circuit breaker suppresses the arithmetic, not the alert."""
        trader = _mock_trader()
        trader.get_account.return_value = {"equity": 10000.0, "cash": 1000.0, "buying_power": 0.0}
        trader.get_positions.return_value = [
            {
                "symbol": "XYZ",
                "qty": 1.0,
                "market_value": 9000.0,
                "avg_entry_price": 9000.0,
                "current_price": 9000.0,
                "unrealized_pl": 0.0,
                "unrealized_plpc": 0.0,
                "tradable": False,
                "asset_status": "inactive",
            }
        ]
        assert AlpacaPaperAccount(trader=trader).get_positions()[0].tradable is False

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
