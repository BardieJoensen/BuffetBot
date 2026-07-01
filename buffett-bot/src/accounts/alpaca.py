"""
Account adapter over the existing, tested PaperTrader (Alpaca paper API).

Pure mapping layer — PaperTrader itself is untouched. All behavior
(safety checks, duplicate-buy prevention, logging, notifications) still
lives there; this class only reshapes its dicts into the broker-agnostic
AccountState / PositionState dataclasses.
"""

from datetime import datetime, timezone
from typing import Optional

from ..paper_trader import PaperTrader
from .base import AccountState, PositionState


class AlpacaPaperAccount:
    account_id = "alpaca_paper"
    currency = "USD"

    def __init__(self, trader: Optional[PaperTrader] = None):
        self._trader = trader if trader is not None else PaperTrader()

    def is_enabled(self) -> bool:
        return self._trader.is_enabled()

    def get_state(self) -> AccountState:
        account = self._trader.get_account()
        # PaperTrader reports API failures as {"error": ...} instead of raising.
        # Surface that as an exception here — mapping it with .get(..., 0.0)
        # would fabricate a valid-looking equity=0 state, which would poison
        # the snapshot equity curve and any sizing math built on it.
        if "error" in account:
            raise RuntimeError(f"Alpaca account query failed: {account['error']}")
        equity = account.get("equity", 0.0)
        cash = account.get("cash", 0.0)
        invested_value = equity - cash
        return AccountState(
            account_id=self.account_id,
            currency=self.currency,
            equity=equity,
            cash=cash,
            buying_power=account.get("buying_power", 0.0),
            invested_value=invested_value,
            invested_pct=(invested_value / equity) if equity else 0.0,
            as_of=datetime.now(timezone.utc),
        )

    def get_positions(self) -> list[PositionState]:
        return [
            PositionState(
                symbol=p["symbol"],
                shares=p["qty"],
                avg_cost=p["avg_entry_price"],
                price=p["current_price"],
                market_value=p["market_value"],
                unrealized_pl=p["unrealized_pl"],
                unrealized_pl_pct=p["unrealized_plpc"],
            )
            for p in self._trader.get_positions()
        ]

    def buy(self, symbol: str, amount: float, *, context: Optional[dict] = None) -> Optional[dict]:
        return self._trader.buy(symbol, amount)

    def sell(self, symbol: str, *, reason: str = "", context: Optional[dict] = None) -> Optional[dict]:
        return self._trader.sell(symbol, reason=reason)
