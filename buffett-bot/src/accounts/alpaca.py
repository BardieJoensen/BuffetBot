"""
Account adapter over the existing, tested PaperTrader (Alpaca paper API).

Pure mapping layer — PaperTrader itself is untouched. All behavior
(safety checks, duplicate-buy prevention, logging, notifications) still
lives there; this class only reshapes its dicts into the broker-agnostic
AccountState / PositionState dataclasses.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from ..paper_trader import PaperTrader
from .base import AccountState, PositionState

logger = logging.getLogger(__name__)

# Refuse to apply a quarantine correction larger than this share of equity. A
# correction that big is far more likely a bad asset-status read than reality,
# and acting on it would collapse equity toward cash.
_MAX_QUARANTINE_FRACTION = 0.5


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
        gross_equity = account.get("equity", 0.0)
        cash = account.get("cash", 0.0)

        # Alpaca's /v2/account equity keeps carrying delisted holdings at a
        # frozen mark long after they stop being tradable — its own
        # portfolio-history endpoint drops them — so the reported figure
        # overstates what the account is actually worth.
        #
        # Subtract the quarantined value rather than recomputing
        # cash + sum(tradable positions): subtraction is an exact no-op when
        # nothing is quarantined, and it doesn't silently absorb unsettled
        # cash, pending dividends or accrued fees into a fabricated equity.
        untradable_value = sum(p["market_value"] for p in self._trader.get_positions() if not p.get("tradable", True))
        if gross_equity > 0 and untradable_value > _MAX_QUARANTINE_FRACTION * gross_equity:
            logger.error(
                "Refusing quarantine correction: %.2f of %.2f equity reported non-tradable. "
                "Treating as a bad asset-status read; equity left uncorrected.",
                untradable_value,
                gross_equity,
            )
            untradable_value = 0.0

        equity = gross_equity - untradable_value
        invested_value = equity - cash
        return AccountState(
            account_id=self.account_id,
            currency=self.currency,
            equity=equity,
            cash=cash,
            # Left uncorrected: Alpaca extends margin against the frozen
            # holding, but buying_power only binds if gap-based sizing exceeds
            # it, which it does not at current targets.
            buying_power=account.get("buying_power", 0.0),
            invested_value=invested_value,
            invested_pct=(invested_value / equity) if equity else 0.0,
            as_of=datetime.now(timezone.utc),
            gross_equity=gross_equity,
            untradable_value=untradable_value,
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
                # .get(): an adapter or fixture predating tradability
                # annotation must keep meaning "tradable", not "quarantined".
                tradable=p.get("tradable", True),
                asset_status=p.get("asset_status"),
            )
            for p in self._trader.get_positions()
        ]

    def buy(self, symbol: str, amount: float, *, context: Optional[dict] = None) -> Optional[dict]:
        return self._trader.buy(symbol, amount)

    def get_order(self, order_id: str) -> dict:
        return self._trader.get_order(order_id)

    def sell(
        self,
        symbol: str,
        *,
        reason: str = "",
        quantity: Optional[float] = None,
        context: Optional[dict] = None,
    ) -> Optional[dict]:
        if quantity is None:
            return self._trader.sell(symbol, reason=reason)
        return self._trader.sell(symbol, reason=reason, quantity=quantity)
