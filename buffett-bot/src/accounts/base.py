"""
Broker-agnostic account interface.

Every account implementation (the live Alpaca paper account today, a
manual-entry Nordnet ASK account later) satisfies this shape, so the
scheduler, deployment engine, and snapshot logging work identically
regardless of which broker is behind them.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Protocol, runtime_checkable


@dataclass
class AccountState:
    """Point-in-time cash/equity summary for one account."""

    account_id: str
    currency: str
    # TRADABLE equity: the broker's figure less any quarantined holdings.
    # Corrected in place rather than exposed as a separate field, because every
    # consumer reads `.equity` — a parallel `tradable_equity` would leave the
    # inflated number as the default and guarantee a call site stays wrong.
    equity: float
    cash: float
    buying_power: float
    invested_value: float
    invested_pct: float  # fraction 0.0-1.0, not percent
    as_of: datetime
    # Broker-reported equity before the quarantine correction, kept so the two
    # can be reconciled against the broker's own dashboard after the fact.
    gross_equity: Optional[float] = None
    untradable_value: float = 0.0


@dataclass
class PositionState:
    """A single held position, broker-agnostic."""

    symbol: str
    shares: float
    avg_cost: float
    price: float
    market_value: float
    unrealized_pl: float
    unrealized_pl_pct: float
    tier_at_entry: Optional[str] = None  # S/A/B/C, matches tier_engine
    # Defaults to True so an adapter that can't report tradability, or a
    # position dict predating this field, keeps its original meaning.
    tradable: bool = True
    asset_status: Optional[str] = None  # broker status, e.g. 'inactive'


@runtime_checkable
class Account(Protocol):
    """Interface every account/broker adapter must satisfy."""

    account_id: str
    currency: str

    def is_enabled(self) -> bool:
        """Whether this account is configured and usable right now."""
        ...

    def get_state(self) -> AccountState:
        """Return the current cash/equity snapshot."""
        ...

    def get_positions(self) -> list[PositionState]:
        """Return current open positions."""
        ...

    def buy(self, symbol: str, amount: float, *, context: Optional[dict] = None) -> Optional[dict]:
        """Place a buy for a currency amount. Returns the order result, or None if skipped."""
        ...

    def get_order(self, order_id: str) -> dict:
        """Return normalized broker status and fill details for an order."""
        ...

    def sell(
        self,
        symbol: str,
        *,
        reason: str = "",
        quantity: Optional[float] = None,
        context: Optional[dict] = None,
    ) -> Optional[dict]:
        """Sell all or part of a position. Returns the order result, or None if skipped."""
        ...
