"""
Broker-agnostic account layer.

`get_accounts()` is the single entry point the scheduler, deployment
engine, and snapshot logging use to reach whichever accounts are
configured and enabled — without caring whether the backing broker is
Alpaca (API) or Nordnet (manual entry, added later).
"""

from .alpaca import AlpacaPaperAccount
from .base import Account, AccountState, PositionState


def get_accounts() -> list[Account]:
    """Return all configured, enabled accounts."""
    accounts: list[Account] = []

    alpaca = AlpacaPaperAccount()
    if alpaca.is_enabled():
        accounts.append(alpaca)

    # Nordnet ASK account joins this list once NORDNET_ENABLED lands (Phase D).

    return accounts


__all__ = ["Account", "AccountState", "PositionState", "AlpacaPaperAccount", "get_accounts"]
