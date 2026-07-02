#!/usr/bin/env python3
"""
Portfolio Status — Phase B

On-demand summary of live account state: equity, cash, invested %, open
positions with unrealized P&L, and realized P&L pulled from the decision
journal (src.database.Database.get_closed_trades / tier_performance) rather
than recomputed from scratch.

Reads accounts live via src.accounts.get_accounts() (no DB write) and the
journal read-only. Multi-account safe: Nordnet ASK joins the loop once
Phase D lands.

Usage:
    python -m scripts.portfolio_status
"""

from __future__ import annotations

import logging

from src.accounts import Account, get_accounts
from src.database import Database
from src.fx import get_cached_usd_dkk_rate, usd_to_dkk

logger = logging.getLogger(__name__)


def _fmt_money(v: float, currency: str) -> str:
    if currency == "USD":
        return f"${v:,.2f}"
    return f"{v:,.2f} {currency}"


def _fmt_pct(v: object) -> str:
    return f"{v:+.1%}" if isinstance(v, (int, float)) else "    —"


def _fmt_rate(v: object) -> str:
    return f"{v:.0%}" if isinstance(v, (int, float)) else "—"


def build_account_section(account: Account, db: Database) -> list[str]:
    """Live account state + open positions + realized P&L from the journal."""
    out: list[str] = []
    state = account.get_state()
    positions = account.get_positions()

    out.append("=" * 78)
    out.append(f"ACCOUNT: {state.account_id} ({state.currency})")
    out.append("=" * 78)
    out.append("")
    out.append(f"  Equity:        {_fmt_money(state.equity, state.currency)}")
    if state.currency == "USD":
        equity_dkk = usd_to_dkk(state.equity, rate=get_cached_usd_dkk_rate())
        if equity_dkk is not None:
            out.append(f"                 ({_fmt_money(equity_dkk, 'DKK')})")
    out.append(f"  Cash:          {_fmt_money(state.cash, state.currency)}")
    out.append(f"  Buying power:  {_fmt_money(state.buying_power, state.currency)}")
    out.append(
        f"  Invested:      {_fmt_money(state.invested_value, state.currency)} ({state.invested_pct:.1%} of equity)"
    )
    out.append("")

    out.append("-" * 78)
    out.append(f"OPEN POSITIONS ({len(positions)})")
    out.append("")
    if positions:
        out.append(
            f"  {'Symbol':<8} {'Shares':>10}  {'Avg Cost':>10}  {'Price':>10}  "
            f"{'Mkt Value':>12}  {'Unreal P&L':>12}  {'%':>8}"
        )
        out.append(
            f"  {'-' * 7:<8} {'-' * 9:>10}  {'-' * 9:>10}  {'-' * 9:>10}  {'-' * 11:>12}  {'-' * 11:>12}  {'-' * 7:>8}"
        )
        total_unrealized = 0.0
        for p in sorted(positions, key=lambda p: p.market_value, reverse=True):
            total_unrealized += p.unrealized_pl
            out.append(
                f"  {p.symbol:<8} {p.shares:>10.3f}  {p.avg_cost:>10.2f}  {p.price:>10.2f}  "
                f"{p.market_value:>12,.2f}  {p.unrealized_pl:>+12,.2f}  {_fmt_pct(p.unrealized_pl_pct):>8}"
            )
        out.append("")
        out.append(f"  Total unrealized P&L: {total_unrealized:+,.2f} {state.currency}")
    else:
        out.append("  No open positions.")
    out.append("")

    trades = db.get_closed_trades(limit=1000)
    out.append("-" * 78)
    out.append("REALIZED P&L (closed trades, from the journal)")
    out.append("")
    if trades:
        realized = [t["realized_pl"] for t in trades if t.get("realized_pl") is not None]
        wins = sum(1 for pl in realized if pl > 0)
        out.append(
            f"  {len(trades)} closed trades, {wins} winners ({_fmt_rate(wins / len(trades) if trades else None)} hit rate)"
        )
        out.append(f"  Total realized P&L: {sum(realized):+,.2f} {state.currency}")
        out.append("")
        perf = db.tier_performance()
        if perf:
            out.append("  Per-tier performance:")
            for r in perf:
                out.append(
                    f"    {r['tier']:<3} n={r['n']:<3}  avg={_fmt_pct(r['avg_realized_pct']):>8}  "
                    f"hit={_fmt_rate(r['hit_rate']):>5}  sound={_fmt_rate(r['soundness_rate']):>5}"
                )
    else:
        out.append("  No closed trades yet.")
    out.append("")

    return out


def build_report(accounts: list[Account], db: Database) -> str:
    """Build the plain-text portfolio status report across all enabled accounts."""
    out: list[str] = []
    out.append("=" * 78)
    out.append("PORTFOLIO STATUS")
    out.append("=" * 78)
    out.append("")

    if not accounts:
        out.append("No enabled accounts.")
        return "\n".join(out)

    total_equity_dkk = 0.0
    have_dkk = False
    for account in accounts:
        out.extend(build_account_section(account, db))
        state = account.get_state()
        if state.currency == "USD":
            equity_dkk = usd_to_dkk(state.equity, rate=get_cached_usd_dkk_rate())
        elif state.currency == "DKK":
            equity_dkk = state.equity
        else:
            equity_dkk = None
        if equity_dkk is not None:
            total_equity_dkk += equity_dkk
            have_dkk = True

    if len(accounts) > 1 and have_dkk:
        out.append("=" * 78)
        out.append(f"COMBINED EQUITY: {total_equity_dkk:,.2f} DKK")
        out.append("=" * 78)

    return "\n".join(out)


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    db = Database()
    accounts = get_accounts()
    print(build_report(accounts, db))


if __name__ == "__main__":
    main()
