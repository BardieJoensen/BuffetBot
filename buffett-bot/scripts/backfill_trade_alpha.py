#!/usr/bin/env python3
"""
Recompute benchmark return and alpha for closed trades.

fetch_benchmark_return used to resolve hold windows incorrectly in two ways,
both silent:

  - yfinance's `end` is exclusive, so the window was measured one bar short of
    the actual hold
  - a boundary landing on a weekend or market holiday yielded fewer than two
    bars, and the trade was journalled with no benchmark at all

So a trade could be stored with a plausible-looking alpha computed over the
wrong window, or with none. This re-derives both from the corrected bracketing
logic, and re-scores reasoning_sound, which depends on alpha.

Idempotent: alpha is always recomputed from the stored realized_pl_pct and a
freshly fetched benchmark, never from the previous alpha.

Usage:
    python -m scripts.backfill_trade_alpha                  # dry run
    python -m scripts.backfill_trade_alpha --apply
    python -m scripts.backfill_trade_alpha --ticker GIS --apply
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

from src.benchmark import fetch_benchmark_return
from src.config import config
from src.database import DEFAULT_DB_PATH, Database

logger = logging.getLogger(__name__)


def _fmt(v: Optional[float]) -> str:
    return "—" if v is None else f"{v:+.4%}"


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--ticker", help="Limit to one ticker")
    parser.add_argument("--symbol", default=config.benchmark_symbol, help="Benchmark symbol")
    parser.add_argument("--apply", action="store_true", help="Write changes (default is a dry run)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING, format="%(message)s")

    if not args.db.exists():
        print(f"No such database: {args.db}", file=sys.stderr)
        return 1

    db = Database(db_path=args.db)
    trades = db.get_closed_trades(ticker=args.ticker, limit=10_000)
    if not trades:
        print("No closed trades to process.")
        return 0

    print(f"Benchmark: {args.symbol}\n")
    header = f"  {'id':>3}  {'ticker':<7} {'hold window':<24} {'bench old':>11} {'bench new':>11} {'alpha old':>11} {'alpha new':>11}"
    print(header)
    print("  " + "─" * (len(header) - 2))

    changes: list[tuple[int, float]] = []
    unfetchable: list[str] = []
    for t in trades:
        entry, exit_ = t.get("entry_date"), t.get("exit_date")
        if not entry:
            print(f"  {t['id']:>3}  {t['ticker']:<7} {'(no entry_date — skipped)':<24}")
            continue

        bench = fetch_benchmark_return(entry, exit_, symbol=args.symbol)
        alpha = None
        if t.get("realized_pl_pct") is not None and bench is not None:
            alpha = t["realized_pl_pct"] - bench

        window = f"{str(entry)[:10]} -> {str(exit_ or '')[:10]}"
        print(
            f"  {t['id']:>3}  {t['ticker']:<7} {window:<24} "
            f"{_fmt(t.get('benchmark_return')):>11} {_fmt(bench):>11} "
            f"{_fmt(t.get('alpha')):>11} {_fmt(alpha):>11}"
        )

        if bench is None:
            # "Couldn't determine" is not the same as "the answer is zero".
            # Overwriting a stored benchmark with None on a transient fetch
            # failure would destroy good data, so leave the row alone.
            unfetchable.append(t["ticker"])
            continue
        if bench != t.get("benchmark_return"):
            changes.append((t["id"], bench))

    if unfetchable:
        print(f"\nCould not fetch a benchmark for {len(unfetchable)} trade(s): {', '.join(unfetchable)}")
        print("Left unchanged — a failed fetch must not erase a stored value.")

    if not changes:
        print("\nAll trades already have a correct benchmark — nothing to change.")
        return 0

    print(f"\n{len(changes)} trade(s) would change.")
    if not args.apply:
        print("DRY RUN — nothing written. Re-run with --apply.")
        return 0

    for trade_id, bench in changes:
        db.set_trade_benchmark(trade_id, bench)
    print(f"Applied {len(changes)} update(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
