#!/usr/bin/env python3
"""
Backfill the quarantine correction into historical portfolio_snapshots.

Alpaca keeps a delisted holding in its /v2/account equity at a frozen mark long
after the asset stops trading (its own portfolio-history endpoint drops it), so
every snapshot taken before the quarantine fix overstates equity. This script
recomputes those rows.

Correction, per row:
    equity          = COALESCE(gross_equity, equity) - sum(untradable market_value)
    invested_value  = equity - cash
    invested_pct    = invested_value / equity
    equity_dkk      = new_equity x the rate implied by the row, so the FX rate
                      actually in effect that day is preserved rather than
                      re-fetched (and re-running cannot compound the change)

Anchoring on COALESCE(gross_equity, equity) makes the script idempotent: a
second run recomputes from the original broker figure and writes an identical
result. --revert restores it.

Dry-run is the default. Stop the scheduler before --apply: SQLite's busy_timeout
is 5s, and the 22:00 daily_snapshot could otherwise interleave and append an
uncorrected row.

Usage:
    python -m scripts.backfill_quarantine_snapshots                    # dry run, live lookup
    python -m scripts.backfill_quarantine_snapshots --symbols AL       # dry run, offline
    python -m scripts.backfill_quarantine_snapshots --symbols AL --apply
    python -m scripts.backfill_quarantine_snapshots --revert --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.database import DEFAULT_DB_PATH, _open

logger = logging.getLogger(__name__)


def _discover_untradable(symbols: set[str]) -> dict[str, str]:
    """
    Ask the broker which of `symbols` are non-tradable today.

    Returns {symbol: status}. Only used when --symbols was not supplied.
    """
    from src.paper_trader import PaperTrader

    trader = PaperTrader()
    if not trader.is_enabled():
        raise SystemExit(
            "Alpaca is not configured, so tradability cannot be looked up.\n"
            "Re-run with --symbols AL (comma-separated) to correct known symbols offline."
        )

    out: dict[str, str] = {}
    for sym in sorted(symbols):
        tradable, status = trader.get_asset_status(sym)
        if not tradable:
            out[sym] = status
    return out


def _freeze_dates(rows: list[dict], symbols: set[str]) -> dict[str, Optional[str]]:
    """
    Infer, per symbol, the `as_of` after which its price stopped moving.

    A symbol non-tradable *today* may have traded perfectly well during the
    earlier snapshots, and blindly subtracting would corrupt history in the
    other direction. A frozen mark is the observable signature of the
    delisting, so rows strictly after the last price change get corrected.

    Returns {symbol: as_of_of_last_price_change}, or None when the price never
    changed across the whole history (correct every row).
    """
    last_change: dict[str, Optional[str]] = {}
    for sym in symbols:
        prev_price = None
        last: Optional[str] = None
        for row in rows:
            pos = next((p for p in (row["positions"] or []) if p.get("symbol") == sym), None)
            if pos is None:
                continue
            price = pos.get("price")
            if prev_price is not None and price != prev_price:
                last = row["as_of"]
            prev_price = price
        last_change[sym] = last
    return last_change


def _load_rows(db_path: Path, account_id: str) -> list[dict]:
    with _open(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM portfolio_snapshots WHERE account_id = ? ORDER BY as_of ASC",
            (account_id,),
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["positions"] = json.loads(d["positions"]) if d["positions"] else []
        out.append(d)
    return out


def _plan_row(row: dict, affected: dict[str, str]) -> Optional[dict]:
    """Compute the corrected values for one row, or None if nothing changes."""
    base_equity = row["gross_equity"] if row["gross_equity"] is not None else row["equity"]
    positions = row["positions"] or []

    untradable = sum(p.get("market_value", 0.0) for p in positions if p.get("symbol") in affected)
    if not untradable:
        return None

    new_equity = base_equity - untradable
    new_invested = new_equity - row["cash"]
    new_pct = (new_invested / new_equity) if new_equity else 0.0

    # Re-derive DKK from the rate implied by the row, not by scaling the stored
    # value: the rate survives correction (both sides scale together) so this
    # is idempotent, whereas scaling the value compounds on every re-run. It
    # also preserves the FX rate actually in effect that day.
    new_dkk = row["equity_dkk"]
    if new_dkk is not None and row["equity"]:
        rate = row["equity_dkk"] / row["equity"]
        new_dkk = new_equity * rate

    # Already corrected to exactly these values — report no change so a re-run
    # is a genuine no-op rather than a rewrite.
    if row["gross_equity"] is not None and row["equity"] == new_equity and row["untradable_value"] == untradable:
        return None

    # Reshape the JSON to match what the live path now writes, so a backfilled
    # row is byte-shaped like a new one and is self-describing.
    new_positions = []
    for p in positions:
        q = dict(p)
        if q.get("symbol") in affected:
            q["tradable"] = False
            q["asset_status"] = affected[q["symbol"]]
        else:
            q.setdefault("tradable", True)
            q.setdefault("asset_status", None)
        new_positions.append(q)

    return {
        "id": row["id"],
        "as_of": row["as_of"],
        "old_equity": row["equity"],
        "equity": new_equity,
        "gross_equity": base_equity,
        "untradable_value": untradable,
        "invested_value": new_invested,
        "invested_pct": new_pct,
        "old_equity_dkk": row["equity_dkk"],
        "equity_dkk": new_dkk,
        "positions": new_positions,
    }


def _apply(db_path: Path, changes: list[dict]) -> None:
    with _open(db_path) as conn:
        for c in changes:
            conn.execute(
                """
                UPDATE portfolio_snapshots
                SET equity = ?, gross_equity = ?, untradable_value = ?,
                    invested_value = ?, invested_pct = ?, equity_dkk = ?, positions = ?
                WHERE id = ?
                """,
                (
                    c["equity"],
                    c["gross_equity"],
                    c["untradable_value"],
                    c["invested_value"],
                    c["invested_pct"],
                    c["equity_dkk"],
                    json.dumps(c["positions"]),
                    c["id"],
                ),
            )


def _plan_revert(rows: list[dict]) -> list[dict]:
    """Restore equity from gross_equity and clear the correction columns."""
    changes = []
    for row in rows:
        if row["gross_equity"] is None:
            continue
        base = row["gross_equity"]
        invested = base - row["cash"]
        dkk = row["equity_dkk"]
        if dkk is not None and row["equity"]:
            dkk = base * (row["equity_dkk"] / row["equity"])
        changes.append(
            {
                "id": row["id"],
                "as_of": row["as_of"],
                "old_equity": row["equity"],
                "equity": base,
                "gross_equity": None,
                "untradable_value": None,
                "invested_value": invested,
                "invested_pct": (invested / base) if base else 0.0,
                "old_equity_dkk": row["equity_dkk"],
                "equity_dkk": dkk,
                "positions": row["positions"],
            }
        )
    return changes


def _print_plan(changes: list[dict], *, revert: bool) -> None:
    verb = "restore" if revert else "correct"
    print(f"\n  {'id':>4}  {'as_of':<21} {'equity':>13} -> {'corrected':>13} {'delta':>12}")
    print(f"  {'─' * 4}  {'─' * 21} {'─' * 13}    {'─' * 13} {'─' * 12}")
    for c in changes:
        delta = c["equity"] - c["old_equity"]
        print(
            f"  {c['id']:>4}  {str(c['as_of'])[:19]:<21} {c['old_equity']:>13,.2f} -> "
            f"{c['equity']:>13,.2f} {delta:>12,.2f}"
        )
    if changes:
        mean = sum(c["equity"] - c["old_equity"] for c in changes) / len(changes)
        print(f"\n{len(changes)} row(s) would {verb}. Mean delta {mean:,.2f} USD.")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--account-id", default="alpaca_paper")
    parser.add_argument("--symbols", help="Comma-separated symbols to treat as non-tradable (skips broker lookup)")
    parser.add_argument(
        "--from",
        dest="from_mode",
        default="frozen",
        help="'frozen' (default, infer from a flat price), 'always', or an ISO date",
    )
    parser.add_argument("--apply", action="store_true", help="Write changes (default is a dry run)")
    parser.add_argument("--revert", action="store_true", help="Restore equity from gross_equity")
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if not args.db.exists():
        print(f"No such database: {args.db}", file=sys.stderr)
        return 1

    rows = _load_rows(args.db, args.account_id)
    if not rows:
        print(f"No snapshots for account {args.account_id!r}.")
        return 0

    if args.revert:
        changes = _plan_revert(rows)
    else:
        if args.symbols:
            affected = {s.strip().upper(): "inactive" for s in args.symbols.split(",") if s.strip()}
        else:
            all_symbols = {p.get("symbol") for r in rows for p in (r["positions"] or []) if p.get("symbol")}
            affected = _discover_untradable(all_symbols)

        if not affected:
            print("No non-tradable symbols found in this history — nothing to correct.")
            return 0

        print(f"Non-tradable symbols: {', '.join(f'{s} ({st})' for s, st in sorted(affected.items()))}")

        cutovers = _freeze_dates(rows, set(affected))
        for sym, cut in sorted(cutovers.items()):
            if args.from_mode == "always":
                print(f"  {sym}: --from always -> correcting every row")
            elif args.from_mode != "frozen":
                print(f"  {sym}: --from {args.from_mode} -> correcting rows on/after that date")
            elif cut is None:
                print(f"  {sym}: price never moved across this history -> correcting every row")
            else:
                print(f"  {sym}: price last moved at {str(cut)[:19]} -> correcting rows after that")

        changes = []
        for row in rows:
            if args.from_mode == "frozen":
                active = {s: st for s, st in affected.items() if cutovers[s] is None or row["as_of"] > cutovers[s]}
            elif args.from_mode == "always":
                active = affected
            else:
                active = affected if row["as_of"] >= args.from_mode else {}
            if not active:
                continue
            planned = _plan_row(row, active)
            if planned:
                changes.append(planned)

    if not changes:
        print("\nNothing to change — already up to date.")
        return 0

    _print_plan(changes, revert=args.revert)

    if not args.apply:
        print("\nDRY RUN — nothing written. Re-run with --apply.")
        return 0

    if not args.no_backup:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup = args.db.with_name(f"{args.db.name}.bak-{stamp}")
        shutil.copy2(args.db, backup)
        print(f"\nBackup written to {backup}")

    _apply(args.db, changes)
    print(f"Applied {len(changes)} row update(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
