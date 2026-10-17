#!/usr/bin/env python3
"""
Normalise stored thesis breakers (ruleset 3.0.0 hygiene).

Since ruleset 3.0.0 a pre-registered thesis breaker is the only news event
that can force a sell, so each stored item must be one observable event. The
old list parser left markdown artifacts ("Dividend cut** - ...") and kept
section headings ("Sell Immediately If:**") and watch-list fragments as if
they were breakers. On 2026-10-17, 949 of 1,169 stored items carried stray
bold markup and 36 were headings.

This applies analysis_parser.normalize_thesis_breakers to every row of
deep_analyses: strips markup, drops headings and fragments, leaves real
events untouched. Idempotent. A SQLite backup is written before --apply.

Usage:
    python -m scripts.clean_thesis_breakers              # dry run, prints a diff summary
    python -m scripts.clean_thesis_breakers --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.analysis_parser import normalize_thesis_breakers
from src.database import DEFAULT_DB_PATH, _open

logger = logging.getLogger(__name__)


def plan_changes(conn: sqlite3.Connection) -> list[tuple[str, str, list[str], list[str]]]:
    """(ticker, analyzed_at, old, new) for every row whose list would change."""
    changes = []
    for row in conn.execute("SELECT ticker, analyzed_at, thesis_breakers FROM deep_analyses"):
        try:
            old = json.loads(row["thesis_breakers"] or "[]")
        except json.JSONDecodeError:
            old = []
        new = normalize_thesis_breakers(old)
        if new != old:
            changes.append((row["ticker"], row["analyzed_at"], old, new))
    return changes


def backup(db_path: Path) -> Path:
    backups = db_path.parent / "backups"
    backups.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = backups / f"{db_path.stem}.pre-clean-breakers.{stamp}.db"
    src = sqlite3.connect(str(db_path))
    dst = sqlite3.connect(str(target))
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    return target


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--apply", action="store_true", help="Write changes (default is a dry run)")
    parser.add_argument("--show", type=int, default=10, help="How many changed rows to print")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    if not args.db.exists():
        print(f"No such database: {args.db}", file=sys.stderr)
        return 1

    with _open(args.db) as conn:
        changes = plan_changes(conn)
        total_rows = conn.execute("SELECT COUNT(*) FROM deep_analyses").fetchone()[0]

    items_before = sum(len(c[2]) for c in changes)
    items_after = sum(len(c[3]) for c in changes)
    emptied = [c[0] for c in changes if c[2] and not c[3]]
    print(f"deep_analyses rows: {total_rows}; rows that change: {len(changes)}")
    print(f"items in changed rows: {items_before} -> {items_after} ({items_before - items_after} dropped)")
    if emptied:
        print(f"rows left with NO usable breakers: {len(emptied)} ({', '.join(sorted(set(emptied)))})")
    for ticker, analyzed_at, old, new in changes[: args.show]:
        print(f"\n{ticker} @ {analyzed_at[:10]}")
        for item in old:
            if item not in new:
                print(f"  - {item[:110]}")
        for item in new:
            if item not in old:
                print(f"  + {item[:110]}")

    if not args.apply:
        print("\nDRY RUN — nothing written. Re-run with --apply.")
        return 0

    saved = backup(args.db)
    print(f"\nBackup written: {saved}")
    with _open(args.db) as conn:
        for ticker, analyzed_at, _old, new in changes:
            conn.execute(
                "UPDATE deep_analyses SET thesis_breakers = ? WHERE ticker = ? AND analyzed_at = ?",
                (json.dumps(new), ticker, analyzed_at),
            )
    print(f"Updated {len(changes)} row(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
