#!/usr/bin/env python3
"""
Shorten the expiry on C-tier analyses written before the short-TTL rule.

save_deep_analysis now caps C-tier verdicts at C_TIER_ANALYSIS_DAYS, but that
only applies going forward. Analyses already on disk keep the 180-day expiry
they were written with, so every stock stranded before the change stays
stranded — AAPL, rated C on 2026-06-23, would not become re-analysable until
December.

The trapdoor: a C-tier stock isn't held (so it misses the portfolio queue),
holds a non-expired analysis (so get_haiku_passes_without_analysis skips it),
and isn't S/A/B (so news never looks at it). Nothing can revisit it until the
analysis lapses. In five months 74 stocks reached C and none ever recovered.

Idempotent: only shortens, never extends, so re-running changes nothing.

Usage:
    python -m scripts.backfill_c_tier_expiry              # dry run
    python -m scripts.backfill_c_tier_expiry --apply
    python -m scripts.backfill_c_tier_expiry --days 30 --apply
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from src.config import config
from src.database import DEFAULT_DB_PATH, _open

logger = logging.getLogger(__name__)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--days", type=int, default=config.c_tier_analysis_days)
    parser.add_argument("--apply", action="store_true", help="Write changes (default is a dry run)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING, format="%(message)s")

    if not args.db.exists():
        print(f"No such database: {args.db}", file=sys.stderr)
        return 1

    cutoff = (datetime.now() + timedelta(days=args.days)).isoformat()

    with _open(args.db) as conn:
        rows = conn.execute(
            """
            SELECT ticker, expires_at FROM deep_analyses
            WHERE tier = 'C' AND expires_at > ?
            ORDER BY expires_at
            """,
            (cutoff,),
        ).fetchall()

    if not rows:
        print(f"No C-tier analyses expire later than {args.days} days out — nothing to change.")
        return 0

    print(f"Shortening {len(rows)} C-tier analyses to {args.days} days out ({cutoff[:10]}):\n")
    print(f"  {'ticker':<8} {'current expiry':<14} -> new expiry")
    print(f"  {'─' * 8} {'─' * 14}    {'─' * 10}")
    for r in rows[:20]:
        print(f"  {r['ticker']:<8} {str(r['expires_at'])[:10]:<14} -> {cutoff[:10]}")
    if len(rows) > 20:
        print(f"  ... and {len(rows) - 20} more")

    if not args.apply:
        print("\nDRY RUN — nothing written. Re-run with --apply.")
        return 0

    with _open(args.db) as conn:
        # Guarded by the same predicate as the SELECT so this only ever
        # shortens — re-running is a no-op rather than a repeated rewrite.
        cur = conn.execute(
            "UPDATE deep_analyses SET expires_at = ? WHERE tier = 'C' AND expires_at > ?",
            (cutoff, cutoff),
        )
        print(f"\nUpdated {cur.rowcount} analyses.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
