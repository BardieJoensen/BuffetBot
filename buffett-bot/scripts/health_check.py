#!/usr/bin/env python3
"""
Run the health checks on demand and print the digest.

Same invariants the 22:30 scheduler job runs, without the Discord throttle —
use this to check state after a deploy, or to confirm a fix landed, rather
than waiting for the next nightly run.

Exit code is 0 when everything passes and 1 when anything is WARN or FAIL, so
it composes into CI or a shell check.

Usage:
    python -m scripts.health_check
    python -m scripts.health_check --db /path/to/buffett_bot_v2.db
    python -m scripts.health_check --quiet     # only problems
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

from src.database import DEFAULT_DB_PATH, Database
from src.health import format_digest, run_health_checks


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--quiet", action="store_true", help="Print only findings that need attention")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING, format="%(message)s")

    if not args.db.exists():
        print(f"No such database: {args.db}", file=sys.stderr)
        return 1

    findings = run_health_checks(Database(db_path=args.db))
    shown = [f for f in findings if f.is_alerting] if args.quiet else findings

    if args.quiet and not shown:
        print("All health checks passed.")
    else:
        print(format_digest(shown))

    return 1 if any(f.is_alerting for f in findings) else 0


if __name__ == "__main__":
    raise SystemExit(main())
