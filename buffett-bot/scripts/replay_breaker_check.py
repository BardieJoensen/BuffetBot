#!/usr/bin/env python3
"""
Replay historical news-triggered C downgrades through the ruleset 3.0.0
breaker check.

Before 3.0.0 a REVIEW from the Haiku news screen sent the company to a full
Sonnet re-analysis, and a C verdict there forced a sell. Since 3.0.0 the only
news event that can force a sell is a pre-registered thesis breaker that
Sonnet matches against the news (CompanyAnalyzer.check_thesis_breakers).

This script takes every `tier_history` row with trigger 'news_event' and
new_tier 'C', rebuilds the input the breaker check would have seen (the
breakers stored by the analysis that preceded the downgrade, and the news
events logged for that ticker in the preceding 24 hours) and asks the model
again. It never writes to the database. Each row costs one Sonnet call.

Production passes Finnhub's own story summaries. Only the headline and the
Haiku write-up survive in `news_events`, so the replay feeds the headline plus
Haiku's listed items, with Haiku's verdict line removed so it cannot steer the
answer. That is a limitation, not a faithful reproduction of the prompt.

Usage:
    python -m scripts.replay_breaker_check                 # prints a summary
    python -m scripts.replay_breaker_check --out replay.json
    python -m scripts.replay_breaker_check --db data/buffett_bot_v2.db --limit 5
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional

from src.analysis_parser import normalize_thesis_breakers
from src.database import DEFAULT_DB_PATH


def _news_text(events: list[sqlite3.Row]) -> str:
    blocks = []
    for e in events:
        summary = (e["summary"] or "").replace("RED FLAGS DETECTED: YES", "").replace("RED FLAGS DETECTED: NO", "")
        summary = summary.replace("CONCERNING ITEMS:", "").strip()
        line = f"[{(e['published_at'] or '')[:10]}] {e['headline']}"
        if e["source"]:
            line += f" ({e['source']})"
        if summary:
            line += f"\n  {summary[:500]}"
        blocks.append(line)
    return "\n\n".join(blocks) or "(no news)"


def load_cases(conn: sqlite3.Connection, limit: Optional[int] = None) -> list[dict]:
    """One dict per historical news downgrade, with the breakers and news it would be checked against."""
    sql = (
        "SELECT ticker, old_tier, changed_at FROM tier_history "
        "WHERE new_tier='C' AND old_tier!='C' AND trigger='news_event' ORDER BY changed_at"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"
    sold = {
        r["ticker"]: r["decided_at"]
        for r in conn.execute("SELECT ticker, decided_at FROM decision_log WHERE action='sell' ORDER BY decided_at")
    }
    cases = []
    for d in conn.execute(sql):
        t, at = d["ticker"], d["changed_at"]
        prior = conn.execute(
            "SELECT analyzed_at, thesis_breakers FROM deep_analyses WHERE ticker=? AND analyzed_at<? "
            "ORDER BY analyzed_at DESC LIMIT 1",
            (t, at),
        ).fetchone()
        breakers = normalize_thesis_breakers(json.loads(prior["thesis_breakers"] or "[]")) if prior else []
        events = list(
            conn.execute(
                "SELECT headline, source, published_at, summary FROM news_events "
                "WHERE ticker=? AND detected_at BETWEEN datetime(?,'-1 day') AND ? ORDER BY detected_at",
                (t, at, at),
            )
        )
        cases.append(
            {
                "ticker": t,
                "old_tier": d["old_tier"],
                "downgraded_at": at,
                "prior_analysis": prior["analyzed_at"] if prior else None,
                "breakers": breakers,
                "news": _news_text(events),
                "sold_under_old_rules": sold.get(t),
            }
        )
    return cases


def replay(cases: list[dict], analyzer, *, retries: int = 3) -> list[dict]:
    for case in cases:
        if not case["breakers"]:
            case["verdict"] = {"fired": False, "note": "no usable pre-registered breakers"}
            continue
        for _ in range(retries):
            try:
                case["verdict"] = analyzer.check_thesis_breakers(case["ticker"], case["breakers"], case["news"])
                break
            except Exception as exc:  # parse error or transient API failure
                case["verdict"] = {"fired": None, "error": f"{type(exc).__name__}: {exc}"}
                time.sleep(2)
        v = case["verdict"]
        print(
            f"{case['ticker']}: fired={v.get('fired')} breaker={v.get('breaker_index')} "
            f"evidence={(v.get('evidence') or v.get('error') or '')[:100]}",
            file=sys.stderr,
            flush=True,
        )
    return cases


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--out", type=Path, help="Write the full replay (inputs + verdicts) as JSON")
    parser.add_argument("--limit", type=int, help="Only the first N downgrades (cost control)")
    args = parser.parse_args(argv)

    if not args.db.exists():
        print(f"No such database: {args.db}", file=sys.stderr)
        return 1
    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    cases = load_cases(conn, args.limit)
    conn.close()
    print(
        f"{len(cases)} historical news downgrade(s); {sum(1 for c in cases if c['breakers'])} with breakers",
        file=sys.stderr,
    )

    from src.analyzer import CompanyAnalyzer

    cases = replay(cases, CompanyAnalyzer())
    fired = [c["ticker"] for c in cases if c["verdict"].get("fired")]
    errors = [c["ticker"] for c in cases if c["verdict"].get("fired") is None]
    print(f"fired: {len(fired)}/{len(cases)} {fired or ''}  errors: {len(errors)} {errors or ''}")
    if args.out:
        args.out.write_text(json.dumps(cases, indent=1, default=str))
        print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
