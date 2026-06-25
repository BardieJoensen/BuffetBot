"""
Decision Journal Report — Phase 1

Standalone, offline report over the decision journal and track record:

    python -m src.journal_report

Prints per-tier realized performance (count, avg realized %, hit rate, avg hold,
avg alpha, soundness rate) followed by the most recent closed trades. Reads only
from SQLite — no API calls.
"""

from __future__ import annotations

import logging

from src.database import Database


def _fmt_pct(v: object) -> str:
    return f"{v:+.1%}" if isinstance(v, (int, float)) else "    —"


def _fmt_days(v: object) -> str:
    return f"{v:.0f}" if isinstance(v, (int, float)) else "—"


def _fmt_rate(v: object) -> str:
    return f"{v:.0%}" if isinstance(v, (int, float)) else "—"


def build_report(db: Database) -> str:
    """Build the plain-text journal report from DB state."""
    out: list[str] = []
    out.append("=" * 78)
    out.append("DECISION JOURNAL — TRACK RECORD")
    out.append("=" * 78)
    out.append("")

    # ── Per-tier performance ───────────────────────────────────────────────
    perf = db.tier_performance()
    out.append("PER-TIER PERFORMANCE (closed trades)")
    out.append("")
    if perf:
        out.append(
            f"  {'Tier':<5} {'N':>3}  {'Avg P&L':>9}  {'Hit':>5}  {'Avg Hold':>9}  {'Avg Alpha':>10}  {'Sound':>6}"
        )
        out.append(
            f"  {'----':<5} {'---':>3}  {'---------':>9}  {'-----':>5}  "
            f"{'---------':>9}  {'----------':>10}  {'------':>6}"
        )
        for r in perf:
            out.append(
                f"  {r['tier']:<5} {r['n']:>3}  {_fmt_pct(r['avg_realized_pct']):>9}  "
                f"{_fmt_rate(r['hit_rate']):>5}  {_fmt_days(r['avg_hold_days']) + 'd':>9}  "
                f"{_fmt_pct(r['avg_alpha']):>10}  {_fmt_rate(r['soundness_rate']):>6}"
            )
    else:
        out.append("  No closed trades yet — the journal starts fresh.")
    out.append("")

    # ── Recent closed trades ───────────────────────────────────────────────
    trades = db.get_closed_trades(limit=25)
    out.append("-" * 78)
    out.append("RECENT CLOSED TRADES")
    out.append("")
    if trades:
        out.append(
            f"  {'Exit Date':<11} {'Ticker':<7} {'Tier':<5} {'P&L':>8}  "
            f"{'Alpha':>8}  {'Hold':>6}  {'Category':<13} {'Sound':<5}"
        )
        out.append(
            f"  {'-' * 10:<11} {'-' * 6:<7} {'----':<5} {'-' * 7:>8}  "
            f"{'-' * 7:>8}  {'-' * 5:>6}  {'-' * 12:<13} {'-----':<5}"
        )
        for t in trades:
            sound = t.get("reasoning_sound")
            sound_s = "✓" if sound == 1 else ("✗" if sound == 0 else "—")
            hold = t.get("hold_days")
            out.append(
                f"  {(t.get('exit_date') or '')[:10]:<11} {t['ticker']:<7} "
                f"{(t.get('tier_at_entry') or '?'):<5} {_fmt_pct(t.get('realized_pl_pct')):>8}  "
                f"{_fmt_pct(t.get('alpha')):>8}  {(str(hold) + 'd') if hold is not None else '—':>6}  "
                f"{(t.get('sell_category') or 'other'):<13} {sound_s:<5}"
            )
    else:
        out.append("  No closed trades yet.")
    out.append("")
    out.append("=" * 78)
    return "\n".join(out)


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    db = Database()
    print(build_report(db))


if __name__ == "__main__":
    main()
