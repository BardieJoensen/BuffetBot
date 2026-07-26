"""
Health checks — runtime invariants over the bot's own state.

WHY THIS EXISTS
---------------
This system is built on "log and continue": ~140 `except Exception` blocks,
about a third of which swallow entirely. For a scheduler that runs unattended
24/7 that is the right default — one bad ticker must not kill the weekly run —
but it has no counterbalance, and the result was a string of faults that
persisted for months while every job reported success:

  - a delisted holding inflated reported equity by 7.2% from February to July
  - per-trade alpha was computed over the wrong window, or not at all
  - the news pipeline checked 99 tickers and made 0 LLM calls, at INFO level
  - the analysis parser would have turned a format drift into "every stock is
    worthless" rather than an error

Every one of those produced a *plausible value*, not an exception. So this
module does not catch errors — it asserts that the outputs are sane. Each
check answers "if this silently broke, what would the data look like?" and
fires when it looks like that.

Checks are read-only and cheap (indexed SQL plus arithmetic). They are meant to
run daily and shout on Discord, not to gate anything.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# Severity ordering matters — the daily job alerts on WARN and above.
OK = "ok"
WARN = "warn"
FAIL = "fail"

_RANK = {OK: 0, WARN: 1, FAIL: 2}


@dataclass
class HealthFinding:
    """One invariant's verdict. `code` is stable and used for alert throttling."""

    code: str
    severity: str
    message: str
    detail: Optional[str] = None

    @property
    def is_alerting(self) -> bool:
        return _RANK[self.severity] >= _RANK[WARN]


def _ok(code: str, message: str) -> HealthFinding:
    return HealthFinding(code=code, severity=OK, message=message)


# ─── Individual checks ─────────────────────────────────────────────────────
#
# Each takes (db) and returns exactly one HealthFinding, so a digest always has
# the same shape whether things are healthy or not.


def check_snapshot_freshness(db, *, max_age_hours: int = 36) -> HealthFinding:
    """
    daily_snapshot writes one row per account per day. A gap means the job is
    dying — and because the scheduler catches per-account exceptions and
    continues, it would otherwise die quietly forever.
    """
    code = "snapshot_freshness"
    # get_latest_snapshots, not get_snapshots: the latter orders ASC, so a
    # limit there returns the oldest rows.
    snaps = db.get_latest_snapshots("alpaca_paper", limit=1)
    if not snaps:
        return HealthFinding(code, WARN, "No portfolio snapshots have ever been written.")

    latest = _parse_ts(snaps[0]["as_of"])
    if latest is None:
        return HealthFinding(code, WARN, f"Unparseable snapshot timestamp: {snaps[0]['as_of']!r}")

    age = datetime.now() - latest
    if age > timedelta(hours=max_age_hours):
        return HealthFinding(
            code,
            FAIL,
            f"No portfolio snapshot for {age.total_seconds() / 3600:.0f}h "
            f"(expected daily). The 22:00 snapshot job may be failing.",
        )
    return _ok(code, f"Snapshot {age.total_seconds() / 3600:.0f}h old.")


def check_frozen_position_marks(db, *, min_snapshots: int = 5) -> HealthFinding:
    """
    A position whose price is byte-identical across every recent snapshot is
    not a quiet stock — it is a stale mark. This is the check that would have
    caught AL months early: it sat at exactly $65.00, unchanged, in all 24
    snapshots while being counted at full value in equity.
    """
    code = "frozen_position_marks"
    recent = db.get_latest_snapshots("alpaca_paper", limit=min_snapshots)
    if len(recent) < min_snapshots:
        return _ok(code, "Not enough snapshot history to judge.")
    prices: dict[str, set[float]] = {}
    quarantined = {q["ticker"] for q in db.get_quarantined("alpaca_paper")}

    for snap in recent:
        for pos in snap.get("positions") or []:
            sym = pos.get("symbol")
            if sym and pos.get("price") is not None:
                prices.setdefault(sym, set()).add(pos["price"])

    # Only flag positions present in every snapshot — a newly opened position
    # legitimately has one observation.
    frozen = [
        sym
        for sym, seen in prices.items()
        if len(seen) == 1
        and sym not in quarantined
        and sum(1 for s in recent if any(p.get("symbol") == sym for p in s.get("positions") or [])) == len(recent)
    ]
    if frozen:
        return HealthFinding(
            code,
            WARN,
            f"{len(frozen)} position(s) have an unchanged price across the last "
            f"{min_snapshots} snapshots — likely a stale or delisted mark: {', '.join(sorted(frozen))}",
        )
    return _ok(code, "No frozen position marks.")


def check_quarantined_positions(db) -> HealthFinding:
    """Surfaces open quarantines in the digest even though they alert on their own."""
    code = "quarantined_positions"
    rows = db.get_quarantined()
    if not rows:
        return _ok(code, "No quarantined positions.")
    total = sum(r.get("market_value") or 0.0 for r in rows)
    names = ", ".join(f"{r['ticker']} (${(r.get('market_value') or 0):,.0f})" for r in rows)
    return HealthFinding(
        code, WARN, f"{len(rows)} non-tradable holding(s) worth ${total:,.0f} excluded from equity: {names}"
    )


def check_trade_alpha_coverage(db) -> HealthFinding:
    """
    Every closed trade should carry a benchmark and an alpha. A gap means the
    benchmark fetch failed at close time and nothing retried — which is exactly
    how AD ended up with no alpha at all for three weeks.
    """
    code = "trade_alpha_coverage"
    trades = db.get_closed_trades(limit=1000)
    if not trades:
        return _ok(code, "No closed trades yet.")

    missing = [t["ticker"] for t in trades if t.get("realized_pl_pct") is not None and t.get("alpha") is None]
    if missing:
        return HealthFinding(
            code,
            WARN,
            f"{len(missing)}/{len(trades)} closed trade(s) have no alpha: {', '.join(missing[:10])}. "
            f"Repair with: python -m scripts.backfill_trade_alpha --apply",
        )
    return _ok(code, f"All {len(trades)} closed trade(s) have alpha.")


def check_budget_reset(db, *, max_age_days: int = 8) -> HealthFinding:
    """
    Budgets reset only from monday_maintenance, and nothing enforces the weekly
    window at spend time. If that job stops running, every LLM budget stays
    exhausted indefinitely and the bot degrades to doing nothing — silently,
    because exhaustion logs at INFO.
    """
    code = "budget_reset"
    status = db.get_budget_status("weekly_news_haiku")
    if not status:
        return HealthFinding(code, WARN, "No budget cap rows found.")

    last_reset = status.get("last_reset")
    if not last_reset:
        return HealthFinding(code, WARN, "Budget caps have never been reset (monday_maintenance may never have run).")

    ts = _parse_ts(last_reset)
    if ts is None:
        return HealthFinding(code, WARN, f"Unparseable last_reset: {last_reset!r}")

    age_days = (datetime.now() - ts).days
    if age_days > max_age_days:
        return HealthFinding(
            code,
            FAIL,
            f"Budgets last reset {age_days} days ago — monday_maintenance is not running. "
            f"All LLM work is silently starved until it does.",
        )
    return _ok(code, f"Budgets reset {age_days} day(s) ago.")


def check_news_pipeline_effectiveness(db, *, days: int = 3) -> HealthFinding:
    """
    The failure that ran for weeks: 99 tickers checked, 76 with material news,
    0 Haiku calls, 0 Sonnet calls, 0 tier changes — reported as success.
    News events logged with haiku_material NULL mean the LLM never ran.
    """
    code = "news_pipeline"
    events = db.get_recent_news_events(days_back=days)
    if not events:
        return _ok(code, f"No news events in the last {days} day(s).")

    evaluated = [e for e in events if e.get("haiku_material") is not None]
    if not evaluated:
        return HealthFinding(
            code,
            WARN,
            f"{len(events)} news event(s) logged in {days} day(s) but none were evaluated by Haiku — "
            f"the news pipeline is running but doing no analysis (budget exhausted, or the call is failing).",
        )
    return _ok(code, f"{len(evaluated)}/{len(events)} news event(s) evaluated.")


def check_degenerate_analyses(db, *, min_sample: int = 5, threshold: float = 0.8) -> HealthFinding:
    """
    Parser drift detector.

    Every rating extractor falls back to the worst value on its scale, so a
    response-format change makes every stock look worthless rather than raising.
    parse_analysis now rejects structurally-broken responses outright, but a
    subtler drift (headers intact, field labels changed) would still slip
    through as a wave of NONE/LOW. A high share of maximally-pessimistic
    analyses is far more likely a parser fault than a real market.
    """
    code = "degenerate_analyses"
    analyses = db.get_all_latest_deep_analyses()
    if len(analyses) < min_sample:
        return _ok(code, "Not enough analyses to judge.")

    def _degenerate(a: dict) -> bool:
        return str(a.get("moat_rating", "")).lower() == "none" and str(a.get("conviction", "")).upper() == "LOW"

    bad = [a for a in analyses if _degenerate(a)]
    ratio = len(bad) / len(analyses)
    if ratio >= threshold:
        return HealthFinding(
            code,
            FAIL,
            f"{len(bad)}/{len(analyses)} ({ratio:.0%}) of stored analyses are maximally pessimistic "
            f"(NONE moat + LOW conviction). That is the signature of a parser failure, not a market.",
        )
    return _ok(code, f"{ratio:.0%} of analyses are maximally pessimistic.")


def check_scheduled_jobs(db, *, max_age_days: int = 9) -> HealthFinding:
    """
    The weekly LLM jobs write run_log rows. Their absence is the only evidence
    that the scheduler stopped doing paid work — nothing else would notice.
    """
    code = "scheduled_jobs"
    runs = db.get_run_history(limit=50)
    if not runs:
        return HealthFinding(code, WARN, "run_log is empty — no scheduled job has ever completed.")

    latest: dict[str, datetime] = {}
    for r in runs:
        ts = _parse_ts(r.get("started_at"))
        rt = r.get("run_type")
        if ts and rt and (rt not in latest or ts > latest[rt]):
            latest[rt] = ts

    stale = []
    for job in ("wednesday_haiku", "friday_sonnet"):
        seen = latest.get(job)
        if seen is None:
            stale.append(f"{job} (never)")
        elif (datetime.now() - seen).days > max_age_days:
            stale.append(f"{job} ({(datetime.now() - seen).days}d ago)")

    if stale:
        return HealthFinding(code, WARN, f"Weekly job(s) overdue: {', '.join(stale)}")
    return _ok(code, "Weekly jobs running on schedule.")


def check_data_freshness(db, *, max_age_days: int = 10) -> HealthFinding:
    """
    Monday's refresh writes fundamentals and syncs the Alpaca position mirror.
    Stale data here means monday_maintenance is failing partway through.

    This check exists because check_scheduled_jobs could not see it:
    monday_maintenance writes nothing to run_log, so the only evidence it ever
    ran is the data it leaves behind. A NaN score aborted its fundamentals step
    from 2026-07-06, and because one try/except wrapped all five steps, the
    position mirror and price alerts went stale with it — for three weeks,
    while every other job reported success and the budget reset (step 1, which
    runs before the failure) kept looking healthy.
    """
    code = "data_freshness"
    stale = []

    fundamentals_date = db.latest_fundamentals_date()
    if fundamentals_date is None:
        # No universe means the bot hasn't been seeded yet — nothing to refresh,
        # so absence is expected rather than a fault.
        if db.get_universe():
            stale.append("fundamentals (never written)")
    else:
        ts = _parse_ts(fundamentals_date)
        if ts and (datetime.now() - ts).days > max_age_days:
            stale.append(f"fundamentals ({(datetime.now() - ts).days}d old)")

    positions = db.get_paper_positions()
    if positions:
        synced = [_parse_ts(p.get("last_synced")) for p in positions]
        newest = max((s for s in synced if s), default=None)
        if newest is None:
            stale.append("position mirror (never synced)")
        elif (datetime.now() - newest).days > max_age_days:
            stale.append(f"position mirror ({(datetime.now() - newest).days}d old)")

    if stale:
        return HealthFinding(
            code,
            FAIL,
            f"Monday maintenance is not completing — stale: {', '.join(stale)}. "
            f"Downstream screening and the Sonnet queue are running on old data.",
        )
    return _ok(code, "Fundamentals and position mirror are current.")


def check_budget_exhaustion(db) -> HealthFinding:
    """
    A cap pinned at 100% isn't fatal, but sustained exhaustion means the pacing
    or the ceiling is wrong and coverage is being silently truncated.
    """
    code = "budget_exhaustion"
    exhausted = []
    for cap_type, _ in _BUDGET_CAPS():
        status = db.get_budget_status(cap_type)
        if status and status.get("max_calls") and status["calls_used"] >= status["max_calls"]:
            exhausted.append(f"{cap_type} ({status['calls_used']}/{status['max_calls']})")
    if exhausted:
        return HealthFinding(code, WARN, f"Budget cap(s) fully consumed: {', '.join(exhausted)}")
    return _ok(code, "No budget caps exhausted.")


def _BUDGET_CAPS():
    from .database import BUDGET_CAPS_DEFAULTS

    return BUDGET_CAPS_DEFAULTS


def _parse_ts(value) -> Optional[datetime]:
    """Parse the several timestamp shapes SQLite hands back."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[: len(fmt) + 6], fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


# Registry. Order is display order in the digest.
ALL_CHECKS: tuple[Callable, ...] = (
    check_snapshot_freshness,
    check_frozen_position_marks,
    check_quarantined_positions,
    check_trade_alpha_coverage,
    check_budget_reset,
    check_news_pipeline_effectiveness,
    check_degenerate_analyses,
    check_scheduled_jobs,
    check_data_freshness,
    check_budget_exhaustion,
)


def run_health_checks(db, checks: Optional[tuple[Callable, ...]] = None) -> list[HealthFinding]:
    """
    Run every invariant and return one finding each.

    A check that raises becomes a FAIL rather than taking the digest down with
    it — the whole point is that this reports rather than breaks.
    """
    findings = []
    for check in checks or ALL_CHECKS:
        try:
            findings.append(check(db))
        except Exception as e:
            logger.exception("Health check %s raised", getattr(check, "__name__", check))
            findings.append(
                HealthFinding(
                    code=getattr(check, "__name__", "unknown"),
                    severity=FAIL,
                    message=f"Health check itself failed: {type(e).__name__}: {e}",
                )
            )
    return findings


def format_digest(findings: list[HealthFinding]) -> str:
    """Human-readable digest, worst first."""
    icon = {OK: "ok  ", WARN: "WARN", FAIL: "FAIL"}
    ordered = sorted(findings, key=lambda f: -_RANK[f.severity])
    lines = ["BUFFETBOT HEALTH CHECK", ""]
    for f in ordered:
        lines.append(f"[{icon[f.severity]}] {f.code}")
        lines.append(f"        {f.message}")
        if f.detail:
            lines.append(f"        {f.detail}")
    alerting = [f for f in findings if f.is_alerting]
    lines.append("")
    lines.append(f"{len(alerting)} issue(s) of {len(findings)} checks.")
    return "\n".join(lines)


def alert_payload(findings: list[HealthFinding]) -> Optional[str]:
    """The digest, or None when everything is healthy (nothing to say)."""
    if not any(f.is_alerting for f in findings):
        return None
    return format_digest(findings)


def findings_fingerprint(findings: list[HealthFinding]) -> str:
    """
    Stable identity for the current set of problems.

    Used to throttle alerting: repeat the same digest weekly rather than daily,
    but speak up immediately when a *new* problem appears. Without this, a
    permanent condition trains you to ignore the channel — the same reasoning
    as the quarantine re-alert window.
    """
    alerting = sorted((f.code, f.severity) for f in findings if f.is_alerting)
    return json.dumps(alerting)
