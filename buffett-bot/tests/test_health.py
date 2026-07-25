"""
Tests for src/health.py.

Each check exists because a specific fault ran undetected in production. So
each is tested twice: it must fire when that fault is present, and stay quiet
when it isn't. A check that can't distinguish the two is worse than no check —
it either cries wolf or provides false assurance.

The "fires" cases reconstruct the real failures:
  - AL frozen at $65.00 across every snapshot
  - AD closed with no benchmark, so no alpha
  - budgets never reset because monday_maintenance stopped
  - news events logged with haiku_material NULL (0 LLM calls)
  - a wave of NONE/LOW analyses, the signature of parser drift
"""

import json
from datetime import datetime, timedelta

import pytest

from src.database import _open
from src.health import (
    ALL_CHECKS,
    FAIL,
    OK,
    WARN,
    HealthFinding,
    alert_payload,
    check_budget_exhaustion,
    check_budget_reset,
    check_degenerate_analyses,
    check_frozen_position_marks,
    check_news_pipeline_effectiveness,
    check_quarantined_positions,
    check_scheduled_jobs,
    check_snapshot_freshness,
    check_trade_alpha_coverage,
    findings_fingerprint,
    format_digest,
    run_health_checks,
)


def _snapshot(db, *, as_of, positions, equity=100_000.0):
    sid = db.save_snapshot("alpaca_paper", currency="USD", equity=equity, cash=30_000.0, positions=positions)
    with _open(db.path) as conn:
        conn.execute("UPDATE portfolio_snapshots SET as_of = ? WHERE id = ?", (as_of, sid))


def _pos(symbol, price):
    return {"symbol": symbol, "price": price, "market_value": price * 10, "shares": 10.0}


def _iso(days_ago=0, hours_ago=0):
    return (datetime.now() - timedelta(days=days_ago, hours=hours_ago)).strftime("%Y-%m-%d %H:%M:%S")


class TestSnapshotFreshness:
    def test_fires_when_snapshots_stop(self, db):
        _snapshot(db, as_of=_iso(days_ago=3), positions=[_pos("AAPL", 100.0)])
        assert check_snapshot_freshness(db).severity == FAIL

    def test_quiet_when_recent(self, db):
        _snapshot(db, as_of=_iso(hours_ago=2), positions=[_pos("AAPL", 100.0)])
        assert check_snapshot_freshness(db).severity == OK

    def test_warns_when_none_exist(self, db):
        assert check_snapshot_freshness(db).severity == WARN

    def test_reads_the_newest_not_the_oldest(self, db):
        """
        get_snapshots orders ASC, so a naive limit returns the OLDEST row and
        a healthy account looks stale forever. This caught exactly that.
        """
        _snapshot(db, as_of=_iso(days_ago=30), positions=[_pos("AAPL", 100.0)])
        _snapshot(db, as_of=_iso(hours_ago=1), positions=[_pos("AAPL", 101.0)])
        assert check_snapshot_freshness(db).severity == OK


class TestFrozenPositionMarks:
    """The check that would have caught AL in February rather than July."""

    def _history(self, db, al_prices):
        for i, p in enumerate(al_prices):
            _snapshot(
                db,
                as_of=_iso(days_ago=len(al_prices) - i),
                positions=[_pos("AGM", 200.0 + i), _pos("AL", p)],
            )

    def test_fires_on_a_frozen_mark(self, db):
        self._history(db, [65.0] * 5)
        finding = check_frozen_position_marks(db)
        assert finding.severity == WARN
        assert "AL" in finding.message

    def test_quiet_when_prices_move(self, db):
        self._history(db, [65.0, 65.4, 64.8, 66.1, 65.9])
        assert check_frozen_position_marks(db).severity == OK

    def test_does_not_double_report_a_known_quarantine(self, db):
        """Already quarantined and alerted — don't say it twice."""
        self._history(db, [65.0] * 5)
        db.record_quarantine("alpaca_paper", "AL")
        assert check_frozen_position_marks(db).severity == OK

    def test_ignores_a_newly_opened_position(self, db):
        """One observation isn't evidence of a frozen mark."""
        for i in range(5):
            positions = [_pos("AGM", 200.0 + i)]
            if i == 4:
                positions.append(_pos("NEW", 50.0))
            _snapshot(db, as_of=_iso(days_ago=5 - i), positions=positions)
        assert check_frozen_position_marks(db).severity == OK

    def test_quiet_without_enough_history(self, db):
        self._history(db, [65.0, 65.0])
        assert check_frozen_position_marks(db).severity == OK


class TestTradeAlphaCoverage:
    def _trade(self, db, ticker, *, benchmark):
        sell_id = db.log_decision(
            ticker,
            "sell",
            tier="B",
            price=95.0,
            shares=10.0,
            notional=950.0,
            order_id="s",
            reason="Take profit",
            regime="fair_value",
            reasoning_snapshot={},
        )
        db.log_decision(
            ticker,
            "buy",
            tier="B",
            price=100.0,
            shares=10.0,
            notional=1000.0,
            order_id="b",
            reason="entry",
            regime="fair_value",
            reasoning_snapshot={},
        )
        db.close_trade(
            ticker,
            exit_decision_id=sell_id,
            entry_price=100.0,
            exit_price=95.0,
            shares=10.0,
            benchmark_return=benchmark,
        )

    def test_fires_when_a_trade_has_no_alpha(self, db):
        self._trade(db, "AD", benchmark=None)
        finding = check_trade_alpha_coverage(db)
        assert finding.severity == WARN
        assert "AD" in finding.message
        assert "backfill_trade_alpha" in finding.message  # actionable

    def test_quiet_when_all_covered(self, db):
        self._trade(db, "GIS", benchmark=0.0137)
        assert check_trade_alpha_coverage(db).severity == OK

    def test_quiet_with_no_trades(self, db):
        assert check_trade_alpha_coverage(db).severity == OK


class TestBudgetReset:
    def test_fires_when_monday_job_stopped(self, db):
        with _open(db.path) as conn:
            conn.execute("UPDATE budget_caps SET last_reset = datetime('now', '-20 days')")
        assert check_budget_reset(db).severity == FAIL

    def test_warns_when_never_reset(self, db):
        assert check_budget_reset(db).severity == WARN

    def test_quiet_after_a_recent_reset(self, db):
        db.reset_weekly_budgets()
        assert check_budget_reset(db).severity == OK


class TestNewsPipelineEffectiveness:
    def test_fires_when_events_logged_but_never_evaluated(self, db):
        """99 tickers checked, 0 Haiku calls — the real failure."""
        for i in range(5):
            db.log_news_event(f"T{i}", f"headline {i}", haiku_material=None)
        finding = check_news_pipeline_effectiveness(db)
        assert finding.severity == WARN
        assert "no analysis" in finding.message.lower() or "none were evaluated" in finding.message.lower()

    def test_quiet_when_events_are_evaluated(self, db):
        db.log_news_event("AAPL", "headline", haiku_material=True)
        assert check_news_pipeline_effectiveness(db).severity == OK

    def test_quiet_with_no_news(self, db):
        assert check_news_pipeline_effectiveness(db).severity == OK


class TestDegenerateAnalyses:
    def test_fires_on_a_wave_of_pessimism(self, db):
        """The parser-drift signature: everything NONE moat + LOW conviction."""
        for i in range(6):
            db.save_deep_analysis(f"T{i}", tier="C", moat_rating="none", conviction="LOW")
        assert check_degenerate_analyses(db).severity == FAIL

    def test_quiet_on_a_healthy_mix(self, db):
        for i, (moat, conv) in enumerate(
            [("strong", "HIGH"), ("moderate", "MEDIUM"), ("none", "LOW"), ("strong", "HIGH"), ("weak", "MEDIUM")]
        ):
            db.save_deep_analysis(f"T{i}", tier="B", moat_rating=moat, conviction=conv)
        assert check_degenerate_analyses(db).severity == OK

    def test_quiet_below_the_sample_floor(self, db):
        db.save_deep_analysis("T0", tier="C", moat_rating="none", conviction="LOW")
        assert check_degenerate_analyses(db).severity == OK


class TestScheduledJobs:
    def test_fires_when_weekly_jobs_are_overdue(self, db):
        run_id = db.start_run("wednesday_haiku")
        db.complete_run(run_id, stocks_screened=1)
        with _open(db.path) as conn:
            conn.execute("UPDATE run_log SET started_at = datetime('now', '-30 days')")
        assert check_scheduled_jobs(db).severity == WARN

    def test_warns_on_an_empty_run_log(self, db):
        assert check_scheduled_jobs(db).severity == WARN

    def test_quiet_when_both_ran_recently(self, db):
        for job in ("wednesday_haiku", "friday_sonnet"):
            db.complete_run(db.start_run(job), stocks_screened=1)
        assert check_scheduled_jobs(db).severity == OK


class TestBudgetExhaustion:
    def test_fires_when_a_cap_is_spent(self, db):
        cap = db.get_budget_status("weekly_news_haiku")["max_calls"]
        db.spend_batch("weekly_news_haiku", cap)
        assert check_budget_exhaustion(db).severity == WARN

    def test_quiet_with_budget_left(self, db):
        assert check_budget_exhaustion(db).severity == OK


class TestQuarantineCheck:
    def test_fires_on_an_open_quarantine(self, db):
        db.record_quarantine("alpaca_paper", "AL", market_value=7242.03)
        finding = check_quarantined_positions(db)
        assert finding.severity == WARN
        assert "AL" in finding.message

    def test_quiet_once_resolved(self, db):
        db.record_quarantine("alpaca_paper", "AL")
        db.resolve_quarantines("alpaca_paper", ["AGM"])
        assert check_quarantined_positions(db).severity == OK


class TestRunner:
    def test_returns_one_finding_per_check(self, db):
        assert len(run_health_checks(db)) == len(ALL_CHECKS)

    def test_a_raising_check_becomes_a_finding_not_a_crash(self, db):
        def exploding(_db):
            raise RuntimeError("boom")

        findings = run_health_checks(db, checks=(exploding,))
        assert len(findings) == 1
        assert findings[0].severity == FAIL
        assert "boom" in findings[0].message

    def test_one_bad_check_does_not_hide_the_others(self, db):
        def exploding(_db):
            raise RuntimeError("boom")

        findings = run_health_checks(db, checks=(exploding, check_budget_exhaustion))
        assert len(findings) == 2


class TestAlertPayload:
    def test_none_when_everything_is_ok(self):
        findings = [HealthFinding("a", OK, "fine"), HealthFinding("b", OK, "fine")]
        assert alert_payload(findings) is None

    def test_digest_when_something_is_wrong(self):
        findings = [HealthFinding("a", OK, "fine"), HealthFinding("b", WARN, "trouble")]
        payload = alert_payload(findings)
        assert payload is not None
        assert "trouble" in payload

    def test_digest_puts_worst_first(self):
        findings = [HealthFinding("a", OK, "fine"), HealthFinding("b", WARN, "warn"), HealthFinding("c", FAIL, "fail")]
        digest = format_digest(findings)
        assert digest.index("fail") < digest.index("warn") < digest.index("fine")

    def test_healthy_checks_still_appear_in_the_digest(self):
        """Seeing what passed is how you know the checks ran at all."""
        findings = [HealthFinding("a", OK, "all good"), HealthFinding("b", WARN, "trouble")]
        assert "all good" in format_digest(findings)


class TestFingerprint:
    def test_stable_across_identical_problems(self):
        a = [HealthFinding("x", WARN, "one"), HealthFinding("y", OK, "fine")]
        b = [HealthFinding("x", WARN, "different wording"), HealthFinding("y", OK, "fine")]
        assert findings_fingerprint(a) == findings_fingerprint(b)

    def test_changes_when_a_new_problem_appears(self):
        a = [HealthFinding("x", WARN, "one")]
        b = [HealthFinding("x", WARN, "one"), HealthFinding("z", FAIL, "new")]
        assert findings_fingerprint(a) != findings_fingerprint(b)

    def test_changes_when_severity_escalates(self):
        a = [HealthFinding("x", WARN, "one")]
        b = [HealthFinding("x", FAIL, "one")]
        assert findings_fingerprint(a) != findings_fingerprint(b)

    def test_ignores_healthy_checks(self):
        a = [HealthFinding("x", WARN, "one")]
        b = [HealthFinding("x", WARN, "one"), HealthFinding("ok1", OK, "fine")]
        assert findings_fingerprint(a) == findings_fingerprint(b)

    def test_is_json_serialisable(self):
        json.loads(findings_fingerprint([HealthFinding("x", WARN, "one")]))


class TestAlertThrottle:
    def test_first_sighting_alerts(self, db):
        assert db.should_alert("health", "fp1") is True

    def test_same_problem_within_window_is_quiet(self, db):
        db.mark_alerted("health", "fp1")
        assert db.should_alert("health", "fp1", realert_days=7) is False

    def test_new_problem_alerts_immediately(self, db):
        db.mark_alerted("health", "fp1")
        assert db.should_alert("health", "fp2", realert_days=7) is True

    def test_same_problem_alerts_again_after_the_window(self, db):
        db.mark_alerted("health", "fp1")
        with _open(db.path) as conn:
            conn.execute("UPDATE alert_state SET last_alerted = datetime('now', '-30 days')")
        assert db.should_alert("health", "fp1", realert_days=7) is True

    def test_unsent_alert_stays_due(self, db):
        """should_alert must not itself record the send."""
        assert db.should_alert("health", "fp1") is True
        assert db.should_alert("health", "fp1") is True

    def test_streams_are_independent(self, db):
        db.mark_alerted("health", "fp1")
        assert db.should_alert("other", "fp1") is True


@pytest.fixture
def db(tmp_path):
    from src.database import Database

    return Database(db_path=tmp_path / "test.db")
