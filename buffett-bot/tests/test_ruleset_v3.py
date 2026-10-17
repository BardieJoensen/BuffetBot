"""
Ruleset 3.0.0 tests.

Covers the rules introduced on 2026-10-18:
  1. a sell needs a pre-registered thesis breaker to fire (database side);
  2. deterministic valuation drives the target entry price;
  3. moat NONE / LOW conviction are never bought (defense in depth);
  4. monthly tier-cohort validation;
  5. ruleset version stamped on decisions, tier changes and validation runs;
  6. Danish lot sizing and the single-buy recommendation.

External dependencies are mocked or injected; no network.
"""

import sqlite3
import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

_anthropic_mock = MagicMock()
for _pkg in ("schedule", "dotenv", "anthropic", "anthropic.types", "anthropic.lib", "anthropic.lib.streaming"):
    sys.modules.setdefault(_pkg, _anthropic_mock)

from src.accounts.base import AccountState, PositionState
from src.backtest import compute_cohort_validation, run_tier_cohort_validation, set_backtest_dir
from src.database import Database
from src.deployment import DeployCandidate, is_quality_excluded, plan_buys, recommend_single_buy
from src.ruleset import RULESET_CHANGELOG, RULESET_VERSION, changelog_text
from src.tier_engine import assign_tier

# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def db(tmp_path):
    return Database(db_path=tmp_path / "test.db")


CFG = SimpleNamespace(
    max_positions=8,
    max_position_pct=0.20,
    min_trade_usd=250.0,
    quality_ceiling_pct=0.125,
    min_lot_dkk=5000.0,
    deploy_target_euphoria=0.65,
    deploy_target_overvalued=0.80,
    deploy_target_fair_value=0.90,
    deploy_target_correction=0.97,
    deploy_target_crisis=1.00,
)


def _state(*, equity=100_000.0, cash=50_000.0, buying_power=50_000.0, invested_value=50_000.0) -> AccountState:
    return AccountState(
        account_id="alpaca_paper",
        currency="USD",
        equity=equity,
        cash=cash,
        buying_power=buying_power,
        invested_value=invested_value,
        invested_pct=invested_value / equity,
        as_of=datetime.now(timezone.utc),
    )


def _position(symbol, market_value=10_000.0, tradable=True) -> PositionState:
    return PositionState(
        symbol=symbol,
        shares=100.0,
        avg_cost=100.0,
        price=market_value / 100.0,
        market_value=market_value,
        unrealized_pl=0.0,
        unrealized_pl_pct=0.0,
        tradable=tradable,
    )


# ─── 5. Ruleset version ───────────────────────────────────────────────────────


class TestRuleset:
    def test_version_is_the_last_changelog_entry(self):
        assert RULESET_CHANGELOG[-1][0] == RULESET_VERSION

    def test_changelog_text_mentions_every_version(self):
        text = changelog_text()
        for version, _, _ in RULESET_CHANGELOG:
            assert f"v{version}" in text

    def test_decisions_and_tier_changes_are_stamped(self, db):
        decision_id = db.log_decision("AAPL", "buy", notional=1000.0, order_status="submitting")
        db.log_tier_change("AAPL", new_tier="A", old_tier=None, trigger="scheduled")

        conn = sqlite3.connect(str(db.path))
        conn.row_factory = sqlite3.Row
        d = conn.execute("SELECT ruleset_version FROM decision_log WHERE id = ?", (decision_id,)).fetchone()
        t = conn.execute("SELECT ruleset_version FROM tier_history WHERE ticker = 'AAPL'").fetchone()
        conn.close()
        assert d["ruleset_version"] == RULESET_VERSION
        assert t["ruleset_version"] == RULESET_VERSION

    def test_migration_adds_column_to_an_old_database(self, tmp_path):
        """A pre-3.0 database has no ruleset_version column; opening it must add one."""
        path = tmp_path / "old.db"
        conn = sqlite3.connect(str(path))
        conn.execute(
            "CREATE TABLE tier_history (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT NOT NULL, "
            "changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, old_tier TEXT, new_tier TEXT NOT NULL, "
            "trigger TEXT, reason TEXT)"
        )
        conn.commit()
        conn.close()

        db = Database(db_path=path)
        db.log_tier_change("X", new_tier="B")
        assert db.get_tier_history("X")[0]["ruleset_version"] == RULESET_VERSION


# ─── 1. Thesis breakers in the database ───────────────────────────────────────


class TestThesisBreakerEvents:
    def _rated(self, db, ticker="CTSH", tier="A"):
        db.save_deep_analysis(
            ticker,
            tier=tier,
            conviction="MEDIUM",
            moat_rating="NARROW",
            fair_value=55.0,
            target_entry=50.0,
            investment_thesis="Cheap IT services compounder",
            key_risks=["AI deflation"],
            thesis_breakers=["Operating margin below 12% for two quarters", "Revenue declines for 3+ quarters"],
        )

    def test_fired_breaker_demotes_to_c_and_keeps_the_breakers(self, db):
        self._rated(db)
        event_id = db.mark_thesis_breaker_fired(
            "CTSH",
            breaker_index=1,
            breaker_text="Operating margin below 12% for two quarters",
            evidence="Q3 margin 11.2%, Q2 11.8%",
            headline="Cognizant margins slide again",
        )
        assert event_id > 0
        latest = db.get_latest_deep_analysis("CTSH")
        assert latest["tier"] == "C"
        assert latest["thesis_breakers"] == [
            "Operating margin below 12% for two quarters",
            "Revenue declines for 3+ quarters",
        ]
        assert latest["investment_thesis"].startswith("Thesis breaker fired: Operating margin")
        assert latest["fair_value"] == 55.0  # copied, not re-estimated
        history = db.get_tier_history("CTSH")
        assert history[0]["trigger"] == "thesis_breaker"
        assert history[0]["old_tier"] == "A" and history[0]["new_tier"] == "C"

    def test_get_fired_returns_latest_event_with_evidence(self, db):
        self._rated(db)
        db.mark_thesis_breaker_fired("CTSH", breaker_index=2, breaker_text="Revenue declines", evidence="-4% y/y x3")
        fired = db.get_fired_thesis_breaker("CTSH")
        assert fired["breaker_index"] == 2
        assert fired["evidence"] == "-4% y/y x3"
        assert fired["ruleset_version"] == RULESET_VERSION

    def test_since_filter_ignores_events_before_the_buy(self, db):
        self._rated(db)
        conn = sqlite3.connect(str(db.path))
        conn.execute(
            "INSERT INTO thesis_breaker_events (ticker, fired_at, breaker_text) VALUES ('CTSH', '2026-01-01 00:00:00', 'old')"
        )
        conn.commit()
        conn.close()
        assert db.get_fired_thesis_breaker("CTSH") is not None
        assert db.get_fired_thesis_breaker("CTSH", since="2026-06-01 00:00:00") is None
        assert db.get_fired_thesis_breaker("CTSH", since="2025-12-31 00:00:00")["breaker_text"] == "old"

    def test_unknown_ticker_has_no_event(self, db):
        assert db.get_fired_thesis_breaker("NOPE") is None

    def test_recent_events_listing(self, db):
        self._rated(db, "A1")
        self._rated(db, "B2")
        db.mark_thesis_breaker_fired("A1", breaker_index=1, breaker_text="x")
        db.mark_thesis_breaker_fired("B2", breaker_index=1, breaker_text="y")
        events = db.get_thesis_breaker_events(days_back=30)
        assert [e["ticker"] for e in events] == ["B2", "A1"]

    def test_already_c_does_not_write_another_analysis_row(self, db):
        self._rated(db, tier="C")
        db.mark_thesis_breaker_fired("CTSH", breaker_index=1, breaker_text="x")
        conn = sqlite3.connect(str(db.path))
        n = conn.execute("SELECT COUNT(*) FROM deep_analyses WHERE ticker='CTSH'").fetchone()[0]
        conn.close()
        assert n == 1


# ─── 2. Deterministic valuation drives the target ─────────────────────────────


class TestDeterministicTarget:
    def _analysis(self, moat, conviction, *, target, current):
        return SimpleNamespace(
            symbol="X",
            moat_rating=SimpleNamespace(value=moat),
            conviction_level=conviction,
            target_entry_price=target,
            current_price=current,
        )

    def test_external_fair_value_overrides_model_target(self, monkeypatch):
        import src.tier_engine as tier_engine

        monkeypatch.setattr(tier_engine, "config", SimpleNamespace(margin_of_safety_pct=0.25))
        # Model says "buy at 95" (price 90 would be A); the deterministic FV of
        # 100 says the target is 75, so at 90 the stock is only B.
        external = SimpleNamespace(current_price=90.0, average_fair_value=100.0)
        a = assign_tier(self._analysis("narrow", "MEDIUM", target=95.0, current=90.0), external_valuation=external)
        assert a.target_entry_price == pytest.approx(75.0)
        assert a.tier == "B"

    def test_model_target_is_only_a_fallback(self, monkeypatch):
        import src.tier_engine as tier_engine

        monkeypatch.setattr(tier_engine, "config", SimpleNamespace(margin_of_safety_pct=0.25))
        external = SimpleNamespace(current_price=90.0, average_fair_value=None)
        a = assign_tier(self._analysis("narrow", "MEDIUM", target=95.0, current=90.0), external_valuation=external)
        assert a.target_entry_price == 95.0
        assert a.tier == "A"


# ─── 3. Quality exclusion ─────────────────────────────────────────────────────


class TestQualityExclusion:
    @pytest.mark.parametrize(
        "moat,conviction,excluded",
        [
            ("NONE", "MEDIUM", True),
            ("none", "HIGH", True),
            ("WIDE", "LOW", True),
            ("NARROW", "low", True),
            ("NARROW", "MEDIUM", False),
            ("WIDE", "HIGH", False),
            (None, None, False),
        ],
    )
    def test_labels(self, moat, conviction, excluded):
        assert is_quality_excluded(moat, conviction) is excluded

    def test_plan_buys_skips_excluded_names_even_at_a_tier(self):
        candidates = [
            DeployCandidate(symbol="NOMOAT", margin_of_safety=0.40, tier="A", moat="NONE", conviction="MEDIUM"),
            DeployCandidate(symbol="LOWCONV", margin_of_safety=0.40, tier="A", moat="WIDE", conviction="LOW"),
            DeployCandidate(symbol="GOOD", margin_of_safety=0.10, tier="A", moat="NARROW", conviction="MEDIUM"),
        ]
        plan = plan_buys(_state(), candidates, "fair_value", current_position_count=0, cfg=CFG)
        assert [b.symbol for b in plan.buys] == ["GOOD"]
        assert sorted(plan.skipped_quality) == ["LOWCONV", "NOMOAT"]


# ─── 6. Single-buy recommendation ─────────────────────────────────────────────


class TestSingleBuyRecommendation:
    def _candidates(self):
        return [
            DeployCandidate(symbol="B1", margin_of_safety=0.30, tier="B"),
            DeployCandidate(symbol="A1", margin_of_safety=0.10, tier="A", moat="NARROW", conviction="MEDIUM"),
            DeployCandidate(symbol="A2", margin_of_safety=0.25, tier="A", moat="WIDE", conviction="MEDIUM"),
            DeployCandidate(symbol="S0", margin_of_safety=-0.30, tier="S"),  # above the ceiling
            DeployCandidate(symbol="NOMOAT", margin_of_safety=0.50, tier="A", moat="NONE"),
        ]

    def test_picks_the_same_ranking_as_plan_buys(self):
        rec = recommend_single_buy(self._candidates(), cfg=CFG, usd_dkk=6.9, cash_dkk=12_000.0)
        assert rec.symbol == "A2"
        assert rec.tier == "A"
        assert rec.lot_dkk == 5000.0
        assert rec.lot_usd == pytest.approx(5000.0 / 6.9)
        assert rec.affordable is True
        assert rec.slot_available is True
        assert "A2" in rec.reason and "5,000 DKK" in rec.reason

    def test_unaffordable_lot_is_said_so(self):
        rec = recommend_single_buy(self._candidates(), cfg=CFG, usd_dkk=6.9, cash_dkk=1_200.0)
        assert rec.symbol == "A2"
        assert rec.affordable is False
        assert "keep saving" in rec.reason

    def test_unknown_cash_and_rate(self):
        rec = recommend_single_buy(self._candidates(), cfg=CFG)
        assert rec.affordable is None and rec.lot_usd is None

    def test_held_names_and_full_portfolio(self):
        rec = recommend_single_buy(
            self._candidates(), held_symbols=frozenset({"A2"}), current_position_count=8, cfg=CFG
        )
        assert rec.symbol == "A1"
        assert rec.slot_available is False
        assert "portfolio full" in rec.reason

    def test_nothing_qualifies(self):
        rec = recommend_single_buy([DeployCandidate(symbol="B1", margin_of_safety=0.3, tier="B")], cfg=CFG)
        assert rec.symbol is None
        assert "hold cash" in rec.reason

    def test_persisted_and_read_back(self, db):
        db.save_single_buy_recommendation(
            ticker="A2",
            tier="A",
            margin_of_safety=0.25,
            lot_dkk=5000.0,
            lot_usd=724.6,
            usd_dkk=6.9,
            affordable=True,
            reason="A2: tier A",
        )
        rec = db.get_latest_single_buy_recommendation()
        assert rec["ticker"] == "A2" and rec["affordable"] == 1
        assert rec["ruleset_version"] == RULESET_VERSION


# ─── 4. Cohort validation ─────────────────────────────────────────────────────


def _series(start: str, days: int, daily: float, p0: float = 100.0) -> pd.Series:
    idx = pd.bdate_range(start, periods=days)
    return pd.Series([p0 * (1 + daily) ** i for i in range(days)], index=idx)


class TestCohortValidation:
    def test_compute_groups_by_tier_moat_and_conviction(self):
        ratings = [
            {
                "ticker": "UP",
                "rated_at": "2026-03-02T05:00:00",
                "tier": "A",
                "moat_rating": "WIDE",
                "conviction": "HIGH",
            },
            {
                "ticker": "DOWN",
                "rated_at": "2026-03-02T05:00:00",
                "tier": "C",
                "moat_rating": "NONE",
                "conviction": "LOW",
            },
            {
                "ticker": "LATE",
                "rated_at": "2026-12-01T05:00:00",
                "tier": "A",
                "moat_rating": "WIDE",
                "conviction": "HIGH",
            },
        ]
        prices = {
            "UP": _series("2026-03-02", 200, 0.002),
            "DOWN": _series("2026-03-02", 200, -0.001),
            "LATE": _series("2026-03-02", 200, 0.0),
        }
        bench = _series("2026-03-02", 200, 0.0005)
        result = compute_cohort_validation(ratings, prices, bench, as_of=pd.Timestamp("2026-10-01"))

        names = {n["ticker"]: n for n in result["names"]}
        assert "LATE" not in names  # rated after as_of
        assert names["UP"]["alpha_since_rating"] > 0 > names["DOWN"]["alpha_since_rating"]
        assert names["UP"]["alpha_90d"] is not None  # 90 days elapsed before as_of

        rows = {(r["cohort_type"], r["cohort"], r["horizon"]): r for r in result["rows"]}
        assert rows[("tier", "A", "since_rating")]["n"] == 1
        assert rows[("tier", "A", "since_rating")]["hit_rate"] == 1.0
        assert rows[("tier", "C", "since_rating")]["hit_rate"] == 0.0
        assert rows[("moat", "NONE", "90d")]["mean_alpha"] < 0
        assert rows[("conviction", "HIGH", "90d")]["mean_alpha"] > 0

    def test_ninety_day_horizon_requires_the_window_to_have_elapsed(self):
        ratings = [
            {"ticker": "NEW", "rated_at": "2026-09-20", "tier": "B", "moat_rating": "WIDE", "conviction": "MEDIUM"}
        ]
        prices = {"NEW": _series("2026-09-01", 60, 0.001)}
        bench = _series("2026-09-01", 60, 0.0)
        result = compute_cohort_validation(ratings, prices, bench, as_of=pd.Timestamp("2026-10-15"))
        assert result["names"][0]["alpha_since_rating"] is not None
        assert result["names"][0]["alpha_90d"] is None
        assert all(r["horizon"] != "90d" for r in result["rows"])

    def test_run_persists_rows_and_json(self, db, tmp_path):
        set_backtest_dir(tmp_path / "backtest")
        db.save_deep_analysis("UP", tier="A", moat_rating="WIDE", conviction="HIGH")
        db.save_deep_analysis("DOWN", tier="C", moat_rating="NONE", conviction="LOW")
        # A later re-rating must not become a second cohort entry
        db.save_deep_analysis("UP", tier="C", moat_rating="NONE", conviction="LOW")

        rated = db.get_first_ratings()
        assert {r["ticker"]: r["tier"] for r in rated} == {"UP": "A", "DOWN": "C"}
        start = rated[0]["rated_at"][:10]

        def loader(tickers, _start):
            return {
                "UP": _series(start, 5, 0.01),
                "DOWN": _series(start, 5, -0.01),
                "SPY": _series(start, 5, 0.0),
            }

        result = run_tier_cohort_validation(db, price_loader=loader)
        assert result["tickers_priced"] == 2
        rows = db.get_latest_cohort_validation()
        by_key = {(r["cohort_type"], r["cohort"]): r for r in rows}
        assert by_key[("tier", "A")]["mean_alpha"] > 0 > by_key[("tier", "C")]["mean_alpha"]
        assert all(r["ruleset_version"] == RULESET_VERSION for r in rows)
        assert list((tmp_path / "backtest").glob("cohort_validation_*.json"))

    def test_run_with_no_analyses_is_a_noop(self, db):
        result = run_tier_cohort_validation(db, price_loader=lambda t, s: {})
        assert result["rows"] == [] and db.get_latest_cohort_validation() == []


# ─── Briefing sections ────────────────────────────────────────────────────────


class TestBriefingSections:
    def test_sections_render_on_an_empty_db(self, db):
        from src.briefing.db_briefing import generate_briefing_from_db

        text, _ = generate_briefing_from_db(db)
        assert f"RULESET v{RULESET_VERSION}" in text
        assert "BEST SINGLE BUY" in text
        assert "THESIS BREAKERS FIRED" in text
        assert "COHORT VALIDATION" in text

    def test_sections_show_recorded_data(self, db):
        from src.briefing.db_briefing import generate_briefing_from_db

        db.save_deep_analysis("CTSH", tier="A", thesis_breakers=["margin < 12%"])
        db.mark_thesis_breaker_fired("CTSH", breaker_index=1, breaker_text="margin < 12%", evidence="Q3 11.2%")
        db.save_single_buy_recommendation(
            ticker="A2",
            tier="A",
            margin_of_safety=0.2,
            lot_dkk=5000,
            lot_usd=720,
            usd_dkk=6.9,
            affordable=True,
            reason="A2: tier A",
        )
        db.save_cohort_validation(
            "2026-10-16",
            [
                {
                    "cohort_type": "tier",
                    "cohort": "A",
                    "horizon": "since_rating",
                    "n": 8,
                    "mean_return": 0.25,
                    "benchmark_return": 0.05,
                    "mean_alpha": 0.206,
                    "median_alpha": 0.209,
                    "hit_rate": 0.875,
                }
            ],
        )
        text, _ = generate_briefing_from_db(db)
        assert "A2: tier A" in text
        assert "margin < 12%" in text and "Q3 11.2%" in text
        assert "+20.6pp" in text and "88%" in text
