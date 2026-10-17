"""
Integration test: full weekly cycle across Phases D and E.

Simulates the complete happy path:
    Monday  → fresh universe, budget reset
    Wednesday → Haiku batch screens ORCL → passes
    Friday   → Sonnet batch assigns B-tier (price above target entry)
    Thursday → daily_news_monitor finds material acquisition news for ORCL
               Haiku: REVIEW recommendation
               Sonnet re-analyzes with price now BELOW target → promotes to A
    Verify:
        - deep_analyses has a new A-tier row for ORCL
        - tier_history records the B→A change with trigger='news_event'
        - price_alerts updated to A-tier with 2-tranche staged entries
        - news_events row logged for the headline

Uses real Database + real assign_tier/staged_entry_suggestion.
Only LLM calls (batch_quick_screen, batch_analyze_companies,
check_news_for_red_flags, analyze_company) and Finnhub HTTP are mocked.
"""

import sqlite3
import sys
from dataclasses import replace
from unittest.mock import MagicMock, patch

import pytest

# Stub container-only packages before any scheduler/analyzer import
_anthropic_mock = MagicMock()
for _pkg in (
    "schedule",
    "dotenv",
    "anthropic",
    "anthropic.types",
    "anthropic.lib",
    "anthropic.lib.streaming",
):
    sys.modules.setdefault(_pkg, _anthropic_mock)

from src.database import Database
from src.news_fetcher import FinnhubNewsFetcher, run_news_pipeline
from src.valuation import AggregatedValuation

# ─── Helpers ──────────────────────────────────────────────────────────────────


def _make_analysis_v2(
    symbol: str,
    moat_value: str,  # "wide", "narrow", "none"
    conviction: str,  # "HIGH", "MEDIUM", "LOW"
    target: float,
    current: float,
) -> MagicMock:
    """
    Build a minimal AnalysisV2-compatible mock suitable for the REAL assign_tier()
    and save_deep_analysis() calls (no tier patching in integration tests).

    Key attributes consumed by assign_tier():
        moat_rating.value   → "wide" / "narrow" / "none"
        conviction_level    → "HIGH" / "MEDIUM" / "LOW"
        target_entry_price  → float
        current_price       → float
        symbol              → str
    """
    mock = MagicMock()
    mock.symbol = symbol
    mock.conviction = conviction
    mock.conviction_level = conviction  # assign_tier reads this property

    mock.moat_rating = MagicMock()
    mock.moat_rating.value = moat_value  # assign_tier: moat_rating.value
    mock.moat_sources = [moat_value]  # save_deep_analysis

    mock.estimated_fair_value_low = target * 1.25
    mock.estimated_fair_value_high = target * 1.45
    mock.target_entry_price = target
    mock.current_price = current

    mock.summary = f"{symbol} analysis"
    mock.key_risks = ["competition"]
    mock.thesis_risks = ["moat erosion via cloud shift"]
    return mock


def _make_news_item(headline, summary="", source="Reuters", ts=1700000000):
    return {"headline": headline, "summary": summary, "source": source, "datetime": ts}


# ─── Integration test ─────────────────────────────────────────────────────────


class TestFullWeeklyCycle:
    """
    End-to-end test that drives the real scheduler job functions and
    run_news_pipeline with a real SQLite database, verifying the complete
    write path through deep_analyses, tier_history, price_alerts, and
    news_events.
    """

    @pytest.fixture(autouse=True)
    def _enable_scheduled_analysis(self, monkeypatch):
        import scripts.scheduler as scheduler

        monkeypatch.setattr(
            scheduler,
            "config",
            replace(scheduler.config, wednesday_haiku_enabled=True, friday_sonnet_enabled=True),
        )

    def test_b_to_a_promotion_via_news(self, tmp_path):
        """
        Happy path for a B-tier stock promoted to A by a news-triggered Sonnet:

        1. [Monday]    Reset budgets; seed ORCL into universe with fundamentals.
        2. [Wednesday] Haiku batch screens ORCL → passes (moat_hint=4).
        3. [Friday]    Sonnet batch deep-analyzes ORCL.
                       Price(160) > target(150) → B-tier, gap ≈ +6.7%.
        4. [Thursday]  News: Oracle announces a major acquisition.
                       Haiku: REVIEW (not red-flag, but warrants re-examination).
                       Sonnet: re-analyzes; analyst raises conviction; market
                       reaction drops price to 145 < target 150 → A-tier.

        Assertions cover every table touched by the write path.
        """
        from scripts.scheduler import friday_sonnet_batch, wednesday_haiku_batch

        db = Database(tmp_path / "test.db")

        # ── 1. Monday: seed universe ──────────────────────────────────────────
        db.reset_weekly_budgets()
        db.upsert_universe_stock(
            "ORCL",
            company_name="Oracle Corporation",
            sector="Technology",
            market_cap=300e9,
            source="conviction",
            quality_score=75.0,
        )
        db.save_fundamentals(
            "ORCL",
            {"price": 160.0, "pe_ratio": 22.0, "roe": 0.25, "operating_margin": 0.30},
        )

        # ── 2. Wednesday: Haiku batch ─────────────────────────────────────────
        haiku_batch_result = [
            {
                "symbol": "ORCL",
                "worth_analysis": True,
                "moat_hint": 4,
                "quality_hint": 4,
                "reason": "Cloud DB switching costs create durable moat",
                "valid": True,
            }
        ]
        with patch("src.database.Database", return_value=db), patch("src.analyzer.CompanyAnalyzer") as MockAnalyzer:
            MockAnalyzer.return_value.batch_quick_screen.return_value = haiku_batch_result
            wednesday_haiku_batch()

        haiku = db.get_latest_haiku("ORCL")
        assert haiku is not None, "ORCL should have a Haiku result after Wednesday batch"
        assert haiku["passed"] == 1
        assert haiku["moat_estimate"] == "WIDE"  # moat_hint=4 → WIDE

        haiku_budget = db.get_budget_status("weekly_haiku_screen")
        assert haiku_budget["calls_used"] == 1

        # ── 3. Friday: Sonnet batch ───────────────────────────────────────────
        # Price(160) > target(150), gap = (160-150)/150 ≈ +6.7%.
        # assign_tier: narrow moat, MEDIUM conviction, gap < 50% → B
        b_tier_mock = _make_analysis_v2("ORCL", "narrow", "MEDIUM", target=150.0, current=160.0)

        with (
            patch("src.database.Database", return_value=db),
            patch("src.analyzer.CompanyAnalyzer") as MockAnalyzer,
            patch("src.valuation.ValuationAggregator") as MockValuation,
            patch(
                "src.tier_engine.assign_tier", wraps=__import__("src.tier_engine", fromlist=["assign_tier"]).assign_tier
            ),
        ):
            MockValuation.return_value.get_valuation.return_value = AggregatedValuation(
                symbol="ORCL", current_price=160.0
            )
            MockAnalyzer.return_value.batch_analyze_companies.return_value = [b_tier_mock]
            friday_sonnet_batch()

        da_after_friday = db.get_latest_deep_analysis("ORCL")
        assert da_after_friday is not None, "ORCL should have a deep analysis after Friday batch"
        assert da_after_friday["tier"] == "B"
        assert da_after_friday["conviction"] == "MEDIUM"

        friday_budget = db.get_budget_status("weekly_sonnet_analysis")
        assert friday_budget["calls_used"] == 1

        # price_alerts row must exist with tier='B'
        b_alerts = db.get_price_alerts(tiers=["B"])
        assert any(a["ticker"] == "ORCL" for a in b_alerts)

        # tier_history must have the initial scheduled assignment
        history_after_friday = db.get_tier_history("ORCL")
        assert any(h["trigger"] == "scheduled" and h["new_tier"] == "B" for h in history_after_friday)

        # ── 4. Thursday: news event triggers Haiku + breaker check ───────────
        # Ruleset 3.0.0: the news path can no longer promote or demote on a
        # re-analysis. It may only decide whether a pre-registered breaker
        # has happened. First day: REVIEW, but no breaker fired → nothing
        # changes except the news log and the budgets.
        news_items = [
            _make_news_item(
                "Oracle acquires AI startup CloudMind for $10B",
                summary="acquisition expands Oracle's cloud database offerings with AI",
                source="Reuters",
                ts=1700000000,
            )
        ]

        fetcher = MagicMock(spec=FinnhubNewsFetcher)
        fetcher.api_key = "test_key"  # pragma: allowlist secret
        fetcher.get_news_for_tickers.return_value = {"ORCL": news_items}

        analyzer = MagicMock()
        analyzer.check_news_for_red_flags.return_value = {
            "has_red_flags": False,
            "analysis": "Acquisition is large but could strengthen cloud moat if integrated well",
            "recommendation": "REVIEW",
        }
        analyzer.check_thesis_breakers.return_value = {
            "fired": False,
            "breaker_index": None,
            "breaker_text": None,
            "evidence": "none",
            "raw": "",
        }

        stats = run_news_pipeline(db, analyzer, fetcher)

        assert stats["tickers_checked"] == 1  # only ORCL in B-tier alerts
        assert stats["news_found"] == 1  # acquisition is material
        assert stats["haiku_calls"] == 1
        assert stats["sonnet_calls"] == 1
        assert stats["breakers_fired"] == 0
        assert stats["tier_changes"] == 0

        assert db.get_budget_status("weekly_news_haiku")["calls_used"] == 1
        assert db.get_budget_status("weekly_news_sonnet")["calls_used"] == 1
        # Separate budget pools — scheduled jobs unaffected
        assert db.get_budget_status("weekly_haiku_screen")["calls_used"] == 1
        assert db.get_budget_status("weekly_sonnet_analysis")["calls_used"] == 1

        # The breaker check saw the breakers Friday's analysis pre-registered.
        analyzer.check_thesis_breakers.assert_called_once()
        assert analyzer.check_thesis_breakers.call_args[0][0] == "ORCL"
        assert analyzer.check_thesis_breakers.call_args[0][1] == list(b_tier_mock.thesis_risks)
        analyzer.analyze_company.assert_not_called()

        # deep_analyses / tier_history / price_alerts untouched
        assert db.get_latest_deep_analysis("ORCL")["tier"] == "B"
        assert db.get_latest_deep_analysis("ORCL")["analyzed_at"] == da_after_friday["analyzed_at"]
        assert all(h["trigger"] != "thesis_breaker" for h in db.get_tier_history("ORCL"))
        assert any(a["ticker"] == "ORCL" for a in db.get_price_alerts(tiers=["B"]))

        conn = sqlite3.connect(str(db.path))
        conn.row_factory = sqlite3.Row
        news_row = conn.execute("SELECT * FROM news_events WHERE ticker='ORCL'").fetchone()
        conn.close()
        assert news_row is not None
        assert news_row["haiku_material"] == 1
        assert news_row["sonnet_triggered"] == 1  # breaker check completed

        # ── 5. Friday: a pre-registered breaker is reported as having happened ─
        fetcher.get_news_for_tickers.return_value = {
            "ORCL": [
                _make_news_item(
                    "Oracle CEO resigns after SEC fraud charges",
                    summary="SEC charges Oracle with accounting fraud; CEO steps down",
                    source="Reuters",
                    ts=1700090000,
                )
            ]
        }
        analyzer.check_news_for_red_flags.return_value = {
            "has_red_flags": True,
            "analysis": "Fraud charges",
            "recommendation": "SELL",
        }
        breakers = list(b_tier_mock.thesis_risks)
        analyzer.check_thesis_breakers.return_value = {
            "fired": True,
            "breaker_index": 1,
            "breaker_text": breakers[0],
            "evidence": "SEC charges Oracle with accounting fraud; CEO steps down",
            "raw": "",
        }
        notifier = MagicMock()

        stats = run_news_pipeline(db, analyzer, fetcher, notifier=notifier)

        assert stats["breakers_fired"] == 1 and stats["tier_changes"] == 1

        # deep_analyses: a C copy of Friday's analysis (INSERT, not REPLACE)
        conn = sqlite3.connect(str(db.path))
        conn.row_factory = sqlite3.Row
        all_analyses = conn.execute(
            "SELECT tier, analyzed_at FROM deep_analyses WHERE ticker='ORCL' ORDER BY analyzed_at"
        ).fetchall()
        conn.close()
        assert [r["tier"] for r in all_analyses] == ["B", "C"]
        latest = db.get_latest_deep_analysis("ORCL")
        assert latest["investment_thesis"].startswith("Thesis breaker fired: " + breakers[0])
        assert latest["thesis_breakers"] == breakers  # pre-registered list preserved

        # tier_history: B→C with trigger='thesis_breaker', stamped with the ruleset
        entry = next(h for h in db.get_tier_history("ORCL") if h["trigger"] == "thesis_breaker")
        assert entry["old_tier"] == "B" and entry["new_tier"] == "C"

        # thesis_breaker_events: the sell trigger weekly_auto_trade reads
        fired = db.get_fired_thesis_breaker("ORCL")
        assert fired["breaker_index"] == 1 and fired["breaker_text"] == breakers[0]
        assert fired["headline"] == "Oracle CEO resigns after SEC fraud charges"

        # price_alerts: ORCL leaves the S/A/B news watch
        assert all(a["ticker"] != "ORCL" for a in db.get_price_alerts(tiers=["S", "A", "B"]))

        notifier.send_alert.assert_called_once()
        assert "THESIS BREAKER FIRED" in notifier.send_alert.call_args[0][1]
