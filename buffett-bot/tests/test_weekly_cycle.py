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
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ── Bootstrap path ────────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))
sys.path.insert(0, str(_PROJECT_ROOT / "scripts"))

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


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _make_analysis_v2(
    symbol: str,
    moat_value: str,       # "wide", "narrow", "none"
    conviction: str,       # "HIGH", "MEDIUM", "LOW"
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
    mock.conviction_level = conviction          # assign_tier reads this property

    mock.moat_rating = MagicMock()
    mock.moat_rating.value = moat_value        # assign_tier: moat_rating.value
    mock.moat_sources = [moat_value]           # save_deep_analysis

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
        from scheduler import friday_sonnet_batch, wednesday_haiku_batch

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
                "reason": "Cloud DB switching costs create durable moat",
            }
        ]
        with patch("src.database.Database", return_value=db), \
                patch("src.analyzer.CompanyAnalyzer") as MockAnalyzer:
            MockAnalyzer.return_value.batch_quick_screen.return_value = haiku_batch_result
            wednesday_haiku_batch()

        haiku = db.get_latest_haiku("ORCL")
        assert haiku is not None, "ORCL should have a Haiku result after Wednesday batch"
        assert haiku["passed"] == 1
        assert haiku["moat_estimate"] == "WIDE"   # moat_hint=4 → WIDE

        haiku_budget = db.get_budget_status("weekly_haiku_screen")
        assert haiku_budget["calls_used"] == 1

        # ── 3. Friday: Sonnet batch ───────────────────────────────────────────
        # Price(160) > target(150), gap = (160-150)/150 ≈ +6.7%.
        # assign_tier: narrow moat, MEDIUM conviction, gap < 50% → B
        b_tier_mock = _make_analysis_v2("ORCL", "narrow", "MEDIUM", target=150.0, current=160.0)

        with patch("src.database.Database", return_value=db), \
                patch("src.analyzer.CompanyAnalyzer") as MockAnalyzer, \
                patch("src.tier_engine.assign_tier", wraps=__import__("src.tier_engine", fromlist=["assign_tier"]).assign_tier):
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
        assert any(h["trigger"] == "scheduled" and h["new_tier"] == "B"
                   for h in history_after_friday)

        # ── 4. Thursday: news event triggers Haiku + Sonnet ──────────────────
        # Market reacts to news; for the re-analysis the analyst assumes the
        # price has dropped to 145, which is below the 150 target → A-tier.
        news_items = [
            _make_news_item(
                "Oracle acquires AI startup CloudMind for $10B",
                summary="acquisition expands Oracle's cloud database offerings with AI",
                source="Reuters",
                ts=1700000000,
            )
        ]

        fetcher = MagicMock(spec=FinnhubNewsFetcher)
        fetcher.api_key = "test_key"
        fetcher.get_news_for_tickers.return_value = {"ORCL": news_items}

        # Haiku: REVIEW — not a red flag, but warrants re-examination
        analyzer = MagicMock()
        analyzer.check_news_for_red_flags.return_value = {
            "has_red_flags": False,
            "analysis": "Acquisition is large but could strengthen cloud moat if integrated well",
            "recommendation": "REVIEW",
        }

        # Sonnet: price 145 < target 150 → gap = -3.3% → A-tier
        a_tier_mock = _make_analysis_v2("ORCL", "narrow", "MEDIUM", target=150.0, current=145.0)
        a_tier_mock.summary = "Oracle's acquisition strengthens AI positioning; moat intact"
        a_tier_mock.key_risks = ["integration risk", "deal premium"]
        a_tier_mock.thesis_risks = ["major cloud provider bundles competing DB free"]
        analyzer.analyze_company.return_value = a_tier_mock

        stats = run_news_pipeline(db, analyzer, fetcher)

        # ── Verify: pipeline stats ────────────────────────────────────────────
        assert stats["tickers_checked"] == 1   # only ORCL in B-tier alerts
        assert stats["news_found"] == 1        # acquisition is material
        assert stats["haiku_calls"] == 1
        assert stats["sonnet_calls"] == 1
        assert stats["tier_changes"] == 1      # B → A

        news_haiku_budget = db.get_budget_status("weekly_news_haiku")
        assert news_haiku_budget["calls_used"] == 1

        news_sonnet_budget = db.get_budget_status("weekly_news_sonnet")
        assert news_sonnet_budget["calls_used"] == 1

        # ── Verify: separate budget pools — scheduled jobs unaffected ─────────
        # Wednesday and Friday budgets must still reflect only the scheduled spend
        assert db.get_budget_status("weekly_haiku_screen")["calls_used"] == 1
        assert db.get_budget_status("weekly_sonnet_analysis")["calls_used"] == 1

        # ── Verify: deep_analyses — new A-tier row added (old B row preserved) ─
        da_latest = db.get_latest_deep_analysis("ORCL")
        assert da_latest["tier"] == "A"
        assert da_latest["conviction"] == "MEDIUM"
        assert "acquisition" in da_latest["investment_thesis"].lower() or \
               "moat" in da_latest["investment_thesis"].lower()

        # Both the B-tier and A-tier rows exist (INSERT, not REPLACE)
        conn = sqlite3.connect(str(db.path))
        conn.row_factory = sqlite3.Row
        all_analyses = conn.execute(
            "SELECT tier, analyzed_at FROM deep_analyses WHERE ticker='ORCL' ORDER BY analyzed_at"
        ).fetchall()
        conn.close()
        assert len(all_analyses) == 2
        assert all_analyses[0]["tier"] == "B"   # Friday's row preserved
        assert all_analyses[1]["tier"] == "A"   # Thursday's news-triggered row

        # ── Verify: tier_history — B→A logged with trigger='news_event' ───────
        history = db.get_tier_history("ORCL")
        news_entry = next(
            (h for h in history if h["trigger"] == "news_event"), None
        )
        assert news_entry is not None, "Expected a news_event entry in tier_history"
        assert news_entry["old_tier"] == "B"
        assert news_entry["new_tier"] == "A"
        assert "REVIEW" in news_entry["reason"]

        # ── Verify: price_alerts — updated to A-tier with 2-tranche entries ───
        a_alerts = db.get_price_alerts(tiers=["A"])
        orcl_alert = next((a for a in a_alerts if a["ticker"] == "ORCL"), None)
        assert orcl_alert is not None, "ORCL should be in A-tier price_alerts"
        assert orcl_alert["tier"] == "A"
        assert orcl_alert["target_entry"] == pytest.approx(150.0)

        staged = orcl_alert["staged_entries"]
        assert isinstance(staged, list), f"staged_entries should be a list, got {type(staged)}"
        assert len(staged) == 2, "A-tier: 2 tranches expected"
        assert staged[0]["tranche"] == 1
        assert staged[1]["tranche"] == 2
        assert staged[0]["price"] == pytest.approx(150.0)          # 1st tranche at target
        assert staged[1]["price"] == pytest.approx(150.0 * 0.95)   # 2nd tranche 5% below

        # ORCL must no longer appear in B-tier alerts
        b_alerts_after = db.get_price_alerts(tiers=["B"])
        assert all(a["ticker"] != "ORCL" for a in b_alerts_after)

        # ── Verify: news_events — headline logged ─────────────────────────────
        conn = sqlite3.connect(str(db.path))
        conn.row_factory = sqlite3.Row
        news_row = conn.execute(
            "SELECT * FROM news_events WHERE ticker='ORCL'"
        ).fetchone()
        conn.close()
        assert news_row is not None, "news_events should have a row for ORCL"
        assert "oracle" in news_row["headline"].lower() or "ai" in news_row["headline"].lower()
        assert news_row["haiku_material"] == 0   # no red flags (just REVIEW)
        assert news_row["sonnet_triggered"] is None   # field not set by pipeline

        # ── Verify: analyzer called with correct args ─────────────────────────
        analyzer.check_news_for_red_flags.assert_called_once()
        call_args = analyzer.check_news_for_red_flags.call_args[0]
        assert call_args[0] == "ORCL"                           # symbol
        assert "Oracle Corporation" in call_args[1] or \
               "ORCL" in call_args[1]                           # thesis includes context

        analyzer.analyze_company.assert_called_once()
        sonnet_call = analyzer.analyze_company.call_args
        assert sonnet_call.kwargs.get("use_cache") is False     # must bypass cache
        assert sonnet_call.kwargs.get("recent_news") is not None  # news context passed
