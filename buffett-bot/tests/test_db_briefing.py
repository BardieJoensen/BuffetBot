"""
Unit tests for Phase F: DB-driven briefing generator.

Uses real Database (tmp_path SQLite) and the real generate_briefing_from_db().
No LLM calls, no external APIs.
"""

import json
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from src.briefing.db_briefing import (
    _APPROACHING_GAP_PCT,
    generate_briefing_from_db,
)
from src.database import Database


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _seed_universe(db: Database, ticker: str, sector: str = "Technology", cap: str = "large", score: float = 70.0):
    db.upsert_universe_stock(
        ticker,
        company_name=f"{ticker} Corp",
        sector=sector,
        market_cap=50e9,
        cap_category=cap,
        source="conviction",
        quality_score=score,
    )


def _seed_alert(
    db: Database,
    ticker: str,
    tier: str,
    target: float,
    last_price: float,
    gap_pct: float,
    staged: list | None = None,
):
    db.upsert_price_alert(
        ticker,
        tier=tier,
        target_entry=target,
        staged_entries=staged,
        last_price=last_price,
        gap_pct=gap_pct,
    )


def _seed_deep_analysis(
    db: Database,
    ticker: str,
    tier: str,
    conviction: str = "MEDIUM",
    moat: str = "NARROW",
    thesis: str = "Strong thesis here",
    risks: list | None = None,
):
    db.save_deep_analysis(
        ticker,
        tier=tier,
        conviction=conviction,
        moat_rating=moat,
        moat_sources=["switching_costs"],
        fair_value=200.0,
        target_entry=150.0,
        investment_thesis=thesis,
        key_risks=risks or ["competition", "regulation"],
        thesis_breakers=["moat erosion"],
    )


def _seed_paper_position(db: Database, ticker: str, tier: str = "A"):
    db.upsert_paper_position(
        ticker,
        tier_at_entry=tier,
        entry_stage="1/2",
        entry_price=145.0,
        entry_date="2026-01-15",
        shares=10.0,
        cost_basis=1450.0,
        current_price=155.0,
        current_value=1550.0,
        gain_loss_pct=0.069,
    )


# ─── Tests ────────────────────────────────────────────────────────────────────


class TestEmptyDB:
    def test_empty_db_generates_without_error(self, tmp_path):
        db = Database(tmp_path / "test.db")
        text, html = generate_briefing_from_db(db)
        assert "BUFFETT BOT BRIEFING" in text
        assert "<title>Buffett Bot Briefing" in html

    def test_empty_db_shows_no_alerts(self, tmp_path):
        db = Database(tmp_path / "test.db")
        text, html = generate_briefing_from_db(db)
        # No tier sections should appear since no price_alerts rows
        assert "S-TIER SPOTLIGHT" not in text
        assert "A-TIER ACTION LIST" not in text

    def test_empty_db_coverage_shows_zero_universe(self, tmp_path):
        db = Database(tmp_path / "test.db")
        text, _ = generate_briefing_from_db(db)
        assert "Universe total:       0" in text


class TestBudgetSection:
    def test_budget_caps_shown(self, tmp_path):
        db = Database(tmp_path / "test.db")
        text, _ = generate_briefing_from_db(db)
        assert "weekly_haiku_screen" in text
        assert "weekly_sonnet_analysis" in text
        assert "weekly_news_haiku" in text
        assert "weekly_news_sonnet" in text

    def test_budget_reflects_actual_usage(self, tmp_path):
        db = Database(tmp_path / "test.db")
        db.reset_weekly_budgets()
        db.spend_batch("weekly_haiku_screen", 5)
        text, _ = generate_briefing_from_db(db)
        assert "5/50" in text


class TestSTierSpotlight:
    def test_s_tier_stock_appears_in_spotlight(self, tmp_path):
        db = Database(tmp_path / "test.db")
        _seed_universe(db, "COST")
        _seed_deep_analysis(db, "COST", "S", conviction="HIGH", moat="WIDE",
                            thesis="Membership model with extraordinary customer loyalty")
        _seed_alert(db, "COST", "S", target=580.0, last_price=570.0, gap_pct=-0.017,
                    staged=[{"tranche": 1, "price": 580.0, "label": "Tranche 1/3 at target"},
                            {"tranche": 2, "price": 551.0, "label": "Tranche 2/3 at -5%"},
                            {"tranche": 3, "price": 523.0, "label": "Tranche 3/3 at -10%"}])
        text, html = generate_briefing_from_db(db)
        assert "S-TIER SPOTLIGHT" in text
        assert "COST" in text
        assert "Membership model" in text
        assert "S-Tier Spotlight" in html
        assert "COST" in html

    def test_s_tier_shows_staged_entries(self, tmp_path):
        db = Database(tmp_path / "test.db")
        _seed_universe(db, "COST")
        _seed_deep_analysis(db, "COST", "S", conviction="HIGH", moat="WIDE")
        _seed_alert(db, "COST", "S", target=580.0, last_price=570.0, gap_pct=-0.017,
                    staged=[{"tranche": 1, "price": 580.0, "label": "Tranche 1 at target"},
                            {"tranche": 2, "price": 551.0, "label": "Tranche 2 at -5%"},
                            {"tranche": 3, "price": 523.0, "label": "Tranche 3 at -10%"}])
        text, _ = generate_briefing_from_db(db)
        assert "STAGED ENTRY PLAN" in text

    def test_s_tier_paper_position_shown(self, tmp_path):
        db = Database(tmp_path / "test.db")
        _seed_universe(db, "COST")
        _seed_deep_analysis(db, "COST", "S", conviction="HIGH", moat="WIDE")
        _seed_alert(db, "COST", "S", target=580.0, last_price=570.0, gap_pct=-0.017)
        _seed_paper_position(db, "COST", tier="S")
        text, html = generate_briefing_from_db(db)
        assert "PAPER POSITION" in text or "Paper position" in html

    def test_no_s_tier_section_when_none(self, tmp_path):
        db = Database(tmp_path / "test.db")
        _seed_universe(db, "ORCL")
        _seed_deep_analysis(db, "ORCL", "B")
        _seed_alert(db, "ORCL", "B", target=150.0, last_price=160.0, gap_pct=0.067)
        text, _ = generate_briefing_from_db(db)
        assert "S-TIER SPOTLIGHT" not in text


class TestATierActionList:
    def test_a_tier_appears_in_action_list(self, tmp_path):
        db = Database(tmp_path / "test.db")
        _seed_universe(db, "MSCI")
        _seed_deep_analysis(db, "MSCI", "A", conviction="MEDIUM", moat="NARROW",
                            thesis="Index licensing monopoly with recurring revenue")
        _seed_alert(db, "MSCI", "A", target=460.0, last_price=446.0, gap_pct=-0.030,
                    staged=[{"tranche": 1, "price": 460.0, "label": "Tranche 1/2"},
                            {"tranche": 2, "price": 437.0, "label": "Tranche 2/2"}])
        text, html = generate_briefing_from_db(db)
        assert "A-TIER ACTION LIST" in text
        assert "MSCI" in text
        assert "A-Tier Action List" in html

    def test_a_tier_shows_staged_entries(self, tmp_path):
        db = Database(tmp_path / "test.db")
        _seed_universe(db, "MSCI")
        _seed_deep_analysis(db, "MSCI", "A")
        _seed_alert(db, "MSCI", "A", target=460.0, last_price=446.0, gap_pct=-0.030,
                    staged=[{"tranche": 1, "price": 460.0, "label": "Tranche 1/2"},
                            {"tranche": 2, "price": 437.0, "label": "Tranche 2/2"}])
        text, _ = generate_briefing_from_db(db)
        # Entry plan shows in A-tier section
        assert "460.00" in text
        assert "437.00" in text


class TestBTierApproachingTarget:
    def test_b_tier_approaching_shown_sorted_by_gap(self, tmp_path):
        """B-tier stocks sorted by gap_pct ascending (closest to target first)."""
        db = Database(tmp_path / "test.db")
        for ticker, gap in [("ORCL", 0.035), ("VISA", 0.074), ("MA", 0.10)]:
            _seed_universe(db, ticker)
            _seed_deep_analysis(db, ticker, "B")
            _seed_alert(db, ticker, "B", target=150.0,
                        last_price=150.0 * (1 + gap), gap_pct=gap)

        text, html = generate_briefing_from_db(db)
        assert "B-TIER APPROACHING TARGET" in text
        assert "ORCL" in text
        assert "VISA" in text

        # ORCL (3.5% gap) should appear before VISA (7.4%) in the text
        assert text.index("ORCL") < text.index("VISA")

    def test_b_tier_not_in_approaching_when_gap_exceeds_threshold(self, tmp_path):
        db = Database(tmp_path / "test.db")
        _seed_universe(db, "NVDA")
        _seed_deep_analysis(db, "NVDA", "B")
        _seed_alert(db, "NVDA", "B", target=500.0, last_price=850.0, gap_pct=0.70)
        text, _ = generate_briefing_from_db(db)
        # NVDA should only appear in B-Tier Watch, not approaching
        assert "B-TIER WATCH" in text
        # The approaching section ends at the leaderboard heading
        leaderboard_pos   = text.find("TOP 10 BY QUALITY SCORE")
        approaching_start = text.find("B-TIER APPROACHING TARGET")
        approaching_text  = text[approaching_start:leaderboard_pos]
        assert "NVDA" not in approaching_text

    def test_approaching_threshold_is_10_pct(self):
        assert _APPROACHING_GAP_PCT == 0.10

    def test_near_target_star_marker(self, tmp_path):
        """Stocks within 5% get a *** marker in text output."""
        db = Database(tmp_path / "test.db")
        _seed_universe(db, "ORCL")
        _seed_deep_analysis(db, "ORCL", "B")
        _seed_alert(db, "ORCL", "B", target=150.0, last_price=154.0, gap_pct=0.027)
        text, _ = generate_briefing_from_db(db)
        assert "***" in text   # very close to target

    def test_no_approaching_message_when_none(self, tmp_path):
        """When all B stocks are far from target, show 'No B-tier stocks within 10%'."""
        db = Database(tmp_path / "test.db")
        _seed_universe(db, "NVDA")
        _seed_deep_analysis(db, "NVDA", "B")
        _seed_alert(db, "NVDA", "B", target=500.0, last_price=850.0, gap_pct=0.70)
        text, _ = generate_briefing_from_db(db)
        assert "No B-tier stocks within 10%" in text


class TestQualityLeaderboard:
    def test_top_10_shown_by_quality_score(self, tmp_path):
        db = Database(tmp_path / "test.db")
        # Seed 12 stocks with descending quality scores
        for i, ticker in enumerate(["V", "COST", "MSCI", "MA", "ORCL", "NVDA",
                                     "AMZN", "GOOGL", "NFLX", "SBUX", "DIS", "META"]):
            score = 95.0 - i * 2.0
            _seed_universe(db, ticker, score=score)

        text, html = generate_briefing_from_db(db)
        assert "TOP 10 BY QUALITY SCORE" in text
        # V should be #1, META should not appear (rank 12)
        assert "V" in text
        v_pos   = text.find("V")
        meta_pos = text.find("META")
        assert v_pos < meta_pos or meta_pos == -1  # V before META or META absent from leaderboard
        # Only 10 entries in the leaderboard
        assert "SBUX" in text  # rank 10
        # META rank 12 — not in top 10
        leaderboard_start = text.find("TOP 10 BY QUALITY SCORE")
        leaderboard_end   = text.find("NEWS EVENTS DIGEST")
        leaderboard_text  = text[leaderboard_start:leaderboard_end]
        assert "META" not in leaderboard_text

    def test_leaderboard_shows_tier_for_analyzed(self, tmp_path):
        db = Database(tmp_path / "test.db")
        _seed_universe(db, "ORCL", score=90.0)
        _seed_deep_analysis(db, "ORCL", "B")
        _seed_alert(db, "ORCL", "B", target=150.0, last_price=160.0, gap_pct=0.067)
        text, _ = generate_briefing_from_db(db)
        leaderboard = text[text.find("TOP 10 BY QUALITY SCORE"):]
        assert "ORCL" in leaderboard

    def test_no_leaderboard_when_no_quality_scores(self, tmp_path):
        """Universe stocks without quality_score are excluded from leaderboard."""
        db = Database(tmp_path / "test.db")
        db.upsert_universe_stock("ORCL", company_name="Oracle", sector="Tech",
                                  market_cap=50e9, source="conviction")
        # No quality_score set
        text, _ = generate_briefing_from_db(db)
        assert "No quality scores available" in text


class TestNewsDigest:
    def test_news_events_shown(self, tmp_path):
        db = Database(tmp_path / "test.db")
        _seed_universe(db, "ORCL")
        db.log_news_event(
            "ORCL",
            "Oracle acquires CloudMind for $10B",
            source="Reuters",
            event_type="acquisition",
            haiku_material=True,
            sonnet_triggered=True,
        )
        text, html = generate_briefing_from_db(db, days_back=1)
        assert "NEWS EVENTS DIGEST" in text
        assert "ORCL" in text
        assert "Oracle acquires CloudMind" in text

    def test_material_flag_shown(self, tmp_path):
        db = Database(tmp_path / "test.db")
        db.log_news_event("ORCL", "Oracle misses earnings", haiku_material=True)
        text, html = generate_briefing_from_db(db, days_back=1)
        assert "Haiku: MATERIAL" in text or "MATERIAL" in html

    def test_no_news_message_when_empty(self, tmp_path):
        db = Database(tmp_path / "test.db")
        text, _ = generate_briefing_from_db(db, days_back=1)
        assert "No news events detected" in text

    def test_old_news_not_shown(self, tmp_path):
        """News older than days_back should not appear."""
        db = Database(tmp_path / "test.db")
        # Insert directly with old detected_at
        conn = sqlite3.connect(str(db.path))
        conn.execute(
            """INSERT INTO news_events (ticker, headline, detected_at)
               VALUES ('ORCL', 'Very old news', '2020-01-01T00:00:00')"""
        )
        conn.commit()
        conn.close()
        text, _ = generate_briefing_from_db(db, days_back=7)
        assert "Very old news" not in text


class TestCoverageDashboard:
    def test_coverage_counts_correct(self, tmp_path):
        db = Database(tmp_path / "test.db")
        for ticker in ["ORCL", "MSCI", "V"]:
            _seed_universe(db, ticker, sector="Technology", cap="large")
        # Only ORCL and MSCI have deep analyses
        _seed_deep_analysis(db, "ORCL", "B")
        _seed_deep_analysis(db, "MSCI", "A")
        text, _ = generate_briefing_from_db(db)
        assert "Universe total:       3" in text
        assert "Deep-analyzed:        2" in text

    def test_sector_breakdown_shown(self, tmp_path):
        db = Database(tmp_path / "test.db")
        _seed_universe(db, "ORCL", sector="Technology", cap="large", score=80.0)
        _seed_universe(db, "JPM", sector="Financials", cap="large", score=70.0)
        text, _ = generate_briefing_from_db(db)
        assert "Technology" in text
        assert "Financials" in text

    def test_cap_breakdown_shown(self, tmp_path):
        db = Database(tmp_path / "test.db")
        _seed_universe(db, "ORCL", sector="Technology", cap="large")
        _seed_universe(db, "SMCO", sector="Technology", cap="mid")
        text, _ = generate_briefing_from_db(db)
        assert "Large" in text
        assert "Mid" in text

    def test_coverage_pct_displayed(self, tmp_path):
        db = Database(tmp_path / "test.db")
        for ticker in ["A", "B", "C", "D"]:
            _seed_universe(db, ticker, sector="Technology")
        _seed_deep_analysis(db, "A", "B")
        text, _ = generate_briefing_from_db(db)
        # 1/4 = 25.0%
        assert "25.0%" in text


class TestPaperTradingScoreboard:
    def test_positions_shown_in_scoreboard(self, tmp_path):
        db = Database(tmp_path / "test.db")
        _seed_paper_position(db, "ORCL", tier="A")
        _seed_paper_position(db, "COST", tier="S")
        text, html = generate_briefing_from_db(db)
        assert "PAPER TRADING SCOREBOARD" in text
        assert "ORCL" in text
        assert "COST" in text

    def test_total_pl_computed(self, tmp_path):
        db = Database(tmp_path / "test.db")
        # cost_basis=1450, current_value=1550 → P&L = +100
        _seed_paper_position(db, "ORCL", tier="A")
        text, _ = generate_briefing_from_db(db)
        assert "+$100" in text or "100" in text

    def test_no_positions_message(self, tmp_path):
        db = Database(tmp_path / "test.db")
        text, html = generate_briefing_from_db(db)
        assert "No paper positions open" in text or "No paper positions open" in html


class TestCTierSection:
    def test_c_tier_listed_concisely(self, tmp_path):
        db = Database(tmp_path / "test.db")
        for ticker in ["TSLA", "NFLX", "ABNB", "DIS"]:
            _seed_universe(db, ticker)
            _seed_deep_analysis(db, ticker, "C")
            _seed_alert(db, ticker, "C", target=100.0, last_price=200.0, gap_pct=1.0)
        text, html = generate_briefing_from_db(db)
        assert "C-TIER MONITOR" in text
        for ticker in ["TSLA", "NFLX", "ABNB", "DIS"]:
            assert ticker in text

    def test_no_c_tier_section_when_none(self, tmp_path):
        db = Database(tmp_path / "test.db")
        _seed_universe(db, "ORCL")
        _seed_alert(db, "ORCL", "B", target=150.0, last_price=160.0, gap_pct=0.067)
        text, _ = generate_briefing_from_db(db)
        assert "C-TIER MONITOR" not in text


class TestOutputFiles:
    def test_saves_text_and_html_to_output_dir(self, tmp_path):
        db = Database(tmp_path / "test.db")
        out_dir = tmp_path / "briefings"
        generate_briefing_from_db(db, output_dir=str(out_dir))
        txt_files = list(out_dir.glob("briefing_*.txt"))
        html_files = list(out_dir.glob("briefing_*.html"))
        assert len(txt_files) == 1
        assert len(html_files) == 1

    def test_returns_text_and_html_strings(self, tmp_path):
        db = Database(tmp_path / "test.db")
        text, html = generate_briefing_from_db(db)
        assert isinstance(text, str)
        assert isinstance(html, str)
        assert len(text) > 100
        assert len(html) > 200

    def test_html_is_valid_structure(self, tmp_path):
        db = Database(tmp_path / "test.db")
        _, html = generate_briefing_from_db(db)
        assert html.startswith("<!DOCTYPE html>")
        assert "</html>" in html
        assert "<title>" in html
        assert "</body>" in html


class TestFullScenario:
    """Integration scenario: multiple tiers, all sections populated."""

    def test_full_scenario_all_sections_present(self, tmp_path):
        db = Database(tmp_path / "test.db")
        db.reset_weekly_budgets()
        db.spend_batch("weekly_haiku_screen", 3)

        # Universe
        for ticker, sector, cap, score in [
            ("COST",  "Consumer",   "large", 93.1),
            ("MSCI",  "Financials", "large", 91.8),
            ("ORCL",  "Technology", "large", 85.0),
            ("NVDA",  "Technology", "large", 80.0),
            ("TSLA",  "Automotive", "large", 60.0),
        ]:
            _seed_universe(db, ticker, sector=sector, cap=cap, score=score)

        # Deep analyses
        _seed_deep_analysis(db, "COST", "S", conviction="HIGH", moat="WIDE",
                            thesis="Membership model creates extraordinary loyalty")
        _seed_deep_analysis(db, "MSCI", "A", conviction="MEDIUM", moat="NARROW",
                            thesis="Index licensing monopoly, 95% recurring revenue")
        _seed_deep_analysis(db, "ORCL", "B", conviction="MEDIUM", moat="NARROW",
                            thesis="Cloud database switching costs")
        _seed_deep_analysis(db, "NVDA", "B", conviction="MEDIUM", moat="NARROW",
                            thesis="AI chip monopoly for now")
        _seed_deep_analysis(db, "TSLA", "C", conviction="LOW", moat="NONE",
                            thesis="Uncertain moat in competitive EV market")

        # Price alerts
        _seed_alert(db, "COST", "S", target=580.0, last_price=570.0, gap_pct=-0.017,
                    staged=[{"tranche": 1, "price": 580.0, "label": "1/3 at target"},
                            {"tranche": 2, "price": 551.0, "label": "2/3 -5%"},
                            {"tranche": 3, "price": 522.0, "label": "3/3 -10%"}])
        _seed_alert(db, "MSCI", "A", target=460.0, last_price=446.0, gap_pct=-0.030,
                    staged=[{"tranche": 1, "price": 460.0, "label": "1/2 at target"},
                            {"tranche": 2, "price": 437.0, "label": "2/2 -5%"}])
        _seed_alert(db, "ORCL", "B", target=150.0, last_price=155.0, gap_pct=0.033)
        _seed_alert(db, "NVDA", "B", target=500.0, last_price=850.0, gap_pct=0.70)
        _seed_alert(db, "TSLA", "C", target=100.0, last_price=220.0, gap_pct=1.20)

        # Paper positions
        _seed_paper_position(db, "COST", tier="S")
        _seed_paper_position(db, "MSCI", tier="A")

        # News
        db.log_news_event("ORCL", "Oracle acquires CloudMind for $10B",
                          event_type="acquisition", haiku_material=True, sonnet_triggered=True)

        text, html = generate_briefing_from_db(db)

        # All major sections present
        assert "S-TIER SPOTLIGHT" in text
        assert "A-TIER ACTION LIST" in text
        assert "B-TIER APPROACHING TARGET" in text
        assert "TOP 10 BY QUALITY SCORE" in text
        assert "NEWS EVENTS DIGEST" in text
        assert "COVERAGE DASHBOARD" in text
        assert "PAPER TRADING SCOREBOARD" in text
        assert "B-TIER WATCH" in text
        assert "C-TIER MONITOR" in text

        # ORCL (gap 3.3%) should be in approaching, NVDA (70%) should be in watch
        approaching_start = text.find("B-TIER APPROACHING TARGET")
        leaderboard_start = text.find("TOP 10 BY QUALITY SCORE")
        approaching_text  = text[approaching_start:leaderboard_start]
        assert "ORCL" in approaching_text
        assert "NVDA" not in approaching_text

        # Quality leaderboard: COST (93.1) > MSCI (91.8) > ORCL (85.0)
        leaderboard_start = text.find("TOP 10 BY QUALITY SCORE")
        news_start        = text.find("NEWS EVENTS DIGEST")
        leaderboard_text  = text[leaderboard_start:news_start]
        assert "COST" in leaderboard_text
        assert "MSCI" in leaderboard_text

        # Paper trading scoreboard has both positions
        assert "COST" in text
        assert "MSCI" in text

        # Budget shows 3 haiku calls used
        assert "3/50" in text

        # HTML also valid
        assert "S-Tier Spotlight" in html
        assert "A-Tier Action List" in html
        assert "Coverage Dashboard" in html
