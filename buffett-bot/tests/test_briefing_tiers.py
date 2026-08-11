"""End-to-end regressions for the S/A/B/C briefing and registry boundary."""

import json
from types import SimpleNamespace

from scripts.run_monthly_briefing import _paper_trade_symbols
from src.briefing import StockBriefing
from src.briefing.html_formatter import generate_html_report
from src.briefing.text_formatter import generate_text_report
from src.registry import Registry
from src.tier_engine import Tier
from src.valuation import AggregatedValuation


def _briefing(tier: Tier) -> StockBriefing:
    analysis = SimpleNamespace(
        moat_rating=SimpleNamespace(value="wide"),
        management_rating=None,
        conviction_level="HIGH",
        moat_sources=["brand"],
        investment_thesis="Durable business.",
        key_risks=[],
        thesis_risks=[],
    )
    return StockBriefing(
        symbol=f"{tier}CO",
        company_name=f"{tier}-tier Company",
        current_price=90.0,
        market_cap=1_000_000.0,
        pe_ratio=20.0,
        debt_equity=0.2,
        roe=0.2,
        revenue_growth=0.1,
        valuation=AggregatedValuation(symbol=f"{tier}CO", current_price=90.0),
        analysis=analysis,
        tier=tier,
        tier_reason="test tier",
        target_entry_price=100.0,
        price_gap_pct=-0.1,
    )


def test_letter_tiers_render_in_text_and_html():
    briefings = [_briefing(tier) for tier in Tier]

    text = generate_text_report(briefings)
    html = generate_html_report(briefings)

    assert "S-tier (Wonderful): 1" in text
    assert "A-tier (Buy Zone):  1" in text
    assert "No S/A picks" not in text
    assert "SCO" in text and "ACO" in text and "BCO" in text and "CCO" in text
    assert "S-TIER" in html and "A-TIER" in html and "B-TIER" in html


def test_recommendations_follow_letter_tiers():
    assert _briefing(Tier.S).recommendation == "BUY"
    assert _briefing(Tier.A).recommendation == "BUY"
    assert _briefing(Tier.B).recommendation == "WATCHLIST"
    assert _briefing(Tier.C).recommendation == "PASS"


def test_registry_migrates_legacy_numeric_tiers(tmp_path):
    registry_data = {
        "version": 1,
        "campaign": {
            "campaign_id": "2026-Q3",
            "started_at": "2026-07-01T00:00:00",
            "haiku_screened": [],
            "haiku_passed": [],
            "haiku_failed": {},
            "analyzed": [],
        },
        "studies": {
            "WIDE": {"tier": 1, "analysis": {"moat_rating": "wide", "conviction": "HIGH"}},
            "NARROW": {"tier": 1, "analysis": {"moat_rating": "narrow", "conviction": "MEDIUM"}},
            "WATCH": {"tier": 2, "analysis": {}},
            "PASS": {"tier": 3, "analysis": {}},
        },
    }
    (tmp_path / "registry.json").write_text(json.dumps(registry_data))

    registry = Registry(tmp_path)

    assert registry._data["version"] == 2
    assert registry.get_studied("WIDE")["tier"] == "S"
    assert registry.get_studied("NARROW")["tier"] == "A"
    assert set(registry.get_tier_entries([Tier.S, Tier.A, Tier.B])) == {"WIDE", "NARROW", "WATCH"}


def test_registry_retries_invalid_haiku_results(tmp_path):
    registry = Registry(tmp_path)

    registry.mark_haiku_screened(
        ["RETRY", "PASS", "FAIL"],
        [
            {
                "symbol": "RETRY",
                "valid": False,
                "worth_analysis": False,
                "moat_hint": 0,
                "quality_hint": 0,
            },
            {"symbol": "PASS", "valid": True, "moat_hint": 4, "quality_hint": 4},
            {"symbol": "FAIL", "valid": True, "moat_hint": 1, "quality_hint": 1},
        ],
        min_score=5,
    )

    assert registry.get_unstudied_symbols(["RETRY", "PASS", "FAIL", "MISSING"]) == ["RETRY", "MISSING"]
    assert registry.campaign["haiku_passed"] == ["PASS"]
    assert set(registry.campaign["haiku_failed"]) == {"FAIL"}


def test_paper_trade_candidates_require_fresh_deterministic_evidence():
    assignments = {
        "FRESH": SimpleNamespace(tier=Tier.A, price_gap_pct=-0.10),
        "STALE": SimpleNamespace(tier=Tier.S, price_gap_pct=-0.20),
        "NOQUOTE": SimpleNamespace(tier=Tier.A, price_gap_pct=-0.10),
        "ABOVE": SimpleNamespace(tier=Tier.A, price_gap_pct=0.01),
    }
    valuations = {
        "FRESH": SimpleNamespace(has_valid_price=True, margin_of_safety=0.20),
        "STALE": SimpleNamespace(has_valid_price=True, margin_of_safety=0.30),
        "NOQUOTE": SimpleNamespace(has_valid_price=False, margin_of_safety=None),
        "ABOVE": SimpleNamespace(has_valid_price=True, margin_of_safety=0.10),
    }

    assert _paper_trade_symbols(assignments, ["FRESH", "NOQUOTE", "ABOVE"], valuations) == ["FRESH"]
