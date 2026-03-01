"""
Tests for src/tier_engine.py — S/A/B/C tier assignment logic.

These are the cases where a subtle bug silently corrupts results:
- A wonderful business at fair value must be S, not A
- A wonderful but overpriced business must be B, not S
- Extreme premium (>50%) must be C, not B
- No price data must never trigger a buy tier (S or A)
- Approaching-target flag must only fire for B-tier within proximity window
- Staged entries must have correct tranche count per tier
- Tier comparisons (tier_up / tier_down) must understand S > A > B > C ordering
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional

import pytest

from src.tier_engine import (
    EXTREME_PREMIUM_THRESHOLD,
    TIER_ORDER,
    TierAssignment,
    WatchlistMovement,
    assign_tier,
    compute_movements,
    staged_entry_suggestion,
)


# ─── Fakes ────────────────────────────────────────────────────────────────


class FakeMoatRating(Enum):
    WIDE = "wide"
    NARROW = "narrow"
    NONE = "none"


@dataclass
class FakeAnalysis:
    """Minimal AnalysisV2-compatible object for testing."""

    symbol: str
    _moat: str       # "wide", "narrow", "none"
    _conviction: str  # "HIGH", "MEDIUM", "LOW"
    target_entry_price: Optional[float] = None
    current_price: Optional[float] = None

    @property
    def moat_rating(self) -> FakeMoatRating:
        return FakeMoatRating(self._moat)

    @property
    def conviction_level(self) -> str:
        return self._conviction.upper()


def make(moat: str, conviction: str, target=None, current=None, symbol="TEST") -> FakeAnalysis:
    return FakeAnalysis(symbol=symbol, _moat=moat, _conviction=conviction,
                        target_entry_price=target, current_price=current)


# ─── S-Tier Tests ─────────────────────────────────────────────────────────


class TestSTier:
    def test_wide_high_at_target(self):
        """Exact target price → S."""
        a = assign_tier(make("wide", "HIGH", target=100.0, current=100.0))
        assert a.tier == "S"

    def test_wide_high_below_target(self):
        """Below target (undervalued) → S."""
        a = assign_tier(make("wide", "HIGH", target=100.0, current=85.0))
        assert a.tier == "S"
        assert a.price_gap_pct is not None and a.price_gap_pct < 0

    def test_wide_high_just_above_target_not_s(self):
        """1 cent above target → must NOT be S."""
        a = assign_tier(make("wide", "HIGH", target=100.0, current=100.01))
        assert a.tier != "S"

    def test_s_tier_no_approaching_flag(self):
        """S-tier stocks are already buyable — approaching_target should be False."""
        a = assign_tier(make("wide", "HIGH", target=100.0, current=95.0))
        assert a.tier == "S"
        assert a.approaching_target is False

    def test_s_tier_quality_level(self):
        a = assign_tier(make("wide", "HIGH", target=100.0, current=90.0))
        assert a.quality_level == "wonderful"


# ─── A-Tier Tests ─────────────────────────────────────────────────────────


class TestATier:
    def test_narrow_high_at_target(self):
        """Narrow moat + HIGH conviction + at target → A (not S)."""
        a = assign_tier(make("narrow", "HIGH", target=100.0, current=100.0))
        assert a.tier == "A"

    def test_narrow_medium_at_target(self):
        """Narrow moat + MEDIUM conviction + at target → A."""
        a = assign_tier(make("narrow", "MEDIUM", target=100.0, current=90.0))
        assert a.tier == "A"

    def test_wide_medium_at_target(self):
        """Wide moat + MEDIUM conviction (not HIGH) → A, not S."""
        a = assign_tier(make("wide", "MEDIUM", target=100.0, current=95.0))
        assert a.tier == "A"

    def test_a_tier_quality_level(self):
        a = assign_tier(make("narrow", "MEDIUM", target=100.0, current=80.0))
        assert a.quality_level == "good"


# ─── B-Tier Tests ─────────────────────────────────────────────────────────


class TestBTier:
    def test_wonderful_above_target_not_extreme(self):
        """Wide + HIGH + 30% above target → B (watch, wonderful but wait)."""
        a = assign_tier(make("wide", "HIGH", target=100.0, current=130.0))
        assert a.tier == "B"

    def test_good_above_target_not_extreme(self):
        """Good quality + 20% above target → B."""
        a = assign_tier(make("narrow", "MEDIUM", target=100.0, current=120.0))
        assert a.tier == "B"

    def test_approaching_target_flag_fires(self):
        """B-tier stock within proximity window gets approaching_target=True."""
        # Default proximity is 10%, so 5% above target should flag
        a = assign_tier(make("wide", "HIGH", target=100.0, current=105.0), proximity_alert_pct=0.10)
        assert a.tier == "B"
        assert a.approaching_target is True

    def test_approaching_target_flag_does_not_fire_too_far(self):
        """30% above target — not approaching yet."""
        a = assign_tier(make("wide", "HIGH", target=100.0, current=130.0), proximity_alert_pct=0.10)
        assert a.tier == "B"
        assert a.approaching_target is False

    def test_approaching_flag_at_exact_proximity_boundary(self):
        """Exactly at proximity boundary (10%) → approaching=True."""
        a = assign_tier(make("wide", "HIGH", target=100.0, current=110.0), proximity_alert_pct=0.10)
        assert a.approaching_target is True

    def test_no_price_data_is_b_not_buy(self):
        """High-quality stock with no price data → B (cannot confirm price is right)."""
        a = assign_tier(make("wide", "HIGH", target=None, current=None))
        assert a.tier == "B"
        assert a.tier not in ("S", "A")

    def test_target_only_no_current_is_b(self):
        """Target known but no current price → B."""
        a = assign_tier(make("narrow", "HIGH", target=100.0, current=None))
        assert a.tier == "B"


# ─── C-Tier Tests ─────────────────────────────────────────────────────────


class TestCTier:
    def test_extreme_premium_wonderful(self):
        """Wide + HIGH + 60% above target → C (extreme premium)."""
        a = assign_tier(make("wide", "HIGH", target=100.0, current=160.0))
        assert a.tier == "C"

    def test_extreme_premium_exact_threshold(self):
        """Exactly at 50% threshold → C."""
        a = assign_tier(make("wide", "HIGH", target=100.0, current=150.0))
        assert a.tier == "C"

    def test_low_conviction_any_moat(self):
        """LOW conviction + no moat → C."""
        a = assign_tier(make("none", "LOW", target=100.0, current=95.0))
        assert a.tier == "C"

    def test_narrow_low_conviction(self):
        """Narrow moat + LOW conviction → moderate/low quality → C."""
        a = assign_tier(make("narrow", "LOW", target=100.0, current=80.0))
        assert a.tier == "C"

    def test_none_moat_medium_conviction(self):
        """No moat + MEDIUM conviction → moderate quality → C."""
        a = assign_tier(make("none", "MEDIUM", target=100.0, current=80.0))
        assert a.tier == "C"

    def test_c_no_buy_action(self):
        """C-tier must never trigger buying (approaching=False)."""
        a = assign_tier(make("none", "LOW", target=100.0, current=101.0))
        assert a.tier == "C"
        assert a.approaching_target is False


# ─── Threshold Edge Cases ──────────────────────────────────────────────────


class TestThresholds:
    def test_extreme_premium_boundary(self):
        """49.9% gap → B (just below extreme_premium). 50.0% → C."""
        just_under = assign_tier(make("wide", "HIGH", target=100.0, current=149.9))
        at_threshold = assign_tier(make("wide", "HIGH", target=100.0, current=150.0))
        assert just_under.tier == "B"
        assert at_threshold.tier == "C"

    def test_exactly_at_target(self):
        """current == target means gap == 0 exactly → buyable."""
        a = assign_tier(make("wide", "HIGH", target=100.0, current=100.0))
        assert a.tier == "S"
        assert a.price_gap_pct == pytest.approx(0.0)

    def test_price_gap_sign(self):
        """Below target → negative gap (we use (current-target)/target)."""
        a = assign_tier(make("wide", "HIGH", target=100.0, current=80.0))
        assert a.price_gap_pct is not None
        assert a.price_gap_pct == pytest.approx(-0.20)

    def test_price_gap_above_target(self):
        a = assign_tier(make("wide", "HIGH", target=100.0, current=130.0))
        assert a.price_gap_pct is not None
        assert a.price_gap_pct == pytest.approx(0.30)


# ─── Staged Entry Tests ───────────────────────────────────────────────────


class TestStagedEntry:
    def test_s_tier_three_tranches(self):
        tranches = staged_entry_suggestion(100.0, tier="S")
        assert len(tranches) == 3

    def test_a_tier_two_tranches(self):
        tranches = staged_entry_suggestion(100.0, tier="A")
        assert len(tranches) == 2

    def test_b_tier_no_tranches(self):
        tranches = staged_entry_suggestion(100.0, tier="B")
        assert len(tranches) == 0

    def test_c_tier_no_tranches(self):
        tranches = staged_entry_suggestion(100.0, tier="C")
        assert len(tranches) == 0

    def test_s_tier_prices_descend(self):
        """Each tranche should be lower than the previous (5% steps)."""
        tranches = staged_entry_suggestion(100.0, tier="S", step_pct=0.05)
        prices = [t["price"] for t in tranches]
        assert prices == sorted(prices, reverse=True)

    def test_s_tier_allocation_sums_to_one(self):
        tranches = staged_entry_suggestion(100.0, tier="S")
        total = sum(t["allocation"] for t in tranches)
        assert total == pytest.approx(1.0)

    def test_a_tier_allocation_sums_to_one(self):
        tranches = staged_entry_suggestion(100.0, tier="A")
        total = sum(t["allocation"] for t in tranches)
        assert total == pytest.approx(1.0)

    def test_s_tier_first_tranche_at_target(self):
        """First S-tier tranche is at target entry price."""
        tranches = staged_entry_suggestion(175.0, tier="S")
        assert tranches[0]["price"] == pytest.approx(175.0)

    def test_s_tier_step_calculation(self):
        """With 5% steps: 175, 175*0.95, 175*0.90."""
        tranches = staged_entry_suggestion(175.0, tier="S", step_pct=0.05)
        assert tranches[0]["price"] == pytest.approx(175.0)
        assert tranches[1]["price"] == pytest.approx(175.0 * 0.95)
        assert tranches[2]["price"] == pytest.approx(175.0 * 0.90)

    def test_label_format(self):
        tranches = staged_entry_suggestion(100.0, tier="S")
        assert tranches[0]["label"].startswith("1/3")
        assert tranches[1]["label"].startswith("2/3")
        assert tranches[2]["label"].startswith("3/3")


# ─── Tier Ordering ────────────────────────────────────────────────────────


class TestTierOrdering:
    def test_s_is_best(self):
        assert TIER_ORDER["S"] < TIER_ORDER["A"]

    def test_a_before_b(self):
        assert TIER_ORDER["A"] < TIER_ORDER["B"]

    def test_b_before_c(self):
        assert TIER_ORDER["B"] < TIER_ORDER["C"]

    def test_complete_ordering(self):
        tiers = sorted(TIER_ORDER, key=TIER_ORDER.get)
        assert tiers == ["S", "A", "B", "C"]


# ─── Movement Tracking ────────────────────────────────────────────────────


class TestComputeMovements:
    def _assignment(self, symbol, tier, target=100.0, current=90.0, approaching=False):
        return TierAssignment(
            symbol=symbol,
            tier=tier,
            quality_level="good",
            tier_reason="test",
            target_entry_price=target,
            current_price=current,
            price_gap_pct=(current - target) / target if target else None,
            approaching_target=approaching,
        )

    def test_new_entry(self):
        current = {"AAPL": self._assignment("AAPL", "A")}
        movements = compute_movements(current, previous_state={})
        assert any(m.change_type == "new" and m.symbol == "AAPL" for m in movements)

    def test_tier_up_b_to_a(self):
        current = {"AAPL": self._assignment("AAPL", "A")}
        previous = {"stocks": {"AAPL": {"tier": "B", "approaching": False}}}
        movements = compute_movements(current, previous)
        ups = [m for m in movements if m.change_type == "tier_up"]
        assert any(m.symbol == "AAPL" for m in ups)

    def test_tier_up_a_to_s(self):
        current = {"MSFT": self._assignment("MSFT", "S")}
        previous = {"stocks": {"MSFT": {"tier": "A", "approaching": False}}}
        movements = compute_movements(current, previous)
        assert any(m.change_type == "tier_up" and m.symbol == "MSFT" for m in movements)

    def test_tier_down_a_to_b(self):
        current = {"V": self._assignment("V", "B")}
        previous = {"stocks": {"V": {"tier": "A", "approaching": False}}}
        movements = compute_movements(current, previous)
        assert any(m.change_type == "tier_down" and m.symbol == "V" for m in movements)

    def test_removed_when_c(self):
        """Stock dropping to C from active tier should register as removed."""
        current = {"TSLA": self._assignment("TSLA", "C")}
        previous = {"stocks": {"TSLA": {"tier": "B", "approaching": False}}}
        movements = compute_movements(current, previous)
        assert any(m.change_type == "removed" and m.symbol == "TSLA" for m in movements)

    def test_removed_when_gone(self):
        """Stock disappearing entirely from current results → removed."""
        current: dict = {}
        previous = {"stocks": {"GE": {"tier": "A", "approaching": False}}}
        movements = compute_movements(current, previous)
        assert any(m.change_type == "removed" and m.symbol == "GE" for m in movements)

    def test_no_movement_same_tier(self):
        """Same tier as before → no tier_up or tier_down movements."""
        current = {"KO": self._assignment("KO", "B")}
        previous = {"stocks": {"KO": {"tier": "B", "approaching": False}}}
        movements = compute_movements(current, previous)
        assert not any(m.change_type in ("tier_up", "tier_down") and m.symbol == "KO" for m in movements)

    def test_approaching_fires_once(self):
        """Approaching flag should fire when it transitions from False → True."""
        current = {"COST": self._assignment("COST", "B", approaching=True)}
        previous = {"stocks": {"COST": {"tier": "B", "approaching": False}}}
        movements = compute_movements(current, previous)
        assert any(m.change_type == "approaching" and m.symbol == "COST" for m in movements)

    def test_approaching_does_not_repeat(self):
        """Already approaching last time → should not fire again."""
        current = {"COST": self._assignment("COST", "B", approaching=True)}
        previous = {"stocks": {"COST": {"tier": "B", "approaching": True}}}
        movements = compute_movements(current, previous)
        assert not any(m.change_type == "approaching" and m.symbol == "COST" for m in movements)

    def test_c_tier_not_in_new(self):
        """C-tier entries should not appear as 'new' in movements."""
        current = {"JUNK": self._assignment("JUNK", "C")}
        movements = compute_movements(current, previous_state={})
        assert not any(m.symbol == "JUNK" and m.change_type == "new" for m in movements)
