"""
Tests for the Phase C regime-driven deployment engine (src/deployment.py).

Pure module — no mocking needed. Covers:
- target_invested_pct per regime, with fallback for unknown/missing regime
- plan_buys: gap sizing off real buying power, tier+margin ranking, the
  quality ceiling, and slot/capital caps
- plan_sells: thesis-break (only on a genuine downgrade from a better tier),
  rotation (overweight or a strictly better candidate waiting), and the
  "otherwise hold" default
"""

from datetime import datetime, timezone

import pytest

from src.accounts.base import AccountState, PositionState
from src.config import Config
from src.deployment import (
    DeployCandidate,
    HeldPosition,
    plan_buys,
    plan_sells,
    target_invested_pct,
)

CFG = Config(
    max_positions=5,
    max_position_pct=0.20,
    deploy_target_euphoria=0.65,
    deploy_target_overvalued=0.80,
    deploy_target_fair_value=0.90,
    deploy_target_correction=0.97,
    deploy_target_crisis=1.00,
    quality_ceiling_pct=0.125,
    min_trade_usd=100.0,
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


def _position(
    symbol, *, market_value=10_000.0, shares=100.0, avg_cost=90.0, price=100.0, tradable=True
) -> PositionState:
    return PositionState(
        symbol=symbol,
        shares=shares,
        avg_cost=avg_cost,
        price=price,
        market_value=market_value,
        unrealized_pl=(price - avg_cost) * shares,
        unrealized_pl_pct=(price - avg_cost) / avg_cost,
        tradable=tradable,
    )


# ─── target_invested_pct ────────────────────────────────────────────────────


class TestTargetInvestedPct:
    def test_each_regime_maps_to_its_config_field(self):
        assert target_invested_pct("euphoria", cfg=CFG) == 0.65
        assert target_invested_pct("overvalued", cfg=CFG) == 0.80
        assert target_invested_pct("fair_value", cfg=CFG) == 0.90
        assert target_invested_pct("correction", cfg=CFG) == 0.97
        assert target_invested_pct("crisis", cfg=CFG) == 1.00

    def test_unknown_regime_falls_back_to_fair_value(self):
        assert target_invested_pct("some_new_label", cfg=CFG) == 0.90

    def test_none_regime_falls_back_to_fair_value(self):
        assert target_invested_pct(None, cfg=CFG) == 0.90


# ─── plan_buys ───────────────────────────────────────────────────────────────


class TestPlanBuys:
    def test_gap_below_min_trade_produces_no_buys(self):
        # equity 100k, target 90% = 90k, invested already 89.95k -> gap 50 < min_trade_usd 100
        state = _state(equity=100_000.0, invested_value=89_950.0, buying_power=10_000.0)
        plan = plan_buys(state, [], "fair_value", current_position_count=2, cfg=CFG)
        assert plan.buys == []
        assert plan.target_pct == 0.90
        assert plan.target_value == 90_000.0
        assert plan.gap == 50.0

    def test_negative_gap_when_already_over_target(self):
        state = _state(equity=100_000.0, invested_value=95_000.0, buying_power=5_000.0)
        candidates = [DeployCandidate(symbol="AAPL", margin_of_safety=0.20, tier="A")]
        plan = plan_buys(state, candidates, "fair_value", current_position_count=2, cfg=CFG)
        assert plan.gap == -5_000.0
        assert plan.buys == []

    @pytest.mark.parametrize("field", ["equity", "invested_value", "buying_power"])
    def test_non_finite_account_state_is_rejected(self, field):
        values = {"equity": 100_000.0, "invested_value": 50_000.0, "buying_power": 50_000.0}
        values[field] = float("nan")
        state = _state(**values)

        with pytest.raises(ValueError, match="finite"):
            plan_buys(state, [], "fair_value", current_position_count=1, cfg=CFG)

    def test_single_candidate_sized_by_tier_weight(self):
        # gap = 90k - 50k = 40k. max_position_value = 100k * 0.20 = 20k.
        # S-tier weight 1.0 -> amount = min(20k, 40k, buying_power) = 20k (capped by max_position_value)
        state = _state(equity=100_000.0, invested_value=50_000.0, buying_power=50_000.0)
        candidates = [DeployCandidate(symbol="AAPL", margin_of_safety=0.20, tier="S")]
        plan = plan_buys(state, candidates, "fair_value", current_position_count=1, cfg=CFG)
        assert len(plan.buys) == 1
        assert plan.buys[0].symbol == "AAPL"
        assert plan.buys[0].amount == 20_000.0

    def test_capped_by_remaining_buying_power(self):
        state = _state(equity=100_000.0, invested_value=50_000.0, buying_power=5_000.0)
        candidates = [DeployCandidate(symbol="AAPL", margin_of_safety=0.20, tier="S")]
        plan = plan_buys(state, candidates, "fair_value", current_position_count=1, cfg=CFG)
        assert plan.buys[0].amount == 5_000.0

    def test_ranks_tier_above_margin_of_safety(self):
        # B-tier with a huge margin is not buyable; A remains eligible.
        state = _state(equity=100_000.0, invested_value=0.0, buying_power=1_000_000.0)
        candidates = [
            DeployCandidate(symbol="BIGMARGIN", margin_of_safety=0.50, tier="B"),
            DeployCandidate(symbol="TIERED", margin_of_safety=0.05, tier="A"),
        ]
        plan = plan_buys(state, candidates, "fair_value", current_position_count=0, cfg=CFG)
        assert [buy.symbol for buy in plan.buys] == ["TIERED"]

    def test_unanalysed_and_b_tier_candidates_are_not_bought(self):
        state = _state(equity=100_000.0, invested_value=0.0, buying_power=1_000_000.0)
        candidates = [
            DeployCandidate(symbol="FRESH", margin_of_safety=0.10, tier=None),
            DeployCandidate(symbol="BTIER", margin_of_safety=0.05, tier="B"),
        ]
        plan = plan_buys(state, candidates, "fair_value", current_position_count=0, cfg=CFG)
        assert plan.buys == []

    def test_c_tier_candidates_are_never_bought(self):
        # A known-C name is one the bot has already judged and rejected.
        # plan_sells keeps held C-tier positions permanently rotation-eligible,
        # so buying one would be churn: bought this Friday, displaced by the
        # first better candidate that appears.
        state = _state(equity=100_000.0, invested_value=0.0, buying_power=1_000_000.0)
        candidates = [DeployCandidate(symbol="CTIER", margin_of_safety=0.50, tier="C")]
        plan = plan_buys(state, candidates, "fair_value", current_position_count=0, cfg=CFG)
        assert plan.buys == []
        assert "CTIER" not in plan.skipped_ceiling  # excluded for tier, not price

    def test_quality_ceiling_excludes_names_too_far_above_fair_value(self):
        state = _state(equity=100_000.0, invested_value=0.0, buying_power=1_000_000.0)
        candidates = [
            DeployCandidate(symbol="TOOEXPENSIVE", margin_of_safety=-0.20, tier="S"),  # 20% above fair value
            DeployCandidate(symbol="OKPREMIUM", margin_of_safety=-0.05, tier="A"),  # 5% above fair value
        ]
        plan = plan_buys(state, candidates, "fair_value", current_position_count=0, cfg=CFG)
        symbols_bought = [b.symbol for b in plan.buys]
        assert "TOOEXPENSIVE" not in symbols_bought
        assert "TOOEXPENSIVE" in plan.skipped_ceiling
        assert "OKPREMIUM" in symbols_bought

    @pytest.mark.parametrize("margin", [None, float("nan"), float("inf")])
    def test_missing_or_non_finite_margin_is_not_bought(self, margin):
        state = _state(equity=100_000.0, invested_value=0.0, buying_power=100_000.0)
        candidates = [DeployCandidate(symbol="INVALID", margin_of_safety=margin, tier="S")]

        assert plan_buys(state, candidates, "fair_value", current_position_count=0, cfg=CFG).buys == []

    def test_held_symbols_excluded_from_candidates(self):
        state = _state(equity=100_000.0, invested_value=0.0, buying_power=1_000_000.0)
        candidates = [DeployCandidate(symbol="AAPL", margin_of_safety=0.20, tier="S")]
        plan = plan_buys(
            state, candidates, "fair_value", current_position_count=0, held_symbols=frozenset({"AAPL"}), cfg=CFG
        )
        assert plan.buys == []

    def test_stops_at_max_positions_slots(self):
        # max_positions=5, current_position_count=4 -> only 1 slot free.
        state = _state(equity=100_000.0, invested_value=0.0, buying_power=1_000_000.0)
        candidates = [
            DeployCandidate(symbol="A", margin_of_safety=0.20, tier="S"),
            DeployCandidate(symbol="B", margin_of_safety=0.20, tier="S"),
        ]
        plan = plan_buys(state, candidates, "fair_value", current_position_count=4, cfg=CFG)
        assert len(plan.buys) == 1

    def test_crisis_regime_targets_full_deployment(self):
        state = _state(equity=100_000.0, invested_value=0.0, buying_power=1_000_000.0)
        plan = plan_buys(state, [], "crisis", current_position_count=0, cfg=CFG)
        assert plan.target_value == 100_000.0


# ─── plan_sells ──────────────────────────────────────────────────────────────


class TestPlanSells:
    def test_thesis_break_always_sells(self):
        """A genuine downgrade — held at a better tier, later C — exits at once."""
        held = [HeldPosition(position=_position("BADTHESIS"), tier="C", margin_of_safety=0.50, downgraded_to_c=True)]
        sells = plan_sells(100_000.0, held, [], cfg=CFG)
        assert len(sells) == 1
        assert sells[0].symbol == "BADTHESIS"
        assert "Thesis breaker" in sells[0].reason

    def test_holds_when_not_near_fair_value(self):
        held = [HeldPosition(position=_position("WINNER"), tier="A", margin_of_safety=0.30)]
        sells = plan_sells(100_000.0, held, [], cfg=CFG)
        assert sells == []

    def test_holds_at_fair_value_with_nothing_better_waiting(self):
        held = [HeldPosition(position=_position("ATFAIR", market_value=10_000.0), tier="A", margin_of_safety=0.02)]
        sells = plan_sells(100_000.0, held, [], cfg=CFG)
        assert sells == []

    def test_sells_to_rotate_into_better_candidate(self):
        held = [HeldPosition(position=_position("ATFAIR", market_value=10_000.0), tier="B", margin_of_safety=0.02)]
        candidates = [DeployCandidate(symbol="BETTER", margin_of_safety=0.30, tier="S")]
        sells = plan_sells(100_000.0, held, candidates, cfg=CFG)
        assert len(sells) == 1
        assert sells[0].symbol == "ATFAIR"
        assert "Rotate" in sells[0].reason

    def test_does_not_rotate_when_candidate_is_not_strictly_better(self):
        held = [HeldPosition(position=_position("ATFAIR", market_value=10_000.0), tier="S", margin_of_safety=0.02)]
        candidates = [DeployCandidate(symbol="WORSE", margin_of_safety=0.30, tier="B")]
        sells = plan_sells(100_000.0, held, candidates, cfg=CFG)
        assert sells == []

    def test_sells_when_overweight_and_near_fair_value(self):
        # market_value 25k / equity 100k = 25% > max_position_pct 20%
        held = [HeldPosition(position=_position("BIGWIN", market_value=25_000.0), tier="A", margin_of_safety=0.02)]
        sells = plan_sells(100_000.0, held, [], cfg=CFG)
        assert len(sells) == 1
        assert "overweight" in sells[0].reason.lower()
        # Trim only the $5k excess above the 20% cap: 50 shares at $100.
        assert sells[0].quantity == pytest.approx(50.0)

    def test_does_not_sell_overweight_when_not_near_fair_value(self):
        # Overweight but still deeply undervalued — let the winner run.
        held = [HeldPosition(position=_position("BIGWIN", market_value=25_000.0), tier="A", margin_of_safety=0.40)]
        sells = plan_sells(100_000.0, held, [], cfg=CFG)
        assert sells == []

    def test_candidate_already_held_does_not_count_as_better_waiting(self):
        held = [
            HeldPosition(position=_position("ATFAIR", market_value=10_000.0), tier="B", margin_of_safety=0.02),
        ]
        # "ATFAIR" itself appearing in candidates (already held) must not trigger rotation.
        candidates = [DeployCandidate(symbol="ATFAIR", margin_of_safety=0.02, tier="B")]
        sells = plan_sells(100_000.0, held, candidates, cfg=CFG)
        assert sells == []

    def test_missing_margin_of_safety_is_not_near_fair_value(self):
        held = [HeldPosition(position=_position("UNKNOWN"), tier="A", margin_of_safety=None)]
        sells = plan_sells(100_000.0, held, [], cfg=CFG)
        assert sells == []


class TestPlanSellsQuarantine:
    """
    A delisted holding cannot be sold — the broker rejects the order. Emitting
    an intent anyway would produce one failed order every week forever, so it
    must be skipped ahead of every other sell branch.
    """

    def test_skips_untradable_downgraded_to_c(self):
        # The realistic case: a delisted name also gets downgraded to C, which
        # would otherwise fire the thesis-breaker branch.
        held = [HeldPosition(position=_position("AL", tradable=False), tier="C", margin_of_safety=0.50)]
        assert plan_sells(100_000.0, held, [], cfg=CFG) == []

    def test_skips_untradable_when_overweight_and_near_fair_value(self):
        held = [
            HeldPosition(
                position=_position("AL", market_value=25_000.0, tradable=False),
                tier="A",
                margin_of_safety=0.02,
            )
        ]
        assert plan_sells(100_000.0, held, [], cfg=CFG) == []

    def test_skips_untradable_when_better_candidate_waiting(self):
        held = [
            HeldPosition(
                position=_position("AL", market_value=10_000.0, tradable=False),
                tier="B",
                margin_of_safety=0.02,
            )
        ]
        candidates = [DeployCandidate(symbol="BETTER", margin_of_safety=0.30, tier="S")]
        assert plan_sells(100_000.0, held, candidates, cfg=CFG) == []

    def test_tradable_positions_still_sell_alongside_a_quarantined_one(self):
        held = [
            HeldPosition(
                position=_position("AL", tradable=False), tier="C", margin_of_safety=0.50, downgraded_to_c=True
            ),
            HeldPosition(position=_position("BADTHESIS"), tier="C", margin_of_safety=0.50, downgraded_to_c=True),
        ]
        sells = plan_sells(100_000.0, held, [], cfg=CFG)
        assert [s.symbol for s in sells] == ["BADTHESIS"]


class TestPlanSellsFirstAnalysisC:
    """
    A thesis breaker requires a thesis. The bot buys unanalysed candidates
    in legacy journal data; holdings then jump the Sonnet queue, and the
    resulting first analysis used to fire an immediate "thesis breaker".
    AD was bought 2026-07-02 unanalysed, rated C the next morning and sold that
    afternoon for -3.67% — a round trip on an opinion the bot formed after
    buying, not on any thesis failing. New buys now require S/A up front, but
    the sell logic still has to handle existing rows correctly.
    """

    def test_first_analysis_c_does_not_emergency_sell(self):
        held = [HeldPosition(position=_position("NEWBUY"), tier="C", margin_of_safety=0.50)]
        assert plan_sells(100_000.0, held, [], cfg=CFG) == []

    def test_downgrade_to_c_still_sells_immediately(self):
        held = [HeldPosition(position=_position("OLDCO"), tier="C", margin_of_safety=0.50, downgraded_to_c=True)]
        sells = plan_sells(100_000.0, held, [], cfg=CFG)
        assert len(sells) == 1
        assert "Thesis breaker" in sells[0].reason

    def test_first_analysis_c_rotates_out_when_something_better_appears(self):
        """
        Declining the emergency sell must not mean holding forever. C-tier
        stays rotation-eligible regardless of price, so a better candidate
        displaces it.
        """
        held = [HeldPosition(position=_position("NEWBUY"), tier="C", margin_of_safety=0.50)]
        candidates = [DeployCandidate(symbol="BETTER", margin_of_safety=0.30, tier="A")]
        sells = plan_sells(100_000.0, held, candidates, cfg=CFG)
        assert len(sells) == 1
        assert "C-tier on first analysis" in sells[0].reason

    def test_first_analysis_c_is_held_when_nothing_better_waits(self):
        held = [HeldPosition(position=_position("NEWBUY"), tier="C", margin_of_safety=0.50)]
        candidates = [DeployCandidate(symbol="ALSOBAD", margin_of_safety=0.30, tier="C")]
        assert plan_sells(100_000.0, held, candidates, cfg=CFG) == []

    def test_c_tier_rotation_ignores_the_near_fair_value_gate(self):
        """
        The trap this avoids: the rotation gate only opens near fair value, so
        a deeply undervalued C-rated name would otherwise be unsellable by any
        path once the emergency exit was removed.
        """
        deep_discount = HeldPosition(position=_position("CHEAP"), tier="C", margin_of_safety=0.60)
        candidates = [DeployCandidate(symbol="BETTER", margin_of_safety=0.10, tier="A")]
        assert len(plan_sells(100_000.0, [deep_discount], candidates, cfg=CFG)) == 1

    def test_non_c_tiers_still_respect_the_near_fair_value_gate(self):
        """The relaxation must apply to C only — a cheap B is still a hold."""
        held = [HeldPosition(position=_position("GOODCO"), tier="B", margin_of_safety=0.60)]
        candidates = [DeployCandidate(symbol="BETTER", margin_of_safety=0.30, tier="S")]
        assert plan_sells(100_000.0, held, candidates, cfg=CFG) == []

    def test_quarantined_first_analysis_c_is_still_skipped(self):
        held = [HeldPosition(position=_position("AL", tradable=False), tier="C", margin_of_safety=0.50)]
        candidates = [DeployCandidate(symbol="BETTER", margin_of_safety=0.30, tier="A")]
        assert plan_sells(100_000.0, held, candidates, cfg=CFG) == []

    def test_overweight_is_measured_against_corrected_equity(self):
        """
        Callers now pass tradable equity, so the overweight denominator shrinks
        and a position can cross the ceiling that previously sat under it.
        18% of gross 100k, but 20.6% of corrected 87.5k against a 20% cap.
        """
        held = [HeldPosition(position=_position("BIG", market_value=18_000.0), tier="A", margin_of_safety=0.02)]

        assert plan_sells(100_000.0, held, [], cfg=CFG) == []

        sells = plan_sells(87_500.0, held, [], cfg=CFG)
        assert len(sells) == 1
        assert "overweight" in sells[0].reason.lower()
