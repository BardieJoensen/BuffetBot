"""
Regime-driven deployment engine — Phase C.

Pure planning logic: given account state, held positions, and a screened
list of buy candidates, decides (a) how much of the account should be
invested for the current market regime, (b) which candidates to buy and
how much, and (c) which held positions to sell to fund a rotation.

No I/O here — no account.buy/sell calls, no DB writes, no network. That
work (and the reasoning-snapshot journaling) stays in scripts/scheduler.py's
weekly_auto_trade, which executes the plans this module returns. Keeping
this module pure makes it fully unit-testable without mocking Alpaca or
SQLite.

Replaces the old flat 25% margin-of-safety hard gate with deploy-to-target
sizing: how much of the account SHOULD be invested is set by market regime
(bubble_detector.classify_market_regime), sized off real buying power.
margin_of_safety demotes from a pass/fail gate to a ranking/sizing tilt —
a quality ceiling (quality_ceiling_pct) still excludes names trading too
far above fair value, but a modest premium no longer disqualifies a high-
tier name the way falling short of 25% undervalued used to.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .accounts.base import AccountState, PositionState
from .config import Config
from .config import config as default_config

# Tier rank: higher = more conviction, used both to rank buy candidates and
# to compare a held position against what else is available. A stock with no
# tier yet (not through Sonnet deep-analysis) ranks between C and B — a fresh
# undervalued find from this week's screen shouldn't be disadvantaged below a
# stock that was already analyzed and downgraded.
TIER_RANK: dict[Optional[str], int] = {"S": 4, "A": 3, "B": 2, None: 1, "C": 0}
TIER_WEIGHT: dict[Optional[str], float] = {"S": 1.0, "A": 0.85, "B": 0.65, None: 0.55, "C": 0.45}

# Regime -> the Config field holding its target invested %.
_REGIME_TARGET_FIELD = {
    "euphoria": "deploy_target_euphoria",
    "overvalued": "deploy_target_overvalued",
    "fair_value": "deploy_target_fair_value",
    "correction": "deploy_target_correction",
    "crisis": "deploy_target_crisis",
}

# Unchanged from the pre-Phase-C take-profit threshold: margin of safety
# below this means the stock has risen to near fair value.
NEAR_FAIR_VALUE_PCT = 0.05


@dataclass
class DeployCandidate:
    """A buy candidate that has already passed the valuation screen."""

    symbol: str
    margin_of_safety: Optional[float]  # (fair_value - price) / fair_value; negative = above fair value
    tier: Optional[str] = None  # S/A/B/C from the latest deep analysis, if any


@dataclass
class HeldPosition:
    """A held position annotated with its current tier and fresh margin of
    safety, for ranking against buy candidates."""

    position: PositionState
    tier: Optional[str] = None
    margin_of_safety: Optional[float] = None  # re-priced at decision time, not the entry margin


@dataclass
class BuyIntent:
    symbol: str
    amount: float
    tier: Optional[str]
    margin_of_safety: Optional[float]


@dataclass
class SellIntent:
    symbol: str
    reason: str


@dataclass
class DeployPlan:
    regime: str
    target_pct: float
    target_value: float
    gap: float
    buys: list[BuyIntent]
    skipped_ceiling: list[str]  # candidates excluded for trading too far above fair value


def _rank_key(tier: Optional[str], margin_of_safety: Optional[float]) -> tuple[int, float]:
    return (TIER_RANK.get(tier, TIER_RANK[None]), margin_of_safety if margin_of_safety is not None else -1.0)


def target_invested_pct(regime: Optional[str], *, cfg: Config = default_config) -> float:
    """
    Target fraction of equity invested for a market regime. Unknown or
    missing regime (classification failed, or a label bubble_detector
    doesn't emit) falls back to fair_value, matching bubble_detector's own
    fallback in _get_regime_info.
    """
    field = _REGIME_TARGET_FIELD.get(regime or "fair_value", "deploy_target_fair_value")
    return getattr(cfg, field)


def plan_buys(
    state: AccountState,
    candidates: list[DeployCandidate],
    regime: Optional[str],
    *,
    current_position_count: int,
    held_symbols: frozenset[str] = frozenset(),
    cfg: Config = default_config,
) -> DeployPlan:
    """
    Decide what to buy this run: deploy toward the regime's target invested
    %, sized off real buying power (not a static PORTFOLIO_VALUE), ranked by
    tier then margin_of_safety. Skips names trading more than
    quality_ceiling_pct above their fair value rather than buying at any
    price; if the target can't be filled with acceptable names, the
    remainder is left as cash (gap simply isn't fully closed this run).
    """
    target_pct = target_invested_pct(regime, cfg=cfg)
    target_value = state.equity * target_pct
    gap = target_value - state.invested_value

    plan = DeployPlan(
        regime=regime or "fair_value",
        target_pct=target_pct,
        target_value=target_value,
        gap=gap,
        buys=[],
        skipped_ceiling=[],
    )

    if gap < cfg.min_trade_usd:
        return plan

    eligible = []
    for c in candidates:
        if c.symbol in held_symbols:
            continue
        # plan_sells treats a held C-tier as a thesis break and always sells
        # it — buying one here would just be churn queued for next week.
        if c.tier == "C":
            continue
        if c.margin_of_safety is not None and c.margin_of_safety < -cfg.quality_ceiling_pct:
            plan.skipped_ceiling.append(c.symbol)
            continue
        eligible.append(c)

    eligible.sort(key=lambda c: _rank_key(c.tier, c.margin_of_safety), reverse=True)

    remaining_gap = gap
    remaining_slots = max(0, cfg.max_positions - current_position_count)
    remaining_power = state.buying_power
    max_position_value = state.equity * cfg.max_position_pct

    for c in eligible:
        if remaining_slots <= 0 or remaining_gap < cfg.min_trade_usd or remaining_power < cfg.min_trade_usd:
            break
        weight = TIER_WEIGHT.get(c.tier, TIER_WEIGHT[None])
        amount = min(max_position_value * weight, remaining_gap, remaining_power)
        if amount < cfg.min_trade_usd:
            continue
        plan.buys.append(BuyIntent(symbol=c.symbol, amount=amount, tier=c.tier, margin_of_safety=c.margin_of_safety))
        remaining_gap -= amount
        remaining_power -= amount
        remaining_slots -= 1

    return plan


def plan_sells(
    equity: float,
    held: list[HeldPosition],
    candidates: list[DeployCandidate],
    *,
    cfg: Config = default_config,
) -> list[SellIntent]:
    """
    Decide what (if anything) to sell.

    A thesis break (current tier downgraded to C) always sells — that's the
    existing news/re-analysis pipeline's own signal that the name no longer
    belongs in the S/A/B watch set. Everything else only sells to rotate:
    a position that has reached fair value sells ONLY if it's also
    overweight (past max_position_pct) or a strictly better-ranked candidate
    is available to take its place. A good name simply hitting fair value,
    with nothing better waiting and no sizing problem, is held — never
    sold to cash.
    """
    held_symbols = {h.position.symbol for h in held}
    open_candidates = [c for c in candidates if c.symbol not in held_symbols]
    best_open_key = max((_rank_key(c.tier, c.margin_of_safety) for c in open_candidates), default=None)

    sells: list[SellIntent] = []
    for h in held:
        pos = h.position

        if h.tier == "C":
            sells.append(SellIntent(symbol=pos.symbol, reason="Thesis breaker: downgraded to C-tier"))
            continue

        near_fair_value = h.margin_of_safety is not None and h.margin_of_safety < NEAR_FAIR_VALUE_PCT
        if not near_fair_value:
            continue

        overweight = equity > 0 and (pos.market_value / equity) > cfg.max_position_pct
        better_candidate_waiting = best_open_key is not None and best_open_key > _rank_key(h.tier, h.margin_of_safety)

        if overweight:
            sells.append(
                SellIntent(
                    symbol=pos.symbol,
                    reason=f"Rotate: overweight at {pos.market_value / equity:.1%} of equity, near fair value",
                )
            )
        elif better_candidate_waiting:
            sells.append(
                SellIntent(symbol=pos.symbol, reason="Rotate: near fair value, better-ranked candidate available")
            )

    return sells
