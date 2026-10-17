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

import math
from dataclasses import dataclass, field
from typing import Optional

from .accounts.base import AccountState, PositionState
from .config import Config
from .config import config as default_config

# Tier rank: higher = more conviction, used to compare held positions and
# already validated S/A buy candidates. B/None/C remain in the map so legacy
# holdings can be ranked for rotation, but they are never eligible to buy.
TIER_RANK: dict[Optional[str], int] = {"S": 4, "A": 3, "B": 2, None: 1, "C": 0}
TIER_WEIGHT: dict[Optional[str], float] = {"S": 1.0, "A": 0.85, "B": 0.65, None: 0.55, "C": 0.45}
BUYABLE_TIERS = frozenset({"S", "A"})

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
    moat: Optional[str] = None  # WIDE/NARROW/NONE from the latest deep analysis, if any
    conviction: Optional[str] = None  # HIGH/MEDIUM/LOW from the latest deep analysis, if any


# Ruleset 3.0.0: names the model rates as having no moat, or that it holds
# with low conviction, are never bought. The tier engine already maps both to
# C; this is defense in depth for callers that build candidates themselves.
# Evidence (2026-10-17 cohort test, 293 first ratings): moat NONE n=22
# averaged -10.5 pp vs SPY with 2 of 22 ahead; LOW conviction n=27 -7.5 pp.
EXCLUDED_MOATS = frozenset({"NONE"})
EXCLUDED_CONVICTIONS = frozenset({"LOW"})


def is_quality_excluded(moat: Optional[str], conviction: Optional[str]) -> bool:
    """True when a candidate's moat or conviction label bars it from buying."""
    m = (moat or "").strip().upper()
    c = (conviction or "").strip().upper()
    return m in EXCLUDED_MOATS or c in EXCLUDED_CONVICTIONS


@dataclass
class HeldPosition:
    """A held position annotated with its current tier and fresh margin of
    safety, for ranking against buy candidates."""

    position: PositionState
    tier: Optional[str] = None
    margin_of_safety: Optional[float] = None  # re-priced at decision time, not the entry margin
    # Ruleset 3.0.0: the only thing that forces a sell is a pre-registered
    # thesis breaker reported as having happened (thesis_breaker_events). A
    # re-analysis that lands on C — scheduled or news-triggered — changes the
    # tier for new buys but never sells on its own; such a holding is only
    # rotated out when something strictly better is waiting. Defaults False so
    # an unannotated caller gets the gentler behaviour.
    thesis_breaker_fired: bool = False


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
    quantity: Optional[float] = None  # None means exit the entire position


@dataclass
class DeployPlan:
    regime: str
    target_pct: float
    target_value: float
    gap: float
    buys: list[BuyIntent]
    skipped_ceiling: list[str]  # candidates excluded for trading too far above fair value
    skipped_quality: list[str] = field(default_factory=list)  # excluded for moat NONE / LOW conviction


@dataclass
class SingleBuyRecommendation:
    """
    The one name to buy next in the real, DKK-lot account (ruleset 3.0.0).

    The real account adds a few thousand kroner at a time, so "deploy to 90%
    invested" is meaningless there; the useful output is a single ranked pick
    and whether one minimum lot is affordable right now.
    """

    symbol: Optional[str]
    tier: Optional[str]
    margin_of_safety: Optional[float]
    lot_dkk: float
    lot_usd: Optional[float]  # None when no FX rate is available
    affordable: Optional[bool]  # None when the available cash is unknown
    slot_available: bool
    reason: str


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
    state_values = (state.equity, state.invested_value, state.buying_power, target_pct)
    if not all(math.isfinite(value) for value in state_values):
        raise ValueError("account state and deployment target must be finite")
    if state.equity <= 0 or state.invested_value < 0 or state.buying_power < 0:
        raise ValueError("account equity must be positive and balances non-negative")
    if not 0 <= target_pct <= 1:
        raise ValueError("deployment target must be between 0 and 1")
    config_values = (cfg.max_position_pct, cfg.min_trade_usd, cfg.quality_ceiling_pct)
    if (
        not all(math.isfinite(value) for value in config_values)
        or not 0 < cfg.max_position_pct <= 1
        or cfg.min_trade_usd <= 0
        or cfg.quality_ceiling_pct < 0
        or cfg.max_positions < 1
    ):
        raise ValueError("position and minimum-trade configuration is invalid")

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
        # Defense in depth: the scheduler applies the same gate before building
        # candidates, but the pure planner must also reject unanalysed/B/C names
        # so another caller cannot bypass the deep-analysis requirement.
        if c.tier not in BUYABLE_TIERS:
            continue
        if is_quality_excluded(c.moat, c.conviction):
            plan.skipped_quality.append(c.symbol)
            continue
        if c.margin_of_safety is None or not math.isfinite(c.margin_of_safety):
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

    A thesis break sells immediately, but only where a thesis existed: the name
    must have fallen to C from a better tier. A first-ever analysis landing on
    C is the bot forming an opinion on something it bought unanalysed, not a
    thesis failing, and treating the two alike produced same-week round trips.
    Those instead stay permanently rotation-eligible — exempt from the
    near-fair-value gate, since a cheap C-rated name would otherwise never
    become sellable by any path.

    Everything else only sells to rotate: a position that has reached fair
    value sells ONLY if it's also overweight (past max_position_pct) or a
    strictly better-ranked candidate is available to take its place. A good
    name simply hitting fair value, with nothing better waiting and no sizing
    problem, is held — never sold to cash.
    """
    held_symbols = {h.position.symbol for h in held}
    open_candidates = [c for c in candidates if c.symbol not in held_symbols and c.tier in BUYABLE_TIERS]
    best_open_key = max((_rank_key(c.tier, c.margin_of_safety) for c in open_candidates), default=None)

    sells: list[SellIntent] = []
    for h in held:
        pos = h.position

        # A quarantined (delisted / non-tradable) holding cannot be sold — the
        # broker rejects the order — so emitting an intent would just produce
        # one failed order every week, forever. It is already excluded from
        # equity, sizing and slot counts upstream, and is surfaced by the
        # quarantine alert rather than here.
        #
        # Checked before the tier=="C" branch on purpose: a delisted name is
        # very likely to also be downgraded to C, which would otherwise fire a
        # thesis-breaker sell.
        if not pos.tradable:
            continue

        # Ruleset 3.0.0: a sell is forced only by a pre-registered thesis
        # breaker that the news monitor found reported as having happened.
        # A downgrade to C by itself — the model changing its mind on a
        # re-analysis — never sells; CTSH was sold at 7% below the bot's own
        # fair value on exactly that path in October 2026.
        if h.thesis_breaker_fired:
            sells.append(SellIntent(symbol=pos.symbol, reason="Thesis breaker fired (pre-registered)"))
            continue

        # C-tier holdings stay eligible for rotation regardless of price.
        # Without this they'd be trapped: the near-fair-value gate below only
        # opens when a position approaches its target, and a C-rated name
        # trading well below fair value would never reach it — so declining to
        # emergency-sell above would silently mean holding it forever.
        near_fair_value = h.margin_of_safety is not None and h.margin_of_safety < NEAR_FAIR_VALUE_PCT
        if not near_fair_value and h.tier != "C":
            continue

        overweight = equity > 0 and (pos.market_value / equity) > cfg.max_position_pct
        better_candidate_waiting = best_open_key is not None and best_open_key > _rank_key(h.tier, h.margin_of_safety)

        if overweight:
            target_value = equity * cfg.max_position_pct
            excess_value = max(0.0, pos.market_value - target_value)
            if pos.price <= 0 or excess_value < cfg.min_trade_usd:
                continue
            trim_quantity = min(pos.shares, excess_value / pos.price)
            sells.append(
                SellIntent(
                    symbol=pos.symbol,
                    reason=f"Rotate: overweight at {pos.market_value / equity:.1%} of equity, near fair value",
                    quantity=trim_quantity,
                )
            )
        elif better_candidate_waiting:
            why = "C-tier on first analysis" if h.tier == "C" else "near fair value"
            sells.append(SellIntent(symbol=pos.symbol, reason=f"Rotate: {why}, better-ranked candidate available"))

    return sells


def recommend_single_buy(
    candidates: list[DeployCandidate],
    *,
    held_symbols: frozenset[str] = frozenset(),
    current_position_count: int = 0,
    cfg: Config = default_config,
    usd_dkk: Optional[float] = None,
    cash_dkk: Optional[float] = None,
) -> SingleBuyRecommendation:
    """
    Rank the buy candidates exactly as plan_buys does and name the single best
    one for the real DKK account, sized as one minimum lot (ruleset 3.0.0).

    Pure: no I/O. ``usd_dkk`` is DKK per 1 USD; ``cash_dkk`` is the cash the
    user has available, or None when unknown.
    """
    lot_dkk = float(cfg.min_lot_dkk)
    lot_usd = lot_dkk / usd_dkk if usd_dkk and math.isfinite(usd_dkk) and usd_dkk > 0 else None
    affordable = None if cash_dkk is None else bool(cash_dkk >= lot_dkk)
    slot_available = current_position_count < cfg.max_positions

    eligible = []
    for c in candidates:
        if c.symbol in held_symbols or c.tier not in BUYABLE_TIERS:
            continue
        if is_quality_excluded(c.moat, c.conviction):
            continue
        if c.margin_of_safety is None or not math.isfinite(c.margin_of_safety):
            continue
        if c.margin_of_safety < -cfg.quality_ceiling_pct:
            continue
        eligible.append(c)
    eligible.sort(key=lambda c: _rank_key(c.tier, c.margin_of_safety), reverse=True)

    if not eligible:
        return SingleBuyRecommendation(
            symbol=None,
            tier=None,
            margin_of_safety=None,
            lot_dkk=lot_dkk,
            lot_usd=lot_usd,
            affordable=affordable,
            slot_available=slot_available,
            reason="No S/A-tier name clears the quality rules this week — hold cash",
        )

    best = eligible[0]
    bits = [f"{best.symbol}: tier {best.tier}"]
    if best.margin_of_safety is not None:
        bits.append(f"{best.margin_of_safety:+.0%} vs fair value")
    bits.append(f"one lot = {lot_dkk:,.0f} DKK" + (f" (~${lot_usd:,.0f})" if lot_usd else ""))
    if not slot_available:
        bits.append(
            f"portfolio full ({current_position_count}/{cfg.max_positions}) — would replace the weakest holding"
        )
    if affordable is False:
        bits.append(f"cash {cash_dkk:,.0f} DKK is below one lot — keep saving")
    return SingleBuyRecommendation(
        symbol=best.symbol,
        tier=best.tier,
        margin_of_safety=best.margin_of_safety,
        lot_dkk=lot_dkk,
        lot_usd=lot_usd,
        affordable=affordable,
        slot_available=slot_available,
        reason="; ".join(bits),
    )
