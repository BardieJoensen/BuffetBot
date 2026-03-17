"""
Tier Engine Module — v2 (S/A/B/C system)

Assigns investment tiers based on business quality and valuation:

    S  — Wonderful business at or below fair value.
         Wide moat + HIGH conviction. Full 3-stage entry.
         You'd hold through a 50% drawdown.

    A  — Good business at or below target entry.
         Solid moat + MEDIUM conviction minimum. 2-stage entry.
         Relying more on valuation discipline than conviction.

    B  — Quality business, price not right yet.
         Same quality criteria as A, but gap to target is < 50%.
         Watch, set price alerts.

    C  — Low conviction, uncertain quality, or extreme premium (>50% gap).
         Monitor passively. Re-evaluate next cycle.

Position sizing:
    S: max 25% of portfolio, 3-stage entries (1/3 each, 5% apart)
    A: max 15% of portfolio, 2-stage entries (1/2 each, 5% apart)
    B: no new buys — price alerts only
    C: no action

Input:
    AnalysisV2 from analyzer (moat_rating, conviction_level, target_entry_price)
    Optional screener_score
    Optional external AggregatedValuation

Output:
    TierAssignment with tier (S/A/B/C), reason, price gap, approaching-target flag
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from .config import config

logger = logging.getLogger(__name__)

# Tier ordering: lower number = better tier (used for movement comparison)
TIER_ORDER: dict[str, int] = {"S": 0, "A": 1, "B": 2, "C": 3}

# Position sizing constraints by tier
TIER_CONFIG: dict[str, dict] = {
    "S": {"max_position_pct": 0.25, "num_tranches": 3, "label": "Wonderful"},
    "A": {"max_position_pct": 0.15, "num_tranches": 2, "label": "Good Enough"},
    "B": {"max_position_pct": 0.00, "num_tranches": 0, "label": "Watch"},
    "C": {"max_position_pct": 0.00, "num_tranches": 0, "label": "Monitor"},
}

# Gap threshold for "approaching target" alerts (within 10% of target entry)
DEFAULT_PROXIMITY_ALERT_PCT: float = config.tier1_proximity_alert_pct

# Gap beyond which a stock is demoted to C regardless of quality (50%)
EXTREME_PREMIUM_THRESHOLD: float = 0.50


def _tier_rank(tier: str) -> int:
    """Lower = better. S=0 is best; unknown tier = 99."""
    return TIER_ORDER.get(tier, 99)


@dataclass
class TierAssignment:
    """Result of tier assignment for a single stock."""

    symbol: str
    tier: str  # S, A, B, or C
    quality_level: str  # "wonderful", "good", "moderate", "low"
    tier_reason: str
    target_entry_price: Optional[float] = None
    current_price: Optional[float] = None
    price_gap_pct: Optional[float] = None  # (current - target) / target; negative = below target
    approaching_target: bool = False  # True when B-tier stock within proximity_alert_pct of target


@dataclass
class WatchlistMovement:
    """A tier change since the last run."""

    symbol: str
    change_type: str  # "new", "removed", "tier_up", "tier_down", "approaching"
    detail: str
    previous_tier: Optional[str] = None
    current_tier: Optional[str] = None


# ─── Assignment Logic ──────────────────────────────────────────────────────


def assign_tier(
    analysis,
    screener_score: float = 0.0,
    external_valuation=None,
    proximity_alert_pct: float = DEFAULT_PROXIMITY_ALERT_PCT,
    quality_score: Optional[float] = None,
) -> TierAssignment:
    """
    Assign S/A/B/C tier based on moat quality and price vs. target entry.

    Decision matrix:
        WIDE moat + HIGH conviction + gap ≤ 0%   → S
        WIDE moat + HIGH conviction + gap < 50%   → B  (wonderful but wait)
        WIDE moat + HIGH conviction + gap ≥ 50%   → C  (extreme premium)
        Any moat + MEDIUM+ conviction + gap ≤ 0%  → A
        Any moat + MEDIUM+ conviction + gap < 50% → B  (good but wait)
        Any moat + MEDIUM+ conviction + gap ≥ 50% → C  (too expensive)
        LOW conviction or NONE moat               → C  (skip)

    When no price data is available, quality stocks default to B (can't
    confirm the price is right, but worth watching).
    """
    moat = analysis.moat_rating.value  # "wide", "narrow", "none"
    conviction = analysis.conviction_level  # "HIGH", "MEDIUM", "LOW"
    symbol = analysis.symbol

    # Resolve target entry price and current price
    target = getattr(analysis, "target_entry_price", None)
    current = getattr(analysis, "current_price", None)

    if target is None and external_valuation:
        avg_fv = external_valuation.average_fair_value
        if avg_fv:
            target = avg_fv * (1 - config.margin_of_safety_pct)

    if current is None and external_valuation:
        current = external_valuation.current_price

    # Compute gap: positive = above target (overpriced), negative = below (buyable)
    gap: Optional[float] = None
    approaching = False
    if target and current and target > 0:
        gap = (current - target) / target
        # approaching_target is meaningful for B-tier: stock is close to becoming buyable
        approaching = 0 < gap <= proximity_alert_pct

    # ── Determine quality level ───────────────────────────────────────────
    if moat == "wide" and conviction == "HIGH":
        quality = "wonderful"
    elif conviction in ("HIGH", "MEDIUM") and moat in ("wide", "narrow"):
        quality = "good"
    elif conviction == "LOW" and moat == "none":
        quality = "low"
    else:
        quality = "moderate"

    # Low quality → always C
    if quality == "low":
        return TierAssignment(
            symbol=symbol,
            tier="C",
            quality_level=quality,
            tier_reason="Low quality: no moat and low conviction — monitor passively",
            target_entry_price=target,
            current_price=current,
            price_gap_pct=gap,
        )

    # Moderate quality (e.g., narrow moat + LOW conviction) → C
    if quality == "moderate":
        return TierAssignment(
            symbol=symbol,
            tier="C",
            quality_level=quality,
            tier_reason=f"Moderate quality: {moat} moat, {conviction} conviction",
            target_entry_price=target,
            current_price=current,
            price_gap_pct=gap,
        )

    # ── Price-aware assignment for "wonderful" and "good" quality ─────────

    if gap is None:
        # No price data — can't confirm the price is right. Watch it.
        return TierAssignment(
            symbol=symbol,
            tier="B",
            quality_level=quality,
            tier_reason=f"{quality.title()} quality ({moat} moat, {conviction}), no price data — watching",
            target_entry_price=target,
            current_price=current,
            price_gap_pct=None,
        )

    # At or below target entry — buy
    if gap <= 0:
        if quality == "wonderful":
            return TierAssignment(
                symbol=symbol,
                tier="S",
                quality_level=quality,
                tier_reason=f"Wonderful business at/below target entry (${current:,.0f} ≤ ${target:,.0f})",
                target_entry_price=target,
                current_price=current,
                price_gap_pct=gap,
                approaching_target=False,
            )
        else:  # "good"
            return TierAssignment(
                symbol=symbol,
                tier="A",
                quality_level=quality,
                tier_reason=f"Good business at/below target entry (${current:,.0f} ≤ ${target:,.0f})",
                target_entry_price=target,
                current_price=current,
                price_gap_pct=gap,
                approaching_target=False,
            )

    # Fair price exception: wonderful business within 10% of target, top-quintile quality.
    # Buffett's evolved principle: moat itself provides margin of safety.
    # Promotes B → A when Wide moat + HIGH conviction + quality_score ≥ 80 + gap ≤ 10%.
    if quality == "wonderful" and 0 < gap <= 0.10 and quality_score is not None and quality_score >= 80:
        return TierAssignment(
            symbol=symbol,
            tier="A",
            quality_level=quality,
            tier_reason=(
                f"Fair price exception: wonderful business {gap:+.1%} above target "
                f"${target:,.0f} (quality score: {quality_score:.0f}) — moat = margin of safety"
            ),
            target_entry_price=target,
            current_price=current,
            price_gap_pct=gap,
            approaching_target=True,
        )

    # Above target but within 50% — watch
    if gap < EXTREME_PREMIUM_THRESHOLD:
        return TierAssignment(
            symbol=symbol,
            tier="B",
            quality_level=quality,
            tier_reason=(f"{quality.title()} quality but {gap:+.0%} above target ${target:,.0f} — watching"),
            target_entry_price=target,
            current_price=current,
            price_gap_pct=gap,
            approaching_target=approaching,
        )

    # Gap ≥ 50% — extreme premium, defer to C
    return TierAssignment(
        symbol=symbol,
        tier="C",
        quality_level=quality,
        tier_reason=(f"{quality.title()} quality but {gap:+.0%} above target — extreme premium, monitor passively"),
        target_entry_price=target,
        current_price=current,
        price_gap_pct=gap,
        approaching_target=False,
    )


# ─── Staged Entry ──────────────────────────────────────────────────────────


def staged_entry_suggestion(
    target_entry_price: float,
    tier: str = "A",
    step_pct: float = 0.05,
) -> list[dict]:
    """
    Generate staged entry price levels.

    S-tier: 3 tranches (1/3 each), 5% apart
    A-tier: 2 tranches (1/2 each), 5% apart

    Example for S-tier at target $175:
        1/3 at $175, 1/3 at $166, 1/3 at $158

    Returns list of dicts with tranche label, price, and allocation fraction.
    """
    num_tranches = TIER_CONFIG.get(tier, {}).get("num_tranches", 2)
    if num_tranches == 0:
        return []  # B/C tiers don't buy

    allocation = 1.0 / num_tranches
    tranches = []
    for i in range(num_tranches):
        price = target_entry_price * (1 - step_pct * i)
        tranches.append(
            {
                "tranche": i + 1,
                "price": round(price, 2),
                "allocation": allocation,
                "label": f"{i + 1}/{num_tranches} at ${price:,.0f}",
            }
        )
    return tranches


# ─── Movement Log ──────────────────────────────────────────────────────────


def compute_movements(
    current_tiers: dict[str, TierAssignment],
    previous_state: dict,
) -> list[WatchlistMovement]:
    """
    Compare current watchlist with previous state and return changes.

    Tier "up" means improvement (e.g., B→A or A→S). Lower TIER_ORDER = better.
    """
    movements = []
    prev_stocks = previous_state.get("stocks", {})

    for symbol, assignment in current_tiers.items():
        if assignment.tier == "C":
            continue  # C-tier not actively tracked in movements

        if symbol not in prev_stocks:
            movements.append(
                WatchlistMovement(
                    symbol=symbol,
                    change_type="new",
                    detail=f"New {assignment.tier}-tier entry",
                    current_tier=assignment.tier,
                )
            )
        else:
            prev_tier = prev_stocks[symbol].get("tier", "C")
            cur_rank = _tier_rank(assignment.tier)
            prv_rank = _tier_rank(prev_tier)

            if cur_rank < prv_rank:
                movements.append(
                    WatchlistMovement(
                        symbol=symbol,
                        change_type="tier_up",
                        detail=f"Upgraded {prev_tier} → {assignment.tier}",
                        previous_tier=prev_tier,
                        current_tier=assignment.tier,
                    )
                )
            elif cur_rank > prv_rank:
                movements.append(
                    WatchlistMovement(
                        symbol=symbol,
                        change_type="tier_down",
                        detail=f"Downgraded {prev_tier} → {assignment.tier}",
                        previous_tier=prev_tier,
                        current_tier=assignment.tier,
                    )
                )

            if assignment.approaching_target and not prev_stocks[symbol].get("approaching", False):
                gap_str = f"{assignment.price_gap_pct:+.0%}" if assignment.price_gap_pct is not None else "?"
                movements.append(
                    WatchlistMovement(
                        symbol=symbol,
                        change_type="approaching",
                        detail=f"Approaching target entry ({gap_str} from target)",
                        current_tier=assignment.tier,
                    )
                )

    # Removed entries (were actively tracked, now C or gone entirely)
    for symbol, prev_data in prev_stocks.items():
        if symbol not in current_tiers or current_tiers[symbol].tier == "C":
            if prev_data.get("tier", "C") != "C":
                movements.append(
                    WatchlistMovement(
                        symbol=symbol,
                        change_type="removed",
                        detail=f"Removed (was {prev_data.get('tier', '?')}-tier)",
                        previous_tier=prev_data.get("tier"),
                    )
                )

    return movements


# ─── State Persistence ────────────────────────────────────────────────────


def load_previous_watchlist(data_dir: Path) -> dict:
    """Load previous watchlist state for movement comparison."""
    path = data_dir / "watchlist_history.json"
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception as e:
            logger.debug("Failed to load previous watchlist: %s", e)
    return {}


def save_watchlist_state(data_dir: Path, current_tiers: dict[str, TierAssignment]) -> None:
    """Save current watchlist state for future comparison."""
    stocks_state: dict[str, dict] = {}
    state: dict = {
        "date": datetime.now().isoformat(),
        "version": 2,
        "stocks": stocks_state,
    }
    for symbol, assignment in current_tiers.items():
        if assignment.tier != "C":
            stocks_state[symbol] = {
                "tier": assignment.tier,
                "target_entry": assignment.target_entry_price,
                "price": assignment.current_price,
                "approaching": assignment.approaching_target,
            }

    path = data_dir / "watchlist_history.json"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2))
        logger.info("Saved watchlist state (%d S/A/B stocks)", len(stocks_state))
    except Exception as e:
        logger.warning("Failed to save watchlist state: %s", e)
