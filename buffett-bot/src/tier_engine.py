"""
Tier Engine Module

Assigns tiers to analyzed stocks based on quality and valuation:
- Tier 1: Wonderful business at or below fair value → consider buying with staged entry
- Tier 2: Wonderful business, currently overpriced → watch and wait for price drop
- Tier 3: Good business worth monitoring → re-evaluate next cycle
- Excluded (tier=0): Low quality → skip

Input:
- AnalysisV2 from analyzer (moat, conviction, target entry price)
- Optional screener score
- Optional external valuation (AggregatedValuation)

Output:
- TierAssignment with tier, reason, price gap, approaching-target flag
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from .config import config

logger = logging.getLogger(__name__)


@dataclass
class TierAssignment:
    """Result of tier assignment for a single stock."""

    symbol: str
    tier: int  # 1, 2, 3, 0=excluded
    quality_level: str  # "high", "moderate", "low"
    tier_reason: str
    target_entry_price: Optional[float] = None
    current_price: Optional[float] = None
    price_gap_pct: Optional[float] = None  # positive = above target
    approaching_target: bool = False


@dataclass
class WatchlistMovement:
    """A change in the watchlist since last run."""

    symbol: str
    change_type: str  # "new", "removed", "tier_up", "tier_down", "approaching"
    detail: str
    previous_tier: Optional[int] = None
    current_tier: Optional[int] = None


def assign_tier(
    analysis,
    screener_score: float = 0.0,
    external_valuation=None,
    proximity_alert_pct: float = config.tier1_proximity_alert_pct,
) -> TierAssignment:
    """
    Assign a tier based on quality assessment and valuation.

    Args:
        analysis: AnalysisV2 (or duck-typed QualitativeAnalysis)
        screener_score: Quantitative score from screener
        external_valuation: AggregatedValuation from valuation module
        proximity_alert_pct: Alert when price is within this % of target
    """
    moat = analysis.moat_rating.value  # wide, narrow, none
    conviction = analysis.conviction_level  # HIGH, MEDIUM, LOW

    # Determine quality level
    if moat in ("wide", "narrow") and conviction in ("HIGH", "MEDIUM"):
        quality = "high"
    elif moat == "none" and conviction == "LOW":
        quality = "low"
    else:
        quality = "moderate"

    # Low quality → excluded
    if quality == "low":
        return TierAssignment(
            symbol=analysis.symbol,
            tier=0,
            quality_level=quality,
            tier_reason="Low quality: weak moat and low conviction",
        )

    # Moderate quality → Tier 3
    if quality == "moderate":
        target = getattr(analysis, "target_entry_price", None)
        current = getattr(analysis, "current_price", None)
        if current is None and external_valuation:
            current = external_valuation.current_price
        return TierAssignment(
            symbol=analysis.symbol,
            tier=3,
            quality_level=quality,
            tier_reason=f"Moderate quality: {moat} moat, {conviction} conviction",
            target_entry_price=target,
            current_price=current,
        )

    # High quality → Tier 1 or 2 based on price vs target
    target = getattr(analysis, "target_entry_price", None)
    current = getattr(analysis, "current_price", None)

    # Fall back to external valuation if analysis lacks values
    if target is None and external_valuation:
        avg_fv = external_valuation.average_fair_value
        if avg_fv:
            mos_pct = config.margin_of_safety_pct
            target = avg_fv * (1 - mos_pct)

    if current is None and external_valuation:
        current = external_valuation.current_price

    price_gap = None
    approaching = False

    if target and current and target > 0:
        price_gap = (current - target) / target
        approaching = 0 < price_gap <= proximity_alert_pct

        if current <= target:
            return TierAssignment(
                symbol=analysis.symbol,
                tier=1,
                quality_level=quality,
                tier_reason=f"High quality at/below target entry (${current:,.0f} <= ${target:,.0f})",
                target_entry_price=target,
                current_price=current,
                price_gap_pct=price_gap,
                approaching_target=False,
            )
        else:
            return TierAssignment(
                symbol=analysis.symbol,
                tier=2,
                quality_level=quality,
                tier_reason=f"High quality but {price_gap:+.0%} above target ${target:,.0f}",
                target_entry_price=target,
                current_price=current,
                price_gap_pct=price_gap,
                approaching_target=approaching,
            )

    # No price data → default to Tier 2 (can't confirm price is right)
    return TierAssignment(
        symbol=analysis.symbol,
        tier=2,
        quality_level=quality,
        tier_reason="High quality, price target unavailable",
        target_entry_price=target,
        current_price=current,
    )


def staged_entry_suggestion(target_entry_price: float, num_tranches: int = 3, step_pct: float = 0.05) -> list[dict]:
    """
    Generate staged entry price levels.

    Returns list of dicts with tranche number, price, and allocation.
    Example: 1/3 at $175, 1/3 at $166, 1/3 at $158
    """
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


# ─────────────────────────────────────────────────────────────
# Movement Log
# ─────────────────────────────────────────────────────────────


def compute_movements(
    current_tiers: dict[str, TierAssignment],
    previous_state: dict,
) -> list[WatchlistMovement]:
    """Compare current watchlist with previous state and return changes."""
    movements = []
    prev_stocks = previous_state.get("stocks", {})

    for symbol, tier in current_tiers.items():
        if tier.tier == 0:
            continue

        if symbol not in prev_stocks:
            movements.append(
                WatchlistMovement(
                    symbol=symbol,
                    change_type="new",
                    detail=f"New Tier {tier.tier} entry",
                    current_tier=tier.tier,
                )
            )
        else:
            prev_tier = prev_stocks[symbol].get("tier", 0)

            if tier.tier < prev_tier:
                movements.append(
                    WatchlistMovement(
                        symbol=symbol,
                        change_type="tier_up",
                        detail=f"Upgraded Tier {prev_tier} -> Tier {tier.tier}",
                        previous_tier=prev_tier,
                        current_tier=tier.tier,
                    )
                )
            elif tier.tier > prev_tier:
                movements.append(
                    WatchlistMovement(
                        symbol=symbol,
                        change_type="tier_down",
                        detail=f"Downgraded Tier {prev_tier} -> Tier {tier.tier}",
                        previous_tier=prev_tier,
                        current_tier=tier.tier,
                    )
                )

            if tier.approaching_target and not prev_stocks[symbol].get("approaching", False):
                movements.append(
                    WatchlistMovement(
                        symbol=symbol,
                        change_type="approaching",
                        detail=f"Approaching target entry ({tier.price_gap_pct:+.0%} from target)"
                        if tier.price_gap_pct
                        else "Approaching target entry price",
                        current_tier=tier.tier,
                    )
                )

    # Removed entries
    for symbol, prev_data in prev_stocks.items():
        if symbol not in current_tiers or current_tiers[symbol].tier == 0:
            movements.append(
                WatchlistMovement(
                    symbol=symbol,
                    change_type="removed",
                    detail=f"Removed (was Tier {prev_data.get('tier', '?')})",
                    previous_tier=prev_data.get("tier"),
                )
            )

    return movements


def load_previous_watchlist(data_dir: Path) -> dict:
    """Load previous watchlist state for movement comparison."""
    path = data_dir / "watchlist_history.json"
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception as e:
            logger.debug(f"Failed to load previous watchlist: {e}")
    return {}


def save_watchlist_state(data_dir: Path, current_tiers: dict[str, TierAssignment]):
    """Save current watchlist state for future comparison."""
    stocks_state: dict[str, dict] = {}
    state: dict[str, object] = {
        "date": datetime.now().isoformat(),
        "stocks": stocks_state,
    }
    for symbol, tier in current_tiers.items():
        if tier.tier > 0:
            stocks_state[symbol] = {
                "tier": tier.tier,
                "target_entry": tier.target_entry_price,
                "price": tier.current_price,
                "approaching": tier.approaching_target,
            }

    path = data_dir / "watchlist_history.json"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2))
        logger.info(f"Saved watchlist state ({len(stocks_state)} stocks)")
    except Exception as e:
        logger.warning(f"Failed to save watchlist state: {e}")
