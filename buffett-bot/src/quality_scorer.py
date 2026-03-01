"""
Quality Score Calculator — Percentile-Based Composite Scoring

Assigns each stock a 0–100 quality score by ranking it within the screened
universe across 5 key metrics:

    Score = ROIC_percentile  × 0.30
          + ROE_percentile   × 0.20
          + FCF_yield_pctl   × 0.20
          + Margin_pctl      × 0.15
          + LowDebt_pctl     × 0.15

All inputs are percentile-ranked across the current universe before weighting.
This means "75" always means "top 25% of what we're looking at right now" —
scores are relative to the universe, not absolute thresholds.

Use case: rank stocks top-down so LLM budget is spent on the best candidates,
not on whichever ticker comes first alphabetically.
"""

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# Metric weights — must sum to 1.0
WEIGHTS = {
    "roic": 0.30,
    "roe": 0.20,
    "fcf_yield": 0.20,
    "operating_margin": 0.15,
    "low_debt": 0.15,  # inverted debt_equity: lower debt = higher score
}

assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-9, "Weights must sum to 1.0"


@dataclass
class QualityScore:
    """Quality score and component percentile ranks for a single stock."""

    ticker: str
    score: float  # 0–100 composite
    roic_pct: Optional[float]  # Percentile rank 0–100 (or None if no data)
    roe_pct: Optional[float]
    fcf_yield_pct: Optional[float]
    operating_margin_pct: Optional[float]
    low_debt_pct: Optional[float]
    data_coverage: float  # Fraction of metrics available (0.0–1.0)

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "score": round(self.score, 1),
            "components": {
                "roic_pct": round(self.roic_pct, 1) if self.roic_pct is not None else None,
                "roe_pct": round(self.roe_pct, 1) if self.roe_pct is not None else None,
                "fcf_yield_pct": round(self.fcf_yield_pct, 1) if self.fcf_yield_pct is not None else None,
                "operating_margin_pct": round(self.operating_margin_pct, 1)
                if self.operating_margin_pct is not None
                else None,
                "low_debt_pct": round(self.low_debt_pct, 1) if self.low_debt_pct is not None else None,
            },
            "data_coverage": round(self.data_coverage, 2),
        }


def _percentile_ranks(
    items: list[tuple[str, Optional[float]]],
    ascending: bool = True,
) -> dict[str, float]:
    """
    Compute 0–100 percentile rank for each (ticker, value) pair.

    Items with None values are excluded from ranking and not returned.
    ascending=True: higher value → higher percentile (ROE, ROIC, FCF, margin)
    ascending=False: lower value → higher percentile (debt — less debt = better)

    Returns {ticker: percentile} only for items with non-None values.
    """
    valid = [(t, v) for t, v in items if v is not None]
    if not valid:
        return {}

    n = len(valid)
    if n == 1:
        return {valid[0][0]: 50.0}  # single stock gets median rank

    # Sort: for ascending metrics, sort ascending so rank 0 is worst
    sorted_valid = sorted(valid, key=lambda x: x[1], reverse=not ascending)

    # Assign average rank to ties so identical values don't split 0%/100%.
    # Example: two stocks both with ROE=0.18 → each gets 50%, not 0% and 100%.
    result = {}
    i = 0
    while i < n:
        # Find the run of identical values starting at i
        j = i
        while j < n - 1 and sorted_valid[j][1] == sorted_valid[j + 1][1]:
            j += 1
        # All items i..j are tied — give them the average rank position
        avg_rank = (i + j) / 2.0
        avg_pct = (avg_rank / (n - 1)) * 100.0
        for k in range(i, j + 1):
            result[sorted_valid[k][0]] = avg_pct
        i = j + 1

    return result


def compute_quality_scores(stocks: list) -> dict[str, "QualityScore"]:
    """
    Compute quality scores for a list of ScreenedStock (or any object with
    the expected numeric attributes).

    Accepts objects with these attributes (all Optional[float]):
        roic, roe, real_fcf_yield or fcf_yield, operating_margin, debt_equity

    Returns {ticker: QualityScore} for every stock in the input list.
    Stocks with zero data coverage still appear with score=0.
    """
    if not stocks:
        return {}

    # Extract metric values per ticker
    roic_data: list[tuple[str, Optional[float]]] = []
    roe_data: list[tuple[str, Optional[float]]] = []
    fcf_data: list[tuple[str, Optional[float]]] = []
    margin_data: list[tuple[str, Optional[float]]] = []
    debt_data: list[tuple[str, Optional[float]]] = []  # will be inverted

    for s in stocks:
        t = s.ticker if hasattr(s, "ticker") else s.get("ticker", "UNKNOWN")

        roic_val = _safe_float(getattr(s, "roic", None) if hasattr(s, "roic") else s.get("roic"))
        roe_val = _safe_float(getattr(s, "roe", None) if hasattr(s, "roe") else s.get("roe"))

        # Prefer SBC-adjusted FCF; fall back to standard FCF yield
        fcf_val = _safe_float(
            getattr(s, "real_fcf_yield", None) if hasattr(s, "real_fcf_yield") else s.get("real_fcf_yield")
        )
        if fcf_val is None:
            fcf_val = _safe_float(getattr(s, "fcf_yield", None) if hasattr(s, "fcf_yield") else s.get("fcf_yield"))

        margin_val = _safe_float(
            getattr(s, "operating_margin", None) if hasattr(s, "operating_margin") else s.get("operating_margin")
        )
        debt_val = _safe_float(getattr(s, "debt_equity", None) if hasattr(s, "debt_equity") else s.get("debt_equity"))

        roic_data.append((t, roic_val))
        roe_data.append((t, roe_val))
        fcf_data.append((t, fcf_val))
        margin_data.append((t, margin_val))
        debt_data.append((t, debt_val))

    # Compute percentile ranks — debt uses ascending=False (less = better)
    roic_ranks = _percentile_ranks(roic_data, ascending=True)
    roe_ranks = _percentile_ranks(roe_data, ascending=True)
    fcf_ranks = _percentile_ranks(fcf_data, ascending=True)
    margin_ranks = _percentile_ranks(margin_data, ascending=True)
    low_debt_ranks = _percentile_ranks(debt_data, ascending=False)  # inverted!

    # Assemble per-stock scores
    results: dict[str, QualityScore] = {}
    for s in stocks:
        t = s.ticker if hasattr(s, "ticker") else s.get("ticker", "UNKNOWN")

        roic_pct = roic_ranks.get(t)
        roe_pct = roe_ranks.get(t)
        fcf_pct = fcf_ranks.get(t)
        margin_pct = margin_ranks.get(t)
        low_debt_pct = low_debt_ranks.get(t)

        components = {
            "roic": roic_pct,
            "roe": roe_pct,
            "fcf_yield": fcf_pct,
            "operating_margin": margin_pct,
            "low_debt": low_debt_pct,
        }

        # Weighted sum — skip missing metrics but scale weights to available ones
        available_weight = 0.0
        weighted_sum = 0.0
        for metric, pct in components.items():
            if pct is not None:
                w = WEIGHTS[metric]
                weighted_sum += pct * w
                available_weight += w

        if available_weight > 0:
            # Scale up so partial data doesn't artificially compress scores.
            # A stock with 3/5 metrics available should still compete on the
            # same 0–100 scale as a stock with all 5.
            # weighted_sum is already sum(pct × weight) where pct ∈ [0, 100],
            # so dividing by available_weight keeps the result in [0, 100].
            composite = weighted_sum / available_weight
            data_coverage = available_weight  # fraction of possible weight
        else:
            composite = 0.0
            data_coverage = 0.0

        results[t] = QualityScore(
            ticker=t,
            score=composite,
            roic_pct=roic_pct,
            roe_pct=roe_pct,
            fcf_yield_pct=fcf_pct,
            operating_margin_pct=margin_pct,
            low_debt_pct=low_debt_pct,
            data_coverage=data_coverage,
        )

    return results


def rank_by_quality(stocks: list) -> list:
    """
    Return stocks sorted by quality score descending.

    Attaches a .quality_score attribute to each stock if possible,
    otherwise returns a sorted list of (QualityScore, stock) tuples.

    For ScreenedStock objects (which have a mutable score attribute),
    this updates stock.score directly so downstream code can use it.
    """
    scores = compute_quality_scores(stocks)
    ticker_to_stock = {}
    for s in stocks:
        t = s.ticker if hasattr(s, "ticker") else s.get("ticker", "UNKNOWN")
        ticker_to_stock[t] = s

    sorted_tickers = sorted(scores.keys(), key=lambda t: scores[t].score, reverse=True)
    sorted_stocks = [ticker_to_stock[t] for t in sorted_tickers if t in ticker_to_stock]

    logger.info(
        "Quality scores computed for %d stocks; top 5: %s",
        len(scores),
        [f"{t}={scores[t].score:.0f}" for t in sorted_tickers[:5]],
    )
    return sorted_stocks


# ─── Helpers ──────────────────────────────────────────────────────────────


def _safe_float(value) -> Optional[float]:
    """Convert a value to float, returning None if conversion fails."""
    if value is None:
        return None
    try:
        f = float(value)
        # Guard against inf/nan which would corrupt percentile ranking
        if f != f or abs(f) == float("inf"):
            return None
        return f
    except (TypeError, ValueError):
        return None
