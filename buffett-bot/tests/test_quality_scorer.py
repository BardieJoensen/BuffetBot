"""
Tests for src/quality_scorer.py — composite quality score calculator.

Critical properties to verify:
- Monotonicity: better raw values → higher percentile rank
- Weight correctness: ROIC (30%) dominates; debt (15%) least influential
- Partial data: stocks with missing metrics still get scored on available ones
- Scale: scores always in 0–100 range
- Edge cases: single stock, all-None values, ties
"""

from dataclasses import dataclass
from typing import Optional

import pytest

from src.quality_scorer import (
    INSIDER_WEIGHT,
    WEIGHTS,
    _percentile_ranks,
    compute_quality_scores,
    rank_by_quality,
)

# ─── Fake Stock Object ────────────────────────────────────────────────────


@dataclass
class FakeStock:
    """Minimal ScreenedStock-compatible object."""

    ticker: str
    roic: Optional[float] = None
    roe: Optional[float] = None
    real_fcf_yield: Optional[float] = None
    fcf_yield: Optional[float] = None
    operating_margin: Optional[float] = None
    debt_equity: Optional[float] = None
    insider_buying: Optional[float] = None


# ─── Weight Validation ────────────────────────────────────────────────────


class TestWeights:
    def test_weights_sum_to_one(self):
        assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-9

    def test_roic_has_highest_weight(self):
        assert WEIGHTS["roic"] == max(WEIGHTS.values())

    def test_low_debt_and_margin_equal_weight(self):
        assert WEIGHTS["low_debt"] == WEIGHTS["operating_margin"]

    def test_expected_weights(self):
        # Fundamental weights are scaled by (1 - INSIDER_WEIGHT); insider is the
        # small tilt added on top so the full set still sums to 1.0.
        scale = 1 - INSIDER_WEIGHT
        assert WEIGHTS["roic"] == pytest.approx(0.30 * scale)
        assert WEIGHTS["roe"] == pytest.approx(0.20 * scale)
        assert WEIGHTS["fcf_yield"] == pytest.approx(0.20 * scale)
        assert WEIGHTS["operating_margin"] == pytest.approx(0.15 * scale)
        assert WEIGHTS["low_debt"] == pytest.approx(0.15 * scale)
        assert WEIGHTS["insider_buying"] == pytest.approx(INSIDER_WEIGHT)


# ─── _percentile_ranks() ──────────────────────────────────────────────────


class TestPercentileRanks:
    def test_highest_value_gets_100(self):
        items = [("A", 10.0), ("B", 20.0), ("C", 30.0)]
        ranks = _percentile_ranks(items, ascending=True)
        assert ranks["C"] == pytest.approx(100.0)

    def test_lowest_value_gets_0(self):
        items = [("A", 10.0), ("B", 20.0), ("C", 30.0)]
        ranks = _percentile_ranks(items, ascending=True)
        assert ranks["A"] == pytest.approx(0.0)

    def test_middle_value_gets_50(self):
        items = [("A", 10.0), ("B", 20.0), ("C", 30.0)]
        ranks = _percentile_ranks(items, ascending=True)
        assert ranks["B"] == pytest.approx(50.0)

    def test_descending_inverts_ranks(self):
        """Low debt = better → highest rank for lowest value."""
        items = [("A", 0.1), ("B", 0.5), ("C", 2.0)]
        ranks = _percentile_ranks(items, ascending=False)
        assert ranks["A"] == pytest.approx(100.0)   # lowest debt → best
        assert ranks["C"] == pytest.approx(0.0)     # highest debt → worst

    def test_none_values_excluded(self):
        items = [("A", None), ("B", 50.0), ("C", 100.0)]
        ranks = _percentile_ranks(items, ascending=True)
        assert "A" not in ranks
        assert "B" in ranks
        assert "C" in ranks

    def test_single_item_gets_50(self):
        items = [("ONLY", 42.0)]
        ranks = _percentile_ranks(items, ascending=True)
        assert ranks["ONLY"] == pytest.approx(50.0)

    def test_empty_list_returns_empty(self):
        ranks = _percentile_ranks([], ascending=True)
        assert ranks == {}

    def test_all_none_returns_empty(self):
        items = [("A", None), ("B", None)]
        ranks = _percentile_ranks(items, ascending=True)
        assert ranks == {}

    def test_monotonically_increasing(self):
        """More items: verify ranks are strictly ordered."""
        items = [(f"S{i}", float(i * 10)) for i in range(10)]
        ranks = _percentile_ranks(items, ascending=True)
        sorted_pcts = [ranks[f"S{i}"] for i in range(10)]
        assert sorted_pcts == sorted(sorted_pcts)

    def test_ties_handled_gracefully(self):
        """Tied values can share a rank — just don't crash."""
        items = [("A", 50.0), ("B", 50.0), ("C", 100.0)]
        ranks = _percentile_ranks(items, ascending=True)
        assert all(0.0 <= v <= 100.0 for v in ranks.values())


# ─── compute_quality_scores() ─────────────────────────────────────────────


class TestComputeQualityScores:
    def test_returns_all_tickers(self):
        stocks = [
            FakeStock("A", roic=0.20, roe=0.18),
            FakeStock("B", roic=0.10, roe=0.09),
        ]
        scores = compute_quality_scores(stocks)
        assert set(scores.keys()) == {"A", "B"}

    def test_empty_list_returns_empty(self):
        assert compute_quality_scores([]) == {}

    def test_scores_in_0_to_100_range(self):
        stocks = [
            FakeStock("HIGH", roic=0.35, roe=0.30, real_fcf_yield=0.05, operating_margin=0.25, debt_equity=0.1),
            FakeStock("LOW", roic=0.05, roe=0.06, real_fcf_yield=0.01, operating_margin=0.05, debt_equity=2.0),
            FakeStock("MID", roic=0.15, roe=0.12, real_fcf_yield=0.03, operating_margin=0.15, debt_equity=0.5),
        ]
        scores = compute_quality_scores(stocks)
        for ticker, qs in scores.items():
            assert 0.0 <= qs.score <= 100.0, f"{ticker} score {qs.score} out of range"

    def test_best_stock_has_highest_score(self):
        """Stock with best values across all metrics should rank first."""
        stocks = [
            FakeStock("BEST", roic=0.40, roe=0.35, real_fcf_yield=0.08, operating_margin=0.30, debt_equity=0.0),
            FakeStock("WORST", roic=0.02, roe=0.03, real_fcf_yield=0.01, operating_margin=0.02, debt_equity=3.0),
        ]
        scores = compute_quality_scores(stocks)
        assert scores["BEST"].score > scores["WORST"].score

    def test_high_debt_hurts_score(self):
        """Two otherwise identical stocks: lower debt should score higher."""
        low_debt = FakeStock("LOW_DEBT", roic=0.20, roe=0.18, debt_equity=0.1)
        high_debt = FakeStock("HIGH_DEBT", roic=0.20, roe=0.18, debt_equity=3.0)
        scores = compute_quality_scores([low_debt, high_debt])
        assert scores["LOW_DEBT"].score > scores["HIGH_DEBT"].score

    def test_partial_data_still_scores(self):
        """Stock with only ROIC available should still get a non-zero score."""
        stocks = [
            FakeStock("PARTIAL", roic=0.25),
            FakeStock("FULL", roic=0.20, roe=0.18, real_fcf_yield=0.04, operating_margin=0.20, debt_equity=0.3),
        ]
        scores = compute_quality_scores(stocks)
        assert scores["PARTIAL"].score > 0.0
        assert scores["PARTIAL"].data_coverage < 1.0

    def test_all_none_gives_zero_score(self):
        stocks = [
            FakeStock("EMPTY"),  # all metrics are None
            FakeStock("REAL", roic=0.15, roe=0.12),
        ]
        scores = compute_quality_scores(stocks)
        assert scores["EMPTY"].score == pytest.approx(0.0)
        assert scores["EMPTY"].data_coverage == pytest.approx(0.0)

    def test_single_stock_midpoint_score(self):
        """Single stock with all metrics should get 50.0 (median of universe=1)."""
        stocks = [FakeStock("SOLO", roic=0.20, roe=0.15, real_fcf_yield=0.04,
                             operating_margin=0.18, debt_equity=0.5)]
        scores = compute_quality_scores(stocks)
        # Single stock: every percentile rank = 50, so composite = 50
        assert scores["SOLO"].score == pytest.approx(50.0)

    def test_fcf_yield_fallback(self):
        """If real_fcf_yield is None, fall back to fcf_yield."""
        stocks = [
            FakeStock("A", roic=0.20, fcf_yield=0.05),   # uses fcf_yield fallback
            FakeStock("B", roic=0.10, fcf_yield=0.02),
        ]
        scores = compute_quality_scores(stocks)
        # A has better fcf_yield, should have higher fcf_yield_pct
        assert scores["A"].fcf_yield_pct > scores["B"].fcf_yield_pct

    def test_roic_dominates_ranking(self):
        """ROIC (30% weight) should be the most influential metric."""
        # Stock A: only ROIC advantage, everything else is same/worse
        # Stock B: only debt advantage
        stocks = [
            FakeStock("ROIC_KING", roic=0.40, roe=0.10, debt_equity=2.0),
            FakeStock("LOW_DEBT", roic=0.10, roe=0.10, debt_equity=0.01),
        ]
        scores = compute_quality_scores(stocks)
        # ROIC weight 30% > debt weight 15%, so ROIC_KING should win
        assert scores["ROIC_KING"].score > scores["LOW_DEBT"].score

    def test_data_coverage_field(self):
        # All five fundamentals + insider → full coverage = 1.0
        stocks = [
            FakeStock("FULL", roic=0.20, roe=0.15, real_fcf_yield=0.04,
                      operating_margin=0.18, debt_equity=0.5, insider_buying=2),
        ]
        scores = compute_quality_scores(stocks)
        assert scores["FULL"].data_coverage == pytest.approx(1.0)

    def test_full_fundamentals_without_insider_coverage(self):
        # All five fundamentals but no insider data → coverage = 1 - INSIDER_WEIGHT
        stocks = [
            FakeStock("FUND", roic=0.20, roe=0.15, real_fcf_yield=0.04,
                      operating_margin=0.18, debt_equity=0.5),
        ]
        scores = compute_quality_scores(stocks)
        assert scores["FUND"].data_coverage == pytest.approx(1 - INSIDER_WEIGHT)
        assert scores["FUND"].insider_buying_pct is None

    def test_partial_data_coverage(self):
        """With only 2 metrics, coverage = their combined (scaled) weight."""
        stocks = [
            FakeStock("PARTIAL", roic=0.20, roe=0.15),
            FakeStock("OTHER", roic=0.10, roe=0.08),
        ]
        scores = compute_quality_scores(stocks)
        # coverage = (ROIC + ROE) base weight × (1 - INSIDER_WEIGHT)
        assert scores["PARTIAL"].data_coverage == pytest.approx(0.50 * (1 - INSIDER_WEIGHT))


# ─── Insider buying tilt (Phase 3) ──────────────────────────────────────────


class TestInsiderTilt:
    def _identical_pair(self, insider_a, insider_b):
        """Two stocks identical on fundamentals, differing only on insider data."""
        return [
            FakeStock("A", roic=0.20, roe=0.15, real_fcf_yield=0.04,
                      operating_margin=0.18, debt_equity=0.5, insider_buying=insider_a),
            FakeStock("B", roic=0.20, roe=0.15, real_fcf_yield=0.04,
                      operating_margin=0.18, debt_equity=0.5, insider_buying=insider_b),
        ]

    def test_cluster_buying_ranks_higher(self):
        # Same fundamentals; A has a strong net-buying cluster, B has net selling.
        scores = compute_quality_scores(self._identical_pair(5, -3))
        assert scores["A"].score > scores["B"].score
        assert scores["A"].insider_buying_pct > scores["B"].insider_buying_pct

    def test_no_insider_coverage_unaffected(self):
        # A stock with no insider data must score exactly as it would have under
        # the original fundamental-only weighting (others here also lack insider).
        fundamentals_only = [
            FakeStock("X", roic=0.20, roe=0.15, real_fcf_yield=0.04,
                      operating_margin=0.18, debt_equity=0.5),
            FakeStock("Y", roic=0.10, roe=0.08, real_fcf_yield=0.02,
                      operating_margin=0.10, debt_equity=1.0),
        ]
        scores = compute_quality_scores(fundamentals_only)
        # X dominates Y on every metric → X gets the top percentile composite.
        assert scores["X"].score == pytest.approx(100.0)
        assert scores["X"].insider_buying_pct is None

    def test_tilt_is_small(self):
        # The insider component must not swamp fundamentals: a stock that is best
        # on fundamentals but worst on insider still beats a fundamentals-laggard.
        stocks = [
            FakeStock("STRONG", roic=0.30, roe=0.25, real_fcf_yield=0.06,
                      operating_margin=0.25, debt_equity=0.2, insider_buying=-5),
            FakeStock("WEAK", roic=0.05, roe=0.03, real_fcf_yield=0.01,
                      operating_margin=0.05, debt_equity=2.0, insider_buying=10),
        ]
        scores = compute_quality_scores(stocks)
        assert scores["STRONG"].score > scores["WEAK"].score


# ─── rank_by_quality() ────────────────────────────────────────────────────


class TestRankByQuality:
    def test_returns_all_stocks(self):
        stocks = [FakeStock("A", roic=0.20), FakeStock("B", roic=0.10)]
        ranked = rank_by_quality(stocks)
        assert len(ranked) == len(stocks)

    def test_best_stock_first(self):
        stocks = [
            FakeStock("WORST", roic=0.05),
            FakeStock("BEST", roic=0.40),
            FakeStock("MID", roic=0.20),
        ]
        ranked = rank_by_quality(stocks)
        assert ranked[0].ticker == "BEST"

    def test_order_is_descending_by_score(self):
        stocks = [
            FakeStock("C", roic=0.05),
            FakeStock("A", roic=0.40),
            FakeStock("B", roic=0.20),
        ]
        ranked = rank_by_quality(stocks)
        tickers = [s.ticker for s in ranked]
        assert tickers == ["A", "B", "C"]

    def test_empty_input_returns_empty(self):
        assert rank_by_quality([]) == []


# ─── nan/inf Guard ────────────────────────────────────────────────────────


class TestNanInfGuard:
    def test_inf_treated_as_none(self):
        """Infinite values should not corrupt percentile ranking."""
        stocks = [
            FakeStock("INF", roic=float("inf")),
            FakeStock("REAL", roic=0.20),
        ]
        scores = compute_quality_scores(stocks)
        # INF stock should get 0 data coverage for the inf metric
        # (it gets excluded from percentile ranking)
        assert 0.0 <= scores["INF"].score <= 100.0

    def test_nan_treated_as_none(self):
        """NaN values should not corrupt percentile ranking."""
        stocks = [
            FakeStock("NAN", roic=float("nan")),
            FakeStock("REAL", roic=0.15),
        ]
        scores = compute_quality_scores(stocks)
        assert 0.0 <= scores["NAN"].score <= 100.0
