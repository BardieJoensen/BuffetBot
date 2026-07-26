"""
Regression tests for the screener's candidate sort.

A single stock with a non-finite effective score used to abort the entire
screen: round() raises ValueError on NaN. Because monday_maintenance wrapped
all five of its steps in one try/except, that one row took down the whole job —
fundamentals, price alerts and the Alpaca position mirror all went a month
stale (2026-06-29 to 2026-07-25) while the job reported nothing but a single
error line nobody read.

It was intermittent in a way that made it hard to spot: field-level
sanitisation runs on cache read, so a fresh fetch could hit the NaN while an
immediate re-run of the same tickers succeeded. Monday is the only job that
refetches the full universe, so Monday was the only job that died.
"""

import math
import random
from datetime import datetime

import pytest

from src.screener import ScreenedStock


def _stock(symbol, *, score=50.0, confidence=1.0, market_cap=1e9):
    return ScreenedStock(
        symbol=symbol,
        name=symbol,
        market_cap=market_cap,
        pe_ratio=None,
        debt_equity=None,
        roe=None,
        revenue_growth=None,
        sector="Technology",
        industry="Software",
        screened_at=datetime.now(),
        price=100.0,
        score=score,
        score_confidence=confidence,
    )


def _sort(stocks, seed=0):
    """Mirror of the production sort key."""
    rng = random.Random(seed)

    def key(s):
        score = s.effective_score
        band = -(round(score * 2) / 2) if math.isfinite(score) else math.inf
        cap = -s.market_cap if math.isfinite(s.market_cap or math.nan) else math.inf
        return (band, cap, rng.random())

    return sorted(stocks, key=key)


class TestNonFiniteScores:
    def test_nan_score_does_not_raise(self):
        """The exact production failure: ValueError from round(NaN)."""
        stocks = [_stock("BAD", score=float("nan")), _stock("GOOD", score=80.0)]
        assert [s.symbol for s in _sort(stocks)] == ["GOOD", "BAD"]

    def test_nan_confidence_does_not_raise(self):
        """effective_score is score x confidence — either can poison it."""
        stocks = [_stock("BAD", confidence=float("nan")), _stock("GOOD", score=80.0)]
        assert [s.symbol for s in _sort(stocks)] == ["GOOD", "BAD"]

    def test_infinite_score_does_not_raise(self):
        stocks = [_stock("INF", score=float("inf")), _stock("GOOD", score=80.0)]
        assert [s.symbol for s in _sort(stocks)] == ["GOOD", "INF"]

    def test_nan_market_cap_does_not_raise(self):
        stocks = [_stock("BAD", market_cap=float("nan")), _stock("GOOD")]
        assert len(_sort(stocks)) == 2

    def test_one_bad_row_cannot_discard_the_others(self):
        """
        The property that actually matters. 1,300 good candidates must survive
        one poisoned row.
        """
        stocks = [_stock(f"OK{i}", score=float(i)) for i in range(50)]
        stocks.insert(25, _stock("POISON", score=float("nan")))
        assert len(_sort(stocks)) == 51

    def test_all_bad_rows_still_sort(self):
        stocks = [_stock(f"BAD{i}", score=float("nan")) for i in range(5)]
        assert len(_sort(stocks)) == 5


class TestNormalOrdering:
    """The fix must not disturb the ranking it was bolted onto."""

    def test_higher_score_ranks_first(self):
        stocks = [_stock("LOW", score=10.0), _stock("HIGH", score=90.0)]
        assert [s.symbol for s in _sort(stocks)][0] == "HIGH"

    def test_confidence_weights_the_score(self):
        confident = _stock("CONFIDENT", score=60.0, confidence=1.0)  # effective 60
        unsure = _stock("UNSURE", score=90.0, confidence=0.5)  # effective 45
        assert [s.symbol for s in _sort([unsure, confident])][0] == "CONFIDENT"

    def test_market_cap_breaks_ties_within_a_band(self):
        small = _stock("SMALL", score=50.0, market_cap=1e8)
        large = _stock("LARGE", score=50.0, market_cap=1e11)
        assert [s.symbol for s in _sort([small, large])][0] == "LARGE"

    def test_scores_are_banded_to_half_points(self):
        """50.1 and 50.4 fall in one band, so market cap decides."""
        a = _stock("A", score=50.1, market_cap=1e8)
        b = _stock("B", score=50.4, market_cap=1e11)
        assert [s.symbol for s in _sort([a, b])][0] == "B"

    def test_non_finite_never_outranks_a_real_score(self):
        stocks = [_stock("NAN", score=float("nan")), _stock("TERRIBLE", score=0.0)]
        assert [s.symbol for s in _sort(stocks)] == ["TERRIBLE", "NAN"]


class TestScoreSanitisation:
    """Second line of defence: a NaN score shouldn't reach the sort at all."""

    def test_effective_score_is_nan_when_score_is(self):
        assert not math.isfinite(_stock("X", score=float("nan")).effective_score)

    def test_round_still_raises_on_nan(self):
        """
        Pins the underlying hazard. If a future refactor drops the isfinite
        guard, this documents what comes back.
        """
        with pytest.raises(ValueError, match="NaN"):
            round(float("nan"))


class TestScoreStockNaNHandling:
    """
    Root cause of the crash chain.

    score_stock skipped missing metrics with `if value is None`, but NaN is not
    None — it passed straight into the arithmetic and poisoned total_score.
    _sanitize_numeric_fields could not prevent it: that runs over a fixed
    allowlist of *cached* fields, while seven scored metrics (roic,
    revenue_cagr, roe_consistency, fcf_consistency, margin_stability,
    earnings_consistency, net_share_change) are computed with numpy over
    historical financials and never touch the cache. Sparse history yields NaN
    readily, so any thinly-covered stock could take down the whole screen.
    """

    @pytest.fixture
    def criteria(self):
        from src.screener import load_criteria_from_yaml

        return load_criteria_from_yaml()

    def _score(self, criteria, data):
        from src.screener import score_stock

        return score_stock(data, criteria, sector="Technology", cap_category="large")

    def test_nan_metric_does_not_poison_the_score(self, criteria):
        score, confidence = self._score(criteria, {"roe": 0.20, "roic": float("nan")})
        assert math.isfinite(score)
        assert math.isfinite(confidence)

    def test_nan_metric_is_treated_as_missing(self, criteria):
        """A NaN metric is absent data, not a zero score — same as None."""
        with_nan = self._score(criteria, {"roe": 0.20, "roic": float("nan")})
        without = self._score(criteria, {"roe": 0.20})
        assert with_nan == without

    def test_infinite_metric_is_also_skipped(self, criteria):
        score, _ = self._score(criteria, {"roe": 0.20, "pe_ratio": float("inf")})
        assert math.isfinite(score)

    def test_string_infinity_is_skipped(self, criteria):
        """The original ALHC bug: yfinance returning the literal string."""
        score, _ = self._score(criteria, {"roe": 0.20, "pe_ratio": "Infinity"})
        assert math.isfinite(score)

    def test_every_trend_metric_survives_nan(self, criteria):
        """
        The seven metrics outside the cache-sanitisation allowlist. Any one of
        them was individually capable of aborting the run.
        """
        trend_metrics = [
            "roic",
            "revenue_cagr",
            "roe_consistency",
            "fcf_consistency",
            "margin_stability",
            "earnings_consistency",
            "net_share_change",
        ]
        for metric in trend_metrics:
            score, confidence = self._score(criteria, {"roe": 0.20, metric: float("nan")})
            assert math.isfinite(score), f"{metric} poisoned the score"
            assert math.isfinite(confidence), f"{metric} poisoned the confidence"

    def test_all_metrics_nan_yields_zero_not_nan(self, criteria):
        data = {m: float("nan") for m in ("roe", "roic", "pe_ratio", "operating_margin")}
        score, confidence = self._score(criteria, data)
        assert score == 0.0
        assert confidence == 0.0
