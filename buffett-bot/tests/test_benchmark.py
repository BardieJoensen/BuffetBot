"""
Tests for fetch_benchmark_return — the hold-window resolution behind
per-trade alpha.

Both real-world failures this guards against were silent. yfinance's `end` is
exclusive, so a hold window was scored one bar short; and a boundary landing on
a market holiday produced too few bars, dropping the benchmark entirely. The
journal therefore looked populated while being partly wrong (a trade scored
over the wrong window) and partly empty (a trade with no alpha at all).

yfinance is mocked throughout — no network.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.benchmark import fetch_benchmark_return

# Real SPY closes around 2026-07-04. Note 2026-07-03 is absent: July 4th fell
# on a Saturday, so the holiday was observed Friday the 3rd and the market was
# shut. That gap is what broke the original implementation.
_CLOSES = {
    "2026-06-29": 741.00,
    "2026-06-30": 746.77,
    "2026-07-01": 745.76,
    "2026-07-02": 744.78,
    # 2026-07-03 — market holiday, no bar
    "2026-07-06": 751.28,
    "2026-07-07": 747.71,
    "2026-07-08": 745.40,
    "2026-07-09": 751.71,
    "2026-07-10": 754.95,
}


def _history(closes=None):
    """Build a yfinance-shaped DataFrame indexed by trading day."""
    data = closes if closes is not None else _CLOSES
    idx = pd.to_datetime(sorted(data))
    return pd.DataFrame({"Close": [data[d] for d in sorted(data)]}, index=idx)


def _patched_yf(frame=None):
    ticker = MagicMock()
    ticker.history.return_value = _history() if frame is None else frame
    yf = MagicMock()
    yf.Ticker.return_value = ticker
    return patch.dict("sys.modules", {"yfinance": yf}), ticker


class TestWindowResolution:
    def test_end_date_is_inclusive(self):
        """
        The regression that mis-scored GIS: the exit date's bar must be
        included. 07-02 close -> 07-10 close, not 07-02 -> 07-09.
        """
        ctx, _ = _patched_yf()
        with ctx:
            result = fetch_benchmark_return("2026-07-02", "2026-07-10")

        assert result == pytest.approx((754.95 - 744.78) / 744.78)

    def test_boundary_on_a_market_holiday_resolves_backwards(self):
        """
        The regression that dropped AD's alpha: 07-03 has no bar, so both
        boundaries resolve to the 07-02 close and the window is flat — but it
        must still return a number rather than None.
        """
        ctx, _ = _patched_yf()
        with ctx:
            result = fetch_benchmark_return("2026-07-02", "2026-07-03")

        assert result == pytest.approx(0.0)

    def test_entry_on_a_holiday_resolves_to_the_prior_close(self):
        ctx, _ = _patched_yf()
        with ctx:
            result = fetch_benchmark_return("2026-07-03", "2026-07-10")

        # 07-03 resolves back to 07-02's close.
        assert result == pytest.approx((754.95 - 744.78) / 744.78)

    def test_same_day_round_trip_is_flat(self):
        ctx, _ = _patched_yf()
        with ctx:
            assert fetch_benchmark_return("2026-07-07", "2026-07-07") == pytest.approx(0.0)

    def test_multi_day_window(self):
        ctx, _ = _patched_yf()
        with ctx:
            result = fetch_benchmark_return("2026-07-06", "2026-07-09")

        assert result == pytest.approx((751.71 - 751.28) / 751.28)

    def test_fetches_a_padded_window(self):
        """Padding is what guarantees a bar exists to resolve each boundary."""
        ctx, ticker = _patched_yf()
        with ctx:
            fetch_benchmark_return("2026-07-06", "2026-07-09")

        kwargs = ticker.history.call_args.kwargs
        assert kwargs["start"] < "2026-07-06"
        assert kwargs["end"] > "2026-07-09"

    def test_accepts_full_timestamps(self):
        """decision_log stores 'YYYY-MM-DD HH:MM:SS', not a bare date."""
        ctx, _ = _patched_yf()
        with ctx:
            result = fetch_benchmark_return("2026-07-02 08:59:37", "2026-07-10 18:01:12")

        assert result == pytest.approx((754.95 - 744.78) / 744.78)


class TestDegenerateInputs:
    def test_missing_start_returns_none(self):
        assert fetch_benchmark_return("") is None

    def test_reversed_window_returns_none(self):
        ctx, _ = _patched_yf()
        with ctx:
            assert fetch_benchmark_return("2026-07-10", "2026-07-02") is None

    def test_empty_history_returns_none(self):
        ctx, _ = _patched_yf(pd.DataFrame({"Close": []}, index=pd.to_datetime([])))
        with ctx:
            assert fetch_benchmark_return("2026-07-02", "2026-07-10") is None

    def test_start_before_all_history_returns_none(self):
        """No bar on-or-before the entry means the window can't be anchored."""
        ctx, _ = _patched_yf()
        with ctx:
            assert fetch_benchmark_return("2020-01-01", "2020-01-15") is None

    def test_nan_close_is_ignored(self):
        """
        yfinance emits a placeholder bar for the unsettled current session;
        letting it through propagates NaN into benchmark_return and alpha.
        """
        closes = dict(_CLOSES)
        closes["2026-07-13"] = float("nan")
        ctx, _ = _patched_yf(_history(closes))
        with ctx:
            result = fetch_benchmark_return("2026-07-02", "2026-07-13")

        assert result is not None
        assert result == result  # not NaN
        assert result == pytest.approx((754.95 - 744.78) / 744.78)

    def test_all_nan_history_returns_none(self):
        frame = pd.DataFrame({"Close": [float("nan")]}, index=pd.to_datetime(["2026-07-06"]))
        ctx, _ = _patched_yf(frame)
        with ctx:
            assert fetch_benchmark_return("2026-07-02", "2026-07-10") is None

    def test_yfinance_exception_returns_none(self):
        yf = MagicMock()
        yf.Ticker.side_effect = RuntimeError("network down")
        with patch.dict("sys.modules", {"yfinance": yf}):
            assert fetch_benchmark_return("2026-07-02", "2026-07-10") is None

    def test_zero_first_close_returns_none(self):
        frame = _history({"2026-07-02": 0.0, "2026-07-10": 754.95})
        ctx, _ = _patched_yf(frame)
        with ctx:
            assert fetch_benchmark_return("2026-07-02", "2026-07-10") is None
