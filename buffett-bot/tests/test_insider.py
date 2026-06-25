"""
Tests for src/insider.py — insider buying signal (Phase 3).

The Finnhub HTTP layer is mocked; no network access. Covers the net-buyers
computation (distinct people, window filtering, code P/S), disk caching, and
graceful degradation when no API key is configured.
"""

from datetime import datetime, timedelta

import pytest

import src.insider as insider


def _recent(days_ago: int) -> str:
    return (datetime.now() - timedelta(days=days_ago)).date().isoformat()


@pytest.fixture
def cache(tmp_path):
    insider.set_insider_cache_dir(tmp_path / "insider")
    yield


# ─── _net_buyers ──────────────────────────────────────────────────────────────


class TestNetBuyers:
    def test_distinct_people_not_transactions(self):
        # Alice buys 3 times, Bob buys once, Carol sells once.
        txns = [
            {"name": "Alice", "transactionCode": "P", "transactionDate": _recent(10)},
            {"name": "Alice", "transactionCode": "P", "transactionDate": _recent(9)},
            {"name": "Alice", "transactionCode": "P", "transactionDate": _recent(8)},
            {"name": "Bob", "transactionCode": "P", "transactionDate": _recent(7)},
            {"name": "Carol", "transactionCode": "S", "transactionDate": _recent(6)},
        ]
        # 2 distinct buyers - 1 distinct seller = +1
        assert insider._net_buyers(txns, 6) == 1.0

    def test_window_excludes_old_transactions(self):
        txns = [
            {"name": "Alice", "transactionCode": "P", "transactionDate": _recent(5)},
            {"name": "Bob", "transactionCode": "P", "transactionDate": _recent(400)},
        ]
        assert insider._net_buyers(txns, 6) == 1.0  # Bob's 400-day-old buy excluded

    def test_ignores_non_open_market_codes(self):
        txns = [
            {"name": "Alice", "transactionCode": "A", "transactionDate": _recent(5)},  # award
            {"name": "Bob", "transactionCode": "M", "transactionDate": _recent(5)},  # exercise
        ]
        assert insider._net_buyers(txns, 6) == 0.0

    def test_net_negative_on_selling_cluster(self):
        txns = [
            {"name": "A", "transactionCode": "S", "transactionDate": _recent(5)},
            {"name": "B", "transactionCode": "S", "transactionDate": _recent(5)},
            {"name": "C", "transactionCode": "P", "transactionDate": _recent(5)},
        ]
        assert insider._net_buyers(txns, 6) == -1.0


# ─── get_insider_buying_signal ──────────────────────────────────────────────


class TestSignal:
    def test_none_without_key(self, cache, monkeypatch):
        monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
        assert insider.get_insider_buying_signal("AAPL") is None

    def test_happy_path_and_cache(self, cache, monkeypatch):
        calls = {"n": 0}
        txns = [{"name": "Alice", "transactionCode": "P", "transactionDate": _recent(5)}]

        def fake_fetch(symbol, key):
            calls["n"] += 1
            return txns

        monkeypatch.setattr(insider, "_fetch_transactions", fake_fetch)
        sig = insider.get_insider_buying_signal("AAPL", finnhub_key="k")
        assert sig == 1.0
        # Second call served from disk cache → no extra fetch.
        assert insider.get_insider_buying_signal("AAPL", finnhub_key="k") == 1.0
        assert calls["n"] == 1

    def test_none_on_fetch_failure(self, cache, monkeypatch):
        monkeypatch.setattr(insider, "_fetch_transactions", lambda s, k: None)
        assert insider.get_insider_buying_signal("AAPL", finnhub_key="k") is None

    def test_none_on_empty_data(self, cache, monkeypatch):
        monkeypatch.setattr(insider, "_fetch_transactions", lambda s, k: [])
        assert insider.get_insider_buying_signal("AAPL", finnhub_key="k") is None


# ─── fetch_insider_signals (batch) ──────────────────────────────────────────


class TestBatch:
    def test_respects_limit(self, cache, monkeypatch):
        seen = []

        def fake_get(symbol, **kwargs):
            seen.append(symbol)
            return 1.0

        monkeypatch.setattr(insider, "get_insider_buying_signal", fake_get)
        out = insider.fetch_insider_signals(["A", "B", "C", "D"], limit=2, finnhub_key="k")
        assert len(seen) == 2
        assert out == {"A": 1.0, "B": 1.0}

    def test_empty_without_key(self, cache, monkeypatch):
        monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
        assert insider.fetch_insider_signals(["A", "B"]) == {}
