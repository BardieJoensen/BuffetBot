"""
Tests for src/fx.py — USD→DKK bridge (Phase 5). No network: yfinance is never
reached because the override and cache paths are exercised, and the live path is
not triggered in these tests.
"""

import json
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

import src.fx as fx


@pytest.fixture
def cache(tmp_path):
    fx.set_fx_cache_dir(tmp_path / "fx")
    yield


def test_override_wins(monkeypatch, cache):
    monkeypatch.setattr(fx, "config", SimpleNamespace(usddkk_override=7.25))
    assert fx.get_usd_dkk_rate() == 7.25


def test_fresh_cache_used(monkeypatch, cache, tmp_path):
    monkeypatch.setattr(fx, "config", SimpleNamespace(usddkk_override=None))
    fx._write_cache(6.80)
    assert fx.get_usd_dkk_rate() == 6.80


def test_stale_cache_is_last_resort(monkeypatch, cache):
    monkeypatch.setattr(fx, "config", SimpleNamespace(usddkk_override=None))
    # Write a cache entry dated well beyond the TTL.
    old = (datetime.now() - timedelta(hours=100)).isoformat()
    fx._cache_dir.mkdir(parents=True, exist_ok=True)
    fx._cache_path().write_text(json.dumps({"rate": 6.5, "fetched_at": old}))
    # Live fetch fails → fall back to the stale cache (no network).
    monkeypatch.setattr(fx, "_fetch_live_rate", lambda: None)
    assert fx.get_usd_dkk_rate() == 6.5


def test_live_fetch_cached(monkeypatch, cache):
    monkeypatch.setattr(fx, "config", SimpleNamespace(usddkk_override=None))
    monkeypatch.setattr(fx, "_fetch_live_rate", lambda: 6.9)
    assert fx.get_usd_dkk_rate() == 6.9
    # Result is now cached → served without another live fetch.
    monkeypatch.setattr(fx, "_fetch_live_rate", lambda: None)
    assert fx.get_usd_dkk_rate() == 6.9


def test_all_sources_fail_returns_none(monkeypatch, cache):
    monkeypatch.setattr(fx, "config", SimpleNamespace(usddkk_override=None))
    monkeypatch.setattr(fx, "_fetch_live_rate", lambda: None)
    assert fx.get_usd_dkk_rate() is None


def test_usd_to_dkk(monkeypatch, cache):
    monkeypatch.setattr(fx, "config", SimpleNamespace(usddkk_override=7.0))
    assert fx.usd_to_dkk(100.0) == 700.0
    assert fx.usd_to_dkk(100.0, rate=6.5) == 650.0
    assert fx.usd_to_dkk(None) is None


def test_usd_to_dkk_none_rate(monkeypatch, cache):
    # When no source resolves the rate, conversion returns None (never fabricated).
    monkeypatch.setattr(fx, "get_usd_dkk_rate", lambda: None)
    assert fx.usd_to_dkk(100.0) is None
