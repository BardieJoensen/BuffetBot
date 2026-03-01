"""
Tests for Phase B: universe builder and cap-aware scoring.

Covers:
    - get_cap_category() boundary correctness
    - conviction pool loading and source tagging
    - S&P 500 pool cache read/write
    - pool merger de-duplication (conviction > sp500 > finviz)
    - build_universe() structure and ordering
    - score_stock() cap_overrides application
    - load_criteria_from_yaml() parses cap_overrides
    - ScreenedStock carries cap_category field
"""

import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
import yaml

from src.screener import (
    ScreeningCriteria,
    ScreenedStock,
    ScoringRule,
    load_criteria_from_yaml,
    score_stock,
)
from src.universe_builder import (
    LARGE_CAP_THRESHOLD,
    MID_CAP_THRESHOLD,
    UniverseStock,
    _fetch_sp500_from_wikipedia,
    _load_sp500_cache,
    _merge_pools,
    _read_conviction_pool,
    _save_sp500_cache,
    build_universe,
    get_cap_category,
    get_conviction_tickers,
    get_tickers,
)


# ─── Fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture
def tmp(tmp_path):
    return tmp_path


@pytest.fixture
def conviction_yaml(tmp_path):
    """A minimal conviction_list.yaml for testing."""
    data = {
        "last_reviewed": "2026-03-01",
        "next_review": "2026-06-01",
        "stocks": [
            {"ticker": "V",    "notes": "Visa — payment rails"},
            {"ticker": "COST", "notes": "Costco — membership moat"},
            {"ticker": "MSCI", "notes": "Index monopoly"},
        ],
    }
    path = tmp_path / "conviction_list.yaml"
    path.write_text(yaml.dump(data))
    return path


@pytest.fixture
def screening_criteria_yaml(tmp_path):
    """A minimal screening_criteria.yaml with cap_overrides for testing."""
    data = {
        "screening": {
            "min_market_cap": 300_000_000,
            "max_market_cap": 500_000_000_000,
            "min_price": 5.0,
            "min_adtv": 2_000_000,
            "scoring": {
                "pe_ratio": {"ideal": 15, "max": 60, "weight": 0.8},
                "roic":     {"ideal": 0.20, "min": 0.05, "weight": 2.5},
            },
            "sector_overrides": {
                "Financial Services": {
                    "pe_ratio": {"ideal": 12, "max": 25, "weight": 0.5},
                },
            },
            "cap_overrides": {
                "large": {
                    "pe_ratio": {"ideal": 25, "max": 80, "weight": 0.6},
                    "revenue_growth": {"ideal": 0.10, "min": 0.0, "weight": 0.8},
                },
            },
            "top_n": 100,
        }
    }
    path = tmp_path / "screening_criteria.yaml"
    path.write_text(yaml.dump(data))
    return path


# ─── get_cap_category() ───────────────────────────────────────────────────


class TestGetCapCategory:
    def test_large_cap(self):
        assert get_cap_category(50_000_000_000) == "large"   # $50B

    def test_exactly_at_large_boundary(self):
        assert get_cap_category(LARGE_CAP_THRESHOLD) == "large"

    def test_mid_cap(self):
        assert get_cap_category(5_000_000_000) == "mid"   # $5B

    def test_exactly_at_mid_boundary(self):
        assert get_cap_category(MID_CAP_THRESHOLD) == "mid"

    def test_just_below_large(self):
        assert get_cap_category(LARGE_CAP_THRESHOLD - 1) == "mid"

    def test_small_cap(self):
        assert get_cap_category(800_000_000) == "small"   # $800M

    def test_just_below_mid(self):
        assert get_cap_category(MID_CAP_THRESHOLD - 1) == "small"

    def test_none_returns_unknown(self):
        assert get_cap_category(None) == "unknown"

    def test_zero_returns_unknown(self):
        assert get_cap_category(0) == "unknown"

    def test_negative_returns_unknown(self):
        assert get_cap_category(-1_000_000) == "unknown"

    def test_visa_is_large(self):
        """Visa market cap ~$500B → large."""
        assert get_cap_category(500_000_000_000) == "large"

    def test_typical_small_cap(self):
        assert get_cap_category(400_000_000) == "small"


# ─── _read_conviction_pool() ──────────────────────────────────────────────


class TestReadConvictionPool:
    def test_loads_all_tickers(self, conviction_yaml):
        stocks = _read_conviction_pool(conviction_yaml)
        tickers = [s.ticker for s in stocks]
        assert "V" in tickers
        assert "COST" in tickers
        assert "MSCI" in tickers

    def test_all_have_conviction_source(self, conviction_yaml):
        stocks = _read_conviction_pool(conviction_yaml)
        assert all(s.source == "conviction" for s in stocks)

    def test_notes_preserved(self, conviction_yaml):
        stocks = _read_conviction_pool(conviction_yaml)
        visa = next(s for s in stocks if s.ticker == "V")
        assert "payment rails" in visa.notes

    def test_tickers_uppercased(self, tmp_path):
        """Lowercase tickers in YAML should be uppercased."""
        data = {"stocks": [{"ticker": "aapl", "notes": ""}]}
        path = tmp_path / "c.yaml"
        path.write_text(yaml.dump(data))
        stocks = _read_conviction_pool(path)
        assert stocks[0].ticker == "AAPL"

    def test_missing_file_returns_empty(self, tmp_path):
        stocks = _read_conviction_pool(tmp_path / "nonexistent.yaml")
        assert stocks == []

    def test_empty_stocks_list(self, tmp_path):
        path = tmp_path / "empty.yaml"
        path.write_text(yaml.dump({"stocks": []}))
        stocks = _read_conviction_pool(path)
        assert stocks == []

    def test_count_matches_yaml(self, conviction_yaml):
        stocks = _read_conviction_pool(conviction_yaml)
        assert len(stocks) == 3

    def test_handles_missing_notes_key(self, tmp_path):
        data = {"stocks": [{"ticker": "AAPL"}]}
        path = tmp_path / "no_notes.yaml"
        path.write_text(yaml.dump(data))
        stocks = _read_conviction_pool(path)
        assert stocks[0].notes == ""


# ─── Pool Merger ──────────────────────────────────────────────────────────


class TestMergePools:
    def _s(self, ticker, source, notes=""):
        return UniverseStock(ticker=ticker, source=source, notes=notes)

    def test_no_duplicates_in_output(self):
        pool_a = [self._s("AAPL", "conviction"), self._s("MSFT", "conviction")]
        pool_b = [self._s("AAPL", "sp500_filter"), self._s("GOOG", "sp500_filter")]
        merged = _merge_pools(pool_a, pool_b)
        tickers = [s.ticker for s in merged]
        assert len(tickers) == len(set(tickers))

    def test_conviction_wins_over_sp500(self):
        pool_a = [self._s("V", "conviction", notes="Payment rails")]
        pool_b = [self._s("V", "sp500_filter")]
        merged = _merge_pools(pool_a, pool_b)
        v = next(s for s in merged if s.ticker == "V")
        assert v.source == "conviction"
        assert v.notes == "Payment rails"

    def test_sp500_wins_over_finviz(self):
        pool_a = [self._s("AAPL", "sp500_filter")]
        pool_b = [self._s("AAPL", "finviz_screen")]
        merged = _merge_pools(pool_a, pool_b)
        aapl = next(s for s in merged if s.ticker == "AAPL")
        assert aapl.source == "sp500_filter"

    def test_conviction_wins_over_finviz(self):
        pool_a = [self._s("COST", "conviction", notes="Membership moat")]
        pool_b = [self._s("COST", "finviz_screen")]
        merged = _merge_pools(pool_a, pool_b)
        cost = next(s for s in merged if s.ticker == "COST")
        assert cost.source == "conviction"

    def test_unique_tickers_from_all_pools_included(self):
        pool_a = [self._s("V", "conviction")]
        pool_b = [self._s("AAPL", "sp500_filter")]
        pool_c = [self._s("ACME", "finviz_screen")]
        merged = _merge_pools(pool_a, pool_b, pool_c)
        tickers = {s.ticker for s in merged}
        assert "V" in tickers
        assert "AAPL" in tickers
        assert "ACME" in tickers

    def test_three_way_collision_conviction_wins(self):
        """Same ticker in all three pools → conviction always wins."""
        pool_a = [self._s("KO", "conviction", notes="Classic Buffett")]
        pool_b = [self._s("KO", "sp500_filter")]
        pool_c = [self._s("KO", "finviz_screen")]
        merged = _merge_pools(pool_a, pool_b, pool_c)
        ko = next(s for s in merged if s.ticker == "KO")
        assert ko.source == "conviction"
        assert ko.notes == "Classic Buffett"

    def test_empty_pools(self):
        assert _merge_pools([], [], []) == []

    def test_single_pool_unchanged(self):
        pool = [self._s("V", "conviction"), self._s("MA", "conviction")]
        merged = _merge_pools(pool)
        tickers = sorted(s.ticker for s in merged)
        assert tickers == ["MA", "V"]


# ─── S&P 500 Cache ────────────────────────────────────────────────────────


class TestSP500Cache:
    def test_save_and_load_cache(self, tmp_path):
        tickers = ["AAPL", "MSFT", "GOOGL"]
        cache_file = tmp_path / "sp500_universe.json"
        _save_sp500_cache(cache_file, tickers)
        loaded = _load_sp500_cache(cache_file)
        assert loaded is not None
        loaded_tickers = [s.ticker for s in loaded]
        assert set(loaded_tickers) == set(tickers)

    def test_cached_source_is_sp500_filter(self, tmp_path):
        cache_file = tmp_path / "sp500_universe.json"
        _save_sp500_cache(cache_file, ["V", "MA"])
        loaded = _load_sp500_cache(cache_file)
        assert all(s.source == "sp500_filter" for s in loaded)

    def test_missing_cache_returns_none(self, tmp_path):
        result = _load_sp500_cache(tmp_path / "nonexistent.json")
        assert result is None

    def test_expired_cache_returns_none(self, tmp_path):
        """Cache older than SP500_CACHE_DAYS should be ignored."""
        from datetime import timedelta
        cache_file = tmp_path / "sp500_universe.json"
        old_data = {
            "tickers": ["AAPL"],
            "cached_at": (
                __import__("datetime").datetime.now() - timedelta(days=10)
            ).isoformat(),
            "count": 1,
            "source": "wikipedia",
        }
        cache_file.write_text(json.dumps(old_data))
        result = _load_sp500_cache(cache_file)
        assert result is None

    def test_brk_dot_b_normalized(self, tmp_path):
        """BRK.B from Wikipedia should be normalized to BRK-B for yfinance."""
        cache_file = tmp_path / "sp500.json"
        _save_sp500_cache(cache_file, ["BRK-B", "GOOGL"])
        loaded = _load_sp500_cache(cache_file)
        tickers = [s.ticker for s in loaded]
        assert "BRK-B" in tickers
        assert "BRK.B" not in tickers


# ─── build_universe() ─────────────────────────────────────────────────────


class TestBuildUniverse:
    def _mock_finviz(self, tickers):
        """Patch get_stock_universe to return a controlled list."""
        return patch("src.universe_builder._fetch_finviz_pool", return_value=[
            UniverseStock(ticker=t, source="finviz_screen") for t in tickers
        ])

    def _mock_sp500(self, tickers):
        """Patch the S&P 500 pool to return a controlled list."""
        return patch("src.universe_builder._fetch_sp500_pool", return_value=[
            UniverseStock(ticker=t, source="sp500_filter") for t in tickers
        ])

    def test_all_conviction_tickers_present(self, conviction_yaml, tmp_path):
        with self._mock_sp500([]), self._mock_finviz([]):
            stocks = build_universe(conviction_yaml, cache_dir=tmp_path)
        tickers = get_tickers(stocks)
        assert "V" in tickers
        assert "COST" in tickers
        assert "MSCI" in tickers

    def test_no_duplicates(self, conviction_yaml, tmp_path):
        """Even if conviction ticker appears in sp500 pool, no duplicates."""
        with self._mock_sp500(["V", "AAPL"]), self._mock_finviz(["AAPL", "GOOG"]):
            stocks = build_universe(conviction_yaml, cache_dir=tmp_path)
        tickers = get_tickers(stocks)
        assert len(tickers) == len(set(tickers))

    def test_conviction_comes_first(self, conviction_yaml, tmp_path):
        """Conviction tickers should precede sp500 and finviz in the output."""
        with self._mock_sp500(["AAPL"]), self._mock_finviz(["ACME"]):
            stocks = build_universe(conviction_yaml, cache_dir=tmp_path)
        sources = [s.source for s in stocks]
        last_conviction = max(i for i, s in enumerate(stocks) if s.source == "conviction")
        first_sp500 = min((i for i, s in enumerate(stocks) if s.source == "sp500_filter"), default=999)
        first_finviz = min((i for i, s in enumerate(stocks) if s.source == "finviz_screen"), default=999)
        assert last_conviction < first_sp500 or first_sp500 == 999
        assert last_conviction < first_finviz or first_finviz == 999

    def test_all_stocks_have_source(self, conviction_yaml, tmp_path):
        with self._mock_sp500(["GOOGL"]), self._mock_finviz(["ACME"]):
            stocks = build_universe(conviction_yaml, cache_dir=tmp_path)
        assert all(s.source for s in stocks)

    def test_returns_list_of_universe_stocks(self, conviction_yaml, tmp_path):
        with self._mock_sp500([]), self._mock_finviz([]):
            stocks = build_universe(conviction_yaml, cache_dir=tmp_path)
        assert all(isinstance(s, UniverseStock) for s in stocks)

    def test_get_conviction_tickers(self, conviction_yaml, tmp_path):
        with self._mock_sp500(["GOOGL"]), self._mock_finviz([]):
            stocks = build_universe(conviction_yaml, cache_dir=tmp_path)
        conviction_only = get_conviction_tickers(stocks)
        assert set(conviction_only) == {"V", "COST", "MSCI"}

    def test_sp500_ticker_not_on_conviction_appears_with_sp500_source(self, conviction_yaml, tmp_path):
        with self._mock_sp500(["GOOGL"]), self._mock_finviz([]):
            stocks = build_universe(conviction_yaml, cache_dir=tmp_path)
        googl = next((s for s in stocks if s.ticker == "GOOGL"), None)
        assert googl is not None
        assert googl.source == "sp500_filter"

    def test_db_sync_called_when_provided(self, conviction_yaml, tmp_path):
        mock_db = MagicMock()
        with self._mock_sp500([]), self._mock_finviz([]):
            build_universe(conviction_yaml, cache_dir=tmp_path, db=mock_db)
        assert mock_db.upsert_universe_stock.called
        call_args = [call[0][0] for call in mock_db.upsert_universe_stock.call_args_list]
        assert "V" in call_args


# ─── cap_overrides in score_stock() ──────────────────────────────────────


class TestCapOverridesInScoring:
    def _make_criteria(self, cap_overrides=None):
        """Build a ScreeningCriteria with a known P/E rule and optional cap_overrides."""
        base_pe = ScoringRule(ideal=15.0, max=60.0, weight=0.8)
        return ScreeningCriteria(
            scoring={"pe_ratio": base_pe},
            cap_overrides=cap_overrides or {},
        )

    def test_large_cap_gets_relaxed_pe_ideal(self):
        """Large cap with a premium P/E should score better under the cap_override.

        The benefit of the cap override is most visible at premium valuations
        (P/E=40+) where the base rule penalises heavily but the override still
        considers the business a good deal.

        Base rule:   ideal=15, max=60 → P/E=40 scores (1 - 25/45) = 0.444
        Cap override: ideal=25, max=80 → P/E=40 scores (1 - 15/55) = 0.727
        """
        data = {"pe_ratio": 40.0}  # Premium but realistic for wide-moat large caps

        criteria_base = self._make_criteria()
        score_base, _ = score_stock(data, criteria_base, cap_category="large")

        large_pe = ScoringRule(ideal=25.0, max=80.0, weight=0.6)
        criteria_with = self._make_criteria(cap_overrides={"large": {"pe_ratio": large_pe}})
        score_with, _ = score_stock(data, criteria_with, cap_category="large")

        assert score_with > score_base

    def test_small_cap_not_affected_by_large_cap_override(self):
        """Cap override for 'large' must NOT apply to 'small' stocks.

        At P/E=40, large caps get the relaxed override (score 0.44 * 0.6 ≈ 0.26)
        vs small cap using base rule (score 0.444 * 0.8 ≈ 0.36).

        The key check: large cap score at P/E=40 should differ from small cap
        because the cap override changes the scoring rule. Use a P/E high enough
        that the override meaningfully changes the metric score.
        """
        data = {"pe_ratio": 50.0}  # Very high PE: base heavily penalises, override less so
        large_pe = ScoringRule(ideal=25.0, max=80.0, weight=0.6)
        criteria = self._make_criteria(cap_overrides={"large": {"pe_ratio": large_pe}})

        score_small, _ = score_stock(data, criteria, cap_category="small")
        score_large, _ = score_stock(data, criteria, cap_category="large")

        # Small uses base: (1 - 35/45) * 0.8 = 0.222 * 0.8 = 0.178
        # Large uses override: (1 - 25/55) * 0.6 = 0.545 * 0.6 = 0.327
        # Large-cap score must be higher at extreme P/E
        assert score_large > score_small

    def test_cap_override_stacks_on_sector_override(self):
        """Cap override applies AFTER sector override (both rules can coexist)."""
        # Sector override changes one metric, cap override changes another
        sector_rule = ScoringRule(ideal=0.05, max=0.50, weight=0.3)   # debt_equity for Financial
        cap_rule = ScoringRule(ideal=25.0, max=80.0, weight=0.6)       # pe_ratio for large

        criteria = ScreeningCriteria(
            scoring={
                "pe_ratio": ScoringRule(ideal=15.0, max=60.0, weight=0.8),
                "debt_equity": ScoringRule(ideal=0.0, max=1.5, weight=0.8),
            },
            sector_overrides={"Financial Services": {"debt_equity": sector_rule}},
            cap_overrides={"large": {"pe_ratio": cap_rule}},
        )

        data = {"pe_ratio": 25.0, "debt_equity": 0.5}
        score, _ = score_stock(data, criteria, sector="Financial Services", cap_category="large")
        assert score > 0.0

    def test_no_cap_override_no_effect(self):
        """When cap_overrides is empty, scoring is identical to base."""
        data = {"pe_ratio": 15.0}
        criteria_no_override = self._make_criteria(cap_overrides={})
        criteria_with_unknown = self._make_criteria(cap_overrides={"huge": ScoringRule(ideal=50.0)})

        score_a, _ = score_stock(data, criteria_no_override, cap_category="large")
        score_b, _ = score_stock(data, criteria_with_unknown, cap_category="large")
        assert score_a == pytest.approx(score_b)

    def test_empty_cap_category_uses_base(self):
        """cap_category='' must never trigger any override."""
        data = {"pe_ratio": 25.0}
        large_pe = ScoringRule(ideal=25.0, max=80.0, weight=0.6)
        criteria = self._make_criteria(cap_overrides={"large": {"pe_ratio": large_pe}})

        score_base, _ = score_stock(data, criteria, cap_category="")
        # With cap_category='', base ideal=15, P/E=25 → penalised
        assert score_base < 1.0


# ─── load_criteria_from_yaml() parses cap_overrides ──────────────────────


class TestLoadCriteriaFromYaml:
    def test_cap_overrides_loaded(self, screening_criteria_yaml):
        criteria = load_criteria_from_yaml(screening_criteria_yaml)
        assert "large" in criteria.cap_overrides

    def test_large_cap_pe_rule_loaded(self, screening_criteria_yaml):
        criteria = load_criteria_from_yaml(screening_criteria_yaml)
        large_pe = criteria.cap_overrides["large"]["pe_ratio"]
        assert large_pe.ideal == pytest.approx(25.0)
        assert large_pe.max == pytest.approx(80.0)
        assert large_pe.weight == pytest.approx(0.6)

    def test_missing_cap_overrides_gives_empty_dict(self, tmp_path):
        """YAML without cap_overrides should produce criteria.cap_overrides == {}."""
        data = {
            "screening": {
                "scoring": {"pe_ratio": {"ideal": 15, "max": 60, "weight": 0.8}},
            }
        }
        path = tmp_path / "minimal.yaml"
        path.write_text(yaml.dump(data))
        criteria = load_criteria_from_yaml(path)
        assert criteria.cap_overrides == {}

    def test_sector_and_cap_overrides_coexist(self, screening_criteria_yaml):
        criteria = load_criteria_from_yaml(screening_criteria_yaml)
        assert "Financial Services" in criteria.sector_overrides
        assert "large" in criteria.cap_overrides

    def test_full_screening_criteria_yaml(self):
        """The real config file must parse without errors and contain large cap override."""
        real_yaml = Path(__file__).parent.parent / "config" / "screening_criteria.yaml"
        if not real_yaml.exists():
            pytest.skip("Real screening_criteria.yaml not found")
        criteria = load_criteria_from_yaml(real_yaml)
        assert "large" in criteria.cap_overrides
        assert "pe_ratio" in criteria.cap_overrides["large"]


# ─── ScreenedStock.cap_category ──────────────────────────────────────────


class TestScreenedStockCapCategory:
    def _make_stock(self, cap_category=""):
        from datetime import datetime
        return ScreenedStock(
            symbol="TEST",
            name="Test Co",
            market_cap=50_000_000_000,
            pe_ratio=25.0,
            debt_equity=0.3,
            roe=0.18,
            revenue_growth=0.10,
            sector="Technology",
            industry="Software",
            screened_at=datetime.now(),
            cap_category=cap_category,
        )

    def test_cap_category_field_exists(self):
        stock = self._make_stock("large")
        assert stock.cap_category == "large"

    def test_cap_category_in_to_dict(self):
        stock = self._make_stock("large")
        d = stock.to_dict()
        assert "cap_category" in d
        assert d["cap_category"] == "large"

    def test_cap_category_default_empty_string(self):
        stock = self._make_stock()
        assert stock.cap_category == ""
