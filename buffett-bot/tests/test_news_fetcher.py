"""
Tests for Phase E: news trigger pipeline.

Covers:
  - MATERIAL_KEYWORDS: keyword set properties
  - apply_keyword_filter: filtering and event_type annotation
  - _infer_event_type: headline classification
  - format_news_for_llm: news formatting for LLM prompts
  - _build_news_context: DB-backed company context builder
  - _ts_to_iso: timestamp normalisation
  - FinnhubNewsFetcher: HTTP calls, rate limiting, missing API key
  - run_news_pipeline: orchestration with mocked analyzer/fetcher
  - daily_news_monitor: scheduler job integration

External dependencies (Anthropic API, Finnhub HTTP, Alpaca) are always mocked.
"""

import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ── Bootstrap path ────────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))
sys.path.insert(0, str(_PROJECT_ROOT / "scripts"))

# Stub container-only packages so scheduler.py and src.analyzer can be
# imported on the host test runner.
_anthropic_mock = MagicMock()
for _pkg in (
    "schedule",
    "dotenv",
    "anthropic",
    "anthropic.types",
    "anthropic.lib",
    "anthropic.lib.streaming",
):
    sys.modules.setdefault(_pkg, _anthropic_mock)

from src.database import Database
from src.news_fetcher import (
    MATERIAL_KEYWORDS,
    FinnhubNewsFetcher,
    _build_news_context,
    _infer_event_type,
    _ts_to_iso,
    apply_keyword_filter,
    format_news_for_llm,
    run_news_pipeline,
)


# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def db(tmp_path):
    """Fresh Database with WAL mode for each test."""
    return Database(db_path=tmp_path / "test.db")


def _make_item(headline, summary="", source="Reuters", ts=1700000000):
    """Construct a minimal Finnhub-style news dict."""
    return {"headline": headline, "summary": summary, "source": source, "datetime": ts}


def _setup_watched_ticker(db, ticker, tier="A"):
    """Insert a universe stock + deep analysis + price alert for a watched ticker."""
    db.upsert_universe_stock(ticker, company_name=f"{ticker} Corp", source="conviction")
    db.save_deep_analysis(
        ticker,
        tier=tier,
        investment_thesis="Great moat",
        thesis_breakers=["CEO change", "fraud"],
    )
    db.upsert_price_alert(
        ticker,
        tier=tier,
        target_entry=150.0,
        staged_entries=None,
        last_price=160.0,
        gap_pct=0.067,
    )


def _exhaust_budget(db, cap_type):
    """Drive a budget cap to its maximum."""
    conn = sqlite3.connect(str(db.path))
    conn.execute(
        "UPDATE budget_caps SET calls_used = max_calls WHERE cap_type = ?", (cap_type,)
    )
    conn.commit()
    conn.close()


def _make_fetcher(news_map: dict) -> MagicMock:
    """Return a FinnhubNewsFetcher mock whose get_news_for_tickers returns news_map."""
    fetcher = MagicMock(spec=FinnhubNewsFetcher)
    fetcher.api_key = "test_key"  # pragma: allowlist secret
    fetcher.get_news_for_tickers.return_value = news_map
    return fetcher


def _make_analyzer(has_red_flags=False, recommendation="HOLD"):
    """Return a CompanyAnalyzer mock with check_news_for_red_flags pre-configured."""
    analyzer = MagicMock()
    analyzer.check_news_for_red_flags.return_value = {
        "has_red_flags": has_red_flags,
        "analysis": "Analysis text",
        "recommendation": recommendation,
    }
    return analyzer


def _make_sonnet_analysis(ticker="AAPL", tier="C"):
    """Return a minimal AnalysisV2-like mock for Sonnet re-analysis."""
    mock = MagicMock()
    mock.symbol = ticker
    mock.conviction = "LOW"
    mock.moat_rating.value = "none"
    mock.moat_sources = []
    mock.estimated_fair_value_low = 100.0
    mock.estimated_fair_value_high = 120.0
    mock.target_entry_price = 85.0
    mock.current_price = 160.0
    mock.summary = "Risk elevated"
    mock.key_risks = ["investigation"]
    mock.thesis_risks = ["SEC enforcement"]
    return mock


# ─── MATERIAL_KEYWORDS ────────────────────────────────────────────────────────


class TestMaterialKeywords:
    def test_is_frozenset(self):
        assert isinstance(MATERIAL_KEYWORDS, frozenset)

    def test_contains_earnings(self):
        assert "earnings" in MATERIAL_KEYWORDS

    def test_contains_ceo(self):
        assert "ceo" in MATERIAL_KEYWORDS

    def test_contains_acquisition_prefix(self):
        # We use "acqui" to match "acquires", "acquisition", etc.
        assert "acqui" in MATERIAL_KEYWORDS

    def test_contains_sec(self):
        assert "sec " in MATERIAL_KEYWORDS  # trailing space avoids "sector" false-positive

    def test_contains_bankruptcy(self):
        assert "bankruptcy" in MATERIAL_KEYWORDS

    def test_contains_dividend(self):
        assert "dividend" in MATERIAL_KEYWORDS


# ─── _infer_event_type ────────────────────────────────────────────────────────


class TestInferEventType:
    def test_earnings(self):
        assert _infer_event_type("Company reports Q3 earnings beat") == "earnings"

    def test_guidance(self):
        assert _infer_event_type("Management raises full-year guidance") == "guidance"

    def test_ceo_change(self):
        assert _infer_event_type("CEO steps down effective Q4") == "ceo_change"

    def test_acquisition(self):
        assert _infer_event_type("Acme acquires rival for $2B") == "acquisition"

    def test_legal(self):
        assert _infer_event_type("SEC launches fraud investigation") == "legal"

    def test_bankruptcy(self):
        assert _infer_event_type("Company files for bankruptcy protection") == "bankruptcy"

    def test_dividend(self):
        assert _infer_event_type("Board declares special dividend of $1.50") == "dividend"

    def test_buyback(self):
        assert _infer_event_type("Company announces $5B buyback programme") == "buyback"

    def test_downgrade(self):
        assert _infer_event_type("Moody's issues downgrade on credit rating") == "downgrade"

    def test_unknown_returns_none(self):
        assert _infer_event_type("Stock price rises on general optimism") is None


# ─── apply_keyword_filter ─────────────────────────────────────────────────────


class TestApplyKeywordFilter:
    def test_keeps_material_headline(self):
        items = [_make_item("Company reports strong earnings beat")]
        result = apply_keyword_filter(items)
        assert len(result) == 1

    def test_drops_noise(self):
        items = [_make_item("Market roundup: global stocks edge higher")]
        result = apply_keyword_filter(items)
        assert len(result) == 0

    def test_matches_keyword_in_summary(self):
        items = [_make_item("Stock update", summary="CEO resigns effective immediately")]
        result = apply_keyword_filter(items)
        assert len(result) == 1

    def test_annotates_event_type(self):
        items = [_make_item("Acme raises guidance for fiscal year")]
        result = apply_keyword_filter(items)
        assert result[0]["event_type"] == "guidance"

    def test_preserves_original_fields(self):
        item = _make_item("CEO departs")
        result = apply_keyword_filter([item])
        assert result[0]["source"] == "Reuters"
        assert result[0]["datetime"] == 1700000000

    def test_event_type_none_when_unclassified(self):
        # "dividend" is in MATERIAL_KEYWORDS and "dividend" matches event type
        items = [_make_item("Company announces special dividend")]
        result = apply_keyword_filter(items)
        assert result[0]["event_type"] == "dividend"

    def test_empty_list(self):
        assert apply_keyword_filter([]) == []

    def test_case_insensitive_match(self):
        items = [_make_item("EARNINGS BEAT CONSENSUS ESTIMATES")]
        result = apply_keyword_filter(items)
        assert len(result) == 1

    def test_multiple_items_filtered_correctly(self):
        items = [
            _make_item("Noise: stocks rally"),
            _make_item("Company CEO resigns"),
            _make_item("More noise"),
            _make_item("SEC launches investigation"),
        ]
        result = apply_keyword_filter(items)
        assert len(result) == 2


# ─── format_news_for_llm ──────────────────────────────────────────────────────


class TestFormatNewsForLlm:
    def test_empty_list_returns_placeholder(self):
        assert format_news_for_llm([]) == "(no news)"

    def test_formats_headline_and_source(self):
        items = [_make_item("CEO departs")]
        result = format_news_for_llm(items)
        assert "CEO departs" in result
        assert "Reuters" in result

    def test_unix_timestamp_converted_to_date(self):
        # ts=1700000000 → 2023-11-14T22:13:20 UTC
        items = [_make_item("Earnings beat", ts=1700000000)]
        result = format_news_for_llm(items)
        assert "2023-11-14" in result

    def test_respects_max_items(self):
        items = [_make_item(f"Headline {i}") for i in range(20)]
        result = format_news_for_llm(items, max_items=5)
        assert result.count("Headline") == 5

    def test_summary_truncated_at_200_chars(self):
        long_summary = "x" * 500
        items = [_make_item("Test", summary=long_summary)]
        result = format_news_for_llm(items)
        # Only 200 x's should appear (from summary truncation)
        assert result.count("x") == 200

    def test_items_separated_by_blank_line(self):
        items = [_make_item("Item A"), _make_item("Item B")]
        result = format_news_for_llm(items)
        assert "\n\n" in result


# ─── _build_news_context ──────────────────────────────────────────────────────


class TestBuildNewsContext:
    def test_full_context(self, db):
        db.upsert_universe_stock(
            "AAPL",
            company_name="Apple Inc",
            sector="Technology",
            market_cap=3e12,
            source="conviction",
        )
        db.save_fundamentals("AAPL", {"price": 180.0, "pe_ratio": 28.5, "roe": 0.15})
        db.save_deep_analysis(
            "AAPL",
            tier="S",
            investment_thesis="Dominant ecosystem",
            key_risks=["competition", "regulation"],
        )
        result = _build_news_context("AAPL", db)

        assert "AAPL" in result
        assert "Apple Inc" in result
        assert "Technology" in result
        assert "$180.00" in result
        assert "Dominant ecosystem" in result
        assert "competition" in result

    def test_no_fundamentals(self, db):
        db.upsert_universe_stock("GOOG", source="conviction")
        result = _build_news_context("GOOG", db)
        # Metrics section absent but no crash
        assert "GOOG" in result
        assert "KEY FINANCIAL METRICS" not in result

    def test_no_universe_entry(self, db):
        # Ticker entirely absent — minimal context
        result = _build_news_context("UNKN", db)
        assert "UNKN" in result

    def test_no_prior_analysis(self, db):
        db.upsert_universe_stock("MSFT", source="conviction")
        db.save_fundamentals("MSFT", {"price": 400.0})
        result = _build_news_context("MSFT", db)
        assert "PRIOR INVESTMENT THESIS" not in result

    def test_thesis_truncated_at_500_chars(self, db):
        db.upsert_universe_stock("ORCL", source="conviction")
        long_thesis = "Z" * 800
        db.save_deep_analysis("ORCL", tier="B", investment_thesis=long_thesis)
        result = _build_news_context("ORCL", db)
        assert result.count("Z") == 500


# ─── _ts_to_iso ───────────────────────────────────────────────────────────────


class TestTsToIso:
    def test_unix_int(self):
        result = _ts_to_iso(1700000000)
        assert "2023-11-14" in result

    def test_unix_float(self):
        result = _ts_to_iso(1700000000.0)
        assert "2023-11-14" in result

    def test_none_returns_none(self):
        assert _ts_to_iso(None) is None

    def test_string_passthrough(self):
        assert _ts_to_iso("2024-01-01") == "2024-01-01"


# ─── FinnhubNewsFetcher ───────────────────────────────────────────────────────


class TestFinnhubNewsFetcher:
    def test_get_company_news_success(self):
        fetcher = FinnhubNewsFetcher(api_key="test_key")  # pragma: allowlist secret
        fake_data = [{"headline": "Earnings beat", "datetime": 1700000000}]
        with patch("requests.get") as mock_get:
            mock_get.return_value.raise_for_status = lambda: None
            mock_get.return_value.json.return_value = fake_data
            result = fetcher.get_company_news("AAPL", "2024-01-01", "2024-01-02")
        assert result == fake_data
        mock_get.assert_called_once()

    def test_get_company_news_no_api_key(self):
        fetcher = FinnhubNewsFetcher(api_key="")
        result = fetcher.get_company_news("AAPL", "2024-01-01", "2024-01-02")
        assert result == []

    def test_get_company_news_http_error(self):
        fetcher = FinnhubNewsFetcher(api_key="test_key")  # pragma: allowlist secret
        with patch("requests.get") as mock_get:
            mock_get.side_effect = Exception("Connection refused")
            result = fetcher.get_company_news("AAPL", "2024-01-01", "2024-01-02")
        assert result == []

    def test_get_company_news_non_list_response(self):
        fetcher = FinnhubNewsFetcher(api_key="test_key")  # pragma: allowlist secret
        with patch("requests.get") as mock_get:
            mock_get.return_value.raise_for_status = lambda: None
            mock_get.return_value.json.return_value = {"error": "not a list"}
            result = fetcher.get_company_news("AAPL", "2024-01-01", "2024-01-02")
        assert result == []

    def test_get_news_for_tickers_calls_each(self):
        fetcher = FinnhubNewsFetcher(api_key="test_key")  # pragma: allowlist secret
        with patch.object(fetcher, "get_company_news", return_value=[]) as mock_fn:
            result = fetcher.get_news_for_tickers(["AAPL", "MSFT", "GOOG"], days_back=1)
        assert set(result.keys()) == {"AAPL", "MSFT", "GOOG"}
        assert mock_fn.call_count == 3

    def test_get_news_for_tickers_empty_input(self):
        fetcher = FinnhubNewsFetcher(api_key="test_key")  # pragma: allowlist secret
        result = fetcher.get_news_for_tickers([], days_back=1)
        assert result == {}

    def test_get_news_for_tickers_uses_date_range(self):
        fetcher = FinnhubNewsFetcher(api_key="test_key")  # pragma: allowlist secret
        captured_calls = []

        def fake_get(symbol, from_date, to_date):
            captured_calls.append((symbol, from_date, to_date))
            return []

        with patch.object(fetcher, "get_company_news", side_effect=fake_get):
            fetcher.get_news_for_tickers(["AAPL"], days_back=3)

        assert len(captured_calls) == 1
        sym, from_d, to_d = captured_calls[0]
        assert sym == "AAPL"
        # from_date should be 3 days before to_date
        from datetime import date, timedelta

        today = date.today()
        assert from_d == (today - timedelta(days=3)).isoformat()
        assert to_d == today.isoformat()


# ─── run_news_pipeline ────────────────────────────────────────────────────────


class TestRunNewsPipeline:
    def test_no_watched_tickers_returns_zero_stats(self, db):
        fetcher = _make_fetcher({})
        analyzer = _make_analyzer()
        stats = run_news_pipeline(db, analyzer, fetcher)
        assert stats["tickers_checked"] == 0
        assert stats["haiku_calls"] == 0

    def test_dry_run_skips_fetch_and_llm(self, db):
        _setup_watched_ticker(db, "AAPL")
        fetcher = _make_fetcher({})
        analyzer = _make_analyzer()
        stats = run_news_pipeline(db, analyzer, fetcher, dry_run=True)
        assert stats["haiku_calls"] == 0
        assert stats["sonnet_calls"] == 0
        fetcher.get_news_for_tickers.assert_not_called()

    def test_no_raw_news_no_haiku(self, db):
        _setup_watched_ticker(db, "AAPL")
        fetcher = _make_fetcher({"AAPL": []})
        analyzer = _make_analyzer()
        stats = run_news_pipeline(db, analyzer, fetcher)
        assert stats["haiku_calls"] == 0
        assert stats["news_found"] == 0

    def test_non_material_news_no_haiku(self, db):
        _setup_watched_ticker(db, "AAPL")
        fetcher = _make_fetcher({"AAPL": [_make_item("Market roundup: stocks drift higher")]})
        analyzer = _make_analyzer()
        stats = run_news_pipeline(db, analyzer, fetcher)
        assert stats["haiku_calls"] == 0
        assert stats["news_found"] == 0

    def test_material_news_triggers_haiku(self, db):
        _setup_watched_ticker(db, "AAPL")
        fetcher = _make_fetcher({"AAPL": [_make_item("Apple CEO Tim Cook steps down")]})
        analyzer = _make_analyzer(has_red_flags=False, recommendation="HOLD")
        stats = run_news_pipeline(db, analyzer, fetcher)
        assert stats["haiku_calls"] == 1
        assert stats["news_found"] == 1
        analyzer.check_news_for_red_flags.assert_called_once()

    def test_hold_recommendation_no_sonnet(self, db):
        _setup_watched_ticker(db, "MSFT")
        fetcher = _make_fetcher({"MSFT": [_make_item("Microsoft earnings beat expectations")]})
        analyzer = _make_analyzer(has_red_flags=False, recommendation="HOLD")
        stats = run_news_pipeline(db, analyzer, fetcher)
        assert stats["haiku_calls"] == 1
        assert stats["sonnet_calls"] == 0

    def test_red_flags_trigger_sonnet(self, db):
        _setup_watched_ticker(db, "AAPL")
        fetcher = _make_fetcher({"AAPL": [_make_item("SEC launches fraud investigation into Apple")]})
        analyzer = _make_analyzer(has_red_flags=True, recommendation="REVIEW")
        mock_analysis = _make_sonnet_analysis("AAPL", "C")
        analyzer.analyze_company.return_value = mock_analysis

        with patch("src.news_fetcher.assign_tier") as mock_tier, \
             patch("src.news_fetcher.staged_entry_suggestion", return_value=None):
            mock_tier.return_value = MagicMock(tier="C", tier_reason="Risk", price_gap_pct=None)
            stats = run_news_pipeline(db, analyzer, fetcher)

        assert stats["haiku_calls"] == 1
        assert stats["sonnet_calls"] == 1

    def test_review_recommendation_triggers_sonnet(self, db):
        _setup_watched_ticker(db, "META")
        fetcher = _make_fetcher({"META": [_make_item("Meta CEO faces regulatory lawsuit")]})
        analyzer = _make_analyzer(has_red_flags=False, recommendation="REVIEW")
        mock_analysis = _make_sonnet_analysis("META")
        analyzer.analyze_company.return_value = mock_analysis

        with patch("src.news_fetcher.assign_tier") as mock_tier, \
             patch("src.news_fetcher.staged_entry_suggestion", return_value=None):
            mock_tier.return_value = MagicMock(tier="B", tier_reason="Monitor", price_gap_pct=0.1)
            run_news_pipeline(db, analyzer, fetcher)

        assert analyzer.analyze_company.called

    def test_haiku_budget_exhausted_skips_check(self, db):
        _setup_watched_ticker(db, "GOOG")
        _exhaust_budget(db, "weekly_news_haiku")
        fetcher = _make_fetcher({"GOOG": [_make_item("Google CEO resigns")]})
        analyzer = _make_analyzer()
        stats = run_news_pipeline(db, analyzer, fetcher)
        assert stats["haiku_calls"] == 0

    def test_sonnet_budget_exhausted_skips_reanalysis(self, db):
        _setup_watched_ticker(db, "AMZN")
        _exhaust_budget(db, "weekly_news_sonnet")
        fetcher = _make_fetcher({"AMZN": [_make_item("SEC investigates Amazon fraud")]})
        analyzer = _make_analyzer(has_red_flags=True, recommendation="REVIEW")
        stats = run_news_pipeline(db, analyzer, fetcher)
        assert stats["haiku_calls"] == 1
        assert stats["sonnet_calls"] == 0

    def test_news_event_logged_to_db(self, db):
        _setup_watched_ticker(db, "META")
        fetcher = _make_fetcher(
            {"META": [{"headline": "Meta CEO steps down", "source": "Reuters",
                       "datetime": 1700000000, "summary": ""}]}
        )
        analyzer = _make_analyzer(has_red_flags=False, recommendation="HOLD")
        run_news_pipeline(db, analyzer, fetcher)

        conn = sqlite3.connect(str(db.path))
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM news_events WHERE ticker = 'META'").fetchone()
        conn.close()

        assert row is not None
        assert "CEO" in row["headline"]
        assert row["haiku_material"] == 0  # no red flags → False → 0

    def test_tier_change_logged_to_history(self, db):
        _setup_watched_ticker(db, "TSLA", tier="A")
        fetcher = _make_fetcher({"TSLA": [_make_item("Tesla SEC fraud probe escalates")]})
        analyzer = _make_analyzer(has_red_flags=True, recommendation="SELL")
        analyzer.analyze_company.return_value = _make_sonnet_analysis("TSLA")

        with patch("src.news_fetcher.assign_tier") as mock_tier, \
             patch("src.news_fetcher.staged_entry_suggestion", return_value=None):
            mock_tier.return_value = MagicMock(tier="C", tier_reason="Sell", price_gap_pct=None)
            run_news_pipeline(db, analyzer, fetcher)

        history = db.get_tier_history("TSLA")
        assert any(h["trigger"] == "news_event" for h in history)

    def test_notifier_called_on_tier_change(self, db):
        _setup_watched_ticker(db, "TSLA", tier="A")
        fetcher = _make_fetcher({"TSLA": [_make_item("Tesla SEC fraud probe escalates")]})
        analyzer = _make_analyzer(has_red_flags=True, recommendation="SELL")
        analyzer.analyze_company.return_value = _make_sonnet_analysis("TSLA")
        notifier = MagicMock()

        with patch("src.news_fetcher.assign_tier") as mock_tier, \
             patch("src.news_fetcher.staged_entry_suggestion", return_value=None):
            mock_tier.return_value = MagicMock(tier="C", tier_reason="Sell", price_gap_pct=None)
            run_news_pipeline(db, analyzer, fetcher, notifier=notifier)

        notifier.send_alert.assert_called_once()
        ticker_arg, msg_arg = notifier.send_alert.call_args[0]
        assert ticker_arg == "TSLA"
        assert "SELL" in msg_arg or "C" in msg_arg

    def test_notifier_not_called_when_tier_unchanged(self, db):
        _setup_watched_ticker(db, "NVDA", tier="A")
        fetcher = _make_fetcher({"NVDA": [_make_item("Nvidia earnings beat")]})
        analyzer = _make_analyzer(has_red_flags=True, recommendation="REVIEW")
        analyzer.analyze_company.return_value = _make_sonnet_analysis("NVDA")
        notifier = MagicMock()

        with patch("src.news_fetcher.assign_tier") as mock_tier, \
             patch("src.news_fetcher.staged_entry_suggestion", return_value=None):
            # Same tier as before ("A")
            mock_tier.return_value = MagicMock(tier="A", tier_reason="Hold", price_gap_pct=0.1)
            run_news_pipeline(db, analyzer, fetcher, notifier=notifier)

        notifier.send_alert.assert_not_called()

    def test_haiku_exception_logs_event_and_continues(self, db):
        _setup_watched_ticker(db, "NVDA")
        fetcher = _make_fetcher({"NVDA": [_make_item("Nvidia CEO retires")]})
        analyzer = MagicMock()
        analyzer.check_news_for_red_flags.side_effect = RuntimeError("API down")

        # Should not raise; still counts the Haiku call attempt
        stats = run_news_pipeline(db, analyzer, fetcher)
        assert stats["haiku_calls"] == 1

    def test_sonnet_exception_does_not_raise(self, db):
        _setup_watched_ticker(db, "AAPL")
        fetcher = _make_fetcher({"AAPL": [_make_item("Apple SEC fraud probe")]})
        analyzer = _make_analyzer(has_red_flags=True, recommendation="SELL")
        analyzer.analyze_company.side_effect = RuntimeError("Sonnet unavailable")

        stats = run_news_pipeline(db, analyzer, fetcher)
        assert stats["sonnet_calls"] == 1  # attempt counted even on failure

    def test_multiple_tickers_independent(self, db):
        for ticker in ["AAPL", "MSFT", "GOOG"]:
            _setup_watched_ticker(db, ticker)
        fetcher = _make_fetcher(
            {
                "AAPL": [_make_item("Apple earnings beat")],
                "MSFT": [],  # no news
                "GOOG": [_make_item("Google CEO resigns")],
            }
        )
        analyzer = _make_analyzer(has_red_flags=False, recommendation="HOLD")
        stats = run_news_pipeline(db, analyzer, fetcher)
        assert stats["tickers_checked"] == 3
        assert stats["news_found"] == 2
        assert stats["haiku_calls"] == 2

    def test_sonnet_saves_new_deep_analysis(self, db):
        _setup_watched_ticker(db, "AAPL", tier="A")
        fetcher = _make_fetcher({"AAPL": [_make_item("Apple CEO Tim Cook resigns")]})
        analyzer = _make_analyzer(has_red_flags=True, recommendation="SELL")
        analyzer.analyze_company.return_value = _make_sonnet_analysis("AAPL")

        with patch("src.news_fetcher.assign_tier") as mock_tier, \
             patch("src.news_fetcher.staged_entry_suggestion", return_value=None):
            mock_tier.return_value = MagicMock(tier="C", tier_reason="Downgrade", price_gap_pct=None)
            run_news_pipeline(db, analyzer, fetcher)

        da = db.get_latest_deep_analysis("AAPL")
        assert da is not None
        assert da["tier"] == "C"

    def test_c_tier_downgrade_updates_price_alert_to_c(self, db):
        """
        Bug regression: when Sonnet demotes a stock to C, the price_alerts row
        must be updated to tier='C'.  Without this fix, the stale B-tier alert
        would keep the stock in the next day's news-monitoring query, wasting
        Haiku budget on a stock we've already decided has no moat.
        """
        _setup_watched_ticker(db, "COIN", tier="B")
        fetcher = _make_fetcher({"COIN": [_make_item("Coinbase faces SEC enforcement action")]})
        analyzer = _make_analyzer(has_red_flags=True, recommendation="SELL")
        analyzer.analyze_company.return_value = _make_sonnet_analysis("COIN")

        with patch("src.news_fetcher.assign_tier") as mock_tier, \
             patch("src.news_fetcher.staged_entry_suggestion", return_value=[]):
            mock_tier.return_value = MagicMock(tier="C", tier_reason="No moat", price_gap_pct=None)
            run_news_pipeline(db, analyzer, fetcher)

        # price_alert must now carry tier='C'
        conn = sqlite3.connect(str(db.path))
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT tier FROM price_alerts WHERE ticker='COIN'").fetchone()
        conn.close()
        assert row is not None
        assert row["tier"] == "C"

        # And must be invisible to the next news cycle's S/A/B query
        visible = db.get_price_alerts(tiers=["S", "A", "B"])
        assert all(a["ticker"] != "COIN" for a in visible)


# ─── daily_news_monitor ───────────────────────────────────────────────────────


class TestDailyNewsMonitor:
    def test_skips_when_no_api_key(self, tmp_path):
        """Job exits early and does not call run_news_pipeline when key is missing."""
        from scheduler import daily_news_monitor

        db = Database(tmp_path / "test.db")
        with patch("src.database.Database", return_value=db), \
             patch("src.news_fetcher.FinnhubNewsFetcher") as MockFetcher, \
             patch("src.news_fetcher.run_news_pipeline") as mock_pipeline:
            MockFetcher.return_value.api_key = ""
            daily_news_monitor()
        mock_pipeline.assert_not_called()

    def test_runs_pipeline_when_key_set(self, tmp_path):
        """Job calls run_news_pipeline when FINNHUB_API_KEY is present."""
        from scheduler import daily_news_monitor

        db = Database(tmp_path / "test.db")
        with patch("src.database.Database", return_value=db), \
             patch("src.analyzer.CompanyAnalyzer"), \
             patch("src.news_fetcher.FinnhubNewsFetcher") as MockFetcher, \
             patch("src.news_fetcher.run_news_pipeline") as mock_pipeline:
            MockFetcher.return_value.api_key = "test_key"  # pragma: allowlist secret
            mock_pipeline.return_value = {
                "tickers_checked": 5,
                "news_found": 2,
                "haiku_calls": 2,
                "sonnet_calls": 0,
                "tier_changes": 0,
            }
            daily_news_monitor()
        mock_pipeline.assert_called_once()

    def test_does_not_raise_on_db_error(self):
        """Exceptions inside the job must be caught and logged, not propagated."""
        from scheduler import daily_news_monitor

        with patch("src.database.Database", side_effect=RuntimeError("DB unavailable")):
            daily_news_monitor()  # must not raise
