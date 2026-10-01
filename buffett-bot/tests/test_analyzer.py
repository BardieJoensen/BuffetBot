"""
Tests for src/analyzer.py response handling.

These exist because the six response-parsing sites had no coverage at all,
which is exactly what made a model upgrade risky: models with thinking enabled
put a thinking block first in `content`, so the old `content[0]` indexing would
have broken every call path at once — and in quick_screen's case, silently,
because its broad `except` swallows the error into a fail-open where every
stock passes screening.

The Anthropic SDK is stubbed at import time by the other test modules, so these
build response objects structurally rather than importing real SDK types.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

# Stub container-only packages, matching tests/test_scheduler.py. Must happen
# before src.analyzer is imported.
_anthropic_mock = MagicMock()
for _pkg in ("schedule", "dotenv", "anthropic", "anthropic.types", "anthropic.lib", "anthropic.lib.streaming"):
    sys.modules.setdefault(_pkg, _anthropic_mock)

import src.analyzer as analyzer_mod  # noqa: E402
from src.analyzer import CompanyAnalyzer, _first_text  # noqa: E402


class _FakeTextBlock:
    def __init__(self, text):
        self.text = text


class _FakeThinkingBlock:
    def __init__(self, thinking="reasoning..."):
        self.thinking = thinking


@pytest.fixture(autouse=True)
def _patch_textblock(monkeypatch):
    """
    `TextBlock` resolves to a MagicMock attribute under the stubbed SDK, which
    isinstance() cannot use. Point it at our structural stand-in.
    """
    monkeypatch.setattr(analyzer_mod, "TextBlock", _FakeTextBlock)


class TestFirstText:
    def test_returns_text_of_a_lone_text_block(self):
        assert _first_text([_FakeTextBlock("hello")]) == "hello"

    def test_skips_a_leading_thinking_block(self):
        """The regression this whole helper exists for."""
        content = [_FakeThinkingBlock(), _FakeTextBlock("the answer")]
        assert _first_text(content) == "the answer"

    def test_skips_several_leading_non_text_blocks(self):
        content = [_FakeThinkingBlock(), _FakeThinkingBlock(), _FakeTextBlock("answer")]
        assert _first_text(content) == "answer"

    def test_returns_the_first_text_block_when_several(self):
        content = [_FakeTextBlock("first"), _FakeTextBlock("second")]
        assert _first_text(content) == "first"

    def test_raises_when_no_text_block(self):
        with pytest.raises(ValueError, match="No TextBlock"):
            _first_text([_FakeThinkingBlock()])

    def test_raises_on_empty_content(self):
        with pytest.raises(ValueError, match="No TextBlock"):
            _first_text([])

    def test_raises_rather_than_asserts(self):
        """
        assert statements are stripped under `python -O`. A ValueError still
        fires there; an AssertionError would not.
        """
        with pytest.raises(ValueError):
            _first_text([_FakeThinkingBlock()])


def _analyzer_with_response(content):
    """Build a CompanyAnalyzer whose client returns `content`."""
    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):  # pragma: allowlist secret
        analyzer = CompanyAnalyzer()
    analyzer.client = MagicMock()
    analyzer.client.messages.create.return_value = MagicMock(content=content)
    return analyzer


class TestModelConfiguration:
    def test_models_come_from_config(self):
        from src.config import config

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):  # pragma: allowlist secret
            analyzer = CompanyAnalyzer()

        assert analyzer.model_deep == config.model_deep
        assert analyzer.model_light == config.model_light
        assert analyzer.model_opus == config.model_opus

    def test_no_api_key_raises(self):
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False):
            with pytest.raises(ValueError, match="ANTHROPIC_API_KEY"):
                CompanyAnalyzer()


class TestQuickScreenParsing:
    def test_parses_a_plain_text_response(self):
        analyzer = _analyzer_with_response([_FakeTextBlock("MOAT: 4\nQUALITY: 5\nREASON: Strong brand")])

        result = analyzer.quick_screen("AAPL", "some filing text")

        assert result["moat_hint"] == 4
        assert result["quality_hint"] == 5
        assert result["worth_analysis"] is True

    def test_parses_past_a_leading_thinking_block(self):
        analyzer = _analyzer_with_response(
            [_FakeThinkingBlock(), _FakeTextBlock("MOAT: 4\nQUALITY: 5\nREASON: Strong brand")]
        )

        result = analyzer.quick_screen("AAPL", "some filing text")

        assert result["moat_hint"] == 4
        assert result["quality_hint"] == 5

    def test_fails_closed_when_no_text_block(self):
        """
        A screening failure must not fabricate a neutral passing score that can
        later reach automated trading.
        """
        analyzer = _analyzer_with_response([_FakeThinkingBlock()])

        result = analyzer.quick_screen("AAPL", "some filing text")

        assert result["worth_analysis"] is False
        assert result["valid"] is False
        assert "error" in result["reason"].lower()

    def test_uses_the_light_model(self):
        analyzer = _analyzer_with_response([_FakeTextBlock("MOAT: 3\nQUALITY: 3\nREASON: ok")])

        analyzer.quick_screen("AAPL", "filing")

        assert analyzer.client.messages.create.call_args.kwargs["model"] == analyzer.model_light


class TestNewsRedFlagParsing:
    def _response(self, text):
        return _analyzer_with_response([_FakeThinkingBlock(), _FakeTextBlock(text)])

    def test_detects_red_flags(self):
        analyzer = self._response("RED FLAGS DETECTED: YES\nRECOMMENDATION: SELL\nEXPLANATION: fraud")

        result = analyzer.check_news_for_red_flags("AAPL", "thesis", [], "news")

        assert result["has_red_flags"] is True
        assert result["recommendation"] == "SELL"

    def test_parses_explicit_hold(self):
        analyzer = self._response("RED FLAGS DETECTED: NO\nRECOMMENDATION: HOLD\nEXPLANATION: routine")

        result = analyzer.check_news_for_red_flags("AAPL", "thesis", [], "news")

        assert result["has_red_flags"] is False
        assert result["recommendation"] == "HOLD"

    def test_missing_recommendation_raises_instead_of_defaulting_to_hold(self):
        analyzer = self._response("RED FLAGS DETECTED: NO\nEXPLANATION: routine")

        with pytest.raises(ValueError, match="news screen must contain"):
            analyzer.check_news_for_red_flags("AAPL", "thesis", [], "news")

    def test_detects_review_recommendation(self):
        analyzer = self._response("RED FLAGS DETECTED: YES\nRECOMMENDATION: REVIEW")

        assert analyzer.check_news_for_red_flags("AAPL", "t", [], "n")["recommendation"] == "REVIEW"

    def test_raises_when_no_text_block(self):
        """
        Unlike quick_screen this path has no local except, so a malformed
        response must surface rather than be mistaken for "no red flags".
        """
        analyzer = _analyzer_with_response([_FakeThinkingBlock()])

        with pytest.raises(ValueError, match="No TextBlock"):
            analyzer.check_news_for_red_flags("AAPL", "thesis", [], "news")


class TestOpusSecondOpinionParsing:
    def _prior_analysis(self):
        from src.analysis_parser import parse_analysis
        from tests.fixtures_analysis import well_formed_analysis

        return parse_analysis("AAPL", "Apple Inc", well_formed_analysis(), "Technology")

    def test_parses_past_a_leading_thinking_block(self):
        text = (
            "## AGREEMENT\nPARTIALLY_AGREE with the thesis\n\n"
            "## OPUS CONVICTION\nMEDIUM\n\n"
            "## CONTRARIAN RISKS\n1. Margin pressure\n\n"
            "## SUMMARY\nReasonable but watch margins."
        )
        analyzer = _analyzer_with_response([_FakeThinkingBlock(), _FakeTextBlock(text)])

        result = analyzer.opus_second_opinion(
            "AAPL", "Apple Inc", "filing text", self._prior_analysis(), use_cache=False
        )

        assert result["agreement"] == "PARTIALLY_AGREE"
        assert result["opus_conviction"] == "MEDIUM"
        assert result["contrarian_risks"] == ["Margin pressure"]

    def test_uses_the_opus_model(self):
        analyzer = _analyzer_with_response([_FakeTextBlock("## AGREEMENT\nAGREE\n\n## OPUS CONVICTION\nHIGH")])

        analyzer.opus_second_opinion("AAPL", "Apple Inc", "filing", self._prior_analysis(), use_cache=False)

        assert analyzer.client.messages.create.call_args.kwargs["model"] == analyzer.model_opus


class TestBatchResultParsing:
    def _batch_analyzer(self, results):
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):  # pragma: allowlist secret
            analyzer = CompanyAnalyzer()
        analyzer.client = MagicMock()
        batch = MagicMock()
        batch.id = "batch-1"
        analyzer.client.messages.batches.create.return_value = batch
        analyzer.client.messages.batches.results.return_value = results
        analyzer._wait_for_batch = MagicMock(return_value=batch)
        return analyzer

    def _succeeded(self, custom_id, content):
        r = MagicMock()
        r.custom_id = custom_id
        r.result.type = "succeeded"
        r.result.message.content = content
        return r

    def test_parses_past_a_leading_thinking_block(self):
        analyzer = self._batch_analyzer(
            [self._succeeded("AAPL", [_FakeThinkingBlock(), _FakeTextBlock("MOAT: 5\nQUALITY: 4\nREASON: moat")])]
        )

        out = analyzer.batch_quick_screen([("AAPL", "filing text")])

        assert out[0]["moat_hint"] == 5
        assert out[0]["quality_hint"] == 4

    def test_fails_closed_on_errored_result(self):
        errored = MagicMock()
        errored.custom_id = "AAPL"
        errored.result.type = "errored"
        analyzer = self._batch_analyzer([errored])

        out = analyzer.batch_quick_screen([("AAPL", "filing text")])

        assert out[0]["worth_analysis"] is False
        assert out[0]["valid"] is False

    def test_fails_closed_on_malformed_success(self):
        analyzer = self._batch_analyzer([self._succeeded("AAPL", [_FakeTextBlock("looks good")])])

        out = analyzer.batch_quick_screen([("AAPL", "filing text")])

        assert out[0]["worth_analysis"] is False
        assert out[0]["valid"] is False

    def test_preserves_input_order(self):
        analyzer = self._batch_analyzer(
            [
                self._succeeded("MSFT", [_FakeTextBlock("MOAT: 5\nQUALITY: 5\nREASON: b")]),
                self._succeeded("AAPL", [_FakeTextBlock("MOAT: 4\nQUALITY: 4\nREASON: a")]),
            ]
        )

        out = analyzer.batch_quick_screen([("AAPL", "t1"), ("MSFT", "t2")])

        assert [r["symbol"] for r in out] == ["AAPL", "MSFT"]


class TestBatchWaitTimeout:
    def _analyzer(self):
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):  # pragma: allowlist secret
            analyzer = CompanyAnalyzer()
        analyzer.client = MagicMock()
        return analyzer

    def test_timeout_cancels_the_batch_before_raising(self):
        analyzer = self._analyzer()
        batch = MagicMock()
        batch.processing_status = "in_progress"
        batch.request_counts.succeeded = 0
        batch.request_counts.errored = 0
        batch.request_counts.processing = 3
        analyzer.client.messages.batches.retrieve.return_value = batch

        with patch("src.analyzer.time.sleep"), pytest.raises(TimeoutError):
            analyzer._wait_for_batch("batch-1", timeout_minutes=0)

        analyzer.client.messages.batches.cancel.assert_called_once_with("batch-1")

    def test_default_timeout_is_two_hours(self):
        assert CompanyAnalyzer.BATCH_TIMEOUT_MINUTES == 120


class TestBatchCacheBypass:
    def test_use_cache_false_forces_a_fresh_call(self, tmp_path):
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):  # pragma: allowlist secret
            analyzer = CompanyAnalyzer()
        analyzer.client = MagicMock()
        batch = MagicMock()
        batch.id = "batch-1"
        analyzer.client.messages.batches.create.return_value = batch
        analyzer.client.messages.batches.results.return_value = []
        analyzer._wait_for_batch = MagicMock(return_value=batch)

        cached = {"schema_version": "v2", "symbol": "AAPL", "conviction": "LOW"}
        with patch("src.analyzer.get_cached_analysis", return_value=cached) as mock_cached:
            analyzer.batch_analyze_companies(
                [
                    {"symbol": "AAPL", "filing_text": "x", "use_cache": False},
                    {"symbol": "MSFT", "filing_text": "y"},
                ]
            )

        # Only MSFT consulted the cache; AAPL went straight to the batch.
        assert [c.args[0] for c in mock_cached.call_args_list] == ["MSFT"]
        requests = analyzer.client.messages.batches.create.call_args.kwargs["requests"]
        assert [r["custom_id"] for r in requests] == ["AAPL"]


class TestNewsPathDoesNotPoisonCache:
    def test_save_cache_false_skips_the_file_cache(self):
        analyzer = _analyzer_with_response(
            [
                _FakeTextBlock(
                    "## MOAT CLASSIFICATION\nDurability: STRONG\n## MANAGEMENT QUALITY\nCapital Allocation: GOOD\n"
                    "## BUSINESS DURABILITY\nok\n## CURRENCY EXPOSURE\nRisk Level: LOW\n## FAIR VALUE ASSESSMENT\n"
                    "Estimated Fair Value: $10 - $12\nTarget Entry Price: $8\n## CONVICTION LEVEL\nHIGH - fine\n"
                    "## INVESTMENT SUMMARY\nok\n## KEY RISKS\n1. a\n## THESIS-BREAKING RISKS\n1. b\n"
                    "## TOTAL RETURN POTENTIAL\nok\n## DIVIDEND YIELD\n1%\n"
                )
            ]
        )
        with patch("src.analyzer.save_analysis_to_cache") as mock_save:
            analyzer.analyze_company("AAPL", "Apple", "text", use_cache=False, save_cache=False)
        mock_save.assert_not_called()


class TestThinkingPerModel:
    """Sonnet 5.5 rejects {"type": "disabled"}; the off-switch is per generation."""

    def test_sonnet_5_5_uses_between_tools(self):
        assert analyzer_mod._thinking_kwargs("claude-sonnet-5-5") == {"thinking": {"type": "between_tools"}}

    def test_older_models_still_disable(self):
        for m in ("claude-sonnet-5", "claude-haiku-4-5", "claude-opus-5"):
            assert analyzer_mod._thinking_kwargs(m) == {"thinking": {"type": "disabled"}}

    def test_models_that_cannot_disable_use_low_effort(self):
        assert analyzer_mod._thinking_kwargs("claude-opus-5-5") == {"output_config": {"effort": "low"}}
        assert "thinking" not in analyzer_mod._thinking_kwargs("claude-fable-5-1")

    def test_default_deep_model_is_sonnet_5_5(self):
        from src.config import config

        assert config.model_deep == "claude-sonnet-5-5"

    def test_deep_request_sends_between_tools_for_sonnet_5_5(self):
        analyzer = _analyzer_with_response([_FakeTextBlock("x")])
        analyzer.model_deep = "claude-sonnet-5-5"
        with patch("src.analyzer.parse_analysis"), patch("src.analyzer.save_analysis_to_cache"):
            analyzer.analyze_company("AAPL", "Apple", "text", use_cache=False)
        kwargs = analyzer.client.messages.create.call_args.kwargs
        assert kwargs["model"] == "claude-sonnet-5-5"
        assert kwargs["thinking"] == {"type": "between_tools"}
        assert "output_config" not in kwargs

    def test_batch_deep_request_sends_between_tools_for_sonnet_5_5(self):
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):  # pragma: allowlist secret
            analyzer = CompanyAnalyzer()
        analyzer.model_deep = "claude-sonnet-5-5"
        analyzer.client = MagicMock()
        batch = MagicMock()
        batch.id = "batch-1"
        analyzer.client.messages.batches.create.return_value = batch
        analyzer.client.messages.batches.results.return_value = []
        analyzer._wait_for_batch = MagicMock(return_value=batch)
        with patch("src.analyzer.get_cached_analysis", return_value=None):
            analyzer.batch_analyze_companies([{"symbol": "AAPL", "filing_text": "x"}])
        params = analyzer.client.messages.batches.create.call_args.kwargs["requests"][0]["params"]
        assert params["thinking"] == {"type": "between_tools"}


class TestRefusalHandling:
    def _refused(self, category="general_harms"):
        response = MagicMock(content=[])
        response.stop_reason = "refusal"
        response.stop_details.category = category
        return response

    def test_deep_analysis_refusal_raises_instead_of_parsing(self):
        analyzer = _analyzer_with_response([])
        analyzer.client.messages.create.return_value = self._refused()
        with pytest.raises(ValueError, match="refusal"):
            analyzer.analyze_company("AAPL", "Apple", "text", use_cache=False)

    def test_quick_screen_refusal_fails_closed(self):
        analyzer = _analyzer_with_response([])
        analyzer.client.messages.create.return_value = self._refused("cyber")
        out = analyzer.quick_screen("AAPL", "text")
        assert out["valid"] is False and out["worth_analysis"] is False

    def test_batch_refusal_is_discarded_not_cached(self):
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):  # pragma: allowlist secret
            analyzer = CompanyAnalyzer()
        analyzer.client = MagicMock()
        batch = MagicMock()
        batch.id = "batch-1"
        refused = MagicMock()
        refused.custom_id = "AAPL"
        refused.result.type = "succeeded"
        refused.result.message = self._refused()
        analyzer.client.messages.batches.create.return_value = batch
        analyzer.client.messages.batches.results.return_value = [refused]
        analyzer._wait_for_batch = MagicMock(return_value=batch)
        with (
            patch("src.analyzer.get_cached_analysis", return_value=None),
            patch("src.analyzer.save_analysis_to_cache") as mock_save,
        ):
            out = analyzer.batch_analyze_companies([{"symbol": "AAPL", "filing_text": "x"}])
        assert out == []
        mock_save.assert_not_called()
