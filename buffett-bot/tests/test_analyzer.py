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
    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
        analyzer = CompanyAnalyzer()
    analyzer.client = MagicMock()
    analyzer.client.messages.create.return_value = MagicMock(content=content)
    return analyzer


class TestModelConfiguration:
    def test_models_come_from_config(self):
        from src.config import config

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
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

    def test_fails_open_when_no_text_block(self):
        """
        Documents the existing fail-open contract: a screening failure must let
        the stock through to deeper analysis rather than silently dropping it.
        """
        analyzer = _analyzer_with_response([_FakeThinkingBlock()])

        result = analyzer.quick_screen("AAPL", "some filing text")

        assert result["worth_analysis"] is True
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

    def test_defaults_to_hold(self):
        analyzer = self._response("RED FLAGS DETECTED: NO\nEXPLANATION: routine")

        result = analyzer.check_news_for_red_flags("AAPL", "thesis", [], "news")

        assert result["has_red_flags"] is False
        assert result["recommendation"] == "HOLD"

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
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
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

    def test_fails_open_on_errored_result(self):
        errored = MagicMock()
        errored.custom_id = "AAPL"
        errored.result.type = "errored"
        analyzer = self._batch_analyzer([errored])

        out = analyzer.batch_quick_screen([("AAPL", "filing text")])

        assert out[0]["worth_analysis"] is True

    def test_preserves_input_order(self):
        analyzer = self._batch_analyzer(
            [
                self._succeeded("MSFT", [_FakeTextBlock("MOAT: 5\nQUALITY: 5\nREASON: b")]),
                self._succeeded("AAPL", [_FakeTextBlock("MOAT: 4\nQUALITY: 4\nREASON: a")]),
            ]
        )

        out = analyzer.batch_quick_screen([("AAPL", "t1"), ("MSFT", "t2")])

        assert [r["symbol"] for r in out] == ["AAPL", "MSFT"]
