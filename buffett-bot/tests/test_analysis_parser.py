"""
Tests for src/analysis_parser.py.

These exist because the parser's failure mode was the most dangerous kind in
the whole system: every extractor degrades to the *pessimistic* end of its
scale, so an unreadable response became a confident, fully-populated verdict
that the business is worthless. That flows into tier_engine, tiers the stock C,
and the deployment engine treats C as a thesis breaker and sells it.

A parser break therefore looked exactly like a market crash, and nothing
anywhere raised. The tests below pin the guard that turns that into a loud
error, and pin the golden format so a model upgrade that drifts the output
shape fails here rather than in production.
"""

import logging

import pytest

from src.analysis_parser import (
    EXPECTED_SECTIONS,
    AnalysisParseError,
    extract_rating,
    extract_rating_checked,
    parse_analysis,
)
from tests.fixtures_analysis import truncated_analysis, well_formed_analysis, wrong_format_response


class TestStructuralGate:
    def test_well_formed_response_parses(self):
        a = parse_analysis("AAPL", "Apple Inc", well_formed_analysis(), "Technology")
        assert a.moat_durability == "strong"
        assert a.mgmt_capital_allocation == "excellent"
        assert a.conviction == "HIGH"

    def test_wrong_format_raises_rather_than_returning_a_verdict(self):
        with pytest.raises(AnalysisParseError, match="not in the expected analysis format"):
            parse_analysis("AAPL", "Apple Inc", wrong_format_response())

    def test_truncated_response_raises(self):
        """
        The max_tokens failure mode. Previously produced NONE/POOR/LOW — a
        confident sell signal manufactured from a cut-off response.
        """
        with pytest.raises(AnalysisParseError):
            parse_analysis("AAPL", "Apple Inc", truncated_analysis())

    def test_empty_response_raises(self):
        with pytest.raises(AnalysisParseError):
            parse_analysis("AAPL", "Apple Inc", "")

    def test_error_names_the_symbol_and_what_was_missing(self):
        """The message has to be actionable from a log line alone."""
        with pytest.raises(AnalysisParseError) as exc:
            parse_analysis("TSLA", "Tesla", wrong_format_response())

        msg = str(exc.value)
        assert "TSLA" in msg
        assert "sections" in msg
        assert "## MOAT CLASSIFICATION" in msg

    def test_partial_response_above_threshold_still_parses(self):
        """
        One missing section must not discard an otherwise good analysis —
        the gate is for format drift, not strictness for its own sake.
        """
        text = well_formed_analysis().replace("## DIVIDEND YIELD\n0.5%\n", "")
        a = parse_analysis("AAPL", "Apple Inc", text)
        assert a.moat_durability == "strong"

    def test_partial_response_logs_what_was_missing(self, caplog):
        text = well_formed_analysis().replace("## DIVIDEND YIELD\n0.5%\n", "")
        with caplog.at_level(logging.WARNING):
            parse_analysis("AAPL", "Apple Inc", text)

        assert "## DIVIDEND YIELD" in caplog.text


class TestPessimisticDefaults:
    """
    The defaults themselves are unchanged — they're a reasonable fallback for
    one missing field. What changed is that falling back is now visible.
    """

    def test_defaulted_rating_is_reported(self):
        assert extract_rating_checked("Durability: STRONG", ["STRONG", "WEAK", "NONE"]) == ("STRONG", True)
        assert extract_rating_checked("no rating here", ["STRONG", "WEAK", "NONE"]) == ("NONE", False)

    def test_extract_rating_keeps_its_old_signature(self):
        assert extract_rating("no rating here", ["STRONG", "WEAK", "NONE"]) == "NONE"

    def test_defaulted_ratings_are_logged(self, caplog):
        """
        A run where this fires across many tickers is parser drift, not a
        sudden collapse in business quality — so it has to be greppable.
        """
        text = well_formed_analysis().replace("Durability: STRONG", "Durability: indeterminate")
        with caplog.at_level(logging.WARNING):
            parse_analysis("AAPL", "Apple Inc", text)

        assert "pessimistic default" in caplog.text
        assert "moat_durability" in caplog.text

    def test_clean_parse_logs_no_default_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            parse_analysis("AAPL", "Apple Inc", well_formed_analysis())

        assert "pessimistic default" not in caplog.text


class TestGoldenFormat:
    """
    Pins the full field mapping. A model upgrade that reshapes the output
    should fail here, loudly, rather than quietly downgrading every holding.
    """

    @pytest.fixture
    def parsed(self):
        return parse_analysis("AAPL", "Apple Inc", well_formed_analysis(), "Technology")

    def test_moat(self, parsed):
        assert parsed.moat_type == "brand + switching costs"
        assert parsed.moat_durability == "strong"

    def test_management(self, parsed):
        assert parsed.mgmt_capital_allocation == "excellent"
        assert parsed.mgmt_insider_ownership == pytest.approx(0.03)

    def test_bear_case(self, parsed):
        assert parsed.customer_concentration_risk == "low"
        assert parsed.switching_cost_rating == 4

    def test_currency(self, parsed):
        assert parsed.domestic_revenue_pct == pytest.approx(0.60)
        assert parsed.international_revenue_pct == pytest.approx(0.40)
        assert parsed.currency_risk_level == "moderate"

    def test_fair_value_range(self, parsed):
        assert parsed.estimated_fair_value_low == pytest.approx(150.0)
        assert parsed.estimated_fair_value_high == pytest.approx(200.0)
        assert parsed.target_entry_price == pytest.approx(120.0)

    def test_lists(self, parsed):
        assert len(parsed.key_risks) == 3
        assert len(parsed.thesis_risks) == 2

    def test_dividend(self, parsed):
        assert parsed.dividend_yield_estimate == pytest.approx(0.005)

    def test_ratings_vary_with_the_response(self):
        a = parse_analysis(
            "X", "X Corp", well_formed_analysis(moat="WEAK", capital_allocation="POOR", conviction="LOW")
        )
        assert a.moat_durability == "weak"
        assert a.mgmt_capital_allocation == "poor"
        assert a.conviction == "LOW"


class TestExpectedSectionsContract:
    def test_every_expected_section_is_in_the_golden_fixture(self):
        """
        Keeps the gate and the fixture honest about each other: adding a
        section to EXPECTED_SECTIONS without updating the fixture would make
        every well-formed response start logging a spurious warning.
        """
        text = well_formed_analysis()
        missing = [h for h in EXPECTED_SECTIONS if h not in text]
        assert missing == []
