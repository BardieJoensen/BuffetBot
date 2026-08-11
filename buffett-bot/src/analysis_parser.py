"""
Analysis Parser Module

Parses Claude's structured text responses into AnalysisV2 dataclass.
Extracted from analyzer.py for testability and separation of concerns.

FAIL-FAST CONTRACT
------------------
Every extractor here degrades to a benign-looking default when it finds
nothing — and because the defaults are the *pessimistic* end of each scale
(NONE moat, POOR capital allocation, LOW conviction), a response the parser
cannot read produces a fully-populated, entirely plausible analysis saying the
business is worthless. That flows into tier_engine, tiers everything C, and the
deployment engine reads C as a thesis breaker and sells. A parser break would
therefore look exactly like a market crash, with nothing in the logs.

So structural failure is now an exception, not a default: if the response does
not contain enough of the expected headers to be the format we asked for,
parse_analysis raises AnalysisParseError. Individual fields still degrade
gracefully — one missing section shouldn't discard a good analysis — but every
defaulted rating is counted and logged so systemic parser drift is visible.
"""

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# Headers the analysis prompt asks for. Used to decide whether a response is
# structurally the thing we requested at all.
EXPECTED_SECTIONS = (
    "## MOAT CLASSIFICATION",
    "## MANAGEMENT QUALITY",
    "## BUSINESS DURABILITY",
    "## CURRENCY EXPOSURE",
    "## FAIR VALUE ASSESSMENT",
    "## CONVICTION LEVEL",
    "## INVESTMENT SUMMARY",
    "## KEY RISKS",
    "## THESIS-BREAKING",
    "## TOTAL RETURN POTENTIAL",
    "## DIVIDEND YIELD",
)

# Below this fraction of expected headers, treat the response as not being in
# the requested format rather than as a pessimistic company. Deliberately
# lenient: the goal is catching format drift and truncation, not policing a
# model that merged or renamed one section.
MIN_SECTION_MATCH_RATIO = 0.5


class AnalysisParseError(ValueError):
    """
    Raised when a response is not structurally the analysis format.

    Distinct from a partial parse: this means returning *anything* would be
    fabricating an opinion the model never expressed.
    """


class QuickScreenParseError(ValueError):
    """Raised when a quick-screen response is incomplete or malformed."""


def extract_section(text: str, header: str, next_header: Optional[str] = None) -> str:
    """Extract a section between two markdown headers."""
    start = text.find(header)
    if start == -1:
        return ""
    start += len(header)
    if next_header:
        end = text.find(next_header, start)
        if end == -1:
            end = len(text)
    else:
        end = len(text)
    return text[start:end].strip()


def extract_field(section: str, field_name: str) -> str:
    """Extract a labeled field like 'Type: brand + switching costs'."""
    for line in section.split("\n"):
        stripped = line.strip()
        if stripped.lower().startswith(field_name.lower() + ":"):
            return stripped.split(":", 1)[1].strip()
    return ""


def extract_rating_checked(text: str, options: list[str]) -> tuple[str, bool]:
    """
    Extract a rating, reporting whether it actually matched.

    Returns (value, matched). On no match the value is options[-1] — the
    pessimistic end — and matched is False. Callers that care about the
    difference between "the model said WEAK" and "the parser found nothing"
    need that second element; extract_rating throws it away.
    """
    text_upper = text.upper()
    for option in sorted(options, key=len, reverse=True):
        if re.search(r"\b" + re.escape(option.upper()) + r"\b", text_upper):
            return option, True
    return options[-1], False


def extract_rating(text: str, options: list[str]) -> str:
    """Extract a rating from text by matching against valid options."""
    return extract_rating_checked(text, options)[0]


def extract_list(text: str) -> list[str]:
    """Extract bullet-point items from text."""
    items = []
    for line in text.split("\n"):
        line = line.strip()
        if line.startswith(("-", "\u2022", "*")) or (line and line[0].isdigit()):
            cleaned = line.lstrip("-\u2022*0123456789.) ")
            cleaned = re.sub(r"\*{1,2}(.+?)\*{1,2}", r"\1", cleaned)
            if cleaned:
                items.append(cleaned)
    return items


def extract_dollar(text: str) -> Optional[float]:
    """Extract a dollar amount from text."""
    match = re.search(r"\$[\d,]+(?:\.\d+)?", text)
    if match:
        return float(match.group().replace("$", "").replace(",", ""))
    return None


def extract_pct(text: str) -> Optional[float]:
    """Extract a percentage from text, returned as decimal."""
    match = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
    if match:
        return float(match.group(1)) / 100
    return None


def parse_analysis(symbol: str, company_name: str, analysis_text: str, sector: str = ""):
    """
    Parse Claude's v2 response into AnalysisV2.

    Import AnalysisV2 here to avoid circular imports at module level.
    """
    from .analyzer import AnalysisV2

    # Structural gate. A response that isn't in the requested format must not
    # be turned into a pessimistic-but-valid opinion — see the module docstring.
    found = [h for h in EXPECTED_SECTIONS if h in analysis_text]
    confidence = len(found) / len(EXPECTED_SECTIONS)
    if confidence < MIN_SECTION_MATCH_RATIO:
        missing = [h for h in EXPECTED_SECTIONS if h not in analysis_text]
        raise AnalysisParseError(
            f"{symbol}: response is not in the expected analysis format — "
            f"found {len(found)}/{len(EXPECTED_SECTIONS)} sections "
            f"(missing: {', '.join(missing[:5])}{'...' if len(missing) > 5 else ''}). "
            f"First 200 chars: {analysis_text[:200]!r}"
        )
    if confidence < 1.0:
        logger.warning(
            "%s: analysis missing %d/%d expected section(s): %s",
            symbol,
            len(EXPECTED_SECTIONS) - len(found),
            len(EXPECTED_SECTIONS),
            ", ".join(h for h in EXPECTED_SECTIONS if h not in analysis_text),
        )

    # Ratings that fell back to their pessimistic default rather than matching.
    defaulted: list[str] = []

    def _rated(field: str, text: str, options: list[str]) -> str:
        value, matched = extract_rating_checked(text, options)
        if not matched:
            defaulted.append(field)
        return value

    # Extract sections
    moat_section = extract_section(analysis_text, "## MOAT CLASSIFICATION", "## MANAGEMENT")
    mgmt_section = extract_section(analysis_text, "## MANAGEMENT QUALITY", "## BUSINESS DURABILITY")
    # Bear case may be absent in old cached responses — fall back gracefully
    has_bear_case = "## BEAR CASE" in analysis_text
    durability_section = extract_section(
        analysis_text, "## BUSINESS DURABILITY", "## BEAR CASE" if has_bear_case else "## CURRENCY"
    )
    bear_section = extract_section(analysis_text, "## BEAR CASE", "## CURRENCY") if has_bear_case else ""
    currency_section = extract_section(analysis_text, "## CURRENCY EXPOSURE", "## FAIR VALUE")
    fv_section = extract_section(analysis_text, "## FAIR VALUE ASSESSMENT", "## CONVICTION")
    conviction_section = extract_section(analysis_text, "## CONVICTION LEVEL", "## INVESTMENT SUMMARY")
    summary_section = extract_section(analysis_text, "## INVESTMENT SUMMARY", "## KEY RISKS")
    risks_section = extract_section(analysis_text, "## KEY RISKS", "## THESIS")
    thesis_risks_section = extract_section(analysis_text, "## THESIS-BREAKING", "## TOTAL RETURN")
    return_section = extract_section(analysis_text, "## TOTAL RETURN POTENTIAL", "## DIVIDEND")
    dividend_section = extract_section(analysis_text, "## DIVIDEND YIELD", None)

    # Parse moat
    moat_type = extract_field(moat_section, "Type") or "unknown"
    moat_durability = _rated(
        "moat_durability",
        extract_field(moat_section, "Durability") or moat_section,
        ["STRONG", "MODERATE", "WEAK", "NONE"],
    ).lower()
    moat_risks = extract_field(moat_section, "Risks") or ""

    # Parse management
    mgmt_cap_alloc = _rated(
        "mgmt_capital_allocation",
        extract_field(mgmt_section, "Capital Allocation") or mgmt_section,
        ["EXCELLENT", "GOOD", "MIXED", "POOR"],
    ).lower()
    insider_str = extract_field(mgmt_section, "Insider Ownership")
    mgmt_insider = extract_pct(insider_str) if insider_str else None
    mgmt_summary = extract_field(mgmt_section, "Summary") or mgmt_section

    # Parse durability
    recession = extract_field(durability_section, "Recession Resilience") or durability_section
    existential = extract_field(durability_section, "Existential Risks") or ""
    outlook = extract_field(durability_section, "10-Year Outlook") or extract_field(durability_section, "Outlook") or ""

    # Parse bear case (gracefully handles missing section from old cached responses)
    customer_concentration = (
        extract_rating(
            extract_field(bear_section, "Customer Concentration") or "",
            ["LOW", "MODERATE", "HIGH"],
        ).lower()
        if bear_section
        else ""
    )
    switching_cost_str = extract_field(bear_section, "Switching Cost Test") or ""
    switching_cost_rating = 0
    if switching_cost_str:
        sc_match = re.search(r"[1-5]", switching_cost_str)
        if sc_match:
            switching_cost_rating = int(sc_match.group())
    regulatory_tech_risk = extract_field(bear_section, "Regulatory/Tech Risk") or ""
    patent_ip_dependency = extract_field(bear_section, "Patent/IP Dependency") or ""
    bear_case_summary = extract_field(bear_section, "Bear Case Summary") or bear_section

    # Parse currency
    domestic_str = extract_field(currency_section, "Domestic Revenue")
    intl_str = extract_field(currency_section, "International Revenue")
    domestic_pct = extract_pct(domestic_str) if domestic_str else None
    intl_pct = extract_pct(intl_str) if intl_str else None
    currency_risk = _rated(
        "currency_risk_level",
        extract_field(currency_section, "Risk Level") or currency_section,
        ["LOW", "MODERATE", "HIGH"],
    ).lower()
    currency_conf = extract_rating(
        extract_field(currency_section, "Confidence") or currency_section,
        ["HIGH", "MODERATE", "LOW"],
    ).lower()

    # Parse fair value
    fv_line = (
        extract_field(fv_section, "Estimated Fair Value")
        or extract_field(fv_section, "Fair Value Range")
        or extract_field(fv_section, "Fair Value")
        or fv_section
    )
    fv_amounts = re.findall(r"\$[\d,]+(?:\.\d+)?", fv_line)
    fv_low = float(fv_amounts[0].replace("$", "").replace(",", "")) if len(fv_amounts) >= 1 else None
    fv_high = float(fv_amounts[1].replace("$", "").replace(",", "")) if len(fv_amounts) >= 2 else fv_low

    target_str = extract_field(fv_section, "Target Entry Price") or ""
    target_entry = extract_dollar(target_str)

    current_price_str = extract_field(fv_section, "Current Price") or ""
    current_price = extract_dollar(current_price_str)

    # Parse conviction
    conviction = _rated("conviction", conviction_section, ["HIGH", "MEDIUM", "LOW"])

    # Parse summary, risks, return, dividend
    summary = summary_section or ""
    key_risks = extract_list(risks_section)
    thesis_risks = extract_list(thesis_risks_section)
    total_return = return_section or ""
    div_yield = extract_pct(dividend_section) if dividend_section else None

    if defaulted:
        # Every one of these silently became the worst value on its scale. Say
        # so: a run where this fires across many tickers is parser drift, not a
        # sudden collapse in business quality.
        logger.warning(
            "%s: %d rating(s) fell back to the pessimistic default (no match found): %s",
            symbol,
            len(defaulted),
            ", ".join(defaulted),
        )

    return AnalysisV2(
        symbol=symbol,
        company_name=company_name,
        sector=sector,
        moat_type=moat_type,
        moat_durability=moat_durability,
        moat_risks=moat_risks,
        mgmt_insider_ownership=mgmt_insider,
        mgmt_capital_allocation=mgmt_cap_alloc,
        mgmt_summary=mgmt_summary,
        recession_resilience=recession,
        existential_risks=existential,
        outlook_10yr=outlook,
        customer_concentration_risk=customer_concentration,
        switching_cost_rating=switching_cost_rating,
        regulatory_tech_risk=regulatory_tech_risk,
        patent_ip_dependency=patent_ip_dependency,
        bear_case_summary=bear_case_summary,
        domestic_revenue_pct=domestic_pct,
        international_revenue_pct=intl_pct,
        currency_risk_level=currency_risk,
        currency_confidence=currency_conf,
        estimated_fair_value_low=fv_low,
        estimated_fair_value_high=fv_high,
        target_entry_price=target_entry,
        current_price=current_price,
        conviction=conviction,
        summary=summary,
        dividend_yield_estimate=div_yield,
        total_return_potential=total_return,
        key_risks=key_risks,
        thesis_risks=thesis_risks,
    )


def parse_quick_screen(text: str, symbol: str) -> dict:
    """Parse and validate an exact three-line quick-screen response."""
    lines = [line.strip() for line in text.strip().split("\n") if line.strip()]

    moat_hint: Optional[int] = None
    quality_hint: Optional[int] = None
    reason = ""

    for line in lines:
        upper = line.upper()
        if upper.startswith("MOAT:"):
            try:
                moat_hint = int(line.split(":", 1)[1].strip())
            except (ValueError, IndexError) as exc:
                raise QuickScreenParseError(f"{symbol}: invalid MOAT line: {line!r}") from exc
        elif upper.startswith("QUALITY:"):
            try:
                quality_hint = int(line.split(":", 1)[1].strip())
            except (ValueError, IndexError) as exc:
                raise QuickScreenParseError(f"{symbol}: invalid QUALITY line: {line!r}") from exc
        elif upper.startswith("REASON:"):
            reason = line.split(":", 1)[-1].strip()

    if (
        moat_hint is None
        or quality_hint is None
        or moat_hint not in range(1, 6)
        or quality_hint not in range(1, 6)
        or not reason
    ):
        raise QuickScreenParseError(
            f"{symbol}: expected MOAT and QUALITY in 1..5 plus a non-empty REASON; "
            f"got moat={moat_hint!r}, quality={quality_hint!r}, reason={reason!r}"
        )

    worth_analysis = (moat_hint + quality_hint) >= 6

    return {
        "symbol": symbol,
        "worth_analysis": worth_analysis,
        "moat_hint": moat_hint,
        "quality_hint": quality_hint,
        "reason": reason,
        "valid": True,
    }
