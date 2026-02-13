"""
Analysis Parser Module

Parses Claude's structured text responses into AnalysisV2 dataclass.
Extracted from analyzer.py for testability and separation of concerns.
"""

import re
from typing import Optional


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


def extract_rating(text: str, options: list[str]) -> str:
    """Extract a rating from text by matching against valid options."""
    text_upper = text.upper()
    for option in sorted(options, key=len, reverse=True):
        if re.search(r"\b" + re.escape(option.upper()) + r"\b", text_upper):
            return option
    return options[-1]


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

    # Extract sections
    moat_section = extract_section(analysis_text, "## MOAT CLASSIFICATION", "## MANAGEMENT")
    mgmt_section = extract_section(analysis_text, "## MANAGEMENT QUALITY", "## BUSINESS DURABILITY")
    durability_section = extract_section(analysis_text, "## BUSINESS DURABILITY", "## CURRENCY")
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
    moat_durability = extract_rating(
        extract_field(moat_section, "Durability") or moat_section,
        ["STRONG", "MODERATE", "WEAK", "NONE"],
    ).lower()
    moat_risks = extract_field(moat_section, "Risks") or ""

    # Parse management
    mgmt_cap_alloc = extract_rating(
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

    # Parse currency
    domestic_str = extract_field(currency_section, "Domestic Revenue")
    intl_str = extract_field(currency_section, "International Revenue")
    domestic_pct = extract_pct(domestic_str) if domestic_str else None
    intl_pct = extract_pct(intl_str) if intl_str else None
    currency_risk = extract_rating(
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
    conviction = extract_rating(conviction_section, ["HIGH", "MEDIUM", "LOW"])

    # Parse summary, risks, return, dividend
    summary = summary_section or ""
    key_risks = extract_list(risks_section)
    thesis_risks = extract_list(thesis_risks_section)
    total_return = return_section or ""
    div_yield = extract_pct(dividend_section) if dividend_section else None

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
    """Parse a quick-screen response into a result dict."""
    lines = [line.strip() for line in text.strip().split("\n") if line.strip()]

    moat_hint = 3
    quality_hint = 3
    reason = "Unable to parse response"

    for line in lines:
        upper = line.upper()
        if upper.startswith("MOAT:"):
            try:
                moat_hint = int(line.split(":")[-1].strip()[0])
                moat_hint = max(1, min(5, moat_hint))
            except (ValueError, IndexError):
                pass
        elif upper.startswith("QUALITY:"):
            try:
                quality_hint = int(line.split(":")[-1].strip()[0])
                quality_hint = max(1, min(5, quality_hint))
            except (ValueError, IndexError):
                pass
        elif upper.startswith("REASON:"):
            reason = line.split(":", 1)[-1].strip()

    worth_analysis = (moat_hint + quality_hint) >= 6

    return {
        "symbol": symbol,
        "worth_analysis": worth_analysis,
        "moat_hint": moat_hint,
        "quality_hint": quality_hint,
        "reason": reason,
    }
