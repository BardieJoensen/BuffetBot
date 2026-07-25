"""
Shared analysis-response fixtures.

Kept in one place so the golden format lives at a single source of truth: if a
model upgrade drifts the output shape, one fixture changes rather than a
scattering of inline strings across test files.
"""


def well_formed_analysis(
    *,
    moat="STRONG",
    capital_allocation="EXCELLENT",
    conviction="HIGH",
    currency_risk="MODERATE",
    fair_value="$150 - $200",
    target_entry="$120",
) -> str:
    """A response containing every section the analysis prompt asks for."""
    return f"""## MOAT CLASSIFICATION
Type: brand + switching costs
Durability: {moat}
Risks: Competitive entry from low-cost rivals.

## MANAGEMENT QUALITY
Capital Allocation: {capital_allocation}
Insider Ownership: 3%
Summary: Disciplined buybacks, no dilutive acquisitions.

## BUSINESS DURABILITY
Recession Resilience: Revenue would decline ~10% in a severe recession.
Existential Risks: Platform disruption over a 10-year horizon.
10-Year Outlook: Larger and more profitable.

## BEAR CASE
Customer Concentration: LOW
Switching Cost Test: 4
Regulatory/Tech Risk: Antitrust scrutiny in the EU.
Patent/IP Dependency: NO
Bear Case Summary: Multiple compression if growth normalizes.

## CURRENCY EXPOSURE
Domestic Revenue: 60%
International Revenue: 40%
Risk Level: {currency_risk}
Confidence: MODERATE

## FAIR VALUE ASSESSMENT
Estimated Fair Value: {fair_value}
Target Entry Price: {target_entry}

## CONVICTION LEVEL
{conviction} - Durable franchise at a reasonable price.

## INVESTMENT SUMMARY
A high-quality compounder worth owning on weakness.

## KEY RISKS
1. Margin compression
2. Regulatory action
3. Key-person dependency

## THESIS-BREAKING RISKS
1. Sustained share loss to a lower-cost entrant
2. Capital allocation turning acquisitive

## TOTAL RETURN POTENTIAL
High single-digit to low double-digit annualized over 5-10 years.

## DIVIDEND YIELD
0.5%
"""


def truncated_analysis() -> str:
    """
    A response cut off partway — the shape a max_tokens truncation produces.

    Historically this parsed "successfully" into NONE moat / POOR management /
    LOW conviction, i.e. a confident verdict that the business is worthless.
    """
    return """## MOAT CLASSIFICATION
Type: brand + switching costs
Durability: STRONG
Risks: Competitive entry from low-cost

## MANAGEMENT QUALITY
Capital Allocation: EXCEL"""


def wrong_format_response() -> str:
    """A plausible-looking response in entirely the wrong shape."""
    return (
        "I've analyzed the company and here are my thoughts. "
        "The business appears to have a reasonably durable competitive position, "
        "though I'd want to see more detail on the capital allocation record before "
        "forming a firm view on valuation."
    )
