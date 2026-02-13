"""
LLM Analyzer Module

Uses Claude API for qualitative analysis of companies.
The LLM reads documents and provides assessments - it does NOT do math.

v2.0 — Shifted from "should I buy this stock" to "assess this business's
quality, durability, and fair entry price." Outputs AnalysisV2 schema with
moat classification, management quality, durability, currency exposure,
and fair value estimates.

Responsibilities:
- Assess competitive moat quality and durability
- Evaluate management capital allocation
- Analyze business durability (10-20 year horizon)
- Estimate currency exposure for Danish ASK investors
- Provide fair value range and target entry price
- Monitor news for thesis-breaking events

COST OPTIMIZATION:
- Sonnet for deep analysis (~$0.05 per stock)
- Haiku for news monitoring (~$0.002 per check) - 20x cheaper
- Prompt caching: static system prompts cached across calls (20-30% savings)
- Batch API: 50% discount on all requests submitted together
- Analysis caching to avoid re-analyzing same stocks
- Reduced input truncation limits
"""

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Union, cast

from anthropic import Anthropic
from anthropic.types import TextBlock

from .analysis_parser import parse_analysis, parse_quick_screen

logger = logging.getLogger(__name__)

# Default cache directory for analysis results
DEFAULT_CACHE_DIR = Path("data/analyses")

# Runtime cache dir (can be overridden via set_cache_dir)
_cache_dir = DEFAULT_CACHE_DIR

# ─────────────────────────────────────────────────────────────
# System prompts (cached across API calls for token savings)
# ─────────────────────────────────────────────────────────────

ANALYSIS_SYSTEM_PROMPT = """\
You are a quality-focused investment analyst in the style of Warren Buffett.
Your job is to assess business quality, durability, and fair entry price —
NOT to make short-term trading recommendations.

A wonderful business at a high price still belongs on the watchlist.
Valuation determines WHEN to buy, not WHETHER to research.

Provide your analysis in the following format:

## MOAT CLASSIFICATION
Type: [e.g., "brand + switching costs", "network effects + cost advantage"]
Durability: [STRONG / MODERATE / WEAK / NONE]
Risks: [What could erode the moat? Be specific about competitive threats, disruption risks, regulatory changes]

## MANAGEMENT QUALITY
Capital Allocation: [EXCELLENT / GOOD / MIXED / POOR]
Insider Ownership: [Estimate percentage if known, otherwise "Unknown (estimate: ~X%)" with confidence note]
Summary: [Assessment of buyback discipline, acquisition track record, compensation alignment, candor]

## BUSINESS DURABILITY
Recession Resilience: [How would this business perform in a severe recession? Revenue impact estimate]
Existential Risks: [What could kill this business in 10-20 years? Technology disruption, regulation, etc.]
10-Year Outlook: [Will this business be larger and more profitable in 10 years? Why or why not?]

## CURRENCY EXPOSURE
Domestic Revenue: [X% — estimate based on your knowledge of the company]
International Revenue: [Y% — estimate]
Risk Level: [LOW / MODERATE / HIGH — considering USD/DKK exposure for a Danish investor]
Confidence: [HIGH / MODERATE / LOW — how confident are you in these revenue split estimates?]

## FAIR VALUE ASSESSMENT
Estimated Fair Value: $LOW - $HIGH [range based on your qualitative assessment of business quality, growth, and comparable companies — NOT a precise DCF]
Target Entry Price: $PRICE [fair value minus 20-30% margin of safety]

## CONVICTION LEVEL
[HIGH / MEDIUM / LOW] - [One sentence explanation]

## INVESTMENT SUMMARY
[2-3 sentences: what makes this business wonderful (or not), and what would make it a compelling buy]

## KEY RISKS
1. [Risk 1]
2. [Risk 2]
3. [Risk 3]

## THESIS-BREAKING RISKS
[What specific events would invalidate an investment thesis? Be specific.]
1. [Event that would signal "sell immediately"]
2. [Event that would signal "sell immediately"]

## TOTAL RETURN POTENTIAL
[Assessment of combined price appreciation + dividend yield potential over 5-10 years]

## DIVIDEND YIELD
[Current yield percentage, or "N/A" if no dividend]

IMPORTANT:
- Do NOT build complex DCF models — estimate fair value qualitatively
- Focus on business quality and durability, not short-term catalysts
- Be skeptical and highlight genuine risks
- For currency exposure, note that the investor uses a Danish ASK account (DKK)
- A high P/E alone is NOT a reason for low conviction — wonderful businesses often trade at premium multiples"""

QUICK_SCREEN_SYSTEM_PROMPT = """\
You are a quality-focused investment analyst. Quickly assess whether this \
company shows signs of a durable competitive advantage and consistent \
financial performance — regardless of current valuation.
Rate moat strength 1-5, business quality 1-5, one-sentence reason.
Respond in exactly 3 lines:
MOAT: <1-5>
QUALITY: <1-5>
REASON: <one sentence focusing on business durability, not price>"""

OPUS_SECOND_OPINION_PROMPT = """\
You are a contrarian investment analyst providing a "second opinion" review.
You have been given a prior analyst's assessment of a company. Your job is to:

1. Challenge the MOAT assessment — is the competitive advantage as durable as claimed?
2. Challenge the DURABILITY thesis — what could disrupt this business in 5-10 years?
3. Assess whether the management quality rating is justified
4. Evaluate whether the fair value estimate is reasonable
5. Consider macro/secular headwinds the prior analyst may have ignored

Respond in this exact format:

## AGREEMENT
[AGREE / PARTIALLY_AGREE / DISAGREE] with the prior analyst's thesis

## OPUS CONVICTION
[HIGH / MEDIUM / LOW] — your independent conviction level

## CONTRARIAN RISKS
1. [Risk or concern the prior analyst missed or underweighted — focus on moat erosion]
2. [Another risk — focus on durability threats]
3. [Another risk — focus on management or valuation]

## ADDITIONAL INSIGHTS
[2-3 sentences with observations the prior analyst missed — could be positive or negative]

## SUMMARY
[2-3 sentence overall second opinion. Be direct and honest.]

IMPORTANT:
- Be genuinely critical, not just agreeable
- If the prior analyst is wrong, say so clearly
- Focus specifically on whether the MOAT and DURABILITY assessments are realistic
- Challenge the fair value estimate if it seems too optimistic or pessimistic
- Consider the current macro environment"""

NEWS_MONITOR_SYSTEM_PROMPT = """\
You are monitoring stock positions for potential red flags.
Analyze news and determine if there are thesis-breaking events.

Respond in this format:
RED FLAGS DETECTED: [YES / NO]
CONCERNING ITEMS:
- [item 1 if any]
- [item 2 if any]
RECOMMENDATION: [HOLD / REVIEW / SELL]
EXPLANATION: [1-2 sentences]"""


def set_cache_dir(path: Path):
    """Override the analysis cache directory (e.g. for permission fallback)"""
    global _cache_dir
    _cache_dir = path
    logger.info(f"Analysis cache dir set to: {_cache_dir}")


class MoatRating(Enum):
    WIDE = "wide"  # Strong, durable competitive advantage
    NARROW = "narrow"  # Some advantage, but less durable
    NONE = "none"  # No meaningful competitive advantage


class ManagementRating(Enum):
    EXCELLENT = "excellent"  # Aligned, competent, honest
    ADEQUATE = "adequate"  # Acceptable
    POOR = "poor"  # Red flags present


# ─────────────────────────────────────────────────────────────
# v1 analysis dataclass (kept for backward compatibility during migration)
# ─────────────────────────────────────────────────────────────


@dataclass
class QualitativeAnalysis:
    """v1 LLM-generated qualitative assessment of a company (legacy)"""

    symbol: str
    company_name: str

    # Moat Assessment
    moat_rating: MoatRating
    moat_sources: list[str]  # e.g., ["switching costs", "network effects"]
    moat_explanation: str

    # Management Assessment
    management_rating: ManagementRating
    management_notes: str
    insider_ownership: Optional[str]

    # Business Quality
    business_summary: str
    competitive_position: str

    # Risks
    key_risks: list[str]
    thesis_risks: list[str]  # What would break the investment thesis

    # Overall
    investment_thesis: str
    conviction_level: str  # HIGH, MEDIUM, LOW

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "company_name": self.company_name,
            "moat": {
                "rating": self.moat_rating.value,
                "sources": self.moat_sources,
                "explanation": self.moat_explanation,
            },
            "management": {
                "rating": self.management_rating.value,
                "notes": self.management_notes,
                "insider_ownership": self.insider_ownership,
            },
            "business_summary": self.business_summary,
            "competitive_position": self.competitive_position,
            "risks": {"key_risks": self.key_risks, "thesis_risks": self.thesis_risks},
            "investment_thesis": self.investment_thesis,
            "conviction_level": self.conviction_level,
        }


# ─────────────────────────────────────────────────────────────
# v2 analysis dataclass
# ─────────────────────────────────────────────────────────────


@dataclass
class AnalysisV2:
    """
    v2 LLM analysis — quality-focused with moat, durability,
    currency exposure, and fair value assessment.

    Provides backward-compatible properties so downstream consumers
    that expect QualitativeAnalysis attributes still work.
    """

    symbol: str
    company_name: str
    sector: str

    # Moat Classification
    moat_type: str  # e.g., "brand + switching costs"
    moat_durability: str  # strong / moderate / weak / none
    moat_risks: str  # what could erode it

    # Management Quality
    mgmt_insider_ownership: Optional[float]  # percentage as decimal, None if unknown
    mgmt_capital_allocation: str  # excellent / good / mixed / poor
    mgmt_summary: str

    # Business Durability
    recession_resilience: str
    existential_risks: str
    outlook_10yr: str

    # Currency Exposure
    domestic_revenue_pct: Optional[float]
    international_revenue_pct: Optional[float]
    currency_risk_level: str  # low / moderate / high
    currency_confidence: str  # high / moderate / low

    # Fair Value Assessment
    estimated_fair_value_low: Optional[float]
    estimated_fair_value_high: Optional[float]
    target_entry_price: Optional[float]
    current_price: Optional[float]

    # Overall
    conviction: str  # HIGH / MEDIUM / LOW
    summary: str
    dividend_yield_estimate: Optional[float] = None
    total_return_potential: str = ""

    # Risk fields (also output by v2 prompt)
    key_risks: list[str] = field(default_factory=list)
    thesis_risks: list[str] = field(default_factory=list)

    # --- Backward compatibility properties (duck-type as QualitativeAnalysis) ---

    @property
    def moat_rating(self) -> MoatRating:
        mapping = {"strong": MoatRating.WIDE, "moderate": MoatRating.NARROW}
        return mapping.get(self.moat_durability.lower(), MoatRating.NONE)

    @property
    def moat_sources(self) -> list[str]:
        return [s.strip() for s in self.moat_type.split("+") if s.strip()]

    @property
    def moat_explanation(self) -> str:
        return self.moat_risks

    @property
    def management_rating(self) -> ManagementRating:
        mapping = {
            "excellent": ManagementRating.EXCELLENT,
            "good": ManagementRating.ADEQUATE,
            "mixed": ManagementRating.ADEQUATE,
        }
        return mapping.get(self.mgmt_capital_allocation.lower(), ManagementRating.POOR)

    @property
    def management_notes(self) -> str:
        return self.mgmt_summary

    @property
    def insider_ownership(self) -> Optional[str]:
        if self.mgmt_insider_ownership is not None:
            return f"{self.mgmt_insider_ownership:.1%}"
        return None

    @property
    def business_summary(self) -> str:
        return self.summary

    @property
    def competitive_position(self) -> str:
        return f"Moat: {self.moat_type} ({self.moat_durability})"

    @property
    def investment_thesis(self) -> str:
        return self.summary

    @property
    def conviction_level(self) -> str:
        return self.conviction.upper()

    def to_dict(self) -> dict:
        return {
            "schema_version": "v2",
            "symbol": self.symbol,
            "company_name": self.company_name,
            "sector": self.sector,
            "moat": {
                "type": self.moat_type,
                "durability": self.moat_durability,
                "risks": self.moat_risks,
            },
            "management": {
                "insider_ownership": self.mgmt_insider_ownership,
                "capital_allocation": self.mgmt_capital_allocation,
                "summary": self.mgmt_summary,
            },
            "durability": {
                "recession_resilience": self.recession_resilience,
                "existential_risks": self.existential_risks,
                "outlook_10yr": self.outlook_10yr,
            },
            "currency_exposure": {
                "domestic_revenue_pct": self.domestic_revenue_pct,
                "international_revenue_pct": self.international_revenue_pct,
                "risk_level": self.currency_risk_level,
                "confidence": self.currency_confidence,
            },
            "valuation": {
                "estimated_fair_value_low": self.estimated_fair_value_low,
                "estimated_fair_value_high": self.estimated_fair_value_high,
                "target_entry_price": self.target_entry_price,
                "current_price": self.current_price,
            },
            "conviction": self.conviction,
            "summary": self.summary,
            "dividend_yield": self.dividend_yield_estimate,
            "total_return_potential": self.total_return_potential,
            "key_risks": self.key_risks,
            "thesis_risks": self.thesis_risks,
        }


def get_cached_analysis(symbol: str, max_age_days: int = 30) -> Optional[dict]:
    """Return cached analysis if recent enough"""
    cache_file = _cache_dir / f"{symbol}.json"
    if cache_file.exists():
        try:
            data = json.loads(cache_file.read_text())
            analyzed_date = datetime.fromisoformat(data.get("analyzed_at", "2000-01-01"))
            if (datetime.now() - analyzed_date).days < max_age_days:
                logger.info(f"Using cached analysis for {symbol} ({(datetime.now() - analyzed_date).days} days old)")
                return data
        except Exception as e:
            logger.warning(f"Error reading cache for {symbol}: {e}")
    return None


def save_analysis_to_cache(symbol: str, analysis: dict):
    """Cache analysis result"""
    try:
        _cache_dir.mkdir(parents=True, exist_ok=True)
        analysis["analyzed_at"] = datetime.now().isoformat()
        (_cache_dir / f"{symbol}.json").write_text(json.dumps(analysis, indent=2))
        logger.info(f"Cached analysis for {symbol}")
    except Exception as e:
        logger.warning(f"Failed to cache analysis for {symbol}: {e}")


class CompanyAnalyzer:
    """
    Uses Claude to perform qualitative company analysis.

    Key principle: LLM does reading and reasoning, NOT calculations.

    Cost optimization:
    - model_deep (Sonnet): For analysis, ~$0.05/stock
    - model_light (Haiku): For news monitoring, ~$0.002/check (20x cheaper)
    """

    # Input truncation limits (reduced from original to save costs)
    # 15k chars ≈ 4k tokens, 8k chars ≈ 2k tokens, 5k chars ≈ 1.2k tokens
    MAX_FILING_CHARS = 15000
    MAX_TRANSCRIPT_CHARS = 8000
    MAX_NEWS_CHARS = 5000

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY not found")

        self.client = Anthropic(api_key=self.api_key)

        # Three models: Opus for second opinion, Sonnet for deep analysis, Haiku for simple tasks
        self.model_opus = "claude-opus-4-6"  # For contrarian second opinion (~$0.30/stock)
        self.model_deep = "claude-sonnet-4-5-20250929"  # For deep analysis
        self.model_light = "claude-haiku-4-5-20251001"  # For news monitoring (20x cheaper)

    def analyze_company(
        self,
        symbol: str,
        company_name: str,
        filing_text: str,  # 10-K summary or full text
        earnings_transcript: Optional[str] = None,
        recent_news: Optional[str] = None,
        use_cache: bool = True,
        cache_max_age_days: int = 30,
        sector: str = "",
    ) -> AnalysisV2:
        """
        Perform deep qualitative analysis of a company.

        Returns AnalysisV2 which is duck-type compatible with QualitativeAnalysis.
        """
        # Check cache first to avoid expensive API calls
        if use_cache:
            cached = get_cached_analysis(symbol, cache_max_age_days)
            if cached:
                return self._dict_to_analysis(cached)

        user_prompt = self._build_analysis_user_prompt(
            symbol, company_name, filing_text, earnings_transcript, recent_news
        )

        logger.info(f"Analyzing {symbol} with Claude (Sonnet)...")

        response = self.client.messages.create(
            model=self.model_deep,
            max_tokens=4096,
            system=[
                {
                    "type": "text",
                    "text": ANALYSIS_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_prompt}],
        )

        # Parse the response
        block = response.content[0]
        assert isinstance(block, TextBlock)
        analysis_text: str = block.text
        analysis = parse_analysis(symbol, company_name, analysis_text, sector)

        # Cache the result
        save_analysis_to_cache(symbol, analysis.to_dict())

        return analysis

    def opus_second_opinion(
        self,
        symbol: str,
        company_name: str,
        filing_text: str,
        sonnet_analysis: Union[QualitativeAnalysis, AnalysisV2],
        use_cache: bool = True,
    ) -> dict:
        """
        Run Opus as a contrarian reviewer of Sonnet's analysis.

        Cost: ~$0.30 per stock (only run on top 3-5 BUY picks).
        Accepts both v1 QualitativeAnalysis and v2 AnalysisV2.
        """
        # Check cache
        if use_cache:
            cache_file = _cache_dir / f"{symbol}_opus.json"
            if cache_file.exists():
                try:
                    data = json.loads(cache_file.read_text())
                    analyzed_date = datetime.fromisoformat(data.get("analyzed_at", "2000-01-01"))
                    if (datetime.now() - analyzed_date).days < 30:
                        logger.info(f"Using cached Opus opinion for {symbol}")
                        return data
                except Exception as e:
                    logger.warning(f"Error reading Opus cache for {symbol}: {e}")

        # Build user prompt with Sonnet's analysis as context
        sonnet_summary = (
            f"PRIOR ANALYST ASSESSMENT FOR {company_name} ({symbol}):\n"
            f"- Moat: {sonnet_analysis.moat_rating.value.upper()} "
            f"({', '.join(sonnet_analysis.moat_sources[:3])})\n"
            f"- Management: {sonnet_analysis.management_rating.value.upper()}\n"
            f"- Conviction: {sonnet_analysis.conviction_level}\n"
            f"- Thesis: {sonnet_analysis.investment_thesis[:300]}\n"
            f"- Key Risks: {'; '.join(sonnet_analysis.key_risks[:3])}\n"
            f"- Business: {sonnet_analysis.business_summary[:300]}"
        )

        # Include v2-specific context if available
        if hasattr(sonnet_analysis, "moat_risks") and sonnet_analysis.moat_risks:
            sonnet_summary += f"\n- Moat Erosion Risks: {sonnet_analysis.moat_risks[:200]}"
        if hasattr(sonnet_analysis, "recession_resilience") and sonnet_analysis.recession_resilience:
            sonnet_summary += f"\n- Recession Resilience: {sonnet_analysis.recession_resilience[:200]}"
        if hasattr(sonnet_analysis, "outlook_10yr") and sonnet_analysis.outlook_10yr:
            sonnet_summary += f"\n- 10-Year Outlook: {sonnet_analysis.outlook_10yr[:200]}"
        if hasattr(sonnet_analysis, "estimated_fair_value_low") and sonnet_analysis.estimated_fair_value_low:
            fv_low = sonnet_analysis.estimated_fair_value_low
            fv_high = getattr(sonnet_analysis, "estimated_fair_value_high", fv_low)
            sonnet_summary += f"\n- Fair Value Range: ${fv_low:,.0f} - ${fv_high:,.0f}"
        if hasattr(sonnet_analysis, "target_entry_price") and sonnet_analysis.target_entry_price:
            sonnet_summary += f"\n- Target Entry Price: ${sonnet_analysis.target_entry_price:,.0f}"

        user_prompt = f"""{sonnet_summary}

=== COMPANY FILING DATA ===
{filing_text[: self.MAX_FILING_CHARS]}

Based on the filing data and the prior analyst's assessment above, provide your contrarian second opinion.
Focus especially on whether the moat and durability assessments are realistic."""

        logger.info(f"Running Opus second opinion on {symbol}...")

        response = self.client.messages.create(
            model=self.model_opus,
            max_tokens=2048,
            system=[
                {
                    "type": "text",
                    "text": OPUS_SECOND_OPINION_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_prompt}],
        )

        block = response.content[0]
        assert isinstance(block, TextBlock)
        text: str = block.text

        # Parse the response
        result = self._parse_opus_opinion(text, symbol)

        # Cache result
        try:
            _cache_dir.mkdir(parents=True, exist_ok=True)
            result["analyzed_at"] = datetime.now().isoformat()
            (_cache_dir / f"{symbol}_opus.json").write_text(json.dumps(result, indent=2))
            logger.info(f"Cached Opus opinion for {symbol}")
        except Exception as e:
            logger.warning(f"Failed to cache Opus opinion for {symbol}: {e}")

        return result

    def _parse_opus_opinion(self, text: str, symbol: str) -> dict:
        """Parse Opus second opinion response into structured dict."""

        def extract_section(full_text: str, header: str, next_header: Optional[str] = None) -> str:
            start = full_text.find(header)
            if start == -1:
                return ""
            start += len(header)
            if next_header:
                end = full_text.find(next_header, start)
                if end == -1:
                    end = len(full_text)
            else:
                end = len(full_text)
            return full_text[start:end].strip()

        agreement_section = extract_section(text, "## AGREEMENT", "## OPUS CONVICTION")
        conviction_section = extract_section(text, "## OPUS CONVICTION", "## CONTRARIAN RISKS")
        risks_section = extract_section(text, "## CONTRARIAN RISKS", "## ADDITIONAL INSIGHTS")
        insights_section = extract_section(text, "## ADDITIONAL INSIGHTS", "## SUMMARY")
        summary_section = extract_section(text, "## SUMMARY", None)

        # Parse agreement
        agreement = "PARTIALLY_AGREE"
        for option in ["PARTIALLY_AGREE", "DISAGREE", "AGREE"]:
            if re.search(r"\b" + re.escape(option) + r"\b", agreement_section.upper()):
                agreement = option
                break

        # Parse conviction
        opus_conviction = "MEDIUM"
        for level in ["HIGH", "MEDIUM", "LOW"]:
            if re.search(r"\b" + level + r"\b", conviction_section.upper()):
                opus_conviction = level
                break

        # Parse risks as list
        contrarian_risks = []
        for line in risks_section.split("\n"):
            line = line.strip()
            if line and (line[0].isdigit() or line.startswith(("-", "•", "*"))):
                cleaned = line.lstrip("-•*0123456789.) ")
                if cleaned:
                    contrarian_risks.append(cleaned)

        return {
            "symbol": symbol,
            "agreement": agreement,
            "opus_conviction": opus_conviction,
            "contrarian_risks": contrarian_risks,
            "additional_insights": insights_section,
            "summary": summary_section,
        }

    def _dict_to_analysis(self, data: dict) -> AnalysisV2:
        """Convert cached dict back to AnalysisV2. Handles both v1 and v2 cache formats."""
        # Detect v2 format
        if data.get("schema_version") == "v2":
            return self._dict_to_analysis_v2(data)
        # Check for v2 structure without explicit version tag
        moat_data = data.get("moat", {})
        if isinstance(moat_data, dict) and "durability" in moat_data:
            return self._dict_to_analysis_v2(data)
        # v1 format — convert to v2
        return self._dict_v1_to_analysis_v2(data)

    def _dict_to_analysis_v2(self, data: dict) -> AnalysisV2:
        """Load v2 cache format into AnalysisV2."""
        moat = data.get("moat", {})
        mgmt = data.get("management", {})
        dur = data.get("durability", {})
        curr = data.get("currency_exposure", {})
        val = data.get("valuation", {})

        return AnalysisV2(
            symbol=data.get("symbol", ""),
            company_name=data.get("company_name", ""),
            sector=data.get("sector", ""),
            moat_type=moat.get("type", ""),
            moat_durability=moat.get("durability", "none"),
            moat_risks=moat.get("risks", ""),
            mgmt_insider_ownership=mgmt.get("insider_ownership"),
            mgmt_capital_allocation=mgmt.get("capital_allocation", "poor"),
            mgmt_summary=mgmt.get("summary", ""),
            recession_resilience=dur.get("recession_resilience", ""),
            existential_risks=dur.get("existential_risks", ""),
            outlook_10yr=dur.get("outlook_10yr", ""),
            domestic_revenue_pct=curr.get("domestic_revenue_pct"),
            international_revenue_pct=curr.get("international_revenue_pct"),
            currency_risk_level=curr.get("risk_level", "moderate"),
            currency_confidence=curr.get("confidence", "low"),
            estimated_fair_value_low=val.get("estimated_fair_value_low"),
            estimated_fair_value_high=val.get("estimated_fair_value_high"),
            target_entry_price=val.get("target_entry_price"),
            current_price=val.get("current_price"),
            conviction=data.get("conviction", "LOW"),
            summary=data.get("summary", ""),
            dividend_yield_estimate=data.get("dividend_yield"),
            total_return_potential=data.get("total_return_potential", ""),
            key_risks=data.get("key_risks", []),
            thesis_risks=data.get("thesis_risks", []),
        )

    def _dict_v1_to_analysis_v2(self, data: dict) -> AnalysisV2:
        """Convert v1 cache format into AnalysisV2 for backward compatibility."""
        moat_data = data.get("moat", {})
        mgmt_data = data.get("management", {})
        risks_data = data.get("risks", {})

        # Map v1 moat rating to v2 durability
        v1_rating = moat_data.get("rating", "none")
        durability_map = {"wide": "strong", "narrow": "moderate", "none": "none"}
        moat_durability = durability_map.get(v1_rating, "none")

        # Map v1 management rating to v2 capital allocation
        v1_mgmt = mgmt_data.get("rating", "poor")
        cap_alloc_map = {"excellent": "excellent", "adequate": "good", "poor": "poor"}
        mgmt_cap_alloc = cap_alloc_map.get(v1_mgmt, "poor")

        return AnalysisV2(
            symbol=data.get("symbol", ""),
            company_name=data.get("company_name", ""),
            sector="",
            moat_type=", ".join(moat_data.get("sources", [])),
            moat_durability=moat_durability,
            moat_risks=moat_data.get("explanation", ""),
            mgmt_insider_ownership=None,
            mgmt_capital_allocation=mgmt_cap_alloc,
            mgmt_summary=mgmt_data.get("notes", ""),
            recession_resilience="",
            existential_risks="",
            outlook_10yr="",
            domestic_revenue_pct=None,
            international_revenue_pct=None,
            currency_risk_level="moderate",
            currency_confidence="low",
            estimated_fair_value_low=None,
            estimated_fair_value_high=None,
            target_entry_price=None,
            current_price=None,
            conviction=data.get("conviction_level", "LOW"),
            summary=data.get("investment_thesis", data.get("business_summary", "")),
            key_risks=risks_data.get("key_risks", []),
            thesis_risks=risks_data.get("thesis_risks", []),
        )

    def _build_analysis_user_prompt(
        self,
        symbol: str,
        company_name: str,
        filing_text: str,
        earnings_transcript: Optional[str],
        recent_news: Optional[str],
    ) -> str:
        """Build the per-stock user prompt (system instructions are separate)."""

        prompt = f"""COMPANY: {company_name} ({symbol})

=== ANNUAL REPORT / COMPANY DATA ===
{filing_text[: self.MAX_FILING_CHARS]}
"""

        if earnings_transcript:
            prompt += f"""
=== RECENT EARNINGS CALL ===
{earnings_transcript[: self.MAX_TRANSCRIPT_CHARS]}
"""

        if recent_news:
            prompt += f"""
=== RECENT NEWS ===
{recent_news[: self.MAX_NEWS_CHARS]}
"""

        prompt += """
Based on the above information, provide your quality-focused analysis.
Assess this business's moat durability, management quality, business longevity,
currency exposure, and estimate a fair value range with target entry price."""

        return prompt

    def quick_screen(self, symbol: str, filing_text: str) -> dict:
        """
        Haiku-powered quick screen to decide if a stock is worth deep analysis.

        Cost: ~$0.002 per stock (25x cheaper than Sonnet deep analysis).
        Results are NOT cached — too cheap to bother, and we want fresh signals.

        Returns:
            dict with worth_analysis (bool), moat_hint (1-5), quality_hint (1-5), reason (str)
        """
        user_prompt = f"""COMPANY: {symbol}

{filing_text[:5000]}

Does this company show signs of a durable competitive advantage and consistent financial performance?
Assess business quality regardless of current valuation."""

        try:
            response = self.client.messages.create(
                model=self.model_light,
                max_tokens=256,
                system=[
                    {
                        "type": "text",
                        "text": QUICK_SCREEN_SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages=[{"role": "user", "content": user_prompt}],
            )

            block = response.content[0]
            assert isinstance(block, TextBlock)
            text: str = block.text
            return parse_quick_screen(text, symbol)

        except Exception as e:
            logger.warning(f"Haiku quick-screen failed for {symbol}: {e}")
            # On failure, assume worth analyzing (fail open)
            return {
                "symbol": symbol,
                "worth_analysis": True,
                "moat_hint": 3,
                "quality_hint": 3,
                "reason": f"Quick-screen error: {e}",
            }

    def check_news_for_red_flags(
        self, symbol: str, investment_thesis: str, thesis_risks: list[str], recent_news: str
    ) -> dict:
        """
        Check if recent news contains thesis-breaking events.

        Returns dict with:
        - has_red_flags: bool
        - flags: list of concerning items
        - recommendation: HOLD / REVIEW / SELL
        """

        user_prompt = f"""STOCK: {symbol}

ORIGINAL INVESTMENT THESIS:
{investment_thesis}

THESIS-BREAKING RISKS (events that would signal sell):
{chr(10).join(f"- {risk}" for risk in thesis_risks)}

RECENT NEWS:
{recent_news[: self.MAX_NEWS_CHARS]}

Analyze the news and determine:
1. Are there any events that match the thesis-breaking risks?
2. Are there any other concerning developments?
3. What is your recommendation?"""

        # Use Haiku for news monitoring - 20x cheaper than Sonnet
        response = self.client.messages.create(
            model=self.model_light,
            max_tokens=1024,
            system=[
                {
                    "type": "text",
                    "text": NEWS_MONITOR_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_prompt}],
        )

        block = response.content[0]
        assert isinstance(block, TextBlock)
        text: str = block.text
        has_flags = "RED FLAGS DETECTED: YES" in text.upper()

        # Extract recommendation
        rec = "HOLD"
        if "RECOMMENDATION: SELL" in text.upper():
            rec = "SELL"
        elif "RECOMMENDATION: REVIEW" in text.upper():
            rec = "REVIEW"

        return {"has_red_flags": has_flags, "analysis": text, "recommendation": rec}

    # ─────────────────────────────────────────────────────────────
    # Batch API methods (50% discount on all requests)
    # ─────────────────────────────────────────────────────────────

    def _wait_for_batch(self, batch_id: str, timeout_minutes: int = 30) -> object:
        """Poll batch status until complete or timeout."""
        deadline = time.time() + timeout_minutes * 60
        while time.time() < deadline:
            batch = self.client.messages.batches.retrieve(batch_id)
            counts = batch.request_counts
            logger.info(
                f"Batch {batch_id}: {counts.succeeded} succeeded, "
                f"{counts.errored} errored, {counts.processing} processing"
            )
            if batch.processing_status == "ended":
                return batch
            time.sleep(30)
        raise TimeoutError(f"Batch {batch_id} did not complete within {timeout_minutes} minutes")

    def batch_quick_screen(self, stocks: list[tuple[str, str]]) -> list[dict]:
        """
        Batch quick-screen via the Batch API (50% discount).

        Args:
            stocks: List of (symbol, filing_text) tuples.

        Returns:
            List of result dicts (same format as quick_screen).
        """
        if not stocks:
            return []

        requests = []
        for symbol, filing_text in stocks:
            user_prompt = f"""COMPANY: {symbol}

{filing_text[:5000]}

Does this company show signs of a durable competitive advantage and consistent financial performance?
Assess business quality regardless of current valuation."""
            requests.append(
                {
                    "custom_id": symbol,
                    "params": {
                        "model": self.model_light,
                        "max_tokens": 256,
                        "system": [
                            {
                                "type": "text",
                                "text": QUICK_SCREEN_SYSTEM_PROMPT,
                                "cache_control": {"type": "ephemeral"},
                            }
                        ],
                        "messages": [{"role": "user", "content": user_prompt}],
                    },
                }
            )

        logger.info(f"Submitting batch of {len(requests)} quick-screen requests...")
        batch = self.client.messages.batches.create(requests=cast(Any, requests))
        logger.info(f"Batch created: {batch.id}")

        self._wait_for_batch(batch.id)

        # Collect results
        symbol_order = [s for s, _ in stocks]
        results_map: dict[str, dict] = {}

        for result in self.client.messages.batches.results(batch.id):
            symbol = result.custom_id
            if result.result.type == "succeeded":
                blk = result.result.message.content[0]
                assert isinstance(blk, TextBlock)
                text: str = blk.text
                results_map[symbol] = parse_quick_screen(text, symbol)
            else:
                logger.warning(f"Batch quick-screen failed for {symbol}: {result.result.type}")
                results_map[symbol] = {
                    "symbol": symbol,
                    "worth_analysis": True,
                    "moat_hint": 3,
                    "quality_hint": 3,
                    "reason": f"Batch error: {result.result.type}",
                }

        return [
            results_map.get(
                s,
                {
                    "symbol": s,
                    "worth_analysis": True,
                    "moat_hint": 3,
                    "quality_hint": 3,
                    "reason": "Missing from batch",
                },
            )
            for s in symbol_order
        ]

    def batch_analyze_companies(self, stocks: list[dict]) -> list[AnalysisV2]:
        """
        Batch deep analysis via the Batch API (50% discount).

        Args:
            stocks: List of dicts with keys: symbol, company_name, filing_text,
                    and optionally earnings_transcript, recent_news, sector.

        Returns:
            List of AnalysisV2 objects (duck-type compatible with QualitativeAnalysis).
        """
        if not stocks:
            return []

        # Separate cached from uncached
        cached_results: dict[str, AnalysisV2] = {}
        uncached_stocks: list[dict] = []

        for stock in stocks:
            cached = get_cached_analysis(stock["symbol"])
            if cached:
                cached_results[stock["symbol"]] = self._dict_to_analysis(cached)
            else:
                uncached_stocks.append(stock)

        if cached_results:
            logger.info(f"Found {len(cached_results)} cached analyses, {len(uncached_stocks)} need API calls")

        if uncached_stocks:
            requests = []
            for stock in uncached_stocks:
                user_prompt = self._build_analysis_user_prompt(
                    symbol=stock["symbol"],
                    company_name=stock.get("company_name", stock["symbol"]),
                    filing_text=stock["filing_text"],
                    earnings_transcript=stock.get("earnings_transcript"),
                    recent_news=stock.get("recent_news"),
                )
                requests.append(
                    {
                        "custom_id": stock["symbol"],
                        "params": {
                            "model": self.model_deep,
                            "max_tokens": 4096,
                            "system": [
                                {
                                    "type": "text",
                                    "text": ANALYSIS_SYSTEM_PROMPT,
                                    "cache_control": {"type": "ephemeral"},
                                }
                            ],
                            "messages": [{"role": "user", "content": user_prompt}],
                        },
                    }
                )

            logger.info(f"Submitting batch of {len(requests)} deep analysis requests...")
            batch = self.client.messages.batches.create(requests=cast(Any, requests))
            logger.info(f"Batch created: {batch.id}")

            self._wait_for_batch(batch.id)

            for result in self.client.messages.batches.results(batch.id):
                symbol = result.custom_id
                if result.result.type == "succeeded":
                    blk = result.result.message.content[0]
                    assert isinstance(blk, TextBlock)
                    text = blk.text
                    company_name = next(
                        (s.get("company_name", s["symbol"]) for s in uncached_stocks if s["symbol"] == symbol),
                        symbol,
                    )
                    sector = next(
                        (s.get("sector", "") for s in uncached_stocks if s["symbol"] == symbol),
                        "",
                    )
                    analysis = parse_analysis(symbol, company_name, text, sector)
                    save_analysis_to_cache(symbol, analysis.to_dict())
                    cached_results[symbol] = analysis
                else:
                    logger.error(f"Batch analysis failed for {symbol}: {result.result.type}")

        # Return in original order
        return [cached_results[s["symbol"]] for s in stocks if s["symbol"] in cached_results]


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    logging.basicConfig(level=logging.INFO)

    # Test with sample text
    analyzer = CompanyAnalyzer()

    sample_filing = """
    Apple Inc. designs, manufactures, and markets smartphones, personal computers,
    tablets, wearables, and accessories worldwide. The company offers iPhone, Mac,
    iPad, and wearables including AirPods and Apple Watch. Services include the
    App Store, Apple Music, Apple TV+, and iCloud.

    Revenue for fiscal year 2024 was $383 billion. iPhone remains the largest
    segment at 52% of revenue. Services grew 14% year-over-year to $85 billion.
    The company returned $90 billion to shareholders through dividends and buybacks.

    Tim Cook has been CEO since 2011. The company maintains $162 billion in cash
    and marketable securities against $111 billion in debt.
    """

    analysis = analyzer.analyze_company(
        symbol="AAPL", company_name="Apple Inc.", filing_text=sample_filing, sector="Technology"
    )

    print("\n=== Analysis Results (v2) ===\n")
    print(f"Moat: {analysis.moat_type} ({analysis.moat_durability})")
    print(f"  Backward compat: {analysis.moat_rating.value}")
    print(f"Management: {analysis.mgmt_capital_allocation}")
    print(f"  Backward compat: {analysis.management_rating.value}")
    print(f"Conviction: {analysis.conviction}")
    print(f"  Backward compat: {analysis.conviction_level}")
    print(f"\nSummary: {analysis.summary}")
    if analysis.estimated_fair_value_low:
        print(f"Fair Value: ${analysis.estimated_fair_value_low:,.0f} - ${analysis.estimated_fair_value_high:,.0f}")
    if analysis.target_entry_price:
        print(f"Target Entry: ${analysis.target_entry_price:,.0f}")
    print(f"\nKey Risks: {analysis.key_risks}")
    if analysis.domestic_revenue_pct:
        print(f"Currency: {analysis.domestic_revenue_pct:.0%} domestic, risk={analysis.currency_risk_level}")
