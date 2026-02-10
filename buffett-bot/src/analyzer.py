"""
LLM Analyzer Module

Uses Claude API for qualitative analysis of companies.
The LLM reads documents and provides assessments - it does NOT do math.

Responsibilities:
- Summarize 10-K annual reports
- Assess competitive moat quality
- Evaluate management from earnings calls
- Identify risks and red flags
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
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional, cast

from anthropic import Anthropic
from anthropic.types import TextBlock

logger = logging.getLogger(__name__)

# Default cache directory for analysis results
DEFAULT_CACHE_DIR = Path("data/analyses")

# Runtime cache dir (can be overridden via set_cache_dir)
_cache_dir = DEFAULT_CACHE_DIR

# ─────────────────────────────────────────────────────────────
# System prompts (cached across API calls for token savings)
# ─────────────────────────────────────────────────────────────

ANALYSIS_SYSTEM_PROMPT = """\
You are a value investing analyst in the style of Warren Buffett.
Analyze companies for potential long-term investment.

Provide your analysis in the following format:

## MOAT ASSESSMENT
Rating: [WIDE / NARROW / NONE]
Sources of moat (list each):
- [e.g., Switching costs, Network effects, Brand, Cost advantages, etc.]
Explanation: [2-3 sentences on durability of competitive advantage]

## MANAGEMENT ASSESSMENT
Rating: [EXCELLENT / ADEQUATE / POOR]
Notes: [Assessment of capital allocation, alignment with shareholders, track record]
Insider Ownership: [If mentioned in documents]

## BUSINESS SUMMARY
[2-3 sentence description of what the company does and how it makes money]

## COMPETITIVE POSITION
[Assessment of market position, competitors, industry dynamics]

## KEY RISKS
1. [Risk 1]
2. [Risk 2]
3. [Risk 3]

## THESIS-BREAKING RISKS
[What specific events would invalidate an investment thesis? Be specific.]
1. [Event that would signal "sell immediately"]
2. [Event that would signal "sell immediately"]

## INVESTMENT THESIS
[If you were to invest, what is the bull case? 2-3 sentences]

## CONVICTION LEVEL
[HIGH / MEDIUM / LOW] - [One sentence explanation]

IMPORTANT:
- Do NOT calculate valuations or fair values
- Do NOT make price predictions
- Focus on qualitative business analysis only
- Be skeptical and highlight genuine risks"""

QUICK_SCREEN_SYSTEM_PROMPT = """\
You are a value investing analyst. Quickly assess companies for long-term value.
Rate moat strength 1-5, business quality 1-5, one-sentence reason.
Respond in exactly 3 lines:
MOAT: <1-5>
QUALITY: <1-5>
REASON: <one sentence>"""

OPUS_SECOND_OPINION_PROMPT = """\
You are a contrarian investment analyst providing a "second opinion" review.
You have been given a prior analyst's assessment of a company. Your job is to:

1. Challenge the thesis — play devil's advocate
2. Identify what the prior analyst might have missed or underweighted
3. Assess whether the conviction level is justified
4. Highlight risks that were downplayed or overlooked
5. Consider macro/secular headwinds the prior analyst may have ignored

Respond in this exact format:

## AGREEMENT
[AGREE / PARTIALLY_AGREE / DISAGREE] with the prior analyst's thesis

## OPUS CONVICTION
[HIGH / MEDIUM / LOW] — your independent conviction level

## CONTRARIAN RISKS
1. [Risk or concern the prior analyst missed or underweighted]
2. [Another risk]
3. [Another risk]

## ADDITIONAL INSIGHTS
[2-3 sentences with observations the prior analyst missed — could be positive or negative]

## SUMMARY
[2-3 sentence overall second opinion. Be direct and honest.]

IMPORTANT:
- Be genuinely critical, not just agreeable
- If the prior analyst is wrong, say so clearly
- Focus on what was MISSED, not what was already covered
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


@dataclass
class QualitativeAnalysis:
    """LLM-generated qualitative assessment of a company"""

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
    - model_deep (Sonnet): For 10-K analysis, ~$0.05/stock
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
        self.model_deep = "claude-sonnet-4-5-20250929"  # For 10-K analysis
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
    ) -> QualitativeAnalysis:
        """
        Perform deep qualitative analysis of a company.

        Args:
            symbol: Stock ticker
            company_name: Full company name
            filing_text: 10-K annual report text (or summary)
            earnings_transcript: Recent earnings call transcript
            recent_news: Recent news articles about the company
            use_cache: If True, return cached analysis if available
            cache_max_age_days: Max age of cached analysis to use

        Returns:
            QualitativeAnalysis with LLM assessments
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
        analysis = self._parse_analysis(symbol, company_name, analysis_text)

        # Cache the result
        save_analysis_to_cache(symbol, analysis.to_dict())

        return analysis

    def opus_second_opinion(
        self,
        symbol: str,
        company_name: str,
        filing_text: str,
        sonnet_analysis: QualitativeAnalysis,
        use_cache: bool = True,
    ) -> dict:
        """
        Run Opus as a contrarian reviewer of Sonnet's analysis.

        Cost: ~$0.30 per stock (only run on top 3-5 BUY picks).

        Args:
            symbol: Stock ticker
            company_name: Full company name
            filing_text: 10-K annual report text (or summary)
            sonnet_analysis: The prior Sonnet analysis to critique
            use_cache: If True, return cached result if available

        Returns:
            dict with agreement, opus_conviction, contrarian_risks,
            additional_insights, and summary
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
            f"- Thesis: {sonnet_analysis.investment_thesis}\n"
            f"- Key Risks: {'; '.join(sonnet_analysis.key_risks[:3])}\n"
            f"- Business: {sonnet_analysis.business_summary[:300]}"
        )

        user_prompt = f"""{sonnet_summary}

=== COMPANY FILING DATA ===
{filing_text[: self.MAX_FILING_CHARS]}

Based on the filing data and the prior analyst's assessment above, provide your contrarian second opinion."""

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
        import re

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

    def _dict_to_analysis(self, data: dict) -> QualitativeAnalysis:
        """Convert cached dict back to QualitativeAnalysis"""
        moat_data = data.get("moat", {})
        mgmt_data = data.get("management", {})
        risks_data = data.get("risks", {})

        return QualitativeAnalysis(
            symbol=data.get("symbol", ""),
            company_name=data.get("company_name", ""),
            moat_rating=MoatRating(moat_data.get("rating", "none")),
            moat_sources=moat_data.get("sources", []),
            moat_explanation=moat_data.get("explanation", ""),
            management_rating=ManagementRating(mgmt_data.get("rating", "poor")),
            management_notes=mgmt_data.get("notes", ""),
            insider_ownership=mgmt_data.get("insider_ownership"),
            business_summary=data.get("business_summary", ""),
            competitive_position=data.get("competitive_position", ""),
            key_risks=risks_data.get("key_risks", []),
            thesis_risks=risks_data.get("thesis_risks", []),
            investment_thesis=data.get("investment_thesis", ""),
            conviction_level=data.get("conviction_level", "LOW"),
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

=== ANNUAL REPORT (10-K) ===
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

        prompt += "\nBased on the above information, provide your analysis."

        return prompt

    def _parse_analysis(self, symbol: str, company_name: str, analysis_text: str) -> QualitativeAnalysis:
        """Parse Claude's response into structured data"""

        # Simple parsing - in production you'd want more robust parsing
        # or ask Claude to return JSON

        def extract_section(text: str, header: str, next_header: Optional[str] = None) -> str:
            """Extract text between headers"""
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

        def extract_rating(text: str, options: list) -> str:
            """Find which rating option appears in text using word boundaries"""
            import re

            text_upper = text.upper()
            # Sort by length descending so "PARTIALLY_AGREE" matches before "AGREE"
            for option in sorted(options, key=len, reverse=True):
                if re.search(r"\b" + re.escape(option.upper()) + r"\b", text_upper):
                    return option
            return options[-1]  # Default to last (usually worst)

        def extract_list(text: str) -> list:
            """Extract bulleted/numbered list items"""
            import re

            items = []
            for line in text.split("\n"):
                line = line.strip()
                if line.startswith(("-", "•", "*")) or (line and line[0].isdigit()):
                    # Remove bullet/number
                    cleaned = line.lstrip("-•*0123456789.) ")
                    # Strip markdown bold/italic markers
                    cleaned = re.sub(r"\*{1,2}(.+?)\*{1,2}", r"\1", cleaned)
                    if cleaned:
                        items.append(cleaned)
            return items

        # Extract each section
        moat_section = extract_section(analysis_text, "## MOAT ASSESSMENT", "## MANAGEMENT")
        mgmt_section = extract_section(analysis_text, "## MANAGEMENT ASSESSMENT", "## BUSINESS")
        business_section = extract_section(analysis_text, "## BUSINESS SUMMARY", "## COMPETITIVE")
        competitive_section = extract_section(analysis_text, "## COMPETITIVE POSITION", "## KEY RISKS")
        risks_section = extract_section(analysis_text, "## KEY RISKS", "## THESIS")
        thesis_risks_section = extract_section(analysis_text, "## THESIS-BREAKING", "## INVESTMENT THESIS")
        thesis_section = extract_section(analysis_text, "## INVESTMENT THESIS", "## CONVICTION")
        conviction_section = extract_section(analysis_text, "## CONVICTION LEVEL", None)

        # Parse ratings
        moat_rating_str = extract_rating(moat_section, ["WIDE", "NARROW", "NONE"])
        moat_rating = MoatRating(moat_rating_str.lower())

        mgmt_rating_str = extract_rating(mgmt_section, ["EXCELLENT", "ADEQUATE", "POOR"])
        mgmt_rating = ManagementRating(mgmt_rating_str.lower())

        conviction = extract_rating(conviction_section, ["HIGH", "MEDIUM", "LOW"])

        return QualitativeAnalysis(
            symbol=symbol,
            company_name=company_name,
            moat_rating=moat_rating,
            moat_sources=extract_list(moat_section),
            moat_explanation=moat_section.split("Explanation:")[-1].strip()
            if "Explanation:" in moat_section
            else moat_section,
            management_rating=mgmt_rating,
            management_notes=mgmt_section,
            insider_ownership=self._extract_insider_ownership(mgmt_section),
            business_summary=business_section,
            competitive_position=competitive_section,
            key_risks=extract_list(risks_section),
            thesis_risks=extract_list(thesis_risks_section),
            investment_thesis=thesis_section,
            conviction_level=conviction,
        )

    @staticmethod
    def _extract_insider_ownership(mgmt_section: str) -> Optional[str]:
        """Extract insider ownership info from management section if present."""
        for line in mgmt_section.split("\n"):
            if "insider ownership" in line.lower():
                value = line.split(":", 1)[-1].strip()
                if value and value.lower() not in ("n/a", "not mentioned", "unknown", "none"):
                    return value
        return None

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

Is this company worth deep analysis for a long-term value investor?"""

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

    def _parse_quick_screen_result(self, text: str, symbol: str) -> dict:
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

Is this company worth deep analysis for a long-term value investor?"""
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
                results_map[symbol] = self._parse_quick_screen_result(text, symbol)
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

    def batch_analyze_companies(self, stocks: list[dict]) -> list[QualitativeAnalysis]:
        """
        Batch deep analysis via the Batch API (50% discount).

        Args:
            stocks: List of dicts with keys: symbol, company_name, filing_text,
                    and optionally earnings_transcript, recent_news.

        Returns:
            List of QualitativeAnalysis objects.
        """
        if not stocks:
            return []

        # Separate cached from uncached
        cached_results: dict[str, QualitativeAnalysis] = {}
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
                    analysis = self._parse_analysis(symbol, company_name, text)
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

    analysis = analyzer.analyze_company(symbol="AAPL", company_name="Apple Inc.", filing_text=sample_filing)

    print("\n=== Analysis Results ===\n")
    print(f"Moat: {analysis.moat_rating.value}")
    print(f"Management: {analysis.management_rating.value}")
    print(f"Conviction: {analysis.conviction_level}")
    print(f"\nThesis: {analysis.investment_thesis}")
    print(f"\nKey Risks: {analysis.key_risks}")
