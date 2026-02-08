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
- Analysis caching to avoid re-analyzing same stocks
- Reduced input truncation limits
"""

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

from anthropic import Anthropic

logger = logging.getLogger(__name__)

# Default cache directory for analysis results
DEFAULT_CACHE_DIR = Path("data/analyses")

# Runtime cache dir (can be overridden via set_cache_dir)
_cache_dir = DEFAULT_CACHE_DIR


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

        # Two models: expensive for deep analysis, cheap for simple tasks
        self.model_deep = "claude-sonnet-4-20250514"  # For 10-K analysis
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

        prompt = self._build_analysis_prompt(symbol, company_name, filing_text, earnings_transcript, recent_news)

        logger.info(f"Analyzing {symbol} with Claude (Sonnet)...")

        response = self.client.messages.create(
            model=self.model_deep,  # Use Sonnet for deep analysis
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )

        # Parse the response
        analysis_text = response.content[0].text

        analysis = self._parse_analysis(symbol, company_name, analysis_text)

        # Cache the result
        save_analysis_to_cache(symbol, analysis.to_dict())

        return analysis

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

    def _build_analysis_prompt(
        self,
        symbol: str,
        company_name: str,
        filing_text: str,
        earnings_transcript: Optional[str],
        recent_news: Optional[str],
    ) -> str:
        """Build the analysis prompt for Claude"""

        prompt = f"""You are a value investing analyst in the style of Warren Buffett.
Analyze the following company for potential long-term investment.

COMPANY: {company_name} ({symbol})

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

        prompt += """
Based on the above information, provide your analysis in the following format:

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
- Be skeptical and highlight genuine risks
"""

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
            """Find which rating option appears in text"""
            text_upper = text.upper()
            for option in options:
                if option.upper() in text_upper:
                    return option
            return options[-1]  # Default to last (usually worst)

        def extract_list(text: str) -> list:
            """Extract bulleted/numbered list items"""
            items = []
            for line in text.split("\n"):
                line = line.strip()
                if line.startswith(("-", "•", "*")) or (line and line[0].isdigit()):
                    # Remove bullet/number
                    cleaned = line.lstrip("-•*0123456789.) ")
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
            insider_ownership=None,  # Would need specific extraction
            business_summary=business_section,
            competitive_position=competitive_section,
            key_risks=extract_list(risks_section),
            thesis_risks=extract_list(thesis_risks_section),
            investment_thesis=thesis_section,
            conviction_level=conviction,
        )

    def quick_screen(self, symbol: str, filing_text: str) -> dict:
        """
        Haiku-powered quick screen to decide if a stock is worth deep analysis.

        Cost: ~$0.002 per stock (25x cheaper than Sonnet deep analysis).
        Results are NOT cached — too cheap to bother, and we want fresh signals.

        Returns:
            dict with worth_analysis (bool), moat_hint (1-5), quality_hint (1-5), reason (str)
        """
        prompt = f"""You are a value investing analyst. Quickly assess this company.

COMPANY: {symbol}

{filing_text[:5000]}

Is this company worth deep analysis for a long-term value investor?
Rate moat strength 1-5, business quality 1-5, one-sentence reason.
Respond in exactly 3 lines:
MOAT: <1-5>
QUALITY: <1-5>
REASON: <one sentence>"""

        try:
            response = self.client.messages.create(
                model=self.model_light, max_tokens=256, messages=[{"role": "user", "content": prompt}]
            )

            text = response.content[0].text
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

        prompt = f"""You are monitoring a stock position for potential red flags.

STOCK: {symbol}

ORIGINAL INVESTMENT THESIS:
{investment_thesis}

THESIS-BREAKING RISKS (events that would signal sell):
{chr(10).join(f"- {risk}" for risk in thesis_risks)}

RECENT NEWS:
{recent_news[: self.MAX_NEWS_CHARS]}

Analyze the news and determine:
1. Are there any events that match the thesis-breaking risks?
2. Are there any other concerning developments?
3. What is your recommendation?

Respond in this format:
RED FLAGS DETECTED: [YES / NO]
CONCERNING ITEMS:
- [item 1 if any]
- [item 2 if any]
RECOMMENDATION: [HOLD / REVIEW / SELL]
EXPLANATION: [1-2 sentences]
"""

        # Use Haiku for news monitoring - 20x cheaper than Sonnet
        response = self.client.messages.create(
            model=self.model_light, max_tokens=1024, messages=[{"role": "user", "content": prompt}]
        )

        text = response.content[0].text

        has_flags = "RED FLAGS DETECTED: YES" in text.upper()

        # Extract recommendation
        rec = "HOLD"
        if "RECOMMENDATION: SELL" in text.upper():
            rec = "SELL"
        elif "RECOMMENDATION: REVIEW" in text.upper():
            rec = "REVIEW"

        return {"has_red_flags": has_flags, "analysis": text, "recommendation": rec}


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
