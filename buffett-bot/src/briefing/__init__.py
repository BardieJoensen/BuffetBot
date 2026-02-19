"""
Briefing Generator Package

Combines quantitative data and qualitative analysis into
a human-readable monthly briefing document.

v2.0 — Tiered Watchlist Format:
- Tier 1: Wonderful business at/below fair value -> staged entry
- Tier 2: Wonderful business, overpriced -> watch and wait
- Tier 3: Good business worth monitoring -> re-evaluate next cycle
- Movement log: what changed since last briefing
- Market regime summary
- Approaching-target alerts
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from ..tier_engine import WatchlistMovement
from ..valuation import AggregatedValuation
from .html_formatter import generate_html_report
from .text_formatter import generate_text_report

logger = logging.getLogger(__name__)

__all__ = ["StockBriefing", "BriefingGenerator"]


@dataclass
class StockBriefing:
    """Complete briefing for a single stock (v2 — tiered)."""

    symbol: str
    company_name: str

    # Quantitative (from APIs)
    current_price: float
    market_cap: float
    pe_ratio: Optional[float]
    debt_equity: Optional[float]
    roe: Optional[float]
    revenue_growth: Optional[float]

    # Valuation (from external sources)
    valuation: AggregatedValuation

    # Qualitative (from LLM — accepts QualitativeAnalysis or AnalysisV2)
    analysis: object

    # Tier assignment (from tier_engine)
    tier: int = 2  # 1, 2, 3, 0=excluded
    tier_reason: str = ""
    target_entry_price: Optional[float] = None
    price_gap_pct: Optional[float] = None
    approaching_target: bool = False

    # Position sizing
    position_size: Optional[dict] = None

    # Deeper Buffett fundamentals
    fcf_yield: Optional[float] = None
    earnings_quality: Optional[float] = None
    payout_ratio: Optional[float] = None
    operating_margin: Optional[float] = None

    # Opus second opinion (contrarian review)
    opus_opinion: Optional[dict] = None

    generated_at: datetime = field(default_factory=datetime.now)

    @property
    def recommendation(self) -> str:
        """Backward-compatible recommendation derived from tier."""
        if self.tier == 1:
            return "BUY"
        elif self.tier in (2, 3):
            return "WATCHLIST"
        return "PASS"


class BriefingGenerator:
    """
    Generates comprehensive monthly investment briefings.

    v2.0 — tiered watchlist format with movement log.
    Orchestrates text, HTML, and JSON output generation.
    """

    def __init__(self, output_dir: str = "./data/briefings"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_briefing(
        self,
        briefings: list[StockBriefing],
        portfolio_summary: Optional[dict] = None,
        market_temp: Optional[dict] = None,
        bubble_warnings: Optional[list] = None,
        radar_stocks: Optional[list[str]] = None,
        performance_metrics: Optional[dict] = None,
        benchmark_data: Optional[dict] = None,
        movements: Optional[list[WatchlistMovement]] = None,
        campaign_progress: Optional[dict] = None,
    ) -> str:
        """
        Generate a complete monthly briefing document.

        Returns:
            Formatted briefing as string (also saves text, HTML, JSON to files)
        """
        now = datetime.now()

        # Generate text report
        briefing_text = generate_text_report(
            briefings,
            portfolio_summary,
            market_temp,
            bubble_warnings,
            radar_stocks,
            performance_metrics,
            benchmark_data,
            movements,
            campaign_progress,
        )

        # Save text
        filename = f"briefing_{now.strftime('%Y_%m')}.txt"
        filepath = self.output_dir / filename
        filepath.write_text(briefing_text)
        logger.info(f"Briefing saved to {filepath}")

        # Save JSON
        json_data = _build_json_output(
            briefings,
            portfolio_summary,
            market_temp,
            bubble_warnings,
            radar_stocks,
            performance_metrics,
            benchmark_data,
            movements,
        )
        json_path = self.output_dir / f"briefing_{now.strftime('%Y_%m')}.json"
        json_path.write_text(json.dumps(json_data, indent=2, default=str))

        # Generate and save HTML
        html_content = generate_html_report(
            briefings,
            portfolio_summary,
            market_temp,
            bubble_warnings,
            radar_stocks,
            performance_metrics,
            benchmark_data,
            movements,
            campaign_progress,
        )
        html_filename = f"briefing_{now.strftime('%Y_%m')}.html"
        self.html_path = self.output_dir / html_filename
        self.html_path.write_text(html_content)
        logger.info(f"HTML briefing saved to {self.html_path}")

        return briefing_text


def _build_json_output(
    briefings: list[StockBriefing],
    portfolio_summary: Optional[dict],
    market_temp: Optional[dict],
    bubble_warnings: Optional[list],
    radar_stocks: Optional[list[str]],
    performance_metrics: Optional[dict],
    benchmark_data: Optional[dict] = None,
    movements: Optional[list[WatchlistMovement]] = None,
) -> dict:
    """Build JSON structure for programmatic access."""
    tier1 = [b for b in briefings if b.tier == 1]
    tier2 = [b for b in briefings if b.tier == 2]
    tier3 = [b for b in briefings if b.tier == 3]

    return {
        "schema_version": "v2",
        "generated_at": datetime.now().isoformat(),
        "market_temperature": market_temp,
        "benchmark": benchmark_data,
        "summary": {
            "total_analyzed": len(briefings),
            "tier1_count": len(tier1),
            "tier2_count": len(tier2),
            "tier3_count": len(tier3),
            "approaching_target": sum(1 for b in tier2 if b.approaching_target),
            "bubble_warnings": len(bubble_warnings) if bubble_warnings else 0,
            "radar": len(radar_stocks) if radar_stocks else 0,
        },
        "portfolio": portfolio_summary,
        "performance": performance_metrics,
        "movements": [
            {
                "symbol": m.symbol,
                "change_type": m.change_type,
                "detail": m.detail,
                "previous_tier": m.previous_tier,
                "current_tier": m.current_tier,
            }
            for m in (movements or [])
        ],
        "tier1": [_briefing_to_dict(b) for b in tier1],
        "tier2": [_briefing_to_dict(b) for b in tier2],
        "tier3": [_briefing_to_dict(b) for b in tier3],
        "radar": radar_stocks or [],
        "bubble_watch": [w.to_dict() if hasattr(w, "to_dict") else w for w in (bubble_warnings or [])],
    }


def _briefing_to_dict(briefing: StockBriefing) -> dict:
    """Convert briefing to dictionary."""
    return {
        "symbol": briefing.symbol,
        "company_name": briefing.company_name,
        "tier": briefing.tier,
        "tier_reason": briefing.tier_reason,
        "target_entry_price": briefing.target_entry_price,
        "price_gap_pct": briefing.price_gap_pct,
        "approaching_target": briefing.approaching_target,
        "recommendation": briefing.recommendation,
        "quantitative": {
            "current_price": briefing.current_price,
            "market_cap": briefing.market_cap,
            "pe_ratio": briefing.pe_ratio,
            "debt_equity": briefing.debt_equity,
            "roe": briefing.roe,
            "revenue_growth": briefing.revenue_growth,
            "fcf_yield": briefing.fcf_yield,
            "operating_margin": briefing.operating_margin,
            "earnings_quality": briefing.earnings_quality,
            "payout_ratio": briefing.payout_ratio,
        },
        "valuation": briefing.valuation.to_dict(),
        "qualitative": briefing.analysis.to_dict() if hasattr(briefing.analysis, "to_dict") else {},
        "position_size": briefing.position_size,
        "opus_opinion": briefing.opus_opinion,
        "generated_at": briefing.generated_at.isoformat() if briefing.generated_at else None,
    }
