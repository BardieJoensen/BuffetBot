"""
Briefing Generator Module

Combines quantitative data and qualitative analysis into 
a human-readable monthly briefing document.

Sections:
- Market Temperature (overall market valuation)
- Portfolio Status (your positions + sector exposure)
- Top Picks (full analysis, BUY recommendations)
- Watchlist (close but not quite there)
- Radar (passed screening, not yet analyzed)
- Bubble Watch (stocks to avoid)
- Performance (how your picks have done)
"""

import os
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from pathlib import Path
import logging

from .valuation import AggregatedValuation
from .analyzer import QualitativeAnalysis

logger = logging.getLogger(__name__)


@dataclass
class StockBriefing:
    """Complete briefing for a single stock"""
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
    
    # Qualitative (from LLM)
    analysis: QualitativeAnalysis
    
    # Bot recommendation
    recommendation: str  # BUY / WATCHLIST / PASS
    
    # Position sizing
    position_size: Optional[dict] = None
    
    generated_at: datetime = field(default_factory=datetime.now)


class BriefingGenerator:
    """
    Generates comprehensive monthly investment briefings.
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
        performance_metrics: Optional[dict] = None
    ) -> str:
        """
        Generate a complete monthly briefing document.
        
        Args:
            briefings: List of analyzed stock briefings
            portfolio_summary: Current portfolio status
            market_temp: Market temperature reading
            bubble_warnings: Stocks to avoid
            radar_stocks: Screened but not analyzed stocks
            performance_metrics: Historical performance data
        
        Returns:
            Formatted briefing as string (also saves to file)
        """
        
        now = datetime.now()
        month_str = now.strftime("%B %Y")
        
        output = []
        output.append("═" * 70)
        output.append(f"INVESTMENT BRIEFING - {month_str}")
        output.append(f"Generated: {now.strftime('%Y-%m-%d %H:%M')}")
        output.append("═" * 70)
        output.append("")
        
        # Categorize briefings
        buy_candidates = [b for b in briefings if b.recommendation == "BUY"]
        watchlist = [b for b in briefings if b.recommendation == "WATCHLIST"]
        
        # ─────────────────────────────────────────────────────────────
        # MARKET TEMPERATURE
        # ─────────────────────────────────────────────────────────────
        if market_temp:
            output.append("## MARKET TEMPERATURE")
            output.append("")
            
            temp_emoji = {
                "COLD": "🥶",
                "COOL": "😎", 
                "WARM": "😰",
                "HOT": "🔥",
                "UNKNOWN": "❓"
            }
            emoji = temp_emoji.get(market_temp.get("temperature", "UNKNOWN"), "❓")
            
            output.append(f"{emoji} {market_temp.get('temperature', 'UNKNOWN')}")
            output.append(f"Market P/E: {market_temp.get('market_pe', 'N/A')}")
            output.append(f"Interpretation: {market_temp.get('interpretation', '')}")
            output.append("")
        
        # ─────────────────────────────────────────────────────────────
        # PORTFOLIO STATUS
        # ─────────────────────────────────────────────────────────────
        if portfolio_summary:
            output.append("─" * 70)
            output.append("## PORTFOLIO STATUS")
            output.append("")
            
            output.append(f"Positions: {portfolio_summary.get('position_count', 0)}")
            output.append(f"Total Invested: ${portfolio_summary.get('total_invested', 0):,.0f}")
            output.append(f"Current Value:  ${portfolio_summary.get('current_value', 0):,.0f}")
            
            gain = portfolio_summary.get('total_gain_loss', 0)
            gain_pct = portfolio_summary.get('total_gain_loss_pct', 0)
            gain_sign = "+" if gain >= 0 else ""
            output.append(f"Gain/Loss:      {gain_sign}${gain:,.0f} ({gain_sign}{gain_pct:.1%})")
            output.append("")
            
            # Sector exposure
            exposure = portfolio_summary.get('sector_exposure', {})
            if exposure:
                output.append("Sector Exposure:")
                for sector, pct in sorted(exposure.items(), key=lambda x: x[1], reverse=True):
                    bar = "█" * int(pct * 20)
                    output.append(f"  {sector:20} {bar} {pct:.0%}")
                output.append("")
            
            # Warnings
            warnings = portfolio_summary.get('sector_warnings', [])
            if warnings:
                output.append("⚠️  CONCENTRATION WARNINGS:")
                for warning in warnings:
                    output.append(f"  • {warning}")
                output.append("")
            
            # Position alerts
            alerts = portfolio_summary.get('alerts', [])
            if alerts:
                output.append("🚨 POSITION ALERTS:")
                for alert in alerts:
                    output.append(f"  • {alert.get('symbol')}: {alert.get('message')}")
                output.append("")
            elif portfolio_summary.get('position_count', 0) > 0:
                output.append("✓ No alerts. All positions appear healthy.")
                output.append("")
        
        # ─────────────────────────────────────────────────────────────
        # EXECUTIVE SUMMARY
        # ─────────────────────────────────────────────────────────────
        output.append("─" * 70)
        output.append("## EXECUTIVE SUMMARY")
        output.append("")
        output.append(f"Stocks Analyzed:  {len(briefings)}")
        output.append(f"Buy Candidates:   {len(buy_candidates)}")
        output.append(f"Watchlist:        {len(watchlist)}")
        output.append(f"Bubble Watch:     {len(bubble_warnings) if bubble_warnings else 0}")
        output.append(f"Radar:            {len(radar_stocks) if radar_stocks else 0}")
        output.append("")
        
        if buy_candidates:
            output.append("Top Opportunities (by margin of safety):")
            for b in sorted(buy_candidates, key=lambda x: x.valuation.margin_of_safety or 0, reverse=True)[:3]:
                mos = b.valuation.margin_of_safety or 0
                output.append(f"  🟢 {b.symbol}: {mos:.1%} margin of safety, {b.analysis.conviction_level} conviction")
            output.append("")
        else:
            output.append("No strong buy candidates this month. Consider holding cash.")
            output.append("")
        
        # ─────────────────────────────────────────────────────────────
        # TOP PICKS (Full Analysis)
        # ─────────────────────────────────────────────────────────────
        if buy_candidates:
            output.append("─" * 70)
            output.append("## TOP PICKS (Buy Candidates)")
            output.append("")
            
            for briefing in sorted(buy_candidates, key=lambda x: x.valuation.margin_of_safety or 0, reverse=True):
                output.append(self._format_stock_briefing(briefing, include_sizing=True))
                output.append("")
        
        # ─────────────────────────────────────────────────────────────
        # WATCHLIST
        # ─────────────────────────────────────────────────────────────
        if watchlist:
            output.append("─" * 70)
            output.append("## WATCHLIST (Monitor for Better Entry)")
            output.append("")
            
            for briefing in sorted(watchlist, key=lambda x: x.valuation.margin_of_safety or 0, reverse=True):
                output.append(self._format_watchlist_item(briefing))
                output.append("")
        
        # ─────────────────────────────────────────────────────────────
        # RADAR
        # ─────────────────────────────────────────────────────────────
        if radar_stocks:
            output.append("─" * 70)
            output.append("## RADAR (Passed Screen, Not Yet Analyzed)")
            output.append("")
            output.append("These stocks passed quantitative screening but haven't received")
            output.append("deep analysis yet. Consider for future research:")
            output.append("")
            
            # Display in columns
            for i in range(0, len(radar_stocks), 5):
                chunk = radar_stocks[i:i+5]
                output.append("  " + "  ".join(f"{s:8}" for s in chunk))
            output.append("")
        
        # ─────────────────────────────────────────────────────────────
        # BUBBLE WATCH
        # ─────────────────────────────────────────────────────────────
        if bubble_warnings:
            output.append("─" * 70)
            output.append("## BUBBLE WATCH (Avoid These)")
            output.append("")
            output.append("These stocks show signs of overvaluation. Do not buy.")
            output.append("If you own them, consider selling.")
            output.append("")
            
            for warning in bubble_warnings[:5]:
                output.append(self._format_bubble_warning(warning))
                output.append("")
        
        # ─────────────────────────────────────────────────────────────
        # PERFORMANCE
        # ─────────────────────────────────────────────────────────────
        if performance_metrics and performance_metrics.get('total_trades', 0) > 0:
            output.append("─" * 70)
            output.append("## PERFORMANCE (Your Track Record)")
            output.append("")
            
            output.append(f"Total Trades:     {performance_metrics.get('total_trades', 0)}")
            output.append(f"Winning Trades:   {performance_metrics.get('winning_trades', 0)}")
            output.append(f"Losing Trades:    {performance_metrics.get('losing_trades', 0)}")
            output.append(f"Win Rate:         {performance_metrics.get('win_rate', 0):.0%}")
            output.append("")
            
            if performance_metrics.get('benchmark_return') is not None:
                your_return = performance_metrics.get('total_return', 0)
                benchmark = performance_metrics.get('benchmark_return', 0)
                alpha = your_return - benchmark
                
                output.append(f"Your Return:      {your_return:+.1%}")
                output.append(f"Benchmark (S&P):  {benchmark:+.1%}")
                output.append(f"Alpha:            {alpha:+.1%}")
                output.append("")
        
        # ─────────────────────────────────────────────────────────────
        # FOOTER
        # ─────────────────────────────────────────────────────────────
        output.append("─" * 70)
        output.append("## REMINDER")
        output.append("")
        output.append("• This briefing is for research purposes only")
        output.append("• All valuations are estimates from external sources")
        output.append("• YOU make the final investment decision")
        output.append("• Past performance does not guarantee future results")
        output.append("")
        output.append("═" * 70)
        
        briefing_text = "\n".join(output)
        
        # Save to file
        filename = f"briefing_{now.strftime('%Y_%m')}.txt"
        filepath = self.output_dir / filename
        filepath.write_text(briefing_text)
        logger.info(f"Briefing saved to {filepath}")
        
        # Also save as JSON for programmatic access
        json_data = self._build_json_output(
            briefings, portfolio_summary, market_temp, 
            bubble_warnings, radar_stocks, performance_metrics
        )
        json_path = self.output_dir / f"briefing_{now.strftime('%Y_%m')}.json"
        json_path.write_text(json.dumps(json_data, indent=2, default=str))
        
        return briefing_text
    
    def _format_stock_briefing(self, briefing: StockBriefing, include_sizing: bool = False) -> str:
        """Format a full stock briefing for top picks"""
        
        lines = []
        
        # Header
        lines.append(f"### {briefing.symbol}: {briefing.company_name}")
        lines.append(f"Recommendation: 🟢 BUY")
        lines.append("")
        
        # Position sizing (if available)
        if include_sizing and briefing.position_size:
            sizing = briefing.position_size
            lines.append(f"💰 POSITION SIZING ({sizing.get('conviction', 'MEDIUM')} conviction):")
            lines.append(f"   Recommended: {sizing.get('recommended_pct', 0):.0%} of portfolio (${sizing.get('recommended_amount', 0):,.0f})")
            lines.append(f"   Maximum:     {sizing.get('max_pct', 0):.0%} of portfolio (${sizing.get('max_amount', 0):,.0f})")
            lines.append("")
        
        # Qualitative Summary
        lines.append("QUALITATIVE ASSESSMENT:")
        lines.append("┌" + "─" * 58 + "┐")
        lines.append(f"│ Moat:       {briefing.analysis.moat_rating.value.upper():12} │ Conviction: {briefing.analysis.conviction_level:10} │")
        lines.append(f"│ Management: {briefing.analysis.management_rating.value.upper():12} │            {'':10} │")
        lines.append("└" + "─" * 58 + "┘")
        
        if briefing.analysis.moat_sources:
            lines.append(f"Moat Sources: {', '.join(briefing.analysis.moat_sources[:3])}")
        
        lines.append("")
        
        # Quantitative Data
        lines.append("QUANTITATIVE DATA:")
        lines.append("┌" + "─" * 58 + "┐")
        lines.append(f"│ Current Price:    ${briefing.current_price:>10.2f}                          │")
        
        if briefing.market_cap:
            lines.append(f"│ Market Cap:       ${briefing.market_cap:>10,.0f}                          │")
        if briefing.pe_ratio:
            lines.append(f"│ P/E Ratio:        {briefing.pe_ratio:>11.1f}                          │")
        if briefing.roe:
            lines.append(f"│ ROE:              {briefing.roe:>10.1%}                          │")
        if briefing.debt_equity:
            lines.append(f"│ Debt/Equity:      {briefing.debt_equity:>11.2f}                          │")
        
        lines.append("└" + "─" * 58 + "┘")
        lines.append("")
        
        # Valuation
        lines.append("VALUATION ESTIMATES:")
        lines.append("┌" + "─" * 58 + "┐")
        
        for est in briefing.valuation.estimates[:4]:
            lines.append(f"│ {est.source[:25]:<25} ${est.fair_value:>10.2f}              │")
        
        avg_fv = briefing.valuation.average_fair_value
        mos = briefing.valuation.margin_of_safety
        upside = briefing.valuation.upside_potential
        
        lines.append("├" + "─" * 58 + "┤")
        if avg_fv:
            lines.append(f"│ AVERAGE FAIR VALUE:       ${avg_fv:>10.2f}              │")
        if mos:
            lines.append(f"│ MARGIN OF SAFETY:         {mos:>10.1%}              │")
        if upside:
            lines.append(f"│ UPSIDE POTENTIAL:         {upside:>10.1%}              │")
        
        lines.append("└" + "─" * 58 + "┘")
        lines.append("")
        
        # Thesis
        lines.append("INVESTMENT THESIS:")
        thesis = briefing.analysis.investment_thesis[:400]
        lines.append(thesis)
        lines.append("")
        
        # Risks
        lines.append("KEY RISKS:")
        for risk in briefing.analysis.key_risks[:3]:
            lines.append(f"  • {risk[:70]}")
        lines.append("")
        
        lines.append("THESIS-BREAKING EVENTS (sell signals):")
        for risk in briefing.analysis.thesis_risks[:2]:
            lines.append(f"  ⚠️  {risk[:70]}")
        
        lines.append("")
        lines.append("─" * 60)
        
        return "\n".join(lines)
    
    def _format_watchlist_item(self, briefing: StockBriefing) -> str:
        """Format a condensed watchlist item"""
        
        lines = []
        
        mos = briefing.valuation.margin_of_safety or 0
        avg_fv = briefing.valuation.average_fair_value or 0
        
        lines.append(f"🟡 {briefing.symbol}: {briefing.company_name}")
        lines.append(f"   Price: ${briefing.current_price:.2f} → Fair Value: ${avg_fv:.2f} ({mos:.0%} margin)")
        lines.append(f"   Moat: {briefing.analysis.moat_rating.value.upper()} | Conviction: {briefing.analysis.conviction_level}")
        lines.append(f"   Why not BUY: {self._get_watchlist_reason(briefing)}")
        
        return "\n".join(lines)
    
    def _get_watchlist_reason(self, briefing: StockBriefing) -> str:
        """Determine why stock is watchlist not buy"""
        
        mos = briefing.valuation.margin_of_safety or 0
        
        if mos < 0.20:
            return f"Margin of safety ({mos:.0%}) below 20% threshold"
        if briefing.analysis.conviction_level == "LOW":
            return "Low conviction - needs more research"
        if briefing.analysis.moat_rating.value == "none":
            return "No clear competitive moat"
        
        return "Close to criteria but not compelling enough"
    
    def _format_bubble_warning(self, warning) -> str:
        """Format a bubble warning"""
        
        lines = []
        
        # Handle both dict and object
        if hasattr(warning, 'risk_level'):
            risk_level = warning.risk_level
            symbol = warning.symbol
            company_name = warning.company_name
            current_price = warning.current_price
            pe_ratio = warning.pe_ratio
            signal_count = warning.signal_count
            signals = warning.signals
        else:
            risk_level = warning.get('risk_level', 'MEDIUM')
            symbol = warning.get('symbol', 'N/A')
            company_name = warning.get('company_name', 'Unknown')
            current_price = warning.get('current_price', 0)
            pe_ratio = warning.get('pe_ratio')
            signal_count = warning.get('signal_count', 0)
            signals = warning.get('signals', [])
        
        risk_emoji = "🔴" if risk_level == "HIGH" else "🟠"
        
        lines.append(f"{risk_emoji} {symbol}: {company_name}")
        lines.append(f"   Price: ${current_price:.2f} | P/E: {pe_ratio or 'N/A'}")
        lines.append(f"   Signals ({signal_count}):")
        
        for signal in signals[:3]:
            lines.append(f"     • {signal[:60]}")
        
        return "\n".join(lines)
    
    def _build_json_output(
        self,
        briefings: list[StockBriefing],
        portfolio_summary: Optional[dict],
        market_temp: Optional[dict],
        bubble_warnings: Optional[list],
        radar_stocks: Optional[list[str]],
        performance_metrics: Optional[dict]
    ) -> dict:
        """Build JSON structure for programmatic access"""
        
        buy_candidates = [b for b in briefings if b.recommendation == "BUY"]
        watchlist = [b for b in briefings if b.recommendation == "WATCHLIST"]
        
        return {
            "generated_at": datetime.now().isoformat(),
            "market_temperature": market_temp,
            "summary": {
                "total_analyzed": len(briefings),
                "buy_candidates": len(buy_candidates),
                "watchlist": len(watchlist),
                "bubble_warnings": len(bubble_warnings) if bubble_warnings else 0,
                "radar": len(radar_stocks) if radar_stocks else 0
            },
            "portfolio": portfolio_summary,
            "performance": performance_metrics,
            "top_picks": [self._briefing_to_dict(b) for b in buy_candidates],
            "watchlist": [self._briefing_to_dict(b) for b in watchlist],
            "radar": radar_stocks or [],
            "bubble_watch": [w.to_dict() if hasattr(w, 'to_dict') else w for w in (bubble_warnings or [])]
        }
    
    def _briefing_to_dict(self, briefing: StockBriefing) -> dict:
        """Convert briefing to dictionary"""
        return {
            "symbol": briefing.symbol,
            "company_name": briefing.company_name,
            "recommendation": briefing.recommendation,
            "quantitative": {
                "current_price": briefing.current_price,
                "market_cap": briefing.market_cap,
                "pe_ratio": briefing.pe_ratio,
                "debt_equity": briefing.debt_equity,
                "roe": briefing.roe,
                "revenue_growth": briefing.revenue_growth
            },
            "valuation": briefing.valuation.to_dict(),
            "qualitative": briefing.analysis.to_dict(),
            "position_size": briefing.position_size,
            "generated_at": briefing.generated_at.isoformat() if briefing.generated_at else None
        }


def determine_recommendation(
    valuation: AggregatedValuation,
    analysis: QualitativeAnalysis,
    min_margin_of_safety: float = 0.20
) -> str:
    """
    Determine buy/watchlist/pass recommendation.
    
    Combines quantitative (margin of safety) with qualitative (moat, conviction).
    """
    
    mos = valuation.margin_of_safety or 0
    has_moat = analysis.moat_rating.value in ["wide", "narrow"]
    high_conviction = analysis.conviction_level == "HIGH"
    medium_conviction = analysis.conviction_level == "MEDIUM"
    
    # BUY: Good margin of safety + moat + conviction
    if mos >= min_margin_of_safety and has_moat and (high_conviction or medium_conviction):
        return "BUY"
    
    # WATCHLIST: Some merit but not compelling enough
    if mos >= 0.10 and has_moat:
        return "WATCHLIST"
    
    if mos >= min_margin_of_safety and medium_conviction:
        return "WATCHLIST"
    
    # PASS: Doesn't meet criteria
    return "PASS"
