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

import html as html_module
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from .analyzer import QualitativeAnalysis
from .valuation import AggregatedValuation

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
        performance_metrics: Optional[dict] = None,
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

            temp_emoji = {"COLD": "🥶", "COOL": "😎", "WARM": "😰", "HOT": "🔥", "UNKNOWN": "❓"}
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

            gain = portfolio_summary.get("total_gain_loss", 0)
            gain_pct = portfolio_summary.get("total_gain_loss_pct", 0)
            gain_sign = "+" if gain >= 0 else ""
            output.append(f"Gain/Loss:      {gain_sign}${gain:,.0f} ({gain_sign}{gain_pct:.1%})")
            output.append("")

            # Sector exposure
            exposure = portfolio_summary.get("sector_exposure", {})
            if exposure:
                output.append("Sector Exposure:")
                for sector, pct in sorted(exposure.items(), key=lambda x: x[1], reverse=True):
                    bar = "█" * int(pct * 20)
                    output.append(f"  {sector:20} {bar} {pct:.0%}")
                output.append("")

            # Warnings
            warnings = portfolio_summary.get("sector_warnings", [])
            if warnings:
                output.append("⚠️  CONCENTRATION WARNINGS:")
                for warning in warnings:
                    output.append(f"  • {warning}")
                output.append("")

            # Position alerts
            alerts = portfolio_summary.get("alerts", [])
            if alerts:
                output.append("🚨 POSITION ALERTS:")
                for alert in alerts:
                    output.append(f"  • {alert.get('symbol')}: {alert.get('message')}")
                output.append("")
            elif portfolio_summary.get("position_count", 0) > 0:
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
                chunk = radar_stocks[i : i + 5]
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
        if performance_metrics and performance_metrics.get("total_trades", 0) > 0:
            output.append("─" * 70)
            output.append("## PERFORMANCE (Your Track Record)")
            output.append("")

            output.append(f"Total Trades:     {performance_metrics.get('total_trades', 0)}")
            output.append(f"Winning Trades:   {performance_metrics.get('winning_trades', 0)}")
            output.append(f"Losing Trades:    {performance_metrics.get('losing_trades', 0)}")
            output.append(f"Win Rate:         {performance_metrics.get('win_rate', 0):.0%}")
            output.append("")

            if performance_metrics.get("benchmark_return") is not None:
                your_return = performance_metrics.get("total_return", 0)
                benchmark = performance_metrics.get("benchmark_return", 0)
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
            briefings, portfolio_summary, market_temp, bubble_warnings, radar_stocks, performance_metrics
        )
        json_path = self.output_dir / f"briefing_{now.strftime('%Y_%m')}.json"
        json_path.write_text(json.dumps(json_data, indent=2, default=str))

        # Generate HTML report
        html_content = self._generate_html(
            briefings, portfolio_summary, market_temp, bubble_warnings, radar_stocks, performance_metrics
        )
        html_filename = f"briefing_{now.strftime('%Y_%m')}.html"
        self.html_path = self.output_dir / html_filename
        self.html_path.write_text(html_content)
        logger.info(f"HTML briefing saved to {self.html_path}")

        return briefing_text

    def _generate_html(
        self,
        briefings: list[StockBriefing],
        portfolio_summary: Optional[dict],
        market_temp: Optional[dict],
        bubble_warnings: Optional[list],
        radar_stocks: Optional[list[str]],
        performance_metrics: Optional[dict],
    ) -> str:
        """Generate a self-contained HTML briefing report."""
        now = datetime.now()
        month_str = now.strftime("%B %Y")
        e = html_module.escape

        buy_candidates = sorted(
            [b for b in briefings if b.recommendation == "BUY"],
            key=lambda x: x.valuation.margin_of_safety or 0,
            reverse=True,
        )
        watchlist = sorted(
            [b for b in briefings if b.recommendation == "WATCHLIST"],
            key=lambda x: x.valuation.margin_of_safety or 0,
            reverse=True,
        )

        # Market temperature colors
        temp_colors = {
            "COLD": ("#2196F3", "&#x1F976;"),
            "COOL": ("#4CAF50", "&#x1F60E;"),
            "WARM": ("#FF9800", "&#x1F630;"),
            "HOT": ("#F44336", "&#x1F525;"),
            "UNKNOWN": ("#9E9E9E", "&#x2753;"),
        }
        temp_val = market_temp.get("temperature", "UNKNOWN") if market_temp else "UNKNOWN"
        temp_color, temp_icon = temp_colors.get(temp_val, temp_colors["UNKNOWN"])

        parts = []
        parts.append(f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Investment Briefing - {e(month_str)}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  background:#f5f5f5;color:#333;line-height:1.6}}
.container{{max-width:900px;margin:0 auto;padding:16px}}
header{{background:#1a237e;color:#fff;padding:32px 24px;border-radius:8px 8px 0 0;
  margin-bottom:0}}
header h1{{font-size:1.5rem;font-weight:600}}
header .date{{opacity:.8;font-size:.9rem;margin-top:4px}}
.temp-badge{{display:inline-block;padding:6px 16px;border-radius:20px;
  font-weight:600;margin-top:12px;font-size:1rem}}
section{{background:#fff;padding:24px;margin-bottom:2px}}
section:last-child{{border-radius:0 0 8px 8px;margin-bottom:24px}}
h2{{font-size:1.2rem;color:#1a237e;border-bottom:2px solid #e8eaf6;
  padding-bottom:8px;margin-bottom:16px}}
.summary-grid{{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:16px}}
.summary-card{{background:#f5f5f5;border-radius:8px;padding:16px;text-align:center;
  flex:1 1 0;min-width:80px}}
.summary-card .num{{font-size:1.8rem;font-weight:700;color:#1a237e}}
.summary-card .label{{font-size:.8rem;color:#666;text-transform:uppercase}}
.stock-card{{border:1px solid #e0e0e0;border-radius:8px;padding:20px;margin-bottom:16px;
  border-left:4px solid #ccc}}
.stock-card.buy{{border-left-color:#4CAF50}}
.stock-card.watchlist{{border-left-color:#FF9800}}
.stock-card.bubble{{border-left-color:#F44336}}
.stock-card h3{{font-size:1.1rem;margin-bottom:4px}}
.stock-card .rec{{display:inline-block;padding:2px 10px;border-radius:12px;
  font-size:.75rem;font-weight:600;color:#fff;margin-bottom:12px}}
.rec-buy{{background:#4CAF50}}
.rec-watchlist{{background:#FF9800}}
.rec-bubble{{background:#F44336}}
table{{width:100%;border-collapse:collapse;margin:12px 0;font-size:.9rem}}
table th{{text-align:left;padding:8px 12px;background:#f5f5f5;border-bottom:2px solid #ddd;
  font-weight:600;color:#555}}
table td{{padding:8px 12px;border-bottom:1px solid #eee}}
table td:last-child{{text-align:right}}
table th:last-child{{text-align:right}}
details{{margin:8px 0}}
summary{{cursor:pointer;font-weight:600;color:#1a237e;padding:4px 0}}
summary:hover{{text-decoration:underline}}
.bar-chart{{margin:8px 0}}
.bar-row{{display:flex;align-items:center;margin:4px 0;font-size:.85rem}}
.bar-label{{width:160px;flex-shrink:0;text-align:right;padding-right:12px;color:#555}}
.bar-track{{flex:1;background:#e8eaf6;border-radius:4px;height:20px;position:relative}}
.bar-fill{{background:#3f51b5;border-radius:4px;height:100%;min-width:2px}}
.bar-pct{{width:50px;text-align:right;padding-left:8px;color:#555;font-size:.8rem}}
.risk-item{{padding:4px 0;font-size:.9rem}}
.radar-grid{{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px;max-width:100%;overflow:hidden}}
.radar-chip{{background:#e8eaf6;color:#3f51b5;padding:4px 12px;border-radius:16px;
  font-size:.8rem;font-weight:500}}
.portfolio-stats{{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:16px}}
.portfolio-stat{{padding:8px 0;flex:1 1 0;min-width:120px}}
.portfolio-stat .val{{font-size:1.2rem;font-weight:600}}
.portfolio-stat .lbl{{font-size:.8rem;color:#666}}
.gain-pos{{color:#4CAF50}}
.gain-neg{{color:#F44336}}
footer{{text-align:center;padding:16px;font-size:.8rem;color:#999}}
.sizing{{background:#e8f5e9;border-radius:6px;padding:12px;margin-bottom:12px;font-size:.9rem}}
@media(max-width:600px){{
  .container{{padding:8px}}
  header{{padding:20px 16px}}
  section{{padding:16px}}
  .summary-card{{min-width:60px}}
  .portfolio-stat{{min-width:100px}}
  .bar-label{{width:100px;font-size:.75rem}}
}}
</style>
</head>
<body>
<div class="container">
<header>
  <h1>Investment Briefing &mdash; {e(month_str)}</h1>
  <div class="date">Generated {now.strftime("%Y-%m-%d %H:%M")}</div>""")

        if market_temp:
            parts.append(f"""  <div class="temp-badge" style="background:{temp_color}">{temp_icon} {e(temp_val)}</div>
  <div style="margin-top:8px;font-size:.9rem;opacity:.9">{e(market_temp.get("interpretation", ""))}</div>""")

        parts.append("</header>")

        # Executive Summary
        parts.append("""<section>
<h2>Executive Summary</h2>
<div class="summary-grid">""")
        parts.append(
            f'<div class="summary-card"><div class="num">{len(briefings)}</div><div class="label">Analyzed</div></div>'
        )
        parts.append(
            f'<div class="summary-card"><div class="num" style="color:#4CAF50">{len(buy_candidates)}</div><div class="label">Buy</div></div>'
        )
        parts.append(
            f'<div class="summary-card"><div class="num" style="color:#FF9800">{len(watchlist)}</div><div class="label">Watchlist</div></div>'
        )
        parts.append(
            f'<div class="summary-card"><div class="num" style="color:#F44336">{len(bubble_warnings) if bubble_warnings else 0}</div><div class="label">Bubble Watch</div></div>'
        )
        parts.append(
            f'<div class="summary-card"><div class="num">{len(radar_stocks) if radar_stocks else 0}</div><div class="label">Radar</div></div>'
        )
        parts.append("</div></section>")

        # Portfolio Status
        if portfolio_summary:
            gain = portfolio_summary.get("total_gain_loss", 0)
            gain_pct = portfolio_summary.get("total_gain_loss_pct", 0)
            gain_class = "gain-pos" if gain >= 0 else "gain-neg"
            gain_sign = "+" if gain >= 0 else ""
            parts.append(f"""<section>
<h2>Portfolio Status</h2>
<div class="portfolio-stats">
  <div class="portfolio-stat"><div class="val">{portfolio_summary.get("position_count", 0)}</div><div class="lbl">Positions</div></div>
  <div class="portfolio-stat"><div class="val">${portfolio_summary.get("total_invested", 0):,.0f}</div><div class="lbl">Invested</div></div>
  <div class="portfolio-stat"><div class="val">${portfolio_summary.get("current_value", 0):,.0f}</div><div class="lbl">Current Value</div></div>
  <div class="portfolio-stat"><div class="val {gain_class}">{gain_sign}${gain:,.0f} ({gain_sign}{gain_pct:.1%})</div><div class="lbl">Gain/Loss</div></div>
</div>""")

            exposure = portfolio_summary.get("sector_exposure", {})
            if exposure:
                parts.append('<h3 style="font-size:1rem;margin-bottom:8px">Sector Exposure</h3><div class="bar-chart">')
                for sector, pct in sorted(exposure.items(), key=lambda x: x[1], reverse=True):
                    width = max(1, int(pct * 100))
                    parts.append(
                        f'<div class="bar-row"><span class="bar-label">{e(sector)}</span><div class="bar-track"><div class="bar-fill" style="width:{width}%"></div></div><span class="bar-pct">{pct:.0%}</span></div>'
                    )
                parts.append("</div>")

            warnings = portfolio_summary.get("sector_warnings", [])
            if warnings:
                parts.append(
                    '<div style="margin-top:12px;color:#F44336;font-weight:600">Concentration Warnings:</div><ul style="margin:4px 0 0 20px;font-size:.9rem">'
                )
                for w in warnings:
                    parts.append(f"<li>{e(w)}</li>")
                parts.append("</ul>")

            parts.append("</section>")

        # Top Picks
        if buy_candidates:
            parts.append("<section><h2>Top Picks</h2>")
            for b in buy_candidates:
                parts.append(self._html_stock_card(b, "buy"))
            parts.append("</section>")

        # Watchlist
        if watchlist:
            parts.append("<section><h2>Watchlist</h2>")
            for b in watchlist:
                parts.append(self._html_stock_card(b, "watchlist"))
            parts.append("</section>")

        # Radar
        if radar_stocks:
            parts.append(
                '<section><h2>Radar</h2><p style="font-size:.9rem;color:#666;margin-bottom:12px">Passed screening, not yet analyzed.</p><div class="radar-grid">'
            )
            for s in radar_stocks:
                parts.append(f'<span class="radar-chip">{e(s)}</span>')
            parts.append("</div></section>")

        # Bubble Watch
        if bubble_warnings:
            parts.append("<section><h2>Bubble Watch</h2>")
            for warning in bubble_warnings[:5]:
                parts.append(self._html_bubble_card(warning))
            parts.append("</section>")

        # Performance
        if performance_metrics and performance_metrics.get("total_trades", 0) > 0:
            pm = performance_metrics
            parts.append(f"""<section>
<h2>Performance</h2>
<table>
<tr><th>Metric</th><th>Value</th></tr>
<tr><td>Total Trades</td><td>{pm.get("total_trades", 0)}</td></tr>
<tr><td>Winning Trades</td><td>{pm.get("winning_trades", 0)}</td></tr>
<tr><td>Losing Trades</td><td>{pm.get("losing_trades", 0)}</td></tr>
<tr><td>Win Rate</td><td>{pm.get("win_rate", 0):.0%}</td></tr>""")
            if pm.get("benchmark_return") is not None:
                your_ret = pm.get("total_return", 0)
                bench = pm.get("benchmark_return", 0)
                alpha = your_ret - bench
                parts.append(f"""<tr><td>Your Return</td><td>{your_ret:+.1%}</td></tr>
<tr><td>Benchmark (S&amp;P)</td><td>{bench:+.1%}</td></tr>
<tr><td>Alpha</td><td>{alpha:+.1%}</td></tr>""")
            parts.append("</table></section>")

        # Footer
        parts.append(f"""<section style="background:#fafafa;font-size:.85rem;color:#777">
<p><strong>Disclaimer:</strong> This briefing is for research purposes only. All valuations are estimates.
You make the final investment decision. Past performance does not guarantee future results.</p>
</section>
<footer>Buffett Bot &middot; {e(month_str)}</footer>
</div>
</body>
</html>""")

        return "\n".join(parts)

    def _html_stock_card(self, briefing: StockBriefing, card_type: str) -> str:
        """Build an HTML card for a stock (buy or watchlist)."""
        e = html_module.escape
        rec_class = "rec-buy" if card_type == "buy" else "rec-watchlist"
        rec_label = "BUY" if card_type == "buy" else "WATCHLIST"

        mos = briefing.valuation.margin_of_safety or 0
        avg_fv = briefing.valuation.average_fair_value or 0
        upside = briefing.valuation.upside_potential or 0

        lines = [f'<div class="stock-card {card_type}">']
        lines.append(f"<h3>{e(briefing.symbol)}: {e(briefing.company_name)}</h3>")
        lines.append(f'<span class="rec {rec_class}">{rec_label}</span>')

        # Position sizing for buys
        if card_type == "buy" and briefing.position_size:
            sz = briefing.position_size
            lines.append(
                f'<div class="sizing"><strong>Position Sizing ({e(str(sz.get("conviction", "MEDIUM")))} conviction):</strong> '
                f"Recommended {sz.get('recommended_pct', 0):.0%} (${sz.get('recommended_amount', 0):,.0f}) &middot; "
                f"Max {sz.get('max_pct', 0):.0%} (${sz.get('max_amount', 0):,.0f})</div>"
            )

        # Data table
        lines.append("<table>")
        lines.append(f"<tr><td>Price</td><td>${briefing.current_price:.2f}</td></tr>")
        if avg_fv:
            lines.append(f"<tr><td>Fair Value (avg)</td><td>${avg_fv:.2f}</td></tr>")
        lines.append(f"<tr><td>Margin of Safety</td><td>{mos:.1%}</td></tr>")
        if upside:
            lines.append(f"<tr><td>Upside Potential</td><td>{upside:.1%}</td></tr>")
        lines.append(f"<tr><td>Moat</td><td>{e(briefing.analysis.moat_rating.value.upper())}</td></tr>")
        lines.append(f"<tr><td>Conviction</td><td>{e(briefing.analysis.conviction_level)}</td></tr>")
        if briefing.pe_ratio:
            lines.append(f"<tr><td>P/E Ratio</td><td>{briefing.pe_ratio:.1f}</td></tr>")
        if briefing.roe:
            lines.append(f"<tr><td>ROE</td><td>{briefing.roe:.1%}</td></tr>")
        if briefing.debt_equity:
            lines.append(f"<tr><td>Debt/Equity</td><td>{briefing.debt_equity:.2f}</td></tr>")
        lines.append("</table>")

        # Valuation estimates
        if briefing.valuation.estimates:
            lines.append("<details><summary>Valuation Estimates</summary><table>")
            lines.append("<tr><th>Source</th><th>Fair Value</th></tr>")
            for est in briefing.valuation.estimates[:6]:
                lines.append(f"<tr><td>{e(est.source)}</td><td>${est.fair_value:.2f}</td></tr>")
            lines.append("</table></details>")

        # Thesis
        lines.append(
            f"<details><summary>Investment Thesis</summary><p style='font-size:.9rem;margin-top:8px'>{e(briefing.analysis.investment_thesis[:600])}</p></details>"
        )

        # Risks
        if briefing.analysis.key_risks:
            lines.append("<details><summary>Key Risks</summary><ul style='font-size:.9rem;margin:8px 0 0 20px'>")
            for risk in briefing.analysis.key_risks[:4]:
                lines.append(f"<li>{e(risk)}</li>")
            lines.append("</ul></details>")

        # Thesis-breaking events
        if briefing.analysis.thesis_risks:
            lines.append(
                "<details><summary>Thesis-Breaking Events</summary><ul style='font-size:.9rem;margin:8px 0 0 20px;color:#d32f2f'>"
            )
            for risk in briefing.analysis.thesis_risks[:3]:
                lines.append(f"<li>{e(risk)}</li>")
            lines.append("</ul></details>")

        if card_type == "watchlist":
            reason = self._get_watchlist_reason(briefing)
            lines.append(f'<p style="font-size:.85rem;color:#666;margin-top:8px"><em>Why not BUY: {e(reason)}</em></p>')

        lines.append("</div>")
        return "\n".join(lines)

    def _html_bubble_card(self, warning) -> str:
        """Build an HTML card for a bubble warning."""
        e = html_module.escape
        if hasattr(warning, "risk_level"):
            risk_level = warning.risk_level
            symbol = warning.symbol
            company_name = warning.company_name
            current_price = warning.current_price
            pe_ratio = warning.pe_ratio
            signals = warning.signals
        else:
            risk_level = warning.get("risk_level", "MEDIUM")
            symbol = warning.get("symbol", "N/A")
            company_name = warning.get("company_name", "Unknown")
            current_price = warning.get("current_price", 0)
            pe_ratio = warning.get("pe_ratio")
            signals = warning.get("signals", [])

        pe_str = f"{pe_ratio:.1f}" if pe_ratio else "N/A"
        lines = ['<div class="stock-card bubble">']
        lines.append(f"<h3>{e(symbol)}: {e(company_name)}</h3>")
        lines.append(f'<span class="rec rec-bubble">{e(risk_level)} RISK</span>')
        lines.append(f"<table><tr><td>Price</td><td>${current_price:.2f}</td></tr>")
        lines.append(f"<tr><td>P/E</td><td>{e(pe_str)}</td></tr></table>")
        if signals:
            lines.append(
                "<details open><summary>Warning Signals</summary><ul style='font-size:.9rem;margin:8px 0 0 20px;color:#d32f2f'>"
            )
            for sig in signals[:4]:
                lines.append(f"<li>{e(sig)}</li>")
            lines.append("</ul></details>")
        lines.append("</div>")
        return "\n".join(lines)

    def _format_stock_briefing(self, briefing: StockBriefing, include_sizing: bool = False) -> str:
        """Format a full stock briefing for top picks"""

        lines = []

        # Header
        lines.append(f"### {briefing.symbol}: {briefing.company_name}")
        lines.append("Recommendation: 🟢 BUY")
        lines.append("")

        # Position sizing (if available)
        if include_sizing and briefing.position_size:
            sizing = briefing.position_size
            lines.append(f"💰 POSITION SIZING ({sizing.get('conviction', 'MEDIUM')} conviction):")
            lines.append(
                f"   Recommended: {sizing.get('recommended_pct', 0):.0%} of portfolio (${sizing.get('recommended_amount', 0):,.0f})"
            )
            lines.append(
                f"   Maximum:     {sizing.get('max_pct', 0):.0%} of portfolio (${sizing.get('max_amount', 0):,.0f})"
            )
            lines.append("")

        # Qualitative Summary
        lines.append("QUALITATIVE ASSESSMENT:")
        lines.append("┌" + "─" * 58 + "┐")
        lines.append(
            f"│ Moat:       {briefing.analysis.moat_rating.value.upper():12} │ Conviction: {briefing.analysis.conviction_level:10} │"
        )
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
        lines.append(
            f"   Moat: {briefing.analysis.moat_rating.value.upper()} | Conviction: {briefing.analysis.conviction_level}"
        )
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
        if hasattr(warning, "risk_level"):
            risk_level = warning.risk_level
            symbol = warning.symbol
            company_name = warning.company_name
            current_price = warning.current_price
            pe_ratio = warning.pe_ratio
            signal_count = warning.signal_count
            signals = warning.signals
        else:
            risk_level = warning.get("risk_level", "MEDIUM")
            symbol = warning.get("symbol", "N/A")
            company_name = warning.get("company_name", "Unknown")
            current_price = warning.get("current_price", 0)
            pe_ratio = warning.get("pe_ratio")
            signal_count = warning.get("signal_count", 0)
            signals = warning.get("signals", [])

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
        performance_metrics: Optional[dict],
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
                "radar": len(radar_stocks) if radar_stocks else 0,
            },
            "portfolio": portfolio_summary,
            "performance": performance_metrics,
            "top_picks": [self._briefing_to_dict(b) for b in buy_candidates],
            "watchlist": [self._briefing_to_dict(b) for b in watchlist],
            "radar": radar_stocks or [],
            "bubble_watch": [w.to_dict() if hasattr(w, "to_dict") else w for w in (bubble_warnings or [])],
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
                "revenue_growth": briefing.revenue_growth,
            },
            "valuation": briefing.valuation.to_dict(),
            "qualitative": briefing.analysis.to_dict(),
            "position_size": briefing.position_size,
            "generated_at": briefing.generated_at.isoformat() if briefing.generated_at else None,
        }


def determine_recommendation(
    valuation: AggregatedValuation, analysis: QualitativeAnalysis, min_margin_of_safety: float = 0.20
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
