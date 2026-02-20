"""
HTML Briefing Formatter

Generates self-contained HTML monthly watchlist briefing reports.
Extracted from briefing.py for separation of concerns.
"""

from __future__ import annotations

import html as html_module
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from ..tier_engine import WatchlistMovement, staged_entry_suggestion

if TYPE_CHECKING:
    from ..bubble_detector import BubbleWarning
    from . import StockBriefing


def generate_html_report(
    briefings: list[StockBriefing],
    portfolio_summary: Optional[dict] = None,
    market_temp: Optional[dict] = None,
    bubble_warnings: Optional[list] = None,
    radar_stocks: Optional[list[str]] = None,
    radar_context: Optional[dict] = None,
    performance_metrics: Optional[dict] = None,
    benchmark_data: Optional[dict] = None,
    movements: Optional[list[WatchlistMovement]] = None,
    campaign_progress: Optional[dict] = None,
) -> str:
    """Generate a self-contained HTML briefing report."""
    now = datetime.now()
    month_str = now.strftime("%B %Y")
    e = html_module.escape

    tier1 = sorted([b for b in briefings if b.tier == 1], key=lambda x: abs(x.price_gap_pct or 0))
    tier2 = sorted([b for b in briefings if b.tier == 2], key=lambda x: abs(x.price_gap_pct or 999))
    tier3 = [b for b in briefings if b.tier == 3]
    approaching = [b for b in tier2 if b.approaching_target]

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

    parts: list[str] = []
    parts.append(f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Watchlist Update - {e(month_str)}</title>
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
.stock-card.tier1{{border-left-color:#4CAF50}}
.stock-card.tier2{{border-left-color:#FF9800}}
.stock-card.tier3{{border-left-color:#90A4AE}}
.stock-card.approaching{{border-left-color:#E91E63;border-left-width:6px}}
.stock-card.bubble{{border-left-color:#F44336}}
.stock-card h3{{font-size:1.1rem;margin-bottom:4px}}
.tier-badge{{display:inline-block;padding:2px 10px;border-radius:12px;
  font-size:.75rem;font-weight:600;color:#fff;margin-bottom:12px}}
.tier-1{{background:#4CAF50}}
.tier-2{{background:#FF9800}}
.tier-3{{background:#90A4AE}}
.tier-approaching{{background:#E91E63}}
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
.staged-entry{{background:#e3f2fd;border-radius:6px;padding:12px;margin:8px 0;font-size:.9rem}}
.movement-log{{margin:8px 0;font-size:.9rem}}
.movement-item{{padding:4px 0;display:flex;align-items:center;gap:8px}}
.movement-badge{{display:inline-block;padding:1px 8px;border-radius:8px;font-size:.7rem;
  font-weight:600;color:#fff}}
.mv-new{{background:#4CAF50}}
.mv-removed{{background:#9E9E9E}}
.mv-up{{background:#2196F3}}
.mv-down{{background:#FF9800}}
.mv-approaching{{background:#E91E63}}
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
  <h1>Watchlist Update &mdash; {e(month_str)}</h1>
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
        f'<div class="summary-card"><div class="num" style="color:#4CAF50">{len(tier1)}</div><div class="label">Tier 1</div></div>'
    )
    parts.append(
        f'<div class="summary-card"><div class="num" style="color:#FF9800">{len(tier2)}</div><div class="label">Tier 2</div></div>'
    )
    parts.append(
        f'<div class="summary-card"><div class="num" style="color:#90A4AE">{len(tier3)}</div><div class="label">Tier 3</div></div>'
    )
    if approaching:
        parts.append(
            f'<div class="summary-card"><div class="num" style="color:#E91E63">{len(approaching)}</div><div class="label">Approaching</div></div>'
        )
    parts.append("</div>")

    # Campaign Progress
    if campaign_progress:
        cp = campaign_progress
        cov_pct = cp.get("coverage_pct", 0)
        bar_width = max(1, int(cov_pct * 100))
        parts.append(
            f'<div style="background:#e8eaf6;border-radius:8px;padding:16px;margin:12px 0">'
            f'<div style="font-weight:600;margin-bottom:8px">Coverage Campaign: {e(str(cp.get("campaign_id", "")))}</div>'
            f'<div style="background:#c5cae9;border-radius:4px;height:24px;position:relative;overflow:hidden">'
            f'<div style="background:#3f51b5;height:100%;width:{bar_width}%;border-radius:4px;'
            f'transition:width .3s"></div>'
            f'<div style="position:absolute;top:0;left:0;right:0;text-align:center;line-height:24px;'
            f'font-size:.8rem;font-weight:600;color:#fff">{cov_pct:.0%} screened</div></div>'
            f'<div style="display:flex;gap:16px;margin-top:8px;font-size:.85rem;color:#555">'
            f"<span>Haiku: {cp.get('haiku_screened', 0)}/{cp.get('universe_size', 0)}</span>"
            f"<span>Passed: {cp.get('haiku_passed', 0)}</span>"
            f"<span>Analyzed: {cp.get('deeply_analyzed', 0)}</span>"
            f"<span>Registry: {cp.get('total_studied_all_time', 0)} total</span>"
            f"</div></div>"
        )

    # Benchmark comparison
    if benchmark_data and (tier1 or tier2):
        bm_name = html_module.escape(benchmark_data.get("name", benchmark_data.get("symbol", "SPY")))
        bm_pe = benchmark_data.get("pe_ratio")
        bm_ytd = benchmark_data.get("ytd_return")
        bm_1y = benchmark_data.get("one_year_return")
        parts.append(f'<h3 style="font-size:1rem;margin:16px 0 8px">Benchmark: {bm_name}</h3>')
        parts.append('<div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:12px;font-size:.9rem">')
        if bm_pe:
            parts.append(f"<span>P/E: <strong>{bm_pe:.1f}</strong></span>")
        if bm_ytd is not None:
            parts.append(f"<span>YTD: <strong>{bm_ytd:+.1%}</strong></span>")
        if bm_1y is not None:
            parts.append(f"<span>1Y: <strong>{bm_1y:+.1%}</strong></span>")
        parts.append("</div>")
        parts.append("<table><tr><th>Stock</th><th>Tier</th><th>Price</th><th>Target</th><th>Gap</th></tr>")
        for b in (tier1 + tier2)[:15]:
            gap_str = f"{b.price_gap_pct:+.0%}" if b.price_gap_pct is not None else "N/A"
            target_str = f"${b.target_entry_price:,.0f}" if b.target_entry_price else "N/A"
            gap_color = ' style="color:#4CAF50"' if (b.price_gap_pct or 0) <= 0 else ""
            parts.append(
                f"<tr><td><strong>{e(b.symbol)}</strong></td>"
                f"<td>T{b.tier}</td>"
                f"<td>${b.current_price:,.0f}</td>"
                f"<td>{target_str}</td>"
                f"<td{gap_color}>{gap_str}</td></tr>"
            )
        parts.append("</table>")

    parts.append("</section>")

    # Movement Log
    if movements:
        parts.append("<section><h2>Movement Log</h2><div class='movement-log'>")
        for m in movements:
            badge_class = {
                "new": "mv-new",
                "removed": "mv-removed",
                "tier_up": "mv-up",
                "tier_down": "mv-down",
                "approaching": "mv-approaching",
            }.get(m.change_type, "mv-removed")
            label = m.change_type.upper().replace("_", " ")
            parts.append(
                f'<div class="movement-item">'
                f'<span class="movement-badge {badge_class}">{e(label)}</span>'
                f"<strong>{e(m.symbol)}</strong> {e(m.detail)}"
                f"</div>"
            )
        parts.append("</div></section>")

    # Approaching Target Alerts
    if approaching:
        parts.append('<section><h2 style="color:#E91E63">Approaching Target Price</h2>')
        parts.append(
            '<p style="font-size:.9rem;color:#666;margin-bottom:12px">'
            "These Tier 2 companies are within striking distance of buy range.</p>"
        )
        for b in approaching:
            gap = b.price_gap_pct or 0
            parts.append('<div class="stock-card approaching">')
            parts.append(f"<h3>{e(b.symbol)}: {e(b.company_name)}</h3>")
            parts.append('<span class="tier-badge tier-approaching">APPROACHING T1</span>')
            parts.append(f"<table><tr><td>Current Price</td><td>${b.current_price:,.2f}</td></tr>")
            parts.append(
                f"<tr><td>Target Entry</td><td>${b.target_entry_price:,.2f}</td></tr>" if b.target_entry_price else ""
            )
            parts.append(f"<tr><td>Gap</td><td>{gap:+.1%}</td></tr></table>")
            parts.append("</div>")
        parts.append("</section>")

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

        positions = portfolio_summary.get("positions", [])
        if positions:
            parts.append('<h3 style="font-size:1rem;margin:16px 0 8px">Positions</h3>')
            parts.append("<table><tr><th>Ticker</th><th>Shares</th><th>Cost</th><th>Value</th><th>P&amp;L</th><th>P&amp;L%</th></tr>")
            for p in positions:
                pl = p.get("unrealized_pl", 0)
                plpc = p.get("unrealized_plpc", 0)
                pl_color = "color:#4CAF50" if pl >= 0 else "color:#F44336"
                pl_sign = "+" if pl >= 0 else ""
                parts.append(
                    f"<tr><td><strong>{e(p['symbol'])}</strong></td>"
                    f"<td>{p.get('qty', 0):.1f}</td>"
                    f"<td>${p.get('avg_entry_price', 0):.2f}</td>"
                    f"<td>${p.get('market_value', 0):,.0f}</td>"
                    f"<td style='{pl_color}'>{pl_sign}${abs(pl):,.0f}</td>"
                    f"<td style='{pl_color}'>{pl_sign}{abs(plpc):.1%}</td></tr>"
                )
            parts.append("</table>")

        exposure = portfolio_summary.get("sector_exposure", {})
        if exposure:
            parts.append('<h3 style="font-size:1rem;margin-bottom:8px">Sector Exposure</h3><div class="bar-chart">')
            for sector, pct in sorted(exposure.items(), key=lambda x: x[1], reverse=True):
                width = max(1, int(pct * 100))
                parts.append(
                    f'<div class="bar-row"><span class="bar-label">{e(sector)}</span><div class="bar-track"><div class="bar-fill" style="width:{width}%"></div></div><span class="bar-pct">{pct:.0%}</span></div>'
                )
            parts.append("</div>")
        parts.append("</section>")

    # Tier 1 Picks
    if tier1:
        parts.append("<section><h2>Tier 1: Buy Zone</h2>")
        for b in tier1:
            parts.append(_html_stock_card(b, "tier1"))
        parts.append("</section>")

    # Second Opinion (Opus)
    opus_picks = [b for b in briefings if b.opus_opinion]
    if opus_picks:
        parts.append("<section><h2>Second Opinion (Opus Contrarian Review)</h2>")
        for b in opus_picks:
            assert b.opus_opinion is not None  # nosec B101 — filtered above
            op = b.opus_opinion
            agreement = op.get("agreement", "N/A")
            opus_conv = op.get("opus_conviction", "N/A")
            badge_colors = {
                "AGREE": "#4CAF50",
                "PARTIALLY_AGREE": "#FF9800",
                "DISAGREE": "#F44336",
            }
            badge_color = badge_colors.get(agreement, "#9E9E9E")
            parts.append(f'<div class="stock-card" style="border-left-color:{badge_color}">')
            parts.append(f"<h3>{e(b.symbol)}: {e(b.company_name)}</h3>")
            parts.append(
                f'<span class="tier-badge" style="background:{badge_color}">{e(agreement)}</span> '
                f'<span style="font-size:.85rem;color:#555">Opus Conviction: {e(opus_conv)}</span>'
            )
            risks = op.get("contrarian_risks", [])
            if risks:
                parts.append(
                    "<details open><summary>Contrarian Risks</summary><ul style='font-size:.9rem;margin:8px 0 0 20px'>"
                )
                for risk in risks[:3]:
                    parts.append(f"<li>{e(risk)}</li>")
                parts.append("</ul></details>")
            summary = op.get("summary", "")
            if summary:
                parts.append(f'<p style="font-size:.9rem;margin-top:8px"><em>{e(summary[:300])}</em></p>')
            parts.append("</div>")
        parts.append("</section>")

    # Tier 2 Watchlist
    if tier2:
        parts.append("<section><h2>Tier 2: Watchlist</h2>")
        for b in tier2:
            parts.append(_html_stock_card(b, "tier2"))
        parts.append("</section>")

    # Tier 3 Monitoring
    if tier3:
        parts.append("<section><h2>Tier 3: Monitoring</h2>")
        parts.append(
            '<p style="font-size:.9rem;color:#666;margin-bottom:12px">Good businesses to re-evaluate next cycle.</p>'
        )
        parts.append("<table><tr><th>Stock</th><th>Moat</th><th>Conviction</th><th>P/E</th><th>FCF Yield</th></tr>")
        for b in tier3:
            moat = getattr(b.analysis, "moat_rating", None)
            moat_str = moat.value.upper() if moat else "N/A"
            conv = getattr(b.analysis, "conviction_level", "N/A")
            pe_str = f"{b.pe_ratio:.1f}" if b.pe_ratio else "—"
            fcf_str = f"{b.fcf_yield:.1%}" if b.fcf_yield is not None else "—"
            parts.append(
                f"<tr><td><strong>{e(b.symbol)}</strong></td>"
                f"<td>{e(moat_str)}</td>"
                f"<td>{e(conv)}</td>"
                f"<td>{e(pe_str)}</td>"
                f"<td>{e(fcf_str)}</td></tr>"
            )
        parts.append("</table></section>")

    # Radar
    if radar_stocks:
        ctx = radar_context or {}
        has_context = any(ctx.get(s) for s in radar_stocks)
        if has_context:
            parts.append(
                '<section><h2>Radar</h2><p style="font-size:.9rem;color:#666;margin-bottom:12px">Passed Haiku screening, not yet deeply analyzed.</p>'
                '<table><tr><th>Ticker</th><th>Haiku Rationale</th></tr>'
            )
            for s in radar_stocks:
                reason = ctx.get(s, "")
                parts.append(
                    f"<tr><td><strong>{e(s)}</strong></td>"
                    f"<td style='font-size:.85rem;color:#555'>{e(reason[:120]) if reason else '—'}</td></tr>"
                )
            parts.append("</table></section>")
        else:
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
            parts.append(_html_bubble_card(warning))
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
<tr><td>Win Rate</td><td>{pm.get("win_rate", 0):.0%}</td></tr></table></section>""")

    # Footer
    parts.append(f"""<section style="background:#fafafa;font-size:.85rem;color:#777">
<p><strong>Disclaimer:</strong> This briefing is for research purposes only. All valuations are estimates.
You make the final investment decision. Past performance does not guarantee future results.
Patience is the strategy.</p>
</section>
<footer>Buffett Bot v2.0 &middot; {e(month_str)}</footer>
</div>
</body>
</html>""")

    return "\n".join(parts)


def _html_stock_card(briefing: StockBriefing, card_type: str) -> str:
    """Build an HTML card for a stock (tier1, tier2, or tier3)."""
    e = html_module.escape
    tier_class = {"tier1": "tier-1", "tier2": "tier-2", "tier3": "tier-3"}.get(card_type, "tier-2")
    tier_label = {"tier1": "TIER 1", "tier2": "TIER 2", "tier3": "TIER 3"}.get(card_type, f"TIER {briefing.tier}")

    lines = [f'<div class="stock-card {card_type}">']
    lines.append(f"<h3>{e(briefing.symbol)}: {e(briefing.company_name)}</h3>")
    lines.append(f'<span class="tier-badge {tier_class}">{tier_label}</span>')
    if briefing.tier_reason:
        lines.append(f'<span style="font-size:.85rem;color:#666;margin-left:8px">{e(briefing.tier_reason)}</span>')

    # Position sizing for Tier 1
    if card_type == "tier1" and briefing.position_size:
        sz = briefing.position_size
        lines.append(
            f'<div class="sizing"><strong>Position Sizing ({e(str(sz.get("conviction", "MEDIUM")))} conviction):</strong> '
            f"Recommended {sz.get('recommended_pct', 0):.0%} (${sz.get('recommended_amount', 0):,.0f}) &middot; "
            f"Max {sz.get('max_pct', 0):.0%} (${sz.get('max_amount', 0):,.0f})</div>"
        )

    # Staged entry for Tier 1
    if card_type == "tier1" and briefing.target_entry_price:
        tranches = staged_entry_suggestion(briefing.target_entry_price)
        lines.append('<div class="staged-entry"><strong>Staged Entry Plan:</strong><br>')
        for t in tranches:
            lines.append(f"&bull; {e(t['label'])}<br>")
        lines.append("</div>")

    # Data table
    lines.append("<table>")
    lines.append(f"<tr><td>Price</td><td>${briefing.current_price:.2f}</td></tr>")
    if briefing.target_entry_price:
        lines.append(f"<tr><td>Target Entry</td><td>${briefing.target_entry_price:.2f}</td></tr>")
    if briefing.price_gap_pct is not None:
        gap_color = "color:#4CAF50" if briefing.price_gap_pct <= 0 else "color:#F44336"
        lines.append(f'<tr><td>Price vs Target</td><td style="{gap_color}">{briefing.price_gap_pct:+.1%}</td></tr>')

    avg_fv = briefing.valuation.average_fair_value or 0
    mos = briefing.valuation.margin_of_safety or 0
    if avg_fv:
        lines.append(f"<tr><td>Fair Value (avg)</td><td>${avg_fv:.2f}</td></tr>")
    if mos:
        lines.append(f"<tr><td>Margin of Safety</td><td>{mos:.1%}</td></tr>")

    moat = getattr(briefing.analysis, "moat_rating", None)
    if moat:
        lines.append(f"<tr><td>Moat</td><td>{e(moat.value.upper())}</td></tr>")
    conv = getattr(briefing.analysis, "conviction_level", None)
    if conv:
        lines.append(f"<tr><td>Conviction</td><td>{e(conv)}</td></tr>")
    if briefing.pe_ratio:
        lines.append(f"<tr><td>P/E Ratio</td><td>{briefing.pe_ratio:.1f}</td></tr>")
    if briefing.roe:
        lines.append(f"<tr><td>ROE</td><td>{briefing.roe:.1%}</td></tr>")
    if briefing.debt_equity:
        lines.append(f"<tr><td>Debt/Equity</td><td>{briefing.debt_equity:.2f}</td></tr>")
    if briefing.fcf_yield is not None:
        lines.append(f"<tr><td>FCF Yield</td><td>{briefing.fcf_yield:.1%}</td></tr>")
    if briefing.operating_margin is not None:
        lines.append(f"<tr><td>Operating Margin</td><td>{briefing.operating_margin:.1%}</td></tr>")
    lines.append("</table>")

    # Valuation estimates
    if briefing.valuation.estimates:
        lines.append("<details><summary>Valuation Estimates</summary><table>")
        lines.append("<tr><th>Source</th><th>Fair Value</th></tr>")
        for est in briefing.valuation.estimates[:6]:
            lines.append(f"<tr><td>{e(est.source)}</td><td>${est.fair_value:.2f}</td></tr>")
        lines.append("</table></details>")

    # Bear case callout for Tier 2 (why you're not buying yet)
    if card_type == "tier2":
        moat_risks = getattr(briefing.analysis, "moat_risks", "")
        key_risks_t2 = getattr(briefing.analysis, "key_risks", [])
        bear_parts = []
        if moat_risks:
            bear_parts.append(f"<strong>Moat risk:</strong> {e(moat_risks[:120])}")
        if key_risks_t2:
            bear_parts.append(f"<strong>Key risk:</strong> {e(key_risks_t2[0][:120])}")
        if bear_parts:
            lines.append(
                '<div style="background:#fff3e0;border-left:3px solid #FF9800;padding:10px 14px;'
                'margin:12px 0;font-size:.88rem;border-radius:0 6px 6px 0">'
                + "<br>".join(bear_parts)
                + "</div>"
            )

    # Thesis
    thesis = getattr(briefing.analysis, "investment_thesis", "")
    if thesis:
        lines.append(
            f"<details><summary>Investment Thesis</summary><p style='font-size:.9rem;margin-top:8px'>{e(thesis[:600])}</p></details>"
        )

    # Risks
    key_risks = getattr(briefing.analysis, "key_risks", [])
    if key_risks:
        lines.append("<details><summary>Key Risks</summary><ul style='font-size:.9rem;margin:8px 0 0 20px'>")
        for risk in key_risks[:4]:
            lines.append(f"<li>{e(risk)}</li>")
        lines.append("</ul></details>")

    # Thesis-breaking events
    thesis_risks = getattr(briefing.analysis, "thesis_risks", [])
    if thesis_risks:
        lines.append(
            "<details><summary>Thesis-Breaking Events</summary><ul style='font-size:.9rem;margin:8px 0 0 20px;color:#d32f2f'>"
        )
        for risk in thesis_risks[:3]:
            lines.append(f"<li>{e(risk)}</li>")
        lines.append("</ul></details>")

    lines.append("</div>")
    return "\n".join(lines)


def _html_bubble_card(warning: BubbleWarning) -> str:
    """Build an HTML card for a bubble warning."""
    e = html_module.escape
    pe_str = f"{warning.pe_ratio:.1f}" if warning.pe_ratio else "N/A"
    lines = ['<div class="stock-card bubble">']
    lines.append(f"<h3>{e(warning.symbol)}: {e(warning.company_name)}</h3>")
    lines.append(f'<span class="tier-badge" style="background:#F44336">{e(warning.risk_level)} RISK</span>')
    lines.append(f"<table><tr><td>Price</td><td>${warning.current_price:.2f}</td></tr>")
    lines.append(f"<tr><td>P/E</td><td>{e(pe_str)}</td></tr></table>")
    if warning.signals:
        lines.append(
            "<details open><summary>Warning Signals</summary><ul style='font-size:.9rem;margin:8px 0 0 20px;color:#d32f2f'>"
        )
        for sig in warning.signals[:4]:
            lines.append(f"<li>{e(sig)}</li>")
        lines.append("</ul></details>")
    lines.append("</div>")
    return "\n".join(lines)
