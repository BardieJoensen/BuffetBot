"""
Text Briefing Formatter

Generates plain-text monthly watchlist briefing reports.
Extracted from briefing.py for separation of concerns.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Optional

from ..tier_engine import WatchlistMovement, staged_entry_suggestion

if TYPE_CHECKING:
    from ..bubble_detector import BubbleWarning
    from . import StockBriefing


def generate_text_report(
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
    """Generate a complete monthly briefing as plain text."""
    now = datetime.now()
    month_str = now.strftime("%B %Y")

    output: list[str] = []
    output.append("=" * 70)
    output.append(f"WATCHLIST UPDATE - {month_str}")
    output.append(f"Generated: {now.strftime('%Y-%m-%d %H:%M')}")
    output.append("=" * 70)
    output.append("")

    # Categorize by tier
    tier1 = [b for b in briefings if b.tier == 1]
    tier2 = [b for b in briefings if b.tier == 2]
    tier3 = [b for b in briefings if b.tier == 3]
    approaching = [b for b in tier2 if b.approaching_target]

    # MARKET TEMPERATURE
    if market_temp:
        output.append("## MARKET REGIME")
        output.append("")
        temp_emoji = {"COLD": ">>>", "COOL": ">>", "WARM": ">", "HOT": "!", "UNKNOWN": "?"}
        indicator = temp_emoji.get(market_temp.get("temperature", "UNKNOWN"), "?")
        output.append(f"[{indicator}] {market_temp.get('temperature', 'UNKNOWN')}")
        market_pe = market_temp.get("market_pe")
        output.append(f"Market P/E: {market_pe:.1f}" if market_pe else "Market P/E: N/A")
        output.append(f"Interpretation: {market_temp.get('interpretation', '')}")
        output.append("")

    # PORTFOLIO STATUS
    if portfolio_summary:
        output.append("-" * 70)
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
        exposure = portfolio_summary.get("sector_exposure", {})
        if exposure:
            output.append("Sector Exposure:")
            for sector, pct in sorted(exposure.items(), key=lambda x: x[1], reverse=True):
                bar = "#" * int(pct * 20)
                output.append(f"  {sector:20} {bar} {pct:.0%}")
            output.append("")
        warnings = portfolio_summary.get("sector_warnings", [])
        if warnings:
            output.append("CONCENTRATION WARNINGS:")
            for warning in warnings:
                output.append(f"  * {warning}")
            output.append("")
        alerts = portfolio_summary.get("alerts", [])
        if alerts:
            output.append("POSITION ALERTS:")
            for alert in alerts:
                output.append(f"  * {alert.get('symbol')}: {alert.get('message')}")
            output.append("")

    # EXECUTIVE SUMMARY
    output.append("-" * 70)
    output.append("## EXECUTIVE SUMMARY")
    output.append("")
    output.append(f"Stocks Analyzed:    {len(briefings)}")
    output.append(f"Tier 1 (Buy Zone):  {len(tier1)}")
    output.append(f"Tier 2 (Watchlist): {len(tier2)}")
    output.append(f"Tier 3 (Monitor):   {len(tier3)}")
    output.append(f"Approaching Target: {len(approaching)}")
    output.append(f"Bubble Watch:       {len(bubble_warnings) if bubble_warnings else 0}")
    output.append(f"Radar:              {len(radar_stocks) if radar_stocks else 0}")
    output.append("")

    # COVERAGE CAMPAIGN
    if campaign_progress:
        output.append("-" * 70)
        output.append("## COVERAGE CAMPAIGN")
        output.append("")
        cp = campaign_progress
        output.append(f"Campaign:           {cp.get('campaign_id', 'N/A')}")
        output.append(
            f"Haiku Screened:     {cp.get('haiku_screened', 0)}/{cp.get('universe_size', 0)} "
            f"({cp.get('coverage_pct', 0):.0%})"
        )
        output.append(f"Haiku Passed:       {cp.get('haiku_passed', 0)}")
        output.append(f"Deeply Analyzed:    {cp.get('deeply_analyzed', 0)}")
        output.append(f"Registry Total:     {cp.get('total_studied_all_time', 0)} companies (all campaigns)")
        est = cp.get("est_runs_remaining", 0)
        if est > 0:
            output.append(f"Est. Runs to Cover: {est}")
        stale = cp.get("stale_symbols", [])
        if stale:
            output.append(f"Stale (>{cp.get('max_age_days', 180)}d): {', '.join(stale[:10])}")
        output.append("")

    if tier1:
        output.append("Tier 1 Opportunities (at/below target entry):")
        for b in tier1:
            conv = getattr(b.analysis, "conviction_level", "N/A")
            output.append(
                f"  [T1] {b.symbol}: ${b.current_price:,.0f} (target ${b.target_entry_price:,.0f}), {conv} conviction"
            )
        output.append("")
    elif approaching:
        output.append("No Tier 1 picks yet, but these are approaching target:")
        for b in approaching:
            gap = b.price_gap_pct or 0
            output.append(f"  [!] {b.symbol}: {gap:+.0%} from target ${b.target_entry_price:,.0f}")
        output.append("")
    else:
        output.append("No Tier 1 picks this month. Patience is the strategy.")
        output.append("")

    # BENCHMARK COMPARISON
    if benchmark_data and (tier1 or tier2):
        output.append("-" * 70)
        output.append("## BENCHMARK COMPARISON")
        output.append("")
        bm_name = benchmark_data.get("name", benchmark_data.get("symbol", "SPY"))
        bm_pe = benchmark_data.get("pe_ratio")
        bm_ytd = benchmark_data.get("ytd_return")
        bm_1y = benchmark_data.get("one_year_return")
        bm_div = benchmark_data.get("dividend_yield")
        output.append(f"Benchmark: {bm_name}")
        if bm_pe:
            output.append(f"  P/E Ratio:      {bm_pe:.1f}")
        if bm_ytd is not None:
            output.append(f"  YTD Return:     {bm_ytd:+.1%}")
        if bm_1y is not None:
            output.append(f"  1Y Return:      {bm_1y:+.1%}")
        if bm_div is not None:
            output.append(f"  Dividend Yield: {bm_div:.2%}")
        output.append("")
        picks = tier1 + tier2
        output.append(f"{'Stock':<8} {'Tier':>4} {'P/E':>8} {'Gap':>10} {'Target':>10}")
        output.append(f"{'---':<8} {'---':>4} {'---':>8} {'---':>10} {'---':>10}")
        for b in picks[:15]:
            pe_str = f"{b.pe_ratio:.1f}" if b.pe_ratio else "N/A"
            gap_str = f"{b.price_gap_pct:+.0%}" if b.price_gap_pct is not None else "N/A"
            target = f"${b.target_entry_price:,.0f}" if b.target_entry_price else "N/A"
            output.append(f"{b.symbol:<8} {'T' + str(b.tier):>4} {pe_str:>8} {gap_str:>10} {target:>10}")
        output.append("")

    # MOVEMENT LOG
    if movements:
        output.append("-" * 70)
        output.append("## MOVEMENT LOG (Changes Since Last Briefing)")
        output.append("")
        for m in movements:
            icon = {
                "new": "[NEW]",
                "removed": "[OUT]",
                "tier_up": "[UP]",
                "tier_down": "[DN]",
                "approaching": "[!!]",
            }.get(m.change_type, "[--]")
            output.append(f"  {icon} {m.symbol}: {m.detail}")
        output.append("")

    # APPROACHING TARGET ALERTS
    if approaching:
        output.append("-" * 70)
        output.append("## APPROACHING TARGET PRICE")
        output.append("")
        output.append("These Tier 2 companies are within striking distance of buy range:")
        output.append("")
        for b in approaching:
            gap = b.price_gap_pct or 0
            output.append(
                f"  [!!] {b.symbol}: ${b.current_price:,.0f} -> target ${b.target_entry_price:,.0f} ({gap:+.0%})"
            )
            moat = getattr(b.analysis, "moat_rating", None)
            if moat:
                output.append(f"       Moat: {moat.value.upper()} | {b.tier_reason}")
        output.append("")

    # TIER 1: BUY ZONE
    if tier1:
        output.append("-" * 70)
        output.append("## TIER 1: BUY ZONE (At/Below Target Entry)")
        output.append("")
        for briefing in tier1:
            output.append(_format_tier1_briefing(briefing))
            output.append("")

    # SECOND OPINION (Opus contrarian review)
    opus_picks = [b for b in briefings if b.opus_opinion]
    if opus_picks:
        output.append("-" * 70)
        output.append("## SECOND OPINION (Opus Contrarian Review)")
        output.append("")
        for b in opus_picks:
            assert b.opus_opinion is not None
            op = b.opus_opinion
            agreement = op.get("agreement", "N/A")
            opus_conv = op.get("opus_conviction", "N/A")
            output.append(f"### {b.symbol}: {b.company_name}")
            output.append(f"   Agreement: {agreement} | Opus Conviction: {opus_conv}")
            risks = op.get("contrarian_risks", [])
            if risks:
                output.append("   Contrarian Risks:")
                for risk in risks[:3]:
                    output.append(f"     * {risk[:80]}")
            summary = op.get("summary", "")
            if summary:
                output.append(f"   Summary: {summary[:200]}")
            output.append("")

    # TIER 2: WATCHLIST
    if tier2:
        output.append("-" * 70)
        output.append("## TIER 2: WATCHLIST (Wonderful Business, Wait for Price)")
        output.append("")
        for briefing in sorted(tier2, key=lambda x: abs(x.price_gap_pct or 999)):
            output.append(_format_tier2_item(briefing))
            output.append("")

    # TIER 3: MONITORING
    if tier3:
        output.append("-" * 70)
        output.append("## TIER 3: MONITORING (Re-evaluate Next Cycle)")
        output.append("")
        for briefing in tier3:
            output.append(_format_tier3_item(briefing))
        output.append("")

    # RADAR
    if radar_stocks:
        output.append("-" * 70)
        output.append("## RADAR (Passed Screen, Not Yet Analyzed)")
        output.append("")
        output.append("These stocks passed quantitative screening but haven't received")
        output.append("deep analysis yet. Consider for future research:")
        output.append("")
        for i in range(0, len(radar_stocks), 5):
            chunk = radar_stocks[i : i + 5]
            output.append("  " + "  ".join(f"{s:8}" for s in chunk))
        output.append("")

    # BUBBLE WATCH
    if bubble_warnings:
        output.append("-" * 70)
        output.append("## BUBBLE WATCH (Avoid These)")
        output.append("")
        output.append("These stocks show signs of overvaluation. Do not buy.")
        output.append("")
        for warning in bubble_warnings[:5]:
            output.append(_format_bubble_warning(warning))
            output.append("")

    # PERFORMANCE
    if performance_metrics and performance_metrics.get("total_trades", 0) > 0:
        output.append("-" * 70)
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

    # FOOTER
    output.append("-" * 70)
    output.append("## REMINDER")
    output.append("")
    output.append("* This briefing is for research purposes only")
    output.append("* All valuations are estimates from external sources")
    output.append("* YOU make the final investment decision")
    output.append("* Past performance does not guarantee future results")
    output.append("* Patience is the strategy — wait for wonderful businesses at fair prices")
    output.append("")
    output.append("=" * 70)

    return "\n".join(output)


def _format_tier1_briefing(briefing: StockBriefing) -> str:
    """Format a full Tier 1 briefing with staged entry."""
    lines: list[str] = []

    lines.append(f"### {briefing.symbol}: {briefing.company_name}")
    lines.append(f"[TIER 1] {briefing.tier_reason}")
    lines.append("")

    # Position sizing
    if briefing.position_size:
        sizing = briefing.position_size
        lines.append(f"POSITION SIZING ({sizing.get('conviction', 'MEDIUM')} conviction):")
        lines.append(
            f"   Recommended: {sizing.get('recommended_pct', 0):.0%} of portfolio (${sizing.get('recommended_amount', 0):,.0f})"
        )
        lines.append(
            f"   Maximum:     {sizing.get('max_pct', 0):.0%} of portfolio (${sizing.get('max_amount', 0):,.0f})"
        )
        lines.append("")

    # Staged entry
    if briefing.target_entry_price:
        tranches = staged_entry_suggestion(briefing.target_entry_price)
        lines.append("STAGED ENTRY PLAN:")
        for t in tranches:
            lines.append(f"  * {t['label']}")
        lines.append("")

    # Qualitative
    lines.append("QUALITY ASSESSMENT:")
    moat = getattr(briefing.analysis, "moat_rating", None)
    conv = getattr(briefing.analysis, "conviction_level", "N/A")
    mgmt = getattr(briefing.analysis, "management_rating", None)
    lines.append(f"  Moat:       {moat.value.upper() if moat else 'N/A'}")
    lines.append(f"  Conviction: {conv}")
    if mgmt:
        lines.append(f"  Management: {mgmt.value.upper()}")
    moat_sources = getattr(briefing.analysis, "moat_sources", [])
    if moat_sources:
        lines.append(f"  Moat Sources: {', '.join(moat_sources[:3])}")
    lines.append("")

    # Quantitative
    lines.append("PRICE & VALUATION:")
    lines.append(f"  Current Price:  ${briefing.current_price:,.2f}")
    if briefing.target_entry_price:
        lines.append(f"  Target Entry:   ${briefing.target_entry_price:,.2f}")
    if briefing.price_gap_pct is not None:
        lines.append(f"  Price vs Target: {briefing.price_gap_pct:+.1%}")
    avg_fv = briefing.valuation.average_fair_value
    if avg_fv:
        lines.append(f"  Fair Value:     ${avg_fv:,.2f}")
    mos = briefing.valuation.margin_of_safety
    if mos:
        lines.append(f"  Margin of Safety: {mos:.1%}")
    lines.append("")
    if briefing.pe_ratio:
        lines.append(f"  P/E Ratio:      {briefing.pe_ratio:.1f}")
    if briefing.roe:
        lines.append(f"  ROE:            {briefing.roe:.1%}")
    if briefing.fcf_yield is not None:
        lines.append(f"  FCF Yield:      {briefing.fcf_yield:.1%}")
    if briefing.operating_margin is not None:
        lines.append(f"  Operating Margin: {briefing.operating_margin:.1%}")
    lines.append("")

    # Thesis
    thesis = getattr(briefing.analysis, "investment_thesis", "")
    if thesis:
        lines.append("INVESTMENT THESIS:")
        lines.append(thesis[:400])
        lines.append("")

    # Risks
    key_risks = getattr(briefing.analysis, "key_risks", [])
    if key_risks:
        lines.append("KEY RISKS:")
        for risk in key_risks[:3]:
            lines.append(f"  * {risk[:70]}")
        lines.append("")
    thesis_risks = getattr(briefing.analysis, "thesis_risks", [])
    if thesis_risks:
        lines.append("THESIS-BREAKING EVENTS (sell signals):")
        for risk in thesis_risks[:2]:
            lines.append(f"  !! {risk[:70]}")

    lines.append("")
    lines.append("-" * 60)

    return "\n".join(lines)


def _format_tier2_item(briefing: StockBriefing) -> str:
    """Format a Tier 2 watchlist item."""
    lines: list[str] = []

    gap = briefing.price_gap_pct
    gap_str = f"({gap:+.0%} from target)" if gap is not None else ""
    target_str = f"-> Target: ${briefing.target_entry_price:,.2f}" if briefing.target_entry_price else ""
    approaching_flag = " [APPROACHING]" if briefing.approaching_target else ""

    moat = getattr(briefing.analysis, "moat_rating", None)
    conv = getattr(briefing.analysis, "conviction_level", "N/A")

    lines.append(f"[T2]{approaching_flag} {briefing.symbol}: {briefing.company_name}")
    lines.append(f"   Price: ${briefing.current_price:,.2f} {target_str} {gap_str}")
    lines.append(f"   Moat: {moat.value.upper() if moat else 'N/A'} | Conviction: {conv}")
    lines.append(f"   {briefing.tier_reason}")

    return "\n".join(lines)


def _format_tier3_item(briefing: StockBriefing) -> str:
    """Format a Tier 3 monitoring item (brief)."""
    moat = getattr(briefing.analysis, "moat_rating", None)
    conv = getattr(briefing.analysis, "conviction_level", "N/A")
    return (
        f"  [T3] {briefing.symbol}: {moat.value.upper() if moat else 'N/A'} moat, "
        f"{conv} conviction - {briefing.tier_reason}"
    )


def _format_bubble_warning(warning: BubbleWarning) -> str:
    """Format a bubble warning."""
    lines: list[str] = []

    risk_indicator = "[HIGH]" if warning.risk_level == "HIGH" else "[MED]"
    lines.append(f"{risk_indicator} {warning.symbol}: {warning.company_name}")
    pe_str = f"{warning.pe_ratio:.1f}" if warning.pe_ratio else "N/A"
    lines.append(f"   Price: ${warning.current_price:.2f} | P/E: {pe_str}")
    lines.append(f"   Signals ({warning.signal_count}):")
    for signal in warning.signals[:3]:
        lines.append(f"     * {signal[:60]}")

    return "\n".join(lines)
