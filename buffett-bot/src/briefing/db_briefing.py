"""
DB-Driven Briefing Generator — Phase F

Reads all state from SQLite and produces S/A/B/C-tier formatted briefings.
No external API calls — entirely offline.

Sections (text output):
    1. Header (date, weekly budget usage)
    2. S-Tier Spotlight (full deep-dive for each S-tier stock)
    3. A-Tier Action List (staged entries, conviction, gap)
    4. B-Tier Approaching Target (sorted by gap_pct ASC)
    5. Quality Score Leaderboard (top 10 by quality_score)
    6. News Events Digest (material events, last 7 days)
    7. Coverage Dashboard (by sector and cap_category)
    8. Paper Trading Scoreboard
    9. B-Tier Watch (remaining B stocks, gap farther out)
   10. C-Tier brief list
   11. Footer

Public API:
    generate_briefing_from_db(db, *, output_dir=None, days_back=7)
        → returns (text: str, html: str)
"""

from __future__ import annotations

import html as html_module
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ..database import Database

# Gap threshold below which a B-tier stock is flagged as "approaching"
_APPROACHING_GAP_PCT = 0.10  # within 10% of target


# ─── Text report ──────────────────────────────────────────────────────────────


def _text_briefing(
    db: "Database",
    *,
    days_back: int = 7,
) -> str:
    """Build the complete plain-text briefing from DB state."""
    now = datetime.now()
    out: list[str] = []

    # ── Header ────────────────────────────────────────────────────────────────
    out.append("=" * 70)
    out.append(f"BUFFETT BOT BRIEFING — {now.strftime('%B %Y')}")
    out.append(f"Generated: {now.strftime('%Y-%m-%d %H:%M')}")
    out.append("=" * 70)
    out.append("")

    # ── Budget status ─────────────────────────────────────────────────────────
    cap_types = [
        ("weekly_haiku_screen", 50, _HAIKU_COST),
        ("weekly_sonnet_analysis", 10, _SONNET_COST),
        ("weekly_news_haiku", 50, _HAIKU_COST),
        ("weekly_news_sonnet", 10, _SONNET_COST),
    ]
    total_cost = 0.0
    out.append("WEEKLY BUDGET STATUS")
    for cap_type, max_calls, unit_cost in cap_types:
        status = db.get_budget_status(cap_type)
        used = status.get("calls_used", 0)
        cost = used * unit_cost
        total_cost += cost
        out.append(f"  {cap_type:<30} {used:>2}/{max_calls}  (~${cost:.3f})")
    out.append(f"  {'Total this week':<30}       (~${total_cost:.3f})")
    out.append("")

    # ── Pull data from DB ─────────────────────────────────────────────────────
    all_alerts = db.get_price_alerts()  # sorted by gap_pct ASC
    universe = db.get_universe()
    analyses = db.get_all_latest_deep_analyses()
    news_items = db.get_recent_news_events(days_back=days_back)
    positions = db.get_paper_positions()

    # Build lookup maps
    analysis_by_ticker = {a["ticker"]: a for a in analyses}
    universe_by_ticker = {u["ticker"]: u for u in universe}

    s_alerts = [a for a in all_alerts if a["tier"] == "S"]
    a_alerts = [a for a in all_alerts if a["tier"] == "A"]
    b_alerts = [a for a in all_alerts if a["tier"] == "B"]
    c_alerts = [a for a in all_alerts if a["tier"] == "C"]

    # Split B into approaching vs. watch
    b_approaching = [a for a in b_alerts if (a.get("gap_pct") or 999) <= _APPROACHING_GAP_PCT]
    b_watch = [a for a in b_alerts if (a.get("gap_pct") or 999) > _APPROACHING_GAP_PCT]

    position_by_ticker = {p["ticker"]: p for p in positions}

    # ── S-Tier Spotlight ──────────────────────────────────────────────────────
    if s_alerts:
        out.append("-" * 70)
        out.append("## S-TIER SPOTLIGHT: WONDERFUL BUSINESSES AT PRICE")
        out.append("")
        for alert in s_alerts:
            out.extend(_text_s_tier(alert, analysis_by_ticker, universe_by_ticker, position_by_ticker))
            out.append("")

    # ── A-Tier Action List ─────────────────────────────────────────────────────
    if a_alerts:
        out.append("-" * 70)
        out.append("## A-TIER ACTION LIST: GOOD BUSINESSES AT PRICE")
        out.append("")
        for alert in a_alerts:
            out.extend(_text_a_tier(alert, analysis_by_ticker, universe_by_ticker, position_by_ticker))
            out.append("")

    # ── B-Tier Approaching Target ─────────────────────────────────────────────
    out.append("-" * 70)
    out.append("## B-TIER APPROACHING TARGET (sorted by gap, closest first)")
    out.append("")
    if b_approaching:
        out.append(f"  {'Ticker':<6}  {'Company':<30}  {'Price':>9}  {'Target':>9}  {'Gap':>7}")
        out.append(f"  {'------':<6}  {'-------':<30}  {'---------':>9}  {'---------':>9}  {'-------':>7}")
        for a in b_approaching:
            ticker = a["ticker"]
            u = universe_by_ticker.get(ticker, {})
            name = (u.get("company_name") or ticker)[:28]
            price = a.get("last_price") or 0
            target = a.get("target_entry") or 0
            gap = a.get("gap_pct") or 0
            star = " ***" if gap <= 0.05 else ""
            out.append(f"  {ticker:<6}  {name:<30}  ${price:>8,.2f}  ${target:>8,.2f}  {gap:>+6.1%}{star}")
        out.append("")
    else:
        out.append("  No B-tier stocks within 10% of target.")
        out.append("")

    # ── Quality Score Leaderboard ─────────────────────────────────────────────
    out.append("-" * 70)
    out.append("## TOP 10 BY QUALITY SCORE (regardless of price)")
    out.append("")
    top10 = sorted(
        [u for u in universe if u.get("quality_score") is not None],
        key=lambda u: u["quality_score"],
        reverse=True,
    )[:10]
    if top10:
        out.append(f"  {'#':<3}  {'Ticker':<6}  {'Company':<30}  {'Score':>5}  {'Tier':<4}  {'Gap':>8}")
        out.append(f"  {'---':<3}  {'------':<6}  {'-------':<30}  {'-----':>5}  {'----':<4}  {'--------':>8}")
        for rank, u in enumerate(top10, 1):
            ticker = u["ticker"]
            alert = next((a for a in all_alerts if a["ticker"] == ticker), {})
            tier = alert["tier"] if alert else "—"
            gap = alert.get("gap_pct") if alert else None
            gap_str = f"{gap:+.1%}" if gap is not None else "—"
            name = (u.get("company_name") or ticker)[:28]
            score = u["quality_score"]
            out.append(f"  {rank:<3}  {ticker:<6}  {name:<30}  {score:>5.1f}  {tier:<4}  {gap_str:>8}")
        out.append("")
    else:
        out.append("  No quality scores available yet. Run Monday maintenance to compute.")
        out.append("")

    # ── News Events Digest ────────────────────────────────────────────────────
    out.append("-" * 70)
    out.append(f"## NEWS EVENTS DIGEST (last {days_back} days)")
    out.append("")
    if news_items:
        for item in news_items:
            ts = (item.get("detected_at") or "")[:10]
            ticker = item.get("ticker", "?")
            headline = (item.get("headline") or "")[:65]
            material = bool(item.get("haiku_material"))
            sonnet = bool(item.get("sonnet_triggered"))
            flags = []
            if material:
                flags.append("Haiku: MATERIAL")
            if sonnet:
                flags.append("Sonnet: re-analyzed")
            flag_str = " → " + " → ".join(flags) if flags else " → Haiku: ignored"
            out.append(f"  {ts}  {ticker:<6}  {headline}")
            out.append(f"  {'':10}  {'':6}  {flag_str}")
        out.append("")
    else:
        out.append(f"  No news events detected in the last {days_back} days.")
        out.append("")

    # ── Coverage Dashboard ────────────────────────────────────────────────────
    out.append("-" * 70)
    out.append("## COVERAGE DASHBOARD")
    out.append("")
    total_universe = len(universe)
    analyzed_tickers = set(a["ticker"] for a in analyses)

    out.append(f"  Universe total:       {total_universe} stocks")
    out.append(
        f"  Deep-analyzed:        {len(analyzed_tickers)} stocks ({len(analyzed_tickers) / max(total_universe, 1):.1%})"
    )
    out.append("")

    # By sector
    sector_total: dict[str, int] = {}
    sector_analyzed: dict[str, int] = {}
    for u in universe:
        sector = u.get("sector") or "Unknown"
        sector_total[sector] = sector_total.get(sector, 0) + 1
        if u["ticker"] in analyzed_tickers:
            sector_analyzed[sector] = sector_analyzed.get(sector, 0) + 1

    if sector_total:
        out.append(f"  {'Sector':<22}  {'Total':>5}  {'Analyzed':>8}  {'Cov%':>5}  Progress")
        out.append(f"  {'------':<22}  {'-----':>5}  {'--------':>8}  {'----':>5}")
        for sector, total in sorted(sector_total.items(), key=lambda x: x[1], reverse=True)[:10]:
            analyzed = sector_analyzed.get(sector, 0)
            pct = analyzed / total
            bar = "#" * int(pct * 20) + "." * (20 - int(pct * 20))
            out.append(f"  {sector:<22}  {total:>5}  {analyzed:>8}  {pct:>4.0%}  |{bar}|")
        out.append("")

    # By cap category
    cap_total: dict[str, int] = {}
    cap_analyzed: dict[str, int] = {}
    for u in universe:
        cap = u.get("cap_category") or "unknown"
        cap_total[cap] = cap_total.get(cap, 0) + 1
        if u["ticker"] in analyzed_tickers:
            cap_analyzed[cap] = cap_analyzed.get(cap, 0) + 1

    if cap_total:
        out.append(f"  {'Cap Size':<10}  {'Total':>5}  {'Analyzed':>8}  {'Cov%':>5}")
        out.append(f"  {'--------':<10}  {'-----':>5}  {'--------':>8}  {'----':>5}")
        for cap_label, cap_key in [("Large ($10B+)", "large"), ("Mid ($1-10B)", "mid"), ("Small (<$1B)", "small")]:
            total = cap_total.get(cap_key, 0)
            analyzed = cap_analyzed.get(cap_key, 0)
            pct = analyzed / total if total else 0
            out.append(f"  {cap_label:<10}  {total:>5}  {analyzed:>8}  {pct:>4.0%}")
        out.append("")

    # ── Paper Trading Scoreboard ──────────────────────────────────────────────
    out.append("-" * 70)
    out.append("## PAPER TRADING SCOREBOARD")
    out.append("")
    if positions:
        total_cost_basis = sum(p.get("cost_basis") or 0 for p in positions)
        total_value = sum(p.get("current_value") or 0 for p in positions)
        total_pl = total_value - total_cost_basis
        total_pl_pct = total_pl / total_cost_basis if total_cost_basis else 0
        pl_sign = "+" if total_pl >= 0 else ""

        out.append(
            f"  {'Ticker':<6}  {'Tier':<4}  {'Stage':<5}  {'Entry':>8}  {'Current':>8}  {'P&L':>10}  {'P&L%':>7}"
        )
        out.append(
            f"  {'------':<6}  {'----':<4}  {'-----':<5}  {'--------':>8}  {'--------':>8}  {'----------':>10}  {'-------':>7}"
        )
        for p in sorted(positions, key=lambda x: -(x.get("current_value") or 0)):
            ticker = p["ticker"]
            tier = p.get("tier_at_entry", "?")
            stage = p.get("entry_stage", "?") or "?"
            entry = p.get("entry_price") or 0
            current = p.get("current_price") or 0
            basis = p.get("cost_basis") or 0
            value = p.get("current_value") or 0
            pl = value - basis
            pl_pct = pl / basis if basis else 0
            pl_s = "+" if pl >= 0 else ""
            out.append(
                f"  {ticker:<6}  {tier:<4}  {stage:<5}  ${entry:>7,.2f}  ${current:>7,.2f}"
                f"  {pl_s}${abs(pl):>8,.0f}  {pl_s}{abs(pl_pct):>5.1%}"
            )
        out.append("")
        out.append(f"  Total cost basis: ${total_cost_basis:,.0f}")
        out.append(f"  Total value:      ${total_value:,.0f}")
        out.append(f"  Total P&L:        {pl_sign}${abs(total_pl):,.0f}  ({pl_sign}{abs(total_pl_pct):.1%})")
        out.append("")
        out.append("  Note: Benchmark comparison auto-updates on Monday maintenance.")
        out.append("")
    else:
        out.append("  No paper positions open.")
        out.append("")

    # ── B-Tier Watch (farther from target) ────────────────────────────────────
    if b_watch:
        out.append("-" * 70)
        out.append("## B-TIER WATCH (price not yet approaching target)")
        out.append("")
        out.append(f"  {'Ticker':<6}  {'Company':<30}  {'Price':>9}  {'Target':>9}  {'Gap':>7}")
        out.append(f"  {'------':<6}  {'-------':<30}  {'---------':>9}  {'---------':>9}  {'-------':>7}")
        for a in b_watch:
            ticker = a["ticker"]
            u = universe_by_ticker.get(ticker, {})
            name = (u.get("company_name") or ticker)[:28]
            price = a.get("last_price") or 0
            target = a.get("target_entry") or 0
            gap = a.get("gap_pct") or 0
            out.append(f"  {ticker:<6}  {name:<30}  ${price:>8,.2f}  ${target:>8,.2f}  {gap:>+6.1%}")
        out.append("")

    # ── C-Tier ────────────────────────────────────────────────────────────────
    if c_alerts:
        out.append("-" * 70)
        out.append("## C-TIER MONITOR (brief — re-evaluate next cycle)")
        out.append("")
        symbols = [a["ticker"] for a in c_alerts]
        for i in range(0, len(symbols), 8):
            out.append("  " + "  ".join(f"{s:<6}" for s in symbols[i : i + 8]))
        out.append(f"  ({len(symbols)} stocks total)")
        out.append("")

    # ── Footer ────────────────────────────────────────────────────────────────
    out.append("-" * 70)
    out.append("## TIER REFERENCE")
    out.append("")
    out.append("  S — Wonderful business at/below fair value → Buy (3 tranches: 1/3, 2/3, 3/3)")
    out.append("  A — Good business at/below target entry  → Buy (2 tranches: 1/2, 2/2)")
    out.append("  B — Quality business, price not right yet → Watch, set alerts")
    out.append("  C — Monitor passively, re-evaluate next cycle")
    out.append("")
    out.append("  * Patience is the strategy.")
    out.append("  * This briefing is for research only. You make the final decision.")
    out.append("=" * 70)

    return "\n".join(out)


# ─── Per-tier text helpers ─────────────────────────────────────────────────────


_HAIKU_COST = 0.001
_SONNET_COST = 0.025


def _fmt_gap(gap: Optional[float]) -> str:
    if gap is None:
        return "N/A"
    return f"{gap:+.1%}"


def _text_s_tier(
    alert: dict,
    analysis_map: dict,
    universe_map: dict,
    position_map: dict,
) -> list[str]:
    ticker = alert["ticker"]
    da = analysis_map.get(ticker, {})
    u = universe_map.get(ticker, {})
    pos = position_map.get(ticker)

    name = da.get("company_name") or u.get("company_name") or ticker
    lines: list[str] = []

    lines.append(f"[S] {ticker}: {name}")
    lines.append("=" * 60)

    conviction = da.get("conviction", "N/A") if da else "N/A"
    moat = da.get("moat_rating", "N/A") if da else "N/A"
    fair_value = da.get("fair_value") if da else None
    target = alert.get("target_entry")
    last_price = alert.get("last_price") or 0
    gap = alert.get("gap_pct")

    lines.append(f"[TIER S]  Moat: {moat} | Conviction: {conviction}")
    lines.append("")

    # Staged entries
    staged = alert.get("staged_entries") or {}
    if staged:
        lines.append("STAGED ENTRY PLAN:")
        if isinstance(staged, list):
            for entry in staged:
                tranche = entry.get("tranche", "?")
                price = entry.get("price", 0)
                label = entry.get("label", f"Tranche {tranche}")
                lines.append(f"  * {label}: ${price:,.2f}")
        elif isinstance(staged, dict):
            for label, price in staged.items():
                lines.append(f"  * {label}: ${price:,.2f}")
        lines.append("")

    lines.append("PRICE & VALUATION:")
    lines.append(f"  Current Price: ${last_price:,.2f}")
    if target:
        lines.append(f"  Target Entry:  ${target:,.2f}")
    lines.append(f"  Gap:           {_fmt_gap(gap)}")
    if fair_value:
        lines.append(f"  Fair Value:    ${fair_value:,.2f}")
    lines.append("")

    thesis = da.get("investment_thesis", "") if da else ""
    if thesis:
        lines.append("INVESTMENT THESIS:")
        # Wrap at 66 chars
        words = thesis[:500].split()
        current_line: list[str] = []
        for word in words:
            if sum(len(w) + 1 for w in current_line) + len(word) > 66:
                lines.append("  " + " ".join(current_line))
                current_line = [word]
            else:
                current_line.append(word)
        if current_line:
            lines.append("  " + " ".join(current_line))
        lines.append("")

    risks = (da.get("key_risks") or []) if da else []
    if risks:
        lines.append("KEY RISKS:")
        for r in risks[:3]:
            lines.append(f"  * {r[:68]}")
        lines.append("")

    thesis_breakers = (da.get("thesis_breakers") or []) if da else []
    if thesis_breakers:
        lines.append("THESIS-BREAKING EVENTS (sell triggers):")
        for tb in thesis_breakers[:2]:
            lines.append(f"  !! {tb[:66]}")
        lines.append("")

    if pos:
        basis = pos.get("cost_basis") or 0
        value = pos.get("current_value") or 0
        pl = value - basis
        pl_s = "+" if pl >= 0 else ""
        stage = pos.get("entry_stage", "?")
        lines.append(f"PAPER POSITION: {stage}  |  P&L: {pl_s}${abs(pl):,.0f}")
        lines.append("")

    lines.append("-" * 60)
    return lines


def _text_a_tier(
    alert: dict,
    analysis_map: dict,
    universe_map: dict,
    position_map: dict,
) -> list[str]:
    ticker = alert["ticker"]
    da = analysis_map.get(ticker, {})
    u = universe_map.get(ticker, {})
    pos = position_map.get(ticker)
    name = da.get("company_name") or u.get("company_name") or ticker
    conviction = da.get("conviction", "N/A") if da else "N/A"
    moat = da.get("moat_rating", "N/A") if da else "N/A"
    target = alert.get("target_entry")
    last_price = alert.get("last_price") or 0
    gap = alert.get("gap_pct")

    lines = [f"[A] {ticker}: {name}"]
    lines.append(f"  Moat: {moat} | Conviction: {conviction} | Gap: {_fmt_gap(gap)}")
    lines.append(f"  Price: ${last_price:,.2f}  Target: ${target:,.2f}" if target else f"  Price: ${last_price:,.2f}")

    staged = alert.get("staged_entries") or {}
    if staged:
        entry_strs = []
        if isinstance(staged, list):
            for entry in staged:
                entry_strs.append(f"{entry.get('label', '?')}: ${entry.get('price', 0):,.2f}")
        elif isinstance(staged, dict):
            for label, price in staged.items():
                entry_strs.append(f"{label}: ${price:,.2f}")
        if entry_strs:
            lines.append("  Entry: " + "  |  ".join(entry_strs))

    thesis = (da.get("investment_thesis") or "")[:120] if da else ""
    if thesis:
        lines.append(f"  Thesis: {thesis}...")

    if pos:
        basis = pos.get("cost_basis") or 0
        value = pos.get("current_value") or 0
        pl = value - basis
        pl_s = "+" if pl >= 0 else ""
        stage = pos.get("entry_stage", "?")
        lines.append(f"  Paper: {stage}  P&L: {pl_s}${abs(pl):,.0f}")

    return lines


# ─── HTML report ──────────────────────────────────────────────────────────────


def _html_briefing(
    db: "Database",
    *,
    days_back: int = 7,
) -> str:
    """Build a self-contained HTML briefing from DB state."""
    now = datetime.now()
    e = html_module.escape
    month_str = now.strftime("%B %Y")

    all_alerts = db.get_price_alerts()
    universe = db.get_universe()
    analyses = db.get_all_latest_deep_analyses()
    news_items = db.get_recent_news_events(days_back=days_back)
    positions = db.get_paper_positions()

    analysis_by_ticker = {a["ticker"]: a for a in analyses}
    universe_by_ticker = {u["ticker"]: u for u in universe}
    position_by_ticker = {p["ticker"]: p for p in positions}

    s_alerts = [a for a in all_alerts if a["tier"] == "S"]
    a_alerts = [a for a in all_alerts if a["tier"] == "A"]
    b_alerts = [a for a in all_alerts if a["tier"] == "B"]
    c_alerts = [a for a in all_alerts if a["tier"] == "C"]
    b_approaching = [a for a in b_alerts if (a.get("gap_pct") or 999) <= _APPROACHING_GAP_PCT]
    b_watch = [a for a in b_alerts if (a.get("gap_pct") or 999) > _APPROACHING_GAP_PCT]

    top10 = sorted(
        [u for u in universe if u.get("quality_score") is not None],
        key=lambda u: u["quality_score"],
        reverse=True,
    )[:10]

    analyzed_tickers = set(a["ticker"] for a in analyses)
    total_universe = len(universe)

    # Budget
    cap_types = [
        ("weekly_haiku_screen", 50, _HAIKU_COST),
        ("weekly_sonnet_analysis", 10, _SONNET_COST),
        ("weekly_news_haiku", 50, _HAIKU_COST),
        ("weekly_news_sonnet", 10, _SONNET_COST),
    ]
    total_cost = 0.0
    budget_rows = []
    for cap_type, max_calls, unit_cost in cap_types:
        status = db.get_budget_status(cap_type)
        used = status.get("calls_used", 0)
        cost = used * unit_cost
        total_cost += cost
        pct = used / max_calls
        budget_rows.append((cap_type, used, max_calls, pct, cost))

    parts: list[str] = []
    parts.append(f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Buffett Bot Briefing &mdash; {e(month_str)}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  background:#f5f5f5;color:#333;line-height:1.6}}
.container{{max-width:960px;margin:0 auto;padding:16px}}
header{{background:#1a237e;color:#fff;padding:28px 24px;border-radius:8px 8px 0 0}}
header h1{{font-size:1.4rem;font-weight:700}}
header .sub{{opacity:.8;font-size:.85rem;margin-top:4px}}
section{{background:#fff;padding:20px 24px;margin-bottom:2px}}
section:last-child{{border-radius:0 0 8px 8px;margin-bottom:24px}}
h2{{font-size:1.1rem;color:#1a237e;border-bottom:2px solid #e8eaf6;padding-bottom:6px;margin-bottom:14px}}
h3{{font-size:1rem;margin:0 0 4px 0}}
.tier-badge{{display:inline-block;padding:1px 10px;border-radius:10px;font-size:.72rem;
  font-weight:700;color:#fff;letter-spacing:.04em}}
.s{{background:#7b1fa2}}.a{{background:#1565c0}}.b{{background:#e65100}}.c{{background:#757575}}
.approaching{{background:#ad1457}}
.card{{border:1px solid #e0e0e0;border-radius:8px;padding:16px;margin-bottom:12px}}
.card.s-tier{{border-left:5px solid #7b1fa2}}
.card.a-tier{{border-left:5px solid #1565c0}}
.card.b-approaching{{border-left:5px solid #ad1457}}
table{{width:100%;border-collapse:collapse;font-size:.88rem;margin:8px 0}}
th{{text-align:left;padding:6px 10px;background:#f5f5f5;border-bottom:2px solid #ddd;
  font-weight:600;color:#555}}
td{{padding:6px 10px;border-bottom:1px solid #eee}}
.num-right{{text-align:right}}
.gap-pos{{color:#c62828}}.gap-neg{{color:#2e7d32}}
.budget-bar{{background:#e8eaf6;border-radius:4px;height:14px;overflow:hidden;display:inline-block;
  width:100px;vertical-align:middle}}
.budget-fill{{background:#3f51b5;height:100%}}
.bar-row{{display:flex;align-items:center;gap:8px;margin:3px 0;font-size:.82rem}}
.bar-label{{width:180px;flex-shrink:0;color:#555;text-align:right;padding-right:8px}}
.bar-track{{flex:1;background:#e8eaf6;border-radius:3px;height:16px;overflow:hidden}}
.bar-fill2{{background:#3f51b5;height:100%}}
.bar-pct{{width:40px;color:#555;font-size:.8rem}}
.summary-grid{{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:14px}}
.scard{{background:#f5f5f5;border-radius:8px;padding:12px 16px;text-align:center;flex:1 1 0;min-width:70px}}
.scard .n{{font-size:1.6rem;font-weight:700}}
.staged{{background:#e3f2fd;border-radius:6px;padding:10px 12px;margin:8px 0;font-size:.85rem}}
.thesis-box{{background:#f9f9f9;border-left:3px solid #9e9e9e;padding:10px 12px;
  margin:8px 0;font-size:.87rem;font-style:italic}}
.news-item{{padding:8px 0;border-bottom:1px solid #f0f0f0;font-size:.87rem}}
.news-item:last-child{{border-bottom:none}}
.chip{{display:inline-block;padding:1px 8px;border-radius:8px;font-size:.7rem;font-weight:600;
  color:#fff;margin-left:4px}}
.chip-material{{background:#c62828}}.chip-sonnet{{background:#6a1b9a}}.chip-ignored{{background:#9e9e9e}}
footer{{text-align:center;padding:12px;font-size:.78rem;color:#999}}
details{{margin:6px 0}}
summary{{cursor:pointer;font-weight:600;color:#1a237e;font-size:.88rem}}
@media(max-width:600px){{.container{{padding:8px}}header{{padding:16px}}
  section{{padding:14px}}.scard{{min-width:60px}}}}
</style>
</head>
<body>
<div class="container">
<header>
  <h1>Buffett Bot Briefing &mdash; {e(month_str)}</h1>
  <div class="sub">Generated {now.strftime("%Y-%m-%d %H:%M")}</div>
</header>
""")

    # ── Executive Summary ─────────────────────────────────────────────────────
    parts.append("<section>")
    parts.append("<h2>Executive Summary</h2>")
    parts.append('<div class="summary-grid">')
    for label, count, color in [
        ("S-tier", len(s_alerts), "#7b1fa2"),
        ("A-tier", len(a_alerts), "#1565c0"),
        ("B-Watch", len(b_alerts), "#e65100"),
        ("C-tier", len(c_alerts), "#757575"),
        ("Approaching", len(b_approaching), "#ad1457"),
    ]:
        parts.append(
            f'<div class="scard"><div class="n" style="color:{color}">{count}</div>'
            f'<div style="font-size:.72rem;color:#666;text-transform:uppercase">{e(label)}</div></div>'
        )
    parts.append("</div>")

    # Budget summary
    parts.append("<h3 style='font-size:.95rem;margin:14px 0 8px'>Weekly Budget</h3>")
    for cap_type, used, max_calls, pct, cost in budget_rows:
        fill_pct = max(1, int(pct * 100))
        label = cap_type.replace("weekly_", "").replace("_", " ")
        parts.append(
            f'<div class="bar-row"><span class="bar-label">{e(label)}</span>'
            f'<div class="bar-track"><div class="bar-fill2" style="width:{fill_pct}%"></div></div>'
            f'<span style="width:70px;font-size:.78rem;color:#555">{used}/{max_calls} (~${cost:.3f})</span></div>'
        )
    parts.append(f'<div style="font-size:.8rem;color:#555;margin-top:6px">Total this week: ~${total_cost:.3f}</div>')
    parts.append("</section>")

    # ── S-Tier Spotlight ──────────────────────────────────────────────────────
    if s_alerts:
        parts.append('<section><h2 style="color:#7b1fa2">S-Tier Spotlight: Wonderful Businesses at Price</h2>')
        for alert in s_alerts:
            parts.append(_html_s_card(alert, analysis_by_ticker, universe_by_ticker, position_by_ticker, e))
        parts.append("</section>")

    # ── A-Tier Action List ─────────────────────────────────────────────────────
    if a_alerts:
        parts.append('<section><h2 style="color:#1565c0">A-Tier Action List: Good Businesses at Price</h2>')
        for alert in a_alerts:
            parts.append(_html_a_card(alert, analysis_by_ticker, universe_by_ticker, position_by_ticker, e))
        parts.append("</section>")

    # ── B-Tier Approaching Target ─────────────────────────────────────────────
    parts.append("<section>")
    if b_approaching:
        parts.append('<h2 style="color:#ad1457">B-Tier Approaching Target (sorted by gap)</h2>')
        parts.append(
            '<p style="font-size:.85rem;color:#666;margin-bottom:10px">'
            "These B-tier stocks are within 10% of their target entry price.</p>"
        )
        parts.append(
            "<table><tr><th>Ticker</th><th>Company</th><th>Price</th><th>Target</th><th>Gap</th><th>Moat</th></tr>"
        )
        for a in b_approaching:
            ticker = a["ticker"]
            u = universe_by_ticker.get(ticker, {})
            da = analysis_by_ticker.get(ticker, {})
            name = (u.get("company_name") or ticker)[:35]
            price = a.get("last_price") or 0
            target = a.get("target_entry") or 0
            gap = a.get("gap_pct") or 0
            moat = da.get("moat_rating") or "—"
            star = " ★" if gap <= 0.05 else ""
            gap_color = 'style="color:#c62828"' if gap > 0 else 'style="color:#2e7d32"'
            parts.append(
                f"<tr><td><strong>{e(ticker)}</strong>{e(star)}</td>"
                f"<td>{e(name)}</td>"
                f"<td>${price:,.2f}</td>"
                f"<td>${target:,.2f}</td>"
                f"<td {gap_color}>{gap:+.1%}</td>"
                f"<td>{e(moat)}</td></tr>"
            )
        parts.append("</table>")
    else:
        parts.append("<h2>B-Tier Approaching Target</h2>")
        parts.append('<p style="font-size:.85rem;color:#888">No B-tier stocks within 10% of target entry.</p>')
    parts.append("</section>")

    # ── Quality Score Leaderboard ─────────────────────────────────────────────
    if top10:
        parts.append("<section><h2>Top 10 by Quality Score (regardless of price)</h2>")
        parts.append(
            "<table><tr><th>#</th><th>Ticker</th><th>Company</th><th>Score</th><th>Tier</th><th>Gap to Target</th></tr>"
        )
        for rank, u in enumerate(top10, 1):
            ticker = u["ticker"]
            alert = next((a for a in all_alerts if a["ticker"] == ticker), {})
            tier = alert["tier"] if alert else "—"
            gap = alert.get("gap_pct") if alert else None
            gap_str = f"{gap:+.1%}" if gap is not None else "—"
            gap_color = 'style="color:#c62828"' if (gap or 0) > 0 else 'style="color:#2e7d32"'
            name = (u.get("company_name") or ticker)[:35]
            score = u["quality_score"]
            tier_color = {"S": "#7b1fa2", "A": "#1565c0", "B": "#e65100", "C": "#757575"}.get(tier, "#555")
            tier_html = (
                f'<span class="tier-badge" style="background:{tier_color}">{e(tier)}</span>' if tier != "—" else "—"
            )
            parts.append(
                f"<tr><td>{rank}</td><td><strong>{e(ticker)}</strong></td>"
                f"<td>{e(name)}</td>"
                f"<td>{score:.1f}</td>"
                f"<td>{tier_html}</td>"
                f"<td {gap_color}>{e(gap_str)}</td></tr>"
            )
        parts.append("</table></section>")

    # ── News Events Digest ────────────────────────────────────────────────────
    parts.append("<section>")
    parts.append(f"<h2>News Events Digest (last {days_back} days)</h2>")
    if news_items:
        for item in news_items:
            ts = (item.get("detected_at") or "")[:16]
            ticker = item.get("ticker", "?")
            headline = item.get("headline") or ""
            material = bool(item.get("haiku_material"))
            sonnet = bool(item.get("sonnet_triggered"))
            event_t = item.get("event_type") or ""
            parts.append('<div class="news-item">')
            parts.append(f'<div><strong>{e(ticker)}</strong> <span style="color:#888;font-size:.8rem">{e(ts)}</span>')
            if event_t:
                parts.append(f' &nbsp;<span class="chip chip-ignored">{e(event_t)}</span>')
            if material:
                parts.append(' <span class="chip chip-material">MATERIAL</span>')
            if sonnet:
                parts.append(' <span class="chip chip-sonnet">SONNET</span>')
            parts.append("</div>")
            parts.append(f'<div style="color:#444">{e(headline[:120])}</div>')
            if item.get("summary"):
                parts.append(f'<div style="color:#666;font-size:.82rem">{e((item["summary"])[:100])}</div>')
            parts.append("</div>")
    else:
        parts.append(f'<p style="color:#888;font-size:.85rem">No news events in the last {days_back} days.</p>')
    parts.append("</section>")

    # ── Coverage Dashboard ────────────────────────────────────────────────────
    parts.append("<section><h2>Coverage Dashboard</h2>")
    analyzed_pct = len(analyzed_tickers) / max(total_universe, 1)
    bar_width = max(1, int(analyzed_pct * 100))
    parts.append(
        f'<div style="margin-bottom:12px">'
        f'<div style="font-size:.9rem;margin-bottom:4px">Universe: <strong>{total_universe}</strong> stocks &nbsp;|&nbsp; '
        f"Analyzed: <strong>{len(analyzed_tickers)}</strong> ({analyzed_pct:.1%})</div>"
        f'<div style="background:#e8eaf6;border-radius:4px;height:18px;overflow:hidden">'
        f'<div style="background:#3f51b5;height:100%;width:{bar_width}%"></div></div>'
        f"</div>"
    )

    # By sector
    sector_total: dict[str, int] = {}
    sector_analyzed: dict[str, int] = {}
    for u in universe:
        s = u.get("sector") or "Unknown"
        sector_total[s] = sector_total.get(s, 0) + 1
        if u["ticker"] in analyzed_tickers:
            sector_analyzed[s] = sector_analyzed.get(s, 0) + 1

    if sector_total:
        parts.append('<h3 style="font-size:.9rem;margin:10px 0 6px">By Sector</h3>')
        parts.append(
            '<div class="bar-row" style="font-weight:600;font-size:.78rem">'
            '<span class="bar-label">Sector</span><span>Coverage</span>'
            '<span style="width:40px;margin-left:8px">%</span></div>'
        )
        for sector, total in sorted(sector_total.items(), key=lambda x: x[1], reverse=True)[:12]:
            analyzed = sector_analyzed.get(sector, 0)
            pct = analyzed / total
            fill_pct = max(1, int(pct * 100))
            parts.append(
                f'<div class="bar-row"><span class="bar-label">{e(sector[:22])}</span>'
                f'<div class="bar-track"><div class="bar-fill2" style="width:{fill_pct}%"></div></div>'
                f'<span class="bar-pct">{pct:.0%}</span>'
                f'<span style="font-size:.75rem;color:#888;margin-left:6px">{analyzed}/{total}</span></div>'
            )

    # By cap
    cap_total: dict[str, int] = {}
    cap_analyzed: dict[str, int] = {}
    for u in universe:
        cap = u.get("cap_category") or "unknown"
        cap_total[cap] = cap_total.get(cap, 0) + 1
        if u["ticker"] in analyzed_tickers:
            cap_analyzed[cap] = cap_analyzed.get(cap, 0) + 1

    if cap_total:
        parts.append('<h3 style="font-size:.9rem;margin:14px 0 6px">By Cap Size</h3>')
        parts.append("<table style='width:auto'><tr><th>Cap</th><th>Total</th><th>Analyzed</th><th>Cov%</th></tr>")
        for cap_label, cap_key in [("Large ($10B+)", "large"), ("Mid ($1-10B)", "mid"), ("Small (<$1B)", "small")]:
            total = cap_total.get(cap_key, 0)
            analyzed = cap_analyzed.get(cap_key, 0)
            pct = analyzed / total if total else 0
            parts.append(f"<tr><td>{e(cap_label)}</td><td>{total}</td><td>{analyzed}</td><td>{pct:.0%}</td></tr>")
        parts.append("</table>")
    parts.append("</section>")

    # ── Paper Trading Scoreboard ──────────────────────────────────────────────
    parts.append("<section><h2>Paper Trading Scoreboard</h2>")
    if positions:
        total_cost_basis = sum(p.get("cost_basis") or 0 for p in positions)
        total_value = sum(p.get("current_value") or 0 for p in positions)
        total_pl = total_value - total_cost_basis
        total_pl_pct = total_pl / total_cost_basis if total_cost_basis else 0
        pl_color = "#2e7d32" if total_pl >= 0 else "#c62828"
        pl_sign = "+" if total_pl >= 0 else ""

        parts.append(
            f'<div style="display:flex;gap:20px;flex-wrap:wrap;margin-bottom:12px;font-size:.9rem">'
            f'<div><div style="font-size:1.2rem;font-weight:700">${total_cost_basis:,.0f}</div>'
            f'<div style="color:#888;font-size:.75rem">COST BASIS</div></div>'
            f'<div><div style="font-size:1.2rem;font-weight:700">${total_value:,.0f}</div>'
            f'<div style="color:#888;font-size:.75rem">CURRENT VALUE</div></div>'
            f'<div><div style="font-size:1.2rem;font-weight:700;color:{pl_color}">'
            f"{pl_sign}${abs(total_pl):,.0f} ({pl_sign}{abs(total_pl_pct):.1%})</div>"
            f'<div style="color:#888;font-size:.75rem">TOTAL P&amp;L</div></div>'
            f"</div>"
        )
        parts.append(
            "<table><tr><th>Ticker</th><th>Tier@Entry</th><th>Stage</th>"
            "<th>Entry</th><th>Current</th><th>P&amp;L</th><th>P&amp;L%</th></tr>"
        )
        for p in sorted(positions, key=lambda x: -(x.get("current_value") or 0)):
            ticker = p["ticker"]
            tier = p.get("tier_at_entry", "?")
            stage = p.get("entry_stage", "?") or "?"
            entry = p.get("entry_price") or 0
            current = p.get("current_price") or 0
            basis = p.get("cost_basis") or 0
            value = p.get("current_value") or 0
            pl = value - basis
            pl_pct = pl / basis if basis else 0
            pl_col = "#2e7d32" if pl >= 0 else "#c62828"
            pl_s = "+" if pl >= 0 else ""
            tier_color = {"S": "#7b1fa2", "A": "#1565c0"}.get(tier, "#555")
            parts.append(
                f"<tr><td><strong>{e(ticker)}</strong></td>"
                f'<td><span class="tier-badge" style="background:{tier_color}">{e(tier)}</span></td>'
                f"<td>{e(stage)}</td>"
                f"<td>${entry:,.2f}</td>"
                f"<td>${current:,.2f}</td>"
                f"<td style='color:{pl_col}'>{pl_s}${abs(pl):,.0f}</td>"
                f"<td style='color:{pl_col}'>{pl_s}{abs(pl_pct):.1%}</td></tr>"
            )
        parts.append("</table>")
        parts.append(
            '<p style="font-size:.78rem;color:#888;margin-top:8px">'
            "Benchmark (S&P 500) comparison auto-updates on Monday maintenance.</p>"
        )
    else:
        parts.append('<p style="color:#888;font-size:.85rem">No paper positions open.</p>')
    parts.append("</section>")

    # ── B-Tier Watch ──────────────────────────────────────────────────────────
    if b_watch:
        parts.append("<section><h2>B-Tier Watch</h2>")
        parts.append(
            '<p style="font-size:.85rem;color:#888;margin-bottom:10px">'
            "Quality businesses — price not yet approaching target.</p>"
        )
        parts.append(
            "<table><tr><th>Ticker</th><th>Company</th><th>Price</th><th>Target</th><th>Gap</th><th>Moat</th></tr>"
        )
        for a in b_watch:
            ticker = a["ticker"]
            u = universe_by_ticker.get(ticker, {})
            da = analysis_by_ticker.get(ticker, {})
            name = (u.get("company_name") or ticker)[:35]
            price = a.get("last_price") or 0
            target = a.get("target_entry") or 0
            gap = a.get("gap_pct") or 0
            moat = da.get("moat_rating") or "—"
            parts.append(
                f"<tr><td><strong>{e(ticker)}</strong></td>"
                f"<td>{e(name)}</td>"
                f"<td>${price:,.2f}</td>"
                f"<td>${target:,.2f}</td>"
                f'<td style="color:#c62828">{gap:+.1%}</td>'
                f"<td>{e(moat)}</td></tr>"
            )
        parts.append("</table></section>")

    # ── C-Tier ────────────────────────────────────────────────────────────────
    if c_alerts:
        parts.append("<section><h2>C-Tier Monitor</h2>")
        parts.append('<p style="font-size:.85rem;color:#888;margin-bottom:10px">Re-evaluate next scheduled cycle.</p>')
        parts.append('<div style="display:flex;flex-wrap:wrap;gap:6px">')
        for a in c_alerts:
            parts.append(f'<span class="tier-badge c">{e(a["ticker"])}</span>')
        parts.append("</div></section>")

    # ── Footer ────────────────────────────────────────────────────────────────
    parts.append(f"""<section style="background:#fafafa;font-size:.82rem;color:#888">
<p><strong>Tier guide:</strong>
  <span class="tier-badge s">S</span> Wonderful business at/below fair value &rarr; Buy (3 tranches) &nbsp;
  <span class="tier-badge a">A</span> Good business at/below target &rarr; Buy (2 tranches) &nbsp;
  <span class="tier-badge b">B</span> Watch &nbsp;
  <span class="tier-badge c">C</span> Monitor passively
</p>
<p style="margin-top:8px">For research purposes only. You make the final decision.</p>
</section>
<footer>Buffett Bot &middot; {e(month_str)}</footer>
</div></body></html>""")

    return "\n".join(parts)


def _html_s_card(
    alert: dict,
    analysis_map: dict,
    universe_map: dict,
    position_map: dict,
    e,  # html.escape
) -> str:
    ticker = alert["ticker"]
    da = analysis_map.get(ticker, {})
    u = universe_map.get(ticker, {})
    pos = position_map.get(ticker)
    name = da.get("company_name") or u.get("company_name") or ticker

    target = alert.get("target_entry")
    last_price = alert.get("last_price") or 0
    gap = alert.get("gap_pct")
    conviction = da.get("conviction", "N/A") if da else "N/A"
    moat = da.get("moat_rating", "N/A") if da else "N/A"
    fair_value = da.get("fair_value") if da else None
    gap_color = "color:#2e7d32" if (gap or 0) <= 0 else "color:#c62828"

    lines = ['<div class="card s-tier">']
    lines.append(f"<h3>{e(ticker)}: {e(name)}</h3>")
    lines.append(
        f'<span class="tier-badge s">S-TIER</span>&nbsp;'
        f'<span style="font-size:.82rem;color:#666">Moat: {e(moat)} | Conviction: {e(conviction)}</span>'
    )

    # Staged entries
    staged = alert.get("staged_entries") or {}
    if staged:
        entry_strs = []
        if isinstance(staged, list):
            for entry in staged:
                entry_strs.append(f"{e(entry.get('label', '?'))}: ${entry.get('price', 0):,.2f}")
        elif isinstance(staged, dict):
            for label, price in staged.items():
                entry_strs.append(f"{e(str(label))}: ${price:,.2f}")
        if entry_strs:
            lines.append(
                '<div class="staged"><strong>Staged Entry:</strong> '
                + "&nbsp;&nbsp;|&nbsp;&nbsp;".join(entry_strs)
                + "</div>"
            )

    # Price table
    lines.append("<table>")
    lines.append(f"<tr><td>Current Price</td><td>${last_price:,.2f}</td></tr>")
    if target:
        lines.append(f"<tr><td>Target Entry</td><td>${target:,.2f}</td></tr>")
    if gap is not None:
        lines.append(f"<tr><td>Gap to Target</td><td style='{gap_color}'>{gap:+.1%}</td></tr>")
    if fair_value:
        lines.append(f"<tr><td>Fair Value</td><td>${fair_value:,.2f}</td></tr>")
    lines.append("</table>")

    thesis = (da.get("investment_thesis") or "") if da else ""
    if thesis:
        lines.append(
            f"<details open><summary>Investment Thesis</summary>"
            f'<div class="thesis-box">{e(thesis[:500])}</div></details>'
        )

    risks = (da.get("key_risks") or []) if da else []
    if risks:
        lines.append("<details><summary>Key Risks</summary><ul style='font-size:.85rem;margin:6px 0 0 18px'>")
        for r in risks[:3]:
            lines.append(f"<li>{e(r[:100])}</li>")
        lines.append("</ul></details>")

    breakers = (da.get("thesis_breakers") or []) if da else []
    if breakers:
        lines.append(
            "<details><summary>Thesis Breakers (sell triggers)</summary>"
            "<ul style='font-size:.85rem;margin:6px 0 0 18px;color:#c62828'>"
        )
        for b in breakers[:2]:
            lines.append(f"<li>{e(b[:100])}</li>")
        lines.append("</ul></details>")

    if pos:
        basis = pos.get("cost_basis") or 0
        value = pos.get("current_value") or 0
        pl = value - basis
        pl_col = "#2e7d32" if pl >= 0 else "#c62828"
        pl_s = "+" if pl >= 0 else ""
        stage = pos.get("entry_stage", "?")
        lines.append(
            f'<div style="background:#f3e5f5;border-radius:4px;padding:8px 10px;margin-top:10px;font-size:.85rem">'
            f"<strong>Paper position:</strong> Stage {e(stage)} &nbsp;|&nbsp; "
            f'P&amp;L: <span style="color:{pl_col}">{pl_s}${abs(pl):,.0f}</span></div>'
        )

    lines.append("</div>")
    return "\n".join(lines)


def _html_a_card(
    alert: dict,
    analysis_map: dict,
    universe_map: dict,
    position_map: dict,
    e,
) -> str:
    ticker = alert["ticker"]
    da = analysis_map.get(ticker, {})
    u = universe_map.get(ticker, {})
    pos = position_map.get(ticker)
    name = da.get("company_name") or u.get("company_name") or ticker
    target = alert.get("target_entry")
    last_price = alert.get("last_price") or 0
    gap = alert.get("gap_pct")
    conviction = da.get("conviction", "N/A") if da else "N/A"
    moat = da.get("moat_rating", "N/A") if da else "N/A"
    gap_color = "color:#2e7d32" if (gap or 0) <= 0 else "color:#c62828"

    staged = alert.get("staged_entries") or {}
    entry_strs = []
    if isinstance(staged, list):
        for entry in staged:
            entry_strs.append(f"{e(entry.get('label', '?'))}: ${entry.get('price', 0):,.2f}")
    elif isinstance(staged, dict):
        for label, price in staged.items():
            entry_strs.append(f"{e(str(label))}: ${price:,.2f}")

    lines = ['<div class="card a-tier">']
    lines.append(f"<h3>{e(ticker)}: {e(name)}</h3>")
    lines.append(
        f'<span class="tier-badge a">A-TIER</span>&nbsp;'
        f'<span style="font-size:.82rem;color:#666">Moat: {e(moat)} | Conviction: {e(conviction)}</span>'
    )

    if entry_strs:
        lines.append(
            '<div class="staged"><strong>Entry Plan:</strong> '
            + "&nbsp;&nbsp;|&nbsp;&nbsp;".join(entry_strs)
            + "</div>"
        )

    lines.append("<table>")
    lines.append(f"<tr><td>Current Price</td><td>${last_price:,.2f}</td></tr>")
    if target:
        lines.append(f"<tr><td>Target Entry</td><td>${target:,.2f}</td></tr>")
    if gap is not None:
        lines.append(f"<tr><td>Gap to Target</td><td style='{gap_color}'>{gap:+.1%}</td></tr>")
    lines.append("</table>")

    thesis = (da.get("investment_thesis") or "") if da else ""
    if thesis:
        lines.append(
            f'<details><summary>Investment Thesis</summary><div class="thesis-box">{e(thesis[:400])}</div></details>'
        )

    if pos:
        basis = pos.get("cost_basis") or 0
        value = pos.get("current_value") or 0
        pl = value - basis
        pl_col = "#2e7d32" if pl >= 0 else "#c62828"
        pl_s = "+" if pl >= 0 else ""
        stage = pos.get("entry_stage", "?")
        lines.append(
            f'<div style="background:#e3f2fd;border-radius:4px;padding:8px 10px;margin-top:10px;font-size:.85rem">'
            f"<strong>Paper position:</strong> Stage {e(stage)} &nbsp;|&nbsp; "
            f'P&amp;L: <span style="color:{pl_col}">{pl_s}${abs(pl):,.0f}</span></div>'
        )

    lines.append("</div>")
    return "\n".join(lines)


# ─── Public API ───────────────────────────────────────────────────────────────


def generate_briefing_from_db(
    db: "Database",
    *,
    output_dir: Optional[str] = None,
    days_back: int = 7,
) -> tuple[str, str]:
    """
    Generate a complete briefing from the SQLite database.

    Args:
        db:         Database instance (read-only access pattern).
        output_dir: If provided, saves briefing_{YYYY_MM}.txt and .html there.
        days_back:  How far back to look for news events (default 7 days).

    Returns:
        (text_str, html_str) — both as in-memory strings.
    """
    text = _text_briefing(db, days_back=days_back)
    html = _html_briefing(db, days_back=days_back)

    if output_dir:
        now = datetime.now()
        stamp = now.strftime("%Y_%m")
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        (out / f"briefing_{stamp}.txt").write_text(text, encoding="utf-8")
        (out / f"briefing_{stamp}.html").write_text(html, encoding="utf-8")

    return text, html
