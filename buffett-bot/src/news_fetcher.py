"""
News Fetcher Module — Phase E

Finnhub daily news pipeline for S/A/B tier stocks.

Pipeline:
1. Fetch company news from Finnhub (free tier: 60 req/min)
2. apply_keyword_filter() — free pre-filter to drop noise
3. Haiku materiality check via analyzer.check_news_for_red_flags()
4. Sonnet re-analysis if Haiku flags concern (budget cap: 10/week)
5. Log to news_events table; notify on tier changes

API keys stay in their modules (FINNHUB_API_KEY env var).
"""

import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import requests

from .tier_engine import assign_tier, staged_entry_suggestion

logger = logging.getLogger(__name__)

# ─── Material event keywords ─────────────────────────────────────────────────
# Free pre-gate: any news item whose headline or summary contains one of these
# words is passed to Haiku for a deeper materiality assessment.

MATERIAL_KEYWORDS: frozenset[str] = frozenset(
    {
        # Earnings & guidance
        "earnings",
        "eps",
        "revenue",
        "guidance",
        "outlook",
        "forecast",
        "beat",
        "miss",
        "raised",
        "lowered",
        "withdrawn",
        # Management
        "ceo",
        "cfo",
        "coo",
        "president",
        "chairman",
        "resign",
        "retire",
        "appoint",
        "named",
        "fired",
        "depart",
        # M&A / corporate events
        "acqui",
        "merger",
        "takeover",
        "buyout",
        "spin-off",
        "spinoff",
        "divest",
        "divestiture",
        "joint venture",
        # Legal / regulatory
        "sec ",
        "lawsuit",
        "litigation",
        "settlement",
        "indictment",
        "fraud",
        "investigation",
        "regulatory",
        "fda",
        "ftc",
        "doj",
        # Financial distress
        "bankruptcy",
        "chapter 11",
        "default",
        "downgrade",
        "credit rating",
        "insolvency",
        "restructuring",
        # Shareholder events
        "dividend",
        "buyback",
        "repurchase",
        "activist",
        "proxy",
        "special meeting",
        # Major operational events
        "recall",
        "plant closure",
        "layoff",
        "write-down",
        "write down",
        "impairment",
        "restatement",
    }
)

# ─── Event type inference ─────────────────────────────────────────────────────
# Each entry: (event_type_label, [substrings to match in lowercase headline])
_EVENT_TYPE_PATTERNS: list[tuple[str, list[str]]] = [
    ("earnings", ["earnings", " eps", "quarterly results", "annual results"]),
    ("guidance", ["guidance", "outlook", "forecast", "raised guidance", "lowered guidance"]),
    ("ceo_change", ["ceo", "chief executive", "president", "chairman"]),
    ("mgmt_change", ["cfo", "coo", "chief financial", "chief operating"]),
    ("acquisition", ["acqui", "merger", "takeover", "buyout", "joint venture"]),
    ("divestiture", ["divest", "spin-off", "spinoff"]),
    ("legal", ["lawsuit", "litigation", "settlement", "indictment", "fraud", "investigation"]),
    ("regulatory", ["sec ", "fda", "ftc", "doj", "regulatory"]),
    ("bankruptcy", ["bankruptcy", "chapter 11", "insolvency"]),
    ("dividend", ["dividend", "distribution"]),
    ("buyback", ["buyback", "repurchase", "share repurchase"]),
    ("activist", ["activist", "proxy", "board seat"]),
    ("restructure", ["restructur", "layoff", "plant closure", "workforce reduction"]),
    ("write_down", ["write-down", "write down", "impairment", "restatement"]),
    ("downgrade", ["downgrade", "credit rating"]),
]


def _infer_event_type(headline: str) -> Optional[str]:
    """Classify a headline into a canonical event type, or None if unrecognised."""
    lower = headline.lower()
    for event_type, keywords in _EVENT_TYPE_PATTERNS:
        if any(kw in lower for kw in keywords):
            return event_type
    return None


def apply_keyword_filter(news_items: list[dict]) -> list[dict]:
    """
    Free pre-filter: keep only items whose headline or summary contains
    at least one MATERIAL_KEYWORDS term.

    Each kept item is annotated with an 'event_type' key (may be None).
    Returns a subset of news_items in original order.
    """
    result = []
    for item in news_items:
        text = (item.get("headline", "") + " " + item.get("summary", "")).lower()
        if any(kw in text for kw in MATERIAL_KEYWORDS):
            annotated = dict(item)
            annotated["event_type"] = _infer_event_type(item.get("headline", ""))
            result.append(annotated)
    return result


def format_news_for_llm(news_items: list[dict], max_items: int = 10) -> str:
    """
    Format a list of (keyword-filtered) news items into a text block
    suitable for inclusion in an LLM prompt.

    Truncates to max_items most recent items, and caps each summary at 200 chars.
    Returns "(no news)" if the input list is empty.
    """
    items = news_items[:max_items]
    if not items:
        return "(no news)"

    lines = []
    for item in items:
        ts = item.get("datetime", "")
        if isinstance(ts, (int, float)):
            ts = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")

        headline = item.get("headline", "")
        source = item.get("source", "")
        summary = (item.get("summary", "") or "")[:200]

        line = f"[{ts}] {headline}"
        if source:
            line += f" ({source})"
        if summary:
            line += f"\n  {summary}"
        lines.append(line)

    return "\n\n".join(lines)


def _build_news_context(ticker: str, db) -> str:
    """
    Build a company context string for news-triggered Sonnet re-analysis.

    Includes DB financial metrics + the existing investment thesis so the
    re-analysis can weigh the news against prior convictions.
    """
    u = db.get_universe_stock(ticker)
    f = db.get_latest_fundamentals(ticker)
    da = db.get_latest_deep_analysis(ticker)

    lines = [f"TICKER: {ticker}"]

    if u:
        if u.get("company_name"):
            lines.append(f"NAME: {u['company_name']}")
        if u.get("sector"):
            lines.append(f"SECTOR: {u['sector']}")
        mc = u.get("market_cap")
        if mc:
            lines.append(f"MARKET CAP: ${mc / 1e9:.1f}B")

    if f:
        lines.append("")
        lines.append("=== KEY FINANCIAL METRICS ===")
        if f.get("price"):
            lines.append(f"Current Price: ${f['price']:.2f}")
        if f.get("pe_ratio"):
            lines.append(f"P/E Ratio: {f['pe_ratio']:.1f}x")
        if f.get("roe") is not None:
            lines.append(f"ROE: {f['roe']:.1%}")
        if f.get("roic") is not None:
            lines.append(f"ROIC: {f['roic']:.1%}")
        if f.get("operating_margin") is not None:
            lines.append(f"Operating Margin: {f['operating_margin']:.1%}")
        if f.get("fcf_yield") is not None:
            lines.append(f"FCF Yield: {f['fcf_yield']:.1%}")
        if f.get("debt_equity") is not None:
            lines.append(f"Debt/Equity: {f['debt_equity']:.2f}x")
        if f.get("revenue_growth") is not None:
            lines.append(f"Revenue Growth (YoY): {f['revenue_growth']:.1%}")

    if da:
        lines.append("")
        lines.append("=== PRIOR INVESTMENT THESIS ===")
        if da.get("investment_thesis"):
            lines.append(da["investment_thesis"][:500])
        risks = da.get("key_risks") or []
        if risks:
            lines.append("")
            lines.append("Key Risks:")
            for risk in risks[:3]:
                lines.append(f"  - {risk}")

    return "\n".join(lines)


def _ts_to_iso(ts) -> Optional[str]:
    """Convert a Unix timestamp (int/float) or ISO string to an ISO datetime string."""
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    return str(ts)


# ─── Finnhub fetcher ──────────────────────────────────────────────────────────

_FINNHUB_BASE = "https://finnhub.io/api/v1"


class FinnhubNewsFetcher:
    """
    Fetches company news from Finnhub.

    Free tier: 60 requests/minute.  Default rate limit is 55 req/min to
    leave headroom for other Finnhub uses.  One API call = one (symbol, date-range) pair.
    """

    def __init__(self, api_key: Optional[str] = None, calls_per_minute: int = 55):
        self.api_key = api_key or os.getenv("FINNHUB_API_KEY", "")
        self._min_interval = 60.0 / max(calls_per_minute, 1)
        self._last_call: float = 0.0

    def _throttle(self) -> None:
        """Sleep if necessary to respect the per-minute rate limit."""
        elapsed = time.monotonic() - self._last_call
        wait = self._min_interval - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def get_company_news(self, symbol: str, from_date: str, to_date: str) -> list[dict]:
        """
        Fetch company news from Finnhub for a single symbol and date range.

        Args:
            symbol:    Ticker symbol (e.g. "AAPL")
            from_date: ISO date "YYYY-MM-DD" (inclusive)
            to_date:   ISO date "YYYY-MM-DD" (inclusive)

        Returns:
            List of news dicts [{headline, summary, source, datetime, url, ...}]
            Empty list on error or missing API key.
        """
        if not self.api_key:
            logger.warning("FINNHUB_API_KEY not set — skipping news fetch for %s", symbol)
            return []

        self._throttle()
        try:
            resp = requests.get(
                f"{_FINNHUB_BASE}/company-news",
                params={
                    "symbol": symbol,
                    "from": from_date,
                    "to": to_date,
                    "token": self.api_key,
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []
        except Exception as exc:
            logger.warning("Finnhub fetch failed for %s: %s", symbol, exc)
            return []

    def get_news_for_tickers(
        self,
        tickers: list[str],
        days_back: int = 1,
    ) -> dict[str, list[dict]]:
        """
        Fetch news for multiple tickers with automatic rate limiting.

        Args:
            tickers:   List of ticker symbols
            days_back: Number of calendar days to look back

        Returns:
            Dict mapping ticker → list of raw news items
        """
        if not tickers:
            return {}

        today = date.today()
        from_date = (today - timedelta(days=days_back)).isoformat()
        to_date = today.isoformat()

        result: dict[str, list[dict]] = {}
        for symbol in tickers:
            result[symbol] = self.get_company_news(symbol, from_date, to_date)

        return result


# ─── Pipeline ─────────────────────────────────────────────────────────────────


def run_news_pipeline(
    db,
    analyzer,
    fetcher: FinnhubNewsFetcher,
    *,
    notifier=None,
    days_back: int = 1,
    dry_run: bool = False,
) -> dict:
    """
    Daily news pipeline: Finnhub fetch → keyword filter → Haiku → Sonnet.

    Stages:
    1. Fetch news for all S/A/B tier tickers (from price_alerts table)
    2. Apply MATERIAL_KEYWORDS filter (free — no LLM calls)
    3. Haiku materiality check for each ticker with material news
       — uses weekly_news_haiku budget cap (50/week, ~$0.05)
    4. Sonnet re-analysis for tickers Haiku flags as REVIEW or SELL
       — uses weekly_news_sonnet budget cap (10/week, ~$0.25)
       — bypasses file cache (use_cache=False) for fresh results
    5. Log news_events; update DB tier + price_alert; notify if tier changed

    Args:
        db:       Database instance
        analyzer: CompanyAnalyzer instance
        fetcher:  FinnhubNewsFetcher instance
        notifier: Optional NotificationManager (None = skip notifications)
        days_back: Days of news history to fetch (default: 1)
        dry_run:  If True, skip Finnhub fetch and all LLM calls (smoke-test mode)

    Returns:
        Stats dict: {tickers_checked, news_found, haiku_calls, sonnet_calls, tier_changes}
    """
    stats = {
        "tickers_checked": 0,
        "news_found": 0,
        "haiku_calls": 0,
        "sonnet_calls": 0,
        "tier_changes": 0,
    }

    # 1. Discover S/A/B watched stocks from price_alerts
    alerts = db.get_price_alerts(tiers=["S", "A", "B"])
    tickers = [a["ticker"] for a in alerts]

    if not tickers:
        logger.info("No S/A/B tier tickers to monitor — news pipeline skipped")
        return stats

    stats["tickers_checked"] = len(tickers)
    logger.info("News pipeline: monitoring %d S/A/B ticker(s)", len(tickers))

    if dry_run:
        logger.info("[dry_run] Skipping Finnhub fetch and LLM calls")
        return stats

    # 2. Fetch news from Finnhub
    news_by_ticker = fetcher.get_news_for_tickers(tickers, days_back=days_back)

    # 3–5. Process each ticker
    for ticker, raw_news in news_by_ticker.items():
        if not raw_news:
            continue

        material = apply_keyword_filter(raw_news)
        if not material:
            continue

        stats["news_found"] += 1
        logger.info("%s: %d raw → %d material news item(s)", ticker, len(raw_news), len(material))

        # Gather context for Haiku check
        da = db.get_latest_deep_analysis(ticker)
        thesis = (da or {}).get("investment_thesis", f"{ticker} is a watched investment")
        thesis_breakers = (da or {}).get("thesis_breakers") or []
        formatted_news = format_news_for_llm(material)

        # 3. Haiku materiality check
        if not db.can_spend("weekly_news_haiku"):
            logger.info("weekly_news_haiku budget exhausted — skipping Haiku check for %s", ticker)
            continue

        stats["haiku_calls"] += 1
        top_item = material[0]

        try:
            haiku_result = analyzer.check_news_for_red_flags(ticker, thesis, thesis_breakers, formatted_news)
        except Exception as exc:
            logger.warning("Haiku news check failed for %s: %s", ticker, exc)
            db.log_news_event(
                ticker,
                top_item.get("headline", ""),
                source=top_item.get("source"),
                published_at=_ts_to_iso(top_item.get("datetime")),
                event_type=top_item.get("event_type"),
                haiku_material=None,
            )
            continue

        has_red_flags = haiku_result.get("has_red_flags", False)
        recommendation = haiku_result.get("recommendation", "HOLD")

        # Log event to DB regardless of red-flag result
        db.log_news_event(
            ticker,
            top_item.get("headline", ""),
            source=top_item.get("source"),
            published_at=_ts_to_iso(top_item.get("datetime")),
            event_type=top_item.get("event_type"),
            haiku_material=has_red_flags,
            summary=(haiku_result.get("analysis", "") or "")[:500],
        )

        if not has_red_flags and recommendation == "HOLD":
            logger.info("%s: Haiku — no red flags, HOLD", ticker)
            continue

        logger.info(
            "%s: Haiku flags red_flags=%s, recommendation=%s",
            ticker,
            has_red_flags,
            recommendation,
        )

        # 4. Sonnet re-analysis
        if not db.can_spend("weekly_news_sonnet"):
            logger.info("weekly_news_sonnet budget exhausted — skipping Sonnet for %s", ticker)
            continue

        u = db.get_universe_stock(ticker)
        company_name = (u or {}).get("company_name") or ticker
        context = _build_news_context(ticker, db)

        stats["sonnet_calls"] += 1
        try:
            analysis = analyzer.analyze_company(
                ticker,
                company_name,
                context,
                recent_news=formatted_news,
                use_cache=False,
            )
        except Exception as exc:
            logger.warning("Sonnet re-analysis failed for %s: %s", ticker, exc)
            continue

        # Persist new analysis and tier
        old_da = db.get_latest_deep_analysis(ticker)
        old_tier = (old_da or {}).get("tier")
        tier_assignment = assign_tier(analysis)

        fair_value_mid = (
            ((analysis.estimated_fair_value_low or 0) + (analysis.estimated_fair_value_high or 0)) / 2
        ) or None

        db.save_deep_analysis(
            ticker,
            tier=tier_assignment.tier,
            conviction=analysis.conviction,
            moat_rating=analysis.moat_rating.value.upper(),
            moat_sources=analysis.moat_sources,
            fair_value=fair_value_mid,
            target_entry=analysis.target_entry_price,
            investment_thesis=analysis.summary,
            key_risks=analysis.key_risks,
            thesis_breakers=analysis.thesis_risks,
        )

        db.log_tier_change(
            ticker,
            new_tier=tier_assignment.tier,
            old_tier=old_tier,
            trigger="news_event",
            reason=f"News-triggered re-analysis: {recommendation}",
        )

        # Always update price_alert regardless of tier.
        # For C-tier: updates the row to tier='C', excluding the stock from
        # the next day's get_price_alerts(tiers=["S","A","B"]) query.
        # Without this, a B→C downgrade leaves a stale B-tier alert and the
        # stock continues to consume Haiku budget on subsequent news cycles.
        entries = staged_entry_suggestion(analysis.target_entry_price or 0, tier_assignment.tier)
        db.upsert_price_alert(
            ticker,
            tier=tier_assignment.tier,
            target_entry=analysis.target_entry_price,
            staged_entries=entries or None,
            last_price=analysis.current_price,
            gap_pct=tier_assignment.price_gap_pct,
        )

        # 5. Notify on tier change
        if old_tier != tier_assignment.tier:
            stats["tier_changes"] += 1
            msg = f"Tier changed: {old_tier} → {tier_assignment.tier} (news-triggered, recommendation={recommendation})"
            logger.info("%s: %s", ticker, msg)
            if notifier:
                try:
                    notifier.send_alert(ticker, msg)
                except Exception as exc:
                    logger.warning("Notification failed for %s: %s", ticker, exc)

    logger.info(
        "News pipeline complete: %d ticker(s) checked, %d with material news, "
        "%d Haiku call(s), %d Sonnet call(s), %d tier change(s)",
        stats["tickers_checked"],
        stats["news_found"],
        stats["haiku_calls"],
        stats["sonnet_calls"],
        stats["tier_changes"],
    )
    return stats
