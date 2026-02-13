"""
Forward Validation Module

v2.0 — Validates whether quality screening correlates with actual returns.

Two modes:

1. **Retrospective quality-return correlation:**
   - Run the screener with current criteria
   - For each scored company, pull historical price data (3-5 years)
   - Check if higher quality scores correlate with stronger returns
   - Not causal proof, but validates signal quality

2. **Forward-looking tracking:**
   - Snapshot current watchlist with tier assignments
   - On subsequent runs, compare current prices to snapshot
   - Builds a real track record over time (6-12 month horizon)

Limitations:
- yfinance provides current financials, not point-in-time historical data
- This is NOT a true backtest — it's a quality-return correlation study
- A proper backtest requires a dedicated data provider (SimFin, Sharadar)

Data: yfinance history() for price data (reliable), current financials for scoring
Cache: data/backtest/

Run independently: python -m src.backtest
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import yfinance as yf

logger = logging.getLogger(__name__)

_backtest_dir = Path("data/backtest")


def set_backtest_dir(path: Path):
    """Override the backtest data directory."""
    global _backtest_dir
    _backtest_dir = path


# ─────────────────────────────────────────────────────────────
# 1. Retrospective Quality-Return Correlation
# ─────────────────────────────────────────────────────────────


def run_quality_return_correlation(
    scored_stocks: list[dict],
    lookback_years: int = 3,
) -> dict:
    """
    Check if quality scores correlate with historical returns.

    Args:
        scored_stocks: List of dicts with at least 'symbol' and 'score'.
                       Optionally include 'score_confidence', 'sector'.
        lookback_years: How many years of price history to check.

    Returns:
        Dict with correlation results, per-stock data, and summary.
    """
    results = []

    for stock in scored_stocks:
        symbol = stock.get("symbol")
        score = stock.get("score", 0)
        if not symbol:
            continue

        try:
            returns = _fetch_historical_returns(symbol, lookback_years)
            if returns is None:
                continue

            results.append(
                {
                    "symbol": symbol,
                    "score": score,
                    "score_confidence": stock.get("score_confidence", 0),
                    "sector": stock.get("sector", "Unknown"),
                    "return_1y": returns.get("return_1y"),
                    "return_3y": returns.get("return_3y"),
                    "return_5y": returns.get("return_5y"),
                    "max_drawdown": returns.get("max_drawdown"),
                    "volatility": returns.get("volatility"),
                }
            )
        except Exception as e:
            logger.debug(f"Error fetching returns for {symbol}: {e}")
            continue

    if not results:
        return {"error": "No valid results", "stocks": [], "summary": {}}

    # Compute correlation (simple rank correlation)
    correlation = _compute_rank_correlation(results)

    # Sort into quintiles by score
    quintile_analysis = _quintile_analysis(results)

    # Summary
    avg_return_top = _avg_return(results, top_pct=0.2)
    avg_return_bottom = _avg_return(results, top_pct=0.2, bottom=True)

    summary = {
        "total_stocks": len(results),
        "lookback_years": lookback_years,
        "rank_correlation_1y": correlation.get("return_1y"),
        "rank_correlation_3y": correlation.get("return_3y"),
        "avg_1y_return_top_20pct": avg_return_top,
        "avg_1y_return_bottom_20pct": avg_return_bottom,
        "quality_premium": (avg_return_top or 0) - (avg_return_bottom or 0),
        "quintiles": quintile_analysis,
        "generated_at": datetime.now().isoformat(),
    }

    # Save results
    _save_correlation_results(results, summary)

    return {"stocks": results, "summary": summary}


def _fetch_historical_returns(symbol: str, years: int = 3) -> Optional[dict]:
    """Fetch historical price returns for a symbol."""
    try:
        ticker = yf.Ticker(symbol)
        period = f"{years}y"
        hist = ticker.history(period=period)

        if hist.empty or len(hist) < 50:
            return None

        current = hist["Close"].iloc[-1]
        returns = {}

        # 1-year return
        if len(hist) >= 252:
            price_1y_ago = hist["Close"].iloc[-252]
            returns["return_1y"] = (current - price_1y_ago) / price_1y_ago
        elif len(hist) >= 200:
            price_start = hist["Close"].iloc[0]
            days = len(hist)
            total_return = (current - price_start) / price_start
            returns["return_1y"] = total_return * (252 / days)  # Annualize

        # 3-year return (annualized)
        if len(hist) >= 252 * 3:
            price_3y_ago = hist["Close"].iloc[-252 * 3]
            total_return = (current - price_3y_ago) / price_3y_ago
            returns["return_3y"] = (1 + total_return) ** (1 / 3) - 1

        # 5-year return (annualized) — if available
        if years >= 5:
            hist_5y = ticker.history(period="5y")
            if len(hist_5y) >= 252 * 4:
                price_5y_ago = hist_5y["Close"].iloc[0]
                total_return = (current - price_5y_ago) / price_5y_ago
                actual_years = len(hist_5y) / 252
                returns["return_5y"] = (1 + total_return) ** (1 / actual_years) - 1

        # Max drawdown (from rolling peak)
        rolling_max = hist["Close"].cummax()
        drawdown = (hist["Close"] - rolling_max) / rolling_max
        returns["max_drawdown"] = drawdown.min()

        # Annualized volatility
        daily_returns = hist["Close"].pct_change().dropna()
        returns["volatility"] = daily_returns.std() * (252**0.5)

        return returns

    except Exception as e:
        logger.debug(f"Error fetching history for {symbol}: {e}")
        return None


def _compute_rank_correlation(results: list[dict]) -> dict:
    """Compute Spearman rank correlation between score and returns."""
    correlations: dict[str, float | None] = {}

    for return_key in ["return_1y", "return_3y"]:
        valid = [(r["score"], r[return_key]) for r in results if r.get(return_key) is not None]
        if len(valid) < 10:
            correlations[return_key] = None
            continue

        # Rank-based correlation (Spearman)
        s_vals, r_vals = zip(*valid)
        s_ranks = _rank(list(s_vals))
        r_ranks = _rank(list(r_vals))

        n = len(s_ranks)
        d_sq_sum = sum((s - r) ** 2 for s, r in zip(s_ranks, r_ranks))
        rho = 1 - (6 * d_sq_sum) / (n * (n**2 - 1))
        correlations[return_key] = round(rho, 3)

    return correlations


def _rank(values: list[float]) -> list[float]:
    """Compute ranks for a list of values (average rank for ties)."""
    indexed = sorted(enumerate(values), key=lambda x: x[1])
    ranks = [0.0] * len(values)

    i = 0
    while i < len(indexed):
        j = i
        while j < len(indexed) and indexed[j][1] == indexed[i][1]:
            j += 1
        avg_rank = (i + j + 1) / 2  # 1-indexed average
        for k in range(i, j):
            ranks[indexed[k][0]] = avg_rank
        i = j

    return ranks


def _quintile_analysis(results: list[dict]) -> list[dict]:
    """Split results into quintiles by score and compute average returns."""
    sorted_results = sorted(results, key=lambda r: r["score"], reverse=True)
    n = len(sorted_results)
    quintile_size = max(1, n // 5)

    quintiles = []
    for q in range(5):
        start = q * quintile_size
        end = start + quintile_size if q < 4 else n
        group = sorted_results[start:end]

        returns_1y = [r["return_1y"] for r in group if r.get("return_1y") is not None]
        avg_score = sum(r["score"] for r in group) / len(group) if group else 0

        quintiles.append(
            {
                "quintile": q + 1,
                "label": ["Top 20%", "20-40%", "40-60%", "60-80%", "Bottom 20%"][q],
                "count": len(group),
                "avg_score": round(avg_score, 2),
                "avg_return_1y": round(sum(returns_1y) / len(returns_1y), 4) if returns_1y else None,
                "stocks": [r["symbol"] for r in group[:5]],
            }
        )

    return quintiles


def _avg_return(results: list[dict], top_pct: float = 0.2, bottom: bool = False) -> Optional[float]:
    """Average 1-year return for top or bottom N% by score."""
    sorted_results = sorted(results, key=lambda r: r["score"], reverse=not bottom)
    n = max(1, int(len(sorted_results) * top_pct))
    group = sorted_results[:n]
    returns = [r["return_1y"] for r in group if r.get("return_1y") is not None]
    return round(sum(returns) / len(returns), 4) if returns else None


def _save_correlation_results(results: list[dict], summary: dict):
    """Save correlation results to disk."""
    try:
        _backtest_dir.mkdir(parents=True, exist_ok=True)
        path = _backtest_dir / f"correlation_{datetime.now().strftime('%Y_%m')}.json"
        data = {"results": results, "summary": summary}
        path.write_text(json.dumps(data, indent=2, default=str))
        logger.info(f"Saved correlation results to {path}")
    except Exception as e:
        logger.warning(f"Failed to save correlation results: {e}")


# ─────────────────────────────────────────────────────────────
# 2. Forward-Looking Watchlist Tracking
# ─────────────────────────────────────────────────────────────


def save_watchlist_snapshot(
    tier_assignments: dict,
    scored_stocks: Optional[dict] = None,
) -> Path:
    """
    Save a dated snapshot of the current watchlist for future tracking.

    Args:
        tier_assignments: Dict of symbol -> TierAssignment
        scored_stocks: Optional dict of symbol -> screened stock data

    Returns:
        Path to the saved snapshot file.
    """
    _backtest_dir.mkdir(parents=True, exist_ok=True)

    stocks_data: dict[str, dict] = {}
    snapshot: dict[str, object] = {
        "snapshot_date": datetime.now().isoformat(),
        "stocks": stocks_data,
    }

    for symbol, tier in tier_assignments.items():
        stock_data = {
            "tier": tier.tier,
            "quality_level": tier.quality_level,
            "target_entry_price": tier.target_entry_price,
            "current_price": tier.current_price,
            "price_gap_pct": tier.price_gap_pct,
        }
        if scored_stocks and symbol in scored_stocks:
            sc = scored_stocks[symbol]
            if hasattr(sc, "score"):
                stock_data["score"] = sc.score
            if hasattr(sc, "score_confidence"):
                stock_data["score_confidence"] = sc.score_confidence
        stocks_data[symbol] = stock_data

    date_str = datetime.now().strftime("%Y_%m_%d")
    path = _backtest_dir / f"watchlist_snapshot_{date_str}.json"
    path.write_text(json.dumps(snapshot, indent=2))
    logger.info(f"Saved watchlist snapshot ({len(stocks_data)} stocks) to {path}")
    return path


def track_watchlist_performance() -> dict:
    """
    Compare current prices to historical watchlist snapshots.

    Finds all snapshots in data/backtest/ and checks how each pick performed.

    Returns:
        Dict with per-snapshot performance data.
    """
    _backtest_dir.mkdir(parents=True, exist_ok=True)

    snapshots = sorted(_backtest_dir.glob("watchlist_snapshot_*.json"))
    if not snapshots:
        return {"error": "No snapshots found", "snapshots": []}

    results = []

    for snapshot_path in snapshots:
        try:
            data = json.loads(snapshot_path.read_text())
            snapshot_date = data.get("snapshot_date", "")
            stocks = data.get("stocks", {})

            snapshot_result = {
                "snapshot_date": snapshot_date,
                "snapshot_file": snapshot_path.name,
                "stocks": [],
            }

            for symbol, stock_data in stocks.items():
                try:
                    ticker = yf.Ticker(symbol)
                    info = ticker.info
                    current_price = info.get("regularMarketPrice") or info.get("currentPrice")

                    if not current_price:
                        continue

                    snapshot_price = stock_data.get("current_price", 0)
                    if snapshot_price and snapshot_price > 0:
                        price_change = (current_price - snapshot_price) / snapshot_price
                    else:
                        price_change = None

                    snapshot_result["stocks"].append(
                        {
                            "symbol": symbol,
                            "tier": stock_data.get("tier"),
                            "score": stock_data.get("score"),
                            "snapshot_price": snapshot_price,
                            "current_price": current_price,
                            "price_change": price_change,
                            "target_entry": stock_data.get("target_entry_price"),
                        }
                    )

                except Exception as e:
                    logger.debug(f"Error tracking {symbol}: {e}")

            # Compute summary stats
            changes = [s["price_change"] for s in snapshot_result["stocks"] if s["price_change"] is not None]
            if changes:
                snapshot_result["avg_return"] = sum(changes) / len(changes)
                snapshot_result["best"] = max(changes)
                snapshot_result["worst"] = min(changes)
                snapshot_result["tracked_count"] = len(changes)

                # By tier
                for tier_num in [1, 2, 3]:
                    tier_changes = [
                        s["price_change"]
                        for s in snapshot_result["stocks"]
                        if s.get("tier") == tier_num and s["price_change"] is not None
                    ]
                    if tier_changes:
                        snapshot_result[f"tier{tier_num}_avg_return"] = sum(tier_changes) / len(tier_changes)

            results.append(snapshot_result)

        except Exception as e:
            logger.warning(f"Error processing snapshot {snapshot_path}: {e}")

    # Save tracking report
    try:
        report_path = _backtest_dir / "tracking_report.json"
        report_path.write_text(
            json.dumps({"generated_at": datetime.now().isoformat(), "snapshots": results}, indent=2, default=str)
        )
        logger.info(f"Saved tracking report to {report_path}")
    except Exception as e:
        logger.warning(f"Failed to save tracking report: {e}")

    return {"snapshots": results}


def generate_validation_report(
    correlation_results: Optional[dict] = None,
    tracking_results: Optional[dict] = None,
) -> str:
    """
    Generate a human-readable validation report.

    Args:
        correlation_results: Output from run_quality_return_correlation()
        tracking_results: Output from track_watchlist_performance()

    Returns:
        Formatted text report.
    """
    lines = []
    lines.append("=" * 60)
    lines.append("QUALITY VALIDATION REPORT")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("=" * 60)
    lines.append("")

    if correlation_results and "summary" in correlation_results:
        s = correlation_results["summary"]
        lines.append("## QUALITY-RETURN CORRELATION")
        lines.append("")
        lines.append(f"Stocks analyzed: {s.get('total_stocks', 0)}")
        lines.append(f"Lookback: {s.get('lookback_years', 0)} years")
        lines.append("")

        rho_1y = s.get("rank_correlation_1y")
        rho_3y = s.get("rank_correlation_3y")
        if rho_1y is not None:
            lines.append(f"Rank correlation (score vs 1Y return): {rho_1y:+.3f}")
        if rho_3y is not None:
            lines.append(f"Rank correlation (score vs 3Y return): {rho_3y:+.3f}")
        lines.append("")

        premium = s.get("quality_premium", 0)
        lines.append(f"Top 20% avg 1Y return:    {s.get('avg_1y_return_top_20pct', 0):+.1%}")
        lines.append(f"Bottom 20% avg 1Y return: {s.get('avg_1y_return_bottom_20pct', 0):+.1%}")
        lines.append(f"Quality premium:          {premium:+.1%}")
        lines.append("")

        quintiles = s.get("quintiles", [])
        if quintiles:
            lines.append("Quintile Analysis:")
            lines.append(f"  {'Quintile':<12} {'Score':>8} {'1Y Return':>10} {'Stocks':>8}")
            lines.append(f"  {'---':<12} {'---':>8} {'---':>10} {'---':>8}")
            for q in quintiles:
                ret_str = f"{q['avg_return_1y']:+.1%}" if q.get("avg_return_1y") is not None else "N/A"
                lines.append(f"  {q['label']:<12} {q['avg_score']:>8.1f} {ret_str:>10} {q['count']:>8}")
            lines.append("")

    if tracking_results and tracking_results.get("snapshots"):
        lines.append("-" * 60)
        lines.append("## WATCHLIST TRACKING (Forward Performance)")
        lines.append("")

        for snap in tracking_results["snapshots"]:
            snap_date = snap.get("snapshot_date", "?")[:10]
            avg_ret = snap.get("avg_return")
            count = snap.get("tracked_count", 0)

            lines.append(f"Snapshot: {snap_date} ({count} stocks tracked)")
            if avg_ret is not None:
                lines.append(f"  Avg return since snapshot: {avg_ret:+.1%}")
                for tier_num in [1, 2, 3]:
                    tier_ret = snap.get(f"tier{tier_num}_avg_return")
                    if tier_ret is not None:
                        lines.append(f"  Tier {tier_num} avg return: {tier_ret:+.1%}")
                lines.append(f"  Best:  {snap.get('best', 0):+.1%}")
                lines.append(f"  Worst: {snap.get('worst', 0):+.1%}")
            lines.append("")

    lines.append("-" * 60)
    lines.append("NOTE: This is a quality-return correlation study, not a")
    lines.append("point-in-time backtest. Past correlation does not prove")
    lines.append("future predictive power.")
    lines.append("=" * 60)

    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("Forward Validation Module")
    print("=" * 40)
    print("")

    # Check for existing snapshots
    tracking = track_watchlist_performance()
    if tracking.get("snapshots"):
        print(f"Found {len(tracking['snapshots'])} watchlist snapshots")
        report = generate_validation_report(tracking_results=tracking)
        print(report)
    else:
        print("No watchlist snapshots found yet.")
        print("Snapshots are created automatically during monthly briefing runs.")
        print("")
        print("To create a snapshot manually, run the monthly briefing first:")
        print("  python scripts/run_monthly_briefing.py")
