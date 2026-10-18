#!/usr/bin/env python3
"""
Replay the light-model calls through a second model and compare it with Haiku.

CompanyAnalyzer sends two jobs to the light model: quick_screen (the
Wednesday batch, which decides which names get a Sonnet analysis) and
check_news_for_red_flags (the daily news screen). This script gives the
current light model and a candidate identical inputs, through the same
CompanyAnalyzer methods and parsers; only the client and the model name
differ. Read-only: it never writes to the database (pass a copy).

Inputs:
  - quick screen: the latest stored screen of N tickers that passed and N
    that failed, rebuilt with the summary the Wednesday batch sends
    (_build_db_summary, so today's fundamentals, not those of the day of
    the stored screen).
  - news: N (ticker, day) groups the production screen rated material and N
    it did not. Finnhub's stories for that day are fetched again, keyword
    filtered and formatted as production does, and limited to the headlines
    that were logged. When Finnhub no longer returns them the logged
    headlines are sent without summaries (marked "headlines-only"). Thesis
    and breakers come from the last analysis before that day.

Each case costs one call per model.

Usage:
    python -m scripts.replay_light_model --db copy.db --out ab.json
    python -m scripts.replay_light_model --db copy.db --per-stratum 5 \\
        --candidate deepseek-flash --base-url https://api.deepseek.com/anthropic \\
        --api-key-env DEEPSEEK_API_KEY
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from src.analysis_parser import normalize_thesis_breakers
from src.database import DEFAULT_DB_PATH

# USD per million tokens: (input, cache read, output). DeepSeek halves its
# prices off-peak; both are listed so the cost table does not depend on when
# the replay ran.
PRICES = {
    "claude-haiku-4-5": (1.00, 0.10, 5.00),
    "deepseek-flash": (0.30, 0.006, 1.20),
    "deepseek-flash (off-peak)": (0.15, 0.003, 0.60),
}


class UsageRecorder:
    """Wraps client.messages.create so every call's usage and latency is kept."""

    def __init__(self, client):
        self.calls: list[dict] = []
        create = client.messages.create

        def recorded(**kwargs):
            start = time.monotonic()
            response = create(**kwargs)
            usage = response.usage
            self.calls.append(
                {
                    "input": usage.input_tokens or 0,
                    "cache_read": getattr(usage, "cache_read_input_tokens", 0) or 0,
                    "output": usage.output_tokens or 0,
                    "seconds": round(time.monotonic() - start, 2),
                }
            )
            return response

        client.messages.create = recorded

    def since(self, count: int) -> Optional[dict]:
        return self.calls[-1] if len(self.calls) > count else None


def load_screen_cases(conn: sqlite3.Connection, db, per_stratum: int) -> list[dict]:
    from scripts.scheduler import _build_db_summary

    cases = []
    for passed in (1, 0):
        rows = conn.execute(
            "SELECT h.ticker, h.screened_at, h.moat_estimate FROM haiku_screens h "
            "JOIN (SELECT ticker, MAX(screened_at) AS at FROM haiku_screens GROUP BY ticker) l "
            "ON h.ticker = l.ticker AND h.screened_at = l.at WHERE h.passed = ? "
            "ORDER BY h.screened_at DESC LIMIT ?",
            (passed, per_stratum),
        )
        for r in rows:
            cases.append(
                {
                    "ticker": r["ticker"],
                    "stored": {"passed": bool(passed), "screened_at": r["screened_at"], "moat": r["moat_estimate"]},
                    "input": _build_db_summary(r["ticker"], db),
                }
            )
    return cases


def _rebuild_news(fetcher, ticker: str, day: str, events: list[sqlite3.Row]) -> tuple[str, str]:
    from src.news_fetcher import apply_keyword_filter, format_news_for_llm

    logged = {e["headline"] for e in events}
    start = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
    material = [
        m for m in apply_keyword_filter(fetcher.get_company_news(ticker, start, day)) if m.get("headline") in logged
    ]
    if material:
        return format_news_for_llm(material), "finnhub"
    items = [
        {"headline": e["headline"], "source": e["source"], "datetime": (e["published_at"] or "")[:10]} for e in events
    ]
    return format_news_for_llm(items), "headlines-only"


def load_news_cases(conn: sqlite3.Connection, per_stratum: int) -> list[dict]:
    from src.news_fetcher import FinnhubNewsFetcher

    fetcher = FinnhubNewsFetcher()
    cases = []
    for material in (1, 0):
        groups = conn.execute(
            "SELECT ticker, substr(detected_at, 1, 10) AS day, MIN(detected_at) AS at FROM news_events "
            "WHERE haiku_material IS NOT NULL GROUP BY ticker, day HAVING MAX(haiku_material) = ? "
            "ORDER BY day DESC",
            (material,),
        )
        taken = 0
        for g in groups:
            if taken >= per_stratum:
                break
            prior = conn.execute(
                "SELECT analyzed_at, investment_thesis, thesis_breakers FROM deep_analyses "
                "WHERE ticker = ? AND analyzed_at < ? ORDER BY analyzed_at DESC LIMIT 1",
                (g["ticker"], g["at"]),
            ).fetchone()
            if not prior or not prior["investment_thesis"]:
                continue
            events = list(
                conn.execute(
                    "SELECT headline, source, published_at FROM news_events "
                    "WHERE ticker = ? AND substr(detected_at, 1, 10) = ? ORDER BY detected_at",
                    (g["ticker"], g["day"]),
                )
            )
            news, source = _rebuild_news(fetcher, g["ticker"], g["day"], events)
            cases.append(
                {
                    "ticker": g["ticker"],
                    "day": g["day"],
                    "stored": {"material": bool(material)},
                    "thesis": prior["investment_thesis"],
                    "breakers": normalize_thesis_breakers(json.loads(prior["thesis_breakers"] or "[]")),
                    "news": news,
                    "news_source": source,
                }
            )
            taken += 1
    return cases


def run_screens(cases: list[dict], models: dict) -> None:
    for case in cases:
        case["results"] = {}
        for name, (analyzer, recorder) in models.items():
            before = len(recorder.calls)
            result = analyzer.quick_screen(case["ticker"], case["input"])
            case["results"][name] = {**result, "usage": recorder.since(before)}
        line = "  ".join(
            f"{n}={'pass' if r.get('worth_analysis') else 'fail'}{'' if r.get('valid') else '(invalid)'}"
            for n, r in case["results"].items()
        )
        print(
            f"screen {case['ticker']}: stored={'pass' if case['stored']['passed'] else 'fail'}  {line}",
            file=sys.stderr,
            flush=True,
        )


def run_news(cases: list[dict], models: dict, *, retries: int = 2) -> None:
    for case in cases:
        case["results"] = {}
        for name, (analyzer, recorder) in models.items():
            before = len(recorder.calls)
            for _ in range(retries):
                try:
                    result = analyzer.check_news_for_red_flags(
                        case["ticker"], case["thesis"], case["breakers"], case["news"]
                    )
                    break
                except Exception as exc:  # parse error or transient API failure
                    result = {"valid": False, "error": f"{type(exc).__name__}: {exc}"[:300]}
                    time.sleep(2)
            case["results"][name] = {**result, "usage": recorder.since(before)}
        line = "  ".join(
            f"{n}={r.get('recommendation') or r.get('error', '?')[:40]}" for n, r in case["results"].items()
        )
        print(
            f"news {case['ticker']} {case['day']}: stored={'material' if case['stored']['material'] else 'no'}  {line}",
            file=sys.stderr,
            flush=True,
        )


def _cost(model: str, usage: dict) -> float:
    # Both APIs report input_tokens without the cache reads (DeepSeek's
    # Anthropic endpoint follows Anthropic's convention).
    price_in, price_cache, price_out = PRICES[model]
    return (usage["input"] * price_in + usage["cache_read"] * price_cache + usage["output"] * price_out) / 1e6


def summarise(screens: list[dict], news: list[dict], baseline: str, candidate: str) -> dict:
    out: dict = {"baseline": baseline, "candidate": candidate}

    both = [c for c in screens if all(c["results"][m].get("valid") for m in (baseline, candidate))]
    out["screen"] = {
        "cases": len(screens),
        "invalid": {m: sum(1 for c in screens if not c["results"][m].get("valid")) for m in (baseline, candidate)},
        "agree_pass_fail": sum(
            1 for c in both if c["results"][baseline]["worth_analysis"] == c["results"][candidate]["worth_analysis"]
        ),
        "both_valid": len(both),
        "candidate_pass_baseline_fail": sum(
            1
            for c in both
            if c["results"][candidate]["worth_analysis"] and not c["results"][baseline]["worth_analysis"]
        ),
        "candidate_fail_baseline_pass": sum(
            1
            for c in both
            if not c["results"][candidate]["worth_analysis"] and c["results"][baseline]["worth_analysis"]
        ),
        "mean_abs_moat_diff": round(
            sum(abs(c["results"][baseline]["moat_hint"] - c["results"][candidate]["moat_hint"]) for c in both)
            / max(len(both), 1),
            2,
        ),
        "mean_abs_quality_diff": round(
            sum(abs(c["results"][baseline]["quality_hint"] - c["results"][candidate]["quality_hint"]) for c in both)
            / max(len(both), 1),
            2,
        ),
        "agree_with_stored": {
            m: sum(
                1
                for c in screens
                if c["results"][m].get("valid") and c["results"][m]["worth_analysis"] == c["stored"]["passed"]
            )
            for m in (baseline, candidate)
        },
    }

    both = [c for c in news if all(c["results"][m].get("valid") for m in (baseline, candidate))]
    out["news"] = {
        "cases": len(news),
        "finnhub_inputs": sum(1 for c in news if c["news_source"] == "finnhub"),
        "invalid": {m: sum(1 for c in news if not c["results"][m].get("valid")) for m in (baseline, candidate)},
        "both_valid": len(both),
        "agree_red_flags": sum(
            1 for c in both if c["results"][baseline]["has_red_flags"] == c["results"][candidate]["has_red_flags"]
        ),
        "agree_recommendation": sum(
            1 for c in both if c["results"][baseline]["recommendation"] == c["results"][candidate]["recommendation"]
        ),
        "recommendations": {
            m: {
                k: sum(1 for c in news if c["results"][m].get("recommendation") == k)
                for k in ("HOLD", "REVIEW", "SELL")
            }
            for m in (baseline, candidate)
        },
        "material_vs_stored": {
            m: sum(
                1
                for c in news
                if c["results"][m].get("valid") and c["results"][m]["has_red_flags"] == c["stored"]["material"]
            )
            for m in (baseline, candidate)
        },
    }

    def per_call(model: str, price_key: str) -> dict:
        usages = [c["results"][model]["usage"] for c in screens + news if c["results"][model].get("usage")]
        n = max(len(usages), 1)
        return {
            "calls": len(usages),
            "usd_per_call": round(sum(_cost(price_key, u) for u in usages) / n, 5),
            "mean_seconds": round(sum(u["seconds"] for u in usages) / n, 2),
            "mean_input_tokens": round(sum(u["input"] for u in usages) / n),
            "mean_output_tokens": round(sum(u["output"] for u in usages) / n),
        }

    out["cost"] = {}
    for model in (baseline, candidate):
        for price_key in (model, f"{model} (off-peak)"):
            if price_key in PRICES:
                out["cost"][price_key] = per_call(model, price_key)
    return out


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="A copy of the database (opened read-only)")
    parser.add_argument("--out", type=Path, help="Write inputs, outputs and the summary as JSON")
    parser.add_argument("--per-stratum", type=int, default=30, help="Cases per stored verdict, per job")
    parser.add_argument("--candidate", default="deepseek-flash")
    parser.add_argument("--base-url", default="https://api.deepseek.com/anthropic")
    parser.add_argument("--api-key-env", default="DEEPSEEK_API_KEY")
    parser.add_argument("--only", choices=("screen", "news"))
    args = parser.parse_args(argv)

    if not args.db.exists():
        print(f"No such database: {args.db}", file=sys.stderr)
        return 1
    candidate_key = os.getenv(args.api_key_env)
    if not candidate_key:
        print(f"{args.api_key_env} is not set", file=sys.stderr)
        return 1

    from anthropic import Anthropic

    from src.analyzer import CompanyAnalyzer
    from src.database import Database

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    screens = [] if args.only == "news" else load_screen_cases(conn, Database(args.db), args.per_stratum)
    news = [] if args.only == "screen" else load_news_cases(conn, args.per_stratum)
    conn.close()
    print(
        f"{len(screens)} screen case(s), {len(news)} news case(s) "
        f"({sum(1 for c in news if c['news_source'] == 'finnhub')} with Finnhub stories)",
        file=sys.stderr,
    )

    baseline = CompanyAnalyzer()
    contender = CompanyAnalyzer()
    contender.client = Anthropic(api_key=candidate_key, base_url=args.base_url)
    contender.model_light = args.candidate
    models = {
        baseline.model_light: (baseline, UsageRecorder(baseline.client)),
        args.candidate: (contender, UsageRecorder(contender.client)),
    }

    run_screens(screens, models)
    run_news(news, models)
    summary = summarise(screens, news, baseline.model_light, args.candidate)
    print(json.dumps(summary, indent=1))
    if args.out:
        args.out.write_text(json.dumps({"summary": summary, "screens": screens, "news": news}, indent=1, default=str))
        print(f"wrote {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
