# BuffetBot system map (2026-10-19, main @ efc181b)

Repo: /home/bardie/git-repos/BuffetBot (git root); project lives in buffett-bot/.
Size: 22,660 lines in src/ + scripts/, 12,677 lines in tests/ (38 files). 1012 tests pass locally in 38 s.
Local main is 1 docs-only commit ahead of origin/main (f127418). Live image was built from origin/main 2026-10-18 10:47 UTC.
No AGENTS.md / CLAUDE.md in repo; .github/copilot-instructions.md is the only agent doc.
Plan docs buffettbot-v3-implementation-plan.md and buffettbot-portfolio-system-plan.md: NOT in the repo, any branch history, ~/ or /opt/docker-apps. Phase names in git log (Phase 1-5, 2.5, A-C) are the only trace.

## Entry points
| Entry | How it runs | What it does |
|---|---|---|
| scripts/scheduler.py `run_scheduler()` | container cmd `python -m scripts.scheduler` (buffett-bot-scheduler, up 24/7) | 13 `schedule` jobs below |
| scripts/run_monthly_briefing.py `run_monthly_briefing()` | Dockerfile default CMD, `docker compose run --rm buffett-bot` (manual profile) | legacy 11-step pipeline using JSON Registry (src/registry.py) + PortfolioTracker (src/portfolio.py), not the DB |
| scripts/run_briefing.py `main()` | manual | DB-driven briefing via src/briefing/db_briefing.py `generate_briefing_from_db()` |
| scripts/bulk_load.py `main()` | manual, one-shot | step1_sync_universe → step2_fetch_fundamentals → step3_quality_scores → step4_haiku_batch → step5_sonnet_batch → step6_report |
| src/backtest.py CLI | manual | mode 1 quality-return Spearman (yfinance current financials), mode 2 watchlist forward tracking, mode 3 `run_point_in_time_backtest()` on EDGAR PIT facts; `--load-pit` fills pit_fundamentals |
| src/journal_report.py `build_report()`, scripts/portfolio_status.py, scripts/health_check.py | manual CLIs | decision journal report, account status, health digest |
| scripts/replay_breaker_check.py, scripts/replay_light_model.py | manual research | the two 2026-10-17/18 research notes |
| scripts/backfill_*.py, scripts/clean_thesis_breakers.py | one-off migrations | |

## Scheduled jobs (TZ Europe/Copenhagen unless noted) and live state
| When | Function (scripts/scheduler.py) | Pipeline | Paid? | Live switch |
|---|---|---|---|---|
| Mon 02:00 | `monday_maintenance()` | `Database.reset_weekly_budgets` → `StockScreener.screen_tickers` on all 1359 universe tickers (yfinance) → `insider.fetch_insider_signals` tilt on top names → `quality_scorer.compute_quality_scores` → `Database.update_quality_score` → price-alert refresh → Alpaca `paper_positions` sync/prune → `run_retention_cleanup` | no | always |
| Wed 07:00 | `wednesday_haiku_batch()` | `get_unscreened_tickers(30)` + `get_expiring_haiku_tickers(20)` → `CompanyAnalyzer.batch_quick_screen` (Batch API) → `save_haiku_result` | Haiku, cap 50/wk | WEDNESDAY_HAIKU_ENABLED=true |
| Fri 07:00 | `friday_sonnet_batch()` | `get_portfolio_tickers_needing_analysis` + `get_held_unconfirmed_c_tickers` + `get_haiku_passes_without_analysis(5)` → `edgar_fetcher.augment_filing_text` → `batch_analyze_companies` → `ValuationAggregator.get_valuation` → `tier_engine.assign_tier` → `save_deep_analysis` / `log_tier_change` / `upsert_price_alert` | Sonnet, cap 10/wk | FRIDAY_SONNET_ENABLED=true |
| Fri 17:00 | `weekly_screen()` | `StockScreener.screen` → `valuation.screen_for_undervalued` → data/watchlist.json | no | always |
| Fri 16:15 NY | `weekly_auto_trade()` | `reconcile_broker_orders` → `bubble_detector.classify_market_regime` → watchlist.json → Haiku `quick_screen` → `deployment.plan_sells` / `plan_buys` → `AlpacaPaperAccount.buy/sell` → `Database.log_decision`; `_record_single_buy_recommendation` | Haiku | AUTO_TRADE_ENABLED=true |
| Daily 08:00 | `daily_watchlist_check()` | prices vs watchlist.json, approaching-target alerts | no | always |
| Daily 20:00 | `daily_news_monitor()` | `news_fetcher.run_news_pipeline`: Finnhub company news for S/A/B (105 names) → `apply_keyword_filter` → Haiku `check_news_for_red_flags` (cap 150/wk) → Sonnet `check_thesis_breakers` (cap 10/wk) → `mark_thesis_breaker_fired` → tier C | Haiku+Sonnet | DAILY_NEWS_ANALYSIS_ENABLED=true |
| Daily 22:00 | `daily_snapshot()` | `Database.save_snapshot` (equity, equity_dkk via `fx.usd_to_dkk`), `log_regime`, quarantine detection | no | always |
| Daily 22:30 | `daily_health_check()` | src/health.py 10 invariant checks → Discord | no | HEALTH_CHECKS_ENABLED |
| Daily 16:30 NY | `reconcile_broker_orders()` | pending `decision_log` rows → Alpaca order status → `close_trade` with `benchmark.fetch_benchmark_return` alpha | no | always |
| 1st 09:00 | `monthly_briefing()` | `generate_briefing_from_db` → NotificationManager (Email + Discord live) | Sonnet | MONTHLY_BRIEFING_ENABLED=true |
| 1st 10:00 | `monthly_cohort_validation()` | `backtest.run_tier_cohort_validation` → cohort_validation | no | always |

All five paid switches and auto-trading are ON in production (restart 2026-10-18 12:48). Budget caps: weekly_haiku_screen 50, weekly_sonnet_analysis 10, weekly_news_haiku 150, weekly_news_sonnet 10. Last night's health check: all three Haiku/news caps exhausted by Saturday.

## Data flow (one pass)
1. Universe: src/universe_builder.py `build_universe()` = conviction_list.yaml (39) + S&P 500 from Wikipedia (`_fetch_sp500_from_wikipedia`, 465) + Finviz small/mid screen (855) → `universe` table (1359 rows, all in_universe=1). src/universe.py is the older Finviz/fallback list still imported by screener.
2. Fundamentals: src/screener.py `StockScreener._fetch_stock_data` (yfinance `.info`, `.cashflow`, `.financials`, `.balance_sheet`), JSON cache data/cache/<T>.json, `_calculate_real_fcf` (SBC haircut), `_fetch_historical_data` (trend metrics). `score_stock()` applies config/screening_criteria.yaml weights + sector_overrides (Real Estate, Financial Services, Utilities, Energy) + cap_overrides (large). `fundamentals` table gets one row per ticker per Monday.
3. Quality score: src/quality_scorer.py `compute_quality_scores` percentile-ranks ROIC .30, ROE .20, FCF yield .20, op margin .15, low debt .15 (all ×0.95) + insider_buying .05 across the whole universe, no industry neutralisation → `universe.quality_score`.
4. Haiku quick screen: src/analyzer.py `QUICK_SCREEN_SYSTEM_PROMPT` (MOAT 1-5, QUALITY 1-5, REASON), input = `_build_db_summary` (DB fundamentals, no filing text), pass when score ≥ HAIKU_MIN_SCORE (5) → `haiku_screens`.
5. Sonnet deep analysis: `ANALYSIS_SYSTEM_PROMPT` (moat, management, durability, bear case, currency exposure for a DKK investor, fair value range, target entry, conviction, thesis breakers) with `edgar_fetcher.augment_filing_text` 10-K sections; parsed by src/analysis_parser.py `parse_analysis` into `AnalysisV2`. Models from src/config.py: deep claude-sonnet-5-5, light claude-haiku-4-5, opus claude-opus-5; thinking disabled; system prompts cache_control ephemeral; Batch API for Wed/Fri batches.
6. Valuation: src/valuation.py `ValuationAggregator.get_valuation` averages yfinance analyst target, Finnhub target (merged as one), P/E 18× EPS, Graham number, 10-yr DCF on real FCF, 10-yr DCF on owner earnings (`_calculate_dcf_estimates`, 10% discount, 12× terminal). `average_fair_value` is a plain mean.
7. Tier: src/tier_engine.py `assign_tier` — target = avg fair value × (1 − MARGIN_OF_SAFETY_PCT 25%); LLM target only as fallback. WIDE+HIGH & gap≤0 → S; HIGH/MEDIUM with wide/narrow moat & gap≤0 → A; gap<50% → B; ≥50% or NONE moat/LOW conviction → C; fair-price exception WIDE+HIGH, gap≤10%, quality_score≥80 → A. `staged_entry_suggestion` gives 3 tranches (S) / 2 (A).
8. Outputs: `deep_analyses`, `tier_history`, `price_alerts`; briefing text/HTML/JSON via src/briefing/{db_briefing,text_formatter,html_formatter}.py; src/notifications.py (Email, Telegram, ntfy, Discord).
9. Deployment: src/bubble_detector.py `classify_market_regime` votes from S&P P/E (thresholds 15/23/30), VIX (12/30/40), drawdown (−10/−20%), 200-dma (+15/−5%) → src/deployment.py `target_invested_pct` (euphoria 65%, overvalued 80%, fair 90%, correction 97%, crisis 100%) → `plan_buys` (TIER_WEIGHT S 1.0 A .85 B .65 None .55 C .45, quality ceiling 12.5% above FV, min trade $250) / `plan_sells` (ruleset 3.0.0: only a fired breaker forces a sell; otherwise rotation only) / `recommend_single_buy` (MIN_LOT_DKK 5000 via `fx.get_usd_dkk_rate`).
10. Journal: `decision_log` → `reconcile_broker_orders` → `closed_trades` (alpha vs SPY via src/benchmark.py); ruleset stamp from src/ruleset.py `RULESET_VERSION = "3.0.0"`.

## Database (SQLite WAL, src/database.py lines 39-420, additive `_COLUMN_MIGRATIONS` at line 435, PRAGMA user_version unused = 0)
Live file /opt/docker-apps/buffett-bot/data/buffett_bot_v2.db, 180 MB. Row counts 2026-10-19:
| table | rows | notes |
|---|---|---|
| universe | 1,359 | conviction 39 / sp500_filter 465 / finviz_screen 855 |
| fundamentals | 12,882 | 1,139 tickers × 25 weekly dates since 2026-04-07 (retention-pruned) |
| haiku_screens | 870 | 422 tickers passed, 430 failed |
| deep_analyses | 293 | 185 tickers; tiers A 8 / B 194 / C 91; **no S tier has ever been assigned** |
| tier_history | 293 | triggers: bulk_load 100, scheduled 96, news_event 97 (19 of them → C under old rules) |
| price_alerts | 124 | |
| news_events | 1,532 | 100 Haiku-material, 18 Sonnet-checked |
| thesis_breaker_events | 0 | ruleset 3.0.0 live since 2026-10-18; nothing fired yet |
| decision_log | 16 | 10 buys 2026-07-02..07-24 (status 'legacy'), 3 legacy sells, 3 sells canceled 2026-10-16 |
| closed_trades | 2 | AD (thesis_breaker, 1 day), GIS (take_profit, 7 days); realized −$577 |
| paper_positions | 7 | mirror of Alpaca (AGM, AIZ, CCI, CPRT, CTSH, DECK, NNI); entry_date NULL on all |
| portfolio_snapshots | 111 | alpaca_paper 2026-07-02..10-18; equity $100.5k → $106.3k; one bad day 2026-09-16 (equity $58.7k = cash only, positions missing) |
| regime_log | 111 | fair_value 101, overvalued 10 |
| quarantined_positions | 1 | AL (delisted, $7,242 frozen) |
| pit_fundamentals | 945,674 | 1,292 tickers, 15 XBRL concepts, used only by backtest mode 3 |
| cohort_validation | 18 | latest run: tier A n=8 since-rating alpha +0.2, hit 0.88; moat NONE n=22 alpha −0.1, hit 0.09 |
| run_log | 48 | bulk_load 9 ($11.97), wednesday_haiku 20 ($0.47), friday_sonnet 19 ($2.45) |
| budget_caps 4, alert_state 1, income_events 0, single_buy_recommendations 0 | | income and single-buy never populated |
Legacy JSON state still read by run_monthly_briefing.py / paper_trader.py: data/registry.json, watchlist_cache.json, watchlist_history.json, trade_log.json. Local repo data/ holds test fixtures only.

## Config
- config/screening_criteria.yaml: hard filters (cap $300M–$500B, price>$5, ADTV $2M, too_hard_industries, US, NASDAQ/NYSE), 16 scored metrics with ideal/min/max/weight, sector_overrides ×4, cap_overrides (large), top_n 100.
- config/conviction_list.yaml: 39 tickers; last_reviewed 2026-03-01, next_review 2026-06-01 (overdue).
- .env (src/config.py `Config` dataclass, ~45 vars): models, switches, budget-ish knobs (MAX_DEEP_ANALYSES 10, HAIKU_BATCH_SIZE, HAIKU_MIN_SCORE 5, ANALYSIS_MAX_AGE_DAYS 180, C_TIER_ANALYSIS_DAYS 30), deploy targets, MARGIN_OF_SAFETY_PCT 25, QUALITY_CEILING_PCT 12.5, MIN_LOT_DKK 5000, ASK_CONTRIBUTION_LIMIT (repo default 174,200; live .env still 135,900), EDGAR_USER_AGENT, USDDKK_OVERRIDE.
- Live config dir is byte-identical to the repo. Live compose differs from repo compose only in the watchtower label (false live, true repo).
- Hard-coded constants outside config: quality_scorer WEIGHTS, tier_engine EXTREME_PREMIUM_THRESHOLD 0.5 and fair-price exception, bubble_detector REGIME_THRESHOLDS, valuation DCF rate/terminal, deployment TIER_WEIGHT and NEAR_FAIR_VALUE_PCT, portfolio ASK_DIVIDEND_TAX_RATE 0.17.

## CI / deploy
.github/workflows/lint.yml (ruff, mypy, pytest, bandit, detect-secrets) and docker-image.yml (build on PR, push ghcr.io/bardiejoensen/buffett-bot:latest on main). Pre-commit hooks mirror CI. Dockerfile python:3.11-slim, non-root. Live compose at /opt/docker-apps/buffett-bot pulls :latest; the src/ and scripts/ copies in that directory are stale March files not mounted anywhere. 9 open dependabot PRs.

## Plan items: implemented / partial / not started
v3 implementation plan:
| Item | Status | Where |
|---|---|---|
| Decision journal | implemented | `decision_log`, `closed_trades`, `Database.log_decision/close_trade/tier_performance`, src/journal_report.py; only 16 decisions and 2 closed trades exist |
| SEC EDGAR 10-K ingestion | implemented | src/edgar_fetcher.py `fetch_10k_sections`, `augment_filing_text` (called in `friday_sonnet_batch` and run_monthly_briefing step 6); 41 filings cached live |
| Insider cluster-buying signal | partial | src/insider.py `fetch_insider_signals` (Finnhub) runs in `monday_maintenance` for top candidates only; 5% weight in `quality_scorer`; absent from tier_engine, analyzer prompt, deployment and briefing |
| Owner-earnings DCF fix | implemented | src/valuation.py `_calculate_dcf_estimates` (real FCF + owner earnings variants), averaged into `average_fair_value` with analyst/P-E/Graham |
| DKK / FX bridge | partial | src/fx.py `get_usd_dkk_rate` (yfinance DKK=X, cached); used for `portfolio_snapshots.equity_dkk`, `recommend_single_buy` lot sizing, legacy portfolio.py dividend DKK; no FX attribution of returns, no DKK equity curve in briefing |
| Phase 2.5 PIT fundamentals (companyfacts) | implemented for research only | src/edgar_fundamentals.py `build_pit_records`, `Database.get_pit_fundamentals_asof`, backtest mode 3; run once (docs/research/pit-backtest-2026-10-17.md); live screening still uses yfinance current financials |
Portfolio system plan:
| Item | Status | Where |
|---|---|---|
| Broker-agnostic accounts | implemented (one broker) | src/accounts/base.py `Account` Protocol, src/accounts/alpaca.py `AlpacaPaperAccount`, `get_accounts()`; src/paper_trader.py still used directly by auto-trade and briefing |
| Snapshot / event logging | implemented, partly unused | `portfolio_snapshots`, `regime_log`, `quarantined_positions` populated daily; `income_events` has 0 rows (no dividend/withholding logging wired) |
| Regime engine | implemented | src/bubble_detector.py → src/deployment.py (`target_invested_pct`, `plan_buys`, `plan_sells`) in `weekly_auto_trade` |
Other items in the plans cannot be verified without the documents.

## Known weakness check (industry blindness)
Sector overrides exist only in score_stock (4 sectors, via yaml) plus a hard TROW bypass; quality_scorer percentile-ranks every metric across all 1359 names with no industry neutralisation; valuation DCF skips loss/FCF-negative names and Graham is down-weighted for some; REITs, lessors (AL was held and then delisted), banks and insurers (AIZ, AGM held) flow through the same FCF-based pipeline.
