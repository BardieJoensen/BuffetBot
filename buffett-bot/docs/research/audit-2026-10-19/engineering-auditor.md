# Engineering audit, BuffetBot, 2026-10-19 (main @ efc181b)

Auditor: engineering-auditor (read-only). Method: code read, `pytest` under `coverage` (branch), `ruff`/`mypy` (no cache), live SQLite via `mode=ro` from `python3 -I`, `docker logs` / `docker inspect` / `docker stats` (read-only), two `gh` list calls (workflow runs, PRs). No LLM calls, no yfinance/EDGAR/Finnhub calls, no repo or container changes. `coverage` was installed with `pip --target` into the session scratchpad, and `.coverage` was written there, not into the repo.

## Headline

1. The paper trader has not bought anything since 2026-07-24 and, as built, cannot: the only candidate source (`data/watchlist.json`, 25 names) contains zero S/A-tier names, and `weekly_auto_trade` requires S/A (F1).
2. Since late July the Monday job refreshes fundamentals for only 100 of 1,359 universe names; Haiku/Sonnet are fed 3.5-month-old data for ~94% of what they screen (F2).
3. The whole paid pipeline was dark for seven weeks (2026-08-11 to 2026-10-01) because a default flipped and the live `.env` lacked the new keys, and nothing alerted (F3). A second, shorter total LLM failure ran 2026-10-01 to 10-07 and cannot be diagnosed because container logs only start 2026-10-18 (F4, F12).
4. Operational safety net is thin: no loop-level exception guard, no watchdog, no timeouts on SMTP/Anthropic, no catch-up after restart, job failures are log-only (F5-F8, F10).
5. Suite is 1,012 green tests but only 60% line+branch coverage overall, 14% on `bubble_detector` (which sets the invested-percent target), 44% on `screener`, and the NaN-sort regression test tests a copy of the code (F18, F19).

## Measured facts

### Test coverage per module (branch coverage, `coverage run --branch --source=src,scripts -m pytest`; 1012 passed in 41 s; Python 3.12.3 locally, image is 3.11)

| module | stmts | miss | cover |
|---|---|---|---|
| TOTAL | 9,887 | 3,717 | 60% |
| src/database.py | 663 | 36 | 94% |
| scripts/scheduler.py | 883 | 274 | 69% |
| src/analyzer.py | 424 | 114 | 70% |
| src/analysis_parser.py | 198 | 5 | 96% |
| src/tier_engine.py | 177 | 28 | 84% |
| src/deployment.py | 172 | 6 | 94% |
| src/quality_scorer.py | 123 | 4 | 97% |
| src/health.py | 203 | 11 | 93% |
| src/news_fetcher.py | 231 | 10 | 94% |
| src/briefing/db_briefing.py | 731 | 34 | 91% |
| src/accounts/alpaca.py | 37 | 2 | 93% |
| src/paper_trader.py | 307 | 144 | 51% |
| src/valuation.py | 261 | 77 | 63% |
| src/screener.py | 541 | 276 | 44% |
| src/bubble_detector.py | 267 | 217 | 14% |
| src/backtest.py | 522 | 309 | 39% |
| src/benchmark.py | 116 | 64 | 47% |
| src/notifications.py | 317 | 200 | 33% |
| src/universe.py | 131 | 108 | 15% |
| src/universe_builder.py | 162 | 68 | 60% |
| src/edgar_fetcher.py | 204 | 41 | 78% |
| src/portfolio.py (legacy) | 337 | 137 | 55% |
| src/registry.py (legacy) | 170 | 90 | 46% |
| scripts/run_monthly_briefing.py (legacy, Docker CMD) | 475 | 388 | 18% |
| scripts/bulk_load.py | 330 | 163 | 50% |
| src/journal_report.py | 47 | 47 | 0% |
| scripts/health_check.py, portfolio_status.py, run_briefing.py, replay_*.py | 25/97/62/75/145 | all | 0% |

Uncovered in `scheduler.py`: `weekly_screen` (154-200), `daily_watchlist_check` (210-260), `run_scheduler` registration (nothing calls it). Uncovered in `screener.py`: `_fetch_historical_data` and trend metrics (596-797), and the production sort + `top_n` truncation (1011-1036). Uncovered in `bubble_detector.py`: all of `classify_market_regime` (170-260) and the three yfinance fetchers.

### Static checks
`ruff check` clean, `ruff format --check` clean (84 files), `mypy src scripts` clean (47 files, CI pins 1.14.1, local 1.19.1). mypy runs without `disallow_untyped_defs` (103 untyped-def errors with it on), `ignore_missing_imports = true`, and tests are not type-checked in CI. 170 `except Exception`/bare-except sites in src+scripts, about 10 are pure swallow (`pass`/`continue`/`return`), 34 `logger.debug` calls, 105 naive `datetime.now()`/`date.today()` vs 3 tz-aware.

## Failure-scenario traces

### Scenario A: Anthropic batch times out (or the process dies) during the Friday 07:00 Sonnet job
Code: `scripts/scheduler.py:friday_sonnet_batch` (1431-1604), `src/analyzer.py:_wait_for_batch` (1062-1088, `BATCH_TIMEOUT_MINUTES = 120`), `Database.spend_batch/refund_batch` (702/750), `start_run/complete_run` (1420/1429).
1. `spend_batch("weekly_sonnet_analysis", n)` reserves n of the 10 weekly slots in `budget_caps` (committed).
2. `augment_filing_text` runs per candidate (EDGAR, file-cached under data/edgar).
3. `db.start_run("friday_sonnet")` inserts a `run_log` row with `completed_at NULL`.
4. `batch_analyze_companies` submits the batch and polls every 30 s inside the scheduler thread. Nothing else runs for up to 2 h (the 08:00 watchlist check and, on the 1st, the 09:00 briefing queue behind it).
5. Timeout: `batches.cancel` is called, `TimeoutError` raised, the `except Exception` in the job calls `refund_batch` (slots returned) and re-raises into the outer `except Exception as e: logger.error(...)`. The job ends.
State left behind (timeout path): budget refunded; one orphan `run_log` row (`completed_at NULL`, 0 calls, $0); requests that completed before the cancel are billed by Anthropic and their results are discarded (never read); no `deep_analyses`/`tier_history` rows; the same tickers are retried next Friday. No notification is sent: the only trace is an ERROR line and the orphan row. `check_scheduled_jobs` counts `started_at` regardless of `completed_at`, so the orphan row makes the job look healthy.
Process-death path (container restart or crash during the up-to-2 h wait): the batch id lives only in memory, so the batch keeps running and billing at Anthropic with nobody to read it; the reserved slots are never refunded (until Monday 02:00); the orphan `run_log` row stays; and after restart `schedule` does not re-run Friday 07:00.
Live evidence: `run_log` ids 44 (friday_sonnet 2026-10-02 05:01 UTC) and 45 (wednesday_haiku 2026-10-07 05:00 UTC) have `completed_at NULL`, 0 calls; ids 8, 9, 10, 15 are the same in April/May. Cause of 44/45 is not recoverable (F4).
Mid-way variant: if the batch succeeds but the process dies inside the per-analysis loop, parsed analyses are already in `data/analyses/*.json` (30-day file cache) so next Friday re-uses them without API cost, but `deep_analyses` rows are written one at a time, so a partial set of tiers can land. The `complete_run` cost is `len(analyses) * 0.025`, an estimate, not usage (F13).

### Scenario B: Alpaca returns a partial or inconsistent account/positions view
Code: `src/accounts/alpaca.py:get_state/get_positions`, `src/paper_trader.py:get_account/get_positions` (BrokerReadError on API exceptions), `scheduler.py:daily_snapshot` (354-428), `monday_maintenance` step 4 (1277-1311), `weekly_auto_trade` (685-1078).
(a) Account call fails or returns invalid numbers: `get_account` returns `{"error":...}`, `AlpacaPaperAccount.get_state` raises `RuntimeError`, `daily_snapshot` catches per account, logs `Snapshot failed`, writes no row, sends no alert. Next-day `snapshot_freshness` WARNs only after 36 h (and the age arithmetic is skewed by 2 h, F11). In `weekly_auto_trade` the same exception at `state = account.get_state()` (line 827) propagates to the job-level `except` after the Haiku screen has been paid for: no orders, nothing journaled, ERROR line only.
(b) Positions call fails: raises `BrokerReadError` (fail-closed, good), same handling as (a).
(c) Positions call SUCCEEDS but is incomplete (the real case, not covered by any guard): live snapshot id 79, 2026-09-16 20:00:13, stored equity 58,744 (cash 30,880 + market value of 3 positions 27,863; AGM, CPRT, DECK) between snapshots of 105,555 and 104,680 with 8 positions. Account equity and the position list agreed with each other, so Alpaca itself returned a truncated view for that call. No code compares position count or equity with the previous snapshot or with the journal. State left: a permanent -44% point in `portfolio_snapshots`, `equity_dkk` 382,790 vs 681,637 next day; all 111 snapshots otherwise satisfy `|gross_equity - (cash + sum(mv))| < 1%`, so the check that would catch this class is cheap and absent. Had it been a Friday 22:15 CEST: `held_symbols` would omit 5 holdings, `PaperTrader.buy`'s duplicate guard reads the same truncated `get_positions`, and `plan_buys` would see a gap of roughly 25k and could re-buy names already held (bounded only by `max_position_pct` 15% of the (equally truncated) portfolio value). On a Monday, `monday_maintenance` step 4 prunes `paper_positions` to the 3 returned names (only an empty list skips the prune).

### Scenario C: yfinance returns an empty/None result for a held ticker
Code: `screener.py:_fetch_stock_data` (452-527), `_calculate_real_fcf` (529-605), `valuation.py:get_valuation` (120-169), `scheduler.py:weekly_auto_trade` pricing loop (845-860), `monday_maintenance` step 2/3.
- Monday: `.info` empty or no `regularMarketPrice` returns None (`except Exception: logger.debug`), counted as an error; the ticker is silently dropped from `screened`, so no new `fundamentals` row and no quality-score update for it. Live log 2026-10-19: `Processed 1359 stocks, 37 errors`, with yfinance 404 "Quote not found" for BK, CTRA, SATS, ASGN, IAC (all live tickers; transient) next to AL (genuinely delisted). 2.7% data loss, logged at INFO, no threshold, no list of names (F14).
- `ticker.cashflow` empty: `_calculate_real_fcf` returns None and the caller silently substitutes plain `fcf_yield` (includes SBC), so the field named `real_fcf_yield` mixes two definitions across the universe week to week (F26).
- Friday trade: `aggregator.get_valuation(pos.symbol)` on empty info yields price 0 and no estimates (`mos=None`), or raises. For a held name this is logged at WARNING and the position enters `plan_sells` with `margin_of_safety=None`: `near_fair_value` is False so a non-C holding is simply kept; a C-tier holding is rotation-eligible regardless. No alert, no retry, no record that pricing failed.
- Friday Sonnet: `friday_sonnet_batch` skips the tier update ("no valid deterministic market price") but the Sonnet slot was already spent and the analysis exists only in the JSON cache; the ticker is re-queued next week and served from cache (free) if within 30 days.
State left: stale `fundamentals`/`universe.quality_score` for that ticker, stale `price_alerts.last_price`, no tier change; the position itself is never touched.

### Scenario D (restart): container restarts at 01:50 on a Monday
`schedule` keeps no persistent state and has no catch-up. `monday_maintenance` (budget reset, fundamentals, price alerts, position mirror, retention) does not run that week; the weekly caps stay at whatever Sunday left (news caps were 150/150 and 10/10 at last check), so Wednesday/Friday paid jobs find `spend_batch` returning 0 and quietly skip; `check_budget_reset` only fails after more than 8 days. The same applies to Friday 22:15 CEST (no auto-trade that week) and to the 1st-of-month gating (restart on the 1st after 09:00 means no briefing and no cohort validation that month).

---

## Findings

### F1. The automated buy path has an empty candidate funnel by construction; no buy since 2026-07-24
- **What:** `weekly_auto_trade` draws candidates only from `data/watchlist.json` (written by the Friday `weekly_screen` from a yfinance score screen), then requires a current S/A deep-analysis tier from the DB. The two populations are disjoint, so no candidate survives. The cash balance has been constant for 86 days.
- **Where:** `scripts/scheduler.py:weekly_auto_trade` lines 745-815 (watchlist read at ~756, Haiku quick-screen at 788-795, S/A gate at 808); `weekly_screen` 147-200 (never reads tiers); `friday_sonnet_batch` docstring ("so the watchlist is primed with fresh tiers") is not true of the code.
- **Evidence:** live `watchlist.json` (updated 2026-10-16T17:05, 25 stocks): latest deep-analysis tier per symbol = None 20, B 3, C 2, **S/A 0**. The four A-tier names in `price_alerts` (BIO, AZZ, IT, AIZ) are not in it (AIZ is held, the others are not). `decision_log`: last buy 2026-07-24 16:01:13, 0 buys since. `portfolio_snapshots`: cash = 30,880.39 in 87 consecutive snapshots 2026-07-25 to 2026-10-18; invested_pct 0.709 on 10-18 versus the 90% `fair_value` target in `deployment.target_invested_pct`. The Haiku call (up to 20 per Friday) is made before the tier check, so it is paid for names that cannot be bought.
- **Why it matters:** the Alpaca paper account, the only real out-of-sample trading record, is idle at ~29% cash while the regime engine says to be 90% invested; the 2026-10-17 ruleset 3.0.0 changes can never be exercised on the buy side, so "paper results" now measure nothing about the S/A thesis. Also wasted Haiku spend.
- **Severity:** high
- **Effort:** S (feed candidates from DB tiers: `price_alerts` S/A joined to valuation, drop the JSON hop) to M (retire the Friday JSON screen)
- **Unlocks / depends on:** any meaningful forward test of tiers; depends on deciding whether the DB tier universe or the score screen is the buy source.
- **Confidence:** high (read both sides of the join and the live data; the only unknown is whether the user intends this).

### F2. Monday refresh persists only the top 100, so ~90% of the universe is screened from 3.5-month-old fundamentals
- **What:** `monday_maintenance` calls `StockScreener.screen_tickers`, which truncates to `criteria.top_n = 100` before returning; the job writes `fundamentals` and re-ranks `quality_score` only for those 100. Haiku/Sonnet summaries read `get_latest_fundamentals`, and the code comments assert the data is "<= 4 days old".
- **Where:** `src/screener.py:1033-1036` (`top_n` truncation inside `screen_tickers`), `config/screening_criteria.yaml:174` (`top_n: 100`), `scheduler.py:monday_maintenance` 1195-1235, `_build_db_summary` 103-145 (docstring claim), `src/quality_scorer.py:compute_quality_scores`.
- **Evidence:** log 2026-10-19: `Keeping top 100 by score (from 1078)` then `Refreshed fundamentals for 100/1359 universe stocks`, `Recomputed quality scores for 100 stocks`. `fundamentals`: 100 rows per Monday since 2026-07-26, versus 1,068-1,093 per Monday through 2026-06-29. Latest fundamentals date per universe ticker: 100 at 2026-10-19, 1 at 10-12, 1 at 10-05, 4 at 09-28 ... and 277 universe tickers with nothing newer than before 06-29. Of the 50 tickers Haiku-screened on 2026-10-14, 46 have latest fundamentals dated 2026-06-29, 1 dated 2026-05-11, 3 current (47/50 stale). `universe.quality_score`: 100 rows re-ranked as percentiles within the 100-name cohort (mean 49.7, max 85.0), 1,039 rows left with older percentiles over the ~1,080-name cohort (max 98.5): the column now mixes two scales, and `tier_engine`'s fair-price exception keys off `quality_score >= 80`.
- **Why it matters:** the LLM stages and the quality-score gate silently run on numbers that are months old for almost everything outside the current top 100 (and a name drops out of the 100 simply by ranking lower, which freezes it). Wednesday's "unscreened first" queue is therefore judging companies on June data.
- **Severity:** high
- **Effort:** S (persist all screened rows, apply `top_n` only to the returned list) plus M to re-baseline `quality_score`
- **Unlocks / depends on:** trustworthy Haiku/Sonnet inputs, any backtest of `quality_score >= 80`.
- **Confidence:** high on the symptom (log + DB). Cause of the earlier ~1,070 rows per week is **unverified**: `top_n` truncation existed in `1de7d8c^` too, so the pre-July path that saved ~1,070 rows is not explained by the code I read.

### F3. Seven weeks of silent pipeline blackout caused by a default flip (2026-08-11 to 2026-10-01)
- **What:** commit `ae3647b` (2026-08-11) changed the paid-job switches from implicit-on to opt-in (`...ENABLED` default false). The live `.env` had no such keys until 2026-10-01 (`.env` mtime 10-01 08:37; the diff of `.env.bak-20261001` shows exactly `WEDNESDAY_HAIKU_ENABLED`, `FRIDAY_SONNET_ENABLED`, `DAILY_NEWS_ANALYSIS_ENABLED` added). Nothing alerted.
- **Where:** `src/config.py:130-136`, `scripts/scheduler.py:run_scheduler` 1662-1760 (switches read once at start; only a startup banner), `src/health.py:check_scheduled_jobs` (9-day WARN, throttled, only two job names).
- **Evidence:** `run_log`: last wednesday_haiku/friday_sonnet runs 2026-08-05/08-07, next 2026-10-02/10-07 (7-week gap). `news_events`: 214 rows in August, **zero** between 2026-08-11 and 2026-09-30. `deep_analyses` by month: Aug 19, Sep 0, Oct 28. No monthly briefing file after `briefing_2026_07` (Aug 1, Sep 1, Oct 1 missing; Aug 1 predates the flip, cause unverified).
- **Why it matters:** the tiers, news-driven breaker detection, and Haiku coverage all stopped for most of Q3 with the container "healthy". Under ruleset 3.0.0 the breaker is the only forced sell, so a dead news monitor is a dead risk control. The class of failure (opt-in switch + no "expected vs actual" assertion) remains.
- **Severity:** high
- **Effort:** S
- **Unlocks / depends on:** an alert when an enabled-by-policy job has not produced its run row in N days, regardless of the flag; a startup log line that diffs `.env` keys vs `config` fields.
- **Confidence:** high on the gap and the commit; medium on causality (no logs from that period survive).

### F4. A second total LLM failure (2026-10-01 to 10-07) is undiagnosable and went through the same hole
- **What:** every Haiku news call failed for seven days; the Friday Sonnet batch (10-02), the Wednesday Haiku batch (10-07) and the 10-01 briefing all produced nothing. The only record is rows with NULLs.
- **Where:** `src/news_fetcher.py:run_news_pipeline` (Haiku failure branch logs the event with `haiku_material=None`), `scheduler.py:friday_sonnet_batch`/`wednesday_haiku_batch` (`start_run` without a failure path), `scripts/run_briefing.py:main`.
- **Evidence:** `news_events` with `haiku_material IS NULL`: 10-01 72/72, 10-02 35/35, 10-04 39/39, 10-05 21/21, 10-06 21/21, 10-07 22/22 (231 total), then 0-2 per day from 10-08. `run_log` ids 44 and 45 `completed_at NULL`. `data/briefings/` directory mtime is 2026-10-01 07:00 UTC (the `.write_test` create/unlink in `run_briefing.main`) with no `briefing_2026_10.*`: the job started and produced no file. `docker logs` begin 2026-10-18 12:48 (container recreated), so the error text is gone.
- **Why it matters:** a week of unmonitored holdings and a missed briefing; and because `can_spend` increments before each call, the failing calls also burned news budget. Without persisted logs the same failure cannot be root-caused next time either.
- **Severity:** high (observability), the underlying fault appears fixed
- **Effort:** S (persist ERROR-level job outcomes in a table; see F10/F12)
- **Unlocks / depends on:** post-mortems; a "N consecutive LLM failures" alert.
- **Confidence:** medium (symptoms are hard data; the cause is unverified).

### F5. The scheduler loop has no exception guard and two jobs have unguarded setup
- **What:** `while True: schedule.run_pending()` has no try/except; `schedule` re-raises job exceptions. `daily_snapshot` and `reconcile_broker_orders` do `Database()` (which writes: schema/migration/budget sync) and `get_accounts()` outside any try.
- **Where:** `scripts/scheduler.py:1760-1762`; `daily_snapshot` 372-376; `reconcile_broker_orders` 515-540 (`db = Database()`, `get_pending_order_decisions()`, `get_accounts()` unguarded).
- **Evidence:** a `sqlite3.OperationalError: database is locked` (busy_timeout 5 s, `src/database.py:453`) while a manual script holds a write transaction, or any import error in a lazy `from src...` inside those jobs, escapes to the process. The other 11 jobs wrap their whole body. `docker inspect`: `restart=unless-stopped`, RestartCount 0, no HEALTHCHECK (`health=null`).
- **Why it matters:** a crash restarts the container and, because of F7, silently skips whatever was due; there is no signal that it happened.
- **Severity:** medium
- **Effort:** S
- **Unlocks / depends on:** wrapping `run_pending` and recording a job outcome row (F10).
- **Confidence:** high (code), the lock scenario is unverified in production.

### F6. No watchdog or timeouts: one hung call stops every job
- **What:** the scheduler is one thread; `smtplib.SMTP(...)` is created without `timeout`, and `Anthropic(api_key=...)` uses SDK defaults (10 min read timeout, 2 retries) for 22-150 sequential news calls and 20 Friday Haiku calls. A stuck SMTP connect or a degraded API can hold the loop for hours; nothing detects a stalled loop.
- **Where:** `src/notifications.py:88,115` (`smtplib.SMTP(self.smtp_host, self.smtp_port)`; also `starttls()` with no context), `src/analyzer.py:514`, `_wait_for_batch` (2 h inside the thread), `scheduler.py:monday_maintenance` (19 min observed 02:00-02:21).
- **Evidence:** the SMTP alerts are sent from inline job paths (`PaperTrader._notify_trade` right after an order submit, `daily_snapshot` quarantine alert, `reconcile_broker_orders` per-order alerts), so a blocked SMTP connect delays order journaling. Requests-based channels do set `timeout=10-30`.
- **Why it matters:** a hung job also delays the 22:00 snapshot (equity timestamp drifts) and the Friday 16:15 NY trade; there is no dead-man switch to notice.
- **Severity:** medium
- **Effort:** S (timeouts) / M (heartbeat file + compose HEALTHCHECK + external ping)
- **Unlocks / depends on:** unattended operation.
- **Confidence:** high on missing timeouts (code), no hang observed.

### F7. No catch-up after a restart; missed Mondays, Fridays and 1st-of-month jobs are lost
- **What:** `schedule` is in-memory; `monthly_*` jobs gate on `datetime.now().day != 1`, budget reset lives only in the Monday job, and nothing records "last successful run" per job to compare at startup.
- **Where:** `scheduler.py:run_scheduler` (registration, no state), `monthly_briefing` 1111, `monthly_cohort_validation` 1081, `Database.reset_weekly_budgets`.
- **Evidence:** the live container was recreated 2026-10-18 12:48 UTC (a Sunday); the Friday 10-16 jobs ran in the old container but their logs are gone. See Scenario D for the Monday case. `check_budget_reset` fails only after 8 days.
- **Why it matters:** each deploy is a coin-flip on skipping a weekly job; with `watchtower` enabled in the repo compose (F16) that would be automatic.
- **Severity:** medium
- **Effort:** M (job-state table + startup catch-up policy per job)
- **Unlocks / depends on:** F10's job-run table.
- **Confidence:** high (code), impact in production unquantified.

### F8. Friday 16:15 NY submission: sells are always "pending", so buys are always deferred a week; ambiguous submit failures become "skipped"
- **What:** orders go in after the close as DAY market orders, so `sell_order["status"]` is never `filled` at submit time; `pending_sell` becomes True and the whole buy half of the cycle is skipped (`continue`). Separately, `PaperTrader.buy/sell` swallow every exception and return None, and the caller marks the journal row `skipped`, even when the exception was a timeout after the broker accepted the order.
- **Where:** `scheduler.py:926-935` (`pending_sell = True`), `994-996` (`if pending_sell: ... continue`), `_mark_skipped` 95-100 used at 917 and 1047; `src/paper_trader.py:403-405, 471-473` (`except Exception: return None`); `_client_order_id` (daily key, only a duplicate guard).
- **Evidence:** 2026-10-16 20:16 UTC (16:16 NY) the three sells (ids 14-16, CCI/CTSH/NNI) were submitted after the bell and later show `canceled` (cause of the cancel unverified; the first notification came 2026-10-18 22:30 after the container restart). A `skipped` row for an order that actually exists would never reach `get_pending_order_decisions`, so it is never reconciled; `get_stale_submitting_decisions` only covers rows stuck in `submitting`.
- **Why it matters:** rotation (sell then redeploy) takes at least two weeks; and an order the journal believes does not exist can sit at the broker, break position counts and alpha accounting.
- **Severity:** medium
- **Effort:** S (treat exception-after-submit as `submitting`, look up by `client_order_id` before marking skipped) / M (submit at the open, or run a second buy pass after Monday reconcile)
- **Unlocks / depends on:** F1 (buys must exist before this matters).
- **Confidence:** high (code), medium on practical frequency.

### F9. Truncated broker positions are accepted as truth (the 2026-09-16 snapshot)
- **What:** the code fails closed on broker errors but has no plausibility check on a successful but incomplete response. See Scenario B(c).
- **Where:** `src/accounts/alpaca.py:get_state/get_positions`, `scheduler.py:daily_snapshot` 379-405, `monday_maintenance` prune (1300-1305), `weekly_auto_trade` held-symbol set 830; `src/health.py` has no equity-continuity or position-count check.
- **Evidence:** snapshot id 79 (3 positions, equity 58,744) between ids 78 (8 positions, 105,555) and 80 (8 positions, 104,680). `decision_log` has no sell for the five missing names.
- **Why it matters:** a Friday repeat could double-buy held names (up to the ~25k gap) and mis-state the equity curve and `equity_dkk`; the same glitch on Monday prunes the mirror. This is exactly the "plausible wrong number" class the health module was written for.
- **Severity:** medium
- **Effort:** S (compare position set with last snapshot and with unreconciled/unfilled `decision_log` rows; refuse trading and alert on an unexplained disappearance or a >25% equity move without a journaled order)
- **Unlocks / depends on:** safer auto-trading once F1 is fixed.
- **Confidence:** high that the glitch happened and is undetected; its cause (Alpaca data) is unverified.

### F10. Job failures are log lines; 8 of 13 jobs leave no run record, and the health checks cover little of it
- **What:** every job ends `except Exception as e: logger.error(...)`; only the monthly briefing alerts. `run_log` is written by two jobs. Health checks assert on data freshness, not on "job X completed its last N runs".
- **Where:** `scheduler.py` job tails (e.g. 1077, 1603, 1658), `Database.start_run/complete_run` called only at 1389/1414/1511/1595 and in `bulk_load.py`; `src/health.py:check_scheduled_jobs` (Wed/Fri names only, uses `started_at`).
- **Evidence:** `weekly_auto_trade` aborts silently (`logger.error("Could not classify market regime - aborting trade cycle")`, empty watchlist, `get_state` failure) and no check asks whether it ran; `monday_maintenance`, `daily_news_monitor`, `weekly_screen`, `weekly_auto_trade`, `reconcile_broker_orders`, `daily_snapshot`, `monthly_*` write no run row. The health digest of 2026-10-18 reported "Weekly jobs running on schedule" while `run_log` 44/45 were incomplete rows.
- **Why it matters:** the digest can be green while the buy side, the news monitor or the refresh job is dead; this is how F1-F4 survived.
- **Severity:** high
- **Effort:** M (one `job_runs` table: job, started, finished, outcome, summary json; wrap every job in a decorator; health checks assert last-success age per job and fail on NULL `finished_at`)
- **Unlocks / depends on:** F7 catch-up, F13 cost tracking, F4 post-mortems.
- **Confidence:** high.

### F11. Health checks compare naive local time with UTC timestamps
- **What:** SQLite `CURRENT_TIMESTAMP` is UTC; the container runs `TZ=Europe/Copenhagen` and the health module subtracts `datetime.now()`.
- **Where:** `src/health.py:86` (`age = datetime.now() - latest`), `191`, `_parse_ts`; `haiku_screens.screened_at` is local-naive isoformat (`2026-10-14T07:02:16`) while `run_log.started_at` is UTC (`05:00:43`).
- **Evidence:** the 22:30 CEST digest said "Snapshot 3h old" for a snapshot taken 22:00 CEST (20:00 UTC); true age 0.5 h, reported 2.5 h.
- **Why it matters:** thresholds (36 h, 8 d, 9 d) are shifted by 1-2 h: harmless today, a trap for any tighter threshold; mixed timestamp conventions across tables complicate every join on time.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** nothing.
- **Confidence:** high.

### F12. Logging: stdout only, history lost on recreate, LOG_LEVEL ignored, root causes hidden at DEBUG
- **What:** `logging.basicConfig(level=INFO)` is hard-coded; there is no file or structured sink; the Docker `local` driver keeps 3 x 10 MB per container and is discarded when the container is recreated. Many failure paths log at DEBUG.
- **Where:** `scheduler.py:36`, `docker-compose.yml` (no logging section; `docker inspect` shows `local`, max-size 10m, max-file 3), `src/bubble_detector.py:312,325,357` (`logger.debug` on every fetch failure), `screener.py:526,602+`, `scheduler.py:247,1269` (`logger.debug`).
- **Evidence:** `docker logs buffett-bot-scheduler` has 318 lines starting 2026-10-18 12:48:28 (container created 10:48 UTC that day); `.env` contains `LOG_LEVEL` and `MAX_SECTOR_PCT` but grep over `src/` and `scripts/` finds no reader for either. The 10-01 to 10-07 failure (F4) and the Aug-Sep window (F3) left no log.
- **Why it matters:** every incident review since July has depended on DB side effects. Regime classification can run on one signal with the reason at DEBUG (F27).
- **Severity:** medium
- **Effort:** S (rotating file in `data/logs/`, honor LOG_LEVEL, promote swallowed fetch failures to WARNING with counts)
- **Unlocks / depends on:** F4, F10.
- **Confidence:** high.

### F13. Cost is tracked as constants, not usage, and only for two of five paid paths
- **What:** `run_log.total_cost_usd` is `n * 0.001` (Haiku) or `n * 0.025` (Sonnet); no code reads `response.usage`. News Haiku/Sonnet, the Friday trade Haiku screen, the briefing Sonnet and the Opus option record nothing.
- **Where:** `scheduler.py:41-42, 1414-1418, 1595-1599`; `grep "\.usage\|input_tokens" src scripts` hits only `scripts/replay_light_model.py`.
- **Evidence:** `run_log` costs are exact multiples (0.05 for 50 Haiku, 0.2 for 8 Sonnet, 0.125 for 5); the 2026-10-18 news run made 22 Haiku calls and has no cost row.
- **Why it matters:** real spend (Sonnet 5.5 deep prompts with 10-K text, thinking settings) is unknown, so the "cap = 10 calls" budget is a call count, not money; a prompt-size drift or a model price change is invisible.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** F10 job table; model A/B decisions (the light-model research note had to compute usage by hand).
- **Confidence:** high.

### F14. The universe is never refreshed; dead and renamed tickers are retried forever and 2.7% screen failures are invisible
- **What:** `universe_builder.build_universe` runs only from the manual `scripts/bulk_load.py`; no scheduled job syncs it and nothing ever sets `in_universe = 0`.
- **Where:** `scripts/bulk_load.py:step1_sync_universe`; `src/database.py` (only `in_universe = 1` writes, lines 791-801); `monday_maintenance`.
- **Evidence:** `universe`: 1,359 rows all `in_universe=1`, `last_screened` ranges 2026-04-07 to 2026-10-19; 277 tickers have no fundamentals since before 06-29; the 2026-10-19 run logged 37 yfinance errors (AL delisted; BK, CTRA, SATS, ASGN, IAC, KW, EXPI "Quote not found", most are live tickers, so these are transient 404s) as `Processed 1359 stocks, 37 errors`. S&P 500 additions/deletions and new listings never enter.
- **Why it matters:** the candidate pool is frozen at the April snapshot while failures are conflated with delistings; a held name that starts failing is not distinguishable from noise.
- **Severity:** medium
- **Effort:** M
- **Unlocks / depends on:** F2 (full-universe persistence) and a retry-with-backoff or failure-rate alert for yfinance.
- **Confidence:** high.

### F15. Two screening pipelines and a legacy JSON pipeline coexist; the Docker default CMD runs the legacy paid one
- **What:** the Friday `weekly_screen` (Finviz-based `universe.py`, JSON cache, `watchlist.json`) and the Monday DB screen (`universe_builder.py`, SQLite) are independent screens of overlapping names; trading uses the first, tiers come from the second (F1). `registry.py` (336), `portfolio.py` (792) and `run_monthly_briefing.py` (1,010) are imported only by the legacy runner and total 2,138 lines (9.4% of src+scripts) at 18-55% coverage. The Dockerfile `CMD` is `python -m scripts.run_monthly_briefing`.
- **Where:** `Dockerfile:last line`, `scripts/scheduler.py:147-200`, import graph (`grep` of `from src.registry|portfolio import` returns only `scripts/run_monthly_briefing.py`); `src/universe.py` (15% covered) vs `src/universe_builder.py`; `src/paper_trader.py` (JSON trade log at `data/trade_log.json`, non-atomic read-modify-write at 551-576) alongside `src/accounts/alpaca.py`.
- **Evidence:** the live data dir still holds `registry.json` (Jun 26), `watchlist_cache.json`, `watchlist_history.json`, `trade_log.json`. `docker run <image>` with no command would start the legacy pipeline, which calls Sonnet.
- **Why it matters:** duplicated logic drifts (F1 is the symptom); dead paths dilute coverage and review attention; a stray `docker compose run buffett-bot` costs money and uses a different state store.
- **Severity:** medium
- **Effort:** L to retire; S to change the Dockerfile CMD to the scheduler and mark legacy modules deprecated
- **Unlocks / depends on:** F1.
- **Confidence:** high.

### F16. CI publishes the `:latest` image even when the tests fail; the repo compose would auto-deploy it
- **What:** `docker-image.yml` and `lint.yml` are independent workflows; the image push on `main` does not depend on tests, lint or mypy. `docker-compose.yml` in the repo carries `com.centurylinklabs.watchtower.enable=true`, the live copy has `false` (`diff` of the two compose files).
- **Where:** `.github/workflows/docker-image.yml` (no `needs`/`workflow_run`), `.github/workflows/lint.yml`, `buffett-bot/docker-compose.yml:labels`.
- **Evidence:** `gh run list`: commit `feat(ruleset): v3.0.0 ...` (2026-10-17 16:47:46Z) run 39050963331 "Lint, Type, Test & Security" = **failure**, run 39050963345 "Build and Push Docker Image" = **success** (image tagged `:latest` and the sha). The next commits fixed it within 2 minutes.
- **Why it matters:** the live host is protected only by someone having disabled the label in a hand-edited compose file; the repo version would roll a failing build onto a trading bot (and, per F7, restart it mid-schedule). The sha tag exists, so rollback is possible, but the compose file pins `:latest`.
- **Severity:** medium
- **Effort:** S (`needs` via a combined workflow or branch protection requiring checks; pin the compose image to a digest/sha tag)
- **Unlocks / depends on:** safe auto-update.
- **Confidence:** high.

### F17. Dependencies are floors only, images are rebuilt from floating versions, dependabot backlog is 4 months old
- **What:** `requirements.txt` is all `>=`; no lock file or hashes; base image `python:3.11-slim` by tag; `gcc` stays in the runtime layer; each push rebuilds with whatever is newest.
- **Where:** `buffett-bot/requirements.txt`, `Dockerfile`, `.github/dependabot.yml`.
- **Evidence:** 9 open dependabot PRs, 7 from 2026-06-26 (pandas `>=3.0.3`, yfinance `>=1.4.1`, finvizfinance, 3 GitHub actions majors incl. checkout 4->7, setup-python 5->7), 2 from July. Because the specs are floors, merging them changes only the floor; the image already takes the newest at build time (local env: pandas 3.0.1, yfinance 1.2.0, anthropic 0.84.0, numpy 2.4.2, alpaca-py 0.43.2, schedule 1.2.2, on Python 3.12 vs 3.11 in image). The scheduler log shows a logger named `httpx2`, i.e. a newer HTTP stack than I have locally.
- **Why it matters:** a yfinance or pandas release can change `info`/DataFrame behavior between two Friday runs with no code change; the August to October outages show how little is known about what broke; no one can reproduce the running image's dependency set from the repo.
- **Severity:** medium
- **Effort:** S (generate a `requirements.lock` / `pip-compile --generate-hashes`, install from it, let dependabot bump the lock) 
- **Unlocks / depends on:** reproducible research runs.
- **Confidence:** high on the facts; I did not inspect the installed versions inside the live image (no exec).

### F18. Coverage is lowest where decisions are made
- **What:** overall 60% line+branch. The regime classifier that sets the target invested percentage, the screener internals, valuation methods, the broker adapter's order paths, notifications and the job registration are weakly or not exercised.
- **Where:** see the coverage table: `bubble_detector.py` 14% (170-260 and all three fetchers uncovered), `screener.py` 44% (`_fetch_historical_data`, trend metrics, sort/top_n), `valuation.py` 63% (DCF and Finnhub fetch 190-291), `paper_trader.py` 51% (portfolio summary, trade log, notify 482-610), `notifications.py` 33%, `scheduler.py` `weekly_screen`/`daily_watchlist_check`/`run_scheduler` untested, `journal_report.py` 0%.
- **Evidence:** `coverage report`: TOTAL 9,887 stmts, 3,717 missed; `grep "run_scheduler\|\.at(" tests/` returns nothing, so no test checks which jobs are registered, at what time or timezone, or which flags gate them.
- **Why it matters:** F3 (a silent switch change) and the timing logic (`at("16:15", "America/New_York")`) have no test; the 2026-07-06 NaN abort lived in `screen_tickers`, which is still 44% covered.
- **Severity:** medium
- **Effort:** M
- **Unlocks / depends on:** safe refactoring of `scheduler.py` and `database.py`.
- **Confidence:** high (measured).

### F19. Several regression tests assert a copy or a stub rather than the production path
- **What:** `tests/test_screener_sort.py::_sort` is documented as "Mirror of the production sort key": the NaN-abort bug test never executes `screen_tickers` (lines 1011-1036 are uncovered). Five test modules stub `schedule`, `dotenv` and `anthropic` with `sys.modules.setdefault(..., MagicMock())`, so what a later test sees depends on collection order, and the real `schedule` timezone logic is never run. `tests/test_scheduler.py` uses 251 `MagicMock`/`patch` uses.
- **Where:** `tests/test_screener_sort.py:46-60`; `tests/test_scheduler.py:37`, `test_analyzer.py:24`, `test_news_fetcher.py:36`, `test_ruleset_v3.py:26`, `test_weekly_cycle.py:39`.
- **Evidence:** coverage misses for `src/screener.py:1011-1036`; `anthropic 0.84.0` and `schedule 1.2.2` are installed locally yet replaced by mocks whenever the module is not already imported. `tests/fixtures_analysis.py` is a hand-written golden format, not a captured Sonnet 5.5 response (live `data/analyses/*.json` holds 193 real parsed outputs that no test reads).
- **Why it matters:** tests give false comfort for exactly the bug class found in July and October; a parser change against real model output would not be caught.
- **Severity:** medium
- **Effort:** S-M (extract the sort key to a module function and test it; replace global stubs with a fixture; add a handful of real, scrubbed model outputs as parser fixtures)
- **Unlocks / depends on:** F20.
- **Confidence:** high.

### F20. No safeguards aimed at silent-wrong-number bugs: no property, snapshot or end-to-end tests, no migration test on a real old DB
- **What:** `requirements-dev.txt` has no `hypothesis`/snapshot library; the migration test builds a synthetic legacy schema; there is no end-to-end test that runs a job against a real SQLite file with stubbed external APIs and asserts on the resulting rows for all jobs; there are no invariants on persisted numbers (rating in set, scale of `debt_equity`, equity vs cash+positions).
- **Where:** `tests/test_database.py:400-430` (`_legacy_db` synthetic), `tests/test_weekly_cycle.py` (one scenario file), `requirements-dev.txt`.
- **Evidence:** the 09-16 snapshot (F9) satisfied every runtime check; `gross_equity` vs `cash + sum(mv)` is satisfied by all 111 snapshots, i.e. an easy invariant exists but is not asserted. The 2026-10-01 bug family (ratings, D/E scale, PIT dedupe) were all numerically plausible.
- **Why it matters:** the test suite cannot see "plausible but wrong" outputs; production DB is the only fixture that does.
- **Severity:** medium
- **Effort:** M
- **Unlocks / depends on:** a sanitized copy of the live DB as a read-only test fixture; F19.
- **Confidence:** medium (absence verified by grep of tests and dev requirements; effort estimate is a judgement).

### F21. Secrets handling: hashed real secrets committed, unverified SMTP TLS, broad local agent permissions
- **What:** (a) `.secrets.baseline` (committed 2026-02-10 in `68c237b`) lists 9 findings for `buffett-bot/.env` with 40-char SHA-1 `hashed_secret` values, i.e. hashes of lines of a real `.env` (lines 10, 14, 18, 26, 27, 41; the 2026-10-01 audit recorded these as real secrets, not independently re-checked here); (b) `smtplib.SMTP(...).starttls()` is called with no SSL context, so Python's default does not verify the server certificate before `login(smtp_user, smtp_password)`; (c) `.claude/settings.local.json` (untracked) allows `Bash(docker exec *)`, `Bash(git push *)`, `Bash(gh pr *)`.
- **Where:** `/.secrets.baseline`; `src/notifications.py:88-90,115-117`; `/.claude/settings.local.json`.
- **Evidence:** (a) the baseline has no live file behind it in git (`.env` is ignored and `git log --all -- buffett-bot/.env` is empty), but the hashes are public; keys that are high-entropy are not practically recoverable from SHA-1, but a weak password (SMTP) would be. CI's `detect-secrets scan` runs without the baseline, which is correct, and tokens in URLs are redacted from logs (Finnhub/Telegram log only `type(exc)`). (c) The scheduler container holds the Alpaca and Anthropic keys and can place orders; `docker exec *` and an unconditional `git push` (which triggers F16's image push) are wider than the audit/dev flow needs.
- **Why it matters:** low-probability but cheap to close; (b) allows a network attacker on the path to the SMTP host to capture the app password.
- **Severity:** low-medium
- **Effort:** S
- **Unlocks / depends on:** rotate any key that was ever in those `.env` lines if the SMTP value was weak (unverified which lines map to which secret).
- **Confidence:** medium (I did not and must not decode the hashes; mapping of lines to secrets is unverified).

### F22. Third-party text reaches prompts with unescaped tag fencing; one Sonnet verdict can force a full sell
- **What:** news and filing text is wrapped in `<news>`/`<filing>` tags and the system prompt says to treat it as evidence, but the text is not escaped, so a headline containing `</news>` can close the fence. Under ruleset 3.0.0, `check_thesis_breakers` (a single Sonnet call) returning a breaker number is the only event that forces a sell of a held name at the next Friday run; there is no second source, confirmation call or human step.
- **Where:** `src/analyzer.py:939-945, 1012-1016` (prompt assembly), `src/news_fetcher.py:format_news_for_llm` 152-181 (headline + 200-char summary, no escaping), `analyzer.check_thesis_breakers` 985-1040, `news_fetcher.py:run_news_pipeline` (fires, `mark_thesis_breaker_fired`, tier C), `scheduler.py:weekly_auto_trade` 868-890.
- **Evidence:** the output parsers are strict regexes (`^BREAKER FIRED:\s*(NONE|\d+)\s*$`) which limits format abuse, but the semantic decision stays with the model reading attacker-influenced Finnhub text; the commit `d922f88` added the fencing text only.
- **Why it matters:** a single poisoned or merely misleading headline is a one-shot path from the open internet to a full-position sell and a permanent C-tier row (C verdicts now expire in 30 days). Cost is paper money today, a bad habit for the ASK account.
- **Severity:** medium
- **Effort:** S (escape or strip closing tags and control sequences; log the quoted evidence) / M (two-call confirmation or require the fired breaker to match across two independent items)
- **Unlocks / depends on:** the real ASK account going live.
- **Confidence:** medium (mechanism verified in code; no exploit attempted).

### F23. Opening a `Database` writes to the live DB, and the default path is CWD-relative
- **What:** `Database.__init__` runs the schema script, column migrations, index script, `INSERT OR IGNORE` and an `UPDATE budget_caps SET max_calls=...` every time it is constructed (every job and every manual script, including read-only reporting).
- **Where:** `src/database.py:_init_schema` 531-560, `src/config.py:database_path` (`./data/buffett_bot_v2.db`), `scheduler.py` (each job constructs its own `Database()`).
- **Evidence:** any script from an older branch run against the live DB re-syncs `max_calls` to its own `BUDGET_CAPS_DEFAULTS` and takes a write lock (F5). Repo `data/` contains an empty `buffett.db` (0 bytes) and a stray 118,784-byte `buffett_bot_v2.db` from a run in the wrong directory; the live `data/` is root-relative only because Docker sets `WORKDIR /app`.
- **Why it matters:** read-only tooling (including this audit) cannot use the class; running a manual script from the wrong directory silently creates a second empty database.
- **Severity:** low
- **Effort:** S (`readonly=True` constructor; resolve the path from an absolute env var and fail if the file is missing)
- **Unlocks / depends on:** safe concurrent manual analysis.
- **Confidence:** high.

### F24. Backup is a live-file `rclone sync` with no consistency step or restore test
- **What:** the nightly B2 job (`/opt/docker-apps/scripts/backup-to-b2.sh`, 03:00 host time) syncs the whole `/opt/docker-apps/buffett-bot` directory, including the SQLite file, `-wal`, `-shm` and `.env`, with plain `rclone sync`. There is no `sqlite3 .backup`/`VACUUM INTO` snapshot and no integrity check of the uploaded copy. The pre-migration copies in `data/backups/` (62 MB) and `*.bak-*` are manual.
- **Where:** `backup-to-b2.sh:255` (`sync_path "$BUFFETT_BOT" buffett-bot 'BuffettBot'`), `sync_path` at line 47.
- **Evidence:** log shows `BuffettBot sync complete` on 2026-10-18 and 10-19 (about 4 min each for ~190 MB). The 03:00 slot is idle for the bot, so a torn copy is unlikely today; the 180 MB file is 97% `pit_fundamentals` (167 MB of 172 MB pages, dbstat), which is re-derivable from EDGAR but is uploaded nightly. `PRAGMA integrity_check` on the live file: ok.
- **Why it matters:** the DB holds the only copies of the decision journal, tier history, snapshots and cohort tables; a mid-write copy of a WAL database is not guaranteed restorable. A restore has never been exercised (unverified).
- **Severity:** low-medium
- **Effort:** S
- **Unlocks / depends on:** excluding `pit_fundamentals` from the nightly (separate monthly artifact).
- **Confidence:** medium (script read, B2 versioning/retention not inspected).

### F25. Schema evolution is additive-only with no version; retention eventually deletes the only local point-in-time history
- **What:** `PRAGMA user_version = 0`; `_COLUMN_MIGRATIONS` can only add columns; index changes ride on `IF NOT EXISTS`; there is no record of which migrations ran. `run_retention_cleanup` deletes non-first-of-month `fundamentals` rows older than 2 years and `news_events` older than 2 years.
- **Where:** `src/database.py:435-444, 2402-2440`.
- **Evidence:** `pragma user_version` returns 0; the retention job logs `deleted 0 ...` today because the oldest row is 2026-04-07. `fundamentals` (yfinance snapshots, taken since 2026-04) is the only locally captured point-in-time fundamentals series besides the EDGAR `pit_fundamentals`; from April 2028 it starts thinning to monthly.
- **Why it matters:** a destructive or type-changing migration has no framework; the retention window will silently cut the history a future backtest would want. Query shapes are mostly index-backed (checked `EXPLAIN` for fundamentals, snapshots, pit); the remaining scans (`news_events` by headline, `decision_log` by status) are tiny.
- **Severity:** low
- **Effort:** S (use `user_version`, numbered migrations) / S (archive instead of delete)
- **Unlocks / depends on:** nothing urgent.
- **Confidence:** high.

### F26. Valuation degrades silently: no minimum number of estimates, source mix not stored, FCF definition switches
- **What:** `average_fair_value` is a confidence-weighted mean of whatever estimates resolved, even one; the tier stores the resulting number but not which sources or how many produced it; `real_fcf_yield` falls back to plain `fcf_yield` when the cash-flow statement is empty.
- **Where:** `src/valuation.py:49-62` (`average_fair_value`), `120-169` (`get_valuation`); `src/tier_engine.py` (no reference to `estimates`); `src/screener.py:496-505` (fallback) and `529-605`.
- **Evidence:** `grep "estimates\|len(" src/tier_engine.py` shows no source-count guard; `deep_analyses` has no column for it. Yahoo `.info` is the single point of failure for all five methods; a partial `.info` yields a different mean week to week for the same company.
- **Why it matters:** margin of safety, tier and the Friday buy gap move for data-availability reasons rather than business reasons, and nobody can tell afterwards (tier flaps would look like analysis changes).
- **Severity:** medium
- **Effort:** S
- **Unlocks / depends on:** a "valuation confidence" field that `plan_buys` can use as a weight.
- **Confidence:** high (code), flap frequency unmeasured.

### F27. Regime classification runs on whatever signals arrive and ignores its own confidence
- **What:** `classify_market_regime` votes from up to four yfinance signals with all failures swallowed at DEBUG. The trade job aborts only when the signal list is empty; confidence (`low` when votes disagree) is ignored by `target_invested_pct`. A drawdown between 0 and -10% casts no vote.
- **Where:** `src/bubble_detector.py:158-262, 302-363`; `scheduler.py:weekly_auto_trade` regime gate (~745-760).
- **Evidence:** code: `elif drawdown >= 0` is the only non-correction branch (so "near the high" means exactly at the max close); one surviving signal can set `euphoria` (65% target) or `crisis` (100%). `regime_log` (111 rows) always has both P/E and VIX populated and `confidence = moderate`, so no degraded day has occurred yet, but the code path is 14% covered.
- **Why it matters:** the invested-percent target is the main sizing lever; a yfinance hiccup on a Friday could swing it by 25-35 points with nothing in the logs.
- **Severity:** medium
- **Effort:** S
- **Unlocks / depends on:** F1.
- **Confidence:** high (code).

### F28. Daily watchlist check is a log-only feature with a misleading message
- **What:** `daily_watchlist_check` loops `stocks[:10]` but logs `Checking {len(stocks)} stocks`; matches (>30% margin) are only logged, never notified; it reads the same `watchlist.json` (F1).
- **Where:** `scheduler.py:203-260`.
- **Evidence:** log 2026-10-19 08:00: "Checking 25 stocks..." followed by 7 alert lines at INFO (IBKR, LULU, DECK, CRUS, LDOS, AX, AA) and no notification; the function is uncovered by tests (210-260).
- **Why it matters:** an alert nobody receives; mild confusion over scope.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** F1.
- **Confidence:** high.

### F29. Journal timestamps are reconcile time, not broker fill time
- **What:** `update_order_status` stamps `filled_at = CURRENT_TIMESTAMP` when reconciliation runs; `reconcile_broker_orders` uses `date.today()` as the exit date for the alpha window, and `decided_at` (Friday 22:15 CEST) as the entry date although the order fills at Monday's open.
- **Where:** `src/database.py:1741`, `scheduler.py:reconcile_broker_orders` 577-606, `weekly_auto_trade` sell journaling (950-975); `src/benchmark.py:fetch_benchmark_return`.
- **Evidence:** all 10 pre-v3 buys in `decision_log` are `legacy` with `shares NULL` and no fill price (ids 2-13); the three v3-era rows (14-16) have no fill data because they were canceled. `closed_trades` has 2 rows.
- **Why it matters:** hold periods and alpha are mis-dated by the weekend plus any reconcile delay (the 2026-10-18 reconcile reported orders from 10-16); small per trade, but the cohort/alpha validation compounds many of them.
- **Severity:** low
- **Effort:** S (use `order.filled_at` from Alpaca for both fields)
- **Unlocks / depends on:** a longer trade history (n is tiny today).
- **Confidence:** high.

### F30. Config: import-time evaluation, no live kill switch, env drift
- **What:** `Config` reads and parses env vars in class-body defaults at import (`int(os.getenv(...))`), so a malformed value crashes the process before any logging and `unless-stopped` loops it. Kill switches are read once at start; flipping `.env` requires container recreation. Boolean parsing accepts only the exact string `true` for the opt-in flags (`1`/`yes` mean off) but `HEALTH_CHECKS_ENABLED`/`QUARANTINE_ALERTS_ENABLED` use the opposite polarity (`!= "false"`).
- **Where:** `src/config.py:30-144`; `.env.example`.
- **Evidence:** 11 tunables read by code are absent from `.env.example` (`ANALYSIS_MAX_AGE_DAYS`, `C_TIER_ANALYSIS_DAYS`, five `DEPLOY_TARGET_*`, `HAIKU_BATCH_SIZE`, `HAIKU_MIN_SCORE`, `MIN_TRADE_USD`, `QUALITY_CEILING_PCT`); `LOG_LEVEL` and `MAX_SECTOR_PCT` are in the live `.env` but read by nothing; the live `.env` pins `ASK_CONTRIBUTION_LIMIT` to the old figure, so commit `b04ea84` (new 174,200 default) has no effect in production (impact limited to legacy `portfolio.py`, its only reader).
- **Why it matters:** F3 was a config-default change that the deployment did not pick up; the same drift can recur for any new switch.
- **Severity:** low-medium
- **Effort:** S
- **Unlocks / depends on:** a startup diff of declared vs present keys (F3).
- **Confidence:** high.

### F31. News pipeline: Finnhub outages look like "no news", the budget is spent before the call, and only one headline is recorded per ticker per day
- **What:** `get_news_for_tickers` returns `[]` for any HTTP/parse error, indistinguishable from no news; `check_news_pipeline_effectiveness` returns OK when there are no events. `can_spend` increments before the Haiku call, so failed calls burn budget (F4). Only `top_item` is logged to `news_events`, so the other fresh items in the same batch are re-evaluated on later days (each day the next headline becomes `material[0]`).
- **Where:** `src/news_fetcher.py:286-310` (fetch), `run_news_pipeline` 380-603 (esp. 450-520), `src/health.py:check_news_pipeline_effectiveness`, `database.can_spend` 570-605.
- **Evidence:** 2026-10-18 20:03 log: `weekly_news_haiku pace floor reached` for the remaining S/A/B names (META, MSFT, V, MA, PG, COST and others; the 22 Haiku calls were spent on earlier tickers in the queue), and `Haiku flags red_flags=True` for PEP with `weekly_news_sonnet budget exhausted — skipping breaker check`; event rows for PEP and CLX stay `sonnet_triggered NULL` and retry only after the Monday reset. All three Haiku/news caps were exhausted by Saturday. Pacing floor and holdings-first ordering do work as designed (AGM, AIZ, CPRT, DECK first).
- **Why it matters:** a Finnhub key expiry would produce a green health digest forever; a red-flag item on a held name can wait up to a week for its breaker check.
- **Severity:** medium
- **Effort:** S (distinguish transport errors from empty results and count them; refund on failed calls; log every headline)
- **Unlocks / depends on:** F10.
- **Confidence:** high on code, medium on the weekly exhaustion pattern (one week of data).

### F32. Order-of-operations waste and weak idempotency in the Friday job
- **What:** the Friday trade job pays for up to 20 Haiku screens before checking tiers (F1), reruns `reconcile_broker_orders()` at its start (fine) and has no per-cycle idempotency key: a manual re-run later the same UTC day is blocked only by the broker's duplicate `client_order_id` and the open-order check, leaving `skipped` journal rows; a re-run after 00:00 UTC bypasses the id guard. `weekly_screen` and the Monday screen both pass through yfinance for ~1,000+ names with the 24 h file cache (cache from Monday is expired by Friday).
- **Where:** `scheduler.py:788-815`, `paper_trader.py:_client_order_id` 578-581, `scheduler.py:weekly_screen`.
- **Evidence:** code; `data/cache` holds 1,627 JSON files; the Friday screen log is not available (logs lost).
- **Why it matters:** modest cost today, but it is the main path where a manual retry after a partial failure could double-submit.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** F8.
- **Confidence:** medium.

## Improvement ideas not tied to a weakness

### F33. One `job_runs` table plus decorator as the spine of observability
- **What:** each job writes start, finish, outcome, counts, real token usage and cost, and an error string; the health module asserts last-success age per job and the Discord digest shows a 7-day matrix.
- **Where:** new `src/jobs.py`, `scripts/scheduler.py` (13 jobs), `src/health.py`.
- **Evidence:** F3, F4, F7, F10, F13 each need this table.
- **Why it matters:** turns today's DB-inference health checks into direct assertions.
- **Severity:** n/a
- **Effort:** M
- **Unlocks / depends on:** startup catch-up (F7), spend reporting (F13); no prerequisites.
- **Confidence:** high.

### F34. Read-only replay harness against a copy of the live DB
- **What:** a script that copies the live DB, stubs the broker and LLM with recorded responses, runs all 13 job functions in order and asserts invariants (cash+positions=equity, every tier has a price alert, every held name has a deep analysis, no orphan `run_log` rows).
- **Where:** `tests/` (new `test_replay_live_copy.py`, skipped unless a copy path is provided).
- **Evidence:** F9, F19, F20.
- **Why it matters:** the only fixture that has ever caught the silent-wrong-number class is the production DB.
- **Severity:** n/a
- **Effort:** M
- **Unlocks / depends on:** F23 (read-only open).
- **Confidence:** medium.

### F35. Split `database.py` (2,770 lines, one class, 87 `def`s) and `scheduler.py` (1,766 lines)
- **What:** carve `database.py` by domain (budget, journal, snapshots, universe/analyses, PIT) and move each job to `src/jobs/<name>.py` with a thin schedule file; the scheduler file currently mixes registration, 13 job bodies, helper prompts and the legacy JSON paths.
- **Where:** `src/database.py`, `scripts/scheduler.py`.
- **Evidence:** file sizes; `scheduler.py` is 69% covered but because the bodies are monoliths (the 150-line Friday job has 12 `except Exception`).
- **Why it matters:** the largest recent bugs lived in 100-300 line job bodies with nested broad excepts; smaller units make per-step failure handling and tests feasible. Do after F10 so behavior is pinned.
- **Severity:** n/a
- **Effort:** L
- **Unlocks / depends on:** F10, F18.
- **Confidence:** medium.

### F36. Money-flow ledger for dividends and cash
- **What:** `income_events` has 0 rows and Alpaca cash has been identical for 86 days despite holding dividend payers (e.g. CCI, AIZ, NNI); either paper dividends are not credited (unverified) or they are not observed. A cash-reconciliation (snapshot cash delta vs journaled fills) would expose either case and feed the DKK/ASK tax-drag analysis.
- **Where:** `src/database.py:income_events`, `daily_snapshot`.
- **Evidence:** `portfolio_snapshots.cash = 30880.39` in 87 consecutive rows; `income_events` row count 0.
- **Why it matters:** total-return and after-tax comparisons for a Danish ASK investor need dividend flow.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** F9-style reconciliation; a decision on how to model Danish dividend taxation.
- **Confidence:** low (cause unverified).

## What I could not verify

- Why the Sep 16 Alpaca response was truncated, and whether Alpaca paper credits dividends (F9, F36).
- The cause of the 2026-10-01 to 10-07 LLM failure and of the missing monthly briefing on Aug 1, Sep 1 and Oct 1: container logs before 2026-10-18 12:48 are gone and no other log sink exists.
- Whether any health WARN/FAIL was delivered to Discord during the August blackout (alert-state rows: only 1 exists in `alert_state`; I did not inspect it beyond count).
- Why the three 2026-10-16 sells were canceled (by the operator, by Alpaca for after-hours DAY orders, or otherwise).
- How `fundamentals` held about 1,070 rows per week through 2026-06-29 when `top_n=100` already existed in the code (F2).
- Installed dependency versions inside the live image (no exec allowed); local versions are listed in F17 only as a proxy. Peak memory of the Monday job against the 256 MB limit (only idle usage, 42 MiB, was sampled).
- Behavior of `schedule` 1.2.2 across the US/EU DST gaps (2026-10-25 to 11-01 and March): I confirmed the next-run computation for `friday.at("16:15","America/New_York")` (Fri 22:15 CEST) and `day.at("16:30","America/New_York")` (22:30 CEST) from a local process, not the transition weeks. Note that the fixed 22:00 CEST snapshot is 16:00:15 ET, i.e. 15 seconds after the close, and 17:00 ET during the five-hour weeks.
- B2 retention/versioning and any restore drill for the backup (F24).
- Whether the single-ticker network allowances were needed: none were used. Two read-only `gh` calls (run list, PR list) and read-only `docker logs/inspect/stats` were used. I also printed one non-secret numeric `.env` setting (`ASK_CONTRIBUTION_LIMIT`) while checking config drift; no secret values were printed.
