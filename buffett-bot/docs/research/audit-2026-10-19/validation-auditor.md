# Validation audit: can BuffetBot prove it has an edge? (2026-10-19)

Auditor: validation-auditor (read-only). Repo main @ efc181b. Live DB opened `mode=ro` from `python3 -I`. No LLM calls.
One external request: a single Yahoo chart-API pull of SPY daily closes and adjusted closes for 2026-06-23 to 2026-10-16. The benchmark cache, `data/benchmark/SPY_benchmark.json`, holds one quote from 2026-07-02 and is not enough. yfinance is not installed on the host, so the request went to the same endpoint yfinance uses. Analysis scripts are in the session scratchpad and are not committed.

## Verdict in one paragraph

**Unproven, and for now unprovable.** The paper account is up +5.77% against SPY's +2.60% total return over 2026-07-01 to 2026-10-16. That is a t-statistic of 0.33. One stock (CTSH, +$8,014 unrealised) earned more than the whole gain (+$5,800). Without CTSH the book lost 2.2%. At a realistic tracking error for 8 stocks (10–12%), detecting 3 pp/yr of alpha at 5%/80% power takes **87–126 years** of paper trading. The quant screen's point-in-time backtest shows no rank power once a split look-ahead bug is removed (pooled Spearman +0.018 falls to +0.003). The ruleset 3.0.0 rule "moat NONE never buyable" rests on a cohort in which **none of the 22 names was really rated NONE**. Twenty are legacy registry rows that the migration code defaulted to NONE; their source files say moat "strong" or "moderate". The other two were rated on the measurement day and have zero-length windows. The ruleset version stamp, meant to segment the history, appears on **0 of 16** decisions and **0 of 293** tier changes. The 3.0.0 "experiment" has no decisions yet and inherits the whole 2.0 book.

## Evidence inventory (computed 2026-10-19)

| Source | Rows | Usable for validation? |
|---|---|---|
| `portfolio_snapshots` | 111 (75 trading days used after removing the 09-16 bad row, weekend duplicates and the 07-25 double row) | Yes, as a single 3.5-month curve. No ruleset tag. Pre-07-25 rows were rewritten by `scripts/backfill_quarantine_snapshots.py` |
| `decision_log` | 16. 10 buys and 3 sells have status `legacy`; 3 sells were `canceled` on 2026-10-16 | Partly. 2 buys are phantoms, 7 of 10 buys have tier NULL, 0 of 16 rows are ruleset-stamped |
| `closed_trades` | 2 (AD, GIS), both from the legacy era | No. n=2, both have tier `?`, and AD's economics are wrong (F5) |
| `paper_positions` | 7 | No. `entry_date`, `entry_price` and `cost_basis` are NULL on all rows, and `tier_at_entry` is not the entry tier (F6) |
| `cohort_validation` | 18 rows from one run (as_of 2026-10-16) | Mislabelled. The NONE and LOW cohorts are artifacts (F13, F14) |
| `tier_history` | 293, all `ruleset_version` NULL | Only by date |
| `regime_log` | 111: fair_value 101, overvalued 10 | No regime variation to attribute anything to |
| `data/backtest/` | `pit_backtest.json` (10,559 obs), `cohort_validation_2026-10-16.json` (185 names) | Yes, with the biases in F20–F23 |
| Mode-2 watchlist snapshots | 0 files | Never produced (F24) |
| `data/trade_log.json` | 19 orders, 2026-05-01..10-16 | Shows the account traded before July (ATEX, ARCO), outside the journal |
| `income_events`, `single_buy_recommendations`, `thesis_breaker_events` | 0, 0, 0 | Nothing to measure |

### Paper equity curve vs SPY (2026-07-01 close → 2026-10-16 close)

| Series | Start | End | Return |
|---|---:|---:|---:|
| Equity, net of quarantined AL (`equity`) | $100,476 | $106,276 | **+5.77%** |
| Gross equity (Alpaca figure, AL at $65.00) | $107,718 | $113,518 | +5.38% |
| Equity in DKK (`equity_dkk`) | 659,213 | 708,352 | +7.45% |
| SPY price | 745.76 | 763.25 | +2.35% |
| SPY total return (adjusted close) | 743.91 | 763.25 | **+2.60%** |
| SPY scaled to the bot's daily invested % (avg 65.2%) | | | +1.76% |
| SPY TR in DKK (implied USDDKK +1.59%) | | | ≈ +4.23% |

Daily-return statistics (74 returns; 09-16 dropped and 09-15→09-17 chained):

- Portfolio volatility 14.7%/yr; SPY 10.8%/yr.
- Beta 0.04 and correlation 0.03 against SPY. A one-day-lagged SPY does not change this (corr 0.07). Seventy-four days cannot estimate beta.
- Active return vs SPY: +10.9%/yr annualised. Tracking error 17.9%. t = 0.33.
- Active return vs exposure-matched SPY: +14.0%/yr. Tracking error 16.1%. t = 0.47.
- Regression alpha: 19.8%/yr, se 27%/yr, t = 0.72.
- Maximum drawdown −7.7%. Including the bad 09-16 row it would read −45.7%.
- Attribution: CTSH's unrealised gain of $8,014 at 10-16 is larger than the whole $5,800 gain. Over 07-24→10-16, with all eight names held, CTSH made +$6,762 of the +$6,143 total. DECK −$1,046, NNI −$392 and AGM −$210 lost money.

---

## Findings

### F1. The paper track record shows outperformance, but the result is statistically empty
- **What:** +5.77% against SPY's +2.60% total return is +3.2 pp over 0.29 years, with t = 0.33 against SPY and t = 0.47 exposure-matched. One name accounts for more than the whole gain.
- **Where:** `portfolio_snapshots` (live DB); calculation in this report.
- **Evidence:** "active vs SPY: mean ann 10.9% TE 17.9% IR 0.61 t=0.33". CTSH `unrealized_pl` is 8014 on 2026-10-16 against a total equity change of +5,800. Ex-CTSH: −$2,214 (−2.2%) while SPY made +2.6%.
- **Why it matters:** Anyone reading "+5.8% vs +2.6%" will see an edge. The window cannot tell skill from one lucky name. CTSH was rated A, so the result is consistent with the A-cohort being noise as well (F16). The book is only ~65% invested, so the raw comparison also mixes in a market-timing effect.
- **Severity:** high, because the risk is the wrong conclusion being drawn, not wrong code.
- **Effort:** S to report honestly; L to resolve (time).
- **Unlocks / depends on:** Honest reporting now. A real verdict needs years (F11).
- **Confidence:** high. Computed from snapshots and SPY adjusted closes.

### F2. The 2026-09-16 snapshot is a broker-side partial read that the bot accepted without checks
- **What:** Alpaca returned only 3 of 8 positions (AGM, CPRT, DECK). Its own account equity and buying power were degraded to match. The bot stored the row because no plausibility check exists.
- **Where:** `scripts/scheduler.py:daily_snapshot` (l.354); `src/accounts/alpaca.py:get_state` (l.35–80). `src/health.py` has freshness, frozen-mark and quarantine checks but no equity-jump or position-count check.
- **Evidence:** Row 79: `equity 58743.6, gross_equity 58743.6, untradable_value 0.0, buying_power 201538.56`, versus 332,609 the day before and 330,160 the day after. Equity = cash 30,880.39 + 27,863 for the three positions. AIZ, AL, CCI, CTSH and NNI are missing, then reappear on 09-17. The 50% quarantine guard (`_MAX_QUARANTINE_FRACTION`) cannot catch missing positions.
- **Other snapshots:** Trustworthy for levels. Cash moves only on trade days, and the cash arithmetic reconciles to the order notionals within cents (F4, F5). Caveats: 32 weekend/holiday rows repeat Friday's equity; 07-25 has two rows (20:00 and 21:44); rows before 2026-07-25 were rewritten by `scripts/backfill_quarantine_snapshots.py`, so the table is not append-only in practice. The original values survive in `gross_equity`.
- **Why it matters:** A naive drawdown or volatility read of this table reports a −45.7% crash. If a Friday auto-trade had hit such a read, sizing would have run on half the true equity. The row cannot be excluded without a flag.
- **Severity:** medium.
- **Effort:** S (reject or flag a snapshot when equity moves more than 15% with no fills, or when the position count drops).
- **Unlocks / depends on:** Any equity-curve statistic.
- **Confidence:** high on what was stored. The Alpaca root cause is unverified.

### F3. Paper economics differ systematically from what a Danish ASK investor would get
- **What:** The paper account has no dividends, no commissions, no FX conversion or spread, and fractional notional fills. A cash takeover was written off instead of turning into cash.
- **Where:** `src/paper_trader.py`, `src/accounts/alpaca.py:get_state`, `income_events` (0 rows).
- **Evidence:**
  - Cash is constant at $30,880.39–$30,880.40 from 07-24 to 10-18 (86 days) while the book holds dividend payers (CCI, AIZ, CTSH, AGM, NNI).
  - AL is marked at exactly $65.0000/share ($7,242.03 / 111.4158 sh) with `asset_status inactive`. That is consistent with a completed cash acquisition. The bot subtracts it from equity as "untradable" instead of treating it as cash, which understates equity by 6.4%.
  - Buys are notional: CTSH $13,681.7547 bought 328.572046 sh.
- **Why it matters:** The paper curve understates income by the portfolio's dividend yield and overstates trading results by the costs and minimum lots a real Nordnet/ASK account pays. The `MIN_LOT_DKK 5000` lot sizing and the single-buy path are not what is being paper-tested. The curve does not test the strategy the user actually runs (one DKK lot at a time).
- **Severity:** medium.
- **Effort:** M (shadow-ledger dividends from yfinance; apply a cost model; treat merger cash as cash).
- **Unlocks / depends on:** An after-cost, DKK, ASK-relevant comparison (F26).
- **Confidence:** high on the observations. Medium on the AL takeover interpretation (inferred from the exact $65.00 mark).

### F4. The journal cannot reconstruct the book: phantom buys and untracked positions
- **What:** Two journalled buys never filled. Two held positions were never journalled. One realised exit is missing from `closed_trades`.
- **Where:** `decision_log` ids 2–3; `scripts/scheduler.py:weekly_auto_trade` (legacy path); `Database.get_open_buy` (treats `legacy` as filled).
- **Evidence:**
  - ACVA and AESI buys, $8,886.76 each on 07-02 07:10, appear in no snapshot.
  - The cash change on 07-02 is −3,781.84. That equals AD buy −8,886.76 plus ACT proceeds +5,104.92, with nothing left over for $17.8k of ACVA/AESI.
  - AGM and AL are held from the first snapshot but have no `decision_log` row.
  - ACT was sold on 07-02 at about $45.37 (avg cost 42.66), but there is no `closed_trades` row because no buy exists to match.
  - `get_open_buy` matches `order_status IN ('filled','legacy')`, so ACVA and AESI stay "open" in the journal forever.
- **Why it matters:** Attribution and per-trade alpha need a journal that equals the broker. Right now 4 of 12 names that touched the book are wrong or missing.
- **Severity:** high.
- **Effort:** S (mark ids 2–3 as unfilled; add synthetic entry rows for AGM/AL flagged `pre_journal`; add a nightly journal-vs-broker reconciliation check).
- **Unlocks / depends on:** F6, F9, any trade-level statistic.
- **Confidence:** high (cash arithmetic).

### F5. The legacy closed-trade numbers are wrong: decision-time prices and dates instead of fills
- **What:** AD's exit price and date are the Friday decision's quote and date. The order actually filled Monday at the open.
- **Where:** `closed_trades` id 1; `Database.close_trade` (`exit_date = today`); `src/benchmark.py:fetch_benchmark_return`.
- **Evidence:**
  - Recorded exit 36.24 on 2026-07-03, a market holiday.
  - Cash on 07-06 fell 33,478.25 while the four buys cost 41,850.07. AD's proceeds were therefore 8,371.82, or **$35.44/share**.
  - Realised P&L is −5.79% (−$514.93), not the recorded −3.67% (−$325.99).
  - `benchmark_return 0.0` because both window ends resolve to the 07-02 close. SPY 07-02→07-06 was +0.87%, so alpha is about −6.7%, not −3.67%.
- **Why it matters:** Both closed trades in the track record are legacy-era. One of them misstates alpha by about 3 pp. The new reconcile path (`reconcile_broker_orders`) has **never closed a trade in production**: every buy is `legacy`, so the corrected path is unexercised.
- **Severity:** medium.
- **Effort:** S.
- **Unlocks / depends on:** Trustworthy `closed_trades`.
- **Confidence:** high, assuming notional buys filled at exactly their notional, which the 07-24 day confirms to the cent: 48,552.46 − 30,880.40 = 17,672.06 vs 17,672.08.

### F6. Tier attribution is impossible for the existing history
- **What:** The tier at the moment of buying is NULL on 7 of 10 buys. The `tier_at_entry` fields that do exist are not entry tiers.
- **Where:** `decision_log.tier`; `scripts/scheduler.py:1290–1295` (`tier_at_entry = da.get("tier","C") if da else "C"`, kept by `COALESCE`); `Database.tier_performance`.
- **Evidence:**
  - Buys ACVA, AESI, AD, AIZ, GIS, CCI and NNI have tier NULL. Their snapshots show `quality_score` 15–44 for ACVA/AESI/AD: they were bought with no deep analysis at all.
  - `paper_positions.tier_at_entry` for NNI is `C`. That is the default for "no analysis", not a C rating at entry.
  - AGM's `B` comes from its first sync after an untracked pre-July buy.
  - `tier_performance` groups both closed trades under `?`.
- **Why it matters:** The core hypothesis ("A/S tiers beat B/C") cannot be tested from the trades. Several holdings were bought on quant valuation alone, so "LLM vs quant" attribution is confounded from the start.
- **Severity:** high.
- **Effort:** S going forward (write the entry tier into the position mirror from the buy row). The past cannot be fixed.
- **Unlocks / depends on:** Per-tier attribution.
- **Confidence:** high.

### F7. The per-trade alpha window and return definition do not match the trade
- **What:** The benchmark runs from the close of the buy-decision date to the close of the reconcile date. The stock runs from next-open fill to exit fill. SPY is total return; the stock gets no dividends.
- **Where:** `scripts/scheduler.py:reconcile_broker_orders` (`fetch_benchmark_return(open_buy["decided_at"], date.today())`); `src/benchmark.py:fetch_benchmark_return` (yfinance `history()` default `auto_adjust=True`, which adjusts for dividends).
- **Evidence:** Friday 16:15 NY orders are submitted after the close and fill Monday at the open. The benchmark starts at Friday's close and ends at the close on the reconcile day, while the stock exits at the open. The docstring says "total price return" but the code uses dividend-adjusted closes. The paper stock leg gets no dividends (F3).
- **Why it matters:** Each trade carries roughly two half-day mismatches (about 1–1.5% s.d. of noise per trade) plus a negative bias of about the dividend yield times the hold length. For short holds this noise is as large as the alpha being measured.
- **Severity:** medium.
- **Effort:** S (use fill timestamps; use SPY open/close matching the fill side; add a dividend shadow ledger or use price-only SPY consistently).
- **Unlocks / depends on:** F5.
- **Confidence:** high on the code. The size of the noise is estimated from typical daily SPY volatility.

### F8. `converged` and `reasoning_sound` do not measure what their names claim
- **What:** `converged` is 1 whenever the stock went up at all, because fair value is above entry by construction. `reasoning_sound` is only ever scored for `take_profit`. The "later post-hoc pass" for thesis-breaker sells does not exist.
- **Where:** `src/database.py:close_trade` (`converged = 1 if predicted_up == moved_up`), `_score_reasoning_soundness` (l.493–511). Grep finds no other writer of `reasoning_sound`.
- **Evidence:** GIS's "Rotate: near fair value" sell is classified `take_profit` by `_classify_sell_reason` because the text contains "fair value". It is scored unsound.
- **Why it matters:** The briefing prints "Sound = share of judgeable closes whose original reasoning held up". With this rubric the number mostly reflects whether the market rose.
- **Severity:** low.
- **Effort:** S.
- **Unlocks / depends on:** None.
- **Confidence:** high.

### F9. No counterfactuals: the journal records only the trades made
- **What:** Nothing records the candidates ranked but not bought, the names excluded by the NONE/LOW rule, the planned sells that were cancelled, or what the sold names did afterwards.
- **Where:** `scripts/scheduler.py:weekly_auto_trade` (`plan.skipped_quality` is only `logger.info`'d, l.~1001); the `single_buy_recommendations` table (0 rows); the reasoning snapshot (`_build_reasoning_snapshot`, l.67) has no valuation breakdown, Haiku score, regime confidence or rank.
- **Evidence:**
  - The three 2026-10-16 thesis-breaker sells (CTSH, CCI, NNI) are `canceled`, and no row tracks what keeping them did. CTSH alone is +$8k.
  - The 3.0.0 exclusion list is never persisted.
- **Why it matters:** "What the system excludes" is the only place the research notes claim value. That can only be measured if the excluded set and the rejected trades are stored and priced.
- **Severity:** medium.
- **Effort:** S–M (one `candidate_log` table per Friday run: rank, tier, moat, conviction, valuation components, action taken or reason skipped).
- **Unlocks / depends on:** The proposed programme (I1).
- **Confidence:** high.

### F10. Nothing in the system compares the equity curve to any benchmark
- **What:** `Database.get_equity_curve` has no callers. The briefing shows a per-tier table of 2 closed trades and the cohort table without n warnings, standard errors or significance. No report says "the paper account vs SPY".
- **Where:** `src/database.py:get_equity_curve` (l.2123); `src/briefing/db_briefing.py` l.297–316 and l.410–427; `src/journal_report.py:build_report`.
- **Evidence:** A grep for `get_equity_curve` finds only the definition. The briefing cohort block prints `alpha`, `median` and `hit` per row; the misleading moat-NONE row is presented the same way as the others.
- **Why it matters:** The one piece of evidence that exists is never reported, while the least reliable piece (the cohort table) is.
- **Severity:** medium.
- **Effort:** S.
- **Unlocks / depends on:** F2 flagging.
- **Confidence:** high.

### F11. Statistical power: the paper account cannot detect realistic alpha within any useful horizon
- **What:** Years needed to detect 3 pp/yr of alpha: n_years = ((z_α + z_β) · TE / α)².
- **Where:** Calculation from the realised series and standard power formulas.
- **Evidence:**

  | Tracking error vs benchmark | 2-sided 5%, 80% power | 1-sided 5%, 80% power |
  |---|---:|---:|
  | 4% (diversified quality fund) | 14 years | 11 years |
  | 6% | 31 | 25 |
  | 8% | 56 | 44 |
  | 10% (typical 8-stock book) | **87** | 69 |
  | 12% | **126** | 99 |
  | 17.9% (realised here, 74 days) | 281 | 221 |

  - The minimum detectable alpha in this 0.29-year window is **93 pp/yr** (2-sided).
  - The trade-level view gives the same answer. The clean cohort's 90-day alpha s.d. is 16.0%, about 32%/yr per name, so detecting a 3 pp/yr mean needs (2.80 × 32 / 3)² ≈ 890 independent position-years. An 8-name book produces 8 a year, which is about 111 years.
  - 2 closed trades support no inference at all. 30 would still give a 95% CI on mean alpha of roughly ±12 pp.
- **Why it matters:** "Wait for the paper track record" is not a validation plan. Only cross-sectional cohort testing of many rated names (F12, I1) can produce evidence on a human timescale. Even that can only detect large effects.
- **Severity:** high.
- **Effort:** n/a (a property of the problem).
- **Unlocks / depends on:** Redesigning validation around cohorts and pre-registration.
- **Confidence:** high on the arithmetic. TE assumptions are stated.

### F12. Even cohort testing is underpowered at the current rating intake, and NONE is too rare to test
- **What:** The cohort approach needs hundreds of ratings per label. The pipeline produces few first ratings, and the labels being tested are rare.
- **Where:** `deep_analyses`; `Database.get_first_ratings` (only first ratings count).
- **Evidence:**
  - Clean 90-day alpha s.d. is 16.0% (n=144). Two groups need **160 per group** to detect a 5 pp/90d difference, **640** for 2.5 pp and 7,116 for 0.75 pp (= 3 pp/yr).
  - First ratings per month: Apr 99, May 25, Jun 20, Jul 34, Aug 5, **Sep 0**, Oct 2. Paid jobs were off from early August to October.
  - NONE appears in **4 of 273** non-migrated analyses (1.5%), because the Haiku moat gate filters no-moat names before Sonnet. LOW conviction appears in 42 of 273.
- **Why it matters:** At 10–30 first ratings a month and 1.5% NONE prevalence, a 160-name NONE cohort would take decades. The "monthly cohort validation, stored per ruleset version" will report noise for years.
- **Severity:** high.
- **Effort:** M (rate a fixed random sample of the universe each month, including Haiku fails, to build an unbiased cohort; I1).
- **Unlocks / depends on:** Budget for extra Sonnet calls (about 20–40 a month).
- **Confidence:** high.

### F13. The ruleset 3.0.0 "moat NONE" evidence is a data-migration artifact; there are 0 real NONE observations
- **What:** All 20 moat-NONE names with an elapsed window are legacy `registry.json` studies that the migration imported with a default of `"none"`, because the legacy schema had no `moat_rating` key. The other 2 were rated on the as_of date (zero-length window, alpha exactly 0, counted as "not ahead").
- **Where:** `src/database.py` registry migration, l.2503–2504: `moat_str = analysis.get("moat_rating", "none")`, `moat_rating = moat_str.upper() if moat_str else "NONE"`. Cited in `src/deployment.py:70`, `README.md:125`, `docs/research/pit-backtest-2026-10-17.md:41` and commit fe1fa95.
- **Evidence:**
  - `deep_analyses` on 2026-04-07: NONE 20 of 20. All 20 share one timestamp second (15:44:07), have `moat_sources '[]'`, `fair_value NULL`, `expires_at = +1 day`, and `run_log` shows 0 LLM calls that day.
  - In `registry.json` all 20 lack `moat_rating`. Their legacy moat durability is "strong" for 13 and "moderate" for 7.
  - CWT's own thesis reads "irreplaceable monopoly infrastructure … a textbook moat".
  - The two zero-window names are ACIC and EVER, `rated_at 2026-10-16 = as_of`. The hit rate is really 2 of 20 for the artifact cohort.
  - The batch is also 9 regulated utilities (AWR, CPK, CWT, IDA, NJR, NWE, NWN, SWX, TXNM) measured from one date over a +16.4% SPY rally.
- **Why it matters:** The headline justification for a hard exclusion rule measures "legacy utilities and industrials migrated on 2026-04-07", not "no moat". The rule may still be sensible a priori, but it is not evidence-based, and the research notes and README state otherwise.
- **Severity:** critical, because a rule and its documentation rest on mislabelled data.
- **Effort:** S (exclude the migrated rows, `trigger='bulk_load'` with reason "Migrated from legacy registry", from cohorts; correct the docs).
- **Unlocks / depends on:** Honest cohort tables.
- **Confidence:** high (code, DB and source file agree).

### F14. The "LOW conviction" cohort was produced by the rating-parser bug, and its effect disappears on clean data
- **What:** Every first rating before 2026-10-01 was parsed by `extract_rating_checked`, which picked the longest matching option. Under that parser, LOW only results when the text mentions neither "medium" nor "high". The cohort also contains 6 migrated rows.
- **Where:** `src/analysis_parser.py:extract_rating_checked` (fixed in d922f88, 2026-10-01); `cohort_validation` rows 8 and 17.
- **Evidence:**
  - Conviction before the fix (excluding migrated rows): MEDIUM 207, LOW 31, HIGH 7, so 84% MEDIUM. After the fix: LOW 11, MEDIUM 17, so 39% LOW.
  - The docs cite "LOW conviction n=27: −7.5 pp". The stored run says n=25, −7.47 pp, and n=27 cannot be reproduced from any stored run.
  - On clean data (migrated and zero-window rows removed), LOW conviction is **n=17, −3.9 pp since rating (t = −0.58)** and **+0.8 pp at 90 days (n=16)**.
  - Tier C on clean data is −1.5 pp (t = −0.46). Raw it was −4.3 pp.
- **Why it matters:** The second leg of the 3.0.0 exclusion rule has no measurable effect once the artifacts are removed. Raw LLM text is not retained (`data/analyses/*.json` hold only parsed fields), so the old labels cannot be re-parsed. They can only be regenerated, at a cost, and a model re-reading April data in October may know what happened since.
- **Severity:** high.
- **Effort:** S to restate. M to rebuild clean cohorts going forward.
- **Unlocks / depends on:** F13; storing raw responses.
- **Confidence:** high.

### F15. Cohort methodology: unequal windows, no risk adjustment, many comparisons, silent drops
- **What:** `since_rating` averages windows of 0 to 192 days. Alpha is raw return minus SPY, with no beta, size or sector adjustment. 18 cohort×horizon cells are tested and the worst two were promoted to a rule. Delisted names drop out silently. The rating date of migrated rows is the migration date.
- **Where:** `src/backtest.py:compute_cohort_validation` (l.859), `_summarize` (l.828), `_download_close_prices` (l.912; names yfinance cannot price disappear).
- **Evidence:**
  - Mean SPY return per cohort: NONE 14.9% vs NARROW 4.6%, WIDE 5.5% and B 6.2%, so the cohorts were measured over different periods.
  - AL (cash takeover at $65, a good outcome) and CPRX are unpriced and dropped.
  - The legacy analyses for the 04-07 batch carry `analyzed_at` 2026-03-17 in `data/analyses/*.json`, three weeks before the date used.
  - A permutation test (20 random names from the 04-27 batch, 20,000 draws) gives P(mean ≤ −11.5 pp) = 0.015. A Bonferroni correction over 18 cells needs p < 0.0028. Even taken at face value this is a date effect, not a label effect.
- **Why it matters:** The table invites reading cohort labels as causal when the dominant drivers are rating date and sector beta.
- **Severity:** medium.
- **Effort:** S–M (fixed horizons only; beta- and sector-adjusted alpha; date-clustered standard errors; publish n_eff and a Bonferroni or FDR note; keep delisted names with their last price or deal value).
- **Unlocks / depends on:** I1.
- **Confidence:** high.

### F16. The only positive cohort (tier A, +20.6 pp) is also underpowered and concentrated
- **What:** Tier A is n=8 (n=7 clean). Two names account for two-thirds of the summed alpha.
- **Where:** `cohort_validation` row 1; per-name JSON.
- **Evidence:** CTSH +56.0 pp and IT +47.2 pp out of a clean total of +154 pp. The other five: +24.6, +22.0, +19.8, +0.5, −16.0. Clean statistics: mean +22.0 pp, t = 2.34, 6 of 7 positive (one-sided sign-test p = 0.06). The A tiers were set by pre-3.0.0 rules (LLM target entry, buggy conviction parser), which no longer exist.
- **Why it matters:** This is worth tracking as a hypothesis, not evidence. It also says nothing about 3.0.0's A tier, which is assigned differently.
- **Severity:** low.
- **Effort:** S.
- **Unlocks / depends on:** None.
- **Confidence:** medium (small n).

### F17. Cohort rows are stamped with the validator's ruleset, not the rating's
- **What:** `save_cohort_validation` writes `RULESET_VERSION` ("3.0.0") on every row, but every first rating it measures was produced under 2.0 or earlier. Monthly re-runs re-measure the same first ratings with longer windows instead of building a new sample.
- **Where:** `src/database.py:save_cohort_validation` (l.1168–1193); `get_first_ratings` (l.1195, earliest analysis per ticker, with no ruleset filter); `scripts/scheduler.py:monthly_cohort_validation` docstring ("accumulates per ruleset version").
- **Evidence:** All 18 rows say `3.0.0`. 183 of 185 first ratings predate 2026-10-01 and all 185 predate 3.0.0 (2026-10-17/18). The briefing prints "(ruleset v3.0.0)" above them.
- **Why it matters:** The intended per-version track record is mislabelled by construction and will stay dominated by legacy ratings for as long as those tickers exist.
- **Severity:** high.
- **Effort:** S (store the rating's version on `deep_analyses`; filter cohorts by it; run fixed-horizon snapshots, not a re-run of the full history).
- **Unlocks / depends on:** F18.
- **Confidence:** high.

### F18. The history is not segmented by ruleset: the stamps are empty and the changelog skips rule changes
- **What:** The claim "a paper equity curve produced under two rulesets is two experiments" is not implemented for any existing data. The changelog lists 2 versions, while git shows at least 6 unversioned rule changes between them.
- **Where:** `src/ruleset.py`; `decision_log.ruleset_version`, `tier_history.ruleset_version` (added by migration); no column on `portfolio_snapshots`, `closed_trades` or `deep_analyses`.
- **Evidence:**
  - Rows stamped: decision_log 0/16, tier_history 0/293.
  - Unversioned rule changes:
    - 75a754d and 37a1c25 (2026-07-25/26): quarantine excluded from sells; "a thesis breaker needs a thesis; stop C being a one-way trapdoor".
    - c1b6602: Sonnet 5 replaces the rater.
    - ae3647b (2026-08-11).
    - d922f88, 4a39dae and 953ac0c (2026-10-01): parser fix, D/E scale fix, "confirm C verdicts before selling".
    - cbc0818: Sonnet 5.5.
  - Post-3.0.0 evidence as of 2026-10-19: 0 decisions, 0 tier changes, 1 snapshot (a Sunday), 0 trading days.
- **Why it matters:** No pre-3.0.0 window is a single ruleset either. The 07-02→10-16 curve spans at least four rule regimes and two rater models. No segment can be called a clean experiment, and pre-3.0.0 evidence is usable only as anecdote.
- **Severity:** high.
- **Effort:** S (add a ruleset column to snapshots, analyses and trades; backfill by date from a corrected changelog; treat model changes as version bumps).
- **Unlocks / depends on:** F17, F19.
- **Confidence:** high.

### F19. Ruleset 3.0.0 inherits the 2.0 book, so its equity curve is contaminated for as long as those names are held
- **What:** All 8 holdings were chosen under old rules or before the journal existed: NNI at default tier "C", AGM and AL pre-journal. Under 3.0.0 only a fired breaker forces a sell, so the legacy positions can persist indefinitely.
- **Where:** `src/deployment.py:plan_sells` (3.0.0 sell rule); the position mirror has no per-position ruleset or entry date.
- **Evidence:** The 10-18 snapshot shows 8 positions and $75.4k invested. `thesis_breaker_events` = 0. The three old-rule sells were cancelled on 10-16.
- **Why it matters:** Any "3.0.0 performance" number in the next year will mostly measure 2.0 picks.
- **Severity:** high.
- **Effort:** S (tag each position with the ruleset of its entry and report 3.0.0-entered positions separately; or liquidate to cash at the version boundary in a paper sub-account).
- **Unlocks / depends on:** F18.
- **Confidence:** high.

### F20. The point-in-time backtest has a split and unit look-ahead in FCF yield
- **What:** `_derive_quality_metrics` computes market cap as yfinance's split-adjusted historical price times the as-filed, pre-split share count. FCF yield is inflated by the future split factor, which is information that did not exist at the rebalance date. XBRL unit errors (1,000× to 1,000,000× share counts) add a second distortion.
- **Where:** `src/backtest.py:_derive_quality_metrics` (l.250–282), `_load_prices` (l.285, `history(period="max")` is split-adjusted).
- **Evidence:**
  - NVDA as-filed `shares_diluted` was 569M in FY2016 and 24.8B in FY2025. Against a 40×-split-adjusted price, its 2016 market cap is about 1/40 of reality.
  - Across the 10,559 observations, **734 sit before a split-like share jump (1.8–50×)** and 109 before a unit error (≥50×, e.g. BRKR and HST at ~1e6×).
  - Affected share by within-date quintile: top 14.6%, Q2–Q4 about 5%, bottom 10.0%. Negative-FCF names are pushed to the bottom.
  - Removing affected observations moves the pooled Spearman from +0.018 to **+0.003**.
- **Why it matters:** The small positive signal reported on 2026-10-17 is partly the bug. Future splitters are past winners that keep winning, which is exactly the look-ahead that flatters a score.
- **Severity:** high.
- **Effort:** S (use unadjusted close × as-filed shares, or adjusted close × split-adjusted shares; sanity-bound share counts against the prior filing).
- **Unlocks / depends on:** Any reuse of mode 3.
- **Confidence:** high on the mechanism. Medium on counts: split detection is inferred from 10-K share jumps, and mergers can mimic splits.

### F21. Mode 3 mixes fiscal years across concepts and uses a crude ROIC proxy
- **What:** `get_pit_fundamentals_asof` takes the latest period independently for each concept. In 14.5% of observations, ROE and margin are computed from numerators and denominators of different fiscal years.
- **Where:** `src/database.py:get_pit_fundamentals_asof` (l.2353–2379); `_derive_quality_metrics` (ROIC = NI/(equity+debt); `if total_debt >= 0` is always true).
- **Evidence:** 1,529 of 10,559 observations have core concepts (NI, equity, revenue, OCF, OI) whose period ends are more than 200 days apart. Only 13 use fundamentals older than 18 months.
- **Why it matters:** This adds noise that biases any rank correlation toward zero and differs from the live yfinance score.
- **Severity:** medium.
- **Effort:** S (align all concepts to one fiscal period per ticker).
- **Unlocks / depends on:** None.
- **Confidence:** high.

### F22. The 2026-10-17 PIT result is not robust and partly not reproducible
- **What:** Every headline statistic is consistent with zero. The per-date table and the medians in the report are produced by no committed code. The test covers `quality_scorer` only, not the `score_stock` funnel or the insider tilt used live.
- **Where:** `data/backtest/pit_backtest_report.md`; `docs/research/pit-backtest-2026-10-17.md`; `src/backtest.py:run_point_in_time_backtest` (computes only pooled Spearman, quintile means and top/bottom).
- **Evidence:**
  - Mean per-year Spearman is +0.026 with a s.d. of about 0.10 across 10 years (t ≈ 0.8).
  - The median top-minus-bottom spread is +2.1 pp/yr, positive in 8 of 10 years but t = 0.49 (−23.6 pp in 2020, −20.6 pp in 2025).
  - The top quintile beat SPY in 5 of 10 years (binomial p = 0.62).
  - The top quintile trailed the equal-weight universe by 5.8 pp/yr (+17.7% vs +23.5%), and the equal-weight universe is the size-matched benchmark.
  - Grep for "universe EW" and "pit_backtest_report" in the repo finds nothing.
- **Why it matters:** The research note's "small, fairly consistent edge on median returns" is not supported once uncertainty is shown. It cannot be regenerated from the stated reproduce commands.
- **Severity:** medium.
- **Effort:** S (commit the report generator; add bootstrap CIs; test `score_stock` as well).
- **Unlocks / depends on:** F20 fix.
- **Confidence:** high.

### F23. Universe survivorship and selection in mode 3 go beyond "today's names"
- **What:** The universe is today's S&P 500 (Wikipedia), today's Finviz small/mid screen (cap $300M–$500B at today's prices, ADTV, price > $5) and the conviction list. All of these select on later survival, size and liquidity.
- **Where:** `src/universe_builder.py:build_universe`; `src/backtest.py` docstring acknowledges only "current universe".
- **Evidence:** 1,292 of 1,359 tickers have EDGAR facts. Observations grow from 889 (2016) to 1,220 (2025) as today's names IPO'd. The bottom quintile's +49.8% mean is driven by 2020/2025 junk rallies among names that, by construction, survived.
- **Why it matters:** Every quintile is biased upward. The bias is largest for distressed and low-quality names, so it compresses any quality premium in an unknown direction. The magnitude cannot be estimated without delisted filers.
- **Severity:** high for any strategy claim; acknowledged in the doc.
- **Effort:** M–L (rebuild the universe each year from EDGAR 10-K filer lists plus delisted price histories, e.g. CRSP-like data, not yfinance).
- **Unlocks / depends on:** A credible backtest of the screen.
- **Confidence:** high on existence. Magnitude unverified.

### F24. Modes 1 and 2 are dead code, and mode 1 looks backwards
- **What:** `run_quality_return_correlation` correlates today's score with trailing 1y/3y returns. Because FCF yield has price in the denominator, the score is mechanically anti-correlated with past price gains. Mode 2's `save_watchlist_snapshot` has no callers, and `data/backtest/` contains no watchlist snapshots.
- **Where:** `src/backtest.py:_fetch_historical_returns` (l.324: `hist["Close"].iloc[-252]` to the current price); grep shows no callers of `save_watchlist_snapshot` or `run_quality_return_correlation` in `src/` or `scripts/`.
- **Evidence:** As quoted above; `ls data/backtest/` returns 3 files, none of them watchlist snapshots.
- **Why it matters:** The module docstring presents modes 1 and 2 as validation. They produce nothing, and mode 1 would produce a misleading number.
- **Severity:** low.
- **Effort:** S (delete or relabel).
- **Unlocks / depends on:** None.
- **Confidence:** high.

### F25. A backtest of the whole pipeline (screen → LLM → tier → deployment) is not feasible
- **What:** The LLM stage cannot be run point-in-time. Sonnet 5.5 and Haiku 4.5 were trained on text covering 2016–2025 outcomes. Any replay of historical filings will leak hindsight: a model "rating" CTSH or NVDA in 2018 knows what happened next. Masking names does not remove this, because financial profiles are recognisable.
- **Where:** `src/analyzer.py` prompts; the absence of any LLM replay in `src/backtest.py`.
- **Evidence:** Structural. The deterministic parts (quality score, valuation aggregator, tier thresholds on quant inputs, regime and deployment) can be backtested PIT. Prerequisites: PIT valuation inputs (analyst targets are not PIT in yfinance or Finnhub), PIT prices, and the F20/F21 fixes.
- **Why it matters:** The only valid evidence on the LLM's contribution is **forward**: ratings made before outcomes are known, stored with raw text, measured on fixed horizons (I1). Every month without a clean forward cohort is lost evidence.
- **Severity:** high (constrains every edge claim).
- **Effort:** n/a.
- **Unlocks / depends on:** I1.
- **Confidence:** high.

### F26. SPY is the wrong benchmark, and nothing is DKK- or ASK-aware
- **What:** The holdings are US mid/small-cap value and quality names (AGM, AIZ, CCI, CPRT, CTSH, DECK, NNI) on a book about 70% invested. SPY is a cap-weighted mega-cap index. No benchmark is computed in DKK, after ASK tax, after costs, or against a random-portfolio null.
- **Where:** `src/benchmark.py` (single symbol, `config.benchmark_symbol` = SPY); `src/backtest.py:run_tier_cohort_validation(benchmark_symbol="SPY")`.
- **Evidence:**
  - Over the PIT decade the equal-weight universe beat SPY by 7.3 pp/yr (+23.5% vs +16.2%). Size and equal-weight effects dwarf any quality effect, so SPY-relative alpha mostly measures the size tilt.
  - The realised daily correlation with SPY is 0.03, so SPY explains almost none of the book's variance in this window.
  - DKK: the book is +7.45% and SPY TR about +4.23% in DKK. FX affects both equally (all USD), so the relative ranking is unchanged before costs.
  - ASK: 17% on annual mark-to-market applies to both legs, scaling after-tax alpha by about 0.83.
  - The investable benchmark for a Danish ASK is a UCITS S&P 500 / MSCI USA ETF, not SPY. Which ETFs are eligible is unverified.
- **Why it matters:** "Beat SPY" can be achieved, or missed, through size and sector tilts alone.
- **Severity:** medium.
- **Effort:** S–M. Report against (a) SPY, (b) an equal-weight benchmark (RSP or the bot's universe), (c) a sector-matched equal-weight basket of the same candidate pool, and (d) a random-portfolio null: draw 8 names from the same week's B-or-better candidate list, 1,000 times.
- **Unlocks / depends on:** F9 (needs the stored candidate lists).
- **Confidence:** high on the mismatch. Medium on ETF and tax details.

---

## Improvement ideas not tied to a weakness

### I1. The minimal validation programme that would make an honest claim possible
- **What:**
  1. **Freeze and pre-register.** Write a dated `docs/research/preregistration-3.0.0.md` with the hypotheses (e.g. "first-rated A/B names beat a sector-matched equal-weight basket by ≥ 5 pp over 180 days"), the metric, the horizons (90/180/365 days, fixed), the benchmark set (F26), the test (date-clustered t-test or bootstrap) and α. Do not change rules mid-sample, or bump the version and restart the clock.
  2. **Build an unbiased rated sample.** Each month, Sonnet-rate a random 20–40 names from the whole universe, including Haiku fails, in addition to the pipeline's own picks. Store the raw response and the ruleset/model version.
  3. **Store everything needed for counterfactuals:** a candidate log, skipped and excluded names, cancelled orders (F9).
  4. **Measure at fixed horizons** with beta- and sector-adjusted alpha, never "since rating".
  5. **Report** n, the CI and the minimum detectable effect next to every number in the briefing.
- **Where:** New research doc; `deep_analyses` (+raw_text, +ruleset_version, +model); new `candidate_log`; `src/backtest.py` cohort code.
- **Evidence:** F11–F17.
- **Why it matters:** At about 30 rated names a month and a 16% per-name 90-day alpha s.d., after 12 months (~360 names, ~180 per arm) the programme can detect about a 5 pp/90d difference between "bot would buy" and "bot would not buy". A 3 pp/yr portfolio alpha remains undetectable at any feasible horizon, and the plan should say so.
- **Severity:** n/a.
- **Effort:** M.
- **Unlocks / depends on:** About 20–40 extra Sonnet calls a month (the budget caps need a line for this); the F13/F14/F17 fixes.
- **Confidence:** high.

### I2. Recover the pre-July account history
- **What:** The account traded from at least May 2026 (trade_log.json: ATEX and ARCO sells on 05-01 and 05-08), and `src/health.py` notes a delisted holding "from February to July". The equity history before 2026-07-02 is not in the DB. Alpaca's portfolio-history endpoint may return it read-only.
- **Where:** `src/paper_trader.py` (no such call exists).
- **Evidence:** The first snapshot is 2026-07-02 07:09. Positions ACT, AGM and AL were already held.
- **Why it matters:** It would double the length of the curve, though it would be yet another rule regime.
- **Severity:** n/a.
- **Effort:** S.
- **Unlocks / depends on:** Alpaca API access (paper, read-only).
- **Confidence:** medium (endpoint availability unverified).

### I3. Commit the research generators and keep scored inputs
- **What:** Store per-observation metric inputs (FCF yield, ROE, …) in `pit_backtest.json`, not only the score, so biases like F20 can be audited without recomputation. Commit the script that wrote the per-date table.
- **Where:** `src/backtest.py:run_point_in_time_backtest`.
- **Evidence:** F22.
- **Why it matters:** Reproducibility.
- **Severity:** n/a.
- **Effort:** S.
- **Unlocks / depends on:** None.
- **Confidence:** high.

### I4. Add a snapshot and journal-vs-broker invariant to the health check
- **What:** Each night, check that the journal's open positions equal the broker's positions (excluding flagged pre-journal names). Also check that equity changes on days without fills stay within ±15% and that the position count matches the previous day plus or minus fills.
- **Where:** `src/health.py`.
- **Evidence:** F2, F4.
- **Why it matters:** It would have caught 09-16 and the ACVA/AESI phantoms the same day.
- **Severity:** n/a.
- **Effort:** S.
- **Unlocks / depends on:** None.
- **Confidence:** high.

---

## What I could not verify

- **Root cause of the 2026-09-16 Alpaca read.** The bot faithfully stored what Alpaca returned. Whether Alpaca's paper environment had an outage is unknown; no container logs were read.
- **Dividends.** Whether Alpaca paper never credits dividends, or just did not in this window, is inferred from 86 days of constant cash. Per-share dividend amounts for the holdings were not fetched.
- **AL.** That AL was a completed $65.00 cash takeover is inferred from the exact mark and the `inactive` status. Deal terms were not checked.
- **The cancelled sells.** Who or what cancelled the three 2026-10-16 sells (manual action vs a broker or deploy event) is not visible in the DB.
- **The n=27 figure.** The source of "LOW conviction n=27" in `src/deployment.py:70` and the research note does not match the only stored run (n=25).
- **Split detection.** Counts in F20 are inferred from consecutive 10-K share jumps, not from a split calendar (that would need a bulk price/split pull, which is out of scope).
- **Danish costs and tax.** Nordnet commission and FX-spread levels, and which UCITS ETFs are on SKAT's ASK positive list, were not checked. The ASK rate of 17% is taken from `ASK_DIVIDEND_TAX_RATE` in the code.
- **The pre-July account.** Its equity history and performance were not available.
