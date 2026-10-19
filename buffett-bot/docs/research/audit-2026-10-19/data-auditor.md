# Data audit, 2026-10-19 (data-auditor)

Scope: data sources, live-DB quality, staleness, point-in-time (PIT) correctness, survivorship, corporate actions, FX, API fragility.
Method: read-only. Live DB opened `mode=ro` from `python3 -I`; yfinance cache JSON under `/opt/docker-apps/buffett-bot/data/cache/`; `docker logs buffett-bot-scheduler` (log starts 2026-10-18 12:48, so earlier runs are not visible). No LLM calls. No network requests at all were made (every claim below comes from the DB, cached files, logs and code).
All "today" numbers are as of 2026-10-19. Paths are relative to `/home/bardie/git-repos/BuffetBot/buffett-bot/` unless absolute.

Headline: the live data layer has one structural defect that explains most of the rest. Since 2026-07-26 the Monday refresh persists only the top 100 screened names, so 952 of 1,359 universe names carry fundamentals frozen at 2026-06-29 (112 days) and 220 have none; the LLM prompts present those as "Current Price". The 2026-10-17 PIT backtest has two defects that bias it in ways the write-up does not mention: split look-ahead in the market-cap used for FCF yield, and missing debt tags scored as zero debt. Separately, the weekly watchlist that feeds the auto-trader is being built from a 183-name hard-coded fallback list, and its intersection with S/A-tier names is empty.

---

## Findings

### F1. Monday refresh persists only the top 100 names; 952 universe names are frozen at 2026-06-29, 220 have no fundamentals
- **What:** `screen_tickers` truncates its result to `criteria.top_n` (100) and `monday_maintenance` writes fundamentals, universe market cap/sector and quality scores only for that truncated list. Before 2026-07-26 ~1,070 names were written per week; since then exactly 100.
- **Where:** `src/screener.py:1033-1036` (`candidates = candidates[:top_n]`); `scripts/scheduler.py:1193-1238` (`screened = screener.screen_tickers(...)`, `save_fundamentals`, `upsert_universe_stock`, `compute_quality_scores(compat)`, `update_quality_score`); `config/screening_criteria.yaml` `top_n: 100`.
- **Evidence:** log `2026-10-19 02:20:15 Keeping top 100 by score (from 1078)` then `Refreshed fundamentals for 100/1359 universe stocks` and `Recomputed quality scores for 100 stocks`. DB: rows per `fundamentals.date` = 671, 1082, 1093, ... 1071 (through 2026-06-29), then 100 on every date from 2026-07-26 to 2026-10-19. Latest fundamentals date per universe ticker: 100 on 10-19, 952 on 06-29, 24 between 07-26 and 08-xx, 6 within 30 days, 277 older or none (220 with no row at all). `universe.last_screened`: 100 rows today, 1 or 2 per older week.
- **Why it matters:** The "weekly refresh" is a weekly refresh of the 100 best-scoring names only. Everything that reads `universe`/`fundamentals` for the other ~1,260 names (Haiku queue ordering, Sonnet queue, held-position context, briefing) runs on 3.5-month-old numbers, and the table can no longer be used as a weekly history of the universe. Names that fall out of the top 100 simply stop being updated, so a deteriorating company is not re-scored.
- **Severity:** high
- **Effort:** S (persist all scored candidates; keep `top_n` only as a downstream display cap)
- **Unlocks / depends on:** F2, F3, F5, F13 all improve once the full scored set is persisted. A future live PIT history needs this first.
- **Confidence:** high (log line plus table counts, two independent sources)

### F2. LLM prompts present 107-day-old fundamentals as "Current Price"; the docstring promises 4 days
- **What:** `_build_db_summary` takes `get_latest_fundamentals(ticker)` with no age check and writes `Current Price: $X`, ROE, ROIC, FCF yield, debt/equity into the Haiku and Sonnet prompt. No as-of date is included.
- **Where:** `scripts/scheduler.py:103-143` (docstring: "Monday's fundamentals refresh ensures this data is <=4 days old"); `src/database.py:881` `get_latest_fundamentals` (`ORDER BY date DESC LIMIT 1`); same pattern in `src/news_fetcher.py:192` `_build_news_context`.
- **Evidence:** For the 80 Haiku screens written since 2026-08-01, the fundamentals row in effect at screening time was a median 107 days old (p90 107, max 156). For the 47 Sonnet analyses since 08-01, median 42 days, p90 109, max 109, and 3 had no row. Example: NVDA, GPOR, INCY, MTCH Haiku-screened 2026-10-14 on the 2026-06-29 row.
- **Why it matters:** Haiku pass/fail and Sonnet fair-value/tier decisions (which gate real paper buys and the weekly paid budget) are made against stale price and margin data that the model is told is current. Sonnet cannot discount for staleness because it is not told.
- **Severity:** high
- **Effort:** S (fetch live `info` for the ~10-50 names actually sent to an LLM, or at least print the as-of date and refuse rows older than N days)
- **Unlocks / depends on:** F1 removes most of the staleness; the as-of stamp is still worth adding.
- **Confidence:** high (computed from `haiku_screens.screened_at`, `deep_analyses.analyzed_at` and `fundamentals.date`)

### F3. Quality scores are now on two incompatible scales
- **What:** `compute_quality_scores` ranks percentiles within whatever list it is given. Until 07-26 that was ~1,070 names; now it is the top-100 screened names. `update_quality_score` writes the new numbers only for those 100, so `universe.quality_score` mixes full-universe percentiles (old rows) with top-100 percentiles (fresh rows).
- **Where:** `src/quality_scorer.py:compute_quality_scores` (docstring: "75 always means top 25% of what we're looking at right now"); `scripts/scheduler.py:1212-1238`; `src/tier_engine.py` fair-price exception (`quality_score >= 80`, per SYSTEM_MAP).
- **Evidence:** `universe.quality_score` by freshness: 100 fresh rows avg 49.7 (min 16.7, max 85.0, by construction centred near 50); 952 rows from the 06-29 run avg 47.4 (range 1.2-98.5); 277 older avg 35.9 with 220 NULL. Rows with `quality_score >= 80`: 21 stale, 2 fresh. A top-decile compounder that is in today's top 100 gets a mid-table score; a mediocre name last scored in June can still hold 98.
- **Why it matters:** Any threshold or ordering on `quality_score` (Haiku/Sonnet queue priority, the tier-engine fair-price exception at 80, briefing ranking) compares numbers with different meanings. It also biases toward stale names that were once ranked against a wider field.
- **Severity:** high
- **Effort:** S (fix F1, rank against the full scored universe)
- **Unlocks / depends on:** F1.
- **Confidence:** high

### F4. 36 universe tickers resolve in neither Yahoo nor SEC; none are ever retired
- **What:** `universe.in_universe` is never set to 0 anywhere in `src/` and the scheduler never rebuilds the universe (only `scripts/bulk_load.py` calls `build_universe`). Delisted, acquired or re-ticked names therefore stay in the weekly fetch loop forever and keep their last quality score.
- **Where:** `src/database.py:791-801` (`upsert_universe_stock` always writes `in_universe = 1`); `src/universe_builder.py:build_universe` (called only from `scripts/bulk_load.py:654`); `src/screener.py:_fetch_stock_data` returns None on a 404 and the caller does `errors += 1`.
- **Evidence:** The 37 yfinance errors in the 2026-10-19 run are 404s such as `Quote not found for symbol: BK`, CTRA, CPRX, APLS, SATS, AL, IAC, KW, CWEN-A, BLD. Their cache files are 35-216 days old (BXMX 216, HOLX 189, AL 189, ASGN 175 ... AVB 56, EQR 56, EA 70). 36 universe tickers are also absent from the SEC `company_tickers.json` (6 are S&P 500 names: AVB, BK, CTRA, EA, EQR, HOLX; BK carries a $94B market cap). The two lists overlap almost entirely. `sp500_universe.json` was cached 2026-04-27 (503 names) and is the only S&P source; the table's universe is the April build. CPRX (404, last fundamentals 06-29) still received a Haiku screen on 2026-10-14 (passed). Stale quality scores remain on dead names, e.g. CPRX 84.4, BK 64.0, EA 64.9.
- **Why it matters:** Dead names consume the weekly Haiku/Sonnet budget (CPRX), keep high stale scores that out-rank live names, and are re-fetched every Monday with no negative caching. New S&P additions, IPOs and ticker changes never enter. BK may be a rename (BNY rebrand) rather than a delisting; the system cannot tell, and would silently lose a $94B S&P bank either way (cause per name is unverified).
- **Severity:** high
- **Effort:** M (rebuild the universe on a schedule, key on CIK, mark names failing N consecutive fetches as `in_universe = 0`, map ticker changes through SEC `company_tickers`)
- **Unlocks / depends on:** A clean survivorship-aware history (F10, I2).
- **Confidence:** high for the dead-ticker count and CPRX; medium for per-name cause

### F5. The weekly watchlist, which feeds the auto-trader, is built from the 183-name hard-coded fallback and shares no names with the A/S tiers
- **What:** `weekly_screen` calls `screener.screen()`, which uses `src/universe.py:get_stock_universe()` (Finviz, then Wikipedia S&P 600, then a curated list), not the DB universe. The Finviz cache file is corrupt and all 25 names on the live watchlist come from the curated fallback.
- **Where:** `scripts/scheduler.py:147-200` (`weekly_screen`), `src/universe.py:get_stock_universe`, `_fetch_sp600_from_wikipedia` (`src/universe.py:136` `pd.read_html(url)` with no User-Agent), `_get_curated_fallback`; consumer `scripts/scheduler.py:755-790` (`weekly_auto_trade`).
- **Evidence:** `/opt/docker-apps/buffett-bot/data/cache/stock_universe.json` (cached_at 2026-09-18T17:03, source "finviz", 1,057 tickers): every ticker has its first letter doubled (`AAAPL`, `AABBV`, `AABNB`, `UUSLM`, `ZZ`). All 1,057 match the pattern; stripping the first letter gives 974 names that are in the DB universe; as-is only 16 exist (`AA`, `BBY`, `CC`, `KKR`, ...). The file has not been rewritten since 09-18 (the 7-day TTL has lapsed three times), consistent with both Finviz and Wikipedia failing and the curated list being used (that path does not save the cache). `/opt/docker-apps/buffett-bot/data/watchlist.json` (updated 2026-10-16T17:05): 25 symbols, all 25 are in the curated list (183 names). Latest-tier S/A names in `deep_analyses`: BIO, AZZ, IT, AIZ; intersection with the watchlist: none. `weekly_auto_trade` only buys names that are on the watchlist and S/A tier.
- **Why it matters:** The auto-trader's reachable set is the intersection of a stale 183-name list and four A-tier names, which is empty today, so no automated buy can occur (consistent with 0 buys since July). The DB-driven universe/tier pipeline and the watchlist pipeline are two different universes. The cause of the Finviz corruption (finvizfinance version change vs a bug in `_apply_sector_caps`) and the Wikipedia failure (no User-Agent on `read_html`) is unverified: pre-10-18 logs are gone.
- **Severity:** high
- **Effort:** S (drive the watchlist from the DB universe plus tiers; validate that fetched universes resolve at >=90% before caching)
- **Unlocks / depends on:** Any claim that the bot "screens 1,359 names weekly" for buying.
- **Confidence:** high on the file contents and the 25/25 match; medium on cause

### F6. PIT backtest: split look-ahead in the market cap behind FCF yield
- **What:** `_load_prices` uses `yf.Ticker(symbol).history(period="max")`, which is split- and dividend-adjusted as of today (yfinance default `auto_adjust=True`). `_derive_quality_metrics` multiplies that adjusted historical price by shares as originally filed (not split-adjusted), then divides OCF-capex by the result. For any company that split after the rebalance date, market cap is understated by the split factor and FCF yield (20% of the score) is overstated by the same factor.
- **Where:** `src/backtest.py:285-300` (`_load_prices`), `:303` (`_price_on_or_before`), `:278` (`market_cap = price * shares`), `:250-282` (`_derive_quality_metrics`).
- **Evidence:** `pit_fundamentals` shares are as filed: AAPL `shares_diluted` FY2013 931,662,000, FY2014 6,122,663,000 (7:1), FY2019 4,648,913,000, FY2020 17,528,214,000 (4:1); NVDA FY2021 628M then FY2022 2,535M (4:1) and FY2025 24,804M (10:1). So for the 2019-06-01 rebalance, AAPL's market cap = 4.65B shares x a price adjusted down 4x = about a quarter of the true value. Proxy count of tickers whose as-filed diluted shares later jump >1.8x between consecutive 10-Ks (includes some M&A, so an upper bound): 376 tickers; share of the cross-section with a later such jump: 16.4% (2016-06-01), 11.9% (2019), 10.2% (2021), 6.3% (2023), 2.4% (2025).
- **Why it matters:** Companies that split later are, on average, winners, so their inflated FCF yield lifts their score before they win. The test is biased toward a better quality premium in earlier years, and the headline "no ranking power" (Spearman +0.018) is therefore, if anything, optimistic for the score. The 10-year result cannot be treated as a clean look-ahead-free test until this is fixed.
- **Severity:** high (for the validity of the one quantitative validation the project has)
- **Effort:** S (use `auto_adjust=False` closes and split-adjust shares, or use as-filed EPS/price from the same filing date; at minimum use the 10-K cover-page shares at the same date)
- **Unlocks / depends on:** A trustworthy answer to "does the quality score rank returns", which gates how much the LLM stage should be trusted as the only alpha.
- **Confidence:** high on mechanism and share counts; medium on the size of the effect on the final quintile table (not re-run)

### F7. PIT backtest: one ticker-reuse/reorganisation artifact is about 46% of the 2020 bottom-quintile mean
- **What:** CHRD returns +19,097% over 2020-06-01 to 2021-06-01. That is one row among 206 in the bottom quintile for that date, contributing about 93 percentage points to the +202.5% mean. The write-up attributes the +202% to a "junk rally" plus survivorship.
- **Where:** `src/backtest.py:_forward_return` (price series from `history(period="max")`, no handling of ticker reuse, bankruptcy or equity cancellation); research note `docs/research/pit-backtest-2026-10-17.md`.
- **Evidence:** `/opt/docker-apps/buffett-bot/data/backtest/pit_backtest.json` `results`: CHRD 2019-06-01 return -90.4%, 2020-06-01 +19,097%, 2021 +91.7%. Recomputing the bottom-20% mean for 2020-06-01 without CHRD: 202.5% to 110.4%. Pooled bottom-quintile mean: 49.8% to 40.9% without that one row. Remaining top outliers: AXTI (+71x, 2025), BW (+23x), ASPI (+14x). I did not verify the exact mechanism (Oasis Petroleum chapter 11 and the new equity trading under the same Yahoo history) and flag it as unverified.
- **Why it matters:** The quintile table and the "quality premium -32%" are dominated by a handful of rows; the note correctly calls the mean lottery-like but still quotes it. A bankrupt name whose old equity was cancelled should be -100%, not a 191x. Means over 10 draws with a 19,000% outlier are not usable; medians (which the note also reports) are the right statistic.
- **Severity:** medium
- **Effort:** S (winsorise or report medians only; drop or hand-check any |return| > 10x; reject rows whose price history spans an Oasis-style reorganisation)
- **Unlocks / depends on:** F6.
- **Confidence:** high on the numbers; medium on cause

### F8. PIT backtest: missing debt tags are scored as zero debt
- **What:** `_derive_quality_metrics` sets `total_debt = (long_term_debt or 0.0) + (short_term_debt or 0.0)`. If neither XBRL tag resolves for a company, it is treated as debt-free: debt/equity 0, ROIC = ROE, and the company lands at the top of the "low debt" percentile (15% of the score).
- **Where:** `src/backtest.py:266`; `src/edgar_fundamentals.py:CONCEPT_MAP` (`long_term_debt`: `LongTermDebtNoncurrent`, `LongTermDebt`; `short_term_debt`: `LongTermDebtCurrent`, `DebtCurrent`, `ShortTermBorrowings`).
- **Evidence:** Among tickers with equity and net income known at the rebalance date, those with no debt tag at all: 147/855 (17.2%) at 2016-06-01, 140/1,007 (13.9%) at 2020-06-01, 148/1,189 (12.4%) at 2025-06-01. The 2025 list includes AES, AFL, AGNC, ACGL, AKAM, ALGN, AXON, ARES, BAM. AES and AGNC are heavily leveraged (AGNC is a mortgage REIT). Concept coverage in `pit_fundamentals`: long_term_debt 1,112 tickers, short_term_debt 965, of 1,292.
- **Why it matters:** The backtest rewards leveraged financials/REITs/utilities for "no debt", contaminating the quality ranking in exactly the sectors where debt matters most. The missing tags are mostly structural (banks, REITs, insurers, converts use other tags), so this correlates with sector, not noise.
- **Severity:** high
- **Effort:** S (treat missing as None; require debt coverage for the low-debt component; add `DebtInstrumentCarryingAmount`, `LongTermDebtAndCapitalLeaseObligations`, `SecuredDebt`, `UnsecuredDebt`, `ConvertibleNotesPayable`)
- **Unlocks / depends on:** F6, F9.
- **Confidence:** high

### F9. PIT backtest: concepts taken from different fiscal years in the same metric
- **What:** `get_pit_fundamentals_asof` picks, per concept independently, the row with the latest `period_end`. If one concept has no tag for the latest year it silently falls back to an older year, so ROE or ROIC can be net income of year N over equity of year N-1.
- **Where:** `src/database.py:get_pit_fundamentals_asof` (lines 2353-2379); consumer `src/backtest.py:_derive_quality_metrics`.
- **Evidence:** Among tickers with >=3 of 6 core concepts known at the date, those whose core concepts do not share one `period_end`: 118/917 (12.9%) at 2016-06-01, 216/1,056 (20.5%) at 2020-06-01, 262/1,233 (21.2%) at 2025-06-01. Concept values a year or more older than the ticker's newest: 129/5,016, 244/5,942, 301/6,980. The age of the newest 10-K period at the rebalance date: median 152-153 days, p90 244-275 days. One ticker (NESR) used a period_end of 2017-12-31 on 2025-06-01 (2,709 days).
- **Why it matters:** Cross-year mixing adds noise to ROE/ROIC/FCF yield; there is no age cap, so a filer that stopped reporting still ranks. (Mixed `period_end` can also be 52/53-week date jitter; I did not separate it.)
- **Severity:** medium
- **Effort:** S (choose one fiscal period per ticker and take all concepts from it; reject if any core concept is missing or older than 15 months)
- **Unlocks / depends on:** F8.
- **Confidence:** medium-high

### F10. PIT backtest: universe selection is itself look-ahead, beyond ordinary survivorship
- **What:** The 1,292-ticker panel is today's investable set: today's S&P 500 membership, today's Finviz screen (market cap >$300M, price >$5, volume >100K, US), today's sector classification. Delisted, acquired and bankrupt names are absent, and so are names that were small in 2016 and large now (and vice-versa) in a way that depends on future outcomes.
- **Where:** `src/backtest.py:run_point_in_time_backtest` (tickers argument); `src/universe_builder.py`, `src/edgar_fundamentals.py` (CIK map from current `company_tickers.json`).
- **Evidence:** `pit_fundamentals` has 1,292 tickers and zero that are not in today's `universe` table. The report shows the equal-weight universe at +23.5%/yr vs SPY +16.2% over the 10 rebalances; per-date: 2020-06-01 universe +92.8% vs SPY +39.6%, 2025-06-01 +52.2% vs +30.2%. SYSTEM_MAP: 36 current universe names cannot even be resolved today (F4), so the panel already loses names that failed in the past six months.
- **Why it matters:** An equal-weight "survivor" universe beating SPY by 7 points a year is the signature of the bias, not of skill. The note says so for the bottom quintile; the same bias lifts every quintile and inflates the apparent edge of any "buy-and-hold the universe" baseline. The conclusion "no ranking power" still stands, but absolute returns and SPY-relative claims do not.
- **Severity:** high
- **Effort:** L (reconstruct historical constituents: SEC filer list by date, delisting returns, S&P membership history, or buy a point-in-time dataset such as Sharadar/CRSP)
- **Unlocks / depends on:** F4 (CIK-keyed universe), delisting returns.
- **Confidence:** high (the bias is acknowledged in the code; the quantification is mine)

### F11. The PIT backtest does not test the live score
- **What:** The backtest scores 4-5 inputs derived from XBRL with definitions that differ from the live pipeline: FCF yield is OCF minus capex over market cap with no SBC haircut, ROIC is net income over equity plus debt, and negative-equity names get no ROE/ROIC/debt value (`eq > 0` gate), while the live screen uses yfinance `.info` TTM numbers, SBC-adjusted FCF yield (`real_fcf_yield`) and 16 weighted metrics in `score_stock`. Only the five-metric percentile score (`quality_scorer`) is shared, and the live `universe.quality_score` additionally ranks on a different cross-section (F3).
- **Where:** `src/backtest.py:250-282`; `src/screener.py:_calculate_real_fcf`, `score_stock`; `src/quality_scorer.py`.
- **Evidence:** Code comparison (`_derive_quality_metrics` docstring: "ROIC uses net income / (equity + total debt) rather than NOPAT"; no `sbc` in the metric dict although `sbc` is loaded into `pit_fundamentals`). Annual-only 10-K data (median 153 days old, F9) versus live TTM `.info`.
- **Why it matters:** "No ranking power on mean returns, small edge on median" is a statement about a different scorer than the one choosing which 100 names get LLM budget each week. It cannot validate or invalidate `score_stock` weights or the 16-metric ideal values.
- **Severity:** medium
- **Effort:** M (rebuild live-equivalent metrics from `pit_fundamentals`, including SBC and TTM from 10-Qs, which the table already holds: 657,673 10-Q rows)
- **Unlocks / depends on:** F6, F8, F9.
- **Confidence:** high

### F12. 10-K excerpts given to Sonnet are frequently the wrong passage and always cut at 8,000 characters
- **What:** `_slice_section` starts at the LAST regex match of the item header. In filings that cite "Item 7. Management's Discussion..." or "Item 1A. Risk Factors" in later cross-references or notes, the last match is the cross-reference, not the section. Output is then trimmed to the first 8,000 characters.
- **Where:** `src/edgar_fetcher.py:219-243` (`_slice_section`, `starts[-1]`), `:245-253` (`_extract_sections`), `_RAW_SECTION_CAP = 8000`, `DEFAULT_MAX_CHARS = 15000`.
- **Evidence:** 40 cached live filings (`/opt/docker-apps/buffett-bot/data/edgar/*_10k.json`): 35/39 business, 32/39 risk-factor and 36/40 MD&A sections are at the 8,000-character cap. A heuristic check (starts with a cross-reference, contains 3+ "Item N." tokens in its first 1,500 characters) flags 34 of 80 risk/MD&A sections. Samples: CCI mda begins `Item 7. Management's Discussion and Analysis ... in our Annual Report on Form 10-K for the year ended December 31, 2024, which was filed with the SEC on March 14, 2025`; AD risk_factors begins with the Private Securities Litigation Reform Act safe-harbor text; DXCM, GRMN and LOW risk_factors begin with the table of contents (20 "Item" entries in 1,500 characters); CF risk_factors begins with a cross-reference. The cache stores `accession` but no filing date, and the prompt header says only "most recent annual filing".
- **Why it matters:** The "real 10-K text" that distinguishes the Sonnet stage from the Haiku stage is, in roughly 40% of the sampled sections, boilerplate, a table of contents or a cross-reference, and for the rest it is only the opening 8k characters. The fix is cheap and affects every paid Sonnet call.
- **Severity:** high
- **Effort:** S (take the longest matching span rather than the last match; require a length floor; store filing date; raise the per-section cap to a budgeted number)
- **Unlocks / depends on:** Better Sonnet moat/risk analysis; a test set of 10 filings with known section starts.
- **Confidence:** medium-high (heuristic flag, not manual review of all 80; 8 samples read by eye)

### F13. `revenue_cagr` is missing for ~72% of names, and `earnings_consistency` can never reach its ideal
- **What:** The trend metrics read yfinance annual statements whose oldest column is frequently NaN. `revenue_cagr` requires `earliest_rev > 0` and is silently dropped when the oldest column is NaN. `earnings_consistency` counts `ni_vals[i] > ni_vals[i-1]` over the same columns, where any comparison with NaN is False, so with 4 real years the maximum count is 3 but the scoring rule has `ideal: 4`.
- **Where:** `src/screener.py:722` (`ni_vals = ni_vals[::-1]`), `:740-742` (`earliest_rev = rev_vals[-1]`, `if earliest_rev > 0 and latest_rev > 0`); `config/screening_criteria.yaml:92-99` (`earnings_consistency ideal: 4`, weight 2.0; `revenue_cagr` weight 1.5).
- **Evidence:** Cache `historical._historical_years`: 876 names with 5 columns, of which exactly 1 has `revenue_cagr`; 341 names with 4 columns, of which 334 have it; the 5-column majority is consistent with a NaN oldest column (not confirmed by a live yfinance call). `earnings_consistency` values observed across 1,242 names: only 0, 1, 2, 3 (counts 5-column: 59/278/361/178; 4-column: 37/89/134/81); the configured ideal is 4.
- **Why it matters:** A 1.5-weight growth metric is absent for most of the universe (the score is scaled by confidence, so those names are not penalised but are scored on fewer metrics), and the 2.0-weight consistency metric tops out at (3-1)/(4-1) = 67% of its weight for everyone.
- **Severity:** medium
- **Effort:** S (drop NaN columns before computing; set `ideal` to the achievable maximum)
- **Unlocks / depends on:** Honest `score_stock` ordering; the screen's `score` is also what the weekly watchlist ranks on (F5).
- **Confidence:** high on the observed counts; medium on the NaN-column explanation

### F14. Dividend yield scale: percent in the data, ratio in the arithmetic; benchmark heuristic also fails
- **What:** yfinance `dividendYield` is now a percent number (KO 2.26 means 2.26%). `src/portfolio.py` multiplies it as if it were a ratio; `src/benchmark.py` divides by 100 only when the value is > 1.
- **Where:** `src/portfolio.py:344` (`position.dividend_yield = info.get("dividendYield")`) and `:466-472` (`est_annual = p.current_value * p.dividend_yield`, then the ASK 17% haircut and DKK conversion); `src/benchmark.py:52-54`; briefing format `src/briefing/text_formatter.py:173-176` (`{bm_div:.2%}`).
- **Evidence:** Cache: `dividend_yield` > 1 for 650 of 1,335 names; KO 2.26, PG 2.73, MSFT 0.74, JNJ 2.1; max ABR 34.94. `/opt/docker-apps/buffett-bot/data/benchmark/SPY_benchmark.json`: `"dividend_yield": 0.98` (a percent value below the >1 threshold, so it is left as 0.98 and the briefing renders `98.00%`); fetched 2026-07-02, 109 days old.
- **Why it matters:** The legacy portfolio tracker's estimated annual dividend (and the DKK-after-17%-tax figure shown to the user) would be roughly 100x overstated whenever `ask_dividend_projection` runs; the benchmark yield printed in the briefing is wrong for any index yield below 1%. The live DB path (`income_events` has 0 rows) does not use it, which limits current impact; the legacy `run_monthly_briefing.py` path does (`PortfolioTracker.get_portfolio_summary` at `src/portfolio.py:639` calls `get_dividend_summary` at `:444`).
- **Severity:** medium
- **Effort:** S (normalise at the fetch boundary like `debtToEquity`, with a `dividend_yield_is_ratio` tag; add a unit test with KO)
- **Unlocks / depends on:** Any future dividend/income logging for the ASK account.
- **Confidence:** high (data and code both read)

### F15. 2026-09-16 snapshot recorded a -45% day; there is no sanity gate on snapshots
- **What:** `daily_snapshot` stores whatever Alpaca returns. On 2026-09-16 the account equity was 58,743.6 with 3 of 8 positions.
- **Where:** `scripts/scheduler.py:354-411` (`daily_snapshot`); `src/accounts/alpaca.py:35-81` (`get_state`; the only guard is the quarantine-fraction check).
- **Evidence:** `portfolio_snapshots` 2026-09-14 equity 106,433.7 with 8 positions (AGM, AIZ, AL, CCI, CPRT, CTSH, DECK, NNI); 2026-09-16 equity 58,743.6, `gross_equity` 58,743.6 and `untradable_value` 0, 3 positions (AGM, CPRT, DECK); 2026-09-22 equity 102,552.9 with 8 positions. `equity_dkk` for the bad day is 382,790. The row is permanent (append-only) and feeds any drawdown, Sharpe or DKK equity curve.
- **Why it matters:** A partial broker response became a permanent -45%/+75% pair in the equity curve and the DKK curve. Health checks (`check_snapshot_freshness`, `check_frozen_position_marks`) look at age and frozen marks, not at day-over-day equity or position-count jumps.
- **Severity:** medium
- **Effort:** S (flag a snapshot when equity moves >10% or position count drops with no `decision_log` sell; store a `suspect` flag; re-fetch once)
- **Unlocks / depends on:** A trustworthy equity curve for the 12-month paper-trading evaluation.
- **Confidence:** high

### F16. Health checks read MAX(date) and so stay green through the F1 staleness and the news outage
- **What:** `check_data_freshness` tests only `MAX(fundamentals.date)`; `check_news_pipeline_effectiveness` returns OK when there are no news events in the last 3 days.
- **Where:** `src/health.py:285-328`, `:202-222`; `src/database.py:1598-1607` (`latest_fundamentals_date`).
- **Evidence:** 2026-10-18 digest: `[ok] data_freshness`, `[ok] news_pipeline` while 952 names are 112 days stale (F1) and `news_events` has no rows at all between 2026-08-11 and 2026-09-30 (daily counts of 21-22 per day through 08-10, then nothing until 72 rows on 10-01). The cause of the 51-day news gap is unverified (logs pre-10-18 gone; paid switches or the Finnhub key are candidates).
- **Why it matters:** The health system is the control against silent regressions, and the two most consequential data regressions of the last quarter pass it (the July NaN abort that these checks were built for is a third, now covered). A coverage metric (rows written per run, share of universe younger than 14 days) would have flagged both.
- **Severity:** medium
- **Effort:** S
- **Unlocks / depends on:** I6 (a run-level data-quality log).
- **Confidence:** high

### F17. News pipeline: fetch failures look like "no news", no catch-up window, and roundup headlines are attributed to unrelated tickers
- **What:** `get_company_news` returns `[]` on any exception or non-200; the daily run fetches only `days_back=1`; and Finnhub's company-news feed attaches market-wide roundup articles to every ticker in the `related` field, with no relevance filter before spending Haiku calls.
- **Where:** `src/news_fetcher.py:276-312` (`get_company_news`), `:317-340` (`get_news_for_tickers`, `days_back=1`), `:429-440`.
- **Evidence:** DB: 97 headlines appear under more than one ticker; example `CVS upgraded, General Mills downgraded: Wall Street's top analyst calls` is stored against ALL and WRB (published 2026-03-12) and `Texas Instruments upgraded, Circle downgraded...` against DECK (published 2026-04-09, detected 2026-05-18, 39-day lag). 687 of 1,532 events have no `event_type`; 231 have `haiku_material` NULL (never evaluated). Only the single top item per ticker per day is logged, so `news_events` is not a news archive. On 2026-10-18 the pipeline fetched 105 tickers, 37 with material news, 22 Haiku calls and logged `weekly_news_haiku pace floor reached`.
- **Why it matters:** A Finnhub 429 or outage is indistinguishable from a quiet day, and a missed 20:00 run loses that day's news permanently (no backfill). Roundup noise wastes the capped Haiku budget that guards held positions.
- **Severity:** medium
- **Effort:** S (distinguish error vs empty; widen to `days_back=3` with headline de-dup that already exists; drop items whose headline does not mention the company or whose `related` list has more than ~3 tickers)
- **Unlocks / depends on:** More reliable thesis-breaker detection for held names.
- **Confidence:** high on the DB evidence; medium on the Finnhub `related` behaviour (inferred from the stored rows)

### F18. No retry, backoff or error-rate guard anywhere; dependencies are unpinned; yfinance 404s on live names are treated as missing, not as failures
- **What:** No HTTP retry/backoff exists in `src/` or `scripts/` (grep for retry/backoff/Retry finds only comments about LLM re-eligibility). yfinance calls swallow every exception and return None (`logger.debug`). `requirements.txt` uses `>=` for every runtime dependency (`yfinance>=0.2.36`, `finvizfinance>=1.0.0`, `pandas>=2.1.0`) and the image tracks `:latest`.
- **Where:** `src/screener.py:_fetch_stock_data` (`except Exception ... logger.debug`), `:_fetch_historical_data`; `src/edgar_fetcher.py:73-84` (`_get`: one attempt, 30 s timeout, logs the status and returns None), `src/insider.py:_fetch_transactions`, `src/news_fetcher.py:get_company_news`, `src/fx.py:_fetch_live_rate`; `requirements.txt`.
- **Evidence:** The 2026-10-19 run logged `Processed 1359 stocks, 37 errors` as its only signal; no threshold stops a run that drops hundreds of names (a 429 storm would just thin the candidates and the DB write). The Monday 404s include live, large, widely held names, BK and CTRA, which Yahoo returns 404 for persistently (F4), so "404 = delisted" cannot be assumed. The finviz corruption in F5 is an example of an unpinned dependency's output being cached without validation.
- **Why it matters:** A yfinance schema change or a transient block degrades the screen silently and is then persisted as truth for a week (and, via F1, for months). Rebuilding the image tomorrow can change behaviour with no code change.
- **Severity:** medium
- **Effort:** M
- **Unlocks / depends on:** I6; F5 (validated universe fetch).
- **Confidence:** high on the code; medium on how often 429s occur (no pre-10-18 logs)

### F19. Valuation inputs: financials' cash flow produces absurd DCFs, Finnhub price targets never contribute, share classes are double counted
- **What:** (a) The DCF treats yfinance operating cash flow as free cash flow; for brokers, banks and insurers that figure includes customer and deposit flows. (b) `ValuationAggregator._get_finnhub_price_target` swallows non-200 responses, and no live valuation shows a Finnhub-sourced estimate. (c) Both classes of dual-class companies are in the universe and both get Sonnet analyses.
- **Where:** `src/valuation.py:_calculate_dcf_estimates`, `:_get_finnhub_price_target`, `:_merge_analyst_targets`; `src/universe_builder.py` (no share-class collapse).
- **Evidence:** `/opt/docker-apps/buffett-bot/data/watchlist.json` (2026-10-16): IBKR price $85.5, DCF fair value $873 ("Real FCF $15.6B"), average $338, margin of safety 75%, ranked first; 0 of 25 entries contain a Finnhub or merged "Yahoo + Finnhub" estimate (every analyst target is "Yahoo Finance Analyst Target"), consistent with the Finnhub price-target endpoint being premium-only (unverified). `deep_analyses`: GOOG (C, fair value 360, 2026-05-15) and GOOGL (C, fair value 360, 2026-05-22) both analysed, as are universe pairs FOX/FOXA, NWS/NWSA, LBRDA/LBRDK, CWEN/CWEN-A; their percentile ranks count one company twice.
- **Why it matters:** Financials get fair values an order of magnitude above price and enter the auto-trade shortlist; the "two-source analyst consensus" claim in the docstring is not what runs; each duplicate share class costs a Sonnet call and doubles a name's weight in the quality ranking.
- **Severity:** medium
- **Effort:** M (exclude or special-case financials from FCF DCF; log source availability; key analyses on CIK)
- **Unlocks / depends on:** F4 (CIK-keyed universe).
- **Confidence:** high on IBKR and GOOG/GOOGL; medium on the Finnhub cause

### F20. Mixed-timezone timestamps compare UTC SQL time with local naive Python time
- **What:** Tables written with `CURRENT_TIMESTAMP` are UTC; values built in Python with `datetime.now()` (container TZ `Europe/Copenhagen`) are local naive. Expiry logic compares the two.
- **Where:** `src/database.py:1014`, `:2555`, `:2584`, `:2613` (`datetime(expires_at) > datetime('now')`); `:907`, `:961` (`datetime.now()` used to build `expires_at`); `:125` etc. (`detected_at DEFAULT CURRENT_TIMESTAMP`); `src/fx.py:_write_cache` (local naive `fetched_at`).
- **Evidence:** `news_events.detected_at` 2026-10-18 18:03:15 for a job the log timestamps at 20:03:15 (UTC vs local); `haiku_screens.screened_at` 2026-10-14T07:02:16 (local, matches the 07:00 schedule); container env `TZ=Europe/Copenhagen`.
- **Why it matters:** A 1-2 hour skew on Haiku/Sonnet validity windows and 24-hour caches; harmless today but it makes any timestamp join across tables ambiguous and breaks as soon as DST changes the offset.
- **Severity:** low
- **Effort:** S (store UTC ISO with offset everywhere)
- **Unlocks / depends on:** n/a
- **Confidence:** high

### F21. `fundamentals` is not a usable history: a 27-day hole, a selected top-100 tail, and sparse columns
- **What:** The table has no row between 2026-06-29 and 2026-07-26 (the NaN-abort weeks), then only top-100 selections; it stores 8 metrics and no sector, market cap, share count, source-filing date or fetch timestamp; and `save_fundamentals` writes `real_fcf_yield or fcf_yield`, so a legitimate 0.0 is replaced by the unadjusted yield.
- **Where:** `src/database.py:856-879` (`save_fundamentals`), table at `src/database.py:39-420`.
- **Evidence:** Dates 2026-06-29 then 2026-07-26: the missing Mondays 07-06, 07-13, 07-20. Since 07-26 only 130 distinct tickers appear (14 snapshots of 100, membership drifting). `SELECT AVG(pe_ratio)` returns `inf` because one row (ALHC, 2026-04-07) holds `pe_ratio = inf`; `operating_margin` min -391.5 (-39,150%). `debt_equity` scale is consistent (ratio) across all dates (a 2026-06-29 vs 2026-10-19 comparison on 90 common tickers gives a median old/new ratio of 1.015), so the earlier percent/ratio fix is confirmed.
- **Why it matters:** Any attempt to validate the live screen against its own history (what the user asked for in "cohort validation") only has the top-100-by-score tail from July, which is selected on the screen's own output (look-ahead in the sample). The history that does exist is thinner than it looks.
- **Severity:** medium
- **Effort:** S (persist all scored names, add `as_of_price_ts`, `fetched_at`, sector, market cap, SBC-adjusted flag)
- **Unlocks / depends on:** F1; a genuine live point-in-time record (I1).
- **Confidence:** high

### F22. Retention policy would delete ~96% of weekly snapshots once they are two years old
- **What:** `run_retention_cleanup` deletes fundamentals rows older than 2 years where the day of month is not `01`, described as "downsample to monthly". Weekly snapshots are Mondays, so only a Monday that falls on the 1st survives.
- **Where:** `src/database.py:2402-2431` (`DELETE FROM fundamentals WHERE date < date('now','-2 years') AND strftime('%d', date) != '01'`).
- **Evidence:** Of the 25 distinct snapshot dates in the table, 1 (2026-06-01) falls on the 1st. Roughly 1 Monday in 30 is the 1st, so a typical year keeps about 1-2 snapshots, not 52 (and not 12 months of data: whole months vanish). First deletions would occur from April 2028. The pit_fundamentals table has no retention and is unaffected.
- **Why it matters:** The only live fundamentals history would be mostly destroyed exactly when it becomes long enough to test (2+ years).
- **Severity:** low
- **Effort:** S (keep the first snapshot of each month: `GROUP BY strftime('%Y-%m', date)`)
- **Unlocks / depends on:** n/a
- **Confidence:** high

### F23. Held positions and the live thesis monitor sit outside the data refresh
- **What:** NNI is held but is not in the `universe` table, has no fundamentals row and a cache file 24 days old; AGM and CCI are held but their fundamentals are the frozen 2026-06-29 row (F1); `paper_positions.entry_date` and `entry_price` are NULL on all 7 rows.
- **Where:** `scripts/scheduler.py:1277-1300` (position mirror writes `tier_at_entry` etc. but no entry data); universe sync; `src/database.py:upsert_paper_position`.
- **Evidence:** `SELECT ... FROM paper_positions p LEFT JOIN universe u`: NNI no universe row; AGM and CCI latest fundamentals 2026-06-29; the other four on 2026-10-19. `paper_positions` entry_date/entry_price NULL for AGM, AIZ, CCI, CPRT, CTSH, DECK, NNI.
- **Why it matters:** The positions the system has money in are exactly where F2's stale data lands most directly, and holding-period or entry-based logic (and journal alpha) has no entry date to work from other than `decision_log` (10 legacy buys).
- **Severity:** medium
- **Effort:** S (union held tickers into the weekly refresh set; backfill entry data from `decision_log`/Alpaca fills)
- **Unlocks / depends on:** F1.
- **Confidence:** high

### F24. Cohort validation silently drops dead names
- **What:** `_download_close_prices` returns no series for tickers Yahoo no longer knows, and `_window_return` returns None for them, so they leave the cohort statistics without a trace.
- **Where:** `src/backtest.py:912-931` (`_download_close_prices`), `:818-825` (`_window_return`), `:934-979` (`run_tier_cohort_validation`).
- **Evidence:** `cohort_validation_2026-10-16.json`: 185 names rated, 2 unpriced: AL and CPRX. AL is the delisted, held, $7,242 frozen position; both are C tier, so the C cohort alpha (-4.3pp, n=63) excludes the one name with a total-loss outcome. The first rating of 99 of the 185 names is dated 2026-04 (bulk load), so most cohorts are 6 months old.
- **Why it matters:** The bot's validation of "what the system excludes" counts survivors only; C-tier alpha is overstated by exactly the names whose C verdicts were right in the worst way. (The effect today is one name and is small; the pattern grows with the universe.)
- **Severity:** low
- **Effort:** S (treat unpriced-after-rating as -100% or last-trade return and report `n_unpriced`)
- **Unlocks / depends on:** F4, I5.
- **Confidence:** high

### F25. `deep_analyses.fair_value` from before the 9f22132 fix is junk for some rows; there is no price sanity bound
- **What:** Pre-fix rows stored the LLM midpoint as `fair_value` while `target_entry` came from the aggregated valuation, so the two disagree. Nothing bounds fair value against price.
- **Where:** `src/tier_engine.py` / `scripts/scheduler.py:friday_sonnet_batch` (fixed in commit 9f22132 for new rows); `deep_analyses` table.
- **Evidence:** 15 of 146 rows with both numbers have `target_entry > fair_value`. AMAT 2026-06-12: `fair_value` 9.57, `target_entry` 192.47, tier C, while the latest fundamentals price is $626.84 (fair value is 1.5% of price). QCOM 2026-07-02: fair value 79.5 vs target 144.2; 2026-08-06 fair value 167.5 vs target 125.0. 20 rows have NULL fair value.
- **Why it matters:** Briefings and cohort tables that read `fair_value` (rather than recomputing) show impossible numbers; AMAT's C verdict rests on that row.
- **Severity:** low
- **Effort:** S (backfill with the aggregated fair value; add a `fair_value/price` outlier flag)
- **Unlocks / depends on:** n/a
- **Confidence:** high on the rows; medium on whether AMAT's tier was caused by the bad value

### F26. Debt and ROE edge cases: negative-equity names have missing debt (not worst), and near-zero equity gives 100x ROE
- **What:** yfinance omits `debtToEquity` for negative equity and for banks; the screener turns that into None, so the debt component is skipped rather than penalised. ROE for near-zero equity is huge and earns full marks (`_compute_metric_score` caps at ideal) and the highest percentile in `quality_scorer`.
- **Where:** `src/screener.py:_de_percent_to_ratio`, `:_compute_metric_score`; `src/quality_scorer.py` (ROE and low-debt percentile ranks).
- **Evidence:** Cache (universe names): 62 with ROE present and `debt_equity` None, including JPM, STT, MTB, PNC, KEY, CFG; 37 names with ROE above 100% (PBT 99.8x, MAS 58.6x, GDDY 4.4x with D/E 573, CQP 15.2x, CCSI 12.5x). Fundamentals max ROE 90.4.
- **Why it matters:** Highly levered buyback-driven names rank top by ROE while their leverage is absent or under-weighted; banks have no debt signal at all, only a sector override on a different metric.
- **Severity:** low
- **Effort:** S (cap ROE or rank only when equity/assets > threshold; treat negative equity as worst)
- **Unlocks / depends on:** F11.
- **Confidence:** high

### F27. Benchmark and FX inputs are fragile in small ways
- **What:** (a) `benchmark.fetch_benchmark_return` uses adjusted closes (total return) while the stock side is price return from Alpaca fills; (b) `_calculate_ytd_return` starts from the first bar of the year, not the prior close; (c) FX uses a 24-hour cache, a stale-cache fallback with no age limit, and `history(period="5d")` last close (weekend carry); (d) the SPY benchmark cache file is 109 days old.
- **Where:** `src/benchmark.py:fetch_benchmark_return`, `:_calculate_ytd_return`; `src/fx.py:get_usd_dkk_rate` (`_read_cache(allow_stale=True)`), `_fetch_live_rate`.
- **Evidence:** `SPY_benchmark.json` fetched_at 2026-07-02T11:00; `/opt/docker-apps/buffett-bot/data/fx/usddkk.json` `{"rate": 6.6823, "fetched_at": "2026-10-18T22:00:18"}` (fresh, written by the snapshot job); `closed_trades` AD (1 day) `benchmark_return` 0.0, GIS 0.0137.
- **Why it matters:** Small, but alpha on 1-7 day trades is dominated by these effects (dividend inclusion alone is about 0.3% a quarter). The stale-forever fallback for FX would present a months-old rate as current without a flag; because DKK is EUR-pegged this is tolerable, but a USD/EUR move is not covered by the peg.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** n/a
- **Confidence:** medium

### F28. Insider signal is thin and asymmetric
- **What:** The tilt counts distinct 'S' (sell) names as negatives, including sell-to-cover and 10b5-1 sales, and ignores 'no activity' names (excluded rather than neutral). It is computed for 50 names of a list that is itself already truncated to 100 (F1).
- **Where:** `src/insider.py:_net_buyers`, `fetch_insider_signals` (`DEFAULT_FETCH_LIMIT = 50`); `scripts/scheduler.py:1215-1235`.
- **Evidence:** Across 95 cached insider files (13,725 transactions): codes S 3,468 vs P 436; A 3,700, M 3,351, F 2,041, G 441 (ignored). Log: `Applied insider-buying tilt to 49 stocks`. Filing lag p50 2 days, p95 5, max 185.
- **Why it matters:** 8x more sales than purchases and a 5% weight means the signal mostly encodes executive pay-sale patterns, and it cannot influence tier or sizing (only the quality rank); its predictive value is unproven.
- **Severity:** low
- **Effort:** M
- **Unlocks / depends on:** n/a
- **Confidence:** medium

### F29. Data hygiene debris
- **What:** The yfinance cache directory is never pruned; the fundamentals table holds an infinite value; three universe names have no sector.
- **Where:** `src/screener.py:_save_cached_data` (no cleanup); `src/database.py:save_fundamentals`.
- **Evidence:** `data/cache/` has 1,627 files: 268 are not in the universe (including mutual funds, SPAC units and warrants such as `AAADX`, `AACIW`, `AACOU`, from earlier universes), 37 are stale per F4. Sector `Unknown` for 2 and empty for 1 universe name (no sector-override scoring). `fundamentals.pe_ratio` of ALHC = inf (F21).
- **Why it matters:** Harmless individually, but the stale cache files can be mistaken for current data by any script that reads them without checking age.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** n/a
- **Confidence:** high

---

## Improvement ideas not tied to a weakness

### I1. Start an own live point-in-time record now
- **What:** Each Monday persist the raw yfinance `.info` + statements (or a normalised superset) for every name that is scored, with a fetched-at timestamp, as append-only rows (or zstd JSON per week). Roughly 1,359 rows x ~40 fields per week (about 70k rows a year) is tiny against the 180 MB DB.
- **Where:** new table next to `fundamentals`; `scripts/scheduler.py:monday_maintenance`.
- **Evidence:** F21: the existing table cannot answer "what did the screen know on date X" for 94% of names.
- **Why it matters:** In 12-24 months this gives a genuine out-of-sample live record against which the quality score and the LLM tiers can be tested without any vendor look-ahead.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** F1.
- **Confidence:** high

### I2. Key the universe on CIK and add a ticker-change/delisting registry
- **What:** Use the SEC `company_tickers.json` plus the submissions feed to map CIK to current ticker, exchange and status; set `in_universe = 0` after N failed fetches; collapse share classes to one analysis per CIK; keep delisted CIKs with their last price for survivorship-aware backtests.
- **Where:** `src/universe_builder.py`, `src/edgar_fetcher.py:get_cik`.
- **Evidence:** F4 (36 unresolvable), F19 (share classes), F24.
- **Why it matters:** It repairs the live universe and is the first step of F10's historical reconstruction.
- **Severity:** n/a
- **Effort:** M
- **Unlocks / depends on:** F4.
- **Confidence:** high

### I3. Move live fundamentals onto EDGAR companyfacts and use one definition set for live and backtest
- **What:** Reuse `edgar_fundamentals.build_pit_records` for the live screen (TTM from the already-stored 10-Q rows), keeping yfinance only for price, market cap and analyst targets.
- **Where:** `src/edgar_fundamentals.py`, `src/database.py` `pit_fundamentals` (945,674 rows, already loaded), `src/screener.py`.
- **Evidence:** F11 (definitions diverge), F6/F8/F9 (fixable once in one place), F13 (yfinance column NaN fragility).
- **Why it matters:** One code path whose backtest result directly validates the live score; removes the dependence on yfinance statement labels.
- **Severity:** n/a
- **Effort:** L
- **Unlocks / depends on:** F6, F8, F9.
- **Confidence:** medium (coverage gaps for foreign filers and unusual tags remain)

### I4. Report the user's real outcome: DKK equity curve with FX attribution
- **What:** Store the USD/DKK rate with each snapshot (it is implicit today as `equity_dkk / equity`), split each period's DKK return into asset return and FX return, and show the unhedged vs hedged-equivalent comparison; add Alpaca-vs-Nordnet cost modelling (DKK 5,000 lots).
- **Where:** `src/fx.py`, `scripts/scheduler.py:daily_snapshot`, `src/briefing/`.
- **Evidence:** From the snapshots: 2026-07-02 equity $100,476 / DKK 659,213 (rate 6.5609) to 2026-10-18 $106,276 / DKK 710,169 (rate 6.6823): USD +5.77%, DKK +7.73%, FX contributed about +1.85 pp of the DKK result. `income_events` has 0 rows; the briefing has no DKK curve.
- **Why it matters:** For a Danish ASK investor the DKK number, including FX and the ASK mark-to-market tax (lagerbeskatning, which the code models only as a 17% dividend haircut in the legacy `portfolio.py`), is the actual result; attribution tells whether stock selection or the dollar produced it.
- **Severity:** n/a
- **Effort:** M
- **Unlocks / depends on:** F15 (a trustworthy curve).
- **Confidence:** high on the numbers; medium on the ASK tax characterisation (domain claim from memory, not checked against the code or a source)

### I5. Add a point-in-time dataset with delisting returns before using the backtest to justify anything
- **What:** Either buy a survivorship-free dataset (Sharadar, CRSP), or reconstruct from EDGAR filer lists and Stooq/Yahoo delisting prices; at minimum apply -100% to cancelled equity and last-price for acquisitions.
- **Where:** `src/backtest.py:_load_prices`, `_forward_return`.
- **Evidence:** F7 (reorganisation artifact), F10 (+7 pp/yr survivor drift in the equal-weight baseline), F24.
- **Why it matters:** It is the only way to make "the quality screen adds nothing / the exclusion rules add something" a number rather than a narrative.
- **Severity:** n/a
- **Effort:** L
- **Unlocks / depends on:** I2.
- **Confidence:** high

### I6. A per-run data-quality log
- **What:** One row per scheduled data job: universe size, fetched OK, 404s, 429s, rows written, share of universe younger than 14 days, median age of inputs used by LLM stages, fetch durations. Alert on thresholds instead of on `MAX(date)`.
- **Where:** `src/health.py`, `scripts/scheduler.py`.
- **Evidence:** F16, F18; today's only signal for a failed Monday is a single log line.
- **Why it matters:** Converts silent degradations into numbers visible in the Discord digest.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** none
- **Confidence:** high

### I7. Pin dependencies and add contract tests for vendor shapes
- **What:** A lock file (or exact pins) for yfinance/finvizfinance/pandas; small recorded-response tests for `.info` units (`dividendYield`, `debtToEquity`), statement column labels, NaN oldest column, and finviz ticker shape.
- **Where:** `requirements.txt`, `tests/`.
- **Evidence:** F5 (finviz output change), F13 (NaN column), F14 (dividend unit).
- **Why it matters:** The three vendor-shape defects found here all slipped through 1,012 passing tests.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** none
- **Confidence:** high

---

## What I could not verify

- Why the Finviz and Wikipedia universe fetches failed from 2026-09-25 and what produced the doubled-first-letter tickers on 2026-09-18: the scheduler log starts 2026-10-18 and no network calls were made. Evidence is the cache file and the 25/25 curated-list match only.
- Why `news_events` has no rows from 2026-08-11 to 2026-09-30 (outage, switch off, key problem, or Finnhub returning empty).
- Whether BK, CTRA, AVB, EQR, EA, HOLX, CPRX and the other 404 names were acquired, renamed or delisted. Yahoo and SEC both fail to resolve them (the common signature), but the cause per name is unchecked (BK may be a rebrand).
- Whether yfinance's oldest annual column is NaN for the 5-column names (F13): inferred from the counts in the cache, no live call. Likewise the exact mechanism of the CHRD +19,097% (F7), the Finnhub price-target endpoint being premium-only (F19) and whether the Finnhub `related` field is the source of roundup headlines (F17).
- The effect size on the PIT result of fixing F6/F8/F9 (not re-run; would need a full price history pull for 1,292 tickers, which the brief excludes).
- The real number of dual-class `sharesOutstanding` errors in the DCF (yfinance reports one class for some multi-class names); not checked because `sharesOutstanding` is not stored in the cache.
- Every 10-K section in the EDGAR cache by eye: 8 were read manually, the rest are heuristic flags.
- Alpaca-side facts (what the account endpoint actually returned on 2026-09-16) and any live Alpaca data; only the stored snapshot rows were inspected.
- The two plan documents named in the system map were not available, so "what the data layer was meant to do" is inferred from code docstrings.
