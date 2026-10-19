# Signal audit: scoring, valuation, tiers, regime, evidence (2026-10-19)

Auditor: signal-auditor (read-only). Repo at main `efc181b`. Live data read from
`/opt/docker-apps/buffett-bot/data/` (DB opened `mode=ro` from `python3 -I`; cache JSON, `watchlist.json`,
`registry.json`, `analyses/*.json`, `backtest/*.json`). No LLM calls, no network calls of any kind
(yfinance is not even installed on the host, so nothing was re-fetched). Scripts used for the
computations live in the session scratchpad (`pit_neutral.py`, `live_scores.py`, `corr.py`); they
re-implement `score_stock`, `_compute_metric_score`, `compute_quality_scores` and `_project_dcf`
line for line against the cached inputs.

## Headline numbers

| Question | Answer (live data) |
|---|---|
| Why has S never been assigned? | S needs WIDE moat **and** HIGH conviction (1 of 293 analyses: FDS) **and** price <= 0.75 x aggregator fair value (7 of 64 price-checked scheduled assignments). Expected S count under independence ~0.1. FDS itself was +27% above target. |
| What does the quality score rank today? | Since 2026-07-26 Monday writes fundamentals and quality scores for only the **top 100** screener names (`top_n`), percentile-ranked among themselves. 1,039 other tickers keep June scores on a different scale; 28 of the top 30 `quality_score` values are stale, and 47 of the 50 Haiku calls on 2026-10-14 went to stale-scored names, including too-hard industries (PBT, CRMD, DMLP, GPOR). |
| Sector neutralisation | Raw composite: Financial Services 47% in top quintile, Utilities 0%, Real Estate 5%, Basic Materials 5% (expected 20%). Sector-neutral: 16-25% for every sector. On 10 PIT years neither version predicts returns (mean Spearman +0.014 raw vs +0.001 neutral, 4-metric version). |
| Fair-value dispersion | Watchlist (25 names, 2026-10-16): max/min source ratio median 2.5x, up to 31x (IBKR DCF = 10.2x price). Aggregator vs LLM fair value on 78 stored analyses: median ratio 0.94, 37% outside +/-25%. |
| Evidence base | The "moat NONE n=22, -10.5 pp" cohort that justified the 3.0.0 exclusion is 20 migrated legacy rows whose moat was mapped to NONE by a key mismatch; 13 of them were rated STRONG. PIT backtest: median Q5 > Q1 in 8/10 years (one-sided sign test p = 0.055), mean Spearman +0.018. |

---

## A. Quant screen (`src/screener.py`, `config/screening_criteria.yaml`)

### F1. The quality score is now computed over 100 names, and the other 1,039 keep stale June scores on a different scale
- **What:** `monday_maintenance` feeds `compute_quality_scores` with the output of `screen_tickers`, which truncates to `criteria.top_n` (100). Percentiles are therefore relative to the top 100 only; every other universe ticker keeps whatever `quality_score` it got before July. The two scales are then mixed in every consumer.
- **Where:** `scripts/scheduler.py:monday_maintenance` (lines 1193-1238); `src/screener.py:screen_tickers` lines 1033-1036 (`candidates = candidates[:top_n]`); `config/screening_criteria.yaml` `top_n: 100`.
- **Evidence:** `fundamentals` rows per date: 1,071-1,093 per week until 2026-06-29, then exactly 100 on every date from 2026-07-26 to 2026-10-19. `universe.last_screened`: 952 rows stuck at 2026-06-29, 100 at 2026-10-19. Of the top 30 by `quality_score`, 28 were last scored 2026-06-29 (PBT 98.5, CRMD 91.5, DMLP 90.6, MO 88.2 ...). The live top-100 scores span 16.7-85.0 (median 49), so the 100th-best screened name today scores ~17 while a June name keeps 80+.
- **Why it matters:** `quality_score` orders the Haiku queue (`get_unscreened_tickers`, `get_expiring_haiku_tickers`, `get_haiku_passes_without_analysis`, all `ORDER BY u.quality_score DESC`), gates the tier engine's fair-price exception (>=80) and fills the briefing leaderboard. LLM budget is being allocated by a number that means two different things.
- **Severity:** high
- **Effort:** S (score the full screened set before truncation, or drop `top_n` for Monday; null out scores not refreshed this week)
- **Unlocks / depends on:** F2, F27; a trustworthy funnel.
- **Confidence:** high (row counts and the code path agree exactly; the pre-July 1,080-row weeks are consistent with the live YAML only being mounted/edited on 2026-07-02 17:28, file mtime, which I could not prove; see "could not verify").

### F2. Hard filters gate only the screener, not the LLM queue: too-hard industries still get Haiku and Sonnet calls
- **What:** Every universe row has `in_universe = 1`; the Haiku/Sonnet selectors rank the universe table by `quality_score` without re-applying the too-hard, market-cap, ADTV or positive-P/E filters. Stale high scores (F1) put excluded names at the top.
- **Where:** `src/database.py:get_unscreened_tickers` (2537), `get_expiring_haiku_tickers` (2564), `get_haiku_passes_without_analysis` (2593); filters in `src/screener.py:screen_tickers` 870-917.
- **Evidence:** Haiku batch of 2026-10-14: 50 screens, only 3 of the tickers appear in any post-2026-07-26 weekly top 100. Screened that day: PBT (royalty trust), CRMD (yfinance industry "Biotechnology", too-hard), DMLP and GPOR ("Oil & Gas E&P", too-hard), INCY, BSM. DMLP, BSM, INCY and TPL also have Sonnet deep analyses. The June fundamentals snapshots contain 48-49 too-hard-industry rows; the post-July ones contain 0.
- **Why it matters:** paid calls (50/week Haiku cap was exhausted) go to names the screen says are out of scope; the "too hard" list is not actually enforced end to end.
- **Severity:** high
- **Effort:** S
- **Unlocks / depends on:** F1.
- **Confidence:** high.

### F3. `revenue_cagr` is missing for 875 of 876 tickers with a five-column history, and the DCF growth rate inherits the bug
- **What:** yfinance returns a fifth, mostly-empty oldest column. `revenue_cagr` takes `rev_vals[-1]` without `dropna()`, so `earliest_rev > 0` is False for NaN and the metric is silently skipped. The same pattern is in the valuation DCF.
- **Where:** `src/screener.py:_fetch_historical_data` lines 729-745; `src/valuation.py:_calculate_dcf_estimates` lines 407-421.
- **Evidence:** cache census (1,625 files): `_historical_years == 5` with `revenue_cagr` present: 1; absent: 875. `_historical_years == 4`: 334 present, 7 absent. AIZ, AGM, CCI all show `revenue_cagr` missing in the held-name breakdown (F16).
- **Why it matters:** a 1.5-weight metric drops out for about two thirds of the universe, which then also lowers `score_confidence` and, through `effective_score = score x confidence`, the ranking (F8). In the DCF, "growth = min(FCF CAGR, revenue CAGR)" silently becomes "FCF CAGR only", the less conservative choice, for the same two thirds.
- **Severity:** high
- **Effort:** S (`.dropna()` before taking endpoints, and compute years from the dates)
- **Unlocks / depends on:** fixes F4 at the same time.
- **Confidence:** high for the screener (data census); medium-high for valuation (same yfinance frame and identical code shape; not re-run live).

### F4. `earnings_consistency` has an unattainable ideal (4) and counts the empty column as a year
- **What:** With 4 real annual columns there are at most 3 year-over-year comparisons; the NaN fifth column adds a comparison that is always False. The ideal of 4 is never reached, so the metric's ceiling is (3-1)/(4-1) = 0.67.
- **Where:** `src/screener.py:_fetch_historical_data` 711-726; yaml `earnings_consistency: ideal 4, min 1, weight 2.0`.
- **Evidence:** distribution over the cache: 0 -> 95, 1 -> 339, 2 -> 437, 3 -> 234, 4 -> 0. CTSH (1 growth year of 3) scores 0 on a 2.0-weight metric.
- **Why it matters:** 0.67 of 2.0 weight is unreachable for everyone; harmless for ranking only if equally unreachable, but it also makes "one down year in three" worth zero, which over-penalises cyclical but durable businesses.
- **Severity:** medium
- **Effort:** S
- **Unlocks / depends on:** F3.
- **Confidence:** high.

### F5. `net_share_change` reads "Share Issued" first, which includes treasury stock, so buybacks held in treasury are invisible
- **What:** The label priority is `["Share Issued", "Ordinary Shares Number", "Diluted Average Shares"]`. "Share Issued" counts treasury shares; companies that keep repurchased shares in treasury show no reduction.
- **Where:** `src/screener.py:_fetch_historical_data` lines 777-795.
- **Evidence:** cache value vs EDGAR `shares_outstanding` CAGR over ~4 years (pit_fundamentals, 10-K): AFL +0.09% vs -5.59%/yr; TRV +0.33% vs -2.16%; JNJ 0.00% vs -1.72%; MO 0.00% vs -1.72%; ALL +0.01% vs -1.58%. Companies that retire shares agree (AAPL -2.51% vs -2.58%; L -4.45% vs -4.55%). PG shows -16.6% in the cache vs -0.7% in EDGAR, a separate anomaly.
- **Why it matters:** a 1.0-weight "capital allocation" metric scores some of the most consistent repurchasers (insurers, staples) as non-buyers, the opposite of what Buffett-style capital-allocation scoring wants.
- **Severity:** medium
- **Effort:** S (prefer "Ordinary Shares Number"; sanity-check against split events)
- **Unlocks / depends on:** none.
- **Confidence:** medium-high (consistent pattern across 5 treasury holders vs retirers; the yfinance row contents themselves were not re-fetched).

### F6. `fcf_yield` and `real_fcf_yield` come from different data sources, so the "SBC haircut" number is often larger than the unhaircut one
- **What:** `fcf_yield` uses Yahoo `info.freeCashflow` (a levered, TTM figure); `real_fcf_yield` uses the latest annual statement OCF - capex - SBC. `earnings_quality` uses the Yahoo figure.
- **Where:** `src/screener.py:_fetch_stock_data` 494-515; `_calculate_real_fcf` 529-584.
- **Evidence:** in 474 of 1,084 names where both exist and differ, `real_fcf_yield > fcf_yield` (impossible if they shared a base). CPRT: `fcf_yield` 3.43%, `real_fcf_yield` 4.62%, `earnings_quality` 0.60 (Yahoo FCF 885M / NI 1,484M), which scores 0.14 of 1.5 on earnings quality although CPRT converts earnings to cash well.
- **Why it matters:** three metrics with combined weight 4.0 rest on inconsistent definitions; the SBC adjustment, the one deliberate improvement, is swamped by source noise.
- **Severity:** medium
- **Effort:** S (derive all FCF metrics from one statement and one period)
- **Unlocks / depends on:** F7.
- **Confidence:** high.

### F7. Double counting and period mixing inside the 16 metrics
- **What:** (a) `real_fcf_yield` falls back to `fcf_yield` when SBC is missing, so the same number is scored twice (2.5 weight). (b) ROE (2.0), ROIC (2.5) and ROE consistency (2.0) are all net-income-over-capital. (c) TTM `.info` fields (ROE, margins, P/E) are mixed with last-fiscal-year statement fields (ROIC, CAGRs).
- **Where:** `src/screener.py` 513-515; yaml weights.
- **Evidence:** 134 of 1,218 names have `real_fcf_yield == fcf_yield` exactly. Rank correlation over the cache: fcf vs real fcf 0.67; ROE vs ROIC only 0.31, with 36 names showing ROE > 20% but ROIC < 5% (CROX 42% vs -2.8%, VIRT 54% vs 4.4%), which is a period/definition mismatch, not economics. Valuation-type metrics (P/E 0.8 + FCF 0.5 + real FCF 2.0) are 14% of the 22.9 total weight.
- **Why it matters:** the effective weights are not the configured ones, and two "return on capital" readings of the same company disagree for reasons unrelated to the business.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** F12 (any re-weighting needs a test).
- **Confidence:** high.

### F8. Missing data is punished quadratically in the screener and rewarded in the composite
- **What:** `score_stock` sums only present metrics but keeps missing weight in the denominator of `confidence`; the sort key then multiplies `score x confidence`, so a name with fraction f of weight present and perfect scores ranks at f^2. `compute_quality_scores` does the opposite: it renormalises over present metrics, so missing ROE or D/E (typically negative equity or a bank) simply disappears.
- **Where:** `src/screener.py:score_stock` 255-282, `ScreenedStock.effective_score` 357-359, `_sort_key` 1015-1030; `src/quality_scorer.py:compute_quality_scores` 205-224.
- **Evidence:** median screener confidence by sector: Financial Services 0.73 (41% missing ROIC and D/E, 30% missing FCF, 35% missing current ratio) vs 0.93 for most sectors; median effective score FS 5.1 vs Technology 9.4. In the composite, MCD (ROE and D/E missing, negative equity) scores 80.1 raw on three metrics; SBUX likewise 59.0.
- **Why it matters:** the same data gap moves a company down the screen and up the composite. Banks are effectively screened out of the top 100 (0 banks in it today) while negative-equity companies look like low-leverage, high-quality names in the composite.
- **Severity:** medium
- **Effort:** S-M (pick one rule; for structural gaps use industry-specific metrics, see F16)
- **Unlocks / depends on:** F16.
- **Confidence:** high.

### F9. The payout-ratio rule scores zero-dividend compounders at 0 and is not monotone above its max
- **What:** `payout_ratio` has `min 0, ideal 0.35, max 0.80`. A value at or below `min` scores 0; above `ideal` the code returns 1.0 up to `max` and then decays as `1 - (v - max)/max`, so 0.9 still scores 0.875.
- **Where:** `src/screener.py:_compute_metric_score` 312-317; yaml `payout_ratio`.
- **Evidence:** 443 cached names have payout 0, including CPRT and DECK (buyback-only, scored 0 of 0.5). CCI's payout 1.73 scores 0; a payout of 0.9 would score 0.875. Berkshire-style "retain and compound" scores worst.
- **Why it matters:** small weight, but it is backwards for the stated philosophy and the both-bounds branch semantics differ from the docstring ("zero at min/max boundary").
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** none.
- **Confidence:** high.

### F10. Loss-making companies pass the "negative P/E" filter through the forward P/E fallback
- **What:** `pe_ratio = trailingPE or forwardPE`. yfinance omits `trailingPE` when trailing EPS is negative, so a loss-maker with positive forward estimates gets a positive P/E, passes `if pe <= 0: continue`, and can score 1.0 on P/E. The P/E valuation method has the same `trailingEps or forwardEps` fallback.
- **Where:** `src/screener.py:_fetch_stock_data` line 470, filter 914-917; `src/valuation.py:_calculate_pe_based_value` 239.
- **Evidence:** 208 cached names above $300M have TTM `net_income < 0` and a positive `pe_ratio`: KHC (P/E 11.4, NI -$3.4B), F (6.4, -$7.4B), IP (11.1, -$2.7B), FLUT, COIN, CRL.
- **Why it matters:** the quality screen's only profitability hard filter is porous; analyst forecasts enter both the quality score and fair value.
- **Severity:** medium
- **Effort:** S
- **Unlocks / depends on:** none.
- **Confidence:** high.

### F11. "ROIC" is net income / (book equity + debt): meaningless for financials and pass-through entities
- **What:** The highest-weighted metric (2.5) is not ROIC (NOPAT / operating invested capital net of cash). For lenders it divides by a funding base, for royalty trusts by near-zero equity, and negative-equity companies drop out.
- **Where:** `src/screener.py:_fetch_historical_data` 649-681 (same proxy in `src/backtest.py:_derive_quality_metrics`).
- **Evidence:** AGM (Farmer Mac) ROIC 0.6% -> score 0 although ROE is 13.5%; NNI 3.7% -> 0; PBT ROIC 8,797% and ROE 9,977% (it is the #1 composite name at 98.5); 172 names have negative "ROIC"; 41% of Financial Services and 40% of Energy names have none.
- **Why it matters:** the metric Buffett is cited for is measured in a way that is noise for a fifth of the universe.
- **Severity:** medium
- **Effort:** M (NOPAT and operating capital need EBIT, tax rate, cash; or use industry metrics, F16)
- **Unlocks / depends on:** F16.
- **Confidence:** high.

### F12. The 16 screener weights have no empirical basis, and the screener score has never been tested
- **What:** Weights are hand-set (comments like "Was 1.5", "Highest weight — Buffett's favorite"). The PIT backtest tested `compute_quality_scores` (the 5-metric composite), not `score_stock`, which is what actually picks the top 100 and the weekly watchlist.
- **Where:** `config/screening_criteria.yaml`; `src/backtest.py:run_point_in_time_backtest` 207-209 (calls `compute_quality_scores`).
- **Evidence:** no file in `docs/research/` evaluates `score_stock`; `git log` on the yaml shows only hand edits. The PIT backtest also uses OCF - capex without the SBC haircut (`_derive_quality_metrics`), so even the composite it tested differs from live.
- **Why it matters:** the component that decides what reaches the LLM and the auto-trader is unvalidated; "unproven" is the honest status.
- **Severity:** high (as an evidence gap)
- **Effort:** M (the PIT table holds most inputs; trend metrics need multi-year as-known series)
- **Unlocks / depends on:** pit_fundamentals (exists); a delisted-filer universe for survivorship.
- **Confidence:** high.

### F13. The top 100 is tilted to large caps and away from whole sectors
- **What:** Within a 0.5-point score band the sort prefers larger market cap; the large-cap P/E override relaxes P/E for names >= $10B; F8's confidence penalty hits sectors with structural gaps.
- **Where:** `src/screener.py:_sort_key` 1015-1030; yaml `cap_overrides.large` (its comment says "vs base 15", but base is 18).
- **Evidence:** 2026-10-19 top 100: 72 large / 24 mid / 4 small vs 42% / 34% / 23% of the scored universe. Sector counts vs proportional expectation: Utilities 0 (5.6), Energy 0 (8.1), Real Estate 3 (8.5), Industrials 25 (12.6), Technology 19 (13.3). The 16 Financial Services names are insurers, brokers, card networks and ratings agencies; no banks.
- **Why it matters:** the funnel silently becomes a large-cap industrials/tech screen; for a concentrated 8-stock portfolio this is a sector bet nobody chose.
- **Severity:** medium
- **Effort:** S
- **Unlocks / depends on:** F14.
- **Confidence:** high.

## B. Composite quality score (`src/quality_scorer.py`)

### F14. The composite is a sector bet, not a quality factor; neutralising it changes who is picked but not (measurably) returns
- **What:** Percentile-ranking ROIC, ROE, FCF yield, op margin and low D/E across all sectors rewards sectors with structurally high margins/low book capital and penalises asset-heavy ones. FCF yield (20%) is a value signal, not quality.
- **Where:** `src/quality_scorer.py:compute_quality_scores`, `_BASE_WEIGHTS`.
- **Evidence (live cache, 1,357 equities, re-implemented scorer):**

  | sector | n | median raw | median neutral | share in raw top 20% | share in neutral top 20% |
  |---|---:|---:|---:|---:|---:|
  | Financial Services | 158 | 64.9 | 49.4 | 47% | 16% |
  | Technology | 164 | 54.9 | 50.2 | 30% | 25% |
  | Energy | 106 | 55.7 | 52.9 | 25% | 18% |
  | Industrials | 162 | 54.5 | 51.7 | 20% | 23% |
  | Healthcare | 148 | 43.1 | 51.5 | 15% | 18% |
  | Real Estate | 114 | 50.5 | 51.5 | 5% | 16% |
  | Basic Materials | 106 | 41.0 | 47.7 | 5% | 21% |
  | Utilities | 69 | 41.8 | 47.9 | 0% | 19% |

  Worked examples (raw -> sector-neutral): CPRT 54.2 -> 77.9, CTSH 52.2 -> 66.4, NEE 47.0 -> 61.8, SO 47.3 -> 64.7, HD 41.2 -> 63.5; JPM 75.2 -> 54.2, BRK-B 73.0 -> 51.1, AGM 54.4 -> 30.5, AIZ 73.5 -> 60.4, CCI 74.0 -> 55.8.

  **PIT test (10 June rebalances 2016-2025, 10,559 obs, 4 non-price components recomputed from pit_fundamentals, forward returns from pit_backtest.json):** mean yearly Spearman stored score +0.029, raw 4-metric +0.014, sector-neutral 4-metric +0.001; all three positive in 6 of 10 years. Top-quintile sector mix in 2025: raw holds 4 REITs of 89 and 3 utilities of 61; neutral holds 18 and 14.
- **Why it matters:** the "quality" label is not earned; whichever version is used, it is a funnel, not alpha. Neutralisation is cheap and removes an unchosen sector bet at no measurable cost.
- **Severity:** medium
- **Effort:** S (rank within sector, fallback to universe below n=8, as in my script)
- **Unlocks / depends on:** F16 for a proper industry grouping.
- **Confidence:** high on composition; medium on the PIT comparison (FCF yield excluded because PIT prices are not stored; current sector labels used historically).

### F15. Royalty trusts, MLPs and tiny-equity companies top the composite
- **What:** Pass-through entities have ~90% margins, little book equity and no debt, so they max out every percentile. Percentile ranking hides absurd magnitudes.
- **Where:** `src/quality_scorer.py`; no entity-type filter in `src/screener.py`.
- **Evidence:** universe top by `quality_score`: PBT 98.5 (Permian Basin Royalty Trust: op margin 0.93, ROE 99.8x, ROIC 88x), DMLP 90.6, BSM 81.7, TPL 80.0, SD 80.0. PBT, DMLP and BSM were Haiku-screened on 2026-10-14; DMLP and BSM have Sonnet analyses.
- **Why it matters:** depleting-asset trusts are the opposite of durable compounders and burn budget (F2).
- **Severity:** medium
- **Effort:** S (exclude trusts/partnerships by quoteType/industry, winsorise inputs)
- **Unlocks / depends on:** F1, F2.
- **Confidence:** high.

### F16. Industry blindness, concretely, on the held names (screener metric scores and why)
- **What:** Banks, insurers, lenders and REITs flow through FCF-, ROIC- and D/E-based rules; `sector_overrides` only loosen D/E and current ratio. Held names were scored as below (cached 2026-10-19 data, base -> sector -> large-cap rules; value, 0-1 score, weight).
- **Where:** `src/screener.py:score_stock`; yaml `sector_overrides`.
- **Evidence:**

  | ticker (industry) | eff. score / max | conf | what drives it |
  |---|---|---|---|
  | AGM (Credit Services, GSE lender) | 5.65 / 22.0 | 0.68 | D/E 19.9 -> 0 even with FS max 5; ROIC 0.6% -> 0; FCF, real FCF, earnings quality, margin stability, revenue CAGR all missing; op margin 0.70 -> 1.0 (interest income counted as revenue) |
  | AIZ (P&C insurance) | 14.19 / 21.8 | 0.93 | real FCF yield 10.8% -> 1.0 and earnings quality 2.04 -> 1.0 (reserve build counted as cash earnings); current ratio 0.38 -> 0; op margin 11.7% -> 0.22 |
  | CCI (tower REIT) | 5.38 / 22.0 | 0.83 | ROE and D/E missing (negative/near-zero book equity); ROE consistency std 14.6 -> 0; payout 1.73 -> 0; ROIC 1.6% -> 0; earnings consistency 1 -> 0; real FCF 8.0% -> 1.0 |
  | NNI (Credit Services) | 4.89 / 22.0 | 0.82 | ROE 6.8% -> 0, ROIC 3.7% -> 0, FCF missing, net share change +8.3%/yr -> 0; not in the DB universe at all (bought from the weekly watchlist, F29) |
  | CPRT (Specialty Business Svcs) | 16.53 / 22.7 | 1.00 | clean: ROIC 16.7% -> 0.78, margins 1.0; loses on payout 0 (F9) and earnings quality 0.14 (F6) |
  | CTSH (IT Services) | 15.39 / 22.7 | 1.00 | earnings consistency 1 -> 0; revenue CAGR 2.8% -> 0.23 |
  | DECK (Footwear) | 18.71 / 22.7 | 1.00 | ROIC 35.6% and real FCF 9.0% -> 1.0; payout 0 -> 0 |

  In the composite the same blindness flatters financials instead (F14: FS 47% in top quintile; JPM 75 raw with D/E missing).
- **Why it matters:** for lenders and insurers the screen measures accounting artefacts, in both directions. Two of seven holdings are financials and one is a REIT; the system cannot tell a good insurer from a bad one.
- **Severity:** high
- **Effort:** L for proper industry models (below), S for interim exclusion/flagging
- **Unlocks / depends on:** industry grouping and new XBRL concepts. What industry-aware scoring needs: (1) grouping by yfinance `industry` mapped to ~12 groups (banks, insurers, asset managers/brokers, REITs by type, utilities, lessors, royalty/MLP, capital-intensive cyclicals, asset-light services, consumer brands, tech, healthcare); (2) per-group metric sets: banks ROA, ROTCE, efficiency ratio, NPL/charge-offs, CET1, P/TBV; insurers combined ratio, reserve development, book value per share CAGR, P/B, ROE; REITs AFFO/FFO yield and payout, net debt/EBITDA, occupancy; utilities allowed vs earned ROE, rate-base growth, FFO/debt; lessors net spread, leverage, residual-value impairments; (3) data fields from EDGAR: InterestIncome/Expense, Deposits, ProvisionForLoanLosses, PremiumsEarned, PolicyholderBenefits, RealEstateDepreciation and gains (FFO = NI + RE D&A - gains), Goodwill and Intangibles (tangible equity); (4) neutralisation: winsorised z-scores within group, fallback to sector when n < 8.
- **Confidence:** high (numbers re-computed from the cache with the repo's rules).

### F17. REIT and utility treatment stops at debt and current ratio
- **What:** The Real Estate override relaxes D/E, FCF yield and current ratio, but REITs are still scored on EPS-based P/E (depressed by depreciation), EPS-based payout, ROE stability on tiny book equity and earnings consistency, with no FFO/AFFO concept anywhere. Utilities get only D/E, revenue CAGR and FCF overrides.
- **Where:** yaml `sector_overrides`; `src/valuation.py` P/E and Graham methods (EPS, book value).
- **Evidence:** CCI breakdown in F16 (P/E 33.4 on EPS, payout 1.73 -> 0, ROE consistency 14.6). Top-100 today: 3 REITs, 0 utilities. In the PIT run 4 of 89 REITs and 3 of 61 utilities reach the raw top quintile.
- **Why it matters:** REITs and regulated utilities are among the most "Buffett-like" predictable cash streams; the screen cannot see them and the valuation misprices them.
- **Severity:** medium
- **Effort:** M
- **Unlocks / depends on:** F16.
- **Confidence:** high.

## C. Valuation (`src/valuation.py`)

### F18. "Fair value" is a weighted mean of incommensurable numbers, and one outlier sets the target
- **What:** It averages a 12-month analyst price target (a forecast that already embeds expected return), 18x EPS (implies ~4.4% perpetual growth at a 10% discount if all earnings are distributable), the Graham number (15x P/E and 1.5x P/B cap), and two DCFs with a 12x terminal (implies 1.5% perpetual growth). Weights: medium 1.0, low 0.5; no source is ever "high". It is not a plain mean, and it is not robust.
- **Where:** `AggregatedValuation.average_fair_value` lines 52-64; source methods 186-475.
- **Evidence:** 25-name watchlist of 2026-10-16. Median FV/price by source: analyst 1.28, P/E 1.18, Graham 0.81, DCF real FCF 1.19, DCF owner earnings 1.35. Max/min source ratio per name: median 2.5x; IBKR 31.4x, STRL 7.1x, EAT 6.4x, PLAB 6.0x. IBKR's weighted mean is 338 on a price of 85.5 (MOS +75%) because the DCF says 873; the median of its sources gives MOS -13%. Switching mean -> median flips the 25% MOS test for 3 of 25 names (IBKR, AX, AA).
- **Why it matters:** the target entry price, the A/S decision, the deployment ranking and the near-fair-value sell gate all read this one number. With a 2.5x typical spread between sources, the "25% margin of safety" is smaller than the measurement noise.
- **Severity:** high
- **Effort:** S (median, or trimmed mean; drop analyst targets from "intrinsic" value) / M (industry-appropriate method selection)
- **Unlocks / depends on:** F19, F21.
- **Confidence:** high.

### F19. DCFs on operating cash flow are applied to brokers and insurers, producing the buys
- **What:** For financials, OCF includes customer-fund and reserve movements; discounting it as owner cash is wrong. There is no industry switch.
- **Where:** `src/valuation.py:_calculate_dcf_estimates`.
- **Evidence:** IBKR DCF $873 vs price $85.5 ("Real FCF $15.6B", 10.2x price). AIZ was bought 2026-07-03 with `fair_value` 417.1 and MOS 33% (decision_log id 8) and rated A on 2026-07-10 ("$279 <= $315"), while the LLM's own fair value was $260 (aggregator/LLM = 1.6). AIZ's cached real FCF (10.8% yield) is 1.47x its net income (earnings quality 2.04): reserve build, not distributable cash. HCA ($641 implied aggregator FV vs $362 price; ROE and D/E missing in the cache) and GDDY ($140 vs $86; D/E 573x on near-zero equity), both FCF-heavy names with negative or near-zero book equity, are the other A-tier entries.
- **Why it matters:** the A tier, and with it the buy list, is partly produced by a valuation method that does not apply to the business.
- **Severity:** high
- **Effort:** S (skip DCF for Financial Services/insurance/brokers; use P/B vs ROE or excess-return models) / M (proper models)
- **Unlocks / depends on:** F16 grouping.
- **Confidence:** high for IBKR (stored estimates), medium-high for AIZ (aggregator FV recovered from the stored target / 0.75 and the decision snapshot; per-source split not stored).

### F20. DCF internals: abrupt growth fade, no balance sheet, and an owner-earnings variant that double counts growth
- **What:** (a) Up to 8% growth in years 6-10 then a 12x terminal (1.5% perpetual) is a cliff; terminal value is 43-54% of total. (b) No cash or non-operating assets are added (levered FCF is fine for debt, but excess cash is ignored). (c) Growth comes from 3-4 year endpoint CAGRs (cyclical endpoints dominate). (d) Owner earnings = OCF - min(capex, D&A) - SBC adds back growth capex but projects the **same** growth rate, so it gets growth without paying for it; it then enters at the same medium weight, so the DCFs carry 2.0 of at most 4.5 weight.
- **Where:** `_project_dcf` 297-312; `_calculate_dcf_estimates` 374-469.
- **Evidence:** my re-run of `_project_dcf`: FV/FCF multiple 10.8x at g=0, 15.3x at 5%, 19.0x at 8%, 25.3x at 15% (capped). FCF yield needed for price <= 0.75 x DCF alone: 12.4% at g=0, 7.0% at 8%, 5.3% at 15%. In the watchlist the owner-earnings DCF median FV/price is 1.35 vs 1.19 for real FCF.
- **Why it matters:** conservative on the terminal, aggressive on owner earnings; the net bias depends on capex intensity, i.e. on industry again.
- **Severity:** medium
- **Effort:** S-M
- **Unlocks / depends on:** F3 (growth input).
- **Confidence:** high.

### F21. The deterministic and LLM fair values disagree a lot, and neither per-source estimates nor the fair-value source are stored for past analyses
- **What:** Since 3.0.0 the aggregator sets the target; before that the LLM target did unless it failed to parse (127 of 293 rows have no LLM target). `deep_analyses.fair_value` held the LLM midpoint until 9f22132 (2026-10-17), after the last analysis. Individual estimates are not persisted anywhere except the weekly `watchlist.json`, which is overwritten.
- **Where:** `src/tier_engine.py:assign_tier` 194-207; `src/database.py` `deep_analyses` schema.
- **Evidence:** 78 rows have a computed (non-cent) target, so aggregator FV = target / 0.75 can be compared with the LLM midpoint: median ratio 0.94, p10 0.70, p90 1.81, 37% outside +/-25%. Extremes: AZO LLM "fair value" $26.35 vs aggregator $2,264 and AMAT $9.58 vs $257, which are parser captures of the wrong dollar figures, not opinions (bold markdown defeats `extract_field`; 143 of 193 cached analyses have no parsed target, 123 have moat type "unknown").
- **Why it matters:** the fair value that decides tiers cannot be audited after the fact, and the LLM number shown in older briefings was partly garbage.
- **Severity:** medium
- **Effort:** S (store the estimates JSON and `fair_value_source` per analysis)
- **Unlocks / depends on:** any future valuation backtest.
- **Confidence:** high (target/0.75 inference is exact for rows where the target is not a whole-cent value).

### F22. The 25% margin of safety is enforced once, then decays to -12.5% for 180 days
- **What:** A requires price <= 0.75 x FV at analysis time. Deployment then buys any valid A as long as the fresh MOS >= -12.5% (`quality_ceiling_pct`), and the analysis stays valid up to 180 days.
- **Where:** `src/tier_engine.py` 280-307; `src/deployment.py:plan_buys` 222-226; `config.analysis_max_age_days`.
- **Evidence:** the policy window is 37.5 pp of MOS; nothing re-tiers a name on price between analyses except the next scheduled run.
- **Why it matters:** the "margin of safety" headline is not what governs execution; a name can be bought 12.5% above the same fair value that once required 25% below.
- **Severity:** medium
- **Effort:** S (re-evaluate the price leg of the tier at trade time, or make the ceiling explicit as the policy)
- **Unlocks / depends on:** none.
- **Confidence:** high.

## D. Tier engine (`src/tier_engine.py`)

### F23. Why S never occurs: two rare conditions multiplied, plus a parser that used to bias against both
- **What:** S = WIDE moat and HIGH conviction (quality "wonderful") and gap <= 0 against target = 0.75 x aggregator FV.
- **Where:** `assign_tier` 226-294; `src/analysis_parser.py:extract_rating_checked` (fixed d922f88, 2026-10-01); `src/analyzer.py:ANALYSIS_SYSTEM_PROMPT`.
- **Evidence:**
  - moat x conviction over 293 analyses: WIDE/MEDIUM 117, NARROW/MEDIUM 107, NARROW/LOW 37, NONE/MEDIUM 14, NONE/LOW 10, NARROW/HIGH 6, WIDE/LOW 1, **WIDE/HIGH 1** (FDS, 2026-05-22). HIGH conviction overall: 7 of 293. After the parser fix (28 analyses since 2026-10-01): 0 HIGH.
  - Price leg: of the 64 scheduled assignments with a computed gap, 7 were at/below target (11%), 25 B, 32 C; median gap +60% (p25 +38%, p75 +88%). The one WIDE/HIGH name, FDS, was "+27% above target $178".
  - Expected S count assuming independence: 293 x (1/293) x 0.11 ~ 0.1.
  - Structural reasons: the prompt asks for "skeptical" analysis and gives no rubric for HIGH; the longest-match parser in use before 2026-10-01 resolved "HIGH ... medium-term" to MEDIUM and "STRONG ... moderate" to MODERATE (NARROW), biasing away from both conditions (magnitude unknown, raw responses were not stored). On price, wonderful businesses usually trade at 20-35x earnings, where the 18x P/E source is 0.5-0.7 of price and Graham is lower still (watchlist median Graham FV/price 0.81 even among cheap names), so "wonderful and 25% below this fair value" is close to contradictory by construction.
- **Why it matters:** the S tier, its 25% sizing and 3-tranche logic are dead code; the system's real buy tier is "narrow/wide moat, MEDIUM conviction, statistically cheap", which is a value screen, not "wonderful company at a fair price".
- **Severity:** high (design)
- **Effort:** M (decide what S should mean; e.g. WIDE + MEDIUM/HIGH + price <= FV, with FV from an appropriate method)
- **Unlocks / depends on:** F18, F19 (a fair value worth trusting); a conviction rubric in the prompt.
- **Confidence:** high on counts; medium on the parser's historical contribution.

### F24. The "extreme premium (>50%)" cliff is only 12.5% above fair value, and half of priced quality names land in C
- **What:** The gap is measured against the target (0.75 x FV), so gap >= 50% means price >= 1.125 x FV. A wide-moat business 13% above the aggregator's fair value is filed with no-moat names as C.
- **Where:** `EXTREME_PREMIUM_THRESHOLD = 0.50` line 74; `assign_tier` 329-356.
- **Evidence:** 32 of 64 price-checked scheduled assignments were C by premium. C names are re-analysed every 30 days (`C_TIER_ANALYSIS_DAYS`, `src/database.py:960`), so the cliff also drives Sonnet spend.
- **Why it matters:** the label misleads the user, and tier C mixes "bad business" with "good business, slightly pricey", which also contaminates the cohort statistics (F31, F32).
- **Severity:** medium
- **Effort:** S (express thresholds as price/FV; keep quality and price as separate fields)
- **Unlocks / depends on:** none.
- **Confidence:** high.

### F25. Tier sizing in the tier engine and the briefing is not what the trader does
- **What:** `TIER_CONFIG` says S max 25% in 3 tranches, A 15% in 2; staged entries go to `price_alerts` and the briefing. `plan_buys` sizes every buy as `MAX_POSITION_PCT (15%) x TIER_WEIGHT` (S 15%, A 12.75%) in one order. `TIER_WEIGHT` for B/None/C is unused for buys because only S/A are buyable.
- **Where:** `src/tier_engine.py` 62-68, 362-398; `src/deployment.py` 37-39, 236-246.
- **Evidence:** `staged_entry_suggestion` is called only in bulk_load, scheduler (for alerts) and the two briefing formatters.
- **Why it matters:** the user is told one sizing policy, the account follows another.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** none.
- **Confidence:** high.

### F26. Most historical tiers were not price-checked, so tier history is weak evidence
- **What:** Before ae3647b (2026-08-11) external valuation was not always supplied; without it `current_price` is None (the prompt has no "Current Price" line) and quality names default to B.
- **Where:** `assign_tier` 266-278; `scripts/bulk_load.py` 440-451; `scripts/scheduler.py` 1531-1538.
- **Evidence:** tier_history reasons: 87 "no price data — watching" B (73 bulk_load, 14 scheduled), 97 news-triggered re-analyses, 20 migrated legacy rows, and only 64 scheduled assignments with a price gap.
- **Why it matters:** "B" in the history mostly means "no price", so tier cohorts conflate rule regimes.
- **Severity:** medium (evidence quality)
- **Effort:** S (tag rows by ruleset and by whether a gap existed; filter cohorts accordingly)
- **Unlocks / depends on:** F31, F32.
- **Confidence:** high.

### F27. The fair-price exception reads a quality score that is either stale or top-100-relative
- **What:** WIDE+HIGH, gap <= 10%, `quality_score >= 80` promotes B to A. The score comes from `universe.quality_score` (F1).
- **Where:** `assign_tier` 309-327; scheduler 1544-1546.
- **Evidence:** only 2 of the 100 freshly scored names are >= 80 (relative to the top 100), vs 21 stale names. The exception has fired 0 times in tier_history.
- **Why it matters:** dead in practice, and arbitrary if it ever fires.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** F1.
- **Confidence:** high.

## E. Regime detection and deployment

### F28. The regime classifier reduces to "did SPY close at a 52-week high today"
- **What:** Four votes, majority wins, ties go to the first-inserted vote (Counter order: P/E first). VOO trailing P/E has been above the 23 "overvalued" threshold on every logged day, and VIX inside the 12-30 "normal" band on every day, so those two votes are constants. `drawdown >= 0` (a new 52-week closing high) adds a second "overvalued" vote and wins the 2-2 tie.
- **Where:** `src/bubble_detector.py:classify_market_regime` 158-246, `REGIME_THRESHOLDS` 59-71; `src/deployment.py:target_invested_pct`.
- **Evidence:** regime_log 111 rows: market P/E 24.2-28.0 (all >= 23), VIX 14.2-20.4; 101 fair_value / 10 overvalued, all "moderate". Target invested flips between 90% and 80% on that one condition. The P/E bands (15/23/30) are not tied to any stated source; no research note tests regime-timed deployment.
- **Why it matters:** a 10 pp deployment swing keyed to an essentially random daily event, with no evidence that timing helps an 8-stock buy-and-hold portfolio. In practice it is also non-binding: the paper account is 71% invested vs a 90% target because no S/A names are available (F29).
- **Severity:** medium
- **Effort:** S (fix one target, or keep only a drawdown-based "buy more in a crash" rule)
- **Unlocks / depends on:** a longer trade history to test anything.
- **Confidence:** high on constants and tie logic; "overvalued days = new-high days" is consistent with the code but not checked against SPY prices.

### F29. Two universes and a four-stage intersection leave the buy funnel empty
- **What:** `weekly_screen` screens `src/universe.py:get_stock_universe()` (cached Finviz list, no force-include, $500B cap), not the DB universe, and keeps the top 50 by screener score. `weekly_auto_trade` then needs: top 30 of that watchlist, a Haiku pass, and a valid S/A deep analysis, which come from the DB-universe queue ordered by stale scores (F1).
- **Where:** `scripts/scheduler.py:weekly_screen` 147-200; `weekly_auto_trade` 760-826.
- **Evidence:** watchlist of 2026-10-16: 25 names, 7 not in the DB universe, only 3 in the Monday top 100; 5 have deep analyses, all B or C. Valid analyses today: 113 (A 3, B 97, C 13). NNI, a holding, is not in the DB universe at all. Uninvested gap ~$20k (71% vs 90%) since July.
- **Why it matters:** the auto-trader cannot buy under ruleset 3.0.0 except by coincidence; the paper record will say little about the rules.
- **Severity:** medium
- **Effort:** S (one universe; feed watchlist from analysed S/A names)
- **Unlocks / depends on:** F1.
- **Confidence:** high.

### F30. Holdings bought under legacy rules are never re-gated by the current ones
- **What:** Breaker-only sells (3.0.0) are coherent with buy-and-hold, but all 7 holdings were bought in July under the old MOS gate (decision_log ids 2-13): DECK and CPRT at tier B, AIZ/CCI/NNI/AGM with no tier at all (AGM has no buy row in decision_log; `paper_positions.entry_date` is NULL for all). None would pass today's S/A requirement (latest tiers: AIZ A, CPRT B, DECK B, AGM B, CCI C, CTSH C, NNI C).
- **Where:** `src/deployment.py:plan_sells` 251-333.
- **Evidence:** as above; CTSH is 20.4% of equity vs `MAX_POSITION_PCT` 15%; as a C name it is rotation-eligible, so only the overweight trim applies.
- **Why it matters:** the live paper portfolio measures legacy rules, not 3.0.0; performance should be reported per entry ruleset.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** journal ruleset stamps (exist for new rows).
- **Confidence:** high.

## F. Evidence

### F31. The "moat NONE underperforms" result that justified the 3.0.0 exclusion is a data-migration artefact
- **What:** 20 of the 22 moat-NONE first ratings are legacy rows migrated from `registry.json` on 2026-04-07. The migration read `analysis.get("moat_rating")`, a key the v2 schema does not have (it stores `moat.durability`), and defaulted to "none".
- **Where:** `src/database.py` registry migration (~lines 2467-2526, `moat_str = analysis.get("moat_rating", "none")`); `src/deployment.py:EXCLUDED_MOATS`; cohort_validation rows of 2026-10-17.
- **Evidence:** in `registry.json` the 20 migrated names have `moat.durability` = strong for 13 (ALH, ALSN, ATR, AWR, CPK, CWST, CWT, IDA, NJR, NWE, NWN, SWX, TXNM) and moderate for 7; none is "none". CWT's stored thesis begins "owns irreplaceable monopoly infrastructure ... a textbook moat" next to `moat_rating = NONE`. 9 of the 22 are utilities, 20 of 22 share one rating date, and the other 2 were rated in October (days of history). The cohort's -10.5 pp / 2-of-22 is therefore "regulated utilities and a few industrials vs SPY, April-October 2026", mislabelled.
- **Why it matters:** a hard exclusion rule now in production (`is_quality_excluded`) has no valid evidence behind it, and the same mislabelled rows still feed the monthly cohort job and the WIDE cohort (13 rows missing from it).
- **Severity:** critical (evidence), though the rule itself is plausible and cheap to keep
- **Effort:** S (re-map the 20 rows from `moat.durability`, rerun cohort validation, restate the 3.0.0 rationale)
- **Unlocks / depends on:** F32.
- **Confidence:** high.

### F32. The tier-A cohort (+20.6 pp, 7 of 8 ahead) is too small and too contaminated to support anything
- **What:** n = 8 over one 3-6 month window in one market. Members: AIZ (insurer DCF artefact, F19), BIO (a migrated row with moat NONE, which today's rules would call C), AZZ, CTSH, GDDY, HCA, IT, PYPL; 3 of 8 Technology.
- **Where:** `src/backtest.py:run_tier_cohort_validation`; cohort_validation table.
- **Evidence:** A alphas since rating: -0.16, 0.01, 0.11, 0.20, 0.22, 0.25, 0.47, 0.56; the mean is carried by two names. HIGH conviction (n = 6) has alpha -0.9 pp since rating and -7.2 pp at 90d (n = 3), the opposite of what the conviction scale should show.
- **Why it matters:** none of the cohort numbers, positive or negative, is evidence yet; they are the start of a forward record.
- **Severity:** medium
- **Effort:** S (report confidence intervals and cohort dates in the briefing; exclude migrated rows)
- **Unlocks / depends on:** 12-24 months of ratings under one ruleset.
- **Confidence:** high.

### F33. What the PIT backtest actually supports
- **What:** The note's conclusions are mostly fair, but the edge it reports is at the edge of chance and applies to a different score than the one that picks stocks.
- **Where:** `docs/research/pit-backtest-2026-10-17.md`; `data/backtest/pit_backtest.json`.
- **Evidence:** recomputed from the JSON: median Q5 > median Q1 in 8 of 10 years (one-sided sign test p = 0.055), mean yearly spread +2.1 pp, median +5.2 pp; pooled Spearman +0.018, yearly mean +0.026, positive 6 of 10. The score tested is the 5-metric composite on the full cross-section (live now uses top-100 percentiles, F1), with FCF without the SBC haircut; universe survivorship inflates Q1. I checked the earlier PIT dedupe concern: in `pit_fundamentals` there are 0 (ticker, concept, period_end, filed_date) 10-K groups with conflicting values, so that bug no longer affects the data.
- **Why it matters:** supports "weak tilt away from blow-ups"; does not support any weight, any threshold, or the screener score.
- **Severity:** medium
- **Effort:** n/a
- **Unlocks / depends on:** F12.
- **Confidence:** high.

### F34. The composite's insider tilt applies only to the names already at the top
- **What:** Insider data is fetched for the top 50 by the first-pass score and percentile-ranked among those only; everyone else is unaffected. Signal = count of net buyers minus sellers over the window, which routine 10b5-1 selling drives negative for large caps.
- **Where:** `scripts/scheduler.py` 1215-1235; `src/insider.py:_net_buyers` 97-120, `DEFAULT_FETCH_LIMIT = 50`.
- **Evidence:** code path; 95 cached insider files. No research note tests it.
- **Why it matters:** it can only reshuffle the top half of a 100-name list by up to ~5 points, and penalises the largest names; unproven and small.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** Form 4 history for a PIT test.
- **Confidence:** medium (effect size not measured).

---

## Improvement ideas not tied to a weakness

### I1. Persist a valuation audit trail per analysis
- **What:** Store the `AggregatedValuation.to_dict()` JSON, `fair_value_source`, price and gap on every `deep_analyses` row and every decision.
- **Where:** `src/database.py:save_deep_analysis`; scheduler 1542-1560.
- **Evidence:** today only `watchlist.json` holds per-source estimates, overwritten weekly (F21).
- **Why it matters:** makes every future valuation question answerable from the DB.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** valuation backtests; F18 decisions.
- **Confidence:** high.

### I2. PIT-test the screener score and a sector-neutral composite before changing weights
- **What:** Extend `run_point_in_time_backtest` to compute `score_stock` from pit_fundamentals (trend metrics from the as-known multi-year series) and report raw vs sector-neutral, with per-year Spearman and median spreads, plus a delisted-filer universe.
- **Where:** `src/backtest.py`.
- **Evidence:** F12, F14.
- **Why it matters:** the only way to move from "plausible" to "evidence-based" weights.
- **Severity:** n/a
- **Effort:** M
- **Unlocks / depends on:** pit_fundamentals (present), stored PIT prices.
- **Confidence:** high.

### I3. Give conviction and moat a rubric, and calibrate it
- **What:** Define HIGH/MEDIUM/LOW and STRONG/MODERATE/WEAK with observable criteria (e.g. ROIC > cost of capital for 10 years, share stability) and track the label distribution per run.
- **Where:** `src/analyzer.py:ANALYSIS_SYSTEM_PROMPT`.
- **Evidence:** HIGH used 7 times in 293; 0 since the parser fix (F23).
- **Why it matters:** the tier engine's top bucket depends on a label the model almost never emits.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** F23.
- **Confidence:** medium.

### I4. Report everything in DKK and against a Danish-relevant benchmark
- **What:** Cohort and trade alpha are vs SPY in USD; for an ASK investor the relevant comparison is the DKK return of a global or US index fund available on the account, net of the 17% ASK tax.
- **Where:** `src/benchmark.py`, `src/backtest.py:compute_cohort_validation`.
- **Evidence:** `portfolio_snapshots.equity_dkk` exists; cohort tables are USD/SPY only.
- **Why it matters:** USD/DKK moves can swamp the alphas being measured.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** fx history (cached).
- **Confidence:** medium.

### I5. Replace the regime engine with a pre-registered, testable rule or a constant
- **What:** Either fix target invested at one level, or use a single crash rule (e.g. raise to 100% after a 20% drawdown) and log it for later evaluation.
- **Where:** `src/bubble_detector.py`, `src/deployment.py`.
- **Evidence:** F28.
- **Why it matters:** removes a parameter with no evidence and no current effect.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** none.
- **Confidence:** medium.

## What I could not verify

- Why the live screen produced ~1,080 rows per week and scored too-hard industries until 2026-06-29 despite `top_n` truncation existing since February. The data fit the live `screening_criteria.yaml` not being effective before its 2026-07-02 17:28 mtime (an empty `criteria.scoring` skips both truncation and the too-hard filter), but there are no container logs to prove it.
- The pre-2026-10-01 parser's actual effect on moat and conviction labels: raw model responses are not stored, only parsed JSON.
- The yfinance "Share Issued" vs "Ordinary Shares Number" row contents (F5): inferred from EDGAR comparisons, not re-fetched (no yfinance on the host, and no network calls were made).
- That the 10 "overvalued" regime days are exactly SPY 52-week-high closes (F28): no stored SPY prices.
- Finnhub analyst targets: in the 2026-10-16 watchlist all 25 analyst estimates are "Yahoo Finance Analyst Target", so Finnhub resolved for none of them that week; whether it ever resolves in production (key, rate limit, plan) I could not check without a network call.
- Sector-neutral PIT results include only 4 of the 5 composite components (no as-known price for FCF yield) and use today's sector labels.
- Per-source fair values for any deep analysis other than via `target / 0.75`; the aggregator split for AIZ, HCA and GDDY is inferred, not observed.
