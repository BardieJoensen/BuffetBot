# AI pipeline audit (2026-10-19, main @ efc181b)

Auditor: ai-pipeline-auditor. Read-only. No LLM or other paid API call was made. The live DB was opened only with `mode=ro` from `python3 -I`. No yfinance, EDGAR or Finnhub request was made. The only other sources were `docker logs buffett-bot-scheduler` (read-only, which only go back to 2026-10-18 12:48), the JSON files under `data/analyses` and `data/edgar`, and `data/backtest`.

## Headline

1. **Roughly 90% of the stored LLM labels (265 of 293 `deep_analyses` rows) were produced by a rating parser that the repo itself fixed on 2026-10-01, and the raw model responses were never stored.** The cohort evidence behind the 3.0.0 exclusion rules (moat NONE, LOW conviction) rests on those labels (F1).
2. **The current parser still mis-reads the model's markdown-bold labels.** 63% of stored rows have `moat_sources = ["unknown"]`, customer concentration parses as "high" for 76% of files, and a synthetic reproduction turns MODERATE into STRONG (WIDE moat) (F2).
3. **The breaker-only sell rule is unproven and structurally blind to part of the book.** The breaker check has never run in production (0 rows in `thesis_breaker_events`; the first live run on 2026-10-18 skipped both REVIEW cases for lack of Sonnet budget). 3 of the 7 held names (CCI, CTSH, NNI) are C tier and are not news-monitored at all, so no breaker can fire for them. The 0/19 replay had no positive control, although real candidate positives are sitting in `news_events` (F6, F7).
4. **A pipeline cliff arrives on Saturday 2026-10-24.** 382 Haiku screens expire, the Friday Sonnet queue pool drops from 299 to 140 names, and re-screening 891 names at 50 a week takes 18 weeks. Clearing the whole backlog would cost about $9 once (F13).
5. **Cost is not a constraint.** The estimate is about $2.1-2.3 a month at the caps, against the README's $10-15. The caps and queue limits are 10-50x smaller than would be sensible, and are code-owned, so a `.env` edit cannot change them (F20).

### Model and prompt timeline (from git, not from the DB, which stores no model id)

| deep_analyses rows dated | deep model (source) | light model | parser state |
|---|---|---|---|
| 2026-04-07 .. 2026-07-24 (about 225 rows) | claude-sonnet-4-5-20250929 (hard-coded in analyzer.py before c1b6602) | claude-haiku-4-5-20251001 | longest-option-anywhere (buggy) |
| 2026-07-25 .. 2026-09-30 (40 rows) | claude-sonnet-5 (c1b6602), assuming prompt redeploy | claude-haiku-4-5 | same buggy parser |
| 2026-10-01 .. (28 rows) | claude-sonnet-5-5 (cbc0818) | claude-haiku-4-5 | earliest-match parser (d922f88) |

The live `.env` carries no model override, so the code defaults apply.

### Prices assumed (2026-10-19; I could not load the `claude-api` skill, so these are from memory and unverified for Sonnet 5.5)

| model | input $/MTok | output $/MTok | Batch API |
|---|---|---|---|
| Haiku 4.5 | 1 | 5 | half price |
| Sonnet 5.5 | 3 | 15 | half price |

Cache reads are 0.1x input. `docs/research/light-model-ab-2026-10-18.json` supports the Haiku price: 480 input and 120 output tokens work out to $0.00108 a call, exactly the recorded figure. The code comment says Sonnet 5.5 costs the same as Sonnet 5. Token counts use 3.6-4 characters per token.

---

## Findings

### F1. Most stored labels come from a parser the repo later declared wrong, and the raw responses were never saved
- **What:** Until 2026-10-01 `extract_rating_checked` picked the longest option that appeared anywhere in a section, so "HIGH - ... medium-term" parsed as MEDIUM and "LOW - high debt" parsed as HIGH. Every `deep_analyses` row dated before 2026-10-01 carries labels from that parser. `deep_analyses` stores only the parsed labels (no raw text, model id, prompt version or parser version), so the labels cannot be re-parsed.
- **Where:** `src/analysis_parser.py:extract_rating_checked` (commit d922f88); `src/database.py:save_deep_analysis` (column list); `src/backtest.py:run_tier_cohort_validation`.
- **Evidence:**
  - Rows dated before 2026-10-01: 265 of 293 (90%).
  - Conviction mix under the old parser (Sonnet 4.5 era, full-context rows): MEDIUM 84%, LOW 14%, HIGH 2%. Under the new parser (Sonnet 5.5, n=12): MEDIUM 50%, LOW 50%.
  - Of the 24 moat-NONE rows, 7 have a thesis text that praises a moat with no negation. Five of them are in the 2026-04-07 first batch: ATR ("genuine competitive advantages"), TXNM ("the strong moat inherent to all utility monopolies"), CWST ("genuine competitive moats"), CPK, ALSN. The pessimistic default (`options[-1]` = NONE) is the likely mechanism. The text check is a crude regex heuristic.
  - `deep_analyses` schema: `ticker, analyzed_at, tier, conviction, moat_rating, moat_sources, fair_value, target_entry, investment_thesis, key_risks, thesis_breakers, expires_at`.
- **Why it matters:** `deployment.is_quality_excluded` bars moat NONE and LOW conviction from buys on the strength of the cohort test (n=22 at -10.5pp; n=25 at -7.5pp). That test is run on labels of unknown accuracy, and there is no way to measure the error rate afterwards.
- **Severity:** high
- **Effort:** S to start storing the raw text, M to re-analyse the cohort. The re-run costs roughly $0.02 per name (see F20).
- **Unlocks / depends on:** an auditable evidence base for every rule that keys off an LLM label. Depends on F2 (a parser or schema fix) first, otherwise the re-run reproduces the same errors.
- **Confidence:** high on the facts (git diff, schema, counts). Medium on how many labels are wrong, because that cannot be measured.

### F2. The current parser mis-reads bold-labelled responses, so most structured fields are empty or defaulted
- **What:** The models write `**Durability:** MODERATE`, but `extract_field` only matches lines that start with the bare label. For bold output it falls back to scanning the whole section. Fields that fall back to nothing become "unknown", None, "" or 0. The two rating fields that scan the whole section can take a word from the wrong line, for example a "Strong brand" in the Type line, so they can be wrong even though the parser logs no failure.
- **Where:** `src/analysis_parser.py:extract_field` and `parse_analysis`, specifically the `extract_field(...) or section` fallbacks.
- **Evidence (193 stored analysis files):**
  - Moat type "unknown": 123 files (64%). `moat_sources == ["unknown"]` in 184 of 293 DB rows (63%), and still 12 of 28 rows (43%) under Sonnet 5.5.
  - Insider ownership null: 123 (64%). Domestic revenue null: 133 (69%). Existential risks empty: 169 (88%). Target entry null: 143 (74%).
  - Customer concentration parsed "high": 147 of 193 (76%). Where the model's own text says LOW, MODERATE or HIGH, the parsed value disagrees in 109 of 152 cases. Switching-cost rating parsed 0 in 132 of 152 cases where the model gave a number.
  - Synthetic reproduction with the current parser (stubbed third-party imports, no API call), bold labels, with the Type line containing "Strong brand": "Durability: MODERATE" parsed as `strong`, which maps to WIDE. "Customer Concentration: LOW" parsed as `high`. Switching cost 4/5 parsed as 0. With the asterisks removed, the same text parses correctly.
- **Why it matters:** Moat durability sets WIDE/NARROW/NONE, which feeds the tier, the buy exclusion and the position weight. Whenever the label line is bold, the moat comes from the first of STRONG, MODERATE, WEAK or NONE found anywhere in the section.
- **Severity:** high
- **Effort:** S for `lstrip("*_ ")` plus label normalisation. M for the better fix, a forced tool call with an enum schema (see the improvement ideas).
- **Unlocks / depends on:** F1, and any future consistency measurement. Re-parsing is only possible for text stored from now on.
- **Confidence:** high. Parsing facts are measured, and the mechanism is reproduced.

### F3. LLM fair values contain unit and ordering garbage, and the model ignores the price it was given
- **What:** The fair-value regex takes the first two `$` amounts on the line, with no sanity check against the current price and no low <= high check. Before 2026-10-17 the stored `deep_analyses.fair_value` was the midpoint of those two numbers.
- **Where:** `src/analysis_parser.py:parse_analysis` (fair-value block); `src/tier_engine.py:assign_tier` (LLM fair value as fallback); `scripts/scheduler.py:friday_sonnet_batch` (old persistence, replaced in 9f22132).
- **Evidence:**
  - 6 of 193 files have low greater than high: NVDA 9.0/6.53, UNH 32/29, MCO 340/270, LRN 65/50, AMAT 10.65/8.5, DLO 15/11.
  - Of 269 rows with a stored fair value and a nearby price, 10 are implausible (below 0.35x or above 2.5x of price). AZO 26.35 vs 3,128; AMAT 9.58 vs 567; WDC 23 vs 746; NVDA 7.77 vs 193; UNH 30.5 vs 388. BKNG 4,550 against a price of 180 (the stock split; Sonnet 4.5's memory predates it). 63 rows (23%) sit outside 0.75-1.25x of price.
  - 74% of files have no `target_entry_price`.
  - After 3.0.0 the LLM value is only a fallback (`tier_engine.py`), but in 87 of the 196 non-news tier assignments (including 73 of the 100 bulk_load ones) the reason is "no price data - watching", that is, a default B with no gap computed.
- **Why it matters:** The damage so far is limited to the stored column and the pre-3.0.0 tier history, but any outage of the deterministic valuation silently re-activates the garbage path. The BKNG case shows the prompt's "Current Price" does not anchor the model when memory disagrees.
- **Severity:** medium
- **Effort:** S (validate `low <= high` and a 0.3-3x price band, reject otherwise)
- **Unlocks / depends on:** nothing.
- **Confidence:** high for the counts. Medium for the cause of each outlier, since raw text is gone.

### F4. The cohort validation does not validate the current ruleset
- **What:** The cohort measures first ratings. Those were made by Sonnet 4.5, mostly without a 10-K, parsed by the old parser, and tiered by the old LLM-target rule.
- **Where:** `src/database.py:get_first_ratings`; `src/backtest.py:run_tier_cohort_validation`; `/opt/docker-apps/buffett-bot/data/backtest/cohort_validation_2026-10-16.json`.
- **Evidence:**
  - 185 first ratings. 99 are dated 2026-04-07 or 2026-04-27. Median holding is 172 days.
  - Roughly 153 of 185 (83%) first ratings had no 10-K in the prompt: EDGAR ingestion shipped 2026-06-25, the first cached filing is dated 2026-07-03, and only 42 of 293 rows are grounded in one.
  - Tier A, n=8: +20.6pp since rating, hit rate 0.88, t = 2.5 (sd 0.23). The A names were rated between 2026-04-07 and 2026-07-10, the mean is dominated by CTSH +56% and IT +47%, and A required a price gap <= 0 against a target that was the LLM's own before 3.0.0.
  - B vs C: -1.5pp vs -4.3pp, difference t = 0.87.
  - Moat NONE: -10.5pp, t = -3.9, n=22 (the only cohort with a clear signal; see F1 for label noise). WIDE +0.9pp (t 0.4), NARROW -1.5pp (t -0.6).
  - Nine cohorts were tested in one market window, with overlapping holding periods, and no multiple-testing adjustment. The t-statistics above treat names as independent.
  - `deployment.py` justifies the exclusion rule with "293 first ratings"; 293 is the number of analyses, there are 185 first ratings.
- **Why it matters:** The A tier today requires gap <= 0 against aggregator fair value x 0.75, which has never been assigned (no S tier ever; 4 A names). Nothing in the cohort tests that rule, and the only evidence-backed LLM feature is a negative filter.
- **Severity:** high (unproven)
- **Effort:** L (needs a longer forward record, or a PIT-style backtest of the deterministic part only)
- **Unlocks / depends on:** F1 (stored raw text) and a record of which rule version produced each tier.
- **Confidence:** high on the numbers. The t-statistics treat names as independent, which they are not.

### F5. Look-ahead and model-version contamination: what is clean and what is not
- **What:** The model generation changed twice during the record and nothing in the DB says which model produced a row. The stated knowledge cutoff of the current deep model overlaps the earliest breaker-replay cases.
- **Where:** `src/config.py` (model defaults); `deep_analyses` (no model column); `docs/research/breaker-replay-2026-10-17.*`; `docs/research/light-model-ab-2026-10-18.md` (suggests a future deep-analysis replay).
- **Evidence:**
  - The first ratings (Apr-Jul 2026) came from Sonnet 4.5. Its knowledge ends in 2025 (from memory of the model card: reliable cutoff about Jan 2025, unverified), so returns since April 2026 cannot have leaked into the labels. The cohort is clean on this axis by construction.
  - The environment I run in states "Assistant knowledge cutoff is June 2026" for Sonnet 5.5, the current deep model. The Sonnet 5 cutoff is unknown (unverified).
  - Breaker replay: 19 headlines dated 2026-05-18 to 2026-10-13, run on Sonnet 5.5. Six (BRBR, UNH, ADBE 05-18/05-25; ROL 06-02; LULU, CART 06-08) sit at or before that cutoff. Because the task is text matching, the effect is probably small, but it is not zero and it is undocumented.
  - Rating mix by era, full-context rows only: Sonnet 4.5 (n=172) LOW 14%, WIDE 46%; Sonnet 5.5 (n=12) LOW 50%, WIDE 0%. That is confounded (the 5.5 rows are mostly re-queued former C names), but 7 tickers rated by both give 3 LOW to MEDIUM, 1 MEDIUM to LOW, and 1 WIDE to NARROW, so there is no clean read on a model effect either way.
- **Why it matters:** Any replay of deep analyses on Sonnet 5.5 using Apr-Jun 2026 data (the A/B note proposes one) will be contaminated. Mixing model generations in one cohort also makes tier-by-tier results hard to interpret.
- **Severity:** medium
- **Effort:** S (add `model`, `prompt_version`, `parser_version` columns and fill them)
- **Unlocks / depends on:** clean cohort slicing by model once F1's storage fix is in.
- **Confidence:** medium. The cutoffs are not verifiable from the repo.

### F6. The breaker check has no production history and the 0/19 replay has no positive control
- **What:** Zero fires in 19 cases says the gate is strict, not that it works. The replay input was Haiku's own 500-character write-up, and the cases were selected from what the old pipeline downgraded.
- **Where:** `scripts/replay_breaker_check.py:load_cases` and `_news_text`; `src/analyzer.py:check_thesis_breakers`; `docs/research/breaker-replay-2026-10-17.md`.
- **Evidence:**
  - `thesis_breaker_events` has 0 rows. On 2026-10-18 20:00 the first live run logged "weekly_news_sonnet budget exhausted - skipping breaker check for CLX" and the same for PEP. The 18 `sonnet_triggered=1` rows in `news_events` all come from the old re-analysis path.
  - Replay news text is 520-1,079 characters per case (mean about 580), built from the headline plus Haiku's bullet list. In production, Finnhub gives about 265 characters per story (headline plus a 200-character summary; mean of the 60 A/B news inputs).
  - 24 of the 100 Haiku-material events contain Haiku saying the news "directly matches/triggers" a breaker. Candidate real positives: CPRT 2026-10-08 ("debt-funded tender offer ... directly matches the thesis-breaking risk of large debt-funded acquisition"; the May analysis listed "Management announces large debt-funded acquisition (>$2B)"), AAPL 2026-04-27 (Cook stepping down, with a Cook-departure breaker), FICO 2026-10-08, and V 2026-08-07. None were in the replay because the old rules rated them B.
- **Why it matters:** The sell rule has no measured recall. The cheap fix is to replay those 24 rows (about 24 x $0.003, see F20) as a positive-control set, scoring by hand whether the event happened.
- **Severity:** high (unproven)
- **Effort:** S
- **Unlocks / depends on:** a defensible statement of the breaker check's recall; independent of F1.
- **Confidence:** high on the replay's construction and the zero-run history. Medium on whether CPRT and AAPL really satisfy their breakers as written.

### F7. The breaker sell rule cannot fire for 3 of the 7 held names, and the chain to a fire is lossy
- **What:** The news pipeline monitors only S/A/B names (`price_alerts`). CCI, CTSH and NNI are held and C tier, so they are never checked.
- **Where:** `src/news_fetcher.py:run_news_pipeline` (watched list from `get_price_alerts(tiers=["S","A","B"])`; `held` is filtered to `watched`); `src/deployment.py` (`thesis_breaker_fired` is the only forced sell).
- **Evidence:**
  - Held (`paper_positions`): AGM B, AIZ A, CCI C, CPRT B, CTSH C, DECK B, NNI C. The 2026-10-18 log reads "Portfolio holdings jump the news queue: AGM, AIZ, CPRT, DECK", so only 4 are watched.
  - The 2026-10-16 deep analyses that made CCI, CTSH and NNI C hold breakers (4, 3, 3), but no code path ever checks them.
  - Chain to a fire: Finnhub story in the last 2 days (`days_back=1`, from yesterday to today), substring keyword filter (F11), not a previously seen headline, Haiku REVIEW (about 17% of material groups in the A/B), Sonnet cap not exhausted (F10), the event stated in about 265 characters, and the breaker observable from news (F8).
  - Zero breakers have ever fired (0 rows).
- **Why it matters:** The positions that were most at risk under the old rules are the ones the new rule can no longer see. A sell rule that cannot trigger for 43% of positions by count is documentation, not protection.
- **Severity:** high
- **Effort:** S (monitor held names regardless of tier)
- **Unlocks / depends on:** F6, F10.
- **Confidence:** high

### F8. Breaker quality: 89% of the stored breakers were written under the old prompt, and a third need data a headline never carries
- **What:** The 3.0.0 prompt asks for 2-4 single observable events. Only 20 of the 185 latest analyses (11%) were written under it. The rest keep the old wording until their 180-day expiry.
- **Where:** `src/analyzer.py:ANALYSIS_SYSTEM_PROMPT` (THESIS-BREAKING RISKS); `src/analysis_parser.py:extract_thesis_breakers`; `deep_analyses.thesis_breakers`.
- **Evidence (665 breakers in the 185 latest analyses; regex heuristics, so the shares are indicative):**
  - Old-prompt breakers (599): 27% carry opinion or sell wording ("sell immediately", "monitor", "warning", "re-evaluate", "reduce position"). New-prompt (66): 0%. Mean length 190 vs 151 characters.
  - 36% (239) are pure financial-metric breakers with no event wording (margin, ROIC, FCF, retention, comps, occupancy). These are normally disclosed in filings and earnings releases, not in a 200-character Finnhub snippet. Another 26% mix a metric with an event.
  - 29% (196) require a multi-period condition ("two consecutive quarters", "3+ years"); 175 combine that with a numeric threshold.
  - Examples that cannot fire from a headline: "ROIC falls below 7% for two consecutive years" (RSG), "Office occupancy falls below 80% portfolio-wide" (AAT).
  - The parser cleanup of 2026-10-17 removed the `**` artifact (0 remain) and headings, and the replay doc states older analyses keep their old wording.
- **Why it matters:** Even with perfect plumbing the fire probability is bounded by how many breakers are news-observable. With the 180-day re-analysis cycle the stock of old-wording breakers drains slowly (F13).
- **Severity:** medium
- **Effort:** M (tag each breaker `news-observable` vs `filing-only` at generation, and check the second kind at the next earnings date)
- **Unlocks / depends on:** F13 (refresh capacity), F6.
- **Confidence:** medium (heuristic classification, no human labelling)

### F9. "Pre-registered" breakers are overwritten at every re-analysis
- **What:** The breaker list lives on the latest `deep_analyses` row, and the news pipeline reads `get_latest_deep_analysis`. A re-analysis replaces the list with whatever the model writes next, including after the very news it was supposed to guard.
- **Where:** `src/news_fetcher.py:run_news_pipeline` (`da = db.get_latest_deep_analysis(ticker)`); `src/database.py:save_deep_analysis`.
- **Evidence:** CPRT's 2026-05-29 breakers include "Management announces large debt-funded acquisition (>$2B)". The ACV tender offer was reported on 2026-10-08, the news-triggered re-analysis the same day rewrote the list, and the new list no longer has that breaker (it now reads "The ACV deal ... closes at a valuation that materially lowers ROIC below ~12%"). FICO: re-analysis on the 2026-10-08 "SELL" flag rewrote the breakers and moved moat WIDE to NARROW and fair value 800 to 1150. This path was removed in 3.0.0, but the scheduled batch still replaces the list on every re-analysis.
- **Why it matters:** The claim in the README ("pre-registered at analysis time") is true only until the next analysis. A breaker that was close to firing can disappear.
- **Severity:** medium
- **Effort:** S (version breakers with a registration timestamp, and check events against any breaker version from the last 90 days)
- **Unlocks / depends on:** F6.
- **Confidence:** high for the CPRT and FICO rows; the mechanism is in the code.

### F10. The Sonnet breaker-check cap is unpaced and already binding
- **What:** `weekly_news_haiku` has pacing (`paced_allowance`), but `weekly_news_sonnet` does not: `can_spend` is checked per call with no daily floor. Skipped checks are not retried beyond the 2-day fetch window.
- **Where:** `src/news_fetcher.py:run_news_pipeline` (breaker block); `src/database.py:can_spend, paced_allowance`.
- **Evidence:**
  - 2026-10-18: "Budget cap exhausted for 'weekly_news_sonnet': 10/10" for CLX and PEP (both Haiku REVIEW). The 10 calls had been used earlier that week by the old re-analysis path (`news_events` for week 2026-10-12: 12 material, 10 Sonnet-triggered), so the first live breaker check was starved before it started; the cap is shared by design.
  - Demand estimate: the A/B gave Haiku REVIEW on 10 of 60 groups (17%); 150 Haiku calls a week would imply roughly 25 REVIEW flags a week against a cap of 10. Earnings season will exceed it.
  - The same log shows the Haiku pool dry at 22 calls: "weekly_news_haiku pace floor reached - skipping Haiku check for" VRSN, WM, V, MSFT, MA, PG, META, WSM, NTAP, COST, ODFL, GWW, KO, SNPS, WDC (15 names with new material news, MSFT 8 items and META 17). The queue order is holdings first, then `gap_pct` ascending, so the high-news large caps sit at the tail every week. The message text ("pace floor reached") is misleading; the weekly cap was 150/150.
- **Why it matters:** The checks that matter most (a REVIEW on a held name) compete with everything else for 10 calls a week at a total weekly cost of about $0.03 (F20).
- **Severity:** high
- **Effort:** S (raise the cap to 60, reserve a slice for held names)
- **Unlocks / depends on:** F7. The caps are code-owned (`BUDGET_CAPS_DEFAULTS` is written back to the DB on every start), so this needs a code change and an image rebuild.
- **Confidence:** high

### F11. The keyword pre-filter matches substrings, so company names trigger paid calls
- **What:** `apply_keyword_filter` does `kw in text`. "eps" matches PepsiCo, "miss" matches Transmission, "coo" matches Cook and Cooper, "ftc" matches CFTC.
- **Where:** `src/news_fetcher.py:apply_keyword_filter`, `MATERIAL_KEYWORDS`.
- **Evidence:** Of 1,532 logged news events, 49 (3.2%) pass the filter only through an in-word artifact in the headline (eps 23, miss 13, coo 12, ftc 1). Every PepsiCo headline ("Protein Push May Reshape Growth Story For Undervalued Shares") and every Allison Transmission headline ("Passes the 'Caviar Cruise' Quality Investing Screen") reaches Haiku. This is a lower bound: summaries can also trigger the filter and are not stored. Stems such as "acqui" and "retire" also match inflected non-events ("Retirement Savings"), which I did not count.
- **Why it matters:** The cost is trivial, but the weekly Haiku pool is the scarce resource (F10), and two tickers (PEP, ALSN) always consume a slot.
- **Severity:** low
- **Effort:** S (word-boundary regex)
- **Unlocks / depends on:** F10.
- **Confidence:** high

### F12. The news Haiku check failed on every call for seven days and nothing flagged it; there is also a seven-week gap with no paid jobs
- **What:** 210 of 210 news Haiku calls between 2026-10-01 and 2026-10-07 logged `haiku_material = NULL`. The budget counter is incremented before the call and never refunded.
- **Where:** `src/news_fetcher.py:run_news_pipeline` (failure branches log `haiku_material=None` and `continue`); `src/health.py:check_news_pipeline_effectiveness` (WARN, looks back 3 days).
- **Evidence:**
  - Per-day null counts: 10-01 72, 10-02 35, 10-04 39, 10-05 21, 10-06 21, 10-07 22 (total 210, 100% of the calls on those days); evaluation resumes 2026-10-08. All null rows over the whole record: 231 of 1,532 (15%).
  - The cause is not in the available data (logs only start 2026-10-18). It coincides with the 2026-10-01 commits (cbc0818 Sonnet 5.5, d922f88 parser/fencing); unverified.
  - `run_log` has 6 started-but-never-completed scheduled rows: wednesday_haiku 04-08, 04-15, 04-22, 05-06, 10-07 and friday_sonnet 10-02. The last two fall in the same outage window.
  - There are no `run_log`, `news_events` or `deep_analyses` rows from 2026-08-11 to 2026-09-30 (seven weeks), while snapshots and fundamentals continue. This fits the 2026-08-11 commit making all paid switches opt-in (default false); I could not confirm when they were turned back on.
- **Why it matters:** For a week the sell-side monitor was blind while reporting success, and the health check only warns, only when something changes.
- **Severity:** medium
- **Effort:** S (alert on any day with more than 50% null verdicts; refund the budget unit on failure)
- **Unlocks / depends on:** nothing.
- **Confidence:** high on the outage (DB counts), low on the cause.

### F13. A refresh cliff on 2026-10-24: the queues will starve, and the caps are not what limits throughput
- **What:** 382 of the 850 latest Haiku screens (all from the 2026-04-27 bulk load) expire on 2026-10-24. The Friday Sonnet queue only takes names whose latest Haiku pass is still valid and that have no valid deep analysis, so expiry removes them from the queue.
- **Where:** `src/database.py:get_haiku_passes_without_analysis` (`datetime(h.expires_at) > datetime('now')`), `get_unscreened_tickers`, `get_expiring_haiku_tickers(limit=20)`; `scripts/scheduler.py:wednesday_haiku_batch` (30 + 20) and `friday_sonnet_batch` (`limit=5`).
- **Evidence (queried 2026-10-19):**
  - Haiku passes eligible for the Friday queue: 299 now, 140 on 2026-10-25, 113 on 2026-11-15. Passes whose Haiku result has expired: 0 now, 272 on 2026-10-25.
  - Names needing a Haiku screen after 10-24: 509 never screened plus 382 expired = 891, against 50 a week (`weekly_haiku_screen`), about 18 weeks.
  - 252 names passed Haiku and have never had a deep analysis. 72 of 185 latest deep analyses are already expired (67 of them C tier, which carry a 30-day expiry) and 48 more (B/A) expire in the week of 2026-10-19.
  - The Friday queue limit is hard-coded at 5 (`get_haiku_passes_without_analysis(limit=5)`), so the `weekly_sonnet_analysis` cap of 10 only binds when held names jump the queue. Throughput is 5 to 10 a week. Observed batch sizes: 5-9 a week since May.
  - Cost to clear everything once: about 372 deep analyses x $0.023 (batch) = $8.5, plus 891 screens x $0.00037 = $0.33 (F20).
- **Why it matters:** Haiku passes are ranked by `quality_score`, and with the cliff the "best" names expire out of the queue without being analysed. The 180-day cycle also means the 165 old-prompt breakers (F8) will be refreshed at 5 a week.
- **Severity:** high
- **Effort:** S (change the limits, rebuild the image; the caps live in code, not `.env`)
- **Unlocks / depends on:** F2 (do not mass-analyse before the parser is fixed), F8.
- **Confidence:** high on the queries. Medium on how the expiry affects held-name sizing downstream (unverified; `plan_buys` only buys S/A tiers, and only 4 A names exist).

### F14. The deep analysis sees about 8 ratio lines, and for the 10-K-grounded 14% it sees a small slice of the filing
- **What:** The deep prompt's evidence is the `_build_db_summary` block (name, sector, market cap, price, P/E, ROE, ROIC, margin, FCF yield, D/E, revenue growth) plus up to 15,000 characters of 10-K excerpts. Each raw 10-K section is cut to 8,000 characters at fetch time, then to 2.8k (business), 5.7k (risk factors) and 5.7k (MD&A) at prompt time.
- **Where:** `src/edgar_fetcher.py:_RAW_SECTION_CAP, _SECTION_WEIGHTS, augment_filing_text`; `src/analyzer.py:MAX_FILING_CHARS`; `scripts/bulk_load.py:_build_company_summary`.
- **Evidence:**
  - 40 cached 10-K files: business median 7,998 characters, risk factors median 7,997, MD&A median 7,998; 35 of 40, 32 of 40 and 36 of 40 sit at the cap, i.e. were truncated. The excerpts actually used average about 13.2k characters, roughly 60% of the stored 8k sections. Real Item 1, 1A and 7 sections are usually many times longer than 8,000 characters (from general knowledge of 10-Ks, not measured here).
  - Rows grounded in a 10-K: 42 of 293 (14%). The 100 bulk_load rows, 54 scheduled rows from May-June and all 97 news-triggered rows had no 10-K (first cache date 2026-07-03).
  - The Wednesday Haiku summary (`scheduler._build_db_summary`) has no INDUSTRY line; the bulk-load summary did. Haiku input averages 350 tokens including the roughly 185-token system prompt.
- **Why it matters:** For most names "moat" is the model's memory of the company plus 8 ratios. That is the root of the stale-memory failures (BKNG split, F3) and a reason to read moat labels as a prior, not a finding.
- **Severity:** medium
- **Effort:** M (pull Item 1 and the segment note in full, or use the PIT XBRL facts already loaded into `pit_fundamentals`)
- **Unlocks / depends on:** a meaningful moat measurement; F15.
- **Confidence:** high for the truncation counts; medium for the claim about typical section lengths.

### F15. The Haiku gate's recall cannot be measured, and its threshold is not the configured one
- **What:** 428 names failed the gate and were never looked at again; the gate result is stored without its numeric hints; the pass threshold is hard-coded.
- **Where:** `src/analysis_parser.py:parse_quick_screen` (`worth_analysis = (moat_hint + quality_hint) >= 6`); `src/config.py:haiku_min_score` (5, used only in the legacy `run_monthly_briefing.py`); `src/database.py:save_haiku_result`.
- **Evidence:**
  - `haiku_screens` stores `passed`, `moat_estimate` (WIDE for hint >= 4, NARROW for 3, NONE for <= 2) and `summary`; the 1-5 moat and quality hints are not kept, so the threshold cannot be re-tuned afterwards.
  - Pass rates: 285 of 400 (71%) on the 2026-04-27 bulk load, then 1-21 of 30 each week, 21 of 50 on 2026-10-14.
  - Only 7 deep-analysed names have a failing latest screen; all 39 conviction-list names (which bypass the gate) passed it, so the bypass has never produced a counterexample.
  - Haiku-vs-Sonnet agreement on the 185 deep-analysed names: Haiku WIDE becomes Sonnet WIDE 66 of 117 (56%), NARROW 50, NONE 1; Haiku NARROW (passed) becomes Sonnet NONE 11 of 51 (22%).
  - Repeat-screen stability: 2 flips in 20 re-screened names; the A/B shows 90% pass/fail agreement between Haiku and Flash and 93% (56/60) agreement of Haiku with its own stored verdict.
  - Five high-quality-score names (PBT 98, CRMD 91, AII 85, GPOR 80, SD 80) were failed as moat NONE; PBT (a royalty trust) is defensible, the others are arguable and unchecked.
- **Why it matters:** The gate is the only thing that decides which of the 1,359 names ever reach Sonnet, and nothing measures what it throws away. At $0.0004 a screen, the gate saves less than it risks (F20).
- **Severity:** medium
- **Effort:** S (store the hints); M (a sample of the failed names through Sonnet, 50 names about $1.2)
- **Unlocks / depends on:** a measured false-negative rate.
- **Confidence:** high on the facts; the recall itself is unmeasured.

### F16. About half of the deep prompt's output feeds nothing, and the prompt asks for memory where it should say "use the data"
- **What:** The deep prompt asks for currency exposure (revenue splits), insider ownership, management quality, bear-case probes, outlook and total return. None of it reaches a decision.
- **Where:** `src/analyzer.py:ANALYSIS_SYSTEM_PROMPT`; consumers: `src/tier_engine.py`, `src/deployment.py`, `src/briefing/text_formatter.py:376`.
- **Evidence:**
  - A grep of `src/` and `scripts/` for `customer_concentration`, `switching_cost_rating`, `currency_risk_level`, `domestic_revenue`, `insider_ownership`, `patent_ip` finds consumers only in the parser, the analyzer and the briefing formatter's management line.
  - Decision inputs from the LLM: moat durability (mapped to WIDE/NARROW/NONE), conviction, the breaker list, and a fair value used only as a fallback.
  - The prompt says "Domestic Revenue: [X% - estimate based on your knowledge of the company]" and "Insider Ownership: [Estimate percentage if known...]" and asks for "Estimated Fair Value ... NOT a precise DCF". There is no instruction to prefer the supplied price or metrics when memory disagrees (BKNG: price 180 supplied, fair value 3,900-5,200 returned).
  - Four-point durability (STRONG/MODERATE/WEAK/NONE) collapses to three (`weak` and `none` both map to NONE), and NONE bars a buy; only 3 of 193 files say `weak`.
- **Why it matters:** The unused fields are about 60% of the output tokens (cost and, more importantly, parser surface: F2). The hallucination risk sits in the fields nobody consumes, which is the good news; the risk that matters is in the three that do.
- **Severity:** low
- **Effort:** S to prune; M to restructure as a forced tool call (see the improvement ideas)
- **Unlocks / depends on:** F2.
- **Confidence:** high on consumers (grep); medium on the 60% (estimated from section sizes).

### F17. Re-analysis consistency cannot be measured on the stored data, and what can be measured is poor
- **What:** 97 of the 108 consecutive re-analysis pairs end in a news-triggered, reduced-context re-analysis (no 10-K, a numeric summary plus headlines). Only 8 pairs compare two full-context analyses, and 7 of those span a model change.
- **Where:** `deep_analyses` joined to `tier_history` by ticker and timestamp (every row matched, 0 unmatched); the removed news path in `src/news_fetcher.py` (`_build_news_context` is now dead code).
- **Evidence:**
  - 293 rows, 185 tickers; 48 tickers have 2 or more rows. Rows per ticker: 137 have 1, 25 have 2, 11 have 3, 7 have 4, and 2 have 6, and one each has 9 (AAPL), 11 (MSFT) and 13 (NKE).
  - All 108 pairs: moat changed 24 (22%), conviction 24 (22%), tier 25 (23%). Fair value (107 pairs with values): median abs log change 0.109, p75 0.309, mean 0.351 (outliers), more than 20% in 36 pairs (34%).
  - Pairs ending in a news re-analysis: moat changed 21 of 97 (22%), conviction 19 (20%), fair value more than 20% in 32 (33%).
  - Full-context to full-context (8 pairs): conviction changed in 5 (62%), tier in 4 (50%); six spans of 154-168 days run from Sonnet 4.5 to Sonnet 5.5, and a seventh (NNI, 70 days) from Sonnet 5 to 5.5.
  - Examples: NKE 13 analyses in 5.5 months, fair value 4.9 to 65, B in 12 of them and C in the last; MSFT 11 in 3.4 months (90 to 410, moat WIDE with one NARROW); AAPL 9 in 8 weeks (97.5 to 240, B then C); UNH conviction MEDIUM, HIGH, MEDIUM, LOW within 4 weeks with fair value 350, 375, 350, 30.5.
  - The 97 news-triggered rows produced 19 C downgrades under the old rules, three of them on held names (CTSH, CCI, NNI), which the system map records as three sell orders cancelled on 2026-10-16. The replay's 0/19 refers to exactly these rows.
- **Why it matters:** The old news path made the book a function of how many headlines a name attracted. 3.0.0 removed the path, so this is mostly history, but it means (a) the DB's latest rating for several names comes from a headline-driven row and (b) a repeat-sample test, which is being done separately, has no usable same-model baseline in the data.
- **Severity:** medium
- **Effort:** n/a for the history; S to mark news-path rows `reduced_context` and exclude them from cohort work
- **Unlocks / depends on:** the 3-run consistency measurement (done separately), F1.
- **Confidence:** high on the counts; the 5.5-vs-4.5 reading is confounded.

### F18. No evidence yet that the LLM's positive ratings carry information; the one signal is a negative filter
- **What:** WIDE, NARROW and MEDIUM-conviction cohorts are indistinguishable from the benchmark. Only moat NONE (and weakly LOW conviction) separates.
- **Where:** `cohort_validation_2026-10-16.json`; `src/deployment.py` (quality exclusion); `src/tier_engine.py`.
- **Evidence:**
  - Since-rating alpha, n and t: WIDE +0.9pp (81, t 0.4); NARROW -1.5pp (80, t -0.6); NONE -10.5pp (22, t -3.9); MEDIUM -0.5pp (152, t -0.35); LOW -7.5pp (25, t -1.6); HIGH -0.9pp (6, t -0.07).
  - Funnel: 1,359 universe, 422 Haiku passes, 185 deep-analysed, 4 tier A, 0 tier S.
  - The deterministic fair value (not the LLM) now sets target and tier (`assign_tier`); the LLM contributes labels.
- **Why it matters:** The system's value claim ("quality at a fair price, with an LLM judging quality") has no support beyond "avoid names the model calls NONE", and that signal is itself exposed to F1. This is an honest "unproven", not a finding of no edge.
- **Severity:** medium (unproven)
- **Effort:** L (needs a longer record, more names, and clean labels)
- **Unlocks / depends on:** F1, F4. A cheap test: compare the LLM moat with objective moat proxies (ROIC persistence, margin stability) from `pit_fundamentals`, since they are free.
- **Confidence:** medium

### F19. Prompt caching does nothing measurable, and the README says otherwise
- **What:** `cache_control` ephemeral is set on all five system prompts, but four are far below any model's minimum cacheable length, the deep prompt is at the edge, and no call ever logs its `usage`.
- **Where:** `src/analyzer.py` (`cache_control` in `analyze_company`, `quick_screen`, `check_news_for_red_flags`, `check_thesis_breakers`, batch builders); `README.md` lines 15 and 99.
- **Evidence:**
  - System prompt sizes: analysis 3,971 characters (about 1,000 tokens); quick screen 662 (about 185); Opus second opinion 1,686 (about 470); news monitor 575 (about 160); breaker check 1,071 (about 300).
  - The A/B JSON records `cache_read: 0` for all 120 Haiku calls (60 screens, 60 news).
  - From my memory of the docs, the minimum cacheable prefix is 1,024 tokens for Sonnet models and 4,096 for Haiku 4.5 (unverified for Sonnet 5.5), so only the deep prompt could ever cache and only at the boundary. Even then the saving is at most the system prompt's 1,000 tokens of about 5,300.
  - `grep usage` in `src/` and `scripts/` finds `response.usage` only in `replay_light_model.py`; production never records tokens.
- **Why it matters:** The financial effect is negligible (cents a month), but the documented saving is not real and no one could tell if it were.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** F20 (usage logging). The place caching could matter is a per-ticker 10-K prefix shared by several questions (see the improvement ideas).
- **Confidence:** medium (the cache minimums are from memory).

### F20. Cost: about $2.1-2.3 a month at the caps; `run_log` is a count times a constant and misses the news path
- **What:** The README says Claude API costs "$10-15/mo". Reconstructing from the caps and measured token sizes gives about a fifth of that. `run_log.total_cost_usd` is `calls x constant` and does not include news calls.
- **Where:** `scripts/scheduler.py` (`_HAIKU_COST_USD = 0.001`, `_SONNET_COST_USD = 0.025`, `complete_run`); `scripts/bulk_load.py:step5_sonnet_batch` (docstring says it excludes cached results, but it returns `len(analyses)`, which includes cache hits); `README.md:144`.
- **Evidence (prices as stated in the header; 4.345 weeks a month):**

| job | calls a week (cap) | tokens per call (in/out) | $ per call | $ a month |
|---|---|---|---|---|
| Wed Haiku screen, batch | 50 | 350 / 78 (measured, A/B) | 0.00037 | 0.08 |
| Fri Sonnet deep with 10-K, batch | 8 observed, 10 cap | 5,300 / 2,000 (estimated) | 0.023 | 0.80 to 0.99 |
| News Haiku, realtime | 150 | 610 / 162 (measured) | 0.0014 | 0.92 |
| News Sonnet breaker check, realtime | 10 | 650 / 60 (estimated) | 0.0029 | 0.12 |
| Auto-trade Haiku quick screen, realtime, uncapped | up to 20 | 1,400 / 80 (estimated) | 0.0018 | 0.16 |
| Monthly briefing | 0 | no LLM call in `src/briefing/db_briefing.py` (grep) | 0 | 0 |
| Opus second opinion | 0 | only in the legacy `run_monthly_briefing.py`, flag false | 0 | 0 |
| **Total** | | | | **about 2.1 to 2.3** |

  - `run_log` totals: bulk_load $11.97 (5 completed runs of about $2.40, each recorded as 400 Haiku and 80 Sonnet calls, the batch limits), wednesday_haiku $0.47, friday_sonnet $2.45; no news run is ever logged. Estimated unlogged spend: 1,532 news Haiku calls (about $2.2) plus 97 realtime Sonnet re-analyses (about $3.6) = about $5.8, against $14.9 logged.
  - Dollar comments in code are stale: the 10 weekly breaker checks are noted as "~$0.25" and the replay doc quotes "about USD 0.02 each", against about $0.003 by my token estimate.
  - Runs 11 and 12 on 2026-04-27 both log 400 Haiku and 80 Sonnet calls within an hour, yet `haiku_screens` holds one batch of 400 for that day; I suspect the second is cache hits counted as calls (unverified).
  - One-off prices: 372 deep analyses $8.5 (batch), 891 Haiku screens $0.33, the whole universe through Sonnet (1,359) about $31.
- **Why it matters:** At these sizes the caps (10 and 50 a week) are the binding resource, not money. Cost-saving moves (the Flash swap, F21) are worth cents.
- **Severity:** low
- **Effort:** S (log `response.usage` and a model id per call; replace constants with measured cost)
- **Unlocks / depends on:** F13, F19.
- **Confidence:** medium. The Haiku figures are measured; Sonnet token counts and all prices are estimates.

### F21. The DeepSeek Flash swap is not planned in the repo, and the A/B measured noise, not recall
- **What:** The brief describes a planned swap, but `docs/research/light-model-ab-2026-10-18.md` records the decision "Keep Haiku 4.5". Its saving is about $0.60 a month.
- **Where:** `docs/research/light-model-ab-2026-10-18.md`, `scripts/replay_light_model.py`.
- **Evidence:** Haiku 0.00108 vs Flash 0.00026 dollars a call; invalid outputs 1/60 vs 0/60 on news; pass/fail agreement 54/60; news recommendation agreement 53/59 (90%); no case in the news sample was a real breaker ("this measures noise, not recall", the note itself says so). Flash has no Batches API (404), so the Wednesday batch would need a sequential path.
- **Why it matters:** The swap would change nothing that matters (the screen is $0.08 a month in batch; news Haiku $0.92). What it would not change is F6, F10 and F15: the problems are recall and coverage, not price.
- **Severity:** n/a (informational)
- **Effort:** S
- **Unlocks / depends on:** the A/B would become informative if run on the F6 positive-control set.
- **Confidence:** high

### F22. Opus is not used in production, and nothing exercises its code or prompt
- **What:** `opus_second_opinion` is reachable only from the legacy monthly script, behind `USE_OPUS_SECOND_OPINION` (false), and only for S-tier picks. No S tier has ever been assigned.
- **Where:** `src/analyzer.py:opus_second_opinion`, `OPUS_SECOND_OPINION_PROMPT`; `scripts/run_monthly_briefing.py:762-788`; `src/config.py:use_opus_second_opinion`.
- **Evidence:** the scheduler (`scripts/scheduler.py`) contains no call to it; `run_log.opus_calls` is 0 in all 48 rows. The prompt asks Opus to "consider the current macro environment", and no macro data is supplied.
- **Why it matters:** Dead code with a stale cost note (about $0.30 a stock). The idea itself (an independent contrarian pass before a buy) is sound and cheap at this volume (improvement ideas).
- **Severity:** low
- **Effort:** S to remove or to wire in
- **Unlocks / depends on:** nothing.
- **Confidence:** high

### F23. Batch and failure handling: partial results discarded, failed items silently dropped, no truncation check
- **What:** Several small handling gaps in the batch path.
- **Where:** `src/analyzer.py:_wait_for_batch`, `batch_analyze_companies`, `batch_quick_screen`, `analyze_company`; `scripts/scheduler.py` (refund logic).
- **Evidence:**
  - On timeout (120 minutes) the batch is cancelled, `TimeoutError` is raised, and the scheduler refunds the whole reservation. From my memory of the docs, requests that already finished in a cancelled batch are still billed and their results retrievable, so the code discards paid-for results and the tickers are re-submitted (unverified).
  - Items that come back `errored`, `expired` or `canceled` are logged and dropped (`logger.error(f"Batch analysis failed for ...")`) without a refund. They reappear in next week's queue.
  - `_check_refusal` is the only `stop_reason` check; `max_tokens` truncation is not detected. The structural gate passes any response with at least 6 of 11 headers, so a truncated analysis that lost its THESIS-BREAKING section would be stored with no breakers. No such case exists in the data (0 of 193 files with empty `thesis_risks`; the longest stored analysis is about 11k characters of an allowed 4096 tokens), so this is latent.
  - The scheduler is single-threaded, so a batch wait blocks every other job. Observed durations are up to 28 minutes (wednesday_haiku 2026-08-05; friday_sonnet 2026-05-22 27 minutes).
  - The file cache is keyed by symbol only, with no model or prompt version, and `batch_analyze_companies` returns a cached analysis under 30 days old; the DB write then stamps it as a fresh analysis.
- **Why it matters:** Mostly cents and rare, but each one is a quiet way for a stale or partial result to look like a new judgment.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** F5 (record the model and prompt version).
- **Confidence:** medium (the cancel behaviour is from memory).

### F24. Model configuration traps for the next upgrade
- **What:** `_thinking_kwargs` selects the thinking parameter by model-name prefix. Any model string it does not know falls through to `{"thinking": {"type": "disabled"}}`, which the function's own docstring says Sonnet 5.5 rejects with a 400.
- **Where:** `src/analyzer.py:_thinking_kwargs` (prefixes `claude-sonnet-5-5`, `claude-opus-5-5`, `claude-fable`, `claude-mythos`); `src/config.py` (model defaults).
- **Evidence:** the live `.env` has no model override, so the defaults apply. A new Sonnet minor version in `ANTHROPIC_MODEL_DEEP` would send `disabled`. Every route would then fail, and the news path would burn its weekly budget on failed calls before the health check noticed (F12).
- **Why it matters:** The upgrade path is a one-line env change that can take down every LLM job. The 2026-10-01 to 10-07 outage (F12) may be an instance of the same class; unverified.
- **Severity:** low
- **Effort:** S (probe the parameter on the first call and cache the result, or fail the container start on a 400)
- **Unlocks / depends on:** F12.
- **Confidence:** medium. I cannot test the API behaviour.

### F25. Prompt-injection fencing is partial
- **What:** Filing, transcript and news text are wrapped in tags and the system prompts say tagged text is evidence. Model-written text that came from third-party content is re-injected unfenced.
- **Where:** `src/analyzer.py:check_news_for_red_flags` (investment thesis and breakers outside tags), `_build_analysis_user_prompt`, `quick_screen`, `check_thesis_breakers`.
- **Evidence:** the stored `investment_thesis` and `thesis_breakers` (model output derived from 10-K and Finnhub text) go into the Haiku and Sonnet user prompts outside any tag. Closing tags are not escaped inside the fenced text (`</filing>` or `</news>` in a story would end the fence). The internal DB summary is wrapped in `<filing>` although it is not third-party text.
- **Why it matters:** A hostile Finnhub story is the realistic vector (the breaker check can force a sell). The structure limits damage (a fired breaker needs a matching index and is journaled), so the risk is low.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** nothing.
- **Confidence:** medium

### F26. The weekly auto-trade makes uncapped Haiku calls on a different input, before a filter that makes them moot
- **What:** `weekly_auto_trade` quick-screens up to 20 watchlist names with Haiku on yfinance's `longBusinessSummary`, and only afterwards drops everything that is not S or A tier.
- **Where:** `scripts/scheduler.py:weekly_auto_trade` (lines about 773-830).
- **Evidence:** the order is `quick_screen`, then the `worth_analysis` and valuation checks, then `if tier not in ("S", "A"): continue`. Only 4 A names exist and no S, so almost every one of the weekly calls is spent on a name that cannot be bought. There is no `can_spend` call, so it sits outside the budget system, and the input differs from the Wednesday screen (business description vs ratio block), so the same name can pass one gate and fail the other.
- **Why it matters:** Small in dollars (about $0.16 a month), but it is a third, differently-fed copy of the gate that can veto an A name, and nobody has measured how often.
- **Severity:** low
- **Effort:** S (check the tier first; reuse the stored screen)
- **Unlocks / depends on:** F15.
- **Confidence:** high

### F27. Health checks do not cover the failures that actually happened
- **What:** The daily health digest reports "degenerate_analyses 4% - ok" while 63% of rows have unparsed moat fields.
- **Where:** `src/health.py:check_degenerate_analyses` (fails only at 80% NONE-plus-LOW), `check_news_pipeline_effectiveness`.
- **Evidence:** the 2026-10-18 digest: "[ok  ] degenerate_analyses 4% of analyses are maximally pessimistic", "[ok  ] news_pipeline 62/65 news event(s) evaluated". F2 and F12 describe a 63% field failure and a 7-day outage that these checks would not have escalated (WARN only, 3-day window).
- **Why it matters:** The checks were written for total collapse, and the real failures are partial.
- **Severity:** low
- **Effort:** S (alert on parser fill-rate below 80%, on null-verdict rate above 20% in a day, and on a failed batch)
- **Unlocks / depends on:** F2, F12.
- **Confidence:** high

---

## Improvement ideas not tied to a weakness

### I1. Replace free-text parsing with a forced tool call
- **What:** Define a `record_analysis` tool with an `input_schema` (enums for durability, conviction and capital allocation, numbers for fair value low/high/target, an array of 2-4 breakers each with a `news_observable` flag), force it with `tool_choice`, and read `tool_use.input`. This removes F2, most of F3 and the bold-label problem, and gives a clean `stop_reason`. Structured-output mode would do the same where the model supports it (unverified for Sonnet 5.5).
- **Where:** `src/analyzer.py`, `src/analysis_parser.py`.
- **Evidence:** F2 (63% unparsed fields).
- **Why it matters:** Removes the largest source of label noise at once.
- **Severity:** n/a
- **Effort:** M
- **Unlocks / depends on:** F1 storage fix; a re-run of the cohort at about $0.02 a name.
- **Confidence:** medium (the tool-use mechanism is standard; model-specific limits unverified)

### I2. Store everything needed to audit a judgment
- **What:** Per call, persist the model id, a prompt hash, the parser version, the raw response, and `usage` (tokens, cache read and write). One append-only `llm_calls` table is enough.
- **Where:** `src/database.py`, every `messages.create` and batch result loop.
- **Evidence:** F1, F5, F19, F20.
- **Why it matters:** It turns every item in this report that says "cannot be re-parsed" or "unverified" into a query.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** all cohort and consistency work.
- **Confidence:** high

### I3. Spend the money: clear the backlog and widen the caps now
- **What:** One-off, run all 252 passed-but-never-analysed names and the 120 expired or expiring analyses through the batch API (about $8.5), re-screen the 891 stale or unscreened names ($0.33), and lift the standing caps to roughly 60 Sonnet and 150 Haiku screens a week (about $4 a month). Do it after I1, so the labels are clean.
- **Where:** `BUDGET_CAPS_DEFAULTS`, `get_haiku_passes_without_analysis(limit=5)`.
- **Evidence:** F13, F20.
- **Why it matters:** Removes the 2026-10-24 cliff and the backlog for the price of two coffees.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** F13, I1.
- **Confidence:** high on the cost; the value depends on the labels being correct.

### I4. A breaker positive-control set and a "news-observable" tag
- **What:** Build 20-30 cases where a breaker clearly happened (start from the 24 `news_events` rows where Haiku claims a direct match, plus synthetic cases made by inserting a breaker-matching headline), run them through `check_thesis_breakers` with the real 200-character Finnhub snippet, and report recall as well as the 0/19 specificity. Store, per breaker, whether it can be seen in a headline; check the filing-only ones against the next 10-Q instead.
- **Where:** `scripts/replay_breaker_check.py`, `src/analyzer.py`.
- **Evidence:** F6, F8.
- **Why it matters:** It is the only way to say whether the sell rule can fire at all.
- **Severity:** n/a
- **Effort:** S (about $0.1 of calls)
- **Unlocks / depends on:** F6, F7, F9.
- **Confidence:** high

### I5. Cache the filing, ask several questions
- **What:** Put the full per-ticker 10-K sections (about 25-40k tokens) in a cached prefix and ask the moat, breaker and consistency questions against it, instead of 5.3k tokens of excerpts per call. With the 5-minute cache, a three-call repeat sample (being run separately) reads the 10-K at 0.1x after the first call.
- **Where:** `src/analyzer.py`, `src/edgar_fetcher.py`.
- **Evidence:** F14, F19.
- **Why it matters:** This is where caching would actually pay, and it fixes the 8,000-character truncation.
- **Severity:** n/a
- **Effort:** M
- **Unlocks / depends on:** I2 (to verify cache hits).
- **Confidence:** medium (cache economics from memory)

### I6. Use Opus where it could change a decision
- **What:** Run the Opus second opinion (or a second independent Sonnet sample) only on names about to be bought (tier A or S) and on held names flagged REVIEW. At 4 A names and a few REVIEW flags a week this costs under $2 a month.
- **Where:** `scripts/scheduler.py:weekly_auto_trade`, `src/analyzer.py:opus_second_opinion`.
- **Evidence:** F22.
- **Why it matters:** Puts the most expensive judgment where a mistake costs the most.
- **Severity:** n/a
- **Effort:** M
- **Unlocks / depends on:** I1, I2.
- **Confidence:** low (the value is unmeasured)

### I7. Version the breakers and monitor held names regardless of tier
- **What:** Make breakers immutable rows (`breaker_id`, `analysis_id`, `registered_at`, text, observable flag) and check news against all versions registered in the last 90 days; monitor every held name whatever its tier.
- **Where:** `src/database.py`, `src/news_fetcher.py:run_news_pipeline`.
- **Evidence:** F7, F9.
- **Why it matters:** Makes "pre-registered" literally true and closes the C-tier hole.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** F6.
- **Confidence:** high

---

## What I could not verify

- **Model and price facts.** Sonnet 5, Sonnet 5.5 and Opus 5 prices, training cutoffs and cache minimums are not in the repo. The `claude-api` skill was denied by the permission system, so I used remembered Anthropic figures and said so. Sonnet 5.5's June 2026 cutoff comes from my own environment's statement. Treat the dollar totals as about 2x accurate.
- **Raw model outputs.** They are not stored anywhere, so the true error rate of the old parser, the true rate of bold-label failures per era, and any truncation cannot be measured. The 7-of-24 "praising thesis but NONE" figure is a regex heuristic.
- **Which model produced each row.** Inferred from git dates, assuming the image was rebuilt on each commit date. The DB has no model id.
- **Cause of the 2026-10-01 to 10-07 news outage and the two failed batch runs (10-02, 10-07).** Container logs start on 2026-10-18; nothing earlier was available. I also could not tell when the paid switches were turned back on after the 2026-08-11 opt-in change.
- **Batch API cancel semantics, and cache behaviour on batches.** From memory of the docs, not tested (no API calls were allowed).
- **The tail of the Haiku check.** Whether the Haiku pass/fail near the threshold matters; the numeric hints are not stored (F15). The recall of the gate and of the breaker check is unmeasured by design.
- **The effect of expiry on position sizing and rotation** (F13). I read `plan_buys` and the queue queries only; I did not trace `plan_sells` for expired analyses.
- **Typical real 10-K section lengths.** The 8,000-character truncation is measured; the claim that real sections are much longer is general knowledge.
- **Same-model consistency.** Left to the separate 3-run measurement, as instructed. The DB-only numbers in F17 are mixed-model and mostly reduced-context.
- **No external requests were made.** No yfinance, EDGAR, Finnhub or Alpaca call was issued; nothing was written to the DB or the repo apart from this report. Scratch scripts are in the session scratchpad only.
