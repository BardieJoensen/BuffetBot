# BuffetBot audit, 2026-10-19

Lead reviewer synthesis of a read-only audit of BuffetBot at `main` @ `efc181b` (repo) and the live deployment at `/opt/docker-apps/buffett-bot` (image built from `origin/main` 2026-10-18). Six auditors worked in parallel from written briefs, a seventh red-teamed their reports. All seven reports, the system map and the decisions log are in `docs/research/audit-2026-10-19/`:

| File | Role | Model | Items |
|---|---|---|---|
| `SYSTEM_MAP.md` | entry points, jobs, data flow, schema, config, plan status | lead | |
| `data-auditor.md` | sources, staleness, PIT, survivorship, FX, API fragility | Sonnet | 29 findings, 7 ideas |
| `signal-auditor.md` | screen weights, quality score, industry blindness, valuation, tiers, regime, evidence | Opus | 34 findings, 5 ideas |
| `ai-pipeline-auditor.md` | escalation, prompts, parser, look-ahead, breakers, caching, cost | Sonnet | 27 findings, 7 ideas |
| `validation-auditor.md` | can the system prove an edge; backtests; journal; power | Opus | 26 findings, 4 ideas |
| `engineering-auditor.md` | code, tests, failure modes, ops, CI, secrets | Sonnet | 33 findings, 3 ideas |
| `scout.md` | outside evidence, data sources, comparables, Danish rules | Sonnet | 47 items |
| `red-team.md` | adversarial check of the six reports | Opus | see section 9 |
| `DECISIONS.md` | every decision the lead made without asking, with reasoning, and the grade given to each report | lead | |

References below use `<report>:F<n>` (for example `val:F13`). Every finding in every report carries file:function evidence; this document compresses them and does not repeat the evidence in full. The plan documents were not available (they exist only in an old chat), so plan-item status is taken from the code and commit history and is marked "unverified against plan" where it cannot be checked.

Ground rules were kept: no code, config, container, order, or live-DB write was touched; no LLM API calls were made; one SPY price pull was made by the validation auditor. Everything in sections 2 and 3 was confirmed by the lead against the live data or code before being accepted (see `DECISIONS.md`, "Report grades").

---

## 1. The honest verdict on edge

**Unproven, and with the current design, unprovable on any useful horizon.**

- **The paper account.** +5.77% vs SPY total return +2.60% from 2026-07-01 to 2026-10-16, t = 0.33. One position (CTSH, +$8,014 unrealised) is larger than the whole gain; without it the book lost 2.2% while SPY gained. The book has been about 65-71% invested the whole time, has not bought anything since 2026-07-24, and all eight holdings were chosen under rules that no longer exist (`val:F1`, `val:F19`, `sig:F30`).
- **Statistical power.** For an 8-stock book with 10-12% tracking error, detecting 3 pp/yr of alpha at conventional power takes 87-126 years. The minimum detectable alpha in the 3.5-month window is about 93 pp/yr. Two closed trades support no inference (`val:F11`, `scout:F11`).
- **The quant screen.** The 2026-10-17 point-in-time backtest reported a pooled Spearman of +0.018 and a "small, consistent edge on median returns". Removing the observations affected by a split look-ahead bug (adjusted prices multiplied by as-filed share counts) moves the Spearman to +0.003; the median edge has t = 0.49; the universe is today's survivors; and the score tested is the 5-metric composite, not the 16-metric `score_stock` that actually picks names (`val:F20`, `val:F22`, `data:F6`, `data:F10`, `sig:F12`, `sig:F33`). What the backtest does support: a weak tilt away from blow-ups. Not a weight, not a threshold, not an alpha source.
- **The LLM stage.** WIDE, NARROW and MEDIUM cohorts are indistinguishable from the benchmark. The only separating cohort, "moat NONE averaged −10.5 pp vs SPY with 2 of 22 ahead", which is the stated evidence for the ruleset 3.0.0 buy exclusion, is a data-migration artefact: 20 of the 22 names are legacy registry rows whose moat was read from a key the legacy schema does not have (`analysis.moat_rating` instead of `analysis.moat.durability`) and defaulted to NONE; the legacy files rate 13 of them STRONG and 7 MODERATE, 9 are regulated utilities, and the other 2 have zero-length windows. The lead confirmed this independently. The "LOW conviction −7.5 pp" leg is a product of the rating parser that was fixed on 2026-10-01 and disappears on clean data (`val:F13`, `val:F14`, `sig:F31`, `ai:F1`, `ai:F18`). A whole-pipeline backtest is not feasible because the models know 2016-2025 outcomes (`val:F25`, `scout:F16`).
- **The LLM moat label tracks company size.** Haiku's pass rate by market-cap quintile is 10%, 30%, 51%, 69%, 88% (median caps $1.1B to $138B); Sonnet's WIDE rate by quartile is 4%, 33%, 52%, 61%. Size and moat are correlated in reality, but a near-monotone ramp of that shape says the gate is largely a recognition filter, selecting famous large caps an index fund already holds, in the segment where the published quality and insider effects are weakest (red team M5, lead re-ran the query; `scout:F20`, `scout:F6`).
- **The one winner argues against the design.** CTSH's stored buy thesis reads "statistically cheap (11x P/E, 9.8% FCF yield) but strategically challenged (weak moat, AI disruption risk, commoditizing industry) ... a potential value trap". It was A because of price, is now C/LOW, and 3.0.0 would not buy it. n=1 proves nothing, but the data point people will read as success supports a statistical value screen, not the LLM quality stage the project is built around (red team M8).
- **After costs, the edge for the real account is untested, not just unproven.** The code's own cost assumption is about 0.75% per 5,000 DKK buy and 1.5% per round trip; realistic alpha from the literature is 2-3%/yr before post-publication decay; the rules still generate rotation, C-tier rotation, trims and regime moves. No report, and nothing in the system, compares that with the null the user faces: the same money in a broad index fund in the same ASK (red team M4).
- **What this means.** Nothing in the record says the strategy has negative edge either. It says the system has not yet produced evidence of any kind, that the evidence it has produced was mislabelled in two places and is quoted in the README and `deployment.py` as if it were real, and that the only route to evidence on a human timescale is forward cross-sectional cohort testing with clean labels, stored raw text, counterfactuals and fixed horizons (section 6, G-ideas).

---

## 2. Live issues found during the audit (flagged, not fixed)

These were reported to you mid-audit as the ground rules require. None places wrong orders or spends money, so the audit continued. All are confirmed from today's logs or live files.

| # | Issue | Evidence | Reports |
|---|---|---|---|
| L1 | **Monday refresh persists only the top 100 of 1,359 names.** `screen_tickers` truncates to `top_n` before the scheduler saves fundamentals and quality scores. Since 2026-07-26: 952 names frozen at 2026-06-29, 220 with no row, quality scores on two incompatible percentile scales, and 47 of 50 names Haiku-screened on 10-14 judged on June data presented as "Current Price". | log 2026-10-19 02:20: "Keeping top 100 by score (from 1078)", "Refreshed fundamentals for 100/1359" | `data:F1-F3`, `sig:F1`, `eng:F2` |
| L2 | **The universe cache is corrupt and the auto-trader runs on a fallback list.** `data/cache/stock_universe.json` (2026-09-18) has every ticker's first letter doubled ("AAAPL"); the weekly screen has fallen back to the 183-name curated list; the watchlist is 25 of those names; none is A or S tier; `weekly_auto_trade` requires A/S. No automated buy has been possible since mid-September and none has occurred since 2026-07-24. Cash has been $30,880.39 in 87 consecutive snapshots. | file contents; `watchlist.json` 2026-10-16; `decision_log` | `data:F5`, `eng:F1`, `sig:F29` |
| L3 | **The breaker-only sell rule cannot fire for 3 of 7 holdings.** The news monitor watches S/A/B names only; CCI, CTSH and NNI are held and C tier, so no breaker can be checked for them. `thesis_breaker_events` has 0 rows; the first live breaker run (2026-10-18) was starved by the 10/week Sonnet cap. | log 2026-10-18 20:00; `price_alerts` tiers | `ai:F7`, `ai:F10` |
| L4 | **A queue step on Saturday 2026-10-24.** 400 Haiku screens expire; the Friday Sonnet pool drops from 299 to 140 names; re-screening 891 names at 50/week takes 18 weeks. The red team showed this is not starvation: 140 names is 28 weeks of supply at the hard-coded 5 per Friday. Throughput, not the cliff, is the constraint. | `haiku_screens.expires_at` | `ai:F13`, red team |
| L5 | **Paid calls go to names the screen excludes.** Royalty trusts and too-hard industries (PBT, CRMD, DMLP, GPOR) sit at the top of the stale-score queue and received Haiku and Sonnet calls on 2026-10-14. | `haiku_screens`, `deep_analyses` | `sig:F2`, `sig:F15` |
| L6 | **The live `.env` still has the 2025 ASK limit (135,900 DKK).** The 2026 figure is 174,200. Only the legacy `portfolio.py` reads it today. | live env key; skat.dk | `scout:F39`, `eng:F30` |
| L7 | **Ruleset 3.0.0 inherits the 2.0 book.** All eight positions were bought under old rules (seven have no tier or a B/C tier at entry); under 3.0.0 only a fired breaker sells, so any "3.0.0 performance" measures 2.0 picks. | `decision_log`, `paper_positions` | `val:F19`, `sig:F30` |
| L8 | **Predicted for Friday 2026-10-23: the only winner gets trimmed.** CTSH is C tier and 20.4% of equity ($21.6k of $106.3k) against `MAX_POSITION_PCT` 15%; `plan_sells` trims overweight C-tier names regardless of price, so a sell of about $5.7k should be submitted after the close. Because every after-close sell is "pending", the `continue` that follows also skips the single-buy recommendation for the real account that week. Paper money, by design, but a 15% cap does not fit "a few long-term holdings". Lead confirmed the weight and the code path. | `paper_positions`, `deployment.plan_sells`, `scheduler.py:994-1019` | red team M2, `eng:F8` |

---

## 3. Weaknesses by area, with severity

Severity is the lead's after merging duplicates across reports and checking the evidence. "critical" = a live rule or published conclusion rests on wrong data; "high" = changes what the system buys, sells, screens or claims; "medium" = degrades quality or evidence materially; "low" = hygiene. Effort: S hours, M days, L weeks.

### 3A. Data layer

| ID | Weakness | Sev | Effort | Sources |
|---|---|---|---|---|
| A1 | Top-100 truncation freezes fundamentals and quality scores for 93% of the universe (L1). | high | S | `data:F1-F3`, `sig:F1`, `eng:F2` |
| A2 | LLM prompts present months-old fundamentals as current; no as-of date anywhere in the prompt; the docstring promises ≤4 days. Median age at Haiku time since August: 107 days. The tier's price leg and the aggregator fair value are fetched live when a tier is set, so the staleness affects the Haiku gate and the LLM labels, not tier pricing (red team). | medium | S | `data:F2` |
| A3 | The universe is never refreshed and dead or renamed tickers are never retired: 36 names resolve in neither Yahoo nor SEC (BK, CTRA, EA, HOLX, AVB, EQR among them), keep stale scores, and still get paid calls (CPRX). They are absent from SEC's 2026-10-02 ticker map too, so they are dead or renamed, not transient (red team corrects `eng:F14`). The 220 names with no fundamentals row never had one: they fail the hard filters and are not a casualty of the truncation. No CIK keying; dual share classes (GOOG/GOOGL, FOX/FOXA) analysed twice. | high | M | `data:F4`, `data:F19`, `eng:F14` |
| A4 | Two universes: the DB universe (Wikipedia + Finviz + conviction) feeds tiers; the older `src/universe.py` (Finviz cache, Wikipedia S&P 600, curated fallback) feeds the weekly screen and the trader (L2). | high | S-M | `data:F5`, `eng:F1`, `eng:F15`, `sig:F29` |
| A5 | 10-K excerpts given to Sonnet: the slicer takes the last regex match, so some sections start at a cross-reference or a table of contents, and every section is cut at 8,000 characters; only 14% of analyses had any 10-K text at all. The red team's content check puts plainly wrong sections at 14 of 118 (12%), not the ~40% the data auditor's heuristic flagged; in several "flagged" cases real MD&A text follows the cross-reference sentence. | medium | S | `data:F12`, `ai:F14`, red team |
| A6 | `revenue_cagr` is missing for ~72% of names (NaN oldest yfinance column); `earnings_consistency` has an unreachable ideal of 4; the DCF growth input inherits the same bug. | medium | S | `data:F13`, `sig:F3`, `sig:F4` |
| A7 | `net_share_change` reads "Share Issued" (includes treasury stock), so buybacks held in treasury are invisible (AFL +0.1% vs −5.6%/yr in EDGAR). | medium | S | `sig:F5` |
| A8 | Three FCF metrics (weight 4.0) use two inconsistent sources; `real_fcf_yield` exceeds `fcf_yield` for 474 of 1,084 names; `real_fcf_yield` silently falls back to the plain figure. | medium | S | `sig:F6`, `sig:F7`, `eng:F26` |
| A9 | yfinance `dividendYield` is a percent but used as a ratio in `portfolio.py` (100× overstatement); the benchmark yield renders as 98%. | medium | S | `data:F14` |
| A10 | News pipeline: fetch errors look like "no news"; one-day window with no catch-up; roundup headlines attributed to unrelated tickers; substring keyword filter ("eps" matches PepsiCo); budget spent before the call; only one headline per ticker per day stored. | medium | S | `data:F17`, `ai:F11`, `ai:F12`, `eng:F31` |
| A11 | No retry, backoff, timeout policy or error-rate threshold on any external call; a 37-error Monday is one INFO line. | medium | M | `data:F18`, `eng:F6` |
| A12 | The `fundamentals` table is not a usable history (27-day hole, top-100 tail since July, 8 columns, no as-of price timestamp) and retention will delete ~96% of weekly rows once they are two years old. | medium | S | `data:F21`, `data:F22`, `eng:F25` |
| A13 | Held positions sit outside the refresh: NNI is not in the universe; AGM and CCI have June fundamentals; `paper_positions.entry_date/entry_price` are NULL for all seven. | medium | S | `data:F23` |
| A14 | Finnhub analyst targets never contribute (all 25 watchlist estimates are Yahoo-only; endpoint probably premium). | medium | S | `data:F19` |
| A15 | Pre-2026-10-17 `deep_analyses.fair_value` rows hold LLM midpoints that are sometimes garbage (AMAT 9.57 vs price 627); no price sanity bound anywhere. | medium | S | `data:F25`, `ai:F3` |
| A16 | Mixed timestamp conventions: SQLite UTC vs naive local Python; expiry and health arithmetic skewed by 1-2 h; breaks on DST change. | low | S | `data:F20`, `eng:F11` |
| A17 | Negative-equity names have missing (not worst) debt; near-zero equity gives 100× ROE and full marks. | low | S | `data:F26` |
| A18 | FX and benchmark fragility: stale-forever FX fallback, 109-day-old SPY cache, adjusted-close benchmark vs price-return stock leg, YTD from first bar. | low | S | `data:F27` |
| A19 | Cohort validation silently drops names yfinance can no longer price (AL, CPRX). The drop mechanism is real; the data auditor's reading that AL was a total loss is wrong: AL is marked at exactly $65.0000 with `asset_status inactive`, the signature of a completed cash takeover, so dropping it probably makes the C cohort look worse, not better (red team, `val:F3`). The bot also books it at zero, understating equity by 6.4%. | low | S | `data:F24`, red team |
| A20 | Insider signal: 8× more sales than purchases in the feed, 10b5-1 and sell-to-cover counted as negatives, computed for 50 names of an already-truncated list. | low | M | `data:F28`, `sig:F34`, `scout:F6` |
| A21 | yfinance is the single fundamentals source, "personal use only" by its README, with no contract and silent schema changes. | medium | L | `scout:F30` |
| A22 | Data hygiene: 1,627 cache files (268 not in the universe), `pe_ratio = inf` in one row, three names without sector. | low | S | `data:F29` |

### 3B. Quant screen and quality score (including industry blindness)

| ID | Weakness | Sev | Effort | Sources |
|---|---|---|---|---|
| B1 | **Industry blindness, confirmed on the held names.** "ROIC" is net income over (book equity + debt), meaningless for lenders, insurers, REITs and pass-throughs. AGM scores 5.7/22 and NNI 4.9/22 on lender artefacts (D/E 19.9, ROIC 0.6%); CCI 5.4/22 on REIT artefacts (payout 1.73, no FFO concept); AIZ 14.2/22 partly because reserve build counts as cash earnings. Sector overrides touch only D/E, FCF yield and current ratio. The composite goes the other way: it percentile-ranks across all sectors, so Financial Services fill 47% of its top quintile and Utilities 0%. Royalty trusts top it (PBT 98.5: ROE 99.8×). | high | L (proper models), S (interim exclusion/flagging) | `sig:F11`, `sig:F14-F17`, `sig:F8`, `scout:F22`, `scout:F24`, `data:F26` |
| B2 | The 16 screener weights are hand-set with no empirical basis and `score_stock` has never been tested; the PIT backtest tested the composite. | high (evidence gap) | M | `sig:F12`, `sig:F33`, `data:F11`, `scout:F4` |
| B3 | Hard filters (too-hard industries, cap, ADTV, positive P/E) gate only the screener; the Haiku/Sonnet queues rank the whole universe by stale `quality_score` (L5). | high | S | `sig:F2`, `sig:F15` |
| B4 | Missing data is punished quadratically in the screener (score × confidence) and rewarded in the composite (renormalised), so banks are screened out of the top 100 while negative-equity names look low-leverage in the composite. | medium | S-M | `sig:F8` |
| B5 | Loss-makers pass the "positive P/E" filter through the forward-P/E fallback (208 names, KHC, F, COIN). | medium | S | `sig:F10` |
| B6 | The top 100 is tilted to large caps and away from whole sectors (72 large / 24 mid / 4 small; 0 utilities, 0 energy, no banks). | medium | S | `sig:F13` |
| B7 | The quality definition is narrower than the tested factor (QMJ): profitability and leverage only; no gross profitability, no growth, no earnings-stability or beta, no payout or net-issuance leg; FCF yield is a value signal inside a "quality" score. | medium | M | `scout:F1`, `scout:F2`, `scout:F5`, `sig:F14` |
| B8 | No momentum anywhere at stock level; value and momentum are strongly negatively correlated, so a cheap-quality screen leans toward losers. | medium | S | `scout:F12` |
| B9 | Payout-ratio rule scores zero-dividend compounders (CPRT, DECK) at 0 and is not monotone above its max. | low | S | `sig:F9` |
| B10 | Double counting and period mixing: `real_fcf_yield` falls back to `fcf_yield` (scored twice), ROE/ROIC/ROE-consistency are all net-income-over-capital, TTM `.info` fields mixed with last-fiscal-year statement fields. | low | S | `sig:F7` |
| B11 | The insider tilt reshuffles only the top 50 by at most ~5 points and penalises the largest names; unproven. | low | S | `sig:F34` |

### 3C. Valuation and tier engine

| ID | Weakness | Sev | Effort | Sources |
|---|---|---|---|---|
| C1 | **Fair value is a confidence-weighted mean of incommensurable numbers** (one merged analyst-target leg, 18× EPS, Graham number, two 10-year DCFs sharing inputs; the scout's "two analyst legs" is wrong, the code merges Yahoo and Finnhub into one) with a median 2.5× spread between sources per name and one outlier setting the target (IBKR: DCF $873 vs price $85.5 gives a "75% margin of safety"; the median of sources gives −13%). Analyst targets are biased ~15 pp high and hit only 38% of the time; the Graham number has no evidence as a fair-value estimator. | high | S (median/trim) / M (industry-appropriate method) | `sig:F18`, `scout:F7`, `scout:F8`, `scout:F9` |
| C2 | **DCFs on operating cash flow are applied to brokers and insurers and produced the buys.** AIZ was bought on an insurer DCF of $417 vs the LLM's $260; HCA and GDDY (negative or near-zero equity) are the other A-tier entries. | high | S (skip DCF for financials) / M | `sig:F19`, `data:F19`, `scout:F24` |
| C3 | **S tier has never been assigned and structurally cannot be:** WIDE+HIGH occurred in 1 of 293 analyses (HIGH conviction 7 of 293, 0 since the parser fix) and price ≤ 0.75 × fair value in 7 of 64 priced assignments; expected S count ≈ 0.1. The real buy tier is "narrow/wide moat, MEDIUM conviction, statistically cheap", a value screen, not "wonderful business at a fair price". | high (design) | M | `sig:F23` |
| C4 | The 25% margin of safety is enforced once; deployment then buys any valid A down to −12.5% MOS for up to 180 days. | medium | S | `sig:F22` |
| C5 | The "extreme premium >50%" cliff is measured against the target, so it fires at 12.5% above fair value; half of priced quality names land in C with no-moat names, and C names are re-analysed every 30 days. | medium | S | `sig:F24` |
| C6 | DCF internals: abrupt growth fade to a 12× terminal (1.5% perpetual), no balance sheet, 3-4-year endpoint CAGRs, and an owner-earnings variant that adds back growth capex but projects the same growth. | medium | S-M | `sig:F20` |
| C7 | Neither per-source estimates nor the fair-value source are stored for past analyses; the valuation that set a tier cannot be audited afterwards. | medium | S | `sig:F21`, `eng:F26` |
| C8 | Most historical tiers were not price-checked (87 "no price data — watching" B rows), so tier history mostly records "no price", not a judgement. | medium (evidence) | S | `sig:F26` |
| C9 | Fair value can rest on a single resolved estimate with no minimum count and no "valuation confidence" field. | medium | S | `eng:F26` |
| C10 | Tier sizing in the tier engine and the briefing (S 25% in 3 tranches, A 15% in 2) is not what the trader does (15% × tier weight in one order). | low | S | `sig:F25` |
| C11 | The fair-price exception reads a quality score that is stale or top-100-relative and has fired 0 times. | low | S | `sig:F27` |

### 3D. AI pipeline

| ID | Weakness | Sev | Effort | Sources |
|---|---|---|---|---|
| D1 | **90% of stored labels (265 of 293) came from the rating parser fixed on 2026-10-01, and raw model responses were never stored**, so the labels cannot be re-parsed; the DB holds no model id, prompt version or parser version. | high | S (store) / M (re-rate) | `ai:F1`, `ai:F5`, `val:F14` |
| D2 | **The current parser still mis-reads bold labels.** `**Durability:** MODERATE` loses the target entry price and routes the moat through a section-wide word scan; 63% of rows have `moat_sources = ["unknown"]`, 74% have no target entry, customer concentration parses "high" for 76% of files. Lead reproduced the target-entry loss on a full 11-section template. | high | S (strip markdown) / M (forced tool call) | `ai:F2` |
| D3 | **The breaker check has never run in production and the 0/19 replay had no positive control**; its input was Haiku's write-up, not news; real candidate positives exist in `news_events` (CPRT debt-funded tender, AAPL CEO change, FICO). | high (unproven) | S | `ai:F6` |
| D4 | The Sonnet breaker cap (10/week) is unpaced, shared with the old re-analysis path, and already binding; the Haiku news pool runs dry with MSFT, META, V, MA, COST at the tail of the queue every week. | high | S (code-owned caps; needs an image rebuild) | `ai:F10`, `eng:F31` |
| D5 | Breakers: 89% were written under the old prompt (27% carry sell/opinion wording), 36% are filing-only metrics a headline never carries, and the list is overwritten at every re-analysis (CPRT lost its debt-funded-acquisition breaker on the day the ACV news arrived). "Pre-registered" is true only until the next analysis. | medium | S-M | `ai:F8`, `ai:F9` |
| D6 | The Haiku gate's recall cannot be measured: 428 names failed and were never looked at again; the 1-5 hints are not stored; the pass threshold is hard-coded at 6, not `HAIKU_MIN_SCORE`. | medium | S | `ai:F15` |
| D7 | Re-analysis consistency on stored data is poor and mostly unmeasurable: of 108 consecutive pairs, 97 end in a reduced-context news re-analysis; moat changed in 22%, conviction 22%, fair value by >20% in 34%; NKE was analysed 13 times in 5.5 months with fair values from 4.9 to 65. | medium | S (tag rows) | `ai:F17` |
| D8 | Model-version contamination: ratings span Sonnet 4.5, 5 and 5.5 with no model column; Sonnet 5.5's stated cutoff (June 2026) overlaps 6 of the 19 breaker-replay cases and any future replay on Apr-Jun 2026 data. | medium | S | `ai:F5`, `scout:F16` |
| D9 | One Sonnet verdict on attacker-influenced Finnhub text forces a full sell; fencing is partial (closing tags not escaped; model-written text re-injected unfenced). | medium | S / M (two-call confirmation) | `eng:F22`, `ai:F25` |
| D10 | About 60% of the deep prompt's output (currency splits, insider ownership, bear case, outlook) feeds nothing; the prompt asks for estimates "from your knowledge" and does not tell the model to prefer the supplied price (BKNG: price 180 supplied, fair value 3,900-5,200 returned). | low | S | `ai:F16`, `ai:F3` |
| D11 | Prompt caching does nothing measurable: four of five system prompts are below any cache minimum and the A/B logged 0 cache reads; the README claims the saving. | low | S | `ai:F19` |
| D12 | Cost is a count × constant in `run_log` for two of five paid paths; news spend (~$5.8 to date) is never logged; actual run-rate is ~$2.1-2.3/month vs the README's $10-15. The caps, not money, are the binding resource. | low | S | `ai:F20`, `eng:F13` |
| D13 | Opus second opinion is dead code (legacy script only, flag false, S tier never assigned). | low | S | `ai:F22` |
| D14 | Batch handling: cancelled-batch results discarded though billed; errored items dropped without refund; no `max_tokens` truncation check; file cache keyed by symbol only, so a cached analysis is re-stamped as fresh. | low | S | `ai:F23` |
| D15 | `_thinking_kwargs` falls through to `disabled` for any unknown model string, which the docstring says Sonnet 5.5 rejects; one env change can take down every LLM job. | low | S | `ai:F24` |
| D16 | The Friday trade job pays for up to 20 Haiku screens on a different input (business description) before a tier filter that makes them moot; outside the budget system. | low | S | `ai:F26`, `eng:F32` |
| D17 | LLMs show documented preference for large, famous, tech names; the Haiku pass rate by cap and sector has never been checked. | medium (untested) | S | `scout:F20` |
| D18 | The news-LLM literature says the tradable part of a news signal is small and decays; a breaker fired at 20:00 Copenhagen sells into a priced move. | medium | S (measure) | `scout:F13` |

### 3E. Deployment, regime, trading and journal

| ID | Weakness | Sev | Effort | Sources |
|---|---|---|---|---|
| E1 | **The journal does not match the broker.** ACVA and AESI buys ($17.8k) never filled but are "open" forever; AGM and AL were held but never journalled; ACT's sale has no `closed_trades` row; AD's recorded exit is the Friday quote on a market holiday (−3.7% recorded, −5.8% actual, alpha ≈ −6.7% not −3.7%); the new reconcile path has never closed a trade in production. | high | S | `val:F4`, `val:F5` |
| E2 | **Tier attribution is impossible for the existing history:** 7 of 10 buys have no tier; `paper_positions.tier_at_entry` is a default, not the entry tier; both closed trades group under "?". | high | S (forward only) | `val:F6` |
| E3 | The regime classifier reduces to "did SPY close at a 52-week high": P/E (24.2-28.0, always ≥23) and VIX (14.2-20.4) votes were constant on all 111 logged days, so the only varying vote is the drawdown; confidence is ignored; a single surviving signal can set euphoria or crisis; nothing tests regime timing. | medium | S | `sig:F28`, `eng:F27`, `scout:F10` |
| E4 | Friday 16:15 NY after-close DAY orders: sells are always pending at submit, so buys are always deferred a week; an exception after submit is journalled as `skipped` and never reconciled. | medium | S / M | `eng:F8`, `eng:F29` |
| E5 | A truncated broker response is accepted as truth (2026-09-16: 3 of 8 positions, equity $58.7k, a permanent −45% point in the curve); on a Friday it could double-buy held names; on a Monday it prunes the mirror. | medium | S | `eng:F9`, `data:F15`, `val:F2` |
| E6 | Per-trade alpha mis-dated and mis-defined: benchmark from decision close to reconcile close vs stock from Monday open fill to exit; SPY dividend-adjusted vs stock price-only; `filled_at` is reconcile time. | medium | S | `val:F7`, `eng:F29` |
| E7 | No counterfactuals: candidates not bought, names excluded by NONE/LOW, cancelled sells and what they did afterwards are never stored; `single_buy_recommendations` has 0 rows. | medium | S-M | `val:F9` |
| E8 | Nothing compares the equity curve to any benchmark (`get_equity_curve` has no callers); the briefing prints the least reliable evidence (cohort table) without n or CIs. | medium | S | `val:F10` |
| E9 | Paper economics differ from the ASK reality: no dividends credited (cash flat 86 days), no commissions, no FX spread, fractional notional fills, and AL's $65 cash takeover is treated as "untradable" rather than cash (understates equity 6.4%). The paper account does not test the one-lot DKK strategy the user would run. | medium | M | `val:F3`, `eng:F36` |
| E10 | `converged` is 1 whenever the stock rose; `reasoning_sound` is scored only for take-profit sells; the briefing's "Sound =" number mostly reflects whether the market rose. | low | S | `val:F8` |
| E11 | `daily_watchlist_check` loops 10 of 25 names, logs "Checking 25", and never notifies. | low | S | `eng:F28` |

### 3F. Validation and evidence

| ID | Weakness | Sev | Effort | Sources |
|---|---|---|---|---|
| F1 | **The moat-NONE cohort behind ruleset 3.0.0 is a migration artefact** (section 1); the same rows still feed the monthly cohort job and are missing from the WIDE cohort; README, `deployment.py` and the research note state the result as evidence. The red team rates it high rather than critical because NONE occurs in only 4 of 273 non-migrated analyses, so the rule rarely binds and is sensible on priors; the lead keeps "critical" for the published claim and agrees "high" for the money impact. The AI auditor's attribution of the NONE labels to the parser default is wrong for 20 of the 24 rows; the migration default is the cause. | critical (claim) / high (impact) | S | `val:F13`, `sig:F31`, `ai:F1`, red team |
| F2 | The LOW-conviction cohort is a parser artefact; n=27 in the docs vs n=25 stored; clean n=17 gives −3.9 pp (t −0.58) since rating and +0.8 pp at 90 days. | high | S | `val:F14` |
| F3 | Ruleset stamps are empty on 0 of 16 decisions and 0 of 293 tier changes; cohort rows are stamped with the validator's version, not the rating's; the changelog omits at least 6 rule changes and 2 model changes; no window is a single-ruleset experiment. | high | S | `val:F17`, `val:F18` |
| F4 | Statistical power: 87-126 years for 3 pp/yr; cohort testing needs ~160 names per arm for 5 pp/90d and the pipeline produces 0-35 first ratings a month with NONE at 1.5% prevalence. | high | n/a | `val:F11`, `val:F12`, `scout:F11` |
| F5 | PIT backtest: split look-ahead (adjusted prices × as-filed shares; 734 split-like + 109 unit-error observations, concentrated in the top and bottom quintiles). | high | S | `val:F20`, `data:F6` |
| F6 | PIT backtest: missing debt tags scored as zero debt for 12-17% of names (AES, AGNC among them). | high | S | `data:F8` |
| F7 | PIT backtest: the universe is today's survivors and today's size/liquidity screen; equal-weight universe +23.5%/yr vs SPY +16.2% is the signature of the bias; one ticker-reuse artefact (CHRD +19,097%) is ~46% of the 2020 bottom-quintile mean. | high | L | `data:F10`, `data:F7`, `val:F23`, `scout:F29` |
| F8 | PIT backtest: concepts mixed across fiscal years in 13-21% of observations; no age cap. | medium | S | `data:F9`, `val:F21` |
| F9 | The PIT backtest does not test the live score (different FCF definition, no SBC haircut, annual-only vs TTM, 5-metric composite vs 16-metric screener, full cross-section vs top-100 percentiles). | medium | M | `data:F11`, `sig:F33` |
| F10 | The 2026-10-17 note's per-date table and medians come from no committed code; every headline statistic is consistent with zero (yearly Spearman t ≈ 0.8; median spread t = 0.49; top quintile beat SPY 5 of 10). | medium | S | `val:F22` |
| F11 | Cohort methodology: since-rating windows of 0-192 days, no beta/sector adjustment, 18 cells tested and the worst two promoted to a rule, dead names dropped, migrated rows dated to the migration. | medium | S-M | `val:F15`, `ai:F4` |
| F12 | The tier-A cohort (+20.6 pp, n=8) is carried by CTSH and IT and contaminated by AIZ (insurer DCF) and BIO (migrated row). | low | S | `val:F16`, `sig:F32` |
| F13 | A backtest of the whole pipeline is not feasible: the LLM stage cannot be run point-in-time; the only valid LLM evidence is forward. | high (constraint) | n/a | `val:F25`, `scout:F16`, `scout:F19` |
| F14 | SPY is the wrong benchmark for a ~70%-invested US mid/small-cap quality book (realised correlation 0.03); nothing is DKK-, after-tax- or random-portfolio-aware. | medium | S-M | `val:F26`, `sig:I4` |
| F15 | Backtest modes 1 and 2 are dead code; mode 1 correlates today's score with trailing returns (mechanically anti-correlated). | low | S | `val:F24` |
| F16 | The first ratings (83%) had no 10-K in the prompt, so the cohort does not test the current prompt either. | medium | n/a | `ai:F4` |

### 3G. Engineering and operations

| ID | Weakness | Sev | Effort | Sources |
|---|---|---|---|---|
| G1 | **Seven weeks of silent blackout (2026-08-11 to 2026-10-01):** commit `ae3647b` made paid jobs opt-in and the live `.env` lacked the keys; a second total LLM failure (10-01 to 10-07: 210 of 210 news calls null, two batch runs never completed, no October briefing) is undiagnosable because container logs start 2026-10-18. Nothing alerted either time. | high | S | `eng:F3`, `eng:F4`, `ai:F12`, `data:F16` |
| G2 | Job failures are log lines; 8 of 13 jobs write no run record; health checks read MAX(date) and stay green through the top-100 staleness, the 51-day news gap, the 63% parser fill failure and orphan `run_log` rows. | high | M | `eng:F10`, `data:F16`, `ai:F27` |
| G3 | One thread, no loop-level exception guard, no watchdog or HEALTHCHECK, no SMTP or Anthropic timeouts, no catch-up after restart (a restart at 01:50 Monday skips the budget reset and starves the week). | medium | S-M | `eng:F5`, `eng:F6`, `eng:F7` |
| G4 | Logging is stdout only with a 3×10 MB driver, lost on recreate; `LOG_LEVEL` is read by nothing; root causes logged at DEBUG. | medium | S | `eng:F12` |
| G5 | Two screening pipelines plus a 2,138-line legacy JSON path (`registry.py`, `portfolio.py`, `run_monthly_briefing.py`) that is the Docker default CMD and calls Sonnet. | medium | S (CMD) / L (retire) | `eng:F15` |
| G6 | CI pushes `:latest` even when lint/tests fail (verified: commit `fe1fa95`, lint failure, image success); the repo compose enables watchtower, the live copy disables it by hand. | medium | S | `eng:F16` |
| G7 | Dependencies are floors only, no lock file, image rebuilt from floating versions, 9 dependabot PRs open since June; the Finviz corruption (L2) is what an unpinned dependency change looks like. | medium | S | `eng:F17`, `data:F18` |
| G8 | Coverage is 60% overall and lowest where decisions are made (bubble_detector 14%, screener 44%, valuation 63%, paper_trader 51%); the NaN-sort regression test checks a copy of the sort key; five test files stub `schedule`/`anthropic` with global MagicMocks; no property, snapshot, or real-model-output fixtures; no migration test on a real old DB. | medium | M | `eng:F18-F20` |
| G9 | Secrets: `.secrets.baseline` holds SHA-1 hashes of real `.env` lines; `starttls()` without a verified context before SMTP login; `.claude/settings.local.json` allows `docker exec *` and `git push *`. | low-medium | S | `eng:F21` |
| G10 | `Database()` writes (schema, migrations, budget sync) on every open, including read-only scripts; the default path is CWD-relative, so a stray empty DB appears in the repo. | low | S | `eng:F23` |
| G11 | Backup is a live-file `rclone sync` of a WAL database with no `.backup`/`VACUUM INTO` step, no integrity check and no restore drill; 97% of the file is re-derivable `pit_fundamentals`. | low-medium | S | `eng:F24` |
| G12 | Schema is additive-only with `user_version = 0`; no record of which migrations ran. | low | S | `eng:F25` |
| G13 | Config evaluated at import (a malformed value crash-loops the container); switches read once at start; opposite boolean polarities; 11 tunables absent from `.env.example`; `LOG_LEVEL` and `MAX_SECTOR_PCT` read by nothing (L6). | low-medium | S | `eng:F30` |
| G14 | The Friday job has no per-cycle idempotency key; a manual re-run after 00:00 UTC bypasses the duplicate guard. | low | S | `eng:F32` |

### 3H. The Danish investor: DKK, ASK, tax, real account

| ID | Weakness | Sev | Effort | Sources |
|---|---|---|---|---|
| H1 | No DKK equity curve and no FX attribution in any report. From the snapshots: USD +5.77%, DKK +7.73% since 2026-07-02; FX contributed about +1.9 pp and nobody can see it. | medium | S | `scout:F44`, `data:I4`, `val:F26` |
| H2 | ASK tax is modelled only as a 17% dividend haircut in legacy `portfolio.py`; the real rule is 17% on annual mark-to-market (so rotation has no tax cost and FX gains are taxed whether or not the stock moved); US 15% withholding with relief capped at 17% is not modelled; `income_events` has 0 rows. | low-medium | S-M | `scout:F41`, `scout:F42`, `eng:F36` |
| H3 | The paper engine tests a different strategy from the real one: fractional USD notionals vs 5,000 DKK lots on Nordnet with ~25 DKK commission and ~0.25% FX; the single-buy path that models the real lot has never written a row; Nordnet's API is closed to new customers, so the real account stays advice-only. | medium | M | `val:F3`, `scout:F31` |
| H4 | The live `.env` ASK limit is the 2025 figure; the room formula uses the previous year-end value, not cumulative deposits. | low | S | `scout:F39` |
| H5 | USD hedging is undecided and unmeasured; the evidence says unhedged USD has dampened equity drawdowns for EUR-bloc investors and a hedge cannot sit inside an ASK, but the briefing should at least show the FX leg. | low | S | `scout:F43` |
| H6 | The 500,000 DKK ceiling is a proposal in the 2027 finance bill, not law; nothing should plan around it before spring 2027. | info | n/a | `scout:F40` |

### 3I. What all six auditors missed (red team)

The red team's own findings, each re-checked by the lead where a query could do it.

| ID | Weakness | Sev | Effort | Sources |
|---|---|---|---|---|
| I1 | **The real ASK account does not exist in the system.** Nordnet is a placeholder (`src/accounts/__init__.py:22`, "Phase D"). The single-buy recommendation for the real account is computed from the paper account's holdings and slot count inside the paper loop; there is no record of what the user actually owns, no sell or breaker advice for real holdings, and `ASK_CASH_DKK` is not set, so "affordable" is always None. The ASK ceiling (174,200 DKK ≈ 35 lots) means the real book grows lot by lot over years, a path the deploy-to-90% paper engine never tests. | high | M | red team M1 |
| I2 | The DKK-lot recommendation is skipped whenever the paper account has a pending sell (every after-close sell), when the regime fetch fails, when `AUTO_TRADE_ENABLED` is false, or when no paper account is enabled (L8). | medium | S | red team M2 |
| I3 | Lot sizing ignores whole shares: at USD/DKK 6.68 a lot is $748; 7 of 113 priced names cost more than one lot (NVR, GWW, MPWR, MCK, COST, NEU, FICO); rounding down leaves >20% of the lot unspent for 30 of 113. | medium | S | red team M3 |
| I4 | No cost or turnover break-even anywhere; no index-fund null (section 1). | high | S | red team M4 |
| I5 | The LLM moat label is mostly company size (section 1); the funnel plus the screener's large-cap tilt selects famous large caps. | high (for the edge claim) | S | red team M5, `scout:F20` |
| I6 | Fixing the watchlist funnel yields at most one buy: of the four A names, BIO is an expired migrated row (moat NONE), IT is 61% above target (beyond the −12.5% ceiling), AIZ is held, AZZ expires 2026-10-28. A-tier supply, not plumbing, is the binding constraint; the three C-tier holdings (37% of equity) have no exit either. | high | M | red team M6 |
| I7 | Live config drift explains the July discontinuity: the live `screening_criteria.yaml` was rewritten 2026-07-02 17:28; with an empty or old file `criteria.scoring` is empty, which skips the sort, the top-100 cut and the too-hard filter (hence ~1,080 rows a week and too-hard names until 06-29) and runs the NaN crash path from 07-06. The loader falls back to defaults silently; config lives outside git and the image with no provenance. | medium | S | red team M7 |
| I8 | The one winner was a cheapness pick the LLM itself doubted (section 1). | medium (interpretation) | S | red team M8 |
| I9 | `weekly_auto_trade` reads a watchlist of any age with a `/tmp` fallback and no `updated_at` check. | low | S | red team M9 |
| I10 | The PIT backtest's FCF yield also carries dividend look-ahead (auto-adjusted prices are lowered by later dividends), concentrated in utilities, staples and REITs; for a 3% yielder the 2016 price is adjusted down by roughly a quarter. | low-medium | S | red team M10 |

---

## 4. Every improvement idea, grouped by area

Nothing from any report is discarded. Each idea carries its source and the weaknesses it addresses. Ideas that fall outside the current direction are in section 5. No ranking is implied by order.

### 4A. Data layer

| ID | Idea | Effort | Addresses | Source |
|---|---|---|---|---|
| IA1 | Persist every scored name on Monday (apply `top_n` only to the returned list); rank quality scores on the full scored set; null out scores not refreshed this week. | S | A1, A2, B3, B6 | `data:F1`, `sig:F1`, `eng:F2` |
| IA2 | Put an as-of date on every fundamentals row and in every LLM prompt; refuse rows older than N days; fetch live `.info` for the 10-50 names actually sent to an LLM. | S | A2 | `data:F2` |
| IA3 | Union held tickers into the weekly refresh set; backfill `paper_positions` entry data from fills. | S | A13 | `data:F23` |
| IA4 | One universe: drive the weekly screen and the trader from the DB universe plus tiers; validate that a fetched universe resolves ≥90% before caching it. | S-M | A4, L2 | `data:F5`, `eng:F1`, `sig:F29` |
| IA5 | Key the universe on CIK via SEC `company_tickers.json` and submissions; rebuild on a schedule; mark names failing N consecutive fetches `in_universe = 0`; map ticker changes; collapse share classes to one analysis per CIK; keep delisted CIKs with last price for survivorship-aware tests. | M | A3, F7 | `data:I2`, `eng:F14` |
| IA6 | Start an own live point-in-time record now: append-only weekly rows of raw `.info` + statements for every scored name, with fetched-at (~70k rows/yr). | S | A12, F9 | `data:I1` |
| IA7 | Move live fundamentals onto EDGAR companyfacts (TTM from the 10-Q rows already in `pit_fundamentals`), keeping yfinance for price, market cap and analyst targets; one definition set for live and backtest. EDGAR frames can pull one concept for the whole market in one call at 10 req/s, no key. | L | A21, F9, A6-A8 | `data:I3`, `scout:F26`, `scout:F47` |
| IA8 | Fix the 10-K slicer: take the longest matching span, not the last match; require a length floor; store the filing date; raise the per-section cap to a budgeted figure; build a 10-filing test set with known section starts. | S | A5 | `data:F12` |
| IA9 | `dropna()` before computing trend endpoints; compute years from dates; set `earnings_consistency` ideal to the achievable maximum. | S | A6 | `data:F13`, `sig:F3`, `sig:F4` |
| IA10 | Prefer "Ordinary Shares Number" for share counts; sanity-check against split events; or take shares from EDGAR. | S | A7 | `sig:F5` |
| IA11 | Derive all FCF metrics from one statement and one period; never fall back silently from `real_fcf_yield` to `fcf_yield`. | S | A8 | `sig:F6`, `eng:F26` |
| IA12 | Normalise `dividendYield` at the fetch boundary like `debtToEquity`, with a unit test on KO. | S | A9 | `data:F14` |
| IA13 | News pipeline: distinguish transport errors from empty results; widen to `days_back=3` with the existing headline de-dup; drop items whose headline does not mention the company or whose `related` list is long; word-boundary keyword regex; refund budget on failed calls; log every headline, not one per ticker per day. | S | A10 | `data:F17`, `ai:F11`, `eng:F31` |
| IA14 | Retry with backoff, timeouts and an error-rate threshold that aborts a run and alerts when more than N% of fetches fail. | M | A11, G3 | `data:F18`, `eng:F6` |
| IA15 | Retention: keep the first snapshot of each month (`GROUP BY strftime('%Y-%m')`), or archive instead of delete. | S | A12, G12 | `data:F22`, `eng:F25` |
| IA16 | Store UTC ISO timestamps with offset everywhere. | S | A16 | `data:F20`, `eng:F11` |
| IA17 | Treat unpriced-after-rating names as −100% (or last trade / deal value) in cohorts and report `n_unpriced`. | S | A19, F11 | `data:F24`, `val:F15` |
| IA18 | Cap ROE or rank only when equity/assets exceed a threshold; treat negative equity as worst for leverage. | S | A17 | `data:F26` |
| IA19 | Log analyst-target source availability; add a second price-target source or accept Yahoo-only and say so. | S | A14 | `data:F19` |
| IA20 | Backfill pre-fix `fair_value` with the aggregated value; add a `fair_value/price` outlier flag and a low ≤ high check. | S | A15, D10 | `data:F25`, `ai:F3` |
| IA21 | Pin dependencies (`pip-compile --generate-hashes`), let dependabot bump the lock; add recorded-response contract tests for vendor shapes (`dividendYield`, `debtToEquity`, NaN oldest column, Finviz ticker shape). | S | A11, G7 | `data:I7`, `eng:F17` |
| IA22 | A per-run data-quality log: universe size, fetched OK, 404s, 429s, rows written, share of universe younger than 14 days, median input age at LLM time; alert on thresholds. | S | A11, G2 | `data:I6` |
| IA23 | Rebuild the insider signal from the SEC Insider Transactions Data Sets (free, 2006 onward, 10b5-1 flag) with a per-insider multi-year history so routine and opportunistic trades can be separated; purchases only. | M | A20, B11 | `scout:F6`, `scout:F27`, `data:F28` |
| IA24 | Take USD/DKK from Danmarks Nationalbank via Frankfurter (free, keyless, since 1977) for tax and DKK reporting. | S | A18, H1 | `scout:F31` |
| IA25 | Prune the yfinance cache; sanitise `inf`; fill missing sectors. | S | A22 | `data:F29` |

### 4B. Quant screen and quality score

| ID | Idea | Effort | Addresses | Source |
|---|---|---|---|---|
| IB1 | Industry-aware scoring: map yfinance `industry` to ~12 groups (banks, insurers, asset managers/brokers, REITs by type, utilities, lessors, royalty/MLP, capital-intensive cyclicals, asset-light services, consumer brands, tech, healthcare); per-group metric sets (banks ROA/ROTCE/efficiency/NPL/CET1/P-TBV; insurers combined ratio/reserve development/BVPS CAGR/P-B/ROE; REITs AFFO-FFO yield and payout/net debt-EBITDA/occupancy; utilities allowed vs earned ROE/rate-base growth/FFO-debt; lessors net spread/leverage/residual impairments); EDGAR fields needed (InterestIncome/Expense, Deposits, ProvisionForLoanLosses, PremiumsEarned, PolicyholderBenefits, RealEstateDepreciation and gains, Goodwill and Intangibles); winsorised z-scores within group, fallback to sector when n < 8. FFO/AFFO are non-GAAP and live in 8-K exhibits, not XBRL tags (unverified), so REIT metrics need press-release parsing or a paid vendor. | L | B1 | `sig:F16`, `scout:F22`, `scout:F24` |
| IB2 | Interim: rank the composite within sector (fallback to universe below n=8); exclude or flag financials, REITs, lessors, royalty trusts and MLPs as "unscored" until IB1 exists. | S | B1, B4 | `sig:F14`, `sig:F15`, `scout:F24` |
| IB3 | Re-apply the hard filters (too-hard industries, cap, ADTV, positive P/E, entity type) in the Haiku/Sonnet queue queries, not only in the screener. | S | B3, L5 | `sig:F2` |
| IB4 | PIT-test `score_stock` itself (trend metrics from as-known multi-year series) and a sector-neutral composite before changing any weight; report raw vs neutral, per-year Spearman, median spreads, bootstrap CIs; commit the report generator. | M | B2, F9, F10 | `sig:I2`, `val:I3` |
| IB5 | Pick one missing-data rule (penalise or renormalise) and use it in both the screener and the composite. | S-M | B4 | `sig:F8` |
| IB6 | Close the forward-P/E loophole: require positive trailing earnings for the P/E filter and the P/E valuation. | S | B5 | `sig:F10` |
| IB7 | Add a gross-profitability leg (GrossProfit/Assets from companyfacts), a net-issuance leg (shares outstanding change), and a safety leg (earnings volatility or beta), so the score resembles the tested quality factor. | S-M | B7 | `scout:F1`, `scout:F2`, `scout:F5` |
| IB8 | A 12-1 month momentum rank as an entry veto or tie-breaker (prices already in yfinance), tested on the PIT panel first. | S | B8 | `scout:F12` |
| IB9 | Fix the payout rule (zero-dividend is not worst; monotone above max) or drop it. | S | B9 | `sig:F9` |
| IB10 | Remove the double counting: do not score `real_fcf_yield` and `fcf_yield` when they are the same number; consolidate the three net-income-over-capital metrics; align periods. | S | B10 | `sig:F7` |
| IB11 | Report sector and cap mix of the top 100 in the briefing; cap industry concentration at 2 of 8 positions; report industry weights of the book. | S | B6, scout industry-momentum point | `sig:F13`, `scout:F23` |
| IB12 | Winsorise inputs and exclude pass-through entities by `quoteType`/industry before percentile ranking. | S | B1 (trusts) | `sig:F15` |
| IB13 | A numeric baseline (logistic or gradient-boosted on the same fundamentals) to beat before paying Sonnet for a judgement. | M | B2, D-area | `scout:F18`, `scout:F45` |
| IB14 | Check the Haiku pass rate and mean moat by market-cap decile and sector (one DB query) to see whether "moat" is a size tilt. | S | D17 | `scout:F20` |

### 4C. Valuation and tier engine

| ID | Idea | Effort | Addresses | Source |
|---|---|---|---|---|
| IC1 | Replace the weighted mean with a median or trimmed mean of sources; drop analyst targets from "intrinsic" value or haircut them by their documented optimism and keep them as a sanity bound. | S | C1 | `sig:F18`, `scout:F7` |
| IC2 | Skip the FCF DCF for Financial Services, insurers and brokers; use P/B vs ROE or excess-return models; route REITs to FFO/AFFO multiples. | S / M | C2, B1 | `sig:F19`, `scout:F24` |
| IC3 | Persist a valuation audit trail per analysis and per decision: `AggregatedValuation.to_dict()`, `fair_value_source`, price, gap, estimate count. | S | C7, C9 | `sig:I1` |
| IC4 | Require a minimum number of resolved estimates and store a "valuation confidence" that `plan_buys` can weight. | S | C9 | `eng:F26` |
| IC5 | Re-evaluate the price leg of the tier at trade time, or make the −12.5% ceiling an explicit policy and say so in the briefing. | S | C4 | `sig:F22` |
| IC6 | Express tier thresholds as price/fair-value, keep quality and price as separate fields, and stop filing "good business, 13% pricey" with "no moat" in C. | S | C5 | `sig:F24` |
| IC7 | Decide what S should mean (for example WIDE + MEDIUM/HIGH + price ≤ fair value from an appropriate method) and give conviction and moat an observable rubric in the prompt (ROIC above cost of capital for 10 years, share stability); track label distribution per run. | M | C3 | `sig:F23`, `sig:I3` |
| IC8 | DCF: smooth the growth fade, add net cash, use multi-year average growth rather than endpoint CAGRs, and either fix the owner-earnings variant's growth or drop it; log a sensitivity table (discount 8-12%, terminal 10-16×) per valuation. | S-M | C6 | `sig:F20`, `scout:F9` |
| IC9 | Leave-one-out ablation of each fair-value leg (analyst, P/E, Graham, DCF real, DCF owner) against forward returns on the PIT panel; keep legs that add information. | M | C1 | `scout:F8`, `scout:F9` |
| IC10 | Down-weight or drop the Graham number for intangible-heavy sectors. | S | C1 | `scout:F3`, `scout:F8` |
| IC11 | Make tier sizing and trader sizing one policy; retire the unused tranche logic or implement it. | S | C10 | `sig:F25` |
| IC12 | Tag tier-history rows by ruleset and by whether a price gap existed; filter cohorts accordingly. | S | C8, F3 | `sig:F26` |
| IC13 | Remove or re-base the fair-price exception once the quality score is on one scale. | S | C11 | `sig:F27` |

### 4D. AI pipeline

| ID | Idea | Effort | Addresses | Source |
|---|---|---|---|---|
| ID1 | Store everything needed to audit a judgement: one append-only `llm_calls` table with model id, prompt hash, parser version, raw response and `usage` (tokens, cache read/write). | S | D1, D8, D11, D12 | `ai:I2` |
| ID2 | Replace free-text parsing with a forced tool call (`record_analysis` with enums for durability/conviction/capital allocation, numbers for fair value and target, an array of 2-4 breakers each with a `news_observable` flag); read `tool_use.input`; clean `stop_reason`. Interim: strip `*`/`_` and normalise labels in `extract_field`. | M (S interim) | D2, D10 | `ai:I1`, `ai:F2` |
| ID3 | Monitor every held name regardless of tier; version breakers as immutable rows (`breaker_id`, `analysis_id`, `registered_at`, text, observable flag) and check news against all versions registered in the last 90 days. | S | L3, D5 | `ai:I7`, `ai:F7`, `ai:F9` |
| ID4 | Build a breaker positive-control set (the 24 `news_events` rows where Haiku claims a direct match, plus synthetic cases) and report recall as well as specificity, using the real ~265-character Finnhub snippet; tag breakers news-observable vs filing-only and check the latter against the next 10-Q. | S (~$0.10) | D3, D5 | `ai:I4`, `ai:F6`, `ai:F8` |
| ID5 | Raise and pace the Sonnet news cap (e.g. 60/week with a reserved slice for held names); raise the Friday queue limit from 5; clear the backlog once (~372 deep analyses ≈ $8.5, 891 screens ≈ $0.33) after the parser is fixed; move the caps to config. | S (image rebuild) | D4, L4 | `ai:I3`, `ai:F10`, `ai:F13` |
| ID6 | Store the 1-5 Haiku hints; run a 50-name sample of Haiku fails through Sonnet (~$1.2) to measure the gate's false-negative rate; make the threshold the configured one. | S / M | D6 | `ai:F15` |
| ID7 | Mark news-path rows `reduced_context` and exclude them from cohort work; add `model`, `prompt_version`, `parser_version` columns and fill them. | S | D7, D8 | `ai:F17`, `ai:F5` |
| ID8 | Prune the unused prompt sections or restructure as a tool call; tell the model to prefer supplied price and metrics over memory; validate low ≤ high and a 0.3-3× price band on LLM fair values. | S | D10 | `ai:F16`, `ai:F3` |
| ID9 | Cache the full per-ticker 10-K sections (25-40k tokens) as a prefix and ask moat, breaker and consistency questions against it; this is where caching would pay and it removes the 8k truncation. | M | D11, A5 | `ai:I5` |
| ID10 | Use Opus (or a second independent Sonnet sample) only where it can change a decision: names about to be bought and held names flagged REVIEW (under $2/month). | M | D13 | `ai:I6` |
| ID11 | Batch hygiene: read results of a cancelled batch before discarding; refund errored items; detect `max_tokens` truncation; key the file cache by model and prompt version. | S | D14 | `ai:F23` |
| ID12 | Probe the thinking parameter on first call and cache it, or fail container start on a 400. | S | D15 | `ai:F24` |
| ID13 | Escape closing tags and control sequences in fenced third-party text; fence model-written text that derives from it; require a fired breaker to match across two independent items, or a second confirmation call, before a forced sell. | S / M | D9 | `ai:F25`, `eng:F22` |
| ID14 | Check tier before the Friday Haiku screen; reuse the stored Wednesday screen instead of a third differently-fed gate. | S | D16 | `ai:F26` |
| ID15 | Measure cost from `usage`, not constants; log every paid path; correct the README figure. | S | D12 | `ai:F20`, `eng:F13` |
| ID16 | Anonymise ticker and name in any retrospective LLM test (distraction can outweigh look-ahead in-sample); include an anonymised arm in the replay harness. | S | D8, F13 | `scout:F14` |
| ID17 | Log price at breaker-flag time and the 1/5/20-day forward return for every flag, to see whether sells chase a priced move. | S | D18 | `scout:F13` |
| ID18 | Alert on parser fill-rate below 80%, on null-verdict rate above 20% in a day, and on any failed batch. | S | D2, G2 | `ai:F27` |
| ID19 | The DeepSeek Flash swap: the repo's own A/B concluded "keep Haiku"; the saving is ~$0.60/month; the A/B measured agreement, not recall. Re-run it on the positive-control set if ever reconsidered. | S | D12 | `ai:F21` |

### 4E. Deployment, regime, trading and journal

| ID | Idea | Effort | Addresses | Source |
|---|---|---|---|---|
| IE1 | Reconcile the journal to the broker: mark the two phantom buys unfilled; add flagged `pre_journal` entry rows for AGM and AL; correct AD's exit from the cash arithmetic; nightly journal-vs-broker and cash-delta-vs-fills invariants in the health check. | S | E1, E5 | `val:F4`, `val:F5`, `val:I4` |
| IE2 | Write the entry tier, ruleset version and entry date into the position mirror from the buy row; report 3.0.0-entered positions separately from inherited ones. | S | E2, L7, F3 | `val:F6`, `val:F19` |
| IE3 | Snapshot plausibility gate: reject or flag when equity moves >15% with no fills or the position count drops with no journalled sell; re-fetch once; never trade on a flagged read. | S | E5 | `eng:F9`, `data:F15`, `val:F2` |
| IE4 | Use broker `filled_at` and fill prices for both journal fields; match the benchmark window to the fill side (open vs close); use price-only SPY or add a dividend shadow ledger consistently. | S | E6 | `val:F7`, `eng:F29` |
| IE5 | A `candidate_log` per Friday run: rank, tier, moat, conviction, valuation components, regime, action taken or reason skipped; persist the NONE/LOW exclusions and cancelled sells; price them afterwards. | S-M | E7 | `val:F9` |
| IE6 | Report the paper equity curve against SPY, an equal-weight benchmark (RSP or the universe), a sector-matched equal-weight basket of the same candidate pool, and a random-portfolio null (8 names from the same week's B-or-better list, 1,000 draws); print n, CI and minimum detectable effect next to every number. | S-M | E8, F14 | `val:F10`, `val:F26`, `scout:F46` |
| IE7 | A money-flow ledger: shadow dividends from yfinance, commission and FX-spread model, treat merger cash as cash; cash reconciliation of snapshot deltas vs journalled fills. | M | E9, H2 | `val:F3`, `eng:F36` |
| IE8 | Replace the regime engine with a constant target or one pre-registered crash rule (e.g. 100% after a 20% drawdown); if kept, use valuation percentiles of trailing history rather than fixed P/E levels, keep the momentum votes, respect confidence, and never classify on a single signal. | S | E3 | `sig:I5`, `eng:F27`, `scout:F10` |
| IE9 | Submit at the open (or run a second buy pass after Monday reconcile); treat exception-after-submit as `submitting` and look up by `client_order_id` before marking skipped; per-cycle idempotency key. | S / M | E4, G14 | `eng:F8`, `eng:F32` |
| IE10 | Fix `converged` (compare to fair value reached, not "went up") and score `reasoning_sound` for every judgeable close or drop the metric. | S | E10 | `val:F8` |
| IE11 | Make `daily_watchlist_check` notify or delete it. | S | E11 | `eng:F28` |
| IE12 | A hash-chained decision ledger (tamper-evidence) and an explicit, live kill switch for auto-trading that does not need a container recreate. | S | G13 | `scout:F33` |
| IE13 | Recover the pre-July account history from Alpaca's portfolio-history endpoint (read-only) to lengthen the curve, labelled as yet another rule regime. | S | E8 | `val:I2` |
| IE14 | Model the real-account execution: 5,000 DKK lots, no fractional shares, Nordnet commission and FX spread, in the paper engine or a shadow ledger, so the paper record tests the strategy the user would run. | M | H3 | `val:F3` |

### 4F. Validation and evidence

| ID | Idea | Effort | Addresses | Source |
|---|---|---|---|---|
| IF1 | Correct the record now: exclude the migrated rows (`trigger='bulk_load'`, reason "Migrated from legacy registry") from every cohort, re-map their moat from `moat.durability`, rerun cohort validation, and restate the 3.0.0 rationale in README, `deployment.py` and the research note. | S | F1, F2 | `val:F13`, `val:F14`, `sig:F31` |
| IF2 | The minimal validation programme: (1) a dated pre-registration doc with hypotheses, fixed horizons (90/180/365 d), benchmark set, test and α; (2) each month Sonnet-rate a random 20-40 universe names including Haiku fails, storing raw text and versions; (3) store counterfactuals (IE5); (4) measure at fixed horizons with beta- and sector-adjusted alpha and date-clustered errors; (5) report n, CI and minimum detectable effect in the briefing. After 12 months (~360 names) this can detect about a 5 pp/90d difference between "bot would buy" and "bot would not buy". Portfolio alpha of 3 pp/yr stays undetectable at any feasible horizon, and the plan should say so. | M (+20-40 Sonnet calls/month) | F3, F4, F11, F13 | `val:I1`, `val:F12` |
| IF3 | Stamp every decision, tier change, analysis, snapshot and trade with the ruleset version; backfill by date from a corrected changelog that includes the 6 rule changes and 2 model changes; treat model changes as version bumps; filter cohorts by the rating's version. | S | F3 | `val:F17`, `val:F18` |
| IF4 | PIT backtest fixes: unadjusted close × as-filed shares (or adjusted × split-adjusted), share-count sanity bounds vs the prior filing; treat missing debt as None and require debt coverage for the low-debt component; add the extra debt tags; take all concepts from one fiscal period and reject >15-month-old data; winsorise or report medians; hand-check any absolute return above 10×. | S | F5, F6, F8, F10 | `data:F6-F9`, `val:F20`, `val:F21` |
| IF5 | Store per-observation metric inputs in `pit_backtest.json`; commit the per-date report generator; add bootstrap CIs; test `score_stock` as well as the composite. | S | F9, F10 | `val:I3`, `sig:I2` |
| IF6 | A survivorship-free universe for backtests: reconstruct from EDGAR 10-K filer lists by year plus delisting prices, or buy one month of Sharadar/Norgate (price unverified) and export once; at minimum −100% for cancelled equity and last price for acquisitions. | L (reconstruct) / M (buy) | F7 | `data:I5`, `scout:F29` |
| IF7 | Rebuild live-equivalent metrics from `pit_fundamentals` (SBC and TTM from the 657,673 10-Q rows already loaded) so the backtest validates the live score. | M | F9 | `data:F11` |
| IF8 | Delete or relabel backtest modes 1 and 2. | S | F15 | `val:F24` |
| IF9 | Fama-French 5-factor + momentum regression and industry-portfolio controls from Ken French's library (free) on the weekly return series; a QuantStats tear sheet with Monte Carlo of 8-stock outcomes to show the luck range. | S | F14, F4 | `scout:F25`, `scout:F37`, `scout:F46` |
| IF10 | Compare the LLM moat label with objective moat proxies (ROIC persistence, margin stability) from `pit_fundamentals`; it is free and says whether the label carries information beyond the numbers. | S | F13, D-area | `ai:F18` |
| IF11 | State expected alpha as a range (published factor premia decay by about half post-publication) and expected tracking error in the briefing; do not tune weights on short samples. | S | F4 | `scout:F4`, `scout:F11` |
| IF12 | Pin model training cutoffs per row and only evaluate LLM labels on dates after the model's cutoff; any Sonnet 5.5 replay on Apr-Jun 2026 data is contaminated. | S | D8, F13 | `ai:F5`, `scout:F16` |

### 4G. Engineering and operations

| ID | Idea | Effort | Addresses | Source |
|---|---|---|---|---|
| IG1 | One `job_runs` table plus a decorator on all 13 jobs (start, finish, outcome, counts, real token usage and cost, error string); health asserts last-success age per job and fails on NULL `finished_at`; Discord digest shows a 7-day matrix. | M | G1, G2, D12 | `eng:F33`, `eng:F10` |
| IG2 | Wrap `run_pending` in a guard; guard the two unguarded job setups; SMTP and Anthropic timeouts; a heartbeat file plus compose HEALTHCHECK and an external ping; a startup catch-up policy per job from the job-state table. | S-M | G3 | `eng:F5-F7` |
| IG3 | Rotating file log in `data/logs/`, honour `LOG_LEVEL`, promote swallowed fetch failures to WARNING with counts; persist ERROR-level job outcomes. | S | G4, G1 | `eng:F12`, `eng:F4` |
| IG4 | Alert when an enabled-by-policy job has not produced its run row in N days regardless of the flag; log a startup diff of declared config fields vs `.env` keys present. | S | G1, G13 | `eng:F3`, `eng:F30` |
| IG5 | Change the Dockerfile CMD to the scheduler; mark `registry.py`, `portfolio.py`, `run_monthly_briefing.py`, `src/universe.py` deprecated; retire them once the DB path covers the briefing and the screen. | S / L | G5 | `eng:F15` |
| IG6 | Make the image push depend on the lint/test workflow (or branch protection); pin the compose image to a digest or sha tag; align the watchtower label. | S | G6 | `eng:F16` |
| IG7 | Extract the production sort key to a module function and test it; replace global `sys.modules` stubs with fixtures; add a handful of real, scrubbed model outputs as parser fixtures; property tests (hypothesis) for parsers and scalers; a migration test on a sanitised copy of the live DB. | S-M | G8 | `eng:F19`, `eng:F20` |
| IG8 | A read-only replay harness: copy the live DB, stub broker and LLM with recorded responses, run all 13 jobs in order, assert invariants (cash + positions = equity, every tier has a price alert, every held name has an analysis, no orphan run rows). | M | G8, E5 | `eng:F34` |
| IG9 | Split `database.py` by domain and move each job to `src/jobs/<name>.py` with a thin schedule file, after IG1 pins behaviour. | L | G-area | `eng:F35` |
| IG10 | Secrets: regenerate `.secrets.baseline` without hashed `.env` lines; rotate any weak secret that was on those lines; `starttls(context=ssl.create_default_context())`; narrow the local agent permissions. | S | G9 | `eng:F21` |
| IG11 | A `readonly=True` constructor for `Database`; resolve the path from an absolute env var and fail if the file is missing. | S | G10 | `eng:F23` |
| IG12 | Back up with `VACUUM INTO` or `sqlite3 .backup` to a snapshot file, integrity-check it, exclude `pit_fundamentals` from the nightly (monthly artefact instead), and run one restore drill. | S | G11 | `eng:F24` |
| IG13 | Use `PRAGMA user_version` with numbered migrations and a record of which ran. | S | G12 | `eng:F25` |
| IG14 | Parse config lazily with clear errors; one boolean convention; document every tunable in `.env.example`; a live kill switch read per cycle. | S | G13 | `eng:F30` |

### 4H. The Danish investor

| ID | Idea | Effort | Addresses | Source |
|---|---|---|---|---|
| IH1 | A DKK equity curve with FX attribution in every briefing: store the rate per snapshot, split each period's DKK return into asset and FX legs, show unhedged vs hedged-equivalent. | S | H1, H5 | `data:I4`, `scout:F44`, `sig:I4` |
| IH2 | Model the ASK correctly: 17% on annual mark-to-market (rotation is tax-free; FX gains taxed), US 15% withholding with relief capped at 17% and not re-depositable, loss carry-forward tied to the account staying open; wire `income_events` from the money-flow ledger (IE7). Verify the FX-in-ASK treatment with Skat before relying on it. | S-M | H2 | `scout:F41`, `scout:F42` |
| IH3 | Benchmark in DKK against a UCITS S&P 500 / MSCI USA ETF that is actually ASK-eligible (list unverified), after 17% tax, not against SPY in USD. | S | H1, F14 | `val:F26` |
| IH4 | Update the live `.env` ASK limit; compute room from the previous year-end value as Skat does; keep the ceiling in config, not code, so the 2027 bill can be applied when it becomes law. | S | H4, H6 | `scout:F39`, `scout:F40` |
| IH5 | Bring the real-account execution model into the paper test (IE14) and make the single-buy recommendation actually write rows. | M | H3 | `val:F3` |
| IH6 | Use Nationalbanken rates (IA24) for anything tax-related. | S | H2 | `scout:F31` |
| IH7 | A manual `real_holdings` table (ticker, shares, DKK cost, date) plus `cash_dkk` and `deposits_ytd`, edited after each Nordnet trade; compute the weekly recommendation from it, outside the paper loop, with whole shares, commission, FX spread and remaining ASK room. | S-M | I1, I2, I3 | red team I1 |
| IH8 | Publish one honest null in every briefing: the DKK value of each real lot had it gone into a chosen ASK-eligible UCITS index fund on the same day after the same costs (eligibility unverified). | S | I4 | red team I2 |
| IH9 | A cost model in the journal and briefing: per-lot commission, FX spread both ways, break-even alpha per holding period; verify Nordnet's actual schedule. | S | I4 | red team M4 |
| IH10 | Report Haiku pass rate and Sonnet moat by size and sector each run; test a size-neutral gate. | S | I5 | red team M5 |
| IH11 | Log a hash of the loaded config at startup, fail loudly on fallback to defaults, and bake config into the image or track the live copy. | S | I7 | red team M7 |
| IH12 | Check `watchlist.json` age before trading; remove the `/tmp` fallback. | S | I9 | red team M9 |
| IH13 | Use unadjusted closes for valuation inputs and adjusted closes for returns in the PIT backtest. | S | I10 | red team M10 |

---

## 5. Parked / wild ideas

Outside the current direction, but not discarded. Each is one line so you can pick it up later.

- **Chronologically consistent open models** (ChronoBERT/ChronoGPT) as a clean text-signal backtester for the news monitor; a tool for one experiment, not a replacement for Claude. `scout:F17`
- **One month of Sharadar or Norgate** to rebuild the quality backtest with delistings, once the price is confirmed; export and keep. `scout:F29`, `data:I5`
- **Liquidate to cash at a ruleset boundary in a paper sub-account** so the new ruleset's curve is clean; the alternative is tagging (IE2). `val:F19`
- **Industry-level momentum or valuation overlay** as a monthly cap on sector crowding. `scout:F23`
- **Hedged USD share classes or an outside-ASK hedge** if FX volatility ever dominates tracking error; the evidence leans unhedged. `scout:F43`
- **Re-test the Graham number and each fair-value leg** by leave-one-out on PIT data; parked until survivorship-clean data exists (also IC9). `scout:F8`, `scout:F9`
- **Revisit the ASK plan after the February 2027 bill** (500k ceiling, original-deposit base). `scout:F40`
- **A Qlib-style walk-forward stack**: the method lesson (rolling retrain, costs, information ratio) applies, the code does not; with ~1,300 names and 25 weekly cross-sections there is only enough data for a linear baseline. `scout:F35`
- **Opus or a second-sample "pre-mortem" before every buy** (ID10); cheap but its value is unmeasured. `ai:I6`
- **A forced-tool-call analysis schema** (ID2) extended into a full structured "analysis card" the briefing renders directly, removing the text formatter. `ai:I1`
- **Build the quality factor from EDGAR frames for the whole market in a few calls** and keep yfinance only for prices (IA7 taken to its end). `scout:F26`, `scout:F47`
- **A random-portfolio "monkey" benchmark published monthly** next to the bot's picks, as the honest null. `val:F26`
- **ai-hedge-fund-style hash-chained ledger and kill switch** (IE12). `scout:F33`
- **DeepSeek Flash for the light model**: measured, decided "keep Haiku"; parked unless the positive-control replay changes the picture. `ai:F21`
- **Treat the system as advice-only for the real ASK** and build the "one lot a week in DKK" recommendation path as the primary product, with the Alpaca engine as its shadow test (IE14, IH5). `scout:F31`, `val:F3`
- **Recover pre-July Alpaca history** (IE13). `val:I2`

---

## 6. Dependency notes: what blocks or enables what

These are prerequisites, not priorities. Several chains share a root.

1. **Persist the full scored set and fix the two-universe split (IA1, IA4) before anything that reads `quality_score` or the watchlist.** Industry-aware scoring (IB1/IB2), queue filters (IB3), the fair-price exception (IC13), the Haiku queue, and any paper-trade evidence all read numbers that are currently on two scales or come from a fallback list. Fixing the trader's candidate source (IA4) is also what makes a 3.0.0 buy possible at all.
2. **Store raw LLM text, model id, prompt and parser version (ID1, ID7) before any LLM validation, re-rating or consistency work.** The cohort programme (IF2), the breaker positive-control set (ID4), the Haiku recall test (ID6), the moat-vs-proxy test (IF10), and the DeepSeek benchmark all need it. Fix the parser (ID2) before mass re-analysis (ID5), or the backlog is re-labelled with the same errors.
3. **Correct the record (IF1, IF3) before the next monthly cohort run and before any public claim.** Everything in `cohort_validation`, the README rule rationale and `deployment.py:70` is downstream of the migration artefact.
4. **Industry grouping (IB1, step 1) is a prerequisite for** industry metrics (IB1), interim exclusions (IB2), valuation routing (IC2), sector-matched benchmarks (IE6) and sector-neutral PIT tests (IB4). It needs only yfinance `industry` plus a mapping table to start; EDGAR bank/insurer tags come later; REIT FFO/AFFO needs a non-XBRL source.
5. **A credible screen backtest needs, in order:** the PIT fixes (IF4), live-equivalent metrics from `pit_fundamentals` (IF7), stored PIT prices and the committed generator (IF5), then a survivorship-free universe (IF6, which itself needs CIK keying, IA5). Until IF6 exists, every absolute or SPY-relative number from mode 3 is biased; only rank statistics are usable, and even those are consistent with zero today.
6. **The whole-pipeline backtest is not feasible (F13).** Forward evidence (IF2) is the only path, and it needs: clean labels (ID2), stored raw text (ID1), counterfactuals (IE5), ruleset stamps (IF3), fixed-horizon and sector-adjusted measurement (IF2 step 4), and 20-40 extra Sonnet calls a month (ID5 cap change).
7. **The `job_runs` table (IG1) is the spine for** health checks that assert on jobs (IG4), catch-up after restart (IG2), real cost tracking (ID15), post-mortems (IG3) and the data-quality log (IA22).
8. **A trustworthy equity curve (IE3 plausibility gate, IE1 journal reconciliation, IE7 money flow) precedes** DKK reporting (IH1), benchmark comparison (IE6), tax modelling (IH2) and any statement about the paper record.
9. **A trustworthy fair value (IC1, IC2, IC3) precedes** redefining S (IC7), the margin-of-safety policy (IC5), the premium cliff (IC6) and deployment ranking changes; the audit trail (IC3) is what makes later ablation (IC9) possible.
10. **Dependency pinning and vendor contract tests (IA21) protect** every yfinance-shaped fix (IA9-IA12) from silently regressing on the next image build.
11. **A real-holdings table (IH7) and a cost model (IH9) precede** any output aimed at the real account: the single-buy recommendation, breaker advice for real holdings, the index-fund null (IH8) and the ASK tax model (IH2). Without them the system's only real-money output is computed from the wrong account.
12. **Deciding what "buyable" means (IC7, open question 3) precedes plumbing fixes to the buy funnel (IA4):** with the current definition the fix yields at most one buy (I6).
13. **Monitoring held names regardless of tier and versioning breakers (ID3) must precede** any reliance on the breaker-only sell rule as a risk control; the positive-control set (ID4) then says whether it can fire; the cap change (ID5) says whether it will be allowed to.

---

## 7. Open questions for you

1. **The two universes.** Is the buy source meant to be the DB universe plus tiers (what the Friday Sonnet docstring implies) or the yfinance score screen (what the code does)? The answer decides IA4's shape.
2. **The moat-NONE rule.** It rests on no valid evidence. Keep it a priori as a cheap conservative rule (the auditors think it is plausible) and restate the rationale, or drop it until forward data exists? Either way the README and `deployment.py` text should change.
3. **What should S mean?** Under the current definition it will never occur. Do you want a "wonderful at fair price" tier (WIDE + price ≤ FV) or should the system admit that its buy tier is "good and statistically cheap"?
4. **Financials, REITs and lessors.** Exclude them until industry models exist (fast, loses AGM/AIZ/CCI-type names), flag them "unscored" but keep tiering by LLM only, or build the industry models (weeks)? Two of seven holdings are financials and one is a REIT.
5. **Regime engine.** Keep as a modest tilt (the literature allows it, the implementation reduces to one flag), replace with a constant, or replace with one crash rule?
6. **The paper account's purpose.** Should it test the strategy you would run (one 5,000 DKK lot a week, no fractions, costs) or keep testing the USD notional engine? The two give different evidence.
7. **The 2026-10-24 cliff and the caps.** Spending ~$9 once and ~$4/month more would clear the backlog and un-starve the breaker check; the caps are in code, so it needs an image rebuild and a deploy. Do you want that before or after the parser fix?
8. **Legacy holdings under 3.0.0.** Tag and report separately, or liquidate the inherited book in paper so the new ruleset starts clean?
9. **The two live malfunctions (L1, L2).** Both are small code fixes plus, for L2, regenerating a cache file. Approval needed because they touch a scheduled job and live data.
10. **Benchmark.** SPY in USD, an ASK-eligible UCITS US ETF in DKK after tax, equal-weight, sector-matched, or all four?
11. **Secrets.** `.secrets.baseline` still carries hashes of real `.env` lines; do you want the baseline regenerated and the SMTP password rotated?
12. **The real account.** Do you want the system to know what you actually hold in Nordnet (a hand-edited table, IH7) so its one real-money output stops being computed from the paper account? If so, should breaker monitoring and sell advice cover those holdings?
13. **Friday's CTSH trim (L8).** The 15% cap will trim the paper book's only winner on 2026-10-23 and silently skip that week's real-account recommendation. Leave it (it is the rule as written), or is the cap not what you want for a concentrated long-term book?
14. **The index-fund null.** Should every briefing show "same lots in an ASK-eligible index fund, same dates, same costs" as the comparison, and which fund?
15. **The old chat's plan docs.** Anything in them that is not in this map (for example items never started) is invisible to this audit; if you can export them, the plan-status table can be completed.

---

## 8. Plan-item status (from code and commits; unverified against the plan documents)

| Item | Status | Where | Audit notes |
|---|---|---|---|
| Decision journal | implemented, unreliable | `decision_log`, `closed_trades`, `journal_report.py` | 16 rows, 2 phantoms, 2 untracked positions, wrong AD exit, no tier on 7 of 10 buys, 0 ruleset stamps (E1, E2, F3) |
| SEC EDGAR 10-K ingestion | implemented, weak | `edgar_fetcher.augment_filing_text` | wrong passage in ~40% of sampled sections, 8k cap, 14% of analyses grounded (A5) |
| Insider cluster-buying signal | partial | `insider.fetch_insider_signals`, 5% of composite | top-50 only, sales-dominated, unproven (A20, B11) |
| Owner-earnings DCF | implemented, flawed | `valuation._calculate_dcf_estimates` | double-counts growth, applied to financials (C2, C6) |
| DKK / FX bridge | partial | `fx.py`, `equity_dkk`, `recommend_single_buy` | no DKK curve or attribution, single-buy never written (H1, H3) |
| Phase 2.5 PIT fundamentals | research only, biased | `edgar_fundamentals`, backtest mode 3 | split look-ahead, zero-debt default, survivor universe (F5-F7) |
| Broker-agnostic accounts | implemented, one broker | `src/accounts/` | `paper_trader.py` still used directly (G5) |
| Snapshot / event logging | implemented, partly unused | `portfolio_snapshots`, `regime_log`, `income_events` | one corrupt day accepted, income never logged (E5, E9) |
| Regime engine | implemented, degenerate | `bubble_detector`, `deployment` | reduces to one flag, non-binding because the funnel is empty (E3, L2) |

---

## 9. Red-team verdicts

The red team (Opus) saw the six reports and the repository, not the system map or the lead's notes. It checked 100 findings (every critical and high, at least five others per report) with its own file:line or query for each verdict.

| Verdict | Count | Examples |
|---|---|---|
| Confirmed | 81 | top-100 truncation, corrupt universe cache, moat-NONE migration artefact, bold-label parser (reproduced), LOW-conviction clean cohort (reproduced: n=17, −3.9 pp, t −0.58), split look-ahead (reproduced: Spearman +0.018 → +0.004), ruleset stamps empty, CI push on failed lint |
| Overstated | 13 | `data:F2` (tier prices are live; only prompts are stale), `data:F12` (~12% bad sections, not ~40%), `ai:F13` (no starvation, 28 weeks of supply), `sig:F2` (the scarce resource is a code constant), `val:F13`/`sig:F31` severity (high for impact), `sig:F33` (too generous to the PIT result), `scout:F2` effort (GrossProfit is not among the loaded concepts), `scout:F13` timing (a fired breaker sells 3-10 days later, which strengthens the point), `scout:F39` (no active reader) |
| Wrong or partly wrong | 5 | `data:F24` and `scout:F29` (AL was a $65 cash takeover, not a loss), `eng:F14` (the 404 tickers are dead, not transient), `ai:F1` (NONE labels come from the migration default, not the parser), `scout:F7` (one analyst leg, not two) |
| Unverifiable | 1 | `sig:F5` treasury-share mechanism (needs the yfinance rows) |

All corrections are applied in sections 1-3 above. Its ten missing findings are section 3I; its two ideas are IH7 and IH8.

**Its attack on the improvement ideas**, which you should read before picking any of them:
- Clearing the backlog and raising the caps now (ID5) re-rates hundreds of names on one date with Sonnet 5.5, whose stated cutoff overlaps the April-June first-rating window, and conflicts with "freeze and pre-register" (IF2). Do it only after ID1/ID2 and as a pre-registered random sample.
- The forced tool call (ID2) will change the label distribution (the parser fix alone moved conviction from 84% MEDIUM to 39% LOW), so it must be a ruleset version bump.
- Opus on buys (ID10) is a solution looking for a problem: about one buyable name exists and it adds a fourth differently-fed gate.
- Caching the full 10-K (ID9) has a weak premise if the moat label is mostly priors about the company (I5); fix extraction (IA8) first.
- Sector-neutral composite (IB2) and new factor legs (IB7, IB8) add degrees of freedom to a score with no ranking power, tuned on a biased panel; fix the PIT harness (IF4-IF6) first and treat any change as a version bump. GrossProfit is not loaded, so IB7 is M effort, not S.
- Industry-specific models for banks, insurers and REITs (IB1) depend on data that is not free; Buffett's own "too hard" pile (IB2's exclusion) is the cheap, consistent option for an investor who wants a few holdings.
- Moving live fundamentals to EDGAR (IA7) is the right direction and the wrong priority next to IA1 and IA22.
- A QuantStats tear sheet (IF9) on 74 days invites misreading unless every number carries a CI and a minimum-detectable-effect line.
- Splitting the big files (IG9) and recovering pre-July history (IE13) are low value now.
- Supported outright: IF2, IA6, ID1, IG1, IH1, provided I1 (real holdings) and I4 (costs) are added so the evidence is about the user's real outcome.

**Contradictions it resolved:** AL (takeover), the 404s (dead), the moat-NONE mechanism (migration), the PIT reading (bug-inflated), tier pricing (live), the July cause (config drift), the "cliff" (throughput), the analyst legs (one), 10-K quality (~12%). **Duplicates:** the six numbered lists hold roughly 95 distinct issues, not ~190; section 3 is written on the merged set.

**Its verdict on the audit's honesty:** mostly honest that no edge is shown; too generous in `sig:F33` and `ai:F4`; silent on the size effect and the CTSH value-trap point until it raised them; and almost silent on the user's real decision, because the ASK account is not modelled, costs and whole shares are ignored, and nothing compares the system with a cheap index fund. The lead accepts all of that and has written it into section 1.

---

## 10. Decisions made during the audit

See `docs/research/audit-2026-10-19/DECISIONS.md` for the full log. In short: plan docs treated as unavailable; all output under `docs/research/`; no LLM calls; agent definitions written to `.claude/agents/` but run as general-purpose agents with the same models because the registry loads at session start; read-only DB and log access allowed, nothing else; the red team saw only the six reports and the repository; every report was graded on evidence and all six were accepted after the lead independently re-verified the headline claims (top-100 truncation, corrupt universe cache, moat-NONE migration artefact, parser bold-label loss, Haiku expiry cliff, regime constants, CI push on failed lint).
