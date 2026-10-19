# Red team review of the 2026-10-19 audit

Reviewer: red-team (read-only). Repo main @ efc181b. Inputs: the six auditor reports in this folder (I did not read SYSTEM_MAP.md or DECISIONS.md). I made no network requests, no LLM calls, no edits, no git state changes and no Docker exec. Sources were the live DB opened `mode=ro` from `python3 -I`, files under `/opt/docker-apps/buffett-bot/data/`, the live `config/` and `.env` (key names only, plus three non-secret values: `ASK_CONTRIBUTION_LIMIT`, `MAX_POSITION_PCT`, `ALPACA_PAPER`/`AUTO_TRADE_ENABLED`), `docker logs buffett-bot-scheduler`, and the repo. My scratch scripts (`parse_repro.py`, `split_check.py`) are in the session scratchpad.

Verdict key: **CONFIRMED** (the evidence holds), **OVERSTATED** (real, but the severity or scope is inflated; the right severity is given), **WRONG** (the evidence does not show the claim), **UNVERIFIABLE** (what would be needed is stated). "Dup" marks a finding that another report also makes (see "Contradictions between reports").

Headline: the six reports are mostly accurate on code and data. Of the 100 findings I checked (every critical and high one, plus at least five others per report), 81 hold (many of them duplicates across reports), 13 are overstated, 5 are wrong or partly wrong, and 1 I could not verify. The main errors are: (1) AL was a $65.00 cash takeover, not a loss (data F24, scout F29); (2) the 404 tickers are dead, not "transient" (engineering F14); (3) AI F1 blames the moat-NONE labels on the parser, but they come from the migration default; (4) data F2 says the tiers are priced on stale data, but tier prices are fetched live. What all six missed is mostly on the user's side. The real Nordnet ASK account is not modelled at all. The DKK-lot recommendation is suppressed whenever the paper account has a pending sell. Nobody computed whether the edge survives costs. The LLM's "moat" label tracks company size almost one to one. And even with the watchlist bug fixed, at most one name can be bought.

---

## Data auditor

| id | sev (theirs) | verdict | reason (my evidence) |
|---|---|---|---|
| F1 | high | CONFIRMED, with one correction | `fundamentals` rows per date: about 1,070-1,093 through 06-29, then exactly 100 from 07-26 (query). `screener.py:1033-1036`. Correction: the 220 universe names with no row never had one on any date, including the ~1,080-row weeks, so they fail the hard filters. They are not a casualty of `top_n`. The names frozen by truncation number about 986 (952 at 06-29 plus 34 later dates). |
| F2 | high | OVERSTATED (medium) | The prompt staleness holds: 46 of the 50 Haiku screens on 2026-10-14 used the 06-29 row (query), and `scheduler.py:103-145` prints it as "Current Price". But the tier's price leg and the aggregator fair value are fetched live when the tier is set (`scheduler.py:1531-1538`, `aggregator.get_valuation(ticker)`, "assign_tier treats this price as authoritative"). So the stale data affects the Haiku gate and the LLM's labels, not tier pricing. |
| F3 | high | CONFIRMED (same root as F1) | 100 fresh scores, max 85.0; 1,039 stale, max 98.5; ≥80: 2 fresh vs 21 stale; 28 of the top 30 are stale (query). |
| F4 | high | CONFIRMED, severity medium | BK, CTRA, HOLX, EA, AVB, EQR and CPRX are all absent from `data/edgar/company_tickers.json` (10,434 tickers, dated 2026-10-02). Their caches were last written Apr-Aug (`cache/BK.json` May 25, `HOLX.json` Apr 13). This is 2.6% of the universe; the damage is a frozen April universe, not budget. |
| F5 | high | CONFIRMED, and worse than stated | All 1,057 cached Finviz tickers have a doubled first letter; stripped, 974 are in the universe, as-is only 16. All 25 watchlist names are in the 183-name curated list (my regex over `src/universe.py`). See missing finding M6: even with the funnel fixed, at most one name is buyable. |
| F6 | high | CONFIRMED, effect reproduced | `backtest.py:278` multiplies adjusted price by as-filed shares. Dropping the 1,145 observations whose as-filed diluted shares later jump more than 1.8x moves the pooled Spearman from +0.0176 to +0.0044 (`split_check.py`). The dividend half of `auto_adjust` is not discussed (M10). |
| F7 | medium | CONFIRMED | CHRD return 2020-06-01 = 190.97x. Bottom-quintile mean 2.025 → 1.104 without it (pit_backtest.json). |
| F8 | high | CONFIRMED (mechanism) | `backtest.py:266` `(long_term_debt or 0.0) + (short_term_debt or 0.0)`. Coverage: short_term_debt resolves for 965 of 1,292 tickers (concept query). I did not re-run the counts. |
| F9 | medium | CONFIRMED (code); dup validation F21 | Per-concept latest `period_end` in `get_pit_fundamentals_asof`. Counts not re-run. |
| F10 | high | CONFIRMED; dup validation F23, scout F29 | Structural; acknowledged in the note. |
| F11 | medium | CONFIRMED; dup signal F12 | `_derive_quality_metrics` has no SBC term, although `sbc` is in `pit_fundamentals` (1,151 tickers). |
| F12 | high | OVERSTATED (medium) | `edgar_fetcher.py:228` `starts[-1]` and the 8,000-character cap are confirmed. But in AIZ, CCI and AZO the MD&A slice starts at a prior-year cross-reference sentence inside the real MD&A, and real content follows ("Executive Summary ... net income increased $112.5 million"). Clearly wrong passages (a table of contents with 6 or more Item tokens, safe-harbor text, MD&A text stored as risk factors) are 14 of 118 sections (12%), not "roughly 40%". Only 42 of 293 analyses ever saw a 10-K. |
| F13 | medium | CONFIRMED; dup signal F3/F4 | Cache census: 5-column history with revenue_cagr 1, without 875; earnings_consistency values only 0-3. |
| F14 | medium | CONFIRMED, severity low | KO cache `dividend_yield` 2.26; `portfolio.py:466` multiplies it as a ratio; SPY_benchmark.json 0.98. Legacy path only. |
| F15 | medium | CONFIRMED; dup validation F2, engineering F9 | Row 79: equity 58,743.6, positions AGM/CPRT/DECK. |
| F16 | medium | CONFIRMED; dup AI F27, engineering F10 | No `news_events` rows 2026-08-11..09-30 (query). |
| F20 | low | CONFIRMED | `news_events.detected_at` max 18:03:15, while the log shows "Daily news monitor complete" at 20:03:15. |
| F22 | low | CONFIRMED; dup engineering F25 | `database.py:2416-2420`. |
| F23 | medium | CONFIRMED | NNI has no universe row; AGM and CCI latest 06-29; `entry_date`/`entry_price` NULL on all 7 positions. |
| F24 | low | WRONG on its key claim | AL is not a "total-loss outcome". `quarantined_positions`: 7,242.028 / 111.4158 shares = $65.0000 exactly, `asset_status inactive`, which is the signature of a completed cash acquisition (validation F3). Dropping AL probably makes C-tier look *worse* than it was, the opposite of the claim. The silent-drop mechanism itself is real. |
| F25 | low | CONFIRMED | AMAT `fair_value` 9.575 with `target_entry` 192.47; AZO 26.35 vs 1,698; 15 rows with target > fair value (query). |
| F19 | medium | CONFIRMED | IBKR watchlist: DCF 872.9 vs price 85.53, weighted mean 338.2. GOOG and GOOGL both analysed (C, fair value 360 each). |

## Signal auditor

| id | sev | verdict | reason |
|---|---|---|---|
| F1 | high | CONFIRMED; dup data F1/F3, engineering F2 | As above. |
| F2 | high | OVERSTATED (medium) | The queue SQL has no filters (`database.py:2547-2621`, `ORDER BY u.quality_score`). CRMD is "Biotechnology" and GPOR/DMLP are "Oil & Gas E&P" in the cache, and both industries are on the too-hard list (`screening_criteria.yaml:15-22`). But the scarce resource is a code constant, not money (AI F20), and the ordering harm is F1's. |
| F3 | high | CONFIRMED for the screener (medium); the DCF half is UNVERIFIABLE | Census as data F13. The `valuation.py` growth path was not re-run. |
| F4 | medium | CONFIRMED | No value of 4 in 1,240 cached rows. |
| F5 | medium | UNVERIFIABLE | Needs the yfinance "Share Issued" rows; their EDGAR comparison is suggestive. |
| F9 | low | CONFIRMED | `screener.py:312-317`; yaml `payout_ratio` min 0 / ideal 0.35 / max 0.80. |
| F10 | medium | CONFIRMED | `screener.py:470` `info.get("trailingPE") or info.get("forwardPE")`. |
| F12 | high | CONFIRMED | `run_point_in_time_backtest` calls `compute_quality_scores`; nothing tests `score_stock`. |
| F16 | high | CONFIRMED in substance | Not re-run, but consistent with the AIZ buy snapshot (`decision_log` id 8, fair value 417.1, vs the LLM's 260). |
| F18 | high | CONFIRMED | IBKR row above; `valuation.py:52-64` weighted mean with no robustness check. |
| F19 | high | CONFIRMED | IBKR stored estimate; AIZ bought at aggregator fair value 417 under the legacy rules. |
| F22 | medium | CONFIRMED | `quality_ceiling_pct` 12.5% (`config.py`), analysis valid for 180 days. Live example: IT is still A but 61% above target (`price_alerts`). |
| F23 | high (design) | CONFIRMED | WIDE/HIGH 1 of 293, HIGH 7, 0 HIGH in the 28 rows since 10-01 (query). |
| F24 | medium | CONFIRMED | `tier_engine.py:74,330`. |
| F26 | medium | CONFIRMED | 87 `tier_history` rows read "no price data". |
| F28 | medium | CONFIRMED | P/E 24.2-28.0 and VIX 14.2-20.4 on every logged day; `Counter.most_common` breaks a 2-2 tie by insertion order (`bubble_detector.py:230-246`). |
| F29 | medium | CONFIRMED; dup | — |
| F31 | critical | CONFIRMED facts, OVERSTATED severity (high) | `database.py:2502-2504` defaults to "none". `registry.json`: none of the 20 rows has a `moat_rating` key; durability is strong for 13 and moderate for 7. But NONE occurs in only 4 of 273 non-migrated analyses, so the rule rarely binds. This is a mislabelled justification for a rule that is sensible on priors, not a money-losing defect. |
| F32 | medium | CONFIRMED | A cohort: AIZ +0.5, AZZ −16.0, BIO +10.9, CTSH +56.0, GDDY +22.0, HCA +19.8, IT +47.2, PYPL +24.6 pp. BIO is a migrated row. |
| F33 | medium | OVERSTATED | "Supports a weak tilt away from blow-ups" ignores the split look-ahead. Without the later-split observations the Spearman is +0.004. |

## AI pipeline auditor

| id | sev | verdict | reason |
|---|---|---|---|
| F1 | high | PARTLY WRONG | The facts hold (265 of 293 rows pre-fix; no raw text; the schema). The mechanism for moat NONE is wrong: 20 of the 24 NONE rows, including 5 of its 7 "praising" examples (ATR, TXNM, CWST, CPK, ALSN), are migration defaults (`database.py:2503`), not `options[-1]` from the parser. |
| F2 | high | CONFIRMED, reproduced | Offline repro: `**Durability:** MODERATE` with "Strong brand" on the Type line parses as `STRONG`; without the asterisks it parses as `MODERATE` (`parse_repro.py`). Moat type is "unknown" in 123 of 193 analysis files. |
| F3 | medium | CONFIRMED | AMAT 9.575, AZO 26.35, BKNG 4,550 then 190 (query). |
| F4 | high (unproven) | CONFIRMED; framing too soft | It calls moat NONE "the only cohort with a clear signal ... see F1 for label noise". It is not noise: the cohort is a 100% artefact (20 migrated rows plus 2 with zero-length windows). |
| F6 | high | CONFIRMED | `thesis_breaker_events` 0 rows. CPRT 2026-10-08 Haiku summary: "directly matches the thesis-breaking risk". My stricter phrase match finds 5 such rows; their 24 used a broader rule. |
| F7 | high | CONFIRMED | `news_fetcher.py:399-402`: `held = [t for t in db.get_held_tickers() if t in set(watched)]`, where watched means S/A/B only. CCI, CTSH and NNI (C tier, 37% of equity) are never checked. |
| F10 | high | CONFIRMED | `BUDGET_CAPS_DEFAULTS` (`database.py:424-429`) is re-synced on every start (`:544-552`). |
| F11 | low | CONFIRMED | `news_fetcher.py:144-145` `kw in text`. |
| F12 | medium | CONFIRMED, and narrowed | Null verdicts: 10-01 72/72 ... 10-07 22/22, 10-08 1/21. No commit lands between 2026-10-01 08:51 and 10-10 (`git log`), and `.env` mtime is 10-01 08:37. So the outage ended without any repo or `.env` change, and the cause was external to both. |
| F13 | high | OVERSTATED (medium) | The counts are right: 299 → 140 (10-25) → 113 (11-15). But 140 eligible passes is 28 weeks of Friday supply at the hard-coded `limit=5` (`scheduler.py:1469`). The queue does not starve; throughput was already the constraint. |
| F19 | low | CONFIRMED | README:15 and :99 claim prompt caching. |
| F20 | low | CONFIRMED; dup engineering F13 | Cost constants. |
| F26 | low | CONFIRMED | `weekly_auto_trade`: `quick_screen` runs before `if tier not in ("S","A")`, with no `can_spend` (`scheduler.py:~788-808`). |

## Validation auditor

| id | sev | verdict | reason |
|---|---|---|---|
| F1 | high | CONFIRMED (the SPY leg is UNVERIFIABLE by me) | Equity 100,476 → 106,276 = +5.77%. CTSH unrealised P/L +8,014 on 10-16, larger than the whole +5,800. I made no network call for SPY. |
| F2 | medium | CONFIRMED; dup | Row 79. (Its "332,609 the day before" is buying power, not equity: a cosmetic slip.) |
| F3 | medium | CONFIRMED | Cash 30,880.39 in all 87 rows since 07-25, while holding dividend payers. AL is marked at exactly $65.0000. |
| F4 | high | CONFIRMED, severity medium | Cash 87,681.73 → 83,899.89 (−3,781.84) on 07-02 matches the AD buy plus ACT proceeds; ids 2-3 (ACVA, AESI) never filled. It affects legacy rows only; the book is reconstructible from snapshots. |
| F5 | medium | CONFIRMED | 2026-07-04 is a Saturday, so NYSE closed on Friday 07-03; `decision_log` id 5 uses price 36.24, the 07-02 close. |
| F6 | high | CONFIRMED | Tier NULL on 7 of 10 buys (query); NNI `tier_at_entry` 'C' is the default. |
| F8 | low | CONFIRMED | `database.py:1864-1868`, `:493-511`. |
| F10 | medium | CONFIRMED | `get_equity_curve` (`database.py:2123`) has no callers. |
| F11 | high | CONFIRMED | ((1.96+0.84)·10/3)² = 87.1 years; at 12% TE, 125. |
| F12 | high | CONFIRMED | NONE occurs in 4 of 273 non-migrated analyses (24 NONE − 20 migrated). |
| F13 | critical | CONFIRMED facts, OVERSTATED severity (high) | Same as signal F31. |
| F14 | high | CONFIRMED, reproduced | LOW conviction clean: n=17, −3.9 pp, t=−0.58; at 90 days n=16, +0.8 pp (my recomputation from `cohort_validation_2026-10-16.json`). |
| F17 | high | CONFIRMED | All 18 `cohort_validation` rows are stamped 3.0.0. |
| F18 | high | CONFIRMED | `ruleset_version` filled on 0 of 16 decisions and 0 of 293 tier changes. |
| F19 | high | CONFIRMED; dup signal F30 | — |
| F20 | high | CONFIRMED, reproduced | See data F6. |
| F22 | medium | CONFIRMED | `pit_backtest_report.md` exists only in `data/backtest/`, not in the repo. |
| F25 | high | CONFIRMED (reasoning) | Consistent with AI F5 and scout F16. |

## Engineering auditor

| id | sev | verdict | reason |
|---|---|---|---|
| F1 | high | CONFIRMED; dup | See data F5 and M6. |
| F2 | high | CONFIRMED; dup | The "unexplained" pre-July path is plausibly explained by config drift (M7). |
| F3 | high | CONFIRMED | `.env` vs `.env.bak-20261001` key-name diff adds exactly DAILY_NEWS_ANALYSIS_ENABLED, FRIDAY_SONNET_ENABLED and WEDNESDAY_HAIKU_ENABLED; ae3647b is dated 2026-08-11 07:27 UTC. |
| F4 | high | CONFIRMED (symptoms) | The fault ended with no code or `.env` change (see AI F12). |
| F5 | medium | CONFIRMED | The `while True: schedule.run_pending()` loop has no try. In `daily_snapshot`, `Database()` and `get_accounts()` run outside the try (`scheduler.py:372-374`). |
| F7 | medium | CONFIRMED | `schedule` keeps its state in memory only. |
| F8 | medium | CONFIRMED, and it misses a consequence | Any non-filled sell sets `pending_sell`, and `continue` then skips `plan_buys` **and** `_record_single_buy_recommendation` (`scheduler.py:994-1019`). See M2. |
| F9 | medium | CONFIRMED; dup | — |
| F10 | high | CONFIRMED | `run_log` has only bulk_load, wednesday_haiku and friday_sonnet rows; 9 rows have `completed_at` NULL. |
| F14 | medium | PARTLY WRONG | "Most are live tickers, so these are transient 404s" has no evidence behind it and the data contradicts it: absent from SEC's ticker map, caches months old (data F4). |
| F15 | medium | CONFIRMED | Dockerfile `CMD ["python", "-m", "scripts.run_monthly_briefing"]`. |
| F16 | medium | CONFIRMED | `docker-image.yml` has no `needs`; the repo compose has `watchtower.enable=true` (`docker-compose.yml:72`), the live copy has false. |
| F19 | medium | CONFIRMED | `tests/test_screener_sort.py:44-54` "Mirror of the production sort key". |
| F21 | low-med | CONFIRMED | `.secrets.baseline` has 9 hashed entries for `buffett-bot/.env`; `starttls()` without a context (`notifications.py:89,116`); `settings.local.json` allows `docker exec *` and `git push *`. |
| F30 | low-med | CONFIRMED | Live `ASK_CONTRIBUTION_LIMIT=135900`; the only reader is legacy `portfolio.py:37`. |

## Scout

| id | sev | verdict | reason |
|---|---|---|---|
| F1 | medium | CONFIRMED (a definitional gap, not code) | — |
| F2 | medium | OVERSTATED effort | GrossProfit is not among the 15 `pit_fundamentals` concepts (concept query: revenue yes; gross profit and COGS no). Effort is M, not S. |
| F7 | medium | WRONG on the system fact | The analyst targets are one merged leg (`valuation.py:172-184`, "One analyst-consensus estimate"), not two of about five. The IBKR and LULU watchlist rows each show one analyst estimate. |
| F10 | low-med | CONFIRMED | Deploy targets 65/80/90/97/100% (`config.py`). |
| F13 | medium | OVERSTATED on timing | A fired breaker sells only at the next Friday 16:15 NY run, and that order is after the close, so it fills the following Monday: a lag of 3-10 days. This strengthens "sells chase the move", but no breaker has ever fired. |
| F16 | high | CONFIRMED (as a constraint); dup | — |
| F22 | high | CONFIRMED (via signal F14's numbers) | Minor errors: AGM is a farm-credit lender, not an insurer, and AL was acquired, not a failed lessor. |
| F23 | low-med | CONFIRMED | No sector or industry cap anywhere in `deployment.py`; `MAX_SECTOR_PCT` has no reader; there is only a legacy warning at `portfolio.py:382`. |
| F24 | high | CONFIRMED | IBKR; AIZ. |
| F29 | medium | WRONG detail | "Already had a delisting loss (AL)" is wrong: AL was a $65 cash deal. The survivorship point stands. |
| F39 | low-med | OVERSTATED (low) | The stale `.env` value is real, but no active lot or room logic reads it (grep: legacy `portfolio.py` only). |
| F20 | medium | CONFIRMED, and stronger than stated (see M5) | I ran the query the scout proposed. |

---

## Missing findings

### M1. The real ASK account does not exist in the system; "the real account" advice is computed from the paper account's state
- **What:** Nordnet is a placeholder (Phase D). The single-buy recommendation for the real account uses the paper account's `held_symbols` and paper position count, and there is no representation of what the user actually owns, no sell or breaker advice for real holdings, and no available-cash input.
- **Where:** `src/accounts/__init__.py:22` ("Nordnet ASK account joins this list once NORDNET_ENABLED lands (Phase D)"); `scripts/scheduler.py:1019` `_record_single_buy_recommendation(db, candidates, held_symbols, current_position_count)` inside the paper `for account in accounts` loop; `src/deployment.py:336-400`; `src/news_fetcher.py:402` (held = paper positions).
- **Evidence:** `src/accounts/` contains only `alpaca.py` and `base.py`. `ASK_CASH_DKK` is not a key in the live `.env` (key-name grep), so `affordable` is always None. `single_buy_recommendations` has 0 rows. Its message "portfolio full (8/8) — would replace the weakest holding" counts paper slots.
- **Why it matters:** The user's goal is a few long-term holdings in a real ASK. The bot would recommend a name the user already owns in Nordnet, and skip one owned only on paper. It cannot warn about a breaker on a real holding unless the same name is held on paper and is S/A/B tier. The ASK ceiling (174,200 DKK, about $26k, about 35 lots of 5,000 DKK) means the real book grows lot by lot over years, a path the deploy-to-90% paper engine never tests.
- **Severity:** high
- **Effort:** M (a manual holdings table with cash and deposits, used for `held_symbols`, slots and news priority)
- **Unlocks / depends on:** M2, M3; any honest "real account" output.
- **Confidence:** high (code read, table empty, env key absent).

### M2. The DKK-lot recommendation is silently skipped whenever the paper account has a pending sell, and that is likely this Friday
- **What:** `weekly_auto_trade` sets `pending_sell` for any sell not filled at submit time, which is every sell, because orders go in after the close. It then hits `continue` before `_record_single_buy_recommendation`. The recommendation is also lost when the regime fetch fails (an early `return`), when `AUTO_TRADE_ENABLED` is false, or when no paper account is enabled.
- **Where:** `scripts/scheduler.py:926-935`, `:994-996` (`if pending_sell: ... continue`), `:1019`; `:711-712`, `:744-750`.
- **Evidence:** CTSH is C tier and holds 20.4% of equity (snapshot 10-16) against `MAX_POSITION_PCT=0.15` (live `.env`). `plan_sells` trims C-tier overweight names regardless of price (`deployment.py:292-318`), so on 2026-10-23 a sell of about $5.8k should be submitted, left pending, and the recommendation for the real account skipped. This is a prediction from code, not yet observed.
- **Why it matters:** The one output aimed at the user's real money depends on the paper account's order state and kill switch. It will also trim the book's only big winner on a rule (a 15% cap) that does not fit "a few long-term holdings".
- **Severity:** medium
- **Effort:** S (compute the recommendation before the paper sell/buy loop, independent of `accounts`)
- **Unlocks / depends on:** M1.
- **Confidence:** high on the code path; medium on the exact trim (depends on Friday prices).

### M3. Lot sizing ignores whole shares
- **What:** `recommend_single_buy` converts 5,000 DKK into dollars and never checks the share price. Nordnet has no fractional shares.
- **Where:** `src/deployment.py:352-354`.
- **Evidence:** At USD/DKK 6.6823 a lot is $748. Of the 113 priced `price_alerts` names, 7 cost more than one lot (NVR $5,920, GWW $1,331, MPWR $1,211, MCK $1,005, COST $989, NEU $876, FICO $771). Rounding down to whole shares leaves more than 20% of the lot uninvested for 30 of the 113, with a median of 90% invested (query).
- **Why it matters:** The bot can name a stock the user cannot buy with one lot, and the realised position is 10-20% smaller than the plan. The paper engine's notional fills (CTSH 328.572046 shares) hide this.
- **Severity:** medium
- **Effort:** S
- **Unlocks / depends on:** M1.
- **Confidence:** high.

### M4. No report checks whether any plausible edge survives the real account's costs and turnover
- **What:** The code's own cost assumption is a 25 DKK minimum commission plus about 0.25% FX per US trade, about 0.75% per 5,000-DKK buy and about 1.5% for a round trip. The rules still generate turnover: rotation, C-tier rotation, overweight trims down to `MIN_TRADE_USD` 250, and regime moves of 10-35 points of deployment. Nobody compared expected alpha with costs, or set the null the user actually faces: a broad index fund in the same ASK. Under the lager rule both alternatives pay 17% on gains, so tax does not favour either one.
- **Where:** `src/config.py:72-76` (cost comment); `src/deployment.py:292-318`; `config.py` `min_trade_usd`; `decision_log` ids 4-5 (AD held 1 day) and 9-10 (GIS held 1 week).
- **Evidence:** Validation F11 and scout F4 put realistic alpha at 2-3% a year before decay. A single rotation (1.5%) uses half of a year's hoped-for alpha. A $250 trim in the real account costs the 25 DKK minimum, about 1.5% of the trim in one direction. Nordnet's actual fee and FX schedule is unverified here; the figures are the code's.
- **Why it matters:** For a Danish retail investor the decision is whether to run this system or buy one fund. Without an after-cost break-even, a positive paper alpha is not a reason to follow it.
- **Severity:** high
- **Effort:** S (a cost model in the journal and briefing: per-lot commission, FX spread both ways, break-even alpha per holding period)
- **Unlocks / depends on:** M1; the validation programme (validation I1) needs a cost-adjusted benchmark.
- **Confidence:** medium (fee levels are unverified; the arithmetic is not).

### M5. The LLM "moat" judgement is mostly company size
- **What:** The Haiku gate's pass rate and Sonnet's WIDE rate rise steadily with market cap. Combined with the screener's large-cap tilt (signal F13), the funnel selects famous large companies.
- **Where:** `haiku_screens`, `deep_analyses` joined to `universe.market_cap` (live DB).
- **Evidence:** Latest Haiku result by market-cap quintile (n=170 each): pass rate 10%, 30%, 51%, 69%, 88% (median caps $1.1B to $138B). Latest Sonnet moat by cap quartile (n=46 each): WIDE 4%, 33%, 52%, 61% (median caps $3.4B to $202B). By sector, Haiku passes Energy 16% and Real Estate 27%, versus Utilities 71% and Technology 67%.
- **Why it matters:** Size and moat are genuinely correlated, but a near-monotone 10% to 88% ramp says the gate is largely a recognition filter. That is consistent with the scout's F20 literature on LLM bias toward large caps and with AI F14 (8 ratio lines plus memory). The output is large caps that an index fund already holds, in the segment where the published quality and insider effects are weakest (scout F6). It also means the moat cohort tests are partly size tests.
- **Severity:** high (for the edge claim)
- **Effort:** S (report pass rate and moat by size and sector each run; test a size-neutral gate)
- **Unlocks / depends on:** AI I2 (stored raw text) to see what drives the label.
- **Confidence:** medium (confounded by the real size-moat link; `universe.market_cap` is stale for about 950 names).

### M6. Fixing the watchlist funnel alone yields at most one buy: A-tier supply is effectively zero
- **What:** Data F5, signal F29 and engineering F1 present the watchlist/tier mismatch as the reason nothing is bought. Even with candidates fed from DB tiers, the buyable set is about one name, and it expires next week.
- **Where:** `price_alerts`, `deep_analyses`; `deployment.py:plan_buys` (ceiling −12.5%); `get_valid_deep_analysis` (`database.py:1014`, `expires_at > now`).
- **Evidence:** The four "A" names: BIO's only analysis is a migrated legacy row (moat NONE, expired 2026-04-08), so it is not valid and would be excluded anyway. IT is 61% above target, a margin of safety of about −21%, beyond the ceiling. AIZ is already held. AZZ (gap −16%) is valid until 2026-10-28. Under 3.0.0, A came from 7 of 64 price-checked scheduled assignments, with Friday throughput at 5 analyses a week (`scheduler.py:1469`).
- **Why it matters:** The 3.0.0 paper "experiment" cannot start on the buy side under any fix the reports propose. Supply of A names (signal F23's design point) is the binding constraint, not plumbing. The three C-tier holdings (37% of equity) also have no exit: they are not news-monitored (AI F7), and rotation needs an S/A candidate that does not exist.
- **Severity:** high
- **Effort:** M (decide what "buyable" should mean before plumbing it)
- **Unlocks / depends on:** signal F18/F19/F23 (a fair value worth trusting).
- **Confidence:** high.

### M7. Live config drift explains the July discontinuity, and the deployment has no config provenance
- **What:** The live `config/screening_criteria.yaml` was rewritten on 2026-07-02 17:28, in the same minute as `.env.example` (17:28) and `.env.bak` (17:29). With an empty or old file, `criteria.scoring` is empty. That skips the sort, the `top_n` truncation and every score (`score_stock` returns 0,0), and the too-hard filter has no list. This would explain three observations no report connected: about 1,080 rows a week until 06-29, too-hard industries present in June and absent after, and a NaN crash in the sort key (which runs only `if criteria.scoring`) that began on the first Monday after the rewrite, 07-06.
- **Where:** `/opt/docker-apps/buffett-bot/config/` (mounted read-only, copied by hand, outside the image); `src/screener.py:131-190` (silent fallback to defaults), `:219-220`, `:1010`.
- **Evidence:** File mtimes as listed. The repo yaml is identical to the live file (`diff`); its last git change was 2026-03-17. `ScreeningCriteria` defaults to `scoring={}` and `too_hard_industries=[]`.
- **Why it matters:** A config copy outside git changed the screen's behaviour with no review or log. The same class produced the August blackout (engineering F3) and the stale ASK limit. The loader also falls back to defaults silently on a missing or broken file.
- **Severity:** medium
- **Effort:** S (bake config into the image, or log a hash of the loaded config at startup; fail loudly on fallback)
- **Unlocks / depends on:** engineering F30.
- **Confidence:** medium (consistent with all three facts; the pre-07-02 file content is unknowable).

### M8. The paper record's only winner was a cheapness pick that the LLM itself called a possible value trap
- **What:** CTSH supplies more than all of the gain. Its stored buy thesis describes a cheap, strategically weak business, not a wonderful one.
- **Where:** `decision_log` id 6 `reasoning_snapshot`; `deep_analyses` CTSH; `tier_history` CTSH.
- **Evidence:** Thesis: "statistically cheap (11x P/E, 9.8% FCF yield ...) but strategically challenged (weak moat, AI disruption risk, commoditizing industry) ... a potential va[lue trap]". It was tier A on 06-19 because of price ("$44 ≤ $51"), was rated C/NONE/LOW on 10-13 and C/NARROW/LOW on 10-16, and 3.0.0 now excludes LOW-conviction names from new buys.
- **Why it matters:** None of the reports lets the Buffett framing pass unchallenged in general (signal F23, AI F18). But none points out that the one data point people will read as success argues for a statistical value screen, and against the LLM quality stage the project is built around.
- **Severity:** medium (honesty of interpretation)
- **Effort:** S (say so in the briefing)
- **Unlocks / depends on:** nothing.
- **Confidence:** high on the record; n=1, so it proves nothing either way.

### M9. The auto-trade reads a watchlist of any age, with a /tmp fallback
- **What:** `weekly_auto_trade` never checks `watchlist.json` `updated_at` and falls back to `tempfile.gettempdir()/buffett-bot-data/watchlist.json`.
- **Where:** `scripts/scheduler.py:~753-762`.
- **Evidence:** Code read. The weekly screen that writes it runs at 17:00 the same Friday; if it fails, last week's or an older file is used without a warning.
- **Why it matters:** Prices are re-fetched, so the risk is a stale candidate set, not a stale price. This matters once the funnel works.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** engineering F10.
- **Confidence:** high.

### M10. The PIT backtest's FCF yield also carries dividend look-ahead, not only split look-ahead
- **What:** `history(period="max")` with yfinance's default `auto_adjust=True` lowers past prices by cumulative later dividends as well as splits. Market cap at the rebalance date is therefore understated for dividend payers too, by more the earlier the date and the higher the yield.
- **Where:** `src/backtest.py:285-300`, `:278`.
- **Evidence:** Code read. Data F6 and validation F20 analyse only the split part. For a 3% yielder, the 2016 price is adjusted down by roughly a quarter by 2026, so its FCF yield is overstated by about a third. Not measured: no price pull was allowed.
- **Why it matters:** The bias is concentrated in utilities, staples and REITs, the sectors the sector-neutrality debate (signal F14, scout F22) is about.
- **Severity:** low-medium
- **Effort:** S (use unadjusted closes for valuation inputs and adjusted closes for returns)
- **Unlocks / depends on:** data F6 fix.
- **Confidence:** high on mechanism; size unmeasured.

---

## Attack on the improvement ideas

| idea | verdict | why |
|---|---|---|
| AI I3: clear the backlog now, raise the caps to about 60 Sonnet and 150 Haiku a week | **Conflicts and contaminates** | (1) It re-rates hundreds of names on one date with Sonnet 5.5, whose stated cutoff (June 2026, AI F5) overlaps the April-June first-rating window. That creates the date-clustered, model-contaminated cohort validation F15 and F25 warn about. (2) It conflicts with validation I1's "freeze, pre-register, do not change rules mid-sample". (3) More analyses do not create buys (M6). Do it only after AI I1/I2 and only as a pre-registered random sample (validation I1). |
| AI I1: forced tool-call schema | **Right fix, with a cost** | It changes the label distribution (the conviction mix already moved from 84% MEDIUM to 39% LOW with the parser fix), so it must count as a ruleset/version bump (validation F18), or cohorts silently mix regimes. |
| AI I6: Opus second opinion on buys | **A solution looking for a problem** | About one buyable name exists (M6), the value is unmeasured, and it adds a fourth differently-fed gate (AI F26 already found a third). |
| AI I5: cache the full 10-K and ask several questions | **Weak premise** | M5 suggests the moat label is dominated by priors about the company. More filing text is plausible but untested, and with 14 of 118 stored sections plainly wrong (data F12), fix extraction first. |
| Signal F14 / I2: sector-neutral composite | **Cheap, no shown benefit, changes the funnel mid-sample** | Their own PIT test gives +0.001 (neutral) vs +0.014 (raw), both zero. Do it only as part of a version bump. |
| Signal F16: industry-specific models for banks, insurers and REITs (L) | **Depends on data that is not free** | Scout F24: FFO/AFFO, combined ratios and CET1 are not reliably in us-gaap XBRL. For an investor who wants a few holdings, Buffett's own "too hard" pile (exclude these industries from automated tiering) is the cheap, consistent option. |
| Scout F2 / F5 / F12: add gross profitability, net issuance, momentum | **Adds degrees of freedom to a score with no ranking power** | GrossProfit is not even loaded (M-effort). Each addition would be tuned on a survivorship-biased PIT panel (data F10) with split and dividend look-ahead (data F6, M10). Scout F4's own decay evidence says to expect half the in-sample figure. Fix the PIT harness first. |
| Data I3 / scout F47: move live fundamentals to EDGAR (L) | **Right direction, wrong priority** | It gives parity with the backtest, but persisting all names (data F1) and per-run quality logs (data I6) remove most of the live damage at S effort. |
| Scout F37: weekly QuantStats tear sheet | **Risky without intervals** | Precise-looking Sharpe and alpha on 74 days invites exactly the misreading validation F1 warns about. Only with confidence intervals and a minimum-detectable-effect line (scout F46). |
| Engineering F35: split database.py and scheduler.py (L) | **Low value now** | Refactoring with no behaviour gain; do it after F10's job table pins behaviour, as the auditor says. |
| Validation I2: recover the pre-July account history | **Low value** | It adds another rule regime of anecdote (validation F18). |
| Validation I1, data I1, AI I2, engineering F33, data I4/scout F44 | **Support** | These create evidence or show the user's real outcome. They need M1 (real holdings) and M4 (costs) to be complete. |

Conflicts among the ideas: AI I3 versus validation I1 (scale up now versus freeze); signal's re-weighting and the scout's new factors versus validation I1; data F1's fix (persist all names) re-bases `quality_score` yet again. That is necessary, but it should be logged as a version change, because the queue ordering and the tier exception at ≥80 move with it.

---

## Contradictions between reports

1. **AL.** Data F24 calls it a "total-loss outcome" and scout F29 a "delisting loss"; validation F3 calls it a $65.00 cash takeover. The evidence (`quarantined_positions` 7,242.028 / 111.4158 = 65.0000, `inactive`) supports validation. The bot also books it at zero equity value (`untradable_value`), understating equity by 6.4%.
2. **The 404 tickers.** Engineering F14 and its Scenario C call BK, CTRA, SATS and IAC "live tickers; transient". Data F4 calls them persistent. SEC's 2026-10-02 ticker map lacks BK, CTRA, HOLX, EA, AVB, EQR and CPRX; data is right.
3. **Moat NONE mechanism.** AI F1 blames the parser's pessimistic default; signal F31 and validation F13 blame the migration's missing key. The code (`database.py:2503`) and `registry.json` support the migration explanation for 20 of 24 rows.
4. **The same bug at three severities.** Moat NONE is "critical" in signal F31 and validation F13 and "high" in AI F1/F4; I rate it high (reasoning in the signal F31 row).
5. **PIT reading.** Signal F33 says the backtest "supports a weak tilt" (p=0.055). Data F6 and validation F20/F22 say it is bug-inflated and not robust. The recomputation sides with data and validation (+0.004 without later-split observations).
6. **Tier pricing.** Data F2 says Sonnet tiers are priced on stale data; signal F26 and the code (`scheduler.py:1531-1538`) show a live aggregator price since ae3647b.
7. **The July cause.** Signal hypothesises the yaml took effect on 07-02; engineering calls it unexplained; data dates it from 07-26 without a cause. M7 reconciles them.
8. **"Cliff" versus throughput.** AI F13 predicts starvation; its own numbers give 28 weeks of supply at 5 a week.
9. **Analyst legs.** Scout F7 says two of about five fair-value inputs are analyst targets; the code merges them into one (signal F18 shows one).
10. **10-K excerpt quality.** Data F12 puts boilerplate at about 40% of sampled sections; my content check finds about 12% clearly wrong.

Duplicates (one defect, several names): top-100 truncation (data F1/F3/F21, signal F1/F27, engineering F2); the empty buy funnel (data F5, signal F29, engineering F1, plus the Haiku-before-tier waste in AI F26 and engineering F32); the 09-16 snapshot (data F15, validation F2, engineering F9 and Scenario B); the August blackout (engineering F3, AI F12, data F16); the October outage (engineering F4, AI F12); split look-ahead (data F6, validation F20); fiscal-year mixing (data F9, validation F21); survivorship (data F10, validation F23, scout F29); revenue_cagr NaN (data F13, signal F3/F4); DCF on financials (data F19, signal F19, scout F24); a universe that is never refreshed (data F4, engineering F14); fetch errors read as "no news" (data F17, engineering F31); cost constants (AI F20, engineering F13); prompt fencing (AI F25, engineering F22); DKK/FX attribution (data I4, signal I4, validation F26, scout F44, engineering F36); fill-time journaling (validation F7, engineering F29); retention (data F22, engineering F25); unpinned dependencies (data F18, engineering F17); insider tilt (data F28, signal F34, scout F6); health-check blind spots (data F16, AI F27, engineering F10); unproven LLM labels (AI F18, validation F25, scout F16); legacy book under 3.0.0 (signal F30, validation F19). Counting these once, the reports contain roughly 95 distinct issues, not the roughly 190 the six numbered lists suggest.

---

## Improvement ideas not tied to a weakness

### I1. Compute the real-account recommendation from a manual holdings table, with a cost and whole-share model
- **What:** A small `real_holdings` table (ticker, shares, cost in DKK, date) plus `cash_dkk` and `deposits_ytd`, edited by hand after each Nordnet trade. The weekly recommendation then uses those holdings, whole shares, the commission and FX spread, and the remaining ASK room.
- **Where:** `src/deployment.py:recommend_single_buy`, `src/database.py`, `scripts/scheduler.py` (called outside the paper loop).
- **Evidence:** M1-M4.
- **Why it matters:** It is the only output that touches the user's money, and today it is computed from the wrong account.
- **Severity:** n/a
- **Effort:** S-M
- **Unlocks / depends on:** M1, M2, M3.
- **Confidence:** high.

### I2. Publish one honest null in every briefing: "same money in a broad index fund in this ASK"
- **What:** Track the hypothetical DKK value of each real lot had it gone into a chosen UCITS index fund on the same day, after the same costs. Which funds are eligible for an ASK is unverified.
- **Where:** `src/briefing/`.
- **Evidence:** M4; validation F26.
- **Why it matters:** It answers the user's actual question, and it accumulates evidence from day one at the user's own scale.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** I1.
- **Confidence:** medium.

---

## What I could not verify

- SPY total return over the paper window (no network call); validation's +2.60% is taken on trust.
- Nordnet's actual commission and FX-spread schedule, ASK fund eligibility and how the annual ASK tax is paid when the account holds no cash. I used only the code's own figures (`config.py:72-76`).
- What the live `screening_criteria.yaml` contained before 2026-07-02 (M7 is an inference from mtimes and code paths).
- The cause of the 2026-10-01..07 LLM failure. I could only rule out a repo or `.env` change.
- Whether the CTSH trim happens on 2026-10-23 (M2 is a code-path prediction).
- Signal F5 (treasury shares), signal F13/F14 (not re-run), data F8/F9 counts (mechanism only), AI F15/F24 (not checked), and the per-name cause of each 404 (only that SEC and Yahoo both fail to resolve them).
- The effect of M10 (dividend adjustment) on the PIT result; that needs a price pull.

---

## Overall: is the audit honest about edge?

Mostly yes. Validation, AI pipeline and data state plainly that there is no demonstrated edge. The paper record is t=0.33 and is carried by one name. The quality score has no ranking power once the split bug is removed (+0.004). The only "significant" LLM cohort is a migration artefact. Detecting 3 pp a year of alpha would take decades. Where the audit is still too generous: signal F33 reads the PIT result as a weak tilt, AI F4 calls a 100% artefact "label noise", and no report says what M5 and M8 show. The LLM's moat label largely tracks size, and the one winning trade was a statistical value pick that the LLM itself doubted. The audit also says almost nothing about the user's real decision. The ASK account is not modelled, costs and whole shares are ignored, and nobody compares the system with simply buying a cheap index fund, so whether the system adds anything after Nordnet costs is not just unproven but untested.
