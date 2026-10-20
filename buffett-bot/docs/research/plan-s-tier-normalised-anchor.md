# Plan: a normalised price anchor for the S tier (ruleset 3.2.0 candidate)

Status: proposed 2026-10-20, not started. Follows the 3.1.0 change (S = WIDE moat with MEDIUM or HIGH conviction at or below target).

## Problem

S is the crash tier: a wonderful business at a wonderful price. Its price leg is `price ≤ 0.75 × average_fair_value`, and every leg of that average falls in a crash: analyst targets are cut, `trailingEps` and `forwardEps` drop (P/E leg and Graham number), and the DCF growth rate is a 3-4 year endpoint CAGR that collapses in a recession (`src/valuation.py:_calculate_dcf_estimates`). The anchor moves with the price, so "25% below fair value" keeps receding exactly when S is meant to fire. Buffett's wonderful price is relative to normal earning power, not to recession-year numbers. Audit items C1, C3, C6 in `BUFFETTBOT_AUDIT.md`.

## Design

One new valuation leg and one asymmetric rule, applied to the S decision only.

1. **Normalised earnings.** `normalised_eps = mean(NetIncomeLoss over the last 5 fiscal years, 10-K only) / sharesOutstanding_now`. Net income per current share avoids the split-adjustment problem of as-filed per-share figures. Require at least 4 fiscal years and a positive mean; otherwise no estimate.
2. **Normalised fair value.** `normalised_fair_value = 18 × normalised_eps` (the same fair multiple as the existing P/E leg, so the two legs differ only in the earnings base).
3. **S price rule.** For `quality == "wonderful"` only, the gap is measured against `max(target, 0.75 × normalised_fair_value)`. In an expensive market the normalised anchor is below the aggregated one and `max` leaves the target unchanged; in a crash the aggregated target falls below the normalised one and the normalised anchor holds it up. A and B are unaffected.
4. **Guard.** Cap the normalised anchor at 1.5 × the aggregated fair value, so a business whose earnings fell for good cannot keep a stale anchor forever. This is a decision for the user: the cap trades crash sensitivity for permanent-impairment protection.
5. **Audit trail.** `TierAssignment.fair_value_source` becomes `"normalised"` when the normalised anchor binds, and the briefing shows both anchors for S and B names with a wide moat.

## Data prerequisite

`pit_fundamentals` already holds `net_income` (NetIncomeLoss) and `shares_diluted` for 1,292 universe tickers, but it is loaded only by hand (`python -m src.backtest --load-pit`, `src/edgar_fundamentals.py:load_universe_fundamentals`). The anchor needs a refresh job: add a monthly step to `scripts/scheduler.py:monday_maintenance` (first Monday of the month) that calls `load_universe_fundamentals(tickers, db)` for universe tickers whose newest 10-K row is older than 400 days. Free, 10 requests/second at SEC, about 1,300 calls.

## Changes, by file

| File | Function | Change |
|---|---|---|
| `src/valuation.py` | `ValuationAggregator._calculate_normalised_pe_value(symbol, info)` (new) | Read the 10-K `net_income` series via `Database.get_pit_concept_series(ticker, "net_income")`, filter `form == "10-K"`, take the last 5 fiscal years, divide the mean by `info["sharesOutstanding"]`, multiply by 18. Return a `ValuationEstimate` with `source="P/E on 5-yr average earnings"`, `confidence="medium"`. |
| `src/valuation.py` | `ValuationAggregator.get_valuation` | Append the new estimate. Set `AggregatedValuation.normalised_fair_value` (new field) to it, separately from the mean. |
| `src/valuation.py` | `AggregatedValuation` | New field `normalised_fair_value: Optional[float]`; include in `to_dict`. |
| `src/tier_engine.py` | `assign_tier` | After `target` is resolved: if `quality == "wonderful"` and `external_valuation.normalised_fair_value` is set, `s_target = max(target, 0.75 × min(normalised_fair_value, 1.5 × avg_fv))`; compute the S gap against `s_target`, set `fair_value_source = "normalised"` when it binds. |
| `src/tier_engine.py` | `TierAssignment` | Field `normalised_target_entry: Optional[float]` for the briefing. |
| `src/database.py` | `save_deep_analysis`, `deep_analyses` | Column `normalised_fair_value REAL` via `_COLUMN_MIGRATIONS`; `friday_sonnet_batch` writes it. |
| `scripts/scheduler.py` | `monday_maintenance` (new step 2b, first Monday of the month) | Monthly EDGAR companyfacts refresh for stale universe tickers (see prerequisite). |
| `src/briefing/text_formatter.py`, `html_formatter.py` | S and wide-moat B entries | Show "target (aggregated)" and "target (normalised)". |
| `src/ruleset.py` | `RULESET_VERSION`, `RULESET_CHANGELOG` | 3.2.0, dated on deploy. |
| `tests/test_valuation.py` | new | Synthetic 10-K series: 5 positive years → estimate; 3 years → None; negative mean → None; shares from `info`. |
| `tests/test_tier_engine.py` | new | Crash case: aggregated fair value 100 → 60, normalised 100, wide moat, price 70: S (gap vs 75). Bubble case: aggregated 100, normalised 60, price 90: B unchanged. Cap case: normalised 300, aggregated 100: anchor capped at 150. |
| `src/backtest.py` | `run_point_in_time_backtest` (optional) | A side table: how often `price ≤ 0.75 × 18 × normalised EPS` held per rebalance date, to size how often S would have fired 2016-2025 (survivorship caveats apply). |

## Effort and dependencies

M: one to two days including tests. Depends on the monthly PIT refresh (same change set). Does not depend on the industry-aware scoring work; note that for financials the normalised P/E is as questionable as the trailing one, so the leg should be skipped for the sectors the DCF already skips once that exclusion exists (audit IC2).

## Decisions needed from the user

1. The 1.5× cap on the normalised anchor (step 4), or no cap.
2. Whether the normalised leg also joins the aggregated mean for every tier, or stays S-only as proposed (S-only keeps A/B behaviour identical to 3.1.0 and makes the change easy to evaluate).
