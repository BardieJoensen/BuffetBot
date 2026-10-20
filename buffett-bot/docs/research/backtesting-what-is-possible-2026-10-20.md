# Backtesting BuffetBot: what is possible, what is not, and what "tweaking knobs" would mean (2026-10-20)

Short answer to "can we gather historical data and see how BuffetBot would have done, so we can tweak some knobs?" Long form with sources: `audit-2026-10-19/scout-backtesting.md` (31 items) and `BUFFETTBOT_AUDIT.md` section 3F.

## 1. Which parts can be replayed on history

| Stage | Replayable? | Why |
|---|---|---|
| Quant screen (`score_stock`, 16 metrics) and quality score | Yes, once the data is fixed | Inputs are 10-K facts and prices. `pit_fundamentals` already holds originally-filed facts for 1,292 names, 10-K coverage from fiscal 2009 (200 tickers), 658 by 2011, 836 by 2016. |
| Valuation arithmetic (P/E, Graham, DCF with fixed constants) | Yes, as a cheapness signal | Same inputs. The analyst-target leg is not stored historically and cannot be replayed; drop it from any backtest and say so. |
| Tier and deployment rules (weights, ceiling, buy/hold band) | Yes, mechanically | Pure arithmetic on the two above. |
| Regime engine | Describable, not testable | VIX, index prices and drawdowns are free historically, but 2011-2025 contains about five regime episodes. Nothing can be validated on five observations. |
| **The LLM stage (Haiku gate, Sonnet moat/conviction, breakers)** | **No. Forward-only.** | Sonnet 5.5's training runs to June 2026; it knows what happened to every company before that. Telling it to "pretend it is 2019" or masking names and dates does not remove the knowledge (Lopez-Lira et al. 2025); LLM backtests lose half or more of their return once the window passes the cutoff. The only clean LLM evidence is ratings made after the cutoff and measured forward, which is what the decision journal (since 2026-07-02) and the monthly cohort job do. One small exception: Haiku 4.5's data ends July 2025, so a replay of the Haiku gate on post-August-2025 dates is leak-free; it can test the gate's behaviour, not alpha. |

So the honest shape of "how BuffetBot would have done" is: the deterministic funnel can be backtested; the judgement layer the project is built around cannot, and the question "would the LLM have picked the winners" has no historical answer, only a forward one.

## 2. What the data needs before any number means anything

1. The three bugs the audit found in the existing point-in-time backtest (IF4): split look-ahead in FCF yield (adjusted prices × as-filed shares), missing debt tags scored as zero debt, concepts mixed across fiscal years. Without these the current +0.018 Spearman is bug-inflated (+0.004 without the affected rows).
2. A survivorship-free universe with delisting returns. Today's universe is today's survivors; the equal-weight "universe" beating SPY by 7 pp a year is the signature of that bias. Free sources do not carry delisted names; the realistic options are one year of Norgate (about $630, or $346 for six months) or a QuantConnect free-tier pass whose point-in-time status is disputed since its September 2026 migration.
3. A cost model: a per-trade commission and a spread term. Low-turnover value and profitability strategies survive costs; the rules as written generate rotation that may not.
4. Test the live score (`score_stock`), not the 5-metric composite the last backtest used.

## 3. What tweaking knobs on history does, and the discipline that limits the damage

- **Tuned backtests carry almost no information.** On ten years of data the best of 45 random configurations has an expected Sharpe around 0.7 and the best of 1,000 around 1.0, from noise alone. BuffetBot has roughly 100 constants (16 metric ideals, mins, maxes and weights; five quality weights; regime thresholds; tier weights; the 25% margin, the 12.5% ceiling, the 50% cliff). Fitting any of them to 2016-2025 returns produces a curve that will not repeat.
- **Two near-identical variants cannot be separated.** With a typical 3% tracking error between them, telling a 1 pp/yr difference apart takes about 36 years of data; 2 pp about 9 years. A 15-year backtest can only distinguish an information ratio of about 0.5 or more from zero. Expect most comparisons to be inconclusive, and decide in advance that inconclusive means "do nothing".
- **The 2016-2025 window is already spent.** The quant screen has been examined on it; Bailey et al.'s point is that once you have looked, a hold-out on the same data is not a hold-out. The one unspent segment is 2011-2015 (EDGAR XBRL begins 2009). Design on 2016-2025, run the frozen rules once on 2011-2015, and treat that single run as the test.
- **Defensible changes:** choices made on economic grounds before looking (published definitions of profitability, equal weights, a stated margin of safety); coarse structural switches tried once and recorded (financials excluded or not; sector-relative ranks or not); bug and look-ahead removals; changes that pass a neighbourhood test (a ±20% change in the knob moves the result smoothly rather than flipping it).
- **Overfitting:** fitting per-metric weights or thresholds to returns; picking between knob values that differ by 1-2 pp a year; adding a rule because it would have avoided a named loss; using the three-month paper account (t = 0.33) to tune anything.
- **The cheap discipline:** a dated freeze file (hypothesis, one headline statistic, every constant) committed before a run; a trial ledger of every configuration tried, needed to deflate the Sharpe ratio for the number of tries; a random-8 null with block-bootstrap bands next to every result; Fama-French 5 + momentum and QMJ regressions to see whether anything is left after known factors.

## 4. What "working" would look like

The best-documented live rule-based quality-value fund (Alpha Architect QVAL, about 52 stocks, 332% turnover) returned 9.1% a year over the ten years to September 2025 against 14.4% for a broad US index. Formula strategies (Magic Formula replications, Piotroski F-score) show a few points of gross alpha in the original samples, weaker after 2000, with large 2018-2020 drawdowns; the US F-score premium faded after 2000. Quality (QMJ) is strong in the original paper and in 44-country extensions, patchy in the latest decade. No public LLM stock-picking result shows durable alpha after costs; the famous ones are 8-week or 3-month windows. A concentrated 8-stock version of any of these will be dominated by luck for years (audit F4). "Working" for BuffetBot means a measurable, modest edge over a size-and-value-matched random portfolio after costs, sustained across the forward cohorts, not a backtest curve.

## 5. Order of effort (cost-ranked)

1. Hours: write the freeze and trial ledger; pre-declare the headline statistic and the decision rule including "inconclusive".
2. Hours: pull Ken French factor data and regress the paper curve and the cohorts on it, for context.
3. Cents: re-rate 20-30 names 3-5 times to measure the LLM's run-to-run noise (the DeepSeek benchmark planned to do this; it must happen before any outcome test of the LLM stage).
4. Days: fix the PIT backtest per IF4, switch it to `score_stock`, add bootstrap and random-portfolio bands and a cost model.
5. Days plus $350-630: survivorship-free prices with delisting returns, then the one 2011-2015 run.
6. Days: the Haiku post-cutoff replay (gate behaviour only).

Everything above the LLM line answers "does the funnel add anything". Nothing on this list answers "does Sonnet pick good businesses" faster than the forward cohort programme already in the audit (IF2).
