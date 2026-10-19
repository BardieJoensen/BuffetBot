# Point-in-time backtest of the quality screen (2026-10-17)

First run of `run_point_in_time_backtest` (backtest.py mode 3). The screener's
quality score was recomputed on SEC EDGAR *originally-filed* XBRL facts as
known on each June 1 from 2016 to 2025, for the current 1,359-name universe
(1,292 with XBRL facts; the rest are mostly foreign filers). Forward return is
the 12-month price return from each date, equal-weight, no costs. 10,559
ticker-year observations. Raw output: the bot's `data/backtest/pit_backtest.json`
and `pit_backtest_report.md`.

## Result

The score has **no ranking power on mean returns** (pooled Spearman +0.018) and
a **small, fairly consistent edge on median returns**.

| quintile (as-known score) | mean 12m | median 12m | share that doubled |
|---|---:|---:|---:|
| Q5 high (avg score 78) | +17.3% | +12.4% | 3.5% |
| Q4 | +15.6% | +9.9% | 4.0% |
| Q3 | +18.2% | +10.6% | 4.4% |
| Q2 | +18.2% | +9.3% | 5.0% |
| Q1 low (avg score 20) | +49.8% | +9.0% | 13.6% |

The mean of the bottom quintile is lottery tickets: 13.6% of its names doubled
within a year, concentrated in the June 2020 (+202% mean) and June 2025
(+144% mean) junk rallies. Universe-level survivorship bias inflates exactly
that quintile: the universe is today's names, so a company that was distressed
in 2020 and is still listed is, by construction, one that survived.

Median Q5 beat median Q1 in 8 of 10 years (all but 2020 and 2025), by about
3 pp a year on average. Top-quintile mean beat SPY in 5 of 10 years and
matched it overall (+17.7% vs +16.2%), while the equal-weight universe did
better than both (+23.5%), again driven by the small-cap tail.

## What this means for the bot

- The quant screen is a reasonable *steady-business* filter (fewer blow-ups,
  slightly better typical outcome) but not an alpha source. It should keep its
  role as the funnel in front of the LLM stage, not become a ranking signal.
- Together with the 2026-10-17 cohort test of the LLM ratings (moat NONE
  n=22: −10.5 pp vs SPY, 2 of 22 ahead; LOW conviction n=27: −7.5 pp), the
  evidence says the system's measurable value is in what it *excludes*, not in
  what it picks. Ruleset 3.0.0 already hard-excludes those labels.
- A "buy the top quintile" strategy would have roughly matched SPY before
  costs; with Nordnet's minimum commissions at DKK 5,000 lots it would have
  trailed it.

## Limitations

- Universe survivorship: today's names scored on past numbers. A delisted-filer
  reconstruction is the next step if this is ever used to justify a strategy.
- Ten annual draws; counts of years, not p-values.
- No sector neutrality, no size control: the quality tilt and the small-cap
  tilt are entangled.
- Score coverage differs by era (XBRL tagging improved after 2012), so early
  years rank fewer metrics.

## Reproduce

```
python -m src.backtest --load-pit       # EDGAR companyfacts -> pit_fundamentals (needs EDGAR_USER_AGENT)
python -m src.backtest --pit-backtest   # run_point_in_time_backtest on June-1 rebalances
```

The EDGAR companyfacts cache (4.0 GB for the universe) was deleted after the
run; the `pit_fundamentals` table (945,674 rows) was kept in the bot database.

## Correction (2026-10-19)

The cohort figures quoted above (moat NONE n=22: −10.5 pp; LOW conviction
n=27: −7.5 pp) do not hold. The 2026-10-19 audit found that 20 of the 22
"moat NONE" first ratings were legacy `registry.json` rows that
`migrate_from_registry` defaulted to NONE (the legacy schema stores the moat
under `analysis.moat.durability`; 13 of them were STRONG, 7 MODERATE), and the
other 2 were rated on the measurement day. The LOW-conviction cohort came from
the rating parser fixed on 2026-10-01; on clean data it is n=17, −3.9 pp
(t = −0.58) since rating and +0.8 pp at 90 days. The exclusion rule in ruleset
3.0.0 is kept as a prior, not as a tested result. The point-in-time result
above also carries a split look-ahead in FCF yield (adjusted prices × as-filed
shares); without the affected observations the pooled Spearman is +0.004.
Details: `docs/research/BUFFETTBOT_AUDIT.md`, section 1 and items F1, F2, F5.
