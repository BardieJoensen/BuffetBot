# Light model A/B: Claude Haiku 4.5 vs DeepSeek Flash (2026-10-18)

Question: can DeepSeek Flash (DeepSeek-V4.1-Flash-1017, through DeepSeek's
Anthropic-compatible endpoint) take over the light-model jobs, and what would
that save?

Method: `scripts/replay_light_model.py` gave both models identical inputs
through the production `CompanyAnalyzer` methods and parsers, with only the
client and the model name swapped. Thinking was off for both
(`_thinking_kwargs`). Read-only, on a copy of the database.

- **Quick screen:** the latest stored screen of 30 tickers that passed and 30
  that failed, rebuilt with the Wednesday batch summary (`_build_db_summary`,
  today's fundamentals).
- **News screen:** the 30 most recent (ticker, day) groups that production
  rated material and the 30 most recent it did not. All 60 had their Finnhub
  stories fetched again, keyword-filtered and formatted as in production.

The raw inputs and outputs are in `light-model-ab-2026-10-18.json` next to this
note.

## Result

| | Haiku 4.5 | Flash |
|---|---|---|
| Quick screen: invalid output | 0 / 60 | 0 / 60 |
| Quick screen: pass/fail agreement | 54 / 60 (90%) | |
| Quick screen: agrees with stored verdict | 56 / 60 | 52 / 60 |
| News: invalid output | 1 / 60 (TRV, no verdict line) | 0 / 60 |
| News: same recommendation | 53 / 59 (90%) | |
| News: HOLD / REVIEW / SELL | 49 / 10 / 0 | 56 / 4 / 0 |
| USD per call | 0.00108 | 0.00026 (0.00013 off-peak) |
| Mean latency | 1.9 s | 1.5 s |

- **Quick screen.** Moat hints differ by 0.37 on average and quality hints by
  0.18. Flash passed 4 names that Haiku failed (AII, CRMD, PBT, WELL) and
  failed 2 that Haiku passed (OGE, BA). Haiku agrees more often with the
  stored verdicts, but those were Haiku verdicts on older data, so that is
  partly self-agreement.
- **News screen.** All six disagreements are Haiku REVIEW against Flash HOLD
  (CLX, PEP, MO, CCI, CTSH, LOW). Flash's reasons hold up:
  - On MO it saw that the article was about Philip Morris (PM), not Altria.
  - On LOW it read +0.2% comps as weak but positive, not the "sustained
    negative comps" the breaker asks for.
  - CCI and CTSH are among the 19 headlines that
    `breaker-replay-2026-10-17.md` found do not fire a breaker.

  No case in the sample is a real breaker, so this measures noise, not
  recall.

## What it would save

The weekly caps allow 150 news checks and 50 screens, which comes to about 870
light calls a month at most. The Wednesday screens already run through the
Batches API at half price. At most, Haiku costs about $0.80 a month and Flash
about $0.15–0.20, so switching saves about $0.60 a month.

## Decision

Keep Haiku 4.5. Flash is at least as good on these jobs and 4–8× cheaper per
call, but the saving is too small to justify the switch:

- DeepSeek has no Batches API (`/v1/messages/batches` answers 404), so the
  Wednesday batch would need a sequential path.
- A second provider would need its own key and its own failure mode in the
  scheduler.

Flash is a tested fallback if Haiku 4.5 is retired or its price rises:
`--candidate`, `--base-url` and `--api-key-env` point the replay at any
Anthropic-compatible model.

The bigger cost is Sonnet: deep analyses and breaker checks make up most of
`run_log.total_cost_usd`. Replacing Sonnet is a judgement-heavy job, and it
would need its own replay against the deep-analysis outputs before anyone
considers it.
