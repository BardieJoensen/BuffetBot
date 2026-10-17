# Replay of historical news downgrades through the breaker check (2026-10-17)

Question: how often does the ruleset 3.0.0 breaker check fire on the kind of
headline that, under the old rules, sent a name to C and forced a sell?

Method: every `tier_history` row with trigger `news_event` and new tier C
(19 rows, 2026-05-18 to 2026-10-13) was rebuilt as a breaker-check input:
the thesis breakers stored by the analysis that preceded the downgrade (after
the 2026-10-17 parser clean-up) and the news events logged for the ticker in
the preceding 24 hours. `CompanyAnalyzer.check_thesis_breakers` (Sonnet) was
asked which breaker, if any, the news reports as having happened. Read-only;
19 calls. Script: `scripts/replay_breaker_check.py`; raw inputs and verdicts in
`breaker-replay-2026-10-17.json` next to this note.

## Result

**0 of 19 fired.** Under the old rules these 19 headlines produced 19 C
verdicts and, for the three names that were held (CTSH, CCI, NNI), three sell
orders. Under 3.0.0 none of them touches the tier or the book.

What the headlines actually were, against what the breakers asked for:

| ticker | headline topic | nearest breaker | why it did not fire |
|---|---|---|---|
| BRBR | 2026 outlook cut, weak Q2 | 2 quarters negative organic growth | one quarter, no organic figure |
| UNH | Berkshire exited | breaker 5 itself says "warning, not breaker" | the analyst pre-labelled it |
| ADBE | CEO change, Semrush deal | enterprise churn >5% | nothing quantified |
| ROL | 52-week low, CFO change | commercial cancellations >10% | price action only |
| LULU | annual forecast cut, NA comps −5% | 2 consecutive quarters SSS decline | first quarter |
| CART | FTC fee fight | retailer ends partnership | regulatory pressure, no event |
| ACN | TD Cowen downgrade | margin <12% two quarters | analyst opinion |
| AAPL | UK iCloud lawsuit | App Store commission cut | different event |
| GL | stock rallied 26% | enforcement action | not even negative |
| PYPL | "margin pressure approaching 14%" | margin <14% | approaching, not breached |
| DPZ | US comps +0.1% | 3 negative quarters | positive comps |
| FSLR | class-action suits | 45X credit cut >50% | lawsuits, no policy change |
| NKE | China "struggles", cautious FY27 | 4 quarters China decline | not quantified |
| MCD | AI price-fixing suit | multi-year negative US comps | lawsuit |
| ACT | JP Morgan downgraded Insmed | any | wrong company entirely |
| CCI | SpaceX "could shake up" wireless | carrier adopts satellite as substitute | speculation |
| CTSH | H-1B / PERM policy at Microsoft | top-5 client loss, margin <12% | indirect, no CTSH figure |
| NNI | revenue −30.5% y/y, "potential" FFELP run-off | loss of ED servicing contract | revenue miss, no contract event |
| NVDA | Microsoft Maia as "main hardware" | hyperscaler >50% in-house, >30% seq. decline | positioning statement |

Three of the nineteen headlines (ACT, GL, DPZ) were not negative news about
the company at all; the old pipeline still re-rated them to C.

## Reading

- The old sell trigger was mostly noise: analyst opinions, lawsuits, macro
  pieces, articles about other companies. The new gate removes all of it.
- The closest calls are gradual deteriorations that the breakers describe with
  a multi-quarter threshold (LULU first quarter of comps decline, NNI −30%
  revenue, NVDA Maia). The breaker check is right not to fire on a single
  data point, and that is the intended behaviour: a deterioration that
  continues shows up in the next scheduled re-analysis, which can rate the
  name C and make it rotation-eligible without forcing a sell.
- A breaker that is phrased as an opinion ("warning signal, not a breaker")
  or that depends on a figure the press never reports (organic growth,
  customer acquisition cost) can never fire from news. The 3.0.0 prompt now
  asks for 2–4 single observable events; older analyses keep their old
  wording until re-analysed.

## Limitations

- The replay feeds the headline plus Haiku's bullet list, not the Finnhub
  summaries production uses, so Sonnet saw a slightly different text.
- 19 cases over five months, all from one Haiku screen. Zero fires says the
  gate is strict; it cannot say how it behaves on a true breaker, because the
  sample contains none that were actually reported as happened.
- One model, one run, no repeat sampling.

## Reproduce

```
python -m scripts.replay_breaker_check --out docs/research/breaker-replay-<date>.json
```

One Sonnet call per historical downgrade (about USD 0.02 each at 2026-10 prices).
