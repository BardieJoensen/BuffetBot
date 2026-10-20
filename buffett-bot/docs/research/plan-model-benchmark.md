# Plan: the model benchmark (which LLM for the light role, which for the deep role)

Status: proposed 2026-10-20, not started. Decided so far: the light model moved to Claude Haiku 5.5 on 2026-10-20 (ruleset 3.1.1) ahead of measurement; this benchmark measures it after the fact and decides the deep role.

## Question and decision rule

Which model should fill each role, judged on quality and reliability, with price as a tie-breaker. The audit measured total Claude spend at about $2 a month (deep analyses under $1), so a cheaper deep model saves cents; cost is not the reason to switch.

A candidate replaces the incumbent for a role only if all four hold:
1. parse validity (share of responses the production parser accepts without defaults) is at least the incumbent's;
2. test-retest consistency (same input, repeated runs) is at least the incumbent's;
3. its moat labels are at least as tied to the fundamentals and no more tied to market cap than the incumbent's;
4. the owner's blind grading of its analyses is not worse.
Price breaks ties. Written down before the run; the run does not change the rule.

## What is measured, per model and role

Nothing below depends on stock outcomes, so training-cutoff differences between models cannot leak into the scores. Outcome-based comparison (per-model forward cohorts) is a separate, slow track and must use only dates after every candidate's cutoff.

| # | Metric | Role | How |
|---|---|---|---|
| M1 | Parse validity | both | `parse_analysis` / `parse_quick_screen` / breaker parser succeed; count fields that fell back to defaults (`moat_sources == ["unknown"]`, target entry None, conviction default). |
| M2 | Test-retest consistency | both | 3 runs per case; share of cases where moat, conviction, tier-relevant labels are identical across runs; fair-value coefficient of variation; quick-screen pass/fail agreement. |
| M3 | Agreement with the numbers | deep | Spearman of the moat label (NONE<NARROW<WIDE) against objective proxies from `pit_fundamentals`: 10-year ROIC persistence (share of years above 10%), operating-margin stability, share-count trend. |
| M4 | Size dependence | both | Spearman of pass rate / WIDE rate against market-cap decile (the current pipeline: Haiku pass 10% to 88% across quintiles; audit section 1). Lower is better, all else equal. |
| M5 | Breaker quality | deep | Share of breakers that are single news-observable events (regex heuristic from the AI audit, F8), and recall on the positive-control set (ID4: the `news_events` rows where a breaker plainly happened, hand-labelled once). |
| M6 | Format discipline | both | Share of responses with bold labels, extra headings, or missing sections (the parser is now markdown-tolerant, but discipline predicts future breakage). |
| M7 | Cost and latency | both | From `response.usage` and wall clock per call; batch vs realtime noted. |
| M8 | Blind grading | deep | 10 analyses per model, model name hidden, graded by the owner on a 1-5 sheet: is the moat argument specific to this business; is the bear case real; are the breakers observable; would you act on it. |

## Case set (frozen once, reused for every model)

30 deep cases and 60 light cases, stratified by market-cap quintile and sector, drawn from the universe with a seed. Context is frozen into one JSON file (`data/benchmark/cases.json`): the DB summary (`_build_db_summary`) and the cached 10-K excerpts (`augment_filing_text`) as of the freeze date, plus the real Finnhub snippets for the news cases. Every model sees byte-identical input.

## Candidates

Light role: Claude Haiku 5.5 (incumbent since 3.1.1), Claude Haiku 4.5 (previous), DeepSeek V4.1 Flash. Deep role: Claude Sonnet 5.5 (incumbent), DeepSeek V4.1 (full), and at most two others reachable through an Anthropic-compatible or OpenAI-compatible endpoint. Any model whose name `_thinking_kwargs` does not know needs an explicit entry before the run (the fallthrough sends `thinking: disabled`, which some generations reject with a 400).

## Estimated cost of one run

30 deep cases × 3 runs × 4 deep models at realtime list prices: about $5 per Sonnet-class model, less for cheaper ones; 60 light cases × 3 runs × 3 light models: under $1 total. Order of $20-25 for the full matrix, measured from `usage` as it runs with a hard stop at a configured ceiling. Needs the owner's approval before the run.

## Changes, by file

| File | Function | Change |
|---|---|---|
| `scripts/benchmark_models.py` (new; generalises `scripts/replay_light_model.py`) | `freeze_cases(db, seed, n_deep, n_light) -> Path` | Build and write `data/benchmark/cases.json` from the DB, the EDGAR cache and Finnhub; refuses to overwrite an existing freeze. |
| | `make_analyzer(spec: ModelSpec) -> CompanyAnalyzer` | A `CompanyAnalyzer` whose client is Anthropic (default), Anthropic-compatible (`base_url`, as the replay script does for DeepSeek), or an OpenAI-compatible adapter (decision 3 below); sets `model_deep`/`model_light`; wraps the client in `UsageRecorder` (already in the replay script). |
| | `run_deep(cases, analyzer, runs=3)` / `run_light(cases, analyzer, runs=3)` | Call `analyze_company`, `quick_screen`, `check_news_for_red_flags`, `check_thesis_breakers` through the production prompts and parsers; store raw text, parsed dict, usage, latency, exceptions per call. |
| | `score(results) -> dict` | M1-M7 per model; proxies for M3 from `Database.get_pit_concept_series` (`roic` needs `net_income`, `equity`, `long_term_debt`; margin from `operating_income`/`revenue`; shares from `shares_diluted`). |
| | `write_grading_sheet(results, out) -> Path` | M8: 10 analyses per deep model, shuffled, model names replaced by letters, with a key file kept separately. |
| | `main()` | `--freeze`, `--models spec.json`, `--runs`, `--ceiling-usd`, `--out`; stops when the running cost from `usage` reaches the ceiling. |
| `src/database.py` | `llm_calls` table (new) + `log_llm_call(...)` | Audit ID1: model id, prompt hash, parser version, raw response, usage, latency, for every production call too. The benchmark writes here with `source="benchmark"`. Prerequisite for any later re-parse or re-score. |
| `src/analyzer.py` | `_thinking_kwargs` | One explicit branch per candidate model name. |
| `src/analyzer.py` | `CompanyAnalyzer.__init__` | Accept an injected client (the replay script already assigns `.client`; make it a constructor argument). |
| `config/benchmark_models.example.json` (new) | | `[{"role": "deep", "name": "claude-sonnet-5-5"}, {"role": "deep", "name": "deepseek-v4.1", "base_url": "...", "api_key_env": "DEEPSEEK_API_KEY"}, ...]`. |
| `docs/research/model-benchmark-<date>.md` | | The report: one table per metric, the grading-sheet results, the decision against the rule above, and the trial ledger (every configuration run). |
| `tests/test_benchmark_models.py` (new) | | Freeze is deterministic for a seed; scoring on synthetic results; grading sheet hides names; ceiling stops the run. |

## Prerequisites and order

1. `llm_calls` table and raw-text storage (hours). Without it the benchmark results cannot be re-scored when the parser changes.
2. Hand-label the breaker positive-control set once (the 24 `news_events` rows the AI audit identified; an hour of the owner's time).
3. Freeze the case set (minutes, free).
4. The run (approval needed).
5. The grading sheet (an hour of the owner's time).
6. Report and decision; a model change in either role is a ruleset bump.

## Decisions needed from the owner

1. Approve the run budget (about $20-25) and a ceiling.
2. The candidate list beyond Sonnet 5.5 and DeepSeek V4.1 for the deep role, and which API keys exist.
3. Whether to add an OpenAI-compatible adapter (needed for any candidate without an Anthropic-compatible endpoint; a day of work and one new dependency) or keep to Anthropic-compatible endpoints only.
