# Scout report: can BuffetBot be tested against history, and how do people usually do it?

Date: 2026-10-20. Scope: web research only (search plus page reads). No code, DB, config or paid LLM call was touched. Builds on `BUFFETTBOT_AUDIT.md` sections 1 and 3F and on `scout.md` F4, F14-F20, F26-F29, F37, which are not repeated here.

Source grades used below: **Read** = I opened the page or PDF text; **Abstract** = abstract or search summary only; **Secondary** = a third party describing the paper; **Own arithmetic** = my calculation from a cited formula, marked as such.

**Short answer to the owner's question.** Yes for the deterministic part (universe filter, quality score, valuation arithmetic, tier arithmetic, regime votes, deployment rules), with three prerequisites: survivorship-free prices with delisting returns, the existing EDGAR point-in-time table fixed as listed in audit IF4, and a frozen rule set before looking. No for the LLM stage: the honest answer is forward-only, and there is now a documented, model-specific clean window (F19). And even a perfect backtest can only reject bad settings; it cannot rank nearby knob settings (F7, F8, F9).

---

## 1. Standard practice for backtesting a systematic equity strategy

### F1. With 10 years of data, the best of 45 noise configurations already looks like a real strategy
- **What:** Bailey, Borwein, Lopez de Prado and Zhu prove that a modest number of tried configurations produces a high in-sample Sharpe from pure noise, and give a minimum backtest length as a function of the number of trials. The Deflated Sharpe Ratio (Bailey and Lopez de Prado) is the corresponding test: it compares the observed Sharpe with the expected maximum Sharpe under the null for the number of trials run.
- **Where:** https://papers.ssrn.com/abstract=2460551 (DSR, JPM 2014); https://carmamaths.org/jon/backtest.pdf and https://mathinvestor.org/2014/08/new-online-tool-to-demonstrate-backtest-overfitting/ (minimum backtest length); R implementations https://www.rdocumentation.org/packages/quantstrat/versions/0.16.7/topics/deflatedSharpe
- **Evidence:** Secondary summaries of the paper: with five years of daily data, no more than 45 independent configurations should be tried, beyond which a strategy with in-sample Sharpe 1 can be expected to have out-of-sample Sharpe 0; this is a necessary, not sufficient, condition. DSR "equals the Probabilistic Sharpe Ratio if only one strategy was tested" and the 0.95 threshold quoted by commentators is not from the paper (Abstract plus Secondary; I did not read the MinBTL formula itself). Own arithmetic using the standard extreme-value approximation with standard error 1/sqrt(T years): the expected best annualised Sharpe among N zero-skill configurations is about 0.70 for N=45 and T=10, and about 1.0 for N=1,000, T=10. This matches the paper's 45-configurations-in-5-years example (about 0.98 at T=5).
- **Why it matters:** The system map lists roughly 100 numeric constants (16 scored metrics times ideal/min/max/weight, five quality weights, 12 regime thresholds, five deployment targets, six tier weights, margin of safety, quality ceiling, DCF rate and terminal multiple). A hobbyist grid over even a handful of them on a 10-year window yields a "best" Sharpe at or above what a real quality-value premium delivers, so a tuned backtest number carries almost no information. The correct reporting unit is the Sharpe after deflating for the trial count, and the trial count has to be recorded (F13).
- **Effort:** S (add a trial counter; DSR is a few lines)
- **Depends on:** a log of every configuration evaluated
- **Confidence:** high on the principle, medium on the specific 45 figure (secondary)

### F2. Hold-out is not a safe fix when the data is public and you have already looked
- **What:** The same group argues the usual train/test hold-out is unreliable for investment backtests, and proposes the Probability of Backtest Overfitting (PBO) via combinatorially symmetric cross-validation.
- **Where:** https://papers.ssrn.com/abstract=2326253 (PBO, Journal of Computational Finance); text read at https://carmamaths.org/jon/backtest2.pdf; CRAN package https://cran.r-project.org/web/packages/pbo/ (README via search)
- **Evidence (Read):** "if the data is publicly available, it is quite likely that the researcher has used the 'hold-out' as part of the IS dataset. Second, even if no 'hold-out' data was used, any seasoned researcher knows well how financial variables performed over the time period covered by the OOS dataset, and that information may well be used in the strategy design, consciously or not." Third, "hold-out is clearly inadequate for small samples." PBO is defined as the probability that the in-sample best configuration underperforms the median out-of-sample.
- **Why it matters:** The 2016-2025 window has already been looked at (the 2026-10-17 PIT note, the cohort tables, the owner's own market memory). It cannot serve as a clean hold-out for any rule written after seeing it. The only genuinely unseen history is earlier (F10) or forward.
- **Effort:** S
- **Depends on:** a family of configurations with return series (PBO needs a matrix of trials)
- **Confidence:** high (read)

### F3. The bar for a "discovered" effect is t of about 3, and weak strategies get a haircut far larger than 50%
- **What:** Harvey-Liu-Zhu, Harvey-Liu and Chordia-Goyal-Saretto give multiple-testing hurdles for backtest evidence.
- **Where:** https://nber.org/papers/w20592 (Harvey, Liu, Zhu, RFS 2016); https://people.duke.edu/~charvey/Research/Published_Papers/P120_Backtesting.PDF (Harvey and Liu); https://www.sfi.ch/de/publications/anomalies-and-false-rejections (Chordia, Goyal, Saretto, RFS 2020)
- **Evidence:** HLZ catalogue 316 factors and conclude a newly discovered factor needs t above 3.0 (p below 0.0027) (Abstract). Harvey-Liu: the common 50% Sharpe haircut is wrong; for gross Sharpe below 0.4 the haircut is almost always above 50%, above 1.0 it is at most 25% (CXO summary, Secondary). Chordia et al., over two million random strategies: thresholds of 3.84 (time series) and 3.38 (cross-section), and 45.3% expected false rejections without correction (Abstract). Hou-Xue-Zhang (already in scout F4): 82% of 452 anomalies fail the 2.78 hurdle.
- **Why it matters:** Audit F10 already shows the existing PIT result is t of about 0.5 to 0.8, so it fails every threshold; the point here is what "passing" would require: t above about 3 on the pre-registered headline test, not 2. For a 10-15 year low-turnover sample that means an information ratio near 0.8 or more (F8), which no published quality-value record reaches (F28-F31). Expect the backtest to be inconclusive, not confirmatory.
- **Effort:** S
- **Depends on:** one pre-declared headline statistic
- **Confidence:** high (abstracts), medium (Harvey-Liu haircut numbers via CXO)

### F4. Delisting returns change anomaly results, and the standard repair is explicit
- **What:** Shumway (1997) and Shumway-Warther (1999) show missing delisting returns are large and negative; Beaver-McNichols-Price (2007) show accounting-based anomaly returns change when delisting firm-years are included.
- **Where:** https://ideas.repec.org/a/bla/jfinan/v52y1997i1p327-40.html ; https://ideas.repec.org/a/bla/jfinan/v54y1999i6p2361-2379.html ; https://ideas.repec.org/a/eee/jaecon/v43y2007i2-3p341-368.html
- **Evidence:** Shumway: correct delisting returns are unavailable for most negative-reason delistings since 1962 and the omitted returns are large (Abstract). Shumway-Warther: replacing missing performance-related Nasdaq delisting returns with -55% removes the apparent Nasdaq size effect (Abstract via search). Beaver et al.: including delisting firm-years raises returns of earnings, cash-flow and book-to-market strategies and lowers accrual strategies, because delisters cluster in the bottom decile of those variables (Abstract). The often-quoted -30% convention for NYSE/Amex is described by secondary papers only; I did not verify it.
- **Why it matters:** BuffetBot's PIT backtest scores today's survivors (audit F7), the opposite of what a quality or value screen needs: the names that blew up are exactly the low-quality or "cheap" ones, so the omission flatters the bottom quintile (the note's own +49.8% Q1 mean) and, if scored properly, would raise the apparent spread. The repair rule for any rebuild: take the last traded price, and for delistings with no price (bankruptcy, cancelled equity) apply the Shumway-style assumed return, not zero and not the last price.
- **Effort:** M (needs delisting reason codes, see F26)
- **Depends on:** a survivorship-free price source
- **Confidence:** high

### F5. How big the survivorship hole is, and the one free piece of the fix
- **What:** QuantRocket quantifies the share of names missing from a survivor-only dataset; a free MIT-licensed file gives historical S&P 500 membership.
- **Where:** https://quantrocket.com/blog/survivorship-bias (Read); https://github.com/fja05680/sp500 (Read)
- **Evidence:** QuantRocket: a survivor-only North American dataset looking back 10 years is missing about 75% of the stocks that were then trading (Europe about 50%, Asia about 25%); delistings are not always distress, so the bias can inflate or deflate results. fja05680/sp500: point-in-time S&P 500 membership from 1996, built from a file in Clenow's *Trading Evolved* plus Wikipedia changes since 2019, "as accurate as possible" but Wikipedia is incomplete; symbol counts vary 487-507, the author suggests starting in 2001; membership only, no prices.
- **Why it matters:** BuffetBot's universe is the current S&P 500 from Wikipedia plus a current Finviz mid/small screen (map), so 75% missing is the right order of magnitude for a 10-year look-back. The free membership file fixes the large-cap half of the universe definition, but not the small/mid half (no free historical Finviz-style screen exists), and it still needs prices for the departed names.
- **Effort:** M
- **Depends on:** F26 for prices; the owner choosing whether to backtest only the S&P 500 sleeve
- **Confidence:** medium-high (QuantRocket is a vendor of survivorship-free data)

### F6. Costs: low-turnover value/profitability survives them, and the buy/hold band is the design lever
- **What:** Novy-Marx and Velikov (RFS 2016) and Frazzini-Israel-Moskowitz (AQR live trade data) give the cost evidence; Corwin-Schultz gives a free spread proxy.
- **Where:** https://www.nber.org/papers/w20721 ; https://papers.ssrn.com/abstract=3229719 ; https://search.r-project.org/CRAN/refmans/bidask/html/spread.html (Corwin-Schultz in the `bidask` R package, GPL-3)
- **Evidence:** Novy-Marx-Velikov: of 23 anomalies, most with turnover below 50% per month earn significant net spreads when designed to mitigate costs, few high-turnover ones do; the most effective mitigation is a buy/hold spread (harder to enter than to stay); size, value and profitability have the greatest capacity (Abstract, with a disclosed DFA consulting tie). Frazzini et al.: actual institutional trading costs are about an order of magnitude below earlier estimates, and size/value/momentum survive (SSRN abstract; sample reported differently across versions, so check before quoting). Corwin-Schultz estimates the bid-ask spread from daily highs and lows (Abstract).
- **Why it matters:** These costs are for institutional long-short books in liquid names. BuffetBot's costs are the opposite regime: tiny order sizes (DKK 5,000 lots) where fixed commission, not impact, dominates (the 2026-10-17 note already says minimum commissions would have made the top quintile trail SPY). So the backtest cost model should be per-trade fixed fee plus a spread term from the Corwin-Schultz estimator on each name's own OHLC, with a rule that raises the bar for entering a name relative to holding it. Alpaca paper trading has no real costs and cannot calibrate this.
- **Effort:** S-M
- **Depends on:** the real broker's fee schedule (Nordnet is the audit's assumption; unverified here)
- **Confidence:** medium-high

### F7. A weekly 8-stock book has very few independent observations, so the standard error of a backtest is large
- **What:** Lo (2002) gives the standard error of a Sharpe ratio; Ledoit-Wolf (2008) give a studentised time-series bootstrap for comparing two Sharpe ratios; Burns' random portfolios give an empirical null for stock selection.
- **Where:** https://traders.studentorg.berkeley.edu/papers/The-Statistics-of-Sharpe-Ratios.pdf ; https://uzh.ch/cmsssl/econ/dam/jcr:ffffffff-935a-b0d6-0000-00007214c2bc/jef_2008pdf.pdf ; https://www.burns-stat.com/documents/working-papers/random-portfolios-for-evaluating-trading-strategies/
- **Evidence:** Under iid returns the standard error of the annualised Sharpe is about sqrt((1+SR^2/2)/T) (Secondary; I did not check Lo's text; Lo also warns against sqrt(12) scaling of monthly Sharpe). Own arithmetic: years needed for t=2 are 18 at SR 0.5, 6 at SR 1.0, and about 46 at an information ratio of 0.3 (audit F4 gets 87-126 years because it adds conventional power). A 10-year, 15-year and 25-year backtest can only distinguish information ratios of about 0.63, 0.52 and 0.40 from zero at t=2 (0.95, 0.77, 0.60 at t=3). Ledoit-Wolf: the Jobson-Korkie test is invalid with heavy tails or serial dependence and their bootstrap performs better in finite samples (Abstract). Burns: compare each run to matching random portfolios under the same constraints (Abstract).
- **Why it matters:** A "weekly cadence" does not give 520 observations per year; the signal updates quarterly and holds for years. Report confidence bands from a date-block bootstrap of the portfolio return series (not of stocks), and compare the 8-stock book to thousands of random 8-stock books drawn from the same filtered universe (which is also the honest way to show how much of any result is luck, extending audit IF9).
- **Effort:** S
- **Depends on:** a return series; for random portfolios, the same filtered universe per date
- **Confidence:** medium (formula via secondary, arithmetic is mine)

### F8. What a credible minimum sample looks like for this kind of strategy (synthesis)
- **What:** Combining F1, F3, F7 and the regime history gives a concrete requirement.
- **Where:** synthesis of the cited sources; the 2018-2020 value/quality drawdown is documented in https://quantpedia.com/out-of-sample-test-of-formula-investing-strategies/ (Read, Schwartz-Hanauer)
- **Evidence:** Own arithmetic and judgement, not a published rule: (a) at least 15 years so that a real premium with information ratio near 0.5 is detectable at t=2; (b) the window must contain at least one extended value or quality drawdown (Schwartz-Hanauer report substantial relative drawdowns for all formulas in 2018-2020, and weaker results after 2000); (c) one headline statistic declared in advance, at most a handful of tried configurations, trial count reported with a deflated Sharpe; (d) a separate unseen segment. The EDGAR XBRL phase-in began June 2009 for the largest filers and June 2011 for all others (Accounting Today via search, Secondary), so a fundamentals-based test cannot honestly start before about 2011 for the whole universe; 2011-2025 is the longest available window (14-15 years), which is only just enough, and the 2016-2025 window already used is two-thirds of it.
- **Why it matters:** It states what "enough" is before anyone looks at numbers, and shows the available history is right at the edge of enough: expect "inconclusive" for effects of 2-3 pp per year.
- **Effort:** n/a
- **Depends on:** F4, F5 for data; F13 for discipline
- **Confidence:** medium

---

## 2. What tweaking knobs on historical data does

### F9. In-sample to out-of-sample decay is large and well measured
- **What:** Several large studies measure how much of a backtested edge survives.
- **Where:** https://papers.ssrn.com/abstract=2745220 (Wiecki et al., Quantopian, 888 algorithms); https://alphaarchitect.com/beware-of-backtest-overfitting/ (Suhonen-Lennkh-Perez, about 215 bank strategies); https://arxiv.org/abs/2105.01380 (Why and how systematic strategies decay); McLean-Pontiff is in scout F4
- **Evidence:** Wiecki et al.: backtest Sharpe explains almost none of later performance (R^2 below 0.025, 888 strategies with at least six months out of sample), and the more a quant had backtested a strategy the larger the in-sample/out-of-sample gap (Abstract plus Secondary). Suhonen et al.: median Sharpe fell 73% from backtest to live across about 215 strategy indices from 15-17 banks (0.31 live vs 1.20 backtest per CXO); only 18 of 215 live Sharpe at or above backtest; most complex strategies took a haircut more than 30% larger than simple ones (Secondary; bank count differs between sources). arXiv 2105.01380: every later publication year adds about 5 points of Sharpe decay for newly published factors; publication year explains about 30% of the variance, overfitting-related variables about 15% more (Abstract).
- **Why it matters:** The right prior for "this setting looked best on history" is a substantial haircut, and the haircut grows with the number of knobs touched. A tuned backtest is not a forecast.
- **Effort:** n/a
- **Depends on:** n/a
- **Confidence:** high on direction, medium on the exact figures

### F10. The one clean historical test is a pre-sample window, if the owner has not looked at it
- **What:** Linnainmaa-Roberts re-test accounting anomalies on data before their discovery sample and find most are weak or absent, a design that applies to BuffetBot directly.
- **Where:** https://www.nber.org/papers/w22894 (RFS 2018)
- **Evidence:** Of 36 accounting-based anomalies, pre-discovery and post-discovery returns and Sharpe ratios are lower than in-sample; they argue data snooping explains much of the published pattern, and that the few survivors link to the shift toward intangible capital (Abstract, Secondary). In BuffetBot's case, the 2026-10-17 PIT backtest used annual June 1 dates 2016-2025 (read in `pit-backtest-2026-10-17.md`) and notes scores rank fewer metrics before 2012. EDGAR XBRL facts exist from 2009-2011 (F8). Whether `pit_fundamentals` contains filings before 2016 is unverified here (no DB access in this task).
- **Why it matters:** If 2011-2015 filings are loaded and nobody has examined that period, it is the only unspent out-of-sample segment: design on 2016-2025, freeze, then run once on 2011-2015 with delisted names included. It is one run only: after looking, it is spent.
- **Effort:** S (if data exists) / M
- **Depends on:** F4/F5 data and a written freeze (F13)
- **Confidence:** medium

### F11. Few parameters, equal weights and economic priors beat fitted values
- **What:** Estimation error swamps optimisation gains in short samples; simpler composites are more robust.
- **Where:** https://ideas.repec.org/a/oup/rfinst/v22y2009i5p1915-1953.html (DeMiguel, Garlappi, Uppal, RFS 2009)
- **Evidence:** Across 14 optimising models and seven datasets none reliably beat the 1/N portfolio; estimation windows of about 3,000 months (25 assets) to 6,000 months (50 assets) are needed for sample-based mean-variance to beat 1/N (Abstract plus Secondary). Same logic as Suhonen's complexity haircut (F9).
- **Why it matters:** The quality score's weights (.30/.20/.20/.15/.15) and the 16 per-metric ideal/min/max/weight tuples are exactly the kind of parameters that a 10-year sample cannot estimate. Equal weights or published weights (for example the AQR/Novy-Marx definition of profitability) are the defensible default; any deviation needs an economic reason written before the test.
- **Effort:** S
- **Depends on:** n/a
- **Confidence:** medium (the paper is about portfolio weights, the transfer to score weights is by analogy)

### F12. Legitimate robustness checks versus data mining
- **What:** A check is legitimate when it cannot change the choice you already made; it is data mining when its result feeds back into a setting.
- **Where:** https://papers.ssrn.com/abstract=2507040 (Statistical overfitting and backtest performance); https://arxiv.org/abs/1902.01802 (Rej, Seager, Bouchaud, discounting backtest PnL); practitioner guide https://harbourfrontquant.substack.com/p/overfitting-and-parameter-selection ; counterweight https://nber.org/system/files/working_papers/w28432/w28432.pdf (Jensen-Kelly-Pedersen)
- **Evidence:** Practitioner guidance (Secondary, Substack): a good strategy works across a range of parameter values and a single peak is a red flag; keep models simple; use the longest sensible period. Rej et al. offer a framework to quantify an in-sample discount factor (Abstract; the "factor of about 2" figure is quoted by a Macrosynergy piece, I could not confirm it in the paper). Jensen-Kelly-Pedersen replicate 153 factors in 93 countries and conclude most replicate and that over 80% of US factors stay significant after construction changes (Alpha Architect summary, Secondary), a useful balance to the pessimistic papers: the factors BuffetBot leans on (quality, value, profitability) are among the better-supported.
- **Why it matters (rule of thumb for the owner):** Legitimate: sub-period splits reported but not used to pick; the same fixed rule on different universes (S&P 500 only, mid caps only, ex-financials) and other regions via free French/AQR data; random-portfolio and block-bootstrap bands; perturbing every constant by plus/minus 20% and showing the result moves smoothly; reporting the whole trial history. Not legitimate: choosing the knob value that wins and then reporting the same window; dropping years or sectors after seeing them; adding a rule because a specific drawdown hurt; redefining the headline metric after the run.
- **Effort:** S
- **Depends on:** F13
- **Confidence:** medium (synthesis, sources mixed grade)

### F13. Pre-registration and a trial ledger are the cheap discipline that works
- **What:** Journal-level guidance converges on declaring tests before running them and reporting every trial.
- **Where:** https://papers.ssrn.com/abstract=3275654 (Arnott, Harvey, Markowitz, "A Backtesting Protocol in the Era of Machine Learning", JFDS 2019); https://academicnewsletter.sufe.edu.cn/info/359647 (Harvey, "The Scientific Outlook in Financial Economics", JF 2017); PBO paper text above
- **Evidence:** Arnott et al. propose a research protocol for quant finance and ML to counter self-deception in backtests and overfitting (Abstract; I did not read the individual protocol steps). Harvey: incentives to report significant results, unreported tests and no multiple-testing adjustment make many published results fail; even t above 3 yields many false positives when the true effect is rare; proposes the minimum Bayes factor and guidelines for a transparent research culture (Abstract; I could not confirm a literal "pre-registration" recommendation). PBO paper (Read): "Backtests ... almost never declare the number of trials involved in a discovery."
- **Why it matters:** For a one-person project the minimum is a dated markdown file listing hypothesis, headline statistic, universe, window, cost model and the exact constants, committed before the run, plus an append-only ledger (one line per configuration tried, with date and window). The ledger is also what F1's deflation needs. The repo's ruleset-version stamping (audit IF3) is the natural home.
- **Effort:** S
- **Depends on:** n/a
- **Confidence:** high on practice, medium on citations (abstract level)

---

## 3. Testing strategies whose signal comes from an LLM or from text

### F14. Telling the model to ignore the future, or masking names and dates, does not work
- **What:** Lopez-Lira, Tang and Zhu test whether LLMs can be trusted for forecasts inside their training window.
- **Where:** https://arxiv.org/abs/2504.14765 (also https://papers.ssrn.com/abstract=5217505)
- **Evidence (Abstract, latest version via fetch):** LLMs "cannot be trusted for economic forecasts during periods covered by their training data"; models recall exact economic and financial values from before their cutoff; instructions to respect historical boundaries do not prevent recall-level accuracy; masking entity names and dates fails because models reconstruct them from minimal context; any memorisation found is only a lower bound; after the cutoff "no recall" was observed.
- **Why it matters:** This partly contradicts the cheap fix in scout F14 (Glasserman-Lin: anonymised headlines did better in-sample). Anonymisation is useful against the distraction effect, but it is not a leak control for a rich prompt: Sonnet gets 10-K sections (the text of a 10-K is highly identifying). Do not run the Sonnet stage on 2016-2025 with names removed and call it point-in-time.
- **Effort:** n/a
- **Depends on:** n/a
- **Confidence:** high

### F15. There is a cheap statistical test for contamination that works through an API
- **What:** Gao, Jiang and Yan define a "Lookahead Propensity" (LAP): ask the model, with only a date, to recall the firm's outcome, and use the result as a contamination score.
- **Where:** https://arxiv.org/abs/2512.23847
- **Evidence (Abstract, fetched):** LAP is substantially positive throughout the in-sample period and drops to nearly zero just after the model's training cutoff; forecasts are more predictive for high-LAP firm-dates; the LAP-by-forecast interaction loses significance after the cutoff. Applied to news-headline returns and earnings-call capex predictions.
- **Why it matters:** It is the only after-the-fact contamination diagnostic I found that needs no logit access, so it can be run against hosted Claude. It diagnoses; it does not rescue a contaminated backtest. A LAP run on 30 names would cost a few cents and tell the owner whether Haiku or Sonnet "remembers" 2022-2025 outcomes for these names. (Contrast scout F16: MemGuard-Alpha found membership inference cannot reliably detect contaminated signals; LAP is a different, outcome-recall design.)
- **Effort:** S-M
- **Depends on:** a paid-call budget (small); not run here
- **Confidence:** medium (one paper, abstract-level)

### F16. LLM trading backtests lose half their return or more once the window passes the cutoff
- **What:** "Profit Mirage" documents that strong LLM-agent backtests largely vanish after the training cutoff.
- **Where:** https://arxiv.org/abs/2510.07920 ; commentary https://news.mccombs.utexas.edu/research/keeping-humans-in-the-loop/
- **Evidence:** Abstract: most LLM financial agents show a "profit mirage" in which back-tested returns disappear once the model's knowledge window ends, attributed to leakage; the authors' own remedy (FactFin, counterfactual perturbation, with the FinLake-Bench benchmark) is their own claim and unreplicated. McCombs commentary (Secondary): best agents lose "half their performance or more" after the cutoff and many "hit statistical zero"; the page gives no agent count or periods.
- **Why it matters:** Same message as F14, from the agent side. The quoted magnitude is a lower bound on how misleading a pre-cutoff LLM backtest is. Treat FactFin as unproven (vendor-style claim from the authors).
- **Effort:** n/a
- **Depends on:** n/a
- **Confidence:** medium

### F17. Chronologically trained models are the research-grade answer, but they are small and cannot do moat analysis
- **What:** Several groups train language models only on text available at each date.
- **Where:** https://arxiv.org/pdf/2502.21206 (ChronoBERT/ChronoGPT, He, Lv, Manela, Wu); https://arxiv.org/abs/2603.11838 (DatedGPT); https://arxiv.org/html/2509.01110v2 (NoLBERT)
- **Evidence:** DatedGPT (Abstract, fetched): twelve 1.3B-parameter models with annual cutoffs 2013-2024, about 100B tokens each; on 61,000 firm-day headlines, models whose training covers the outcome period show a lookahead premium of 26.4 bp per standard deviation (significant at 1%), while the lookahead-free instruct model reaches annualised Sharpe 3.20 on headline returns (a headline-news result, no costs stated; treat as hype-prone). ChronoBERT: real-time outputs gave a Sharpe similar to a much larger Llama model, "indicating that lookahead bias is modest" for that task (Abstract, Secondary). NoLBERT is trained only on 1976-1995 text (Abstract).
- **Why it matters:** These support evaluating text signals with date-bounded models on tasks like headline sentiment. Nobody has shown a 1.3B-parameter model can do a 10-K moat and management judgement, so this does not give BuffetBot a replay path for the Sonnet stage. Use it only as a reference design.
- **Effort:** L
- **Depends on:** n/a
- **Confidence:** medium

### F18. A credible leakage control for an LLM-on-fundamentals test has three parts, and BuffetBot's prompt would fail two of them
- **What:** Kim, Muhn and Nikolaev give the cleanest design I found.
- **Where:** https://arxiv.org/html/2407.17866v2 (Read, section 6.1)
- **Evidence (Read):** (1) Inputs are anonymised, standardised statements. (2) A recall test: asked to name the firm from 10,000 observations, GPT identified the firm 0.07% of the time and the year 2.95%. (3) An out-of-training-window holdout: FY2022 statements predicting FY2023 earnings, for a model with an April 2023 cutoff, gave GPT 58.96% accuracy vs ANN 59.10%, and the earlier 10-year average was 58.01%. Results were similar with a time-stamped model version.
- **Why it matters:** The design works because the input is numeric and nameless and the result matches a plain neural net. BuffetBot's Sonnet prompt has names and 10-K prose (fails 1 and 2); only the third control (post-cutoff window) is available, and see F19 for what that window is. The paper also says the model adds nothing over a conventional ML baseline on numbers, which is the baseline BuffetBot lacks (scout F18, F45).
- **Effort:** M
- **Depends on:** a numeric baseline on the same names and dates
- **Confidence:** high

### F19. Anthropic's published cutoffs define the only clean windows, and they are model-specific
- **What:** The help-centre and platform docs state training-data cutoffs per model. BuffetBot uses `claude-sonnet-5-5` (deep) and `claude-haiku-4-5` (light) per the system map.
- **Where:** https://support.claude.com/en/articles/8114494-how-up-to-date-is-claude-s-training-data (Read); https://platform.claude.com/docs/en/models/overview (Read; Sonnet 5.5 row)
- **Evidence (Read):** Help centre: "Claude Sonnet 5.5 was trained on data up until June 2026"; "Claude Haiku 4.5 was trained on data up until July 2025". The platform overview lists Sonnet 5.5 with reliable knowledge cutoff June 2026 and training data cutoff June 2026 (its table shows the 5.5 family; Haiku 4.5 is under legacy models, and I did not find a separate reliable-cutoff figure for Haiku 4.5, so use the help-centre July 2025).
- **Why it matters (three concrete consequences):** (a) The decision journal starts 2026-07-02, one day after Sonnet 5.5's June 2026 cutoff, so Sonnet ratings made from July 2026 and returns measured afterwards are clean; any Sonnet 5.5 rating dated earlier is not. Check `deep_analyses` for rows dated before 2026-07-01 made by that model (unverified). (b) A Sonnet replay on any past date is contaminated by construction (F14); the window after the cutoff is just the live stream. (c) There is a genuine retrospective window for the Haiku screen only: Haiku 4.5's outcomes for rating dates from about August 2025 to now (about 14 months) post-date its cutoff, so a replay of the Haiku quick-screen on point-in-time inputs rebuilt from `pit_fundamentals` for those dates is leak-free in the sense of F14 ("post-cutoff, no recall"). It is a short window (a few independent quarterly cross-sections), so it can test the Haiku gate's pass/fail rate and the size/recognition bias in the audit, not prove alpha. Caveat: replay inputs are not identical to the live `fundamentals` table (the audit's F9/IF7 differences).
- **Effort:** S-M
- **Depends on:** rebuilt PIT inputs (audit IF4, IF7); stored model_id and date per rating (audit IF12)
- **Confidence:** medium (cutoff dates from Anthropic pages read; the replay value is my inference)

### F20. Run-to-run noise of the LLM instrument should be measured before any outcome test
- **What:** Two recent papers measure repeatability of LLM financial scores.
- **Where:** https://arxiv.org/pdf/2503.16974 ; https://arxiv.org/pdf/2601.11958 ; https://arxiv.org/pdf/2311.15180
- **Evidence (Abstract/search summary only):** 2503.16974: reproducibility is high for binary classification and sentiment, lower for complex tasks; aggregating 3-5 runs improves consistency noticeably; downstream conclusions stayed fairly robust. 2601.11958: 30 stocks queried about 350 times each, Spearman 0.902 across repeats, but single-query scores correlated only 0.629 with the averaged score. 2311.15180: substantial sentence-level label variation passed through to portfolio returns; lowering temperature helps. Thinking is disabled and temperature settings for BuffetBot's prompts are not in this report.
- **Why it matters:** Test-retest agreement needs no outcome data, no look-ahead handling and no waiting. Re-rating 20-30 names 3-5 times would show how much of a tier is noise (a stock flipping between WIDE and NARROW or MEDIUM and HIGH between identical calls). If tier flips are common, outcome cohorts built on single calls are blurred and need more names, not different knobs.
- **Effort:** S
- **Depends on:** a small paid-call budget (not spent here)
- **Confidence:** medium-low (abstract level, different tasks)

### F21. Forward-only is the honest answer; the public forward tests are short, and several famous results are hype
- **What:** Benchmarks that evaluate LLM agents only after training cutoffs exist, but none is long enough to show alpha; consumer "ChatGPT picked stocks" stories are marketing.
- **Where:** https://arxiv.org/abs/2510.02209 (StockBench); https://arxiv.org/abs/2511.03628 (LiveTradeBench); https://arxiv.org/pdf/2510.11695 (When Agents Trade); https://www.finder.com/uk/press-release-chatgpt-investment-fund (Finder); https://www.cxoadvisory.com/fundamental-valuation/online-real-time-test-of-ai-stock-picking/ (AIEQ test, paywalled)
- **Evidence:** StockBench (Abstract): contamination-controlled multi-month trading using data the agents have not seen; most LLM agents "struggle to outperform" buy-and-hold. LiveTradeBench: 50-day live evaluation of 21 LLMs on US stocks and Polymarket; high LMArena rank does not imply better trading (Abstract). When Agents Trade: reports agents "often surpassing" buy-and-hold in real time but no consistent winner, TSLA agent returns spanning -38.72% to +21.91% (Abstract). Finder: 8 weeks, 38 stocks, +4.9% vs an average -0.8% for ten UK funds in the widely quoted version, but its own press release said +0.48% vs -0.78% (two figures for the same experiment; Secondary). AIEQ (an EquBot/IBM Watson AI-picked ETF with real money since October 2017): about 64% vs 75% for the S&P 500 total return from launch to May 2021 per a Bloomberg piece, quoted second-hand; still trading mid-2026.
- **Why it matters:** The decisive point for the owner: no public LLM stock-picking result, forward or backward, shows durable alpha net of costs. Finder and the Alpha Arena style tournaments are hype (weeks of data, a handful of names). The only credible design for BuffetBot is the audit's IF2 forward cohort programme plus counterfactuals; this report adds only that the cutoff dates in F19 tell it from when each model's forward data counts.
- **Effort:** n/a
- **Depends on:** audit IF2
- **Confidence:** medium-high on the conclusion (abstracts), low on individual Finder/AIEQ numbers

---

## 4. Tooling a hobbyist actually uses

All maintenance facts below are from the GitHub API on 2026-10-20 (single read each, no clone).

### F22. vectorbt and bt: maintained, fast for price-signal tests, no data and no delistings
- **What:** vectorbt (open-source "community edition") and `bt` are the two Python frameworks that fit a periodic top-N rebalance with minimal code.
- **Where:** https://github.com/polakowo/vectorbt ; https://github.com/pmorissette/bt
- **Evidence:** vectorbt: 9,446 stars, last push 2026-10-19, 128 open issues, license Apache-2.0 with the Commons Clause ("does not grant ... the right to Sell the Software"), README says it is the community edition of the paid VectorBT PRO (Read). bt: 2,996 stars, last push 2026-10-20, 15 open issues, MIT; designed around rebalancing algos and allocation trees.
- **What it would let BuffetBot test:** the portfolio mechanics (weekly or quarterly rebalance, equal weight, 8 names, cash fraction from the regime table, a buy/hold band, per-trade cost) given a panel of prices and a signal matrix. **What it would not:** fundamentals, point-in-time rules, delisted names, split/ticker-reuse hygiene (all on the user), anything LLM. Commons Clause is irrelevant for personal use but blocks some commercial wrappers.
- **Why it matters:** The engine is a one-day job; the data is the project. Do not choose a framework before the survivorship-free price and PIT-fundamentals panel exists.
- **Effort:** S
- **Depends on:** F25, F26
- **Confidence:** high (API facts), medium (fit judgement)

### F23. zipline-reloaded: maintained and rigorous, but the built-in data is dead
- **What:** Stefan Jansen's fork of Quantopian's Zipline keeps the Pipeline API for cross-sectional ranking.
- **Where:** https://github.com/stefan-jansen/zipline-reloaded ; https://zipline.ml4trading.io/bundles.html
- **Evidence:** 1,966 stars, last push 2026-10-19, 8 open issues, Apache-2.0. Bundles page (Read): the shipped `quandl` bundle "has been discontinued early 2018 and it no longer updates"; custom data via `csvdir` (daily OHLCV with `dividend` and `split` columns) or a custom ingest function; the page covers adjustments but says nothing about delistings or fundamentals.
- **What it would let BuffetBot test:** a daily-bar, rank-then-hold strategy with realistic order handling and commission/slippage models. **What it would not:** supply data; there is no fundamentals pipeline (Quantopian's Morningstar feed is gone), so the EDGAR PIT table would have to be wired in as a custom Pipeline factor. Heaviest to set up of the options.
- **Why it matters:** Best choice only if the owner wants one engine for the long run; more machinery than a rank-and-rebalance needs.
- **Effort:** M
- **Depends on:** F25, F26
- **Confidence:** high (API and docs read)

### F24. backtrader and QSTrader are stale; QuantConnect LEAN is alive and has a free cloud tier with delisting-aware data
- **What:** Two older frameworks have not been updated in over two years; QuantConnect is the one full-stack option that includes data.
- **Where:** https://github.com/mementum/backtrader ; https://github.com/mhallsmoore/qstrader ; https://github.com/QuantConnect/Lean ; https://www.quantconnect.com/pricing ; https://www.quantconnect.com/docs/v2/writing-algorithms/datasets/quantconnect/us-equity-security-master ; https://www.quantconnect.com/docs/v2/writing-algorithms/datasets/morningstar/us-fundamental-data
- **Evidence:** backtrader: 23,522 stars, last push 2024-08-19, GPL-3.0. QSTrader: 3,477 stars, last push 2024-06-30, MIT. LEAN: 22,111 stars, last push 2026-10-19, Apache-2.0, 281 open issues. QuantConnect pricing page (Read): free plan has "Unlimited Backtesting" with 1 backtest node, equity/index/forex/crypto/futures/options and "corporate fundamentals" at minute to daily resolution, no live or paper trading, no local LEAN-CLI use, community support; paid prices not displayed. Security master dataset (Read): corporate actions including delistings, mergers and ticker changes from January 1998; it is "NOT the underlying Equity data" (prices are sold separately by AlgoSeek). Fundamentals: coverage about 8,000 US equities from January 1998, "As Original Reported" with an approximated 45-day filing lag for older symbols, but a QuantConnect migration page says the dataset was switched to new feeds on 2026-09-23 across the entire history (a community post says both feeds run in parallel in October 2026, so the two sources conflict); a forum thread notes fundamentals for some delisted names may be missing, and the founder is quoted saying the data was changed from point-in-time (all Secondary, via search).
- **What it would let BuffetBot test:** the quant screen and deployment rules on a survivorship-aware panel with no data purchase, at the cost of coding in QuantConnect's API and trusting its fundamentals. **What it would not:** the LLM stage, yfinance-derived metrics (SBC haircut, the FCF definition) unless approximated from their fields; and results depend on a feed in the middle of a migration. Whether hosted-platform results can be exported or are bound by terms is unverified.
- **Why it matters:** The cheapest route to a first survivorship-aware answer is a free QuantConnect account; the cost is lock-in and uncertain PIT behaviour. A quick sanity check against `pit_fundamentals` on a handful of names would reveal feed differences.
- **Effort:** M
- **Depends on:** a decision on whether to trust a hosted fundamentals feed
- **Confidence:** medium (several secondary and contradictory sources)

### F25. Point-in-time fundamentals: EDGAR companyfacts is the right free base; Frames is not point-in-time
- **What:** Of the free sources, only companyfacts carries filing dates per fact; Frames returns the last-filed value.
- **Where:** https://www.sec.gov/search-filings/edgar-application-programming-interfaces (Read); https://eodhd.com/pricing (Read); https://www.quantrocket.com/pricing/data/sharadar (Read); https://github.com/nlpaueb/edgar-crawler ; https://huggingface.co/datasets/c3po-ai/edgar-corpus
- **Evidence:** SEC page: companyfacts returns all XBRL data for one company; frames returns one fact per company for a period using "the entity's last-filed fact that most closely fits the period" (so a later restatement replaces the original, and Frames cannot give as-first-reported history); bulk ZIPs rebuilt nightly about 03:00 ET; annual frames span 365 plus or minus 30 days. The page did not state a rate limit (the 10 requests per second in scout F26 is not confirmed by this page). XBRL phase-in: June 2009 (largest filers), June 2010, June 2011 (all others) (Secondary). Sharadar bundle per QuantRocket (Read): US company fundamentals 1990-present, prices 1998-present, S&P 500 constituents 1957-present, insider holdings 2005-present, 8-K events 1993-present; prices shown only after login, non-professional licence for personal use certified, professional users must buy via Nasdaq Data Link. EODHD: fundamentals from the $59.99/month plan ($49.99 annual), 10 API calls per fundamentals request, 100,000 calls/day; fundamentals back to 1985 for major US companies; whether they are as-reported is not stated (treat as restated). EDGAR-CORPUS: 10-K sections 1993-2020, open (licence not stated in what I read).
- **What it would let BuffetBot test:** companyfacts (already loaded, 945,674 rows) supports the quality score honestly from 2011. Sharadar adds 1990-2010 and cleaner as-reported dimensions, but it is not free and its price is unverified. For text signals, EDGAR-CORPUS plus filing dates gives an as-of corpus.
- **Why it matters:** BuffetBot already chose the right free source; the audit's issues (split look-ahead, debt tags, mixed fiscal years) are fixes to how it is used, not a reason to change source. Do not switch to Frames for the backtest.
- **Effort:** n/a
- **Depends on:** audit IF4
- **Confidence:** high for SEC facts (read), medium for vendor pricing

### F26. Delisted prices and delisting reasons: paid, but one year of Norgate or an EODHD plan is the realistic hobbyist route
- **What:** No free source covers delisted US stocks properly; two inexpensive paid ones do.
- **Where:** https://norgatedata.com/stockmarketpackages.php (Read); https://eodhd.com/financial-apis/delisted-stock-companies-data-2 ; https://www.quantstart.com/articles/evaluating-data-coverage-with-tiingo/ ; https://github.com/fja05680/sp500
- **Evidence:** Norgate: Silver $270/year (10 years, no delisted), Gold $360/year (20 years, no delisted, current fundamentals), Platinum $630/year or $346.50 for six months (history and delisted securities to 1990, historical index constituents, current fundamentals only, Python package listed but plan and platform not stated; a search summary says the plugin is Windows only, unverified), Diamond $787.50/year (back to 1950). EODHD: delisted names keep history (60,191 US delisted symbols listed), but data types depend on delisting date: EOD only before 2018, plus fundamentals and corporate actions from 2018 on; one user reports ticker reuse returning the new company's data under the old ticker (single anecdote). Tiingo: EOD history back to 1962 for 100,000+ tickers; QuantStart (old, beta-era) lists the free plan at 500 symbols per month, 50 requests per hour, 1,000 per day; delisted coverage not found.
- **What it would let BuffetBot test:** prices with delistings let F4 be done properly: universe at each date, last traded price, delisting return for the departing stock, and ticker identity (permno-like) so a reused symbol is not confused with an earlier company. **What it would not:** the delisting reason is not guaranteed; Norgate fundamentals are current, not PIT, so the EDGAR table stays the fundamental source.
- **Why it matters:** The CHRD ticker-reuse artefact in audit F7 (about 46% of the 2020 bottom-quintile mean) is the same hazard EODHD users report. A vendor identifier, not a symbol, must be the key. One-off cost, around $350-630, versus audit IF6's alternatives; export and licence-lapse terms must be read first (scout F29 already notes Norgate access ends if the subscription lapses).
- **Effort:** M
- **Depends on:** owner's decision to spend; licence read
- **Confidence:** medium

### F27. Analytics libraries compute the numbers, they do not supply the statistics that matter here
- **What:** quantstats and pyfolio-reloaded are maintained; neither deflates for trials nor adjusts for 8-name concentration.
- **Where:** https://github.com/ranaroussi/quantstats ; https://github.com/stefan-jansen/pyfolio-reloaded
- **Evidence:** quantstats: 7,727 stars, last push 2026-09-27, Apache-2.0, 26 open issues. pyfolio-reloaded: 616 stars, last push 2026-10-19, Apache-2.0, 4 open issues (earlier scout F37 recorded only weaker evidence). Neither page was read for features beyond the descriptions "Portfolio analytics for quants" and "Portfolio and risk analytics".
- **What it would let BuffetBot test:** tear sheets, drawdowns, rolling Sharpe, benchmark-relative stats once a return series exists (live or simulated). **What it would not:** deflated Sharpe, block-bootstrap bands, random-portfolio nulls (F7), or factor regressions; those are 30-60 lines of numpy/statsmodels on top.
- **Why it matters:** Cheap to adopt, easy to over-read. The tear sheet's Sharpe is the number F1 says to deflate.
- **Effort:** S
- **Depends on:** a clean return series
- **Confidence:** medium-high

---

## 5. Comparable rule-based long-only value/quality strategies and their out-of-sample record

### F28. The best-documented live record of a rule-based quality-value strategy trailed a broad index by about 5 points a year over ten years
- **What:** Alpha Architect's QVAL (Wes Gray's Quantitative Value process, run as an ETF since December 2015) is the nearest public analogue to BuffetBot's rule layer.
- **Where:** https://etfarchitect.com/wp-content/uploads/compliance/etf/TSR/QVAL_TSR-AR.pdf (Read, annual report for the year to 2025-09-30); https://funds.alphaarchitect.com/wp-content/uploads/2026/02/investment_case_qval.pdf (Read)
- **Evidence (Read):** Annual report, period ended 30 September 2025, average annual total returns for QVAL NAV vs Solactive GBS United States 1000 Index: 1 year 2.83% vs 17.40%; 5 years 15.89% vs 15.42%; 10 years 9.10% vs 14.43%; $10,000 grew to $23,899 vs $38,481; expense ratio 0.29%; portfolio turnover 332%; 52 holdings; the fund attributes the year's gap to its "tilt towards smaller value stocks". Investment-case deck: process is 1,500 largest US stocks, remove low ADV, accounting red flags, bad price action and extreme valuations, rank on EBIT/TEV, remove low Financial Strength Score, rebalance monthly, about 50 stocks (value first, quality as a filter). Standardised returns at 2025-03-31: 3-year 7.36%, 5-year 20.83%, 10-year 6.26%, since inception 7.23% (the deck prints market and NAV figures nearly identical). The authors' book claimed the model beat the Magic Formula by 3.74%/yr over 1974-2011 with Sharpe 0.74 vs 0.55 (CFA Institute review, Secondary), a backtest by the strategy's own authors.
- **Why it matters:** This is what "working" looks like in the wild: a serious team, a monthly systematic process, a documented mid-cap value tilt, 10 years, and it lagged the cap-weighted index by 5.3 points per year, mostly style drought (large-cap growth dominance), not noise. A 10-year live window would not have vindicated or condemned the idea. BuffetBot has fewer holdings (8 vs about 50), lower turnover and an LLM filter, but the same exposure. Any benchmark choice (audit F14) must make the style tilt visible so a lag is read for what it is.
- **Effort:** n/a
- **Depends on:** n/a
- **Confidence:** high (primary documents read)

### F29. Formula strategies: robust factor exposure, weaker than first published, with big 2018-2020 drawdowns
- **What:** Schwartz and Hanauer (2024) test the F-score, Magic Formula, Acquirer's Multiple and Conservative Formula in one framework.
- **Where:** https://quantpedia.com/out-of-sample-test-of-formula-investing-strategies/ (Read); SSRN 5043197; https://www.quant-investing.com/blog/magic-formula-performance-backtest-2000-2022 (vendor)
- **Evidence (Read via Quantpedia, Secondary to the paper):** US 1963-2022, CRSP/Compustat, microcaps excluded, quarterly rebalancing; top-minus-bottom decile spreads of about 5.6-6.4% a year raw and CAPM alphas of about 7.0-12.2%; the Magic Formula has the most alpha left after factor controls; outperformance comes "mainly from exposure to value, profitability and momentum"; for concentrated 40-stock capped value-weighted portfolios from 2000 the Magic Formula has the highest raw return and the Conservative Formula the best risk-adjusted; all formulas had substantial relative drawdowns in 2018-2020; performance is weaker after 2000 and returns were lower than in the original papers; costs are not analysed. Vendor page (Read; label as marketing): Magic Formula 40 stocks, 2000-2022, 17.2% average annual vs 8.0% for the value-weighted universe, Sharpe 0.69; excludes microcaps and financials; no survivorship or cost discussion. Alpha Architect's 2011 replication reportedly found real outperformance well short of the roughly 31% claimed in Greenblatt's book and strong sensitivity to small method changes (Secondary via search). An Oslo Stock Exchange thesis (2003-2022) finds about 0.5% monthly alpha that is only significant at p below 0.1 after costs (Secondary, thesis).
- **Why it matters:** These formulas are in effect what BuffetBot's quant layer is (cheap plus profitable). Credible gross alpha is a few points a year over a size-matched, value-weighted universe, mostly explained by known factors, with multi-year underperformance spells, and weaker after publication. That is the figure the owner should hold in mind; 17-31% numbers are the original or vendor ones.
- **Effort:** n/a
- **Depends on:** n/a
- **Confidence:** medium-high

### F30. Piotroski F-score: still sorts stocks internationally, but the US premium faded after 2000 and depends on how returns are computed
- **What:** Replications of the nine-point financial-strength score.
- **Where:** https://ideas.repec.org/a/pal/assmgt/v21y2020i2d10.1057_s41260-020-00157-2.html (Walkshäusl); https://orca.cardiff.ac.uk/id/eprint/189413/ ; https://www.cxoadvisory.com/fundamental-valuation/revisiting-performance-of-piotroskis-fscore/
- **Evidence:** Walkshäusl (2000-2018, developed ex-US and emerging markets): high-score firms beat low-score firms by about 10% a year, in all size segments (Abstract). A Cardiff replication (1997-2019): the strategy lost strength after 2000 (low scores no longer clearly underperform); using log returns restores it (Abstract). CXO: synchronising portfolio reformation dates cuts the US hedge return from 8.9% to 6.0% (1976-2007) (Secondary). A literature review says gross returns erode with liquidity constraints and implementation costs, and I found no study isolating small illiquid stocks (Secondary).
- **Why it matters:** Same message: signal real, shrinking, long-short spread overstates what a long-only 8-stock book gets, and method details move the number by a third. QVAL uses an F-score-like screen as a filter, not a ranker (F28), which is how BuffetBot's quality score is used.
- **Effort:** n/a
- **Depends on:** n/a
- **Confidence:** medium

### F31. Quality (QMJ): strong in the original paper and in 44-country extensions, patchy in the 2010s, and the data is free for the owner to check
- **What:** Asness-Frazzini-Pedersen's QMJ is the reference "quality" factor; AQR publishes its monthly returns.
- **Where:** https://research-api.cbs.dk/ws/portalfiles/portal/60211462/lasse_heje_pedersen_et_al_quality_minus_junk_publishersversion.pdf (publisher's version, not read); https://www.aqr.com/insights/datasets/quality-minus-junk-factors-monthly ; https://www.evidenceinvestor.com/post/quality-the-flip-side-of-value (Verdad analysis, Secondary); https://uis.brage.unit.no/uis-xmlui/handle/11250/2682140 (44-country thesis)
- **Evidence:** CXO summary (Secondary): optimal quality/value combination gross annualised Sharpe about 0.7 (US long sample) and 0.9 (short global sample). A 2020 thesis extending to 44 countries reports a factor-hedged Sharpe just above 1 (Secondary, low grade). Verdad (a gross-profit-to-EV quality measure, not QMJ): quality did well in 1997-2003 and 2017-September 2020 and added little in between (Secondary). AQR's dataset page lists QMJ monthly returns for the US and 23 other markets, updated through July 2026. I did not find a post-2012 QMJ record in text.
- **Why it matters:** Instead of trusting a summary, the owner can download QMJ (and the French factors) and look at post-2012 behaviour in an hour (audit IF9 already proposes the French data). Quality's weak patch overlapped a growth-led decade; BuffetBot's quality-first filter should be expected to have long flat or lagging spells.
- **Effort:** S
- **Depends on:** n/a
- **Confidence:** medium

---

## 6. What this means for BuffetBot (short and blunt)

**Can be backtested honestly (deterministic parts), once the data is fixed:**
1. The quant screen and quality score, on EDGAR PIT facts, with delisted names and delisting returns (F4, F26), the audit's IF4 fixes (split look-ahead, debt tags, fiscal-period mixing), and the live 16-metric `score_stock` rather than the 5-metric composite (audit F9).
2. The valuation arithmetic (Graham, P/E, DCF with fixed constants) as a standalone "cheapness" signal. The analyst-target input cannot be replayed (not stored historically); drop it from any backtest and say so.
3. Tier and deployment arithmetic, treated as a mechanical rule on top of (1) and (2): weights, quality ceiling, buy/hold band, regime votes. Regime inputs are free historically (VIX, index prices, drawdown, 200-day average; the S&P P/E series needs an as-reported source, not a restated CAPE). Expect the regime layer to rest on only a handful of independent episodes in 2011-2025 (2011, 2015-16, 2018, 2020, 2022), so it can be described, not validated.
4. Costs: a fixed per-trade fee plus a spread term (F6).

**Cannot be backtested (the LLM stage):** forward-only (F14-F19, F21). A replay of Sonnet 5.5 on any pre-July-2026 date is contaminated; masking and "pretend it is 2019" instructions do not fix it. The one retrospective exception is a Haiku 4.5 replay on post-July-2025 dates (F19), good for the gate's behaviour, not for alpha. Honest statement for the README: "the LLM stage has no historical validation and cannot have one; its value is being measured forward by cohort."

**Minimum credible setup (my judgement, not a published standard):**
- Written freeze: hypothesis, one headline statistic (for example, top-8-by-rules equal weight vs a size- and value-matched random-8 null, annual information ratio after costs), universe rule, window, cost model, every constant (F13).
- Survivorship-free prices with delisting returns for 2011-2025 (about $350-630 one-off, or a free QuantConnect pass first; F24, F26), plus the EDGAR PIT table fixed per IF4.
- Design on 2016-2025; run once on 2011-2015 if that data was never examined (F10).
- Report: block-bootstrap band, random-portfolio null, deflated Sharpe with the trial count, FF5-plus-momentum and QMJ regressions (F7, F12, F31).
- A pre-declared decision rule that includes "inconclusive" (expected, F3/F8) with a default of doing nothing.

**Defensible knob-tweaking:** choices made on economic grounds before looking (published definitions of profitability, equal weights, a 25% margin of safety on the stated rationale); coarse structural switches, not fine values (financials excluded or not; sector-relative ranks or not), each tried once and recorded; changes that remove a bug or a look-ahead; any change that fails the neighbourhood test (a plus/minus 20% change in the knob moves the result smoothly).

**Overfitting:** fitting the 16 per-metric ideal/min/max/weights, the five quality weights, the regime thresholds or the tier weights to the 2016-2025 returns; choosing between knob values that differ by 1-2 points of annual return (F8: with a typical 3% tracking error between two near-identical variants, own arithmetic gives about 36 years to detect a 1 pp/yr difference at t=2, and about 9 years for 2 pp); adding a rule because it would have avoided a named loss (AL, CTSH-type cases); using the paper account to "tune" (three months, t of 0.33).

**Order of effort for the owner (cost-ranked):** (1) write the freeze and trial ledger (hours); (2) look at QMJ and French factor data for context (hours); (3) re-rate 20-30 names 3-5 times to measure LLM noise (cents, F20); (4) fix the PIT backtest per IF4 and add bootstrap and random-portfolio bands (days); (5) only then decide whether survivorship-free data is worth about $350-630 (days); (6) the Haiku post-cutoff replay (days, F19).

---

## What I could not verify

- The exact Minimum Backtest Length formula (only the 45 configurations in 5 years example, via secondary summaries and my own extreme-value approximation).
- The detailed steps of the Arnott-Harvey-Markowitz protocol and whether Harvey (2017) literally recommends pre-registration (abstract level only; Alpha Architect page returned 403).
- Lo's Sharpe standard-error formula against the paper itself; Ledoit-Wolf finite-sample performance; the "factor of about 2" backtest-overfitting rule of thumb.
- Harvey-Liu haircut numbers (CXO summary), Suhonen et al. bank count (15 vs 17), Wiecki R^2 (abstract level), Profit Mirage's magnitudes (commentary only).
- The -30% NYSE/Amex delisting convention; Shumway-Warther's -55% is abstract-level.
- Sharadar's price (login-gated); FMP's current plans (403); whether EODHD, Tiingo or FMP fundamentals are point-in-time or restated, and Tiingo's delisted coverage.
- QuantConnect: whether its fundamentals are truly point-in-time after the September 2026 migration, whether delisted-company fundamentals are complete, and any export or data-use terms for results.
- Norgate: whether the Python package requires Windows and which plan includes it, and export terms.
- SEC rate limit (the page read did not state it); EDGAR-CORPUS licence.
- Whether `pit_fundamentals` holds filings before 2016, and which model produced each `deep_analyses` row dated before 2026-07-01 (no DB access in this task).
- Post-2012 QMJ returns; AIEQ's since-inception figures beyond the 2021 Bloomberg quote; the Finder experiment's later months.
- Haiku 4.5's "reliable knowledge cutoff" (only its training-data cutoff of July 2025 was found).
- Nothing was run: no backtest, no LLM call, no paid API; GitHub facts are single API reads on 2026-10-20.
