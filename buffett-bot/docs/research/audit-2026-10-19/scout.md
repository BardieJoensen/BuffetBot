# Scout report: outside evidence for the BuffetBot audit (2026-10-19)

Scope: web research only (WebSearch and WebFetch). No code was audited, no paid LLM calls, no yfinance/EDGAR/Finnhub requests, no database access. System facts are taken from `SYSTEM_MAP.md` (cited as "map"). Where a figure comes from a search summary and not from a page I read myself, I say so.

Source grading used below:
- **Read** = I fetched the page and the quoted figure is on it.
- **Abstract** = confirmed from an abstract or catalogue record in search results, full text not read.
- **Secondary** = blog, vendor page, forum, news; treat as a pointer, not as evidence.

Findings are grouped (a) to (f) as in the brief. "Where" is the source URL. Findings that are weaknesses carry a severity; pure improvements carry n/a. Effort estimates are for the BuffetBot change, not for the research.

---
## (a) Evidence on the factors and timing rules the system uses

### F1. Quality is supported, but the system's definition is narrower than the one that was tested
- **What:** The Asness-Frazzini-Pedersen "Quality Minus Junk" paper defines quality as profitability, growth, safety (low leverage, low volatility) and payout, and finds a long-short quality factor earns significant risk-adjusted returns in the US and in 24 countries. BuffetBot's quality score is ROIC .30, ROE .20, FCF yield .20, operating margin .15, low debt .15 (map, Data flow 3): profitability and leverage only, no growth, no earnings-stability or beta/safety component, no payout/net-issuance component.
- **Where:** https://ideas.repec.org/a/spr/reaccs/v24y2019i1d10.1007_s11142-018-9470-2.html (published abstract, Review of Accounting Studies 24(1), 2019); working paper https://papers.ssrn.com/abstract=2312432
- **Evidence:** Abstract: higher-quality stocks trade at only modestly higher prices, "puzzling", while earning high risk-adjusted returns; QMJ earns significant risk-adjusted returns in the US and across 24 countries; "the price of quality" bottomed in the internet bubble and a low price of quality predicts higher future QMJ returns. A secondary source (NHH master's thesis, Secondary) reports QMJ profits are dominated by the short side, which matters because BuffetBot is long-only. Source grade: Abstract.
- **Why it matters:** The quality tilt is not unproven, but the live score is not the tested factor, and the published factor is long-short, so the long-only expectation is lower than the headline. Also, ROE and FCF yield are scored against all 1,359 names with no industry control (see F24).
- **Severity:** medium
- **Effort:** M
- **Unlocks / depends on:** a safety leg (leverage already, add earnings volatility or beta) and a net-issuance leg; needs the PIT quality-screen backtest (docs/research, 2016-2025) re-run on the new definition.
- **Confidence:** high on the paper's claims, medium on how much of the gap matters for 8 positions.

### F2. Gross profitability (Novy-Marx) is the best-documented quality measure and is absent
- **What:** Gross profits-to-assets predicts the cross-section of returns about as well as book-to-market, and adjusting value for profitability raises value-strategy performance most among large, liquid stocks. BuffetBot uses FCF yield and ROE instead.
- **Where:** https://nber.org/papers/w15940 (abstract, via search); JFE 2013 version; AQR award page https://www.aqr.com/Our-Firm/AQR-Insight-Award/2012/Distinguished-Paper-1
- **Evidence:** "Profitability, measured by gross profits-to-assets, has roughly the same power as book-to-market predicting the cross-section of average returns" and "controlling for profitability also dramatically increases the performance of value strategies, especially among the largest, most liquid stocks." A 2025 follow-up by Medhat and Novy-Marx is reported (Secondary, snippet only) to conclude that profitability subsumes the other quality measures. Source grade: Abstract.
- **Why it matters:** Free cash flow to equity penalises firms for investing in growth, gross profit to assets does not. The system's FCF-yield weight (.20) and its real-FCF DCF both penalise reinvesting compounders. Gross profit is also the one profitability number that is reasonably comparable across non-financial industries.
- **Severity:** medium
- **Effort:** S (a new column and rank, if XBRL GrossProfit/Assets are pulled from companyfacts, which the repo already loads for research)
- **Unlocks / depends on:** the 945,674-row `pit_fundamentals` table (map) already holds PIT facts; check whether GrossProfit and Assets are among the 15 concepts. Unverified.
- **Confidence:** high for the paper, medium for incremental value after ROIC is already in the score.

### F3. Value's lost decade is partly an intangibles artefact, which hurts book-based and P/E-based fair value
- **What:** HML (book-to-market) value had a 55% drawdown to mid-2020. Arnott, Harvey, Kalesnik and Linnainmaa attribute it to intangibles missing from book value and to a collapse in the value spread, and say capitalising intangibles fixes much of it.
- **Where:** https://ideas.repec.org/a/taf/ufajxx/v77y2021i1p44-67.html (Financial Analysts Journal 2021); https://scholars.duke.edu/publication/1472748
- **Evidence:** Abstract: drawdown reached 55% by mid-2020; relative valuation of value stocks fell from top quartile in early 2007 to bottom percentile by June 2020; capitalised-intangibles value measure "beats the traditional one by a wide margin". Authors have a commercial interest (Research Affiliates). Source grade: Abstract.
- **Why it matters:** The fair-value blend includes a Graham number (book-based) and a fixed 18x P/E (map, Data flow 6). Both mechanically call asset-light software and brand businesses expensive. This does not make the blend wrong, but the Graham leg has no support for intangible-heavy names.
- **Severity:** low-medium
- **Effort:** S (down-weight Graham for high-intangible sectors; map says it is already down-weighted for some)
- **Unlocks / depends on:** nothing.
- **Confidence:** medium.

### F4. Most published anomalies fail replication, and survivors decay after publication
- **What:** Two replication papers set the realistic expectation for any factor screen.
- **Where:** https://ideas.repec.org/a/oup/rfinst/v33y2020i5p2019-2133..html (Hou-Xue-Zhang, RFS 2020); https://afajof.org/issue/volume-71-issue-1/ (McLean-Pontiff, JF 2016); PDF mirror https://counterpointfunds.com/wp-content/uploads/2017/07/PredictabilityMcleanPontiff.pdf
- **Evidence:** Hou-Xue-Zhang: of 452 anomalies, 65% fail |t|>=1.96 and 82% fail the multiple-testing hurdle of 2.78 (abstract); commentary says value, momentum, investment and profitability replicate (Secondary). McLean-Pontiff: 97 predictors, long-short returns 26% lower out of sample and 58% lower post-publication (search summary of abstract). Source grade: Abstract.
- **Why it matters:** Value, profitability and momentum are among the survivors, which supports the screen. But post-publication decay of about half means a live edge from any one of them should be assumed to be roughly half of the in-sample figure, and with 8 positions it cannot be measured in under a decade (see F11).
- **Severity:** n/a (calibration)
- **Effort:** S
- **Unlocks / depends on:** state expected alpha as a range in the briefing; do not tune weights on short samples.
- **Confidence:** high.

### F5. Net share issuance and buybacks: a documented signal with no place in the scorer
- **What:** Net share issuance predicts returns, and the effect is mostly low returns after issuance outside the US. Buyback announcements earned a 12.1% abnormal four-year return in 1980-1990 but only for undervalued (value) firms.
- **Where:** https://ideas.repec.org/a/bla/jfinan/v63y2008i2p921-945.html (Pontiff-Woodgate, JF 2008); https://ideas.repec.org/a/eee/jfinec/v39y1995i2-3p181-208.html (Ikenberry-Lakonishok-Vermaelen 1995); international: McLean-Pontiff-Watanabe 2009 (search summary).
- **Evidence:** Pontiff-Woodgate abstract: post-1970 share issuance predicts returns more significantly than size, book-to-market or momentum. ILV: average abnormal four-year buy-and-hold return 12.1%, 45.3% for value stocks, no positive drift for glamour stocks. McLean-Pontiff-Watanabe (41 countries): effect driven more by low returns after share creation than by high returns after buybacks. Source grade: Abstract.
- **Why it matters:** The map shows an SBC haircut inside `_calculate_real_fcf` but no net-issuance or dilution factor in `quality_scorer` or `tier_engine`. A company that buys back stock to offset SBC passes the haircut but is treated the same as a heavy diluter.
- **Severity:** low-medium
- **Effort:** S (shares outstanding change from XBRL `CommonStockSharesOutstanding` or `WeightedAverageNumberOfDilutedSharesOutstanding`)
- **Unlocks / depends on:** PIT shares data (companyfacts, free).
- **Confidence:** medium.

### F6. Insider buying works mostly for purchases, small firms and non-routine trades, which the Finnhub feed cannot separate
- **What:** The best-known evidence is concentrated in three conditions: purchases not sales (Lakonishok-Lee), smaller firms (Lakonishok-Lee), and opportunistic not routine insiders (Cohen-Malloy-Pomorski). The system has a 5% weight on a Finnhub signal for top candidates only (map, Data flow and Plan items).
- **Where:** https://papers.ssrn.com/abstract=1692517 (Cohen-Malloy-Pomorski, JF 2012); https://ideas.repec.org/a/oup/rfinst/v14y2001i1p79-111.html (Lakonishok-Lee, RFS 2001); https://www.nber.org/papers/w6913 (Jeng-Metrick-Zeckhauser, REStat 2003)
- **Evidence:** CMP: 1986-2007 Thomson insider data, opportunistic-trade portfolio earns value-weighted abnormal 82 bp/month (equal-weighted 180 bp), routine trades about zero (abstract and Harvard CorpGov summary). Lakonishok-Lee: 1975-1995, informational content is in buying, insider selling has no predictive ability, result driven by smaller firms (abstract). JMZ: purchases earn abnormal returns above 6% a year, sales none (abstract). Post-2002: five-day announcement return on purchases reportedly rose from 1.0% to 2.3% after SOX faster disclosure (Brochet 2010, cited secondhand), but a 2024 intraday study finds smaller abnormal returns that vanish once tradable dollar size is limited (search summary, Secondary). I found no peer-reviewed 2010-2024 replication; vendor claims of "cluster buys earn double" are Secondary and unverified.
- **Why it matters:** CMP's routine/opportunistic split needs several years of each insider's trade history (the paper classifies by calendar pattern), which a snapshot API call per ticker cannot give. 10b5-1 plans are a further filter. The system's S&P 500 half of the universe is large-cap, where the effect is weakest.
- **Severity:** medium
- **Effort:** M (see F30 for a free source that carries the history)
- **Unlocks / depends on:** SEC Insider Transactions Data Sets (2006 onward); a 3-year lookback per insider.
- **Confidence:** high on the direction, low on the size today.

### F7. Analyst price targets are biased high and rarely hit, yet two of the ~5 fair-value inputs are analyst targets
- **What:** Targets are optimistic and noisy. The system merges yfinance and Finnhub targets into one leg of a plain mean.
- **Where:** https://ideas.repec.org/a/spr/reaccs/v18y2013i4d10.1007_s11142-012-9216-5.html (Bradshaw-Brown-Huang, RAST 2013)
- **Evidence:** Abstract (2000-2009 data): only 38% of 12-month targets are met at the end of the horizon, 64% at some point during it; implied target-based returns exceed realised returns by about 15 percentage points on average; absolute target errors average 45%; only weak evidence that analysts differ persistently in skill. An earlier working-paper version (1997-2002) reported 26% hit at horizon end (Secondary). Source grade: Abstract.
- **Why it matters:** The 25% margin of safety (map, Data flow 7) partly offsets the 15-point optimism, but it was chosen as a round number. A fair value built from a biased input will place more names in the cheaper tiers than a neutral one would; whether that is happening here is untested (map: `deep_analyses` has A 8 / B 194 / C 91 and no S ever).
- **Severity:** medium
- **Effort:** S (haircut the target leg by roughly the reported optimism, or drop it from the mean and keep it as a sanity bound)
- **Unlocks / depends on:** decision-journal outcomes (F40) to calibrate the haircut on own data.
- **Confidence:** high on the paper, medium on the transfer to current targets.

### F8. Graham-style evidence is for net-current-asset stocks, not for the Graham number
- **What:** The academic support for Graham is Oppenheimer's NCAV (net-net) study, which is a different screen. I found no study of the Graham number or the defensive-investor criteria as a fair-value estimator.
- **Where:** https://rpc.cfainstitute.org/research/financial-analysts-journal/1986/ben-grahams-net-current-asset-values-a-performance-update (Oppenheimer 1986); https://alphaarchitect.com/an-analysis-of-grahams-net-nets-outdated-or-outstanding/
- **Evidence:** Oppenheimer, 1970-1983, 645 net-net stocks, mean return reported as 29% a year against 11.5% for the market (secondary summary of the paper); hypothetical, equal-weight annual rebalance, 18 to 89 stocks a year. No source found for the Graham number. Source grade: Abstract plus Secondary.
- **Why it matters:** Graham's number (sqrt of 22.5 x EPS x book value per share) enters the equal-weight mean as if validated. It is unproven here; keep the leg only if the PIT backtest shows it adds information.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** ablation using `pit_fundamentals` (leave-one-out of each fair-value leg against forward return).
- **Confidence:** medium.

### F9. DCF reliability: no evidence that a 10%-discount, 12x-terminal DCF on yfinance FCF predicts anything
- **What:** The literature is argument not test: Damodaran stresses that inputs, especially growth and terminal value, dominate, and that analysts over-estimate growth. I found no peer-reviewed evidence that a retail-grade DCF ranks stocks.
- **Where:** https://rpc.cfainstitute.org/blogs/enterprising-investor/2012/the-dcf-model-question-your-assumptions ; https://www.bvresources.com/articles/bvwire/damodaran-takes-on-dcf-myths-151-1
- **Evidence:** Secondary summaries only: DCF "relies on a number of forward-looking assumptions"; Damodaran: analysts "generally too optimistic" on growth; high terminal-value share is not by itself a flaw (his myth 5). No numbers.
- **Why it matters:** Two of the ~5 fair-value legs are DCFs (real FCF and owner earnings) sharing one discount rate, one terminal multiple and the same FCF input, so they are highly correlated and effectively one vote counted twice. Whether the blend is better than any single leg is testable (F8 ablation).
- **Severity:** medium
- **Effort:** S (test) / M (rebuild)
- **Unlocks / depends on:** PIT fundamentals; sensitivity table (discount rate 8-12%, terminal 10-16x) logged per valuation.
- **Confidence:** low on size, high that it is untested.

### F10. Regime timing: weak out of sample, but a small tilt with a momentum component is the defensible form
- **What:** The S&P P/E, VIX, drawdown and 200-dma vote moves the target invested level between 65% and 100% (map, Data flow 9). The evidence says valuation timing is directionally right but weak out of sample; adding momentum helps.
- **Where:** https://www.nber.org/papers/w10483 (Goyal-Welch, RFS 2008); https://www.Aqr.Com/-/media/AQR/Documents/Insights/White-Papers/Market-Timing-Sin-a-Little.pdf (Asness-Ilmanen-Maloney, JOIM 2017, PDF read via pdftotext); VIX: Smales 2016 and Cipollini-Manzini 2007 via https://www.cxoadvisory.com/?p=15080 (Secondary)
- **Evidence:** Goyal-Welch: "not a single one would have helped a real-world investor outpredicting the then-prevailing historical equity premium mean"; 2024 update (Goyal-Welch-Zafirov, search summary): over a third of 29 newer variables no longer significant even in-sample. AIM 2017 (Read): contrarian valuation timing beat buy-and-hold over 115 years but underperformed in the latter half; drifting valuations made recent decades hard; adding momentum improves risk-adjusted results; recommends "modest market timing tilts". VIX: one-SD rise in VIX associated with a 0.70% lower next-month S&P return, 1990-2015, in-sample (Smales, Secondary); the only out-of-sample test I found was a 2-year holdout.
- **Why it matters:** The system's range (65-100%) is a modest tilt, consistent with the "sin a little" conclusion. The risks are the hard-coded P/E thresholds (15/23/30) on a market whose valuation level has drifted, and the cash drag in an ASK where lager tax applies to the account value either way. Its live record is 10 days of "overvalued" in 111 (map, regime_log), so the tilt has had almost no effect to date.
- **Severity:** low-medium
- **Effort:** S (replace fixed P/E levels with a percentile of the trailing history; keep the momentum votes)
- **Unlocks / depends on:** free long-history valuation data (Shiller CAPE, not verified, see end).
- **Confidence:** high.

### F11. Eight positions: diversification and tracking-error evidence says outcomes will be dominated by luck for years
- **What:** With 8 holdings, residual risk is large and skewed. Bessembinder shows the return distribution of individual stocks is so skewed that a small share of stocks creates all net wealth, so a concentrated pick-set misses the winners most of the time; Domian et al. find shortfall risk keeps falling beyond 100 stocks.
- **Where:** https://wpcarey.asu.edu/sites/default/files/2021-10/do-stocks-outperform-treasury-bills.pdf (Bessembinder, JFE 2018, summaries read); https://ideas.repec.org/a/bla/finrev/v42y2007i4p557-570.html (Domian-Louton-Racine 2007); https://academicnewsletter.sufe.edu.cn/info/354996 (Cremers-Petajisto active share)
- **Evidence:** Bessembinder: the best-performing 4% of listed companies explain the net dollar gain of the US stock market since 1926, the rest matched Treasury bills; the share of stocks beating bills over their lives is reported as 43% or 38% depending on the summary. Domian et al.: 1,000 large US stocks, 20-year horizon, shortfall risk keeps falling above 100 stocks; the 2003 version found 5% of 20-stock portfolios had ending-wealth shortfalls above 28%. Cremers-Petajisto: highest active-share funds beat benchmarks and showed persistence in 1980-2003 (they say nothing on 8-stock portfolios). My own arithmetic (assumption, not sourced): at 25-35% idiosyncratic volatility per stock, 8 equal weights leave 9-12% a year of residual tracking error before any sector or factor bets, so a 2-3% annual alpha has a t-stat below 0.3 per year of data.
- **Why it matters:** The map's `closed_trades` has 2 rows and `cohort_validation` tier A has n=8. Nothing the system will see in the next 3-5 years can distinguish skill from luck. Concentration is a defensible choice for a high-conviction investor, but the briefing should state this and the user should know it is a risk-for-conviction trade, not a tested edge.
- **Severity:** medium (honesty of the evidence, not the construction)
- **Effort:** S (add expected tracking error and a one-line "what this sample can show" to the journal report)
- **Unlocks / depends on:** nothing.
- **Confidence:** high on direction; the 9-12% is illustrative.

### F12. Value and momentum are strongly negatively correlated; a quality-value-only book has no momentum hedge
- **What:** Asness-Moskowitz-Pedersen find value and momentum returns negatively correlated, so combining them improves Sharpe. A cheap-quality screen leans toward losers by construction.
- **Where:** https://pages.stern.nyu.edu/~lpederse/papers/ValMomEverywhere.pdf (JF 2013, abstract via search)
- **Evidence:** Average within-asset-class correlation -0.49, -0.60 for stocks (Swedroe summary, Secondary); blended value-momentum beat either alone in every market (Chicago Booth Review summary). Alpha Architect's practitioner funds combine value, quality and momentum but show hypothetical or short live records only (Secondary).
- **Why it matters:** The system only has momentum-adjacent inputs in the regime vote (200-dma) and none at stock level. A cheap name in a downtrend and a cheap name in an uptrend are scored alike. Momentum as a timing filter on entry (not as a signal) is the least invasive way to use it.
- **Severity:** medium
- **Effort:** S (12-1 month return rank as a veto or tie-breaker; data already in yfinance prices)
- **Unlocks / depends on:** PIT backtest to confirm on this universe.
- **Confidence:** medium-high.

---
## (b) Evidence on LLM stock analysis and look-ahead bias

### F13. LLM news signals are real but are mostly an initial reaction, not tradable, and decay as adoption rises
- **What:** Lopez-Lira and Tang show GPT scores of headlines predict returns, but the strong "90% hit rate" is for the non-tradable initial reaction; the later drift exists mainly for small stocks and negative news; returns fall as LLM adoption rises.
- **Where:** https://arxiv.org/abs/2304.07619v6 (read abstract)
- **Evidence:** "approximately 90% portfolio-day hit rates for the non-tradable initial reaction"; scores "also predict subsequent drift, especially for small stocks and negative news"; "strategy returns decline as LLM adoption rises". Sample period not stated on the abstract page.
- **Why it matters:** `daily_news_monitor` runs at 20:00 Copenhagen on company news for 105 S/A/B names, then Haiku and Sonnet flag a breaker, which moves a stock to tier C and can force a sell the next session (map). By then the initial reaction is priced. The paper's drift effect is in small stocks and negative news, a minority of BuffetBot's names. Expect sells to chase the move.
- **Severity:** medium
- **Effort:** S (log price at flag time and 1/5/20-day forward return for every thesis-breaker flag; 19 historical downgrades were replayed in the 2026-10-17 note)
- **Unlocks / depends on:** the replay research note already in docs/research.
- **Confidence:** medium.

### F14. Anonymise tickers when evaluating an LLM, because distraction can outweigh look-ahead bias
- **What:** Glasserman and Lin compare GPT sentiment on original headlines with headlines stripped of company identifiers.
- **Where:** https://arxiv.org/abs/2309.17322
- **Evidence:** In-sample, anonymised headlines outperform, "indicating that the distraction effect has a greater impact than look-ahead bias", strongest for larger companies; out of sample, look-ahead bias was not a concern. Source grade: Abstract.
- **Why it matters:** This is the cheapest look-ahead control for a retrospective LLM test: run the same prompt with name and ticker removed, and compare. Because Haiku is called with DB fundamentals only (no filing text), an anonymised run is trivial. The A/B in `replay_light_model.py` could include it.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** any new LLM replay or backtest.
- **Confidence:** high.

### F15. Standard LLMs show alpha decay across regimes in a look-ahead benchmark, but the benchmark comes from a vendor of the "fix"
- **What:** Look-Ahead-Bench measures how performance degrades between market regimes for general models versus point-in-time models.
- **Where:** https://arxiv.org/abs/2601.13770 (read abstract)
- **Evidence:** Llama 3.1 8B and 70B and DeepSeek 3.2 compared with the Pitinf family from PiT-Inference; "standard LLMs show substantial look-ahead bias when measured by alpha decay", Pitinf models do not. The abstract gives no numbers or dates. Possible conflict of interest (PiT-Inference). Source grade: Abstract; the figures in a secondary blog post could not be verified.
- **Why it matters:** The qualitative point is sound and matches F16 and F17: any LLM evaluation on dates before the model's training cutoff is contaminated. Do not buy the vendor solution on this evidence.
- **Severity:** n/a
- **Effort:** n/a
- **Unlocks / depends on:** n/a
- **Confidence:** medium.

### F16. Memorisation is real and cannot be detected after the fact; the only clean test is post-cutoff
- **What:** Two 2026 papers say the same thing from different angles. MemGuard-Alpha tested seven LLMs on 50 S&P 100 stocks and found membership-inference scores cannot reliably identify contaminated signals; FinCAD, an inference-time fix, needs logit access and was tested on five mega-caps and 7-14B models only.
- **Where:** https://arxiv.org/abs/2603.26797 (read); https://arxiv.org/abs/2605.24564 (read)
- **Evidence:** MemGuard-Alpha: raw scores reached AUC 0.99 but after within-model normalisation fell to 0.487-0.537, close to chance; no filtering variant beat the unfiltered ensemble on risk-adjusted performance and none achieved significant FF5 alpha; 42,800 prompts, 2019-2024. FinCAD: largest in-sample return correction -67.1%, five 7-14B models, five mega-cap stocks, 2025 out of sample; needs per-entity confidence and decoding access. Lopez-Lira and co-authors (cited in MemGuard) report GPT-4o can recall S&P 500 closes and headline dates from its training window.
- **Why it matters:** Any "historical test" of the Sonnet moat/management judgments on 2016-2025 is contaminated by what Claude already knows; FinCAD cannot be applied to a hosted Claude API. The decision journal, which started 2026-07-02 (map), is the only clean evidence, and only if the deep model's training cutoff is earlier than the decisions. The training cutoffs of claude-sonnet-5-5 and claude-haiku-4-5 should be read from the model cards. Unverified here.
- **Severity:** high for any claim that Sonnet's tiering has a validated edge; none is made in the map, but `cohort_validation` and the tier labels read as if one exists.
- **Effort:** S
- **Unlocks / depends on:** a per-row `model_id`, `analysis_date` and stated cutoff stored in `deep_analyses`; forward-only evaluation windows.
- **Confidence:** high.

### F17. Chronologically consistent models exist and show the bias can be modest for a downstream predictor
- **What:** He, Lv, Manela and Wu train ChronoBERT/ChronoGPT using only text available at each date.
- **Where:** https://arxiv.org/pdf/2502.21206 ; instruction-tuned version https://arxiv.org/pdf/2510.11677
- **Evidence:** Real-time outputs gave a Sharpe ratio similar to state-of-the-art models, "indicating that lookahead bias is modest" for their prediction task (search summary of abstract). Source grade: Abstract, secondhand.
- **Why it matters:** These are small open models; they are a tool for a cleaner backtest of text signals, not a replacement for Claude, and there is no evidence they can do moat analysis. Mention only as the reference practice.
- **Severity:** n/a
- **Effort:** L
- **Unlocks / depends on:** n/a
- **Confidence:** low-medium.

### F18. GPT-4 on anonymised financial statements matches a plain neural net, not magic; names and prose are where the system differs
- **What:** Kim, Muhn and Nikolaev feed GPT-4 anonymised, standardised statements (no names, no narrative) and ask for the direction of next-year earnings.
- **Where:** https://arxiv.org/html/2407.17866v2 (read, first 100,000 characters)
- **Evidence:** Accuracy: GPT-4 chain-of-thought 60.35%, simple prompt 52.33%, analysts (1-month) 52.71%, 3-month 55.95%, 6-month 56.58%, naive 49.11%, stepwise logit 52.94%, ANN with 59 predictors 60.45%. Full sample 1968-2021, 150,678 firm-years; analyst sample 1983-2021. The excerpt says the GPT long-short strategy has FF3 alpha above 12% a year (strategy details were in the truncated part).
- **Why it matters:** The best evidence for LLMs in fundamentals says the model matches a conventional ML model on numbers, without names. BuffetBot's Sonnet step uses names plus 10-K text for a qualitative moat/management judgement; the paper gives no evidence for that task. A numeric model would be a cheap baseline to beat before paying for Sonnet.
- **Severity:** medium (no baseline)
- **Effort:** M
- **Unlocks / depends on:** PIT fundamentals; logistic or gradient-boosted baseline against Sonnet tier on the same names and dates.
- **Confidence:** medium-high.

### F19. Long-horizon, survivorship-aware tests find LLM trading agents lose to buy-and-hold
- **What:** FINSABER re-tests published LLM trading agents over 2004-2024 with delisted names included.
- **Where:** https://arxiv.org/html/2505.07078v3 (read; arXiv 2505.07078, KDD)
- **Evidence:** In the bias-mitigated composite setups (63-91 symbols each) buy-and-hold beat FinMem and FinAgent, for example Random Five: buy-and-hold Sharpe 0.315 / return 6.7%, FinAgent 0.094 / 4.5%, FinMem -0.253; paired t-tests favour buy-and-hold; no LLM alpha significant (all p > 0.34). Agents were "too conservative in bull markets and too aggressive in bear markets" (FinAgent Sharpe 0.12 bulls / -0.38 bears; FinMem -0.19 / -0.97; buy-and-hold 0.61 / -0.28). Caveats from the page: the authors could not replicate some published figures, long-only, limited set of agents, data leakage not adjusted (generous to LLMs).
- **Why it matters:** These are timing/trading agents, not a quality-value stock selector, so the transfer is partial. The relevant lesson is process: they find short windows and few tickers inflate results, and BuffetBot's evidence base is 16 decisions and 2 closed trades.
- **Severity:** n/a (calibration)
- **Effort:** n/a
- **Unlocks / depends on:** n/a
- **Confidence:** high.

### F20. LLMs have documented stock-selection biases toward large, famous, tech names and contrarian ideas
- **What:** Lee et al. test several models on 427 continuously listed S&P 500 stocks.
- **Where:** https://arxiv.org/abs/2507.20957 (search summary of abstract); CFA Institute write-up https://blogs.cfainstitute.org/investor/2026/02/18/attention-bias-in-ai-driven-investing/
- **Evidence:** "a consistent preference for large-cap stocks and contrarian strategies across most models", often hardening into confirmation bias. Source grade: Abstract, secondhand. I found no source on repeat-run inconsistency; unverified.
- **Why it matters:** Haiku screens 870 rows and Sonnet 293; the universe is 465 S&P names plus 855 small/mid. The Haiku pass rate by market cap and sector is a one-query check on the live DB that I did not run (read-only DB access was available but outside the scout's scope). Moat scores that correlate with firm size are a size tilt, not moat knowledge.
- **Severity:** medium
- **Effort:** S
- **Unlocks / depends on:** DB query: pass rate and mean moat by market-cap decile and sector; same per tier.
- **Confidence:** medium.

### F21. Predictive LLM embeddings of news exist with a delay consistent with limits to arbitrage, but only abstract-level evidence
- **What:** Chen, Kelly and Xiu use LLM embeddings of news to forecast returns in 16 markets and 13 languages.
- **Where:** https://abfer.org/media/abfer-events-2024/cmd/Large-Language-Models-and-Return-Prediction-in-China_slides_discuss_YinanSu.pdf (related); seminar abstracts via https://www.pbcsf.tsinghua.edu.cn/info/1504/7668.htm
- **Evidence:** Seminar abstracts say LLM representations improve on technical signals and simpler NLP methods and that news is absorbed with a delay (Secondary). No Sharpe figures found.
- **Why it matters:** Balanced counter-evidence to F19, but it is an embeddings-plus-regression approach trained on years of data, not prompt-based judgements. Not a template for BuffetBot.
- **Severity:** n/a
- **Effort:** n/a
- **Unlocks / depends on:** n/a
- **Confidence:** low.

---
## (c) Industry-aware quality scoring and the data it needs

### F22. Rank within industry, not across the market: the decomposition is in the literature, and the system does not do it
- **What:** Asness, Porter and Stevens split each characteristic into a within-industry and an across-industry component; the two carry different information and sometimes opposite signs.
- **Where:** https://www.aqr.com/Insights/Research/Working-Paper/Predicting-Stock-Returns-Using-IndustryRelative-Firm-Characteristics
- **Evidence:** NYSE/AMEX/NASDAQ, June 1963 to November 1998; significant within-industry momentum; at short horizons contrarian within-industry and momentum across-industry effects; suggests industry-relative sorting is "possibly better" for quantitative selection (search summary of abstract). A later peer-deviation version tests 94 characteristics (CXO Advisory, Secondary).
- **Why it matters:** `compute_quality_scores` percentile-ranks ROIC, ROE, FCF yield, operating margin and debt across all 1,359 names (map). Structurally high-margin, low-debt industries (software, asset-light services) dominate the top and capital-heavy ones (REITs, utilities, lessors, banks) sink or pass on leverage artefacts. The five held names include a REIT (CCI), insurers (AIZ, AGM), and a lessor was held and delisted (AL). The map lists sector overrides in `score_stock` for 4 sectors but not in the quality score that feeds `tier_engine`'s fair-price exception (quality_score >= 80).
- **Severity:** high
- **Effort:** M
- **Unlocks / depends on:** GICS-like sector/industry per ticker (yfinance `.info` has sector and industry strings; Finviz screen already carries industry); minimum group size rule (e.g. fall back to sector when an industry has fewer than 8 names).
- **Confidence:** high on the direction, medium on the magnitude.

### F23. Industry momentum is a separate, documented effect and implicit industry bets will dominate an 8-name book
- **What:** Moskowitz and Grinblatt find industry momentum explains much of individual-stock momentum and survives controls.
- **Where:** https://ideas.repec.org/a/bla/jfinan/v54y1999i4p1249-1290.html (JF 1999)
- **Evidence:** Abstract: controlling for industry momentum, ordinary stock momentum looks far less profitable; industry momentum profits survive size, book-to-market and stock-momentum controls. Later work is mixed (Canada, Europe negative; 38-country study positive; Arnott et al. attribute it to factor momentum). Source grade: Abstract.
- **Why it matters:** With 8 positions and no cap on how many come from one industry (not verified in code), the book's industry exposure is an unmanaged side-bet. Measuring it costs nothing.
- **Severity:** low-medium
- **Effort:** S (report industry weights in the briefing; cap at 2 per industry)
- **Unlocks / depends on:** industry tags.
- **Confidence:** medium.

### F24. Financials, insurers and REITs need different metrics, not different weights on the same FCF-based pipeline
- **What:** Damodaran argues financial service firms are best valued with equity models and dividends or excess returns (ROE minus cost of equity), because debt and reinvestment cannot be defined. REITs report FFO and AFFO because GAAP depreciation of buildings distorts earnings.
- **Where:** https://pages.stern.nyu.edu/~adamodar/pdfiles/val3ed/c21.pdf (Damodaran, chapter 21, search summary); FFO definition: https://resources.tellusapp.com/passive-income/reits/ffo-and-affo-explained (Secondary; NAREIT's own page not read)
- **Evidence:** Damodaran: equity models with "actual or potential dividends" for banks and insurers, ROE and cost of equity as the value drivers, regulation shapes capital and dividends. FFO = net income excluding property sale gains, plus real estate depreciation; AFFO also deducts recurring capex; companies often deviate from the NAREIT definition so filings must be checked (Secondary).
- **Why it matters:** The map says DCF "skips loss/FCF-negative names", the real-FCF DCF and FCF yield are meaningless for insurers and banks (FCF includes float and investment flows), and for REITs P/E and FCF are poor. Data needed: for banks and insurers, book value and tangible book, ROE, cost of equity, CET1 or RBC ratio, combined ratio and reserve development, net interest margin and loan loss provisions; for REITs FFO/AFFO per share, net debt/EBITDAre, occupancy, same-store NOI, lease expiry; for lessors, utilisation, residual value risk, fleet age. Free sources: SEC XBRL companyfacts has us-gaap StockholdersEquity, Revenues, and bank/insurer tags; FFO and AFFO are non-GAAP and normally appear in 8-K earnings exhibits, not as us-gaap XBRL tags (unverified), so a hobbyist must parse press releases or use a paid fundamentals vendor.
- **Severity:** high for the 2-3 names held in these categories (AIZ, AGM, CCI per map)
- **Effort:** M (route financials/REITs to an alternate scorer, or mark them "unscored: exclude from tiering") / L (full sector models)
- **Unlocks / depends on:** industry tags (F22); the simpler interim fix is to exclude or flag names in `too_hard_industries`.
- **Confidence:** high on need, medium on data availability.

### F25. Free industry and factor return data for neutralising and attributing
- **What:** Kenneth French's library gives industry portfolios (10, 12, 17, 30, 48) and the 5-factor and momentum series, daily and monthly, free.
- **Where:** https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html (read)
- **Evidence:** Lists FF 5 factors (2x3) monthly/daily in TXT and CSV, industry portfolios 10/12/17/30/48 monthly/daily, Momentum Factor and 10 momentum portfolios; latest period shown August 2026; no usage terms beyond the copyright line; returns can change when CRSP revises. I did not confirm whether financials are excluded from RMW construction.
- **Why it matters:** This is the standard control for "is the book earning anything beyond market, value, profitability, investment and momentum exposure". With paper equity since 2026-07-02 it cannot yet say anything, but the pipeline can be built now.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** a weekly return series (the `portfolio_snapshots` table has 111 rows).
- **Confidence:** high.

---
## (d) Data sources and tools not yet used (terms and limits as found)

### F26. SEC EDGAR APIs and bulk files: free, no key, 10 requests/second, PIT by `filed` date
- **What:** companyfacts, companyconcept, frames, submissions JSON on data.sec.gov, plus nightly bulk zips. The system uses companyfacts for research only (map).
- **Where:** https://www.sec.gov/edgar/sec-api-documentation ; https://www.sec.gov/edgar/searchedgar/accessing-edgar-data.htm (both read)
- **Evidence:** "Current max request rate: 10 requests/second"; user agent must include a company name and admin email; no authentication or API keys; XBRL data typically updated in under a minute; `companyfacts.zip` and `submissions.zip` rebuilt nightly about 3:00 a.m. ET; frames returns one fact per entity for a period (e.g. `CY2019Q1I`); no CORS; no technical support for scripted downloads.
- **Why it matters:** Frames lets the weekly screen pull one concept for the whole market in one call instead of 1,359 yfinance `.info` calls, and removes the dependence on yfinance for fundamentals. The `filed` date makes any backtest PIT.
- **Severity:** n/a
- **Effort:** M
- **Unlocks / depends on:** a concept map (revenue, gross profit, operating income, assets, equity, shares, debt) and a TTM builder; the existing `edgar_fundamentals.py` is the start.
- **Confidence:** high.

### F27. SEC Insider Transactions Data Sets: free bulk Form 3/4/5 history from 2006, with the 10b5-1 flag
- **What:** Quarterly flattened data sets from the XML portion of Forms 3, 4 and 5.
- **Where:** https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets (read)
- **Evidence:** Coverage January 2006 through September 2026, updated quarterly, filings after 5:30 pm ET on the last business day of a quarter roll into the next posting; a PDF documents tables; field `AFF10B5ONE` was added to the SUBMISSION file for the 2023-2025 sets in July 2025. The page does not list transaction code or price fields (documented in the PDF; not read).
- **Why it matters:** This is the only free source with the multi-year per-insider history that CMP's routine/opportunistic classification needs, and the flag to drop 10b5-1 plan trades. Quarterly latency suits a backtest, not live use; for live, daily Form 4 via EDGAR submissions.
- **Severity:** n/a
- **Effort:** M
- **Unlocks / depends on:** F6.
- **Confidence:** high.

### F28. Financial Modeling Prep, Tiingo, Alpha Vantage, Polygon/Massive, EODHD: what free gets you
- **What:** None of these replaces EDGAR for fundamentals on a free tier, but some give price history and ratios cheaply.
- **Where:** Tiingo https://www.tiingo.com/about/pricing (read); Alpha Vantage https://www.alphavantage.co/premium/ (read); FMP: official pricing page returned HTTP 403, figures from https://site.financialmodelingprep.com/insights/platform/how-to-choose-the-right-financial-modeling-prep-plan-for-your-workflow and aggregators (Secondary); Polygon/Massive: https://apicostcalc.com/polygon.html and https://veryfront.com/docs/cloud/integrations/polygon (Secondary); EODHD: https://apis.io/plans/eodhd/eodhd-plans-pricing/ (Secondary)
- **Evidence:** Tiingo free (Starter): 50 requests/hour, 1,000/day, 500 unique symbols/month; fundamentals not included (add-on); Power $30/month. Alpha Vantage free: 25 requests/day; premium $49.99 (75/min) to $249.99 (1,200/min) a month. FMP free Basic: 250 calls/day, end-of-day and profile data; Starter $22/month billed annually with 5 years history and annual fundamentals; Premium $59/month with 30 years; redistribution needs agreement. Polygon renamed Massive (dates conflict between sources): free 5 calls/min and 2 years; Financials and Ratios only on the $199 Advanced plan (one third-party guide). EODHD fundamentals feed from $59.99/month; a forum post says fundamentals for companies delisted after 2018 only.
- **Why it matters:** At about $5-15 a month of API budget, EDGAR (free) plus a $22-30 price/ratio feed is the realistic ceiling. None of these supply survivorship-free fundamentals cheaply.
- **Severity:** n/a
- **Effort:** S-M
- **Unlocks / depends on:** an abstraction in `screener.py` so a second provider can fill gaps; prices change often, recheck before subscribing.
- **Confidence:** medium (Secondary for FMP, Polygon, EODHD).

### F29. Survivorship-free universes: paid, small, and the only way to test delistings properly
- **What:** Sharadar (Nasdaq Data Link) and Norgate provide delisted names and PIT data; the system has already had a delisting loss (AL, $7,242 frozen, map).
- **Where:** https://data.nasdaq.com/databases/SFA (read); https://www.quantrocket.com/docs/data/fundamental/sharadar/ ; https://norgatedata.com/ ; price lists: Norgate ~$35/month on one listing vs ~$270-788/year on another (Secondary, conflicting).
- **Evidence:** Sharadar Core US Equities Bundle includes prices, delisted companies, and as-reported fundamentals dimensions ARQ, ART, ARY; QuantRocket says fundamentals history since 1990 and prices since 1998. Sharadar price not found (unverified). Norgate: access ends if the subscription lapses.
- **Why it matters:** The system's PIT table covers 1,292 tickers that exist today (current universe), so its backtest has survivorship bias, which the 2016-2025 quality backtest note should disclose. A one-off month of Sharadar would let the backtest include delistings and bankruptcies. Cost is unverified.
- **Severity:** medium (backtest bias)
- **Effort:** M
- **Unlocks / depends on:** confirm quote before buying; export once and keep.
- **Confidence:** medium.

### F30. yfinance is the single fundamental source and its own README restricts use to personal research
- **What:** The library is not Yahoo-endorsed and Yahoo's API is "intended for personal use only".
- **Where:** https://cdn.jsdelivr.net/gh/ranaroussi/yfinance@main/README.md (search summary of the README)
- **Evidence:** README: "intended for research and educational purposes", "finance API is intended for personal use only", users must read the Yahoo terms pages. I did not retrieve Yahoo's current terms or any rate limit; unverified.
- **Why it matters:** Personal use is what this is, so no breach, but it means no contract, silent schema changes, and fields missing for some tickers. The weekly screen over 1,359 tickers depends on it; a failure mode is quiet partial data, as with the 2026-09-16 bad snapshot day (map).
- **Severity:** medium
- **Effort:** M (see F26)
- **Unlocks / depends on:** EDGAR frames and a staleness check on `fundamentals`.
- **Confidence:** medium.

### F31. FX: Danmarks Nationalbank rates via Frankfurter (free) and Statistics Denmark; Nordnet's API is closed to new customers
- **What:** The system takes USD/DKK from yfinance `DKK=X`. An official, keyless alternative exists.
- **Where:** https://frankfurter.dev/providers/dnb/ (read); https://www.dst.dk/en/Statistik/emner/penge-og-kapitalmarked/rente-og-kursudvikling/valutakurser ; https://www.nordnet.no/externalapi/docs/getting_started (search summary)
- **Evidence:** Frankfurter DNB provider: daily indicative rates, 45 currencies, from 1977-01-03, free, no key, no limits stated; indicative not necessarily official fixing. Nordnet getting-started page (via search): "Nordnet API is currently not onboarding new customers" and access is tied to one account. Saxo OpenAPI not researched.
- **Why it matters:** For a Danish tax computation or a DKK equity curve, using the Nationalbank rate is the defensible choice (Skat guidance for pension returns uses Nationalbank rates, search summary; ASK-specific guidance not found). The real-account connection by API is not available through Nordnet today, so BuffetBot stays advice-only for the real ASK.
- **Severity:** low
- **Effort:** S
- **Unlocks / depends on:** DKK return attribution (F44).
- **Confidence:** medium.

### F32. Alpaca market data free tier is IEX-only; OpenBB is an AGPL connector layer, not a data source
- **What:** Two tools that look useful and mostly are not.
- **Where:** https://docs.alpaca.markets/docs/about-market-data-api (read); https://docs.openbb.co/platform/licensing ; https://docs.openbb.co/platform/usage/extensions/data_extensions (search summaries)
- **Evidence:** Alpaca Basic: IEX real-time only, history from 2016 but limited to the latest 15 minutes for the free feed, 200 historical calls/minute, Algo Trader Plus $99/month; no fundamentals or corporate-action endpoints mentioned on that page. OpenBB moved from MIT to AGPL with a commercial option; FMP, Polygon and Tiingo are listed as free connectors needing their own keys.
- **Why it matters:** Neither adds data the system lacks. OpenBB would only save writing three provider adapters.
- **Severity:** n/a
- **Effort:** n/a
- **Unlocks / depends on:** n/a
- **Confidence:** medium.

---
## (e) Comparable systems: what they do differently, hype versus results

### F33. virattt/ai-hedge-fund: popular, educational, no performance claims; the useful ideas are the paper ledger and kill switch
- **What:** A "proof of concept for an AI-powered hedge fund" with investor-persona agents, a backtester and a paper-trading mode.
- **Where:** https://github.com/virattt/ai-hedge-fund (read)
- **Evidence:** 64.1k stars; "for educational and research purposes only"; data from Financial Datasets API; paper trading "with a hash-chained session ledger and a kill switch"; backtesting against a benchmark; no return figures or accuracy metrics anywhere on the page.
- **Why it matters:** Popularity is not evidence. What BuffetBot does better: a real decision log with realised alpha, tier cohort validation, breaker replays. What it could borrow: a hash-chained immutable decision ledger (cheap tamper-evidence for the journal) and an explicit kill switch for auto-trading.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** n/a
- **Confidence:** high.

### F34. TradingAgents and Alpha Arena: hype until tested over long windows; treat headline Sharpe ratios as artefacts
- **What:** TradingAgents reports superior cumulative return, Sharpe and drawdown for a multi-agent "trading firm"; Nof1's Alpha Arena put LLMs on live crypto perpetuals.
- **Where:** https://arxiv.org/abs/2412.20138 ; commentary https://dev.to/trow126/the-most-starred-llm-trading-paper-claims-buy-and-hold-lost-523-it-actually-gained-912-1jj6 ; https://www.gncrypto.news/ru/news/qwen-wins-alpha-arena-season-1-with-22-percent-returns/
- **Evidence:** TradingAgents: one summary says the simulated window was 2024-01-01 to 2024-03-29, another cites AAPL 26.62% with Sharpe 8.21 for 2024-06-19 to 2024-11-19; the paper itself is said to attribute the high Sharpe to a short backtest (all Secondary, unreconciled). A blog claims the paper's buy-and-hold baseline lost 5.23% when it actually gained 9.12% (unverified blog). Alpha Arena season 1 (2025-10-18 to 11-03, six models, $10,000 each, Hyperliquid perps): only Qwen 3 Max (+22%) and DeepSeek v3.1 ended in profit, GPT-5 worst at about $3,733 (one Russian-language news source, unverified). Flag: hype.
- **Why it matters:** Three months and a handful of tickers is the evidence base for the most-starred LLM trading framework. FINSABER (F19) is the sober counterweight.
- **Severity:** n/a
- **Effort:** n/a
- **Unlocks / depends on:** n/a
- **Confidence:** medium (Secondary).

### F35. Qlib shows what a serious quant stack has that BuffetBot lacks: walk-forward ML on a PIT database, but no live US dataset
- **What:** Microsoft Qlib offers 20+ forecasting models, Alpha158/Alpha360 features, a point-in-time database feature and RL execution.
- **Where:** https://github.com/microsoft/qlib (read)
- **Evidence:** 49.5k stars, MIT, about 2,070 commits; sample LightGBM run annualised excess return about 17.8% before costs and 12.9% after, information ratio about 2.0 and 1.4 (the page does not say which market or period; likely China data, unverified); official dataset download "disabled temporarily", no US data source given; latest tagged release v0.9.0 (December 2022), unreleased breaking change on main.
- **Why it matters:** The lesson is method (rolling retrain, costs included, information ratio reported), not code to adopt: with ~1,300 names and 25 weekly cross-sections, there is not enough data to train anything beyond a linear baseline.
- **Severity:** n/a
- **Effort:** L
- **Unlocks / depends on:** n/a
- **Confidence:** medium.

### F36. FinGPT and FinRobot: no independent evaluation found; treat as unproven
- **What:** Both are AI4Finance Foundation open-source frameworks.
- **Where:** https://aimultiple.com/de/agentic-ai-finance (vendor-style comparison, Secondary); https://neurohive.io/ru/papers/finrobot-otkrytyj-multiagentnyj-frejmvork-dlya-analiza-rynka-akcij/
- **Evidence:** One 2026 comparison reports FinGPT 86% on a financial statement analysis test and FinRobot 75% on valuation, with no test set or method described. Nothing found on look-ahead, leakage or backtests.
- **Why it matters:** Do not borrow from them without a leakage audit.
- **Severity:** n/a
- **Effort:** n/a
- **Unlocks / depends on:** n/a
- **Confidence:** medium-low.

### F37. Tooling for attribution and tear sheets: quantstats and pyfolio-reloaded exist; neither fixes the sample-size problem
- **What:** Open-source analytics libraries for performance and risk.
- **Where:** https://github.com/ranaroussi/quantstats (read); https://github.com/stefan-jansen/pyfolio-reloaded (search summary)
- **Evidence:** QuantStats: Apache-2.0, 601 commits, HTML tear sheets, a recently added Monte Carlo feature; the page gives no release dates and says "Full documentation coming soon". pyfolio-reloaded: community fork, v0.9.8 September 2024 per a Snyk snapshot (stale); original pyfolio called unmaintained in a vendor page (Secondary). zipline-reloaded and vectorbt exist; not evaluated for maintenance.
- **Why it matters:** A weekly QuantStats tear sheet of `portfolio_snapshots` against SPY, plus FF5 regression (F25), is half a day's work and replaces hand-rolled alpha maths. Monte Carlo on 8 positions is the honest way to show the luck range (F11).
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** a clean equity curve; fix the 2026-09-16 bad snapshot first (map).
- **Confidence:** medium.

### F38. Decision journals: practitioner advice, no hard evidence found
- **What:** I found no peer-reviewed evidence that keeping an investment decision journal improves returns.
- **Where:** none (search returned only blogs and vendor material)
- **Evidence:** absent. Unproven.
- **Why it matters:** The journal is valuable for BuffetBot because it creates the only clean out-of-contamination evidence (F16), not because journalling is known to help.
- **Severity:** n/a
- **Effort:** n/a
- **Unlocks / depends on:** n/a
- **Confidence:** high that I found nothing; low that nothing exists.

---
## (f) Danish ASK and FX angle (2026 rules where found)

### F39. The ASK limit is 174,200 DKK for 2026; the live .env still holds 135,900
- **What:** The 2026 deposit ceiling is 174,200 kr (166,200 in 2025). Per the map, the repo default was updated in commit b04ea84 but the live `.env` still reads 135,900.
- **Where:** https://skat.dk/borger/aktier-og-andre-vaerdipapirer/aktiesparekonto (read); https://www.nordnet.dk/academy/hvad-er-en-aktiesparekonto
- **Evidence:** skat.dk: ceiling 174,200 kr for 2026, calculated from the account value on 31 December of the previous year (room = ceiling minus that value), applies per owner, net deposits count (withdrawals free room, redeposit within the year allowed), excess deposits carry a 3% annual fee, tax can always be paid in above the ceiling.
- **Why it matters:** Any lot-size or "how much room is left" logic reading the live env understates the room by 38,300 kr. Also the room formula uses the previous year-end value, not the cumulative deposits, so a good year shrinks next year's room.
- **Severity:** low-medium (live config stale)
- **Effort:** S
- **Unlocks / depends on:** the live `.env` is outside the scout's scope; confirm in the audit.
- **Confidence:** high on the rule, high that the map states the mismatch.

### F40. The 500,000 DKK proposal is not law; do not plan around it before spring 2027
- **What:** The government proposes raising the ceiling to 500,000 kr, phased over three years, with the ceiling measured against original deposits instead of current value.
- **Where:** https://aktietip.dk/?p=9791 (read, Secondary); https://www.danskerhverv.dk/presse-og-nyheder/nyheder/2026/juni/regeringen-vil-have-loftet-pa-aktiesparekontoen-til-500.000-kroner-det-skal-du-vide (search summary)
- **Evidence:** Finance bill for 2027 presented 9 October 2026; a bill expected in the Folketing in February 2027; no effective date; "until then the current 174,200 kr ceiling applies"; the 17% lager rate is unchanged; the three annual steps were not published. No official government or Folketing source found.
- **Why it matters:** The deployment logic's "limit" and contribution-room checks should keep reading a config value, not hard-code 500k. Under the proposed original-deposit base, gains no longer use up room, which would change how the system should value an unfilled ASK.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** recheck after February 2027.
- **Confidence:** medium (single secondary source for the details).

### F41. Taxed on mark-to-market at 17%, so rotation inside the ASK has no tax cost and losses carry forward only while the account stays open
- **What:** The ASK is taxed on the year's change in value, not on realised gains.
- **Where:** https://skat.dk/borger/aktier-og-andre-vaerdipapirer/aktiesparekonto (read)
- **Evidence:** "Du beskattes med 17 % af afkastet"; lager principle; the bank calculates and withdraws the tax; a negative result carries forward against the earliest future positive year, and is lost if the account is closed with unused negative tax; eligible assets are shares on regulated markets or MTFs and certain share-based funds, no mention of derivatives. Whether currency gains are taxed in DKK terms is not addressed on the page; a pension-return guidance snippet says currency gains and losses are included (not ASK-specific), so unverified for the ASK.
- **Why it matters:** Selling a winner or loser inside the ASK triggers no extra tax, so "hold for deferral" has no tax logic here, and tax-driven reluctance to sell should not appear in the rules (ruleset 3.0.0 sells only on breaker or rotation, map). Because tax is on market value, unrealised USD/DKK gains are taxed whether or not the stock moved. Closing the account loses a carried loss, which argues against closing after a bad year.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** a DKK return series (F44).
- **Confidence:** high on the rule, low on the FX treatment.

### F42. US dividend withholding: 15% treaty rate, partial relief up to 17%, not re-depositable, details unsettled
- **What:** US dividends carry 30% withholding by default and 15% under the Denmark-US treaty with a valid form; inside the ASK Danish tax is reduced for the foreign tax paid, capped at 17%.
- **Where:** https://www.ey.com/da_dk/insights/tax/aktiesparekontoen-hvad-skal-man-vaere-opmaerksom-paa (read; the article quotes a deposit limit of about 100,000 kr so it predates 2026); https://www.nordnet.dk/faq/5607-hvordan-fungerer-udbytteskat-pa-en-aktiesparekonto (read); https://www.ey.com/da_dk/insights/tax/reduktion-af-kildeskat-pa-udbytter-fra-usa (search summary)
- **Evidence:** EY: with 15% foreign tax paid, the Danish ASK tax is 2% effective; relief capped at 17%; "De 15% som er betalt i udlandet kan altså ikke genindbetales"; whether foreign tax can be redeposited is "uafklaret" and some banks refuse. Nordnet: withholds 15% on US shares traded on a US exchange and "applies the double-taxation treaty provided the share trades on the local market". Treaty article 10: source state limited to 15%.
- **Why it matters:** For a US dividend payer, total tax on the dividend is about 17% (15% US plus 2% Danish) and the 15% is cash that never reaches the account, so it lowers the base and cannot be topped up above the ceiling. The map shows `ASK_DIVIDEND_TAX_RATE = 0.17` in legacy `portfolio.py` and an empty `income_events` table; confirm whether the 15% US withholding is modelled separately. High-yield names also cost compounding relative to buyback-heavy names.
- **Severity:** low-medium
- **Effort:** S
- **Unlocks / depends on:** `income_events` wiring.
- **Confidence:** medium; the EY page is dated and Skat's own treatment not read.

### F43. Currency hedging for a DKK investor in US stocks: unhedged USD is not obviously a risk, and hedging costs the rate gap and is awkward in an ASK
- **What:** For a risk-minimising global equity investor, USD, EUR and CHF were the hedging currencies; DKK is pegged to EUR so a DKK investor is close to a euro investor.
- **Where:** https://www.nber.org/papers/w13088 (Campbell, Serfaty-de Medeiros, Viceira, "Global Currency Hedging", JF 2010, abstract via search); https://cbonds.pl/indexes/215421/ (USD/DKK swap points, Secondary); https://think.ing.com/downloads/pdf/article/little-evidence-of-increase-in-dollar-hedge-ratios (ING, 7 April 2026, search summary)
- **Evidence:** CSV: over 1975-2005, USD, EUR and CHF moved against world equity markets, the risk-minimising equity investor shorts AUD, CAD, JPY and GBP and is long USD, EUR and CHF; works for horizons of one month to one year; later 15 years show a slightly weaker USD role. Cbonds USD/DKK swap points, last dated 2026-06-03: 1-month -107.45, 3-month -304.09, 1-year -1,094.5 (units unverified; the sign reflects the USD-DKK rate gap). ING: Danish buy-side investors cut USD hedge ratios in January-February 2026. The ASK's eligible-asset list is shares and share funds only (F41), so a forward hedge cannot sit inside it; a hedge outside would be separately taxed (unverified).
- **Why it matters:** USD exposure of an 8-stock US book is a large unhedged position (the Sonnet prompt asks about currency exposure for a DKK investor, map). The evidence does not say hedge; it says unhedged USD has dampened equity drawdowns historically, with a carry cost to hedge. The decision is not testable inside the ASK. What is missing is measurement: the briefing has no DKK equity curve and no FX attribution (map, Plan items).
- **Severity:** low (policy), medium (missing measurement)
- **Effort:** S
- **Unlocks / depends on:** F31 and F44.
- **Confidence:** medium.

### F44. DKK return and FX attribution are missing from the briefing
- **What:** `portfolio_snapshots.equity_dkk` is stored daily but the briefing shows no DKK curve and no split into local-currency return versus USD/DKK.
- **Where:** map, Plan items ("DKK / FX bridge: partial"); method: any standard currency return decomposition, e.g. the log-return identity, data from F31.
- **Evidence:** map line: "no FX attribution of returns, no DKK equity curve in briefing".
- **Why it matters:** The user's real objective is DKK after Danish tax; a stock return of +8% in USD with USD/DKK -6% is +1.5% in DKK before 17% tax. Without attribution the user cannot see which part of a result was stock selection.
- **Severity:** medium
- **Effort:** S
- **Unlocks / depends on:** the F31 rate series.
- **Confidence:** high that it is missing (per map); I did not read the briefing code.

---
## Improvement ideas not tied to a weakness

### F45. Add a numeric baseline and anonymised replay before spending more on Sonnet
- **What:** Combine F14 and F18: a logistic or gradient-boosted model on the same fundamentals, and an anonymised Haiku/Sonnet run, scored on forward returns only from the dates after the models' cutoffs.
- **Where:** https://arxiv.org/html/2407.17866v2 ; https://arxiv.org/abs/2309.17322
- **Evidence:** GPT-4 60.35% vs ANN 60.45% on earnings direction (F18); anonymisation outperformed in-sample (F14).
- **Why it matters:** Sonnet is the main paid line (about $2.45 across 19 weekly runs, map) and its value has not been shown to exceed a free model.
- **Severity:** n/a
- **Effort:** M
- **Unlocks / depends on:** a growing forward sample; PIT features.
- **Confidence:** medium.

### F46. Write down what the sample can show, and publish the luck range in the briefing
- **What:** State expected tracking error, years needed to detect 2% alpha, and a Monte Carlo band of 8-stock outcomes.
- **Where:** F11 sources; QuantStats Monte Carlo https://github.com/ranaroussi/quantstats
- **Evidence:** residual tracking error 9-12% (illustrative arithmetic, F11).
- **Why it matters:** Prevents over-reading a good or bad quarter.
- **Severity:** n/a
- **Effort:** S
- **Unlocks / depends on:** none.
- **Confidence:** high.

### F47. Replace yfinance fundamentals with EDGAR frames as the primary source, keep yfinance as a cross-check
- **What:** See F26 and F30.
- **Where:** https://www.sec.gov/edgar/sec-api-documentation
- **Evidence:** 10 requests/second, no key, nightly bulk files.
- **Why it matters:** Removes a ToS-grey single point of failure and gives PIT by construction.
- **Severity:** n/a
- **Effort:** L
- **Unlocks / depends on:** an XBRL concept map and TTM logic; disagreement report between sources.
- **Confidence:** medium.

---
## Ideas worth parking
- Chronologically consistent open models (F17) as a clean text-signal backtester, if the news monitor is ever to be validated seriously.
- One month of Sharadar or Norgate to rebuild the quality backtest with delistings (F29), once the price is confirmed.
- A hash-chained decision ledger and a kill switch for auto-trading (F33).
- Industry-level momentum or valuation overlay (F23) as a monthly cap on sector crowding.
- Hedged USD share classes or an outside-ASK hedge, if FX volatility ever dominates tracking error (F43).
- Re-test the Graham number and each fair-value leg by leave-one-out on PIT data (F8, F9); parked because it needs survivorship-clean data to mean anything.
- Revisit the ASK plan after the Danish bill in February 2027 (F40).

## What I could not verify
- Whether financial firms are excluded from Fama-French RMW construction (page did not say).
- Sharadar pricing; Norgate pricing (conflicting third-party figures); FMP official pricing page (HTTP 403, figures from third parties); Polygon/Massive plan table and rename date (sources disagree); EODHD delisted-fundamentals coverage (single forum post).
- NAREIT's own FFO definition (only secondary pages read); whether FFO/AFFO or bank-specific metrics exist as us-gaap XBRL tags.
- The exact Look-Ahead-Bench numbers and the periods (abstract has none); the TradingAgents test window (sources disagree); the Alpha Arena figures (single Russian-language news source).
- Whether the 15% US withholding is computed inside or outside the ASK tax base, whether it can be redeposited, and how USD/DKK gains are taxed in the ASK; the EY article predates 2026.
- USD/DKK swap point units and current level (Cbonds data dated 2026-06-03).
- The training cutoffs of claude-sonnet-5-5 and claude-haiku-4-5 (not looked up).
- Shiller CAPE data (yale.edu fetch refused the connection) and FRED API terms (landing page only); Saxo OpenAPI and Nordnet current onboarding status beyond one search snippet.
- Any peer-reviewed 2010-2024 replication of insider-purchase returns; any study of repeat-run inconsistency of LLM stock ratings; any peer-reviewed evidence that a decision journal improves returns.
- The live content of repo files (I did not read code). All statements about the system rest on SYSTEM_MAP.md.
