# Buffett Bot

A quality-first value investing research assistant that combines LLM qualitative analysis with deterministic quantitative data to generate monthly tiered watchlist briefings.

**Philosophy (v2.0):** Find wonderful businesses, track them patiently, deploy capital when price meets patience.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        LAYER 1: LLM                             │
│                   (Claude API - Qualitative)                    │
│  • AnalysisV2: moat, management, durability, currency          │
│  • Three-tier: Haiku (pre-screen) → Sonnet (deep) → Opus      │
│  • Prompt caching + Batch API for cost savings                  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                   LAYER 2: Data Services                        │
│               (APIs + External Tools - Quantitative)            │
│  • yfinance: Quality screening, fundamentals, trend metrics     │
│  • SEC EDGAR: 10-K filings (primary source)                     │
│  • Finnhub: News, backup fundamentals, price targets            │
│  • GuruFocus/SimplyWallSt: Fair value estimates                 │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                   LAYER 3: Tier Engine                          │
│  • Market regime classification (Euphoria → Crisis)             │
│  • Tiered watchlist: Tier 1 (buy) / Tier 2 (watch) / Tier 3    │
│  • Staged entry suggestions (3 tranches)                        │
│  • Movement tracking between runs                               │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                      LAYER 4: Output                            │
│  • Tiered briefing (text, HTML, JSON)                           │
│  • ASK portfolio tracker (5-8 concentrated positions)           │
│  • Forward validation (quality-return correlation)              │
│  • Notifications (email/Discord/Telegram/ntfy.sh)              │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
                    [ YOU DECIDE ]
```

## Project Structure

```
buffett-bot/
├── src/
│   ├── __init__.py
│   ├── universe.py          # Dynamic stock universe (Finviz/Wikipedia/fallback)
│   ├── screener.py          # Quality-first screening with sector-aware scoring
│   ├── valuation.py         # Aggregate fair value estimates
│   ├── analyzer.py          # LLM qualitative analysis (Haiku/Sonnet/Opus)
│   ├── tier_engine.py       # Tiered watchlist assignment & staged entry
│   ├── briefing.py          # Generate tiered investment reports
│   ├── portfolio.py         # ASK portfolio management & concentration controls
│   ├── bubble_detector.py   # Market regime classification (Euphoria→Crisis)
│   ├── notifications.py     # Email/Telegram/ntfy.sh/Discord alerts
│   ├── paper_trader.py      # Alpaca paper trading integration
│   ├── benchmark.py         # Benchmark comparison (SPY default)
│   └── backtest.py          # Forward validation & quality-return correlation
├── config/
│   └── screening_criteria.yaml
├── data/
│   ├── watchlist_cache.json  # Cached watchlist
│   ├── analyses/             # Stored company analyses
│   ├── briefings/            # Generated briefing reports
│   ├── backtest/             # Watchlist snapshots for forward tracking
│   ├── benchmark/            # Benchmark data cache
│   └── cache/                # yfinance data cache
├── scripts/
│   ├── run_monthly_briefing.py  # Manual pipeline (10-step)
│   └── scheduler.py             # Automated job scheduling
├── docs/
│   └── SETUP.md
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

## Data Flow

### Monthly Pipeline (10 steps)
1. **Market regime** — classify current market (Euphoria/Overvalued/Fair/Correction/Crisis)
2. **Quality screen** — score ~800 stocks on ROIC, consistency, margins, sector-adjusted
3. **Bubble check** — flag individual stocks showing bubble characteristics
4. **Haiku pre-screen** — cheap LLM filter on top candidates
5. **Sonnet deep analysis** — AnalysisV2 (moat, management, durability, currency, fair value)
6. **Valuations** — aggregate fair value estimates for all analyzed stocks
7. **Tier engine** — assign Tier 1/2/3 based on quality + price vs target entry
8. **Portfolio check** — concentration status, ASK contributions, gap analysis
9. **Opus second opinion** — contrarian review on Tier 1 picks (optional)
10. **Tiered briefing** — generate text/HTML/JSON report with movement log

Batch API (50% discount) and prompt caching are enabled by default to reduce costs.

### Continuous Monitoring (automated)
1. News monitoring checks daily for portfolio holdings
2. LLM flags potential thesis-breaking events
3. Regime-shift and approaching-target alerts sent via notifications

### Your Decision (manual)
1. Read monthly briefing
2. Review Tier 1 picks and staged entry suggestions
3. Execute trades yourself via broker

## External Services Required

| Service | Purpose | Cost | Sign Up |
|---------|---------|------|---------|
| yfinance | Screening + fundamentals | Free (no key) | pypi.org/project/yfinance |
| Claude API | Qualitative analysis | ~$10-15/mo | console.anthropic.com |
| Finnhub | News + backup data | Free (60/min) | finnhub.io |
| SEC EDGAR | 10-K filings | Free | No signup needed |
| Alpaca | Paper trading | Free | alpaca.markets |

### Optional (for fair value estimates)
- GuruFocus: $450/year (has free limited access)
- Simply Wall St: Free tier available
- Morningstar: Requires subscription

**Budget alternative:** Use analyst price targets from Finnhub (free) as a rough fair value proxy.

## Quick Start

```bash
# 1. Copy example env and add your Anthropic API key
cp .env.example .env
nano .env

# 2. Pull the latest image
docker compose pull

# 3. Start the scheduler (runs free weekly screens automatically)
docker compose up -d scheduler

# 4. Run a full briefing manually (costs ~$0.50 in Claude API)
docker compose run --rm buffett-bot
```

After updating code, pull the new image and restart:
```bash
docker compose pull
docker compose up -d scheduler
```

Results are saved to `./data/briefings/` and sent via your configured notifications.

## Screening Criteria (v2.0)

Stocks pass hard filters first, then are ranked by a quality-weighted score. Higher-scoring stocks rise to the top. Valuation is de-weighted — wonderful businesses at fair prices are preferred over mediocre businesses at discounts.

### Hard Filters (must pass)

| Filter | Value | Rationale |
|--------|-------|-----------|
| Market Cap | $300M - $500B | Avoids micro-caps and mega-caps |
| Price | > $5 | Avoids penny stocks |
| P/E Ratio | > 0 | Excludes loss-making companies |
| Quote Type | EQUITY only | Excludes ETFs, CEFs |

### Quality Metrics (high weight)

| Metric | Ideal | Threshold | Weight | Rationale |
|--------|-------|-----------|--------|-----------|
| ROIC | 20% | min 5% | 2.5 | Capital efficiency — the core quality signal |
| ROE | 20% | min 5% | 2.0 | Return on equity |
| ROE Consistency | 2% std | max 15% std | 2.0 | Stable returns = durable moat |
| Earnings Consistency | 4/4 yrs | min 1/4 yrs | 2.0 | Reliable growth trajectory |
| Operating Margin | 25% | min 10% | 1.5 | Pricing power |
| Margin Stability | 2% std | max 12% std | 1.5 | Consistent profitability |
| Earnings Quality | 1.2x | min 0.5x | 1.5 | FCF backs reported earnings |
| FCF Yield | 8% | min 2% | 1.5 | Cash generation vs price |
| Revenue CAGR | 12% | min 0% | 1.5 | Multi-year growth trend |
| FCF Consistency | 0.10 std | max 0.50 std | 1.5 | Stable cash conversion |

### Valuation Metrics (de-weighted)

| Metric | Ideal | Threshold | Weight | Rationale |
|--------|-------|-----------|--------|-----------|
| P/E Ratio | 15 | max 60 | 0.8 | Allows quality compounders |
| Debt/Equity | 0.0 | max 1.5 | 0.8 | Conservative balance sheet |
| Revenue Growth | 15% | min 0% | 0.8 | Not shrinking |

### Sector Overrides

Real Estate, Financial Services, Utilities, and Energy sectors have adjusted scoring (e.g., REITs get higher debt tolerance, financials skip FCF metrics).

Customize in `config/screening_criteria.yaml`

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | (required) | Claude API key |
| `USE_BATCH_API` | `true` | Use Batch API for 50% cost reduction |
| `USE_OPUS_SECOND_OPINION` | `false` | Run Opus contrarian review on Tier 1 picks |
| `BENCHMARK_SYMBOL` | `SPY` | Benchmark to compare picks against |
| `PORTFOLIO_VALUE` | `50000` | Portfolio size for position sizing |
| `MAX_POSITIONS` | `8` | Maximum concentrated positions (ASK) |
| `ASK_CONTRIBUTION_LIMIT` | `135900` | Annual ASK contribution limit (DKK) |
| `MARGIN_OF_SAFETY_PCT` | `25` | Minimum margin of safety % for Tier 1 |
| `TIER1_PROXIMITY_ALERT_PCT` | `10` | Alert when Tier 2 stock is within this % of target |

See `.env.example` for the full list including notification and Alpaca settings.

## Important Limitations

1. **LLMs make math errors** - All calculations come from APIs, not Claude
2. **No price predictions** - Fair values are estimates, not forecasts
3. **You decide** - Bot provides research, not financial advice
4. **Paper trade first** - Validate the system before real money
5. **Quality != guaranteed returns** - High quality scores indicate durable businesses, not short-term price targets
