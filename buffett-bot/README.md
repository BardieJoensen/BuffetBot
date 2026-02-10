# Buffett Bot

A value investing research assistant that combines LLM qualitative analysis with deterministic quantitative data to generate monthly investment briefings.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        LAYER 1: LLM                             │
│                   (Claude API - Qualitative)                    │
│  • Summarize 10-K reports                                       │
│  • Assess moat and management quality                           │
│  • Monitor news for red flags                                   │
│  • Generate briefing narratives                                 │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                   LAYER 2: Data Services                        │
│               (APIs + External Tools - Quantitative)            │
│  • yfinance: Screening, fundamentals, ratios                    │
│  • SEC EDGAR: 10-K filings (primary source)                     │
│  • Seeking Alpha: Earnings transcripts (scraped/free)           │
│  • GuruFocus/SimplyWallSt: Fair value estimates                 │
│  • Finnhub: News, backup fundamentals                           │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                      LAYER 3: Output                            │
│  • Monthly briefing document                                    │
│  • Watchlist database                                           │
│  • Portfolio tracker                                            │
│  • Grafana dashboard (optional)                                 │
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
│   ├── screener.py          # Score-based stock screening
│   ├── valuation.py         # Aggregate fair value estimates
│   ├── analyzer.py          # LLM qualitative analysis (Sonnet + Haiku + Opus second opinion)
│   ├── briefing.py          # Generate monthly reports
│   ├── portfolio.py         # Portfolio tracking and thesis management
│   ├── bubble_detector.py   # Market bubble/froth detection
│   ├── notifications.py     # Email/Telegram/ntfy.sh alerts
│   ├── paper_trader.py      # Alpaca paper trading integration
│   └── benchmark.py         # Benchmark comparison (SPY default)
├── config/
│   └── screening_criteria.yaml
├── data/
│   ├── watchlist_cache.json # Cached watchlist
│   ├── analyses/            # Stored company analyses
│   ├── briefings/           # Generated briefing reports
│   └── cache/               # yfinance data cache
├── scripts/
│   ├── run_monthly_briefing.py  # Manual pipeline
│   └── scheduler.py             # Automated job scheduling
├── docs/
│   └── SETUP.md
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

## Data Flow

### Weekly Screen (automated, free)
1. `universe.py` fetches ~800 stocks dynamically from Finviz (fallback: Wikipedia S&P 600, then curated list)
2. `screener.py` filters with yfinance data by market cap, P/E, debt, ROE
3. Returns ~20-50 candidates, stored in watchlist cache

### Monthly Deep Dive (manual, costs ~$0.25-0.50)
1. Haiku pre-screens top 30 candidates (cheap filter before expensive Sonnet)
2. Top candidates go to Sonnet for deep qualitative analysis (moat, management, risks)
3. Optionally, Opus provides a contrarian second opinion on top 5 BUY picks
4. `briefing.py` combines quant + qual into report (text, HTML, JSON)
5. Results compared against benchmark (SPY by default)
6. Notifications sent via email/Discord/Telegram

Batch API (50% discount) and prompt caching are enabled by default to reduce costs.

### Continuous Monitoring (automated)
1. `monitor.py` checks news daily for portfolio holdings
2. LLM flags potential thesis-breaking events
3. Alerts you if action needed

### Your Decision (manual)
1. Read monthly briefing
2. Decide: buy, pass, or watchlist
3. Execute trade yourself via broker

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

**Budget alternative:** Use analyst price targets from Finnhub (free) as a rough fair value proxy, or calculate simple valuation multiples yourself.

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

Results are saved to `./data/briefings/` and sent via your configured notifications (email, Discord, etc).

## Screening Criteria (Default)

Stocks pass hard filters first, then are ranked by a weighted score. Higher-scoring stocks rise to the top.

### Hard Filters (must pass)

| Filter | Value | Rationale |
|--------|-------|-----------|
| Market Cap | $300M - $500B | Avoids micro-caps and mega-caps |
| Price | > $5 | Avoids penny stocks |
| P/E Ratio | > 0 | Excludes loss-making companies |
| Quote Type | EQUITY only | Excludes ETFs, CEFs |

### Scored Metrics (weighted ranking)

| Metric | Ideal | Threshold | Weight | Rationale |
|--------|-------|-----------|--------|-----------|
| P/E Ratio | 12 | max 30 | 2.0 | Not overvalued |
| ROE | 20% | min 5% | 2.0 | Efficient capital use |
| FCF Yield | 8% | min 2% | 2.0 | Cash generation vs price |
| Debt/Equity | 0.0 | max 1.0 | 1.5 | Conservative balance sheet |
| Operating Margin | 25% | min 10% | 1.5 | Pricing power |
| Earnings Quality | 1.2x | min 0.5x | 1.5 | FCF backs reported earnings |
| Revenue Growth | 15% | min 0% | 1.0 | Not shrinking |
| Current Ratio | 2.0 | min 1.0 | 1.0 | Short-term liquidity |
| Payout Ratio | 35% | 0-80% | 0.5 | Disciplined capital return |

Customize in `config/screening_criteria.yaml`

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | (required) | Claude API key |
| `USE_BATCH_API` | `true` | Use Batch API for 50% cost reduction |
| `USE_OPUS_SECOND_OPINION` | `false` | Run Opus contrarian review on top BUY picks |
| `BENCHMARK_SYMBOL` | `SPY` | Benchmark to compare picks against |
| `PORTFOLIO_VALUE` | `50000` | Portfolio size for position sizing |

See `.env.example` for the full list.

## Important Limitations

1. **LLMs make math errors** - All calculations come from APIs, not Claude
2. **No price predictions** - Fair values are estimates, not forecasts
3. **You decide** - Bot provides research, not financial advice
4. **Paper trade first** - Validate the system before real money
