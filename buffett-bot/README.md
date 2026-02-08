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
│   ├── analyzer.py          # LLM qualitative analysis (Sonnet + Haiku)
│   ├── briefing.py          # Generate monthly reports
│   ├── portfolio.py         # Portfolio tracking and thesis management
│   ├── bubble_detector.py   # Market bubble/froth detection
│   ├── notifications.py     # Email/Telegram/ntfy.sh alerts
│   └── paper_trader.py      # Alpaca paper trading integration
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

### Monthly Deep Dive (manual, costs ~$0.50)
1. Top candidates by margin of safety go to `analyzer.py`
2. Claude reads company summary and provides qualitative assessment (moat, management, risks)
3. `briefing.py` combines quant + qual into report
4. Notifications sent via email/Discord/Telegram

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

Buffett-style value filter:

| Metric | Criteria | Rationale |
|--------|----------|-----------|
| Market Cap | $300M - $500B | Avoids micro-caps |
| P/E Ratio | < 20 | Not overvalued |
| Debt/Equity | < 0.5 | Conservative balance sheet |
| ROE | > 12% | Efficient capital use |
| Revenue Growth | > 5% YoY | Not shrinking |
| Free Cash Flow | Positive (3yr) | Actually makes money |
| Dividend | Optional | Shareholder returns |

Customize in `config/screening_criteria.yaml`

## Important Limitations

1. **LLMs make math errors** - All calculations come from APIs, not Claude
2. **No price predictions** - Fair values are estimates, not forecasts
3. **You decide** - Bot provides research, not financial advice
4. **Paper trade first** - Validate the system before real money
