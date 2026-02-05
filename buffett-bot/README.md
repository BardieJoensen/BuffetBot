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
│  • FMP API: Screening, fundamentals, ratios                     │
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
│   ├── screener.py          # Stock screening logic
│   ├── fundamentals.py      # Fetch financial data
│   ├── valuation.py         # Aggregate fair value estimates
│   ├── analyzer.py          # LLM qualitative analysis
│   ├── monitor.py           # News and thesis monitoring
│   ├── briefing.py          # Generate monthly reports
│   └── database.py          # Watchlist and portfolio storage
├── config/
│   ├── config.yaml          # Main configuration
│   ├── prompts/
│   │   ├── moat_analysis.txt
│   │   ├── management_analysis.txt
│   │   ├── news_monitor.txt
│   │   └── briefing_summary.txt
│   └── screening_criteria.yaml
├── data/
│   ├── watchlist.json       # Current watchlist
│   ├── portfolio.json       # Your positions
│   └── analyses/            # Stored company analyses
├── scripts/
│   ├── run_weekly_screen.py
│   ├── run_monthly_briefing.py
│   └── check_news.py
├── docs/
│   └── SETUP.md
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

## Data Flow

### Weekly Screen (automated)
1. `screener.py` calls FMP API with value criteria
2. Returns ~30-50 candidates passing quantitative filters
3. Stores in `watchlist.json` with timestamp

### Weekly Valuation Check (automated)
1. For each watchlist company:
   - `fundamentals.py` fetches current ratios
   - `valuation.py` fetches fair value from external sources
   - Calculates margin of safety
2. Updates watchlist with current valuations

### Monthly Deep Dive (automated)
1. Top candidates by margin of safety go to `analyzer.py`
2. LLM reads 10-K summary, earnings transcript
3. Generates qualitative assessment (moat, management, risks)
4. `briefing.py` combines quant + qual into report

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
| FMP | Screening + fundamentals | Free (250/day) | financialmodelingprep.com |
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

1. Copy `.env.example` to `.env` and add your API keys
2. Run `docker-compose up -d`
3. Execute first screen: `docker exec buffett-bot python scripts/run_weekly_screen.py`
4. Check results in `data/watchlist.json`

## Screening Criteria (Default)

Buffett-style value filter:

| Metric | Criteria | Rationale |
|--------|----------|-----------|
| Market Cap | $300M - $10B | Small/mid-cap, under-followed |
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
