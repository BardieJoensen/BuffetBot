# Buffett Bot - Setup Guide

## Quick Start (5 minutes)

### 1. Get API Keys

You need accounts (free tier is fine) from:

| Service | Sign Up | What You Get |
|---------|---------|--------------|
| Anthropic Claude | [console.anthropic.com](https://console.anthropic.com/) | Pay per use (~$10-15/mo) |
| Finnhub | [finnhub.io](https://finnhub.io/) | 60 calls/min |
| Alpaca (optional) | [alpaca.markets](https://alpaca.markets/) | Paper trading |

### 2. Configure Environment

```bash
# Clone or copy the project to your server
cd /path/to/buffett-bot

# Copy example env file
cp .env.example .env

# Edit with your API keys
nano .env
```

Fill in your keys:
```
ANTHROPIC_API_KEY=your_key_here
FINNHUB_API_KEY=your_key_here
```

### 3. Run with Docker (Recommended)

```bash
# Build and start
docker-compose up -d

# Check logs
docker logs buffett-bot

# Run first briefing manually
docker exec buffett-bot python -m scripts.run_monthly_briefing
```

### 4. Run Without Docker

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Run manually
python scripts/run_monthly_briefing.py
```

---

## Understanding The Output

After running, check `./data/briefings/` for your reports:

- `briefing_YYYY_MM.txt` - Human-readable tiered watchlist report
- `briefing_YYYY_MM.html` - Styled HTML version
- `briefing_YYYY_MM.json` - Machine-readable data (schema v2)

### Sample Briefing Output

```
======================================================================
INVESTMENT BRIEFING - February 2026
======================================================================

MARKET REGIME: Fair Value (Confidence: HIGH)
Deploy capital normally — focus on quality businesses at reasonable prices.

## EXECUTIVE SUMMARY

Stocks Analyzed: 10
Tier 1 (Buy Zone):  2
Tier 2 (Watch):     4
Tier 3 (Monitor):   3

## WATCHLIST MOVEMENTS
  NEW  → MSFT entered Tier 2 (wide moat, above target entry)
  ↑ UP → V moved from Tier 2 → Tier 1 (price dropped to target)

## APPROACHING TARGET
  ⚡ COST is 8.2% above target entry ($785.00) — close to buy zone

----------------------------------------------------------------------
## TIER 1: BUY ZONE
----------------------------------------------------------------------

### V: Visa Inc
Tier: 1 (Buy Zone) | Quality Score: 87.2 | Confidence: 0.92

QUALITATIVE ASSESSMENT:
┌──────────────────────────────────────────────────────────────┐
│ Moat:       WIDE         │ Conviction: HIGH               │
│ Management: EXCELLENT    │ Durability: HIGH               │
│ Currency:   STRONG       │                                │
└──────────────────────────────────────────────────────────────┘

STAGED ENTRY:
  Tranche 1: $265.00 (1/3 position)
  Tranche 2: $258.00 (1/3 position)
  Tranche 3: $251.00 (1/3 position)

[... more details ...]

----------------------------------------------------------------------
## TIER 2: WATCH
----------------------------------------------------------------------

### MSFT: Microsoft Corporation
Tier: 2 (Watch) | Quality Score: 91.5 | Confidence: 0.95
Gap to target: +15.3% above entry price
[... summary ...]
```

---

## Customizing Screening Criteria

Edit `config/screening_criteria.yaml`:

```yaml
screening:
  min_market_cap: 300000000    # $300M - avoid micro-caps
  max_market_cap: 500000000000 # $500B - include large-caps

scoring:
  # Quality metrics (high weight)
  roic:
    ideal: 0.20
    min: 0.05
    weight: 2.5
  roe_consistency:
    ideal: 0.02
    max: 0.15
    weight: 2.0

  # Valuation (de-weighted — quality matters more)
  pe_ratio:
    ideal: 15
    max: 60
    weight: 0.8

sector_overrides:
  Real Estate:
    debt_equity: { ideal: 0.5, max: 3.0, weight: 0.3 }
  Financial Services:
    debt_equity: { ideal: 0.3, max: 2.0, weight: 0.3 }
```

The v2.0 scoring philosophy: quality metrics (ROIC, consistency, durability) are weighted 2-3x higher than valuation metrics. This lets wonderful businesses like Costco or Visa appear even at premium P/E ratios.

---

## Scheduling Automated Runs

### Option A: Use Built-in Scheduler

```bash
# Start scheduler container
docker-compose up -d scheduler

# Check it's running
docker logs buffett-bot-scheduler
```

Default schedule:
- **Weekly (Friday 17:00):** Update watchlist with fresh screen (free — yfinance only)
- **Monthly (1st at 09:00):** Full briefing with LLM analysis
- **Daily (08:00):** Check watchlist prices for margin-of-safety alerts

### Option B: Use Cron (More Control)

```bash
# Edit crontab
crontab -e

# Add these lines:
# Weekly screen on Fridays at 5pm
0 17 * * 5 cd /path/to/buffett-bot && docker compose run --rm buffett-bot python -c "from scripts.scheduler import weekly_screen; weekly_screen()"

# Monthly briefing on the 1st at 9am
0 9 1 * * cd /path/to/buffett-bot && docker compose run --rm buffett-bot python -m scripts.run_monthly_briefing
```

---

## Adding Your Portfolio (For Monitoring)

Create `./data/portfolio.json`:

```json
{
  "positions": [
    {
      "symbol": "AAPL",
      "shares": 10,
      "cost_basis": 150.00,
      "purchase_date": "2025-06-15",
      "thesis": "Strong ecosystem moat, services growth"
    },
    {
      "symbol": "MSFT",
      "shares": 5,
      "cost_basis": 380.00,
      "purchase_date": "2025-08-01",
      "thesis": "Cloud dominance, AI integration"
    }
  ]
}
```

If Alpaca paper trading is configured, the bot will pull positions from Alpaca automatically instead of this file.

---

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
| `MARGIN_OF_SAFETY_PCT` | `25` | Minimum margin of safety for Tier 1 |
| `TIER1_PROXIMITY_ALERT_PCT` | `10` | Alert when Tier 2 stock is within this % of target |

See `.env.example` for the full list including notification and Alpaca settings.

---

## Troubleshooting

### "API rate limit exceeded"
- yfinance may throttle requests if too many are made quickly
- Solution: Reduce `MAX_DEEP_ANALYSES` in `.env` or wait and try again

### "No stocks passed screening"
- v2.0 screening is quality-first, so this is less likely than before
- Check that yfinance is returning data (network connectivity)
- Try clearing the universe cache: `rm data/cache/stock_universe.json`

### "LLM analysis failed"
- Check your Anthropic API key
- Check you have credits in your account
- Claude API has its own rate limits

### Container won't start
```bash
# Check logs
docker-compose logs buffett-bot

# Rebuild if needed
docker-compose build --no-cache
docker-compose up -d
```

---

## Cost Estimates

| Component | Monthly Cost |
|-----------|--------------|
| yfinance | $0 (free, no key) |
| Finnhub API | $0 (free tier) |
| Claude API | $5-15 (depends on analyses) |
| Alpaca | $0 (paper trading) |
| **Total** | **~$5-15/month** |

Batch API (enabled by default) reduces Claude costs by ~50%.

---

## Next Steps

1. Run your first briefing
2. Review the tiered output and understand Tier 1/2/3 classification
3. Paper trade Tier 1 picks using staged entry for 3-6 months
4. Track performance vs. benchmark (SPY comparison included in briefing)
5. Adjust quality weights based on what you learn
6. Consider real money only after validating the system
