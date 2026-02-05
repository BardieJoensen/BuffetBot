# Buffett Bot - Setup Guide

## Quick Start (5 minutes)

### 1. Get API Keys

You need accounts (free tier is fine) from:

| Service | Sign Up | What You Get |
|---------|---------|--------------|
| Financial Modeling Prep | [financialmodelingprep.com](https://financialmodelingprep.com/developer) | 250 API calls/day |
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
FMP_API_KEY=your_key_here
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

- `briefing_YYYY_MM.txt` - Human-readable report
- `briefing_YYYY_MM.json` - Machine-readable data

### Sample Briefing Output

```
======================================================================
INVESTMENT BRIEFING - February 2026
======================================================================

## EXECUTIVE SUMMARY

Stocks Analyzed: 8
Buy Candidates:  2
Watchlist:       3

Top Opportunities (by margin of safety):
  • ACME: 29.1% margin of safety, HIGH conviction
  • XYZ:  22.5% margin of safety, MEDIUM conviction

----------------------------------------------------------------------
### ACME: Acme Corporation
Recommendation: 🟢 BUY

QUALITATIVE ASSESSMENT:
┌──────────────────────────────────────────────────────────────┐
│ Moat:       WIDE         │ Conviction: HIGH       │
│ Management: EXCELLENT    │                        │
└──────────────────────────────────────────────────────────────┘

VALUATION ESTIMATES:
┌──────────────────────────────────────────────────────────────┐
│ FMP Analyst Consensus         $58.00              │
│ FMP DCF Model                 $62.00              │
│ P/E Multiple (Conservative)   $55.00              │
├──────────────────────────────────────────────────────────────┤
│ AVERAGE FAIR VALUE:           $58.33              │
│ MARGIN OF SAFETY:             29.1%               │
└──────────────────────────────────────────────────────────────┘

[... more details ...]
```

---

## Customizing Screening Criteria

Edit the criteria in `src/screener.py`:

```python
@dataclass
class ScreeningCriteria:
    min_market_cap: float = 300_000_000      # $300M minimum
    max_market_cap: float = 10_000_000_000   # $10B maximum
    max_pe_ratio: float = 20.0               # Not overvalued
    max_debt_equity: float = 0.5             # Conservative debt
    min_roe: float = 0.12                    # 12% return on equity
    min_revenue_growth: float = 0.05         # 5% growth
```

Adjust these based on your risk tolerance and market conditions.

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
- **Weekly (Sunday 18:00):** Update watchlist with fresh screen
- **Monthly (1st at 19:00):** Full briefing with LLM analysis
- **Daily (08:00):** Check news for portfolio holdings

### Option B: Use Cron (More Control)

```bash
# Edit crontab
crontab -e

# Add these lines:
# Weekly screen on Sundays at 6pm
0 18 * * 0 cd /path/to/buffett-bot && docker exec buffett-bot python -c "from scripts.scheduler import weekly_screen; weekly_screen()"

# Monthly briefing on the 1st at 7pm
0 19 1 * * cd /path/to/buffett-bot && docker exec buffett-bot python -m scripts.run_monthly_briefing
```

---

## Adding Your Portfolio (For Monitoring)

Create `./data/portfolio.json`:

```json
{
  "holdings": [
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

The bot will monitor news and alert you to potential thesis-breaking events.

---

## Troubleshooting

### "API rate limit exceeded"
- FMP free tier: 250 calls/day
- Solution: Reduce `max_analyses` in `.env` or wait until tomorrow

### "No stocks passed screening"
- Your criteria might be too strict
- Try relaxing `max_pe_ratio` or `min_roe` in screening criteria

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
| FMP API | $0 (free tier) |
| Finnhub API | $0 (free tier) |
| Claude API | $5-15 (depends on analyses) |
| Alpaca | $0 (paper trading) |
| **Total** | **~$5-15/month** |

---

## Next Steps

1. ✅ Run your first briefing
2. ✅ Review the output and understand the format
3. ⬜ Paper trade based on recommendations for 3-6 months
4. ⬜ Track performance vs. just buying an index fund
5. ⬜ Adjust criteria based on what you learn
6. ⬜ Consider real money only after validating the system
