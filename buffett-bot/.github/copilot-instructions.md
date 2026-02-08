# Buffett Bot - AI Copilot Instructions

## Project Overview
Buffett Bot is a **Buffett-style value investing research assistant** that combines quantitative stock screening with LLM qualitative analysis. It generates monthly investment briefings by orchestrating multiple external APIs (yfinance, SEC EDGAR, Finnhub) and using Claude for document analysis and thesis assessment.

**Key Philosophy**: LLMs do narrative analysis (moat, management, risks); APIs do math (valuation, financials).

## Architecture Layers
```
LLM Layer (Claude) → Data Services (APIs) → Output (Briefing/DB)
```

### 1. **Screening Layer** (`src/screener.py`)
- Uses yfinance to filter stocks by value criteria (`ScreeningCriteria` dataclass)
- Returns 30-50 candidates passing P/E, debt, ROE, growth thresholds
- Criteria loaded from `config/screening_criteria.yaml`

### 2. **Valuation Layer** (`src/valuation.py`)
- Aggregates fair value estimates from multiple sources (yfinance, Finnhub)
- **Does NOT calculate intrinsic value** - fetches external estimates only
- Computes margin of safety: `(Fair Value - Price) / Fair Value`
- Returns `AggregatedValuation` dataclass

### 3. **Analysis Layer** (`src/analyzer.py`)
- Uses Claude API with multi-turn conversations (`Anthropic` client)
- Analyzes: moat rating (WIDE/NARROW/NONE), management quality, risks
- Returns `QualitativeAnalysis` dataclass with enum ratings
- Key: LLM reads documents (10-K summaries, transcripts) and provides text assessments

### 4. **Briefing Layer** (`src/briefing.py`)
- Combines quant + qual into human-readable report
- Sections: market temp, portfolio status, top picks, watchlist, radar, bubble watch
- Outputs markdown file with recommendations

### 5. **Portfolio/Monitoring** (`src/portfolio.py`, `src/notifications.py`)
- Tracks positions with thesis and conviction levels
- Monitors portfolio for thesis-breaking events via news
- Delivers briefings via email/Telegram/ntfy.sh

## Core Data Structures
- **`ScreenedStock`**: Passes screening (from screener)
- **`AggregatedValuation`**: Valuation assessment with margin of safety
- **`QualitativeAnalysis`**: LLM output (moat/management/risks)
- **`StockBriefing`**: Complete analysis (quant + qual)
- **`Position`**: Portfolio holding with thesis and conviction

## Critical Developer Workflows

### Running Monthly Pipeline
```bash
python scripts/run_monthly_briefing.py
```
Executes: screen → detect bubbles → aggregate valuations → LLM analysis → generate briefing → send notifications

### Using Docker (Recommended)
```bash
docker-compose up -d  # Starts scheduler
docker exec buffett-bot python -m scripts.run_monthly_briefing  # Manual run
```

### Configuration
Edit `config/screening_criteria.yaml` to tune:
- Market cap range ($300M-$500B default)
- P/E threshold (20x default)
- Debt/equity, ROE, growth minimums
- Margin of safety for BUY flag (20% default)

### Environment
Create `.env` with:
- `ANTHROPIC_API_KEY` (Claude)
- `FINNHUB_API_KEY` (backup data, optional)
- SMTP/email/Telegram config (optional)

## Project-Specific Patterns & Conventions

### 1. **API Rate Limiting**
- yfinance: no hard limit but avoid excessive requests - cache watchlist results
- Finnhub: 60 calls/minute - respect limits in loops
- Use `ratelimit` decorator when fetching multiple stocks

### 2. **Data Serialization**
All dataclasses have `.to_dict()` and `.from_dict()` methods for JSON persistence.
Example: `Position.to_dict()` converts to JSON-serializable dict; `Position.from_dict(data)` reconstructs.
See [src/portfolio.py](src/portfolio.py#L45) for pattern.

### 3. **LLM Integration Pattern**
- Use `Anthropic()` client (not async)
- Pass document text directly (10-K, transcript excerpts)
- Prompt: ask for structured output (moat sources, management score, thesis risks)
- Return `QualitativeAnalysis` dataclass with enums (not strings) for ratings
- See [src/analyzer.py](src/analyzer.py#L80) for multi-turn conversation pattern

### 4. **Enum Ratings** (not strings)
- `MoatRating`: WIDE, NARROW, NONE
- `ManagementRating`: EXCELLENT, ADEQUATE, POOR
- `Recommendation`: BUY, WATCHLIST, PASS
- Ensures type safety and prevents string typos

### 5. **Caching Strategy**
- Weekly watchlist cached in `data/watchlist.json` with timestamp
- Reuse if <7 days old to avoid re-screening (expensive)
- See [scripts/run_monthly_briefing.py](scripts/run_monthly_briefing.py#L40) for pattern
- Analyses cached in `data/analyses/{symbol}.json`

### 6. **Error Handling**
- Log API errors but continue pipeline (don't fail on one missing stock)
- Return `None` for unavailable data (e.g., private companies have no P/E)
- Use optional types: `Optional[float]` in dataclasses

### 7. **Thesis Tracking**
- Each position has `thesis` (why bought) and `conviction` (HIGH/MEDIUM/LOW)
- `thesis_breaking_events` list tracks red flags detected via news monitoring
- When defining investment thesis in analysis, think about what would invalidate it
- See [src/portfolio.py](src/portfolio.py#L20) for Position dataclass

## Integration Points & Dependencies
- **yfinance**: Stock screening, fundamentals, ratios, company profiles
- **SEC EDGAR**: 10-K annual reports (parsed by `sec-edgar-downloader`)
- **Finnhub**: News, backup financial data
- **Anthropic Claude**: Qualitative analysis via API
- **Alpaca (optional)**: Paper trading, position management
- External fair value sources: GuruFocus, SimplyWallSt (estimates fetched)

## Common Tasks

### Adding a New Stock Metric
1. Add field to relevant dataclass (e.g., `ScreeningCriteria`)
2. Fetch from API in `screener.py` or `valuation.py`
3. Filter in `ScreeningCriteria` if it's a screening criterion
4. Include in `to_dict()` serialization
5. Update config file

### Extending LLM Analysis
1. Add prompt text (ask Claude to extract new field)
2. Add field to `QualitativeAnalysis` dataclass
3. Parse LLM response and populate field in `analyzer.py`
4. Use enum for ratings (not free text)
5. Include in briefing output

### Adding New Notification Channel
1. Create new Notifier class in `src/notifications.py` (inherit pattern from `EmailNotifier`)
2. Implement `send_briefing()` and `send_alert()` methods
3. Check if configured (optional features should degrade gracefully)
4. Add config to `.env` example
5. Call from `NotificationManager` in pipeline script

## Testing & Validation
- Run screening: verify ~30-50 results, check filtering logic
- Run analyzer: verify LLM returns valid enums and non-empty text
- Validate briefing: check markdown renders, all sections populated
- Check caching: verify watchlist reused within 7 days
- Monitor API rate limits: log API call counts

## Important Constraints
- **No shorting**: Bubble detector is "stay away", not trading signal
- **Margin of safety**: Require 20%+ discount to fair value for BUY
- **Market cap focus**: $300M-$500B range (avoids micro-caps)
- **US stocks only**: NASDAQ/NYSE for now
- **Monthly cadence**: Heavy compute/LLM runs once/month; lighter daily news monitoring
