# Buffett Bot - AI Copilot Instructions

## Project Overview
Buffett Bot is a **quality-first value investing research assistant** that combines quantitative stock screening with LLM qualitative analysis. It generates monthly tiered watchlist briefings by orchestrating multiple external APIs (yfinance, SEC EDGAR, Finnhub) and using Claude for document analysis and thesis assessment.

**Key Philosophy (v2.0)**: "Find wonderful businesses, track them patiently, deploy capital when price meets patience." LLMs do narrative analysis (moat, management, durability); APIs do math (valuation, financials, trends).

## Architecture Layers
```
LLM Layer (Claude) → Data Services (APIs) → Tier Engine → Output (Briefing/DB)
```

### 1. **Screening Layer** (`src/screener.py`)
- Quality-first scoring: ROIC (weight 2.5), ROE consistency, margin stability, earnings consistency
- Valuation de-weighted: P/E weight 0.8 (was 2.0), allows P/E up to 60
- 6 trend metrics from historical financials (revenue CAGR, FCF consistency, etc.)
- Sector-aware overrides (REITs get higher debt tolerance, financials skip FCF, etc.)
- Criteria loaded from `config/screening_criteria.yaml`

### 2. **Valuation Layer** (`src/valuation.py`)
- Aggregates fair value estimates from multiple sources (yfinance, Finnhub)
- **Does NOT calculate intrinsic value** - fetches external estimates only
- Computes margin of safety: `(Fair Value - Price) / Fair Value`
- Returns `AggregatedValuation` dataclass

### 3. **Analysis Layer** (`src/analyzer.py`)
- Uses Claude API with multi-turn conversations (`Anthropic` client)
- **AnalysisV2** schema: moat sources, management quality, earnings durability, competitive currency, fair value range
- Three-tier model: Haiku (pre-screen), Sonnet (deep analysis), Opus (contrarian second opinion)
- Backward-compat `@property` accessors mapping to legacy `QualitativeAnalysis` interface

### 4. **Tier Engine** (`src/tier_engine.py`)
- Assigns stocks to tiers: Tier 1 (buy zone), Tier 2 (watch), Tier 3 (monitor), 0 (excluded)
- Tier 1 = high quality (wide/narrow moat + HIGH/MEDIUM conviction) + price ≤ target entry
- Staged entry suggestions: 3 tranches at descending prices from target
- Movement tracking: detects new/removed/tier_up/tier_down/approaching changes between runs

### 5. **Briefing Layer** (`src/briefing.py`)
- Tiered watchlist format: Market Regime → Portfolio → Executive Summary → Movements → Tier 1/2/3
- Tier 1 stocks include staged entry suggestions and Opus second opinions
- Outputs text, HTML (with tier-specific CSS), and JSON (`schema_version: "v2"`)

### 6. **Market Regime** (`src/bubble_detector.py`)
- `classify_market_regime()`: Euphoria / Overvalued / Fair Value / Correction / Crisis
- 4 signals: market P/E, VIX, drawdown from 52-week high, distance from 200-day MA
- Regime-specific deployment guidance (e.g., "Deploy aggressively" in Crisis)
- Legacy `get_market_temperature()` preserved as backward-compat wrapper

### 7. **Portfolio** (`src/portfolio.py`)
- ASK (Aktiesparekonto) portfolio: 5-8 concentrated positions, 17% dividend tax
- Staged entry tracking (tranches filled/planned)
- Concentration monitoring, dividend summary, annual contribution tracking
- Gap analysis: identifies missing sectors or quality gaps

### 8. **Validation** (`src/backtest.py`)
- Quality-return Spearman rank correlation (does higher quality score predict higher returns?)
- Watchlist snapshot tracking for forward performance measurement
- Quintile analysis of quality scores vs actual returns

## Core Data Structures
- **`ScreenedStock`**: Quality-scored stock with 6 trend metrics and score confidence
- **`AggregatedValuation`**: Valuation assessment with margin of safety
- **`AnalysisV2`**: LLM output (moat/management/durability/currency/fair_value)
- **`TierAssignment`**: Tier + reason + target entry + staged entry tranches
- **`StockBriefing`**: Complete analysis (quant + qual + tier)
- **`Position`**: Portfolio holding with thesis, conviction, and tranche tracking
- **`MarketRegime`**: Current regime classification with confidence and deployment guidance

## Critical Developer Workflows

### Running Monthly Pipeline
```bash
python scripts/run_monthly_briefing.py
```
10-step pipeline: market regime → screen by quality → bubbles → Haiku pre-screen → Sonnet analysis → valuations → tier engine → portfolio check → Opus on Tier 1 → generate tiered briefing

### Using Docker (Recommended)
```bash
docker-compose up -d  # Starts scheduler
docker exec buffett-bot python -m scripts.run_monthly_briefing  # Manual run
```

### Configuration
Edit `config/screening_criteria.yaml` to tune:
- Market cap range ($300M-$500B default)
- Quality weights (ROIC, ROE consistency, margin stability)
- Sector overrides (Real Estate, Financial Services, Utilities, Energy)
- Valuation de-weighting (P/E weight 0.8)

### Environment
Create `.env` with:
- `ANTHROPIC_API_KEY` (Claude) — required
- `FINNHUB_API_KEY` (backup data, optional)
- `MAX_POSITIONS`, `ASK_CONTRIBUTION_LIMIT` (portfolio sizing)
- `MARGIN_OF_SAFETY_PCT`, `TIER1_PROXIMITY_ALERT_PCT` (tier thresholds)
- SMTP/email/Telegram config (optional)

## Project-Specific Patterns & Conventions

### 1. **API Rate Limiting**
- yfinance: no hard limit but avoid excessive requests - cache watchlist results
- Finnhub: 60 calls/minute - respect limits in loops
- Use `ratelimit` decorator when fetching multiple stocks

### 2. **Data Serialization**
All dataclasses have `.to_dict()` and `.from_dict()` methods for JSON persistence.
Example: `Position.to_dict()` converts to JSON-serializable dict; `Position.from_dict(data)` reconstructs.

### 3. **LLM Integration Pattern**
- Use `Anthropic()` client (not async)
- Three models: `model_light` (Haiku), `model_deep` (Sonnet), `model_opus` (Opus)
- Prompt caching: system prompts use `cache_control: {"type": "ephemeral"}`
- Batch API: `batch_quick_screen()` and `batch_analyze_companies()` for 50% discount
- AnalysisV2 has `@property` accessors for backward compat with QualitativeAnalysis

### 4. **Tier System** (replaces BUY/WATCHLIST/PASS)
- Tier 1: Buy zone — high quality + price below target entry
- Tier 2: Watch — high quality but price above target
- Tier 3: Monitor — moderate quality, needs more research
- Tier 0: Excluded — low quality or thesis broken

### 5. **Caching Strategy**
- Weekly watchlist cached in `data/watchlist.json` with timestamp
- Reuse if <7 days old to avoid re-screening (expensive)
- Analyses cached in `data/analyses/{symbol}.json` (30-day TTL)
- Historical data cached in per-symbol JSON under `"historical"` key
- Watchlist snapshots in `data/backtest/` for forward tracking

### 6. **Error Handling**
- Log API errors but continue pipeline (don't fail on one missing stock)
- Return `None` for unavailable data (e.g., private companies have no P/E)
- Historical trend metrics individually wrapped in try/except for graceful degradation
- Use optional types: `Optional[float]` in dataclasses

### 7. **Thesis Tracking**
- Each position has `thesis` (why bought) and `conviction` (HIGH/MEDIUM/LOW)
- `thesis_breaking_events` list tracks red flags detected via news monitoring
- Staged entry: 3 tranches at descending prices from target

## Integration Points & Dependencies
- **yfinance**: Stock screening, fundamentals, ratios, historical financials
- **SEC EDGAR**: 10-K annual reports (parsed by `sec-edgar-downloader`)
- **Finnhub**: News, backup financial data
- **Anthropic Claude**: Qualitative analysis via API (Haiku/Sonnet/Opus)
- **Alpaca (optional)**: Paper trading, position management
- External fair value sources: GuruFocus, SimplyWallSt (estimates fetched)

## Common Tasks

### Adding a New Stock Metric
1. Add field to `ScreenedStock` dataclass (with `Optional` default)
2. Compute in `_fetch_historical_data()` if trend-based, or in `screen()` if snapshot
3. Add `ScoringRule` to `screening_criteria.yaml`
4. Include in `to_dict()` serialization
5. Consider adding sector overrides if metric varies by industry

### Extending LLM Analysis
1. Add prompt text (ask Claude to extract new field)
2. Add field to `AnalysisV2` dataclass
3. Parse LLM response and populate field in `analyzer.py`
4. Add `@property` accessor if backward compat needed
5. Include in briefing output

### Adding New Notification Channel
1. Create new Notifier class in `src/notifications.py` (inherit pattern from `EmailNotifier`)
2. Implement `send_briefing()` and `send_alert()` methods
3. Check if configured (optional features should degrade gracefully)
4. Add config to `.env` example
5. Call from `NotificationManager` in pipeline script

## Important Constraints
- **No shorting**: Bubble detector is "stay away", not trading signal
- **Quality over price**: Wonderful business at fair price > mediocre at discount
- **Concentrated portfolio**: 5-8 positions max (ASK account)
- **Staged entry**: Never full position at once — 3 tranches at descending prices
- **Market cap focus**: $300M-$500B range (avoids micro-caps)
- **US stocks only**: NASDAQ/NYSE for now
- **Monthly cadence**: Heavy compute/LLM runs once/month; lighter daily news monitoring
