"""
SQLite Database Module — BuffettBot v2

Single source of truth for all persistent state. Designed for two concurrent
Docker services (buffett-bot manual + scheduler always-on).

WAL mode allows concurrent reads from both services with a single writer.
busy_timeout handles the rare case where both try to write simultaneously —
the loser waits 5 seconds instead of crashing.

Pragmas applied on every connection open:
    journal_mode = WAL        — concurrent reads + single writer
    busy_timeout = 5000       — wait 5s on lock contention, not fail
    foreign_keys = ON         — referential integrity enforced
    synchronous = NORMAL      — safe with WAL, better write performance
"""

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterator, Optional, Union

import yaml

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path("data/buffett_bot_v2.db")

# ─── Schema ────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
-- Every stock that has ever entered the screener
CREATE TABLE IF NOT EXISTS universe (
    ticker          TEXT PRIMARY KEY,
    company_name    TEXT,
    sector          TEXT,
    market_cap      REAL,
    cap_category    TEXT,       -- 'large', 'mid', 'small'
    source          TEXT,       -- 'conviction', 'sp500_filter', 'finviz_screen'
    quality_score   REAL,       -- composite percentile score 0-100
    last_screened   TIMESTAMP,
    in_universe     INTEGER NOT NULL DEFAULT 1  -- BOOLEAN: 1=yes, 0=no
);

-- Weekly snapshots of key fundamentals (time-series)
CREATE TABLE IF NOT EXISTS fundamentals (
    ticker          TEXT NOT NULL,
    date            TEXT NOT NULL,  -- ISO date YYYY-MM-DD
    price           REAL,
    pe_ratio        REAL,
    roe             REAL,
    roic            REAL,
    operating_margin REAL,
    fcf_yield       REAL,
    debt_equity     REAL,
    revenue_growth  REAL,
    PRIMARY KEY (ticker, date)
);

-- Haiku pre-screen results (cached ~6 months)
CREATE TABLE IF NOT EXISTS haiku_screens (
    ticker          TEXT NOT NULL,
    screened_at     TIMESTAMP NOT NULL,
    passed          INTEGER NOT NULL,   -- BOOLEAN
    moat_estimate   TEXT,               -- 'WIDE', 'NARROW', 'NONE'
    summary         TEXT,
    expires_at      TIMESTAMP NOT NULL,
    PRIMARY KEY (ticker, screened_at)
);

-- Sonnet deep analysis results (cached ~6 months)
CREATE TABLE IF NOT EXISTS deep_analyses (
    ticker          TEXT NOT NULL,
    analyzed_at     TIMESTAMP NOT NULL,
    tier            TEXT NOT NULL,      -- 'S', 'A', 'B', 'C'
    conviction      TEXT,               -- 'HIGH', 'MEDIUM', 'LOW'
    moat_rating     TEXT,               -- 'WIDE', 'NARROW', 'NONE'
    moat_sources    TEXT,               -- JSON array
    fair_value      REAL,
    target_entry    REAL,
    investment_thesis TEXT,
    key_risks       TEXT,               -- JSON array
    thesis_breakers TEXT,               -- JSON array
    expires_at      TIMESTAMP NOT NULL,
    PRIMARY KEY (ticker, analyzed_at)
);

-- Active price alerts for watched stocks (S/A/B tier)
CREATE TABLE IF NOT EXISTS price_alerts (
    ticker          TEXT PRIMARY KEY,
    tier            TEXT NOT NULL,      -- 'S', 'A', 'B', 'C'
    target_entry    REAL,
    staged_entries  TEXT,               -- JSON: {"1/3": 172, "2/3": 163, "3/3": 155}
    last_price      REAL,
    gap_pct         REAL,               -- (current - target) / target; negative = below target
    alert_triggered INTEGER NOT NULL DEFAULT 0  -- BOOLEAN
);

-- Every pipeline execution with cost tracking
CREATE TABLE IF NOT EXISTS run_log (
    run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_type        TEXT NOT NULL,  -- 'bulk_load', 'weekly_refresh', 'monthly_briefing', 'news_triggered'
    started_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at    TIMESTAMP,
    stocks_screened INTEGER NOT NULL DEFAULT 0,
    haiku_calls     INTEGER NOT NULL DEFAULT 0,
    sonnet_calls    INTEGER NOT NULL DEFAULT 0,
    opus_calls      INTEGER NOT NULL DEFAULT 0,
    total_cost_usd  REAL NOT NULL DEFAULT 0.0
);

-- Finnhub news events that were processed
CREATE TABLE IF NOT EXISTS news_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    headline        TEXT NOT NULL,
    source          TEXT,
    published_at    TIMESTAMP,
    detected_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type      TEXT,   -- 'earnings', 'ceo_change', 'acquisition', 'lawsuit', etc.
    haiku_material  INTEGER,    -- BOOLEAN: did Haiku judge this material?
    sonnet_triggered INTEGER,   -- BOOLEAN: did this lead to a Sonnet re-analysis?
    summary         TEXT
);

-- Audit trail of every tier change
CREATE TABLE IF NOT EXISTS tier_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    changed_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    old_tier        TEXT,       -- NULL for first assignment
    new_tier        TEXT NOT NULL,
    trigger         TEXT,       -- 'scheduled', 'news_event', 'price_move', 'bulk_load', 'manual'
    reason          TEXT
);

-- Weekly API cost budget caps (reset every Monday)
CREATE TABLE IF NOT EXISTS budget_caps (
    cap_type        TEXT PRIMARY KEY,   -- 'weekly_news_sonnet', 'weekly_news_haiku'
    period_start    TEXT NOT NULL,      -- ISO date of current week's Monday
    calls_used      INTEGER NOT NULL DEFAULT 0,
    max_calls       INTEGER NOT NULL,
    last_reset      TIMESTAMP
);

-- Local mirror of Alpaca paper trading positions (synced Monday)
CREATE TABLE IF NOT EXISTS paper_positions (
    ticker          TEXT PRIMARY KEY,
    tier_at_entry   TEXT NOT NULL,      -- 'S' or 'A'
    entry_stage     TEXT,               -- '1/3', '2/3', '3/3' or '1/2', '2/2'
    entry_price     REAL,
    entry_date      TEXT,               -- ISO date
    shares          REAL,
    cost_basis      REAL,
    current_price   REAL,
    current_value   REAL,
    gain_loss_pct   REAL,
    last_synced     TIMESTAMP
);

-- Append-only event log of every buy/sell decision with full reasoning context.
-- Unlike paper_positions (open positions only, overwritten on sync), this is
-- never mutated — it is the permanent record of what was decided and why.
CREATE TABLE IF NOT EXISTS decision_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    action          TEXT NOT NULL,      -- 'buy' / 'sell'
    decided_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tier            TEXT,               -- tier at decision time, if known
    price           REAL,               -- fill price if known (NULL for queued notional buys)
    shares          REAL,
    notional        REAL,               -- dollar amount (known at buy time even when price isn't)
    order_id        TEXT,
    reason          TEXT,               -- trigger string (e.g. "Take profit: ...")
    regime          TEXT,               -- market regime at decision time
    reasoning_snapshot TEXT             -- JSON: thesis, fair_value, target_entry, margin_of_safety,
                                        --       moat_rating, conviction, quality_score
);

-- One row per closed (sold) position, written on sell. Holds the realized
-- outcome and a verdict on whether the original reasoning held up.
CREATE TABLE IF NOT EXISTS closed_trades (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker            TEXT NOT NULL,
    entry_decision_id INTEGER REFERENCES decision_log(id),
    exit_decision_id  INTEGER REFERENCES decision_log(id),
    tier_at_entry     TEXT,
    entry_date        TEXT,
    exit_date         TEXT,
    hold_days         INTEGER,
    entry_price       REAL,
    exit_price        REAL,
    shares            REAL,
    cost_basis        REAL,
    proceeds          REAL,
    realized_pl       REAL,
    realized_pl_pct   REAL,
    benchmark_return  REAL,             -- benchmark total return over the same hold window
    alpha             REAL,             -- realized_pl_pct - benchmark_return
    entry_fair_value  REAL,
    converged         INTEGER,          -- BOOLEAN (nullable): did price move toward entry fair value?
    sell_category     TEXT,             -- 'take_profit'/'thesis_breaker'/'stop'/'manual'/'other'
    reasoning_sound   INTEGER,          -- BOOLEAN (nullable): did the original reasoning hold up?
    notes             TEXT
);

-- Point-in-time fundamentals from SEC EDGAR companyfacts (Phase 2.5). Each XBRL
-- fact carries the date it was FILED (became public) — exactly what a look-ahead-
-- free backtest needs. One row per (ticker, concept, period, form), holding the
-- ORIGINALLY-FILED value (earliest filed_date) so a later restatement can't leak
-- backward in time.
CREATE TABLE IF NOT EXISTS pit_fundamentals (
    ticker        TEXT NOT NULL,
    cik           TEXT,
    concept       TEXT NOT NULL,      -- canonical field name (see edgar_fundamentals.CONCEPT_MAP)
    period_end    TEXT NOT NULL,      -- ISO date (XBRL 'end')
    fiscal_year   INTEGER,
    fiscal_period TEXT,               -- 'FY', 'Q1', 'Q2', 'Q3'
    form          TEXT,               -- '10-K', '10-Q'
    value         REAL,
    filed_date    TEXT NOT NULL,      -- when it became public (XBRL 'filed')
    accession     TEXT,
    PRIMARY KEY (ticker, concept, period_end, form)
);

-- Append-only daily snapshot of account state, one row per (account_id, as_of).
-- Fixes the core gap: today the bot queries the broker live and throws the
-- number away, so once cash moves the old level is unrecoverable. Positions
-- are stored as a JSON blob rather than a separate per-position table — queryable
-- enough for an equity curve; split out later if per-ticker time series is needed.
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id      TEXT NOT NULL,
    as_of           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    currency        TEXT NOT NULL,
    equity          REAL NOT NULL,
    cash            REAL NOT NULL,
    buying_power    REAL,
    invested_value  REAL,
    invested_pct    REAL,
    equity_dkk      REAL,           -- NULL if the FX rate was unavailable
    positions       TEXT            -- JSON array of position dicts
);

-- One row per day the market regime is classified. Regime is computed at
-- trade time today but never stored standalone — this lets deployment
-- behavior be compared against market conditions after the fact.
CREATE TABLE IF NOT EXISTS regime_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    regime          TEXT NOT NULL,
    confidence      TEXT,           -- 'high' / 'moderate' / 'low'
    market_pe       REAL,
    vix             REAL
);

-- Cash events outside of buy/sell trades. Dividends come from Alpaca's
-- activities API where available; contribution/withdrawal/tax are manual
-- entries (Nordnet is entirely manual, Phase D).
CREATE TABLE IF NOT EXISTS income_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id      TEXT NOT NULL,
    event_date      TEXT NOT NULL,      -- ISO date
    event_type      TEXT NOT NULL,      -- 'dividend' / 'contribution' / 'withdrawal' / 'tax'
    symbol          TEXT,               -- NULL for contribution/withdrawal
    amount          REAL NOT NULL,
    currency        TEXT NOT NULL,
    withholding     REAL,               -- tax withheld, if any (same currency as amount)
    logged_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

INDEXES_SQL = """
-- Priority: rank universe by quality score for Haiku batching
CREATE INDEX IF NOT EXISTS idx_universe_quality
    ON universe(quality_score DESC) WHERE in_universe = 1;

-- Find analyses expiring soon for re-screening Wednesday
CREATE INDEX IF NOT EXISTS idx_deep_expiry
    ON deep_analyses(expires_at) WHERE expires_at IS NOT NULL;

-- Constant query: latest analysis per ticker
CREATE INDEX IF NOT EXISTS idx_deep_latest
    ON deep_analyses(ticker, analyzed_at DESC);

-- Daily: recent news for a ticker
CREATE INDEX IF NOT EXISTS idx_news_date
    ON news_events(ticker, detected_at DESC);

-- Briefing: fundamentals history per ticker
CREATE INDEX IF NOT EXISTS idx_fund_ticker
    ON fundamentals(ticker, date DESC);

-- Tier change audit queries
CREATE INDEX IF NOT EXISTS idx_tier_history_ticker
    ON tier_history(ticker, changed_at DESC);

-- Haiku expiry check
CREATE INDEX IF NOT EXISTS idx_haiku_expiry
    ON haiku_screens(expires_at) WHERE expires_at IS NOT NULL;

-- Latest Haiku result per ticker
CREATE INDEX IF NOT EXISTS idx_haiku_latest
    ON haiku_screens(ticker, screened_at DESC);

-- Decision journal: recent decisions per ticker, and open-buy lookup on close
CREATE INDEX IF NOT EXISTS idx_decision_log_ticker
    ON decision_log(ticker, decided_at DESC);

-- Track record: closed trades per ticker, and entry-decision back-reference
CREATE INDEX IF NOT EXISTS idx_closed_trades_ticker
    ON closed_trades(ticker, exit_date DESC);
CREATE INDEX IF NOT EXISTS idx_closed_trades_entry
    ON closed_trades(entry_decision_id);

-- Point-in-time as-of lookups: latest filed value per (ticker, concept) at a date
CREATE INDEX IF NOT EXISTS idx_pit_asof
    ON pit_fundamentals(ticker, concept, filed_date);

-- Equity curve: snapshots per account in time order
CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_account
    ON portfolio_snapshots(account_id, as_of);

-- Regime history in time order
CREATE INDEX IF NOT EXISTS idx_regime_log_asof
    ON regime_log(as_of);

-- Income history per account, most recent first
CREATE INDEX IF NOT EXISTS idx_income_events_account
    ON income_events(account_id, event_date DESC);
"""

BUDGET_CAPS_DEFAULTS = [
    ("weekly_news_sonnet", 10),
    ("weekly_news_haiku", 50),
    ("weekly_haiku_screen", 50),  # scheduled Wednesday Haiku batch
    ("weekly_sonnet_analysis", 10),  # scheduled Friday Sonnet batch
]


# ─── Connection Helpers ────────────────────────────────────────────────────


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    """Apply required pragmas to every new connection."""
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.row_factory = sqlite3.Row


@contextmanager
def _open(db_path: Path) -> Iterator[sqlite3.Connection]:
    """Open a connection with pragmas, auto-commit on clean exit."""
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    _apply_pragmas(conn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ─── Decision Journal Helpers ──────────────────────────────────────────────


def _classify_sell_reason(reason: str) -> str:
    """
    Map a free-text sell reason to a category. Order matters: the most specific
    phrases are checked first.
    """
    r = (reason or "").lower()
    if "take profit" in r or "take-profit" in r or "fair value" in r:
        return "take_profit"
    if "thesis" in r or "breaker" in r or "broke" in r:
        return "thesis_breaker"
    if "stop" in r:
        return "stop"
    if "manual" in r:
        return "manual"
    return "other"


def _score_reasoning_soundness(category: str, alpha: Optional[float], converged: Optional[int]) -> Optional[int]:
    """
    Verdict on whether the original buy reasoning held up, given the realized
    outcome. Default rubric (tunable; recorded in the Phase 1 commit):

      - take_profit  → sound iff the stock converged toward our entry fair value
                       AND we beat the benchmark (alpha > 0). This is the case we
                       can fully judge at close: we predicted it would rise to
                       fair value, and it did, better than the market.
      - thesis_breaker / stop / manual / other → None (unknown at close time).
        A thesis-breaker's soundness ("did selling avoid a larger drawdown than
        holding?") needs forward prices after the exit, so it is left for a
        later post-hoc pass rather than guessed now.
    """
    if category == "take_profit":
        if alpha is None or converged is None:
            return None
        return 1 if (alpha > 0 and converged == 1) else 0
    return None


# ─── Database Class ────────────────────────────────────────────────────────


class Database:
    """
    Manages all BuffettBot v2 persistent state via SQLite.

    Designed for two concurrent Docker services. WAL mode + busy_timeout
    prevent lock conflicts between the always-on scheduler and the manual
    briefing runner.
    """

    def __init__(self, db_path: Path = DEFAULT_DB_PATH):
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    # ── Schema ──────────────────────────────────────────────────────────────

    def _init_schema(self) -> None:
        """Create all tables, indexes, and seed budget_caps defaults."""
        with _open(self.path) as conn:
            conn.executescript(SCHEMA_SQL)
            conn.executescript(INDEXES_SQL)
            # Seed budget caps only if table is empty
            for cap_type, max_calls in BUDGET_CAPS_DEFAULTS:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO budget_caps (cap_type, period_start, calls_used, max_calls)
                    VALUES (?, date('now', 'weekday 1', '-7 days'), 0, ?)
                    """,
                    (cap_type, max_calls),
                )

    # ── Budget Caps ──────────────────────────────────────────────────────────

    def can_spend(self, cap_type: str) -> bool:
        """
        Atomically check and increment a budget cap.

        Uses BEGIN IMMEDIATE to acquire the write lock before reading,
        making the check-then-increment race-free even with two services
        hitting the database simultaneously.

        Returns True if the spend is allowed (and has been counted).
        Returns False if the cap is exhausted.
        """
        # Use isolation_level=None (autocommit) so we can issue BEGIN IMMEDIATE
        conn = sqlite3.connect(str(self.path), check_same_thread=False, isolation_level=None)
        _apply_pragmas(conn)
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT calls_used, max_calls FROM budget_caps WHERE cap_type = ?",
                (cap_type,),
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                logger.warning("Unknown budget cap type: %r", cap_type)
                return False
            if row["calls_used"] >= row["max_calls"]:
                conn.execute("ROLLBACK")
                logger.info(
                    "Budget cap exhausted for %r: %d/%d",
                    cap_type,
                    row["calls_used"],
                    row["max_calls"],
                )
                return False
            conn.execute(
                "UPDATE budget_caps SET calls_used = calls_used + 1 WHERE cap_type = ?",
                (cap_type,),
            )
            conn.execute("COMMIT")
            return True
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            conn.close()

    def get_budget_status(self, cap_type: str) -> dict:
        """Return current usage for a budget cap."""
        with _open(self.path) as conn:
            row = conn.execute(
                "SELECT cap_type, calls_used, max_calls, period_start FROM budget_caps WHERE cap_type = ?",
                (cap_type,),
            ).fetchone()
            if row is None:
                return {}
            return dict(row)

    def reset_weekly_budgets(self) -> None:
        """
        Reset all budget caps to 0. Call every Monday.
        Sets period_start to today so the weekly window is clear.
        """
        today = date.today().isoformat()
        with _open(self.path) as conn:
            conn.execute(
                "UPDATE budget_caps SET calls_used = 0, period_start = ?, last_reset = CURRENT_TIMESTAMP",
                (today,),
            )
        logger.info("Weekly budget caps reset (period_start=%s)", today)

    def spend_batch(self, cap_type: str, n: int) -> int:
        """
        Atomically reserve up to N budget cap slots for a batch job.

        Unlike can_spend() (one slot at a time), this reserves all N slots
        upfront before the batch is submitted.  If fewer than N slots are
        available, it reserves however many remain and returns that count.

        Returns 0 if the cap is already exhausted or the cap_type is unknown.
        """
        conn = sqlite3.connect(str(self.path), check_same_thread=False, isolation_level=None)
        _apply_pragmas(conn)
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT calls_used, max_calls FROM budget_caps WHERE cap_type = ?",
                (cap_type,),
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                logger.warning("Unknown budget cap type: %r", cap_type)
                return 0
            available = row["max_calls"] - row["calls_used"]
            actual = min(n, max(0, available))
            if actual == 0:
                conn.execute("ROLLBACK")
                logger.info(
                    "Budget cap exhausted for %r: %d/%d",
                    cap_type,
                    row["calls_used"],
                    row["max_calls"],
                )
                return 0
            conn.execute(
                "UPDATE budget_caps SET calls_used = calls_used + ? WHERE cap_type = ?",
                (actual, cap_type),
            )
            conn.execute("COMMIT")
            return actual
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            conn.close()

    # ── Universe ─────────────────────────────────────────────────────────────

    def upsert_universe_stock(
        self,
        ticker: str,
        *,
        company_name: Optional[str] = None,
        sector: Optional[str] = None,
        market_cap: Optional[float] = None,
        cap_category: Optional[str] = None,
        source: str,
        quality_score: Optional[float] = None,
    ) -> None:
        """Insert or update a stock in the universe table."""
        with _open(self.path) as conn:
            conn.execute(
                """
                INSERT INTO universe (ticker, company_name, sector, market_cap, cap_category,
                                      source, quality_score, last_screened, in_universe)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 1)
                ON CONFLICT(ticker) DO UPDATE SET
                    company_name  = COALESCE(excluded.company_name, company_name),
                    sector        = COALESCE(excluded.sector, sector),
                    market_cap    = COALESCE(excluded.market_cap, market_cap),
                    cap_category  = COALESCE(excluded.cap_category, cap_category),
                    source        = excluded.source,
                    quality_score = COALESCE(excluded.quality_score, quality_score),
                    last_screened = CURRENT_TIMESTAMP,
                    in_universe   = 1
                """,
                (ticker, company_name, sector, market_cap, cap_category, source, quality_score),
            )

    def sync_conviction_list(self, yaml_path: Path) -> list[str]:
        """
        Read conviction_list.yaml and upsert all tickers into the universe table
        as source='conviction'. Returns list of conviction tickers.
        """
        with open(yaml_path) as f:
            data = yaml.safe_load(f)
        stocks = data.get("stocks", [])
        tickers = []
        for entry in stocks:
            ticker = entry["ticker"]
            tickers.append(ticker)
            self.upsert_universe_stock(
                ticker,
                source="conviction",
                # name/sector/market_cap populated later during fundamentals refresh
            )
        logger.info("Synced %d conviction tickers into universe", len(tickers))
        return tickers

    def update_quality_score(self, ticker: str, quality_score: float) -> None:
        """Update the quality score for a ticker already in the universe."""
        with _open(self.path) as conn:
            conn.execute(
                "UPDATE universe SET quality_score = ? WHERE ticker = ?",
                (quality_score, ticker),
            )

    def get_universe(self, source_filter: Optional[str] = None) -> list[dict]:
        """Return all active universe stocks, optionally filtered by source."""
        with _open(self.path) as conn:
            if source_filter:
                rows = conn.execute(
                    "SELECT * FROM universe WHERE in_universe = 1 AND source = ? ORDER BY quality_score DESC NULLS LAST",
                    (source_filter,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM universe WHERE in_universe = 1 ORDER BY quality_score DESC NULLS LAST"
                ).fetchall()
            return [dict(r) for r in rows]

    def get_universe_stock(self, ticker: str) -> Optional[dict]:
        """Return a single universe row for ticker, or None if not found."""
        with _open(self.path) as conn:
            row = conn.execute("SELECT * FROM universe WHERE ticker = ?", (ticker,)).fetchone()
            return dict(row) if row else None

    # ── Fundamentals ─────────────────────────────────────────────────────────

    def save_fundamentals(self, ticker: str, data: dict, as_of_date: Optional[str] = None) -> None:
        """Store a weekly fundamentals snapshot."""
        as_of = as_of_date or date.today().isoformat()
        with _open(self.path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO fundamentals
                    (ticker, date, price, pe_ratio, roe, roic, operating_margin,
                     fcf_yield, debt_equity, revenue_growth)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ticker,
                    as_of,
                    data.get("price"),
                    data.get("pe_ratio"),
                    data.get("roe"),
                    data.get("roic"),
                    data.get("operating_margin"),
                    data.get("real_fcf_yield") or data.get("fcf_yield"),
                    data.get("debt_equity"),
                    data.get("revenue_growth"),
                ),
            )

    def get_latest_fundamentals(self, ticker: str) -> Optional[dict]:
        """Return the most recent fundamentals snapshot for a ticker, or None."""
        with _open(self.path) as conn:
            row = conn.execute(
                """
                SELECT * FROM fundamentals
                WHERE ticker = ?
                ORDER BY date DESC
                LIMIT 1
                """,
                (ticker,),
            ).fetchone()
            return dict(row) if row else None

    # ── Haiku Screens ─────────────────────────────────────────────────────────

    def save_haiku_result(
        self,
        ticker: str,
        *,
        passed: bool,
        moat_estimate: Optional[str] = None,
        summary: Optional[str] = None,
        expires_days: int = 180,
    ) -> None:
        """Store a Haiku pre-screen result."""
        now = datetime.now()
        expires = (now + timedelta(days=expires_days)).isoformat()
        with _open(self.path) as conn:
            conn.execute(
                """
                INSERT INTO haiku_screens (ticker, screened_at, passed, moat_estimate, summary, expires_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (ticker, now.isoformat(), int(passed), moat_estimate, summary, expires),
            )

    def get_latest_haiku(self, ticker: str) -> Optional[dict]:
        """Return the most recent Haiku result for a ticker, or None."""
        with _open(self.path) as conn:
            row = conn.execute(
                """
                SELECT * FROM haiku_screens
                WHERE ticker = ?
                ORDER BY screened_at DESC
                LIMIT 1
                """,
                (ticker,),
            ).fetchone()
            return dict(row) if row else None

    # ── Deep Analyses ─────────────────────────────────────────────────────────

    def save_deep_analysis(
        self,
        ticker: str,
        *,
        tier: str,
        conviction: Optional[str] = None,
        moat_rating: Optional[str] = None,
        moat_sources: Optional[list] = None,
        fair_value: Optional[float] = None,
        target_entry: Optional[float] = None,
        investment_thesis: Optional[str] = None,
        key_risks: Optional[list] = None,
        thesis_breakers: Optional[list] = None,
        expires_days: int = 180,
    ) -> None:
        """Store a Sonnet deep analysis result."""
        now = datetime.now()
        expires = (now + timedelta(days=expires_days)).isoformat()
        with _open(self.path) as conn:
            conn.execute(
                """
                INSERT INTO deep_analyses
                    (ticker, analyzed_at, tier, conviction, moat_rating, moat_sources,
                     fair_value, target_entry, investment_thesis, key_risks,
                     thesis_breakers, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ticker,
                    now.isoformat(),
                    tier,
                    conviction,
                    moat_rating,
                    json.dumps(moat_sources or []),
                    fair_value,
                    target_entry,
                    investment_thesis,
                    json.dumps(key_risks or []),
                    json.dumps(thesis_breakers or []),
                    expires,
                ),
            )

    def get_latest_deep_analysis(self, ticker: str) -> Optional[dict]:
        """Return the most recent deep analysis for a ticker, or None."""
        with _open(self.path) as conn:
            row = conn.execute(
                """
                SELECT * FROM deep_analyses
                WHERE ticker = ?
                ORDER BY analyzed_at DESC
                LIMIT 1
                """,
                (ticker,),
            ).fetchone()
            if not row:
                return None
            d = dict(row)
            d["moat_sources"] = json.loads(d["moat_sources"] or "[]")
            d["key_risks"] = json.loads(d["key_risks"] or "[]")
            d["thesis_breakers"] = json.loads(d["thesis_breakers"] or "[]")
            return d

    def get_expiring_analyses(self, within_days: int = 30) -> list[str]:
        """Return tickers whose latest analysis expires within N days."""
        cutoff = (datetime.now() + timedelta(days=within_days)).isoformat()
        with _open(self.path) as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT ticker FROM deep_analyses
                WHERE expires_at <= ?
                  AND analyzed_at = (
                      SELECT MAX(analyzed_at) FROM deep_analyses d2
                      WHERE d2.ticker = deep_analyses.ticker
                  )
                """,
                (cutoff,),
            ).fetchall()
            return [r["ticker"] for r in rows]

    # ── Tier History ─────────────────────────────────────────────────────────

    def log_tier_change(
        self,
        ticker: str,
        new_tier: str,
        *,
        old_tier: Optional[str] = None,
        trigger: str = "scheduled",
        reason: str = "",
    ) -> None:
        """Record a tier assignment or change for audit trail."""
        with _open(self.path) as conn:
            conn.execute(
                """
                INSERT INTO tier_history (ticker, old_tier, new_tier, trigger, reason)
                VALUES (?, ?, ?, ?, ?)
                """,
                (ticker, old_tier, new_tier, trigger, reason),
            )

    def get_tier_history(self, ticker: str, limit: int = 20) -> list[dict]:
        """Return recent tier history for a ticker."""
        with _open(self.path) as conn:
            rows = conn.execute(
                """
                SELECT * FROM tier_history
                WHERE ticker = ?
                ORDER BY changed_at DESC, id DESC
                LIMIT ?
                """,
                (ticker, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    # ── Price Alerts ──────────────────────────────────────────────────────────

    def upsert_price_alert(
        self,
        ticker: str,
        *,
        tier: str,
        target_entry: Optional[float],
        staged_entries: Optional[Union[dict, list]],
        last_price: Optional[float],
        gap_pct: Optional[float],
        alert_triggered: bool = False,
    ) -> None:
        with _open(self.path) as conn:
            conn.execute(
                """
                INSERT INTO price_alerts
                    (ticker, tier, target_entry, staged_entries, last_price, gap_pct, alert_triggered)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticker) DO UPDATE SET
                    tier            = excluded.tier,
                    target_entry    = excluded.target_entry,
                    staged_entries  = excluded.staged_entries,
                    last_price      = excluded.last_price,
                    gap_pct         = excluded.gap_pct,
                    alert_triggered = excluded.alert_triggered
                """,
                (
                    ticker,
                    tier,
                    target_entry,
                    json.dumps(staged_entries) if staged_entries else None,
                    last_price,
                    gap_pct,
                    int(alert_triggered),
                ),
            )

    def get_price_alerts(self, tiers: Optional[list[str]] = None) -> list[dict]:
        """Return price alerts, optionally filtered to specific tiers."""
        with _open(self.path) as conn:
            if tiers:
                placeholders = ",".join("?" * len(tiers))
                rows = conn.execute(
                    f"SELECT * FROM price_alerts WHERE tier IN ({placeholders}) ORDER BY gap_pct ASC",  # nosec B608
                    tiers,
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM price_alerts ORDER BY gap_pct ASC").fetchall()
            results = []
            for r in rows:
                d = dict(r)
                d["staged_entries"] = json.loads(d["staged_entries"] or "{}")
                results.append(d)
            return results

    # ── Run Log ───────────────────────────────────────────────────────────────

    def start_run(self, run_type: str) -> int:
        """Log the start of a pipeline run. Returns run_id."""
        with _open(self.path) as conn:
            cursor = conn.execute(
                "INSERT INTO run_log (run_type, started_at) VALUES (?, CURRENT_TIMESTAMP)",
                (run_type,),
            )
            return cursor.lastrowid  # type: ignore[return-value]

    def complete_run(
        self,
        run_id: int,
        *,
        stocks_screened: int = 0,
        haiku_calls: int = 0,
        sonnet_calls: int = 0,
        opus_calls: int = 0,
        total_cost_usd: float = 0.0,
    ) -> None:
        """Update a run log entry with completion stats."""
        with _open(self.path) as conn:
            conn.execute(
                """
                UPDATE run_log SET
                    completed_at    = CURRENT_TIMESTAMP,
                    stocks_screened = stocks_screened + ?,
                    haiku_calls     = haiku_calls     + ?,
                    sonnet_calls    = sonnet_calls    + ?,
                    opus_calls      = opus_calls      + ?,
                    total_cost_usd  = total_cost_usd  + ?
                WHERE run_id = ?
                """,
                (stocks_screened, haiku_calls, sonnet_calls, opus_calls, total_cost_usd, run_id),
            )

    def get_run_history(self, limit: int = 20) -> list[dict]:
        with _open(self.path) as conn:
            rows = conn.execute(
                "SELECT * FROM run_log ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    # ── News Events ───────────────────────────────────────────────────────────

    def get_recent_news_events(self, days_back: int = 7) -> list[dict]:
        """Return news events from the last N days, newest first."""
        since = (datetime.now() - timedelta(days=days_back)).isoformat()
        with _open(self.path) as conn:
            rows = conn.execute(
                """
                SELECT * FROM news_events
                WHERE detected_at >= ?
                ORDER BY detected_at DESC
                """,
                (since,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_all_latest_deep_analyses(self) -> list[dict]:
        """Return the most recent deep analysis for every ticker that has one."""
        with _open(self.path) as conn:
            rows = conn.execute(
                """
                SELECT da.* FROM deep_analyses da
                INNER JOIN (
                    SELECT ticker, MAX(analyzed_at) AS max_at
                    FROM deep_analyses
                    GROUP BY ticker
                ) latest ON da.ticker = latest.ticker
                         AND da.analyzed_at = latest.max_at
                ORDER BY da.analyzed_at DESC
                """,
            ).fetchall()
            results = []
            for row in rows:
                d = dict(row)
                d["moat_sources"] = json.loads(d["moat_sources"] or "[]")
                d["key_risks"] = json.loads(d["key_risks"] or "[]")
                d["thesis_breakers"] = json.loads(d["thesis_breakers"] or "[]")
                results.append(d)
            return results

    def log_news_event(
        self,
        ticker: str,
        headline: str,
        *,
        source: Optional[str] = None,
        published_at: Optional[str] = None,
        event_type: Optional[str] = None,
        haiku_material: Optional[bool] = None,
        sonnet_triggered: Optional[bool] = None,
        summary: Optional[str] = None,
    ) -> None:
        with _open(self.path) as conn:
            conn.execute(
                """
                INSERT INTO news_events
                    (ticker, headline, source, published_at, event_type,
                     haiku_material, sonnet_triggered, summary)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ticker,
                    headline,
                    source,
                    published_at,
                    event_type,
                    None if haiku_material is None else int(haiku_material),
                    None if sonnet_triggered is None else int(sonnet_triggered),
                    summary,
                ),
            )

    # ── Paper Positions ───────────────────────────────────────────────────────

    def upsert_paper_position(
        self,
        ticker: str,
        *,
        tier_at_entry: str,
        entry_stage: Optional[str] = None,
        entry_price: Optional[float] = None,
        entry_date: Optional[str] = None,
        shares: Optional[float] = None,
        cost_basis: Optional[float] = None,
        current_price: Optional[float] = None,
        current_value: Optional[float] = None,
        gain_loss_pct: Optional[float] = None,
    ) -> None:
        with _open(self.path) as conn:
            conn.execute(
                """
                INSERT INTO paper_positions
                    (ticker, tier_at_entry, entry_stage, entry_price, entry_date,
                     shares, cost_basis, current_price, current_value, gain_loss_pct,
                     last_synced)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(ticker) DO UPDATE SET
                    tier_at_entry = excluded.tier_at_entry,
                    entry_stage   = COALESCE(excluded.entry_stage, entry_stage),
                    entry_price   = COALESCE(excluded.entry_price, entry_price),
                    entry_date    = COALESCE(excluded.entry_date, entry_date),
                    shares        = COALESCE(excluded.shares, shares),
                    cost_basis    = COALESCE(excluded.cost_basis, cost_basis),
                    current_price = COALESCE(excluded.current_price, current_price),
                    current_value = COALESCE(excluded.current_value, current_value),
                    gain_loss_pct = COALESCE(excluded.gain_loss_pct, gain_loss_pct),
                    last_synced   = CURRENT_TIMESTAMP
                """,
                (
                    ticker,
                    tier_at_entry,
                    entry_stage,
                    entry_price,
                    entry_date,
                    shares,
                    cost_basis,
                    current_price,
                    current_value,
                    gain_loss_pct,
                ),
            )

    def get_paper_positions(self) -> list[dict]:
        with _open(self.path) as conn:
            rows = conn.execute("SELECT * FROM paper_positions ORDER BY ticker").fetchall()
            return [dict(r) for r in rows]

    # ── Decision Journal & Track Record ───────────────────────────────────────

    def log_decision(
        self,
        ticker: str,
        action: str,
        *,
        tier: Optional[str] = None,
        price: Optional[float] = None,
        shares: Optional[float] = None,
        notional: Optional[float] = None,
        order_id: Optional[str] = None,
        reason: str = "",
        regime: Optional[str] = None,
        reasoning_snapshot: Optional[dict] = None,
    ) -> int:
        """
        Append a buy/sell decision to the permanent journal.

        Returns the new decision_log row id (used as entry/exit reference in
        closed_trades). `reasoning_snapshot` is stored as JSON.
        """
        if action not in ("buy", "sell"):
            raise ValueError(f"action must be 'buy' or 'sell', got {action!r}")
        with _open(self.path) as conn:
            cur = conn.execute(
                """
                INSERT INTO decision_log
                    (ticker, action, tier, price, shares, notional, order_id,
                     reason, regime, reasoning_snapshot)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ticker,
                    action,
                    tier,
                    price,
                    shares,
                    notional,
                    order_id,
                    reason,
                    regime,
                    json.dumps(reasoning_snapshot) if reasoning_snapshot is not None else None,
                ),
            )
            new_id = cur.lastrowid
            return int(new_id) if new_id is not None else 0

    def get_open_buy(self, ticker: str) -> Optional[dict]:
        """
        Return the most recent 'buy' decision for a ticker that has not yet been
        matched to a closed trade, or None. This is the entry that a subsequent
        sell closes against.
        """
        with _open(self.path) as conn:
            row = conn.execute(
                """
                SELECT * FROM decision_log
                WHERE ticker = ? AND action = 'buy'
                  AND id NOT IN (
                      SELECT entry_decision_id FROM closed_trades
                      WHERE entry_decision_id IS NOT NULL
                  )
                ORDER BY decided_at DESC, id DESC
                LIMIT 1
                """,
                (ticker,),
            ).fetchone()
            if not row:
                return None
            d = dict(row)
            d["reasoning_snapshot"] = json.loads(d["reasoning_snapshot"]) if d["reasoning_snapshot"] else None
            return d

    def close_trade(
        self,
        ticker: str,
        *,
        exit_decision_id: int,
        entry_price: float,
        exit_price: float,
        shares: float,
        exit_date: Optional[str] = None,
        sell_category: Optional[str] = None,
        benchmark_return: Optional[float] = None,
        notes: str = "",
    ) -> Optional[int]:
        """
        Record a closed trade: match the open buy for `ticker`, compute the
        realized outcome (P&L, alpha, convergence) and a soundness verdict, and
        insert one closed_trades row.

        entry/exit prices and shares come from the broker position (Alpaca's
        avg_entry_price / current_price), not the queued order — a notional buy
        has no fill price at decision time. `benchmark_return` is fetched by the
        caller over the hold window (kept out of the DB layer so this module
        stays network-free and unit-testable offline). Returns the new
        closed_trades id, or None if no matching open buy exists.
        """
        open_buy = self.get_open_buy(ticker)
        if open_buy is None:
            logger.warning(f"close_trade: no open buy found for {ticker} — skipping")
            return None

        exit_date = exit_date or date.today().isoformat()
        entry_date = (open_buy.get("decided_at") or "")[:10] or None
        snapshot = open_buy.get("reasoning_snapshot") or {}
        entry_fair_value = snapshot.get("fair_value")
        tier_at_entry = open_buy.get("tier")

        cost_basis = entry_price * shares
        proceeds = exit_price * shares
        realized_pl = proceeds - cost_basis
        realized_pl_pct = (realized_pl / cost_basis) if cost_basis else None

        hold_days = None
        if entry_date:
            try:
                hold_days = (date.fromisoformat(exit_date) - date.fromisoformat(entry_date)).days
            except ValueError:
                hold_days = None

        alpha = None
        if realized_pl_pct is not None and benchmark_return is not None:
            alpha = realized_pl_pct - benchmark_return

        converged = None
        if entry_fair_value is not None:
            predicted_up = entry_fair_value > entry_price
            moved_up = exit_price > entry_price
            converged = 1 if predicted_up == moved_up else 0

        # Category comes from the *sell* reason, not the buy's. Look it up from
        # the exit decision unless the caller passed one explicitly.
        if sell_category is None:
            with _open(self.path) as conn:
                exit_row = conn.execute("SELECT reason FROM decision_log WHERE id = ?", (exit_decision_id,)).fetchone()
            exit_reason = (exit_row["reason"] if exit_row else "") or ""
            category = _classify_sell_reason(exit_reason)
        else:
            category = sell_category
        reasoning_sound = _score_reasoning_soundness(category, alpha, converged)

        with _open(self.path) as conn:
            cur = conn.execute(
                """
                INSERT INTO closed_trades
                    (ticker, entry_decision_id, exit_decision_id, tier_at_entry,
                     entry_date, exit_date, hold_days, entry_price, exit_price,
                     shares, cost_basis, proceeds, realized_pl, realized_pl_pct,
                     benchmark_return, alpha, entry_fair_value, converged,
                     sell_category, reasoning_sound, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ticker,
                    open_buy["id"],
                    exit_decision_id,
                    tier_at_entry,
                    entry_date,
                    exit_date,
                    hold_days,
                    entry_price,
                    exit_price,
                    shares,
                    cost_basis,
                    proceeds,
                    realized_pl,
                    realized_pl_pct,
                    benchmark_return,
                    alpha,
                    entry_fair_value,
                    converged,
                    category,
                    reasoning_sound,
                    notes,
                ),
            )
            new_id = cur.lastrowid
            return int(new_id) if new_id is not None else 0

    def get_decision_log(self, ticker: Optional[str] = None, limit: int = 100) -> list[dict]:
        """Return recent decisions (all tickers, or one), newest first."""
        with _open(self.path) as conn:
            if ticker:
                rows = conn.execute(
                    """
                    SELECT * FROM decision_log WHERE ticker = ?
                    ORDER BY decided_at DESC, id DESC LIMIT ?
                    """,
                    (ticker, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM decision_log ORDER BY decided_at DESC, id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            out = []
            for r in rows:
                d = dict(r)
                d["reasoning_snapshot"] = json.loads(d["reasoning_snapshot"]) if d["reasoning_snapshot"] else None
                out.append(d)
            return out

    def get_closed_trades(self, ticker: Optional[str] = None, limit: int = 100) -> list[dict]:
        """Return closed trades (all tickers, or one), most recently closed first."""
        with _open(self.path) as conn:
            if ticker:
                rows = conn.execute(
                    """
                    SELECT * FROM closed_trades WHERE ticker = ?
                    ORDER BY exit_date DESC, id DESC LIMIT ?
                    """,
                    (ticker, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM closed_trades ORDER BY exit_date DESC, id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [dict(r) for r in rows]

    def tier_performance(self) -> list[dict]:
        """
        Per-tier aggregates over closed trades: count, avg realized %, hit rate,
        avg hold days, avg alpha, and soundness rate. Tiers with no closed trades
        do not appear. soundness_rate is computed only over trades that have a
        non-null verdict.
        """
        with _open(self.path) as conn:
            rows = conn.execute(
                """
                SELECT
                    COALESCE(tier_at_entry, '?')                         AS tier,
                    COUNT(*)                                             AS n,
                    AVG(realized_pl_pct)                                 AS avg_realized_pct,
                    AVG(CASE WHEN realized_pl > 0 THEN 1.0 ELSE 0.0 END) AS hit_rate,
                    AVG(hold_days)                                       AS avg_hold_days,
                    AVG(alpha)                                           AS avg_alpha,
                    AVG(CASE WHEN reasoning_sound IS NOT NULL
                             THEN CAST(reasoning_sound AS REAL) END)     AS soundness_rate
                FROM closed_trades
                GROUP BY COALESCE(tier_at_entry, '?')
                ORDER BY tier
                """
            ).fetchall()
            return [dict(r) for r in rows]

    # ── Portfolio Snapshots, Regime Log & Income Events ───────────────────────

    def save_snapshot(
        self,
        account_id: str,
        *,
        currency: str,
        equity: float,
        cash: float,
        buying_power: Optional[float] = None,
        invested_value: Optional[float] = None,
        invested_pct: Optional[float] = None,
        equity_dkk: Optional[float] = None,
        positions: Optional[list[dict]] = None,
    ) -> int:
        """Append one point-in-time account snapshot. Never overwrites — the
        history is the point."""
        with _open(self.path) as conn:
            cur = conn.execute(
                """
                INSERT INTO portfolio_snapshots
                    (account_id, currency, equity, cash, buying_power,
                     invested_value, invested_pct, equity_dkk, positions)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_id,
                    currency,
                    equity,
                    cash,
                    buying_power,
                    invested_value,
                    invested_pct,
                    equity_dkk,
                    json.dumps(positions) if positions is not None else None,
                ),
            )
            new_id = cur.lastrowid
            return int(new_id) if new_id is not None else 0

    def get_snapshots(self, account_id: str, since: Optional[str] = None, limit: int = 365) -> list[dict]:
        """Return snapshots for one account, oldest first. `since` is an ISO
        date/timestamp lower bound on `as_of`."""
        with _open(self.path) as conn:
            if since:
                rows = conn.execute(
                    """
                    SELECT * FROM portfolio_snapshots
                    WHERE account_id = ? AND as_of >= ?
                    ORDER BY as_of ASC LIMIT ?
                    """,
                    (account_id, since, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM portfolio_snapshots
                    WHERE account_id = ?
                    ORDER BY as_of ASC LIMIT ?
                    """,
                    (account_id, limit),
                ).fetchall()
            out = []
            for r in rows:
                d = dict(r)
                d["positions"] = json.loads(d["positions"]) if d["positions"] else None
                out.append(d)
            return out

    def get_equity_curve(self, account_id: str) -> list[dict]:
        """Return (as_of, equity, equity_dkk) triples for one account, oldest
        first — the minimal series a chart or CLI table needs."""
        with _open(self.path) as conn:
            rows = conn.execute(
                """
                SELECT as_of, equity, equity_dkk FROM portfolio_snapshots
                WHERE account_id = ?
                ORDER BY as_of ASC
                """,
                (account_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def log_regime(
        self,
        regime: str,
        *,
        confidence: Optional[str] = None,
        market_pe: Optional[float] = None,
        vix: Optional[float] = None,
    ) -> int:
        """Append one market-regime classification. Called once per day
        alongside save_snapshot, independent of any specific account."""
        with _open(self.path) as conn:
            cur = conn.execute(
                """
                INSERT INTO regime_log (regime, confidence, market_pe, vix)
                VALUES (?, ?, ?, ?)
                """,
                (regime, confidence, market_pe, vix),
            )
            new_id = cur.lastrowid
            return int(new_id) if new_id is not None else 0

    def log_income_event(
        self,
        account_id: str,
        *,
        event_date: str,
        event_type: str,
        amount: float,
        currency: str,
        symbol: Optional[str] = None,
        withholding: Optional[float] = None,
    ) -> int:
        """Record a cash event outside of buy/sell trades (dividend,
        contribution, withdrawal, tax)."""
        if event_type not in ("dividend", "contribution", "withdrawal", "tax"):
            raise ValueError(f"event_type must be one of dividend/contribution/withdrawal/tax, got {event_type!r}")
        with _open(self.path) as conn:
            cur = conn.execute(
                """
                INSERT INTO income_events
                    (account_id, event_date, event_type, symbol, amount, currency, withholding)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (account_id, event_date, event_type, symbol, amount, currency, withholding),
            )
            new_id = cur.lastrowid
            return int(new_id) if new_id is not None else 0

    # ── Point-in-Time Fundamentals (EDGAR companyfacts) ───────────────────────

    def save_pit_fundamentals(self, records: list[dict]) -> int:
        """
        Bulk-insert originally-filed fundamental facts. Each record needs:
        ticker, cik, concept, period_end, fiscal_year, fiscal_period, form,
        value, filed_date, accession.

        Uses INSERT OR IGNORE against the (ticker, concept, period_end, form)
        primary key — callers should pass originally-filed values (the caller
        dedupes restatements to the earliest filed_date). Returns rows inserted.
        """
        if not records:
            return 0
        with _open(self.path) as conn:
            before = conn.total_changes
            conn.executemany(
                """
                INSERT OR IGNORE INTO pit_fundamentals
                    (ticker, cik, concept, period_end, fiscal_year, fiscal_period,
                     form, value, filed_date, accession)
                VALUES (:ticker, :cik, :concept, :period_end, :fiscal_year,
                        :fiscal_period, :form, :value, :filed_date, :accession)
                """,
                records,
            )
            return conn.total_changes - before

    def get_pit_fundamentals_asof(self, ticker: str, as_of_date: str, *, annual_only: bool = True) -> dict[str, float]:
        """
        Fundamentals for a ticker **as known at `as_of_date`**: for each concept,
        the value from the most recent period whose filing was public by that date
        (filed_date <= as_of_date). This is the look-ahead-free read the backtest
        uses. `annual_only` restricts to 10-K figures (consistent annual basis).
        """
        with _open(self.path) as conn:
            sql = """
                SELECT concept, value, period_end, filed_date FROM pit_fundamentals
                WHERE ticker = ? AND filed_date <= ?
            """
            params: list = [ticker, as_of_date]
            if annual_only:
                sql += " AND form = '10-K'"
            rows = conn.execute(sql, params).fetchall()

        # Per concept, keep the value from the latest period_end (tie-break: latest
        # filed_date) — i.e. the freshest figure that was public by as_of_date.
        best: dict[str, tuple] = {}
        for r in rows:
            if r["value"] is None:
                continue
            key = (r["period_end"], r["filed_date"])
            if r["concept"] not in best or key > best[r["concept"]][0]:
                best[r["concept"]] = (key, r["value"])
        return {concept: val for concept, (_, val) in best.items()}

    def get_pit_concept_series(self, ticker: str, concept: str) -> list[dict]:
        """Return the originally-filed time series for one (ticker, concept), oldest first."""
        with _open(self.path) as conn:
            rows = conn.execute(
                """
                SELECT * FROM pit_fundamentals
                WHERE ticker = ? AND concept = ?
                ORDER BY period_end ASC, filed_date ASC
                """,
                (ticker, concept),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_pit_tickers(self) -> list[str]:
        """Distinct tickers present in pit_fundamentals."""
        with _open(self.path) as conn:
            rows = conn.execute("SELECT DISTINCT ticker FROM pit_fundamentals ORDER BY ticker").fetchall()
            return [r["ticker"] for r in rows]

    # ── Data Retention ────────────────────────────────────────────────────────

    def run_retention_cleanup(self) -> dict[str, int]:
        """
        Prune old data per retention policy. Run weekly (Monday after fundamentals refresh).

        Retention rules:
        - fundamentals: keep weekly snapshots for 2 years; downsample older to monthly
        - haiku_screens: delete expired entries older than 1 year
        - news_events: keep 2 years
        - run_log, tier_history: keep forever (tiny, valuable for learning)
        """
        deleted: dict[str, int] = {}
        with _open(self.path) as conn:
            # Fundamentals: delete sub-monthly snapshots older than 2 years
            cur = conn.execute(
                """
                DELETE FROM fundamentals
                WHERE date < date('now', '-2 years')
                  AND strftime('%d', date) != '01'
                """
            )
            deleted["fundamentals"] = cur.rowcount

            # Haiku screens: delete expired entries older than 1 year
            cur = conn.execute(
                """
                DELETE FROM haiku_screens
                WHERE expires_at < date('now', '-1 year')
                """
            )
            deleted["haiku_screens"] = cur.rowcount

            # News events: keep 2 years
            cur = conn.execute(
                """
                DELETE FROM news_events
                WHERE detected_at < date('now', '-2 years')
                """
            )
            deleted["news_events"] = cur.rowcount

        logger.info(
            "Retention cleanup: deleted %d fundamentals, %d haiku_screens, %d news_events",
            deleted["fundamentals"],
            deleted["haiku_screens"],
            deleted["news_events"],
        )
        return deleted

    # ── Migration ─────────────────────────────────────────────────────────────

    def migrate_from_registry(self, registry_path: Path) -> int:
        """
        Import existing studies from the old registry.json into the new schema.

        Maps old integer tiers to new letter tiers:
            1 → S  (Wonderful at fair value — keep as best tier)
            2 → B  (High quality but overpriced → Watch)
            3 → C  (Moderate quality → Monitor)
            0 → C  (Excluded)

        Returns number of studies imported.
        """
        if not registry_path.exists():
            logger.info("No registry.json found at %s — skipping migration", registry_path)
            return 0

        import json as _json

        with open(registry_path) as f:
            registry = _json.load(f)

        studies = registry.get("studies", {})
        if not studies:
            logger.info("registry.json has no studies — nothing to migrate")
            return 0

        TIER_MAP = {1: "S", 2: "B", 3: "C", 0: "C"}
        count = 0

        for ticker, entry in studies.items():
            try:
                old_tier = entry.get("tier", 0)
                new_tier = TIER_MAP.get(old_tier, "C")
                analysis = entry.get("analysis", {})

                # Upsert into universe
                self.upsert_universe_stock(
                    ticker,
                    company_name=entry.get("company_name"),
                    sector=entry.get("sector"),
                    source="finviz_screen",  # best guess for legacy data
                    quality_score=entry.get("screener_score", 0) * 100,  # normalize 0-1 → 0-100
                )

                # Save deep analysis
                conviction = analysis.get("conviction", "LOW")
                moat_str = analysis.get("moat_rating", "none")
                moat_rating = moat_str.upper() if moat_str else "NONE"

                self.save_deep_analysis(
                    ticker,
                    tier=new_tier,
                    conviction=conviction,
                    moat_rating=moat_rating,
                    moat_sources=analysis.get("moat_sources", []),
                    fair_value=analysis.get("estimated_fair_value_low"),
                    target_entry=entry.get("target_entry_price"),
                    investment_thesis=analysis.get("investment_thesis") or analysis.get("summary", ""),
                    key_risks=analysis.get("key_risks", []),
                    thesis_breakers=analysis.get("thesis_risks", []),
                    expires_days=1,  # immediately expiring so they get re-analyzed
                )

                # Log initial tier assignment
                self.log_tier_change(
                    ticker,
                    new_tier=new_tier,
                    old_tier=None,
                    trigger="bulk_load",
                    reason=f"Migrated from legacy registry (old tier {old_tier})",
                )
                count += 1
            except Exception as exc:
                logger.warning("Failed to migrate %s: %s", ticker, exc)

        logger.info("Migrated %d studies from registry.json", count)
        return count

    # ── Scheduling Queries ────────────────────────────────────────────────────

    def get_unscreened_tickers(self, limit: int = 30) -> list[str]:
        """
        Return universe tickers with no valid (non-expired) Haiku result,
        ranked by quality score descending.

        Covers two cases:
        - ticker has no haiku_screens row at all
        - ticker's most recent haiku result is expired
        """
        with _open(self.path) as conn:
            rows = conn.execute(
                """
                SELECT u.ticker
                FROM universe u
                WHERE u.in_universe = 1
                  AND NOT EXISTS (
                      SELECT 1 FROM haiku_screens h
                      WHERE h.ticker = u.ticker
                        AND h.expires_at > datetime('now')
                  )
                ORDER BY u.quality_score DESC NULLS LAST
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [r["ticker"] for r in rows]

    def get_expiring_haiku_tickers(self, within_days: int = 30, limit: int = 20) -> list[str]:
        """
        Return tickers whose latest Haiku result expires within N days
        (but has not yet expired).  Ranked by quality score descending.

        These are candidates for re-screening on Wednesday before their
        cached result goes stale.
        """
        cutoff = (datetime.now() + timedelta(days=within_days)).isoformat()
        with _open(self.path) as conn:
            rows = conn.execute(
                """
                SELECT u.ticker
                FROM universe u
                JOIN haiku_screens h ON h.ticker = u.ticker
                WHERE u.in_universe = 1
                  AND h.screened_at = (
                      SELECT MAX(screened_at) FROM haiku_screens h2
                      WHERE h2.ticker = u.ticker
                  )
                  AND h.expires_at > datetime('now')   -- still valid, not yet expired
                  AND h.expires_at <= ?                -- but expiring soon
                ORDER BY u.quality_score DESC NULLS LAST
                LIMIT ?
                """,
                (cutoff, limit),
            ).fetchall()
            return [r["ticker"] for r in rows]

    def get_haiku_passes_without_analysis(self, limit: int = 10) -> list[str]:
        """
        Return tickers whose latest Haiku result passed but have no valid
        (non-expired) deep analysis.  Ranked by quality score descending.

        These are the Friday Sonnet batch candidates: stocks that cleared
        the Haiku gate and are waiting for a full Sonnet deep-dive.
        """
        with _open(self.path) as conn:
            rows = conn.execute(
                """
                SELECT u.ticker
                FROM universe u
                JOIN haiku_screens h ON h.ticker = u.ticker
                WHERE u.in_universe = 1
                  AND h.screened_at = (
                      SELECT MAX(screened_at) FROM haiku_screens h2
                      WHERE h2.ticker = u.ticker
                  )
                  AND h.passed = 1
                  AND h.expires_at > datetime('now')
                  AND NOT EXISTS (
                      SELECT 1 FROM deep_analyses da
                      WHERE da.ticker = u.ticker
                        AND da.expires_at > datetime('now')
                  )
                ORDER BY u.quality_score DESC NULLS LAST
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [r["ticker"] for r in rows]

    def get_portfolio_tickers_needing_analysis(self, *, recent_buy_days: int = 14) -> list[str]:
        """
        Held positions plus recent auto-trade buys that lack a valid
        (non-expired) deep analysis.

        These jump the Friday Sonnet queue ahead of the quality-ranked
        universe: the deployment engine sizes buys and evaluates sells by
        tier, so an untiered holding is invisible to the sell logic and a
        fresh buy stays at the small unranked weight forever. Being in the
        portfolio is a stronger reason to analyze than a Haiku pass, so the
        Haiku gate is intentionally not required here.

        Held = paper_positions (synced Monday) UNION recent decision_log
        buys not since sold — the latter covers the gap between a Friday
        buy and the following Monday's position sync.
        """
        with _open(self.path) as conn:
            rows = conn.execute(
                """
                SELECT t.ticker FROM (
                    SELECT ticker FROM paper_positions
                    UNION
                    SELECT d.ticker FROM decision_log d
                    WHERE d.action = 'buy'
                      AND d.decided_at >= datetime('now', ?)
                      AND NOT EXISTS (
                          SELECT 1 FROM decision_log s
                          WHERE s.ticker = d.ticker
                            AND s.action = 'sell'
                            AND (s.decided_at > d.decided_at
                                 OR (s.decided_at = d.decided_at AND s.id > d.id))
                      )
                ) t
                WHERE NOT EXISTS (
                    SELECT 1 FROM deep_analyses da
                    WHERE da.ticker = t.ticker
                      AND da.expires_at > datetime('now')
                )
                ORDER BY t.ticker
                """,
                (f"-{recent_buy_days} days",),
            ).fetchall()
            return [r["ticker"] for r in rows]

    # ── Priority Queue ────────────────────────────────────────────────────────

    def get_priority_queue(self) -> list[dict]:
        """
        Return stocks ranked by analysis priority for Haiku/Sonnet scheduling.

        Priority levels (lower number = higher priority):
            0: Conviction list + never analyzed
            1: High quality score + never analyzed
            2: High quality score + analysis expiring within 30 days
            3: Previously S/A tier + price moved significantly
            4: Medium quality score + never analyzed
            5: Low quality score — deferred
        """
        with _open(self.path) as conn:
            rows = conn.execute(
                """
                SELECT
                    u.ticker,
                    u.company_name,
                    u.source,
                    u.quality_score,
                    u.cap_category,
                    da.tier AS current_tier,
                    da.analyzed_at AS last_analyzed_at,
                    da.expires_at,
                    CASE
                        WHEN u.source = 'conviction' AND da.ticker IS NULL                  THEN 0
                        WHEN u.source = 'conviction'                                         THEN 0
                        WHEN da.ticker IS NULL AND u.quality_score >= 70                    THEN 1
                        WHEN da.expires_at <= date('now', '+30 days')
                             AND u.quality_score >= 70                                       THEN 2
                        WHEN da.tier IN ('S','A')                                            THEN 3
                        WHEN da.ticker IS NULL AND u.quality_score >= 40                    THEN 4
                        ELSE 5
                    END AS priority
                FROM universe u
                LEFT JOIN (
                    SELECT ticker, tier, analyzed_at, expires_at
                    FROM deep_analyses
                    WHERE (ticker, analyzed_at) IN (
                        SELECT ticker, MAX(analyzed_at) FROM deep_analyses GROUP BY ticker
                    )
                ) da ON u.ticker = da.ticker
                WHERE u.in_universe = 1
                ORDER BY priority ASC, u.quality_score DESC NULLS LAST
                """
            ).fetchall()
            return [dict(r) for r in rows]
