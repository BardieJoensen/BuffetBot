"""
Regression tests for the fixes that followed the 2026-10-19 audit
(docs/research/BUFFETTBOT_AUDIT.md). Each test names the audit item it pins.
"""

import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

_anthropic_mock = MagicMock()
for _pkg in ("schedule", "dotenv", "anthropic", "anthropic.types", "anthropic.lib", "anthropic.lib.streaming"):
    sys.modules.setdefault(_pkg, _anthropic_mock)

from src.database import Database, _open  # noqa: E402
from src.screener import StockScreener, load_criteria_from_yaml  # noqa: E402


@pytest.fixture
def db(tmp_path):
    return Database(db_path=tmp_path / "test.db")


def _mock_data(symbol, market_cap=10e9):
    return {
        "symbol": symbol,
        "name": f"{symbol} Corp",
        "quote_type": "EQUITY",
        "price": 100.0,
        "market_cap": market_cap,
        "pe_ratio": 20.0,
        "debt_equity": 0.3,
        "roe": 0.18,
        "revenue_growth": 0.10,
        "current_ratio": 1.5,
        "sector": "Technology",
        "industry": "Software",
        "beta": 1.0,
        "dividend_yield": None,
        "profit_margin": 0.2,
        "52_week_high": 120.0,
        "52_week_low": 80.0,
        "free_cashflow": 2e9,
        "operating_cashflow": 3e9,
        "net_income": 2.5e9,
        "payout_ratio": 0.25,
        "operating_margin": 0.25,
        "avg_volume": 5_000_000,
        "fcf_yield": 0.04,
        "earnings_quality": 0.9,
        "real_fcf_yield": 0.04,
        "sbc_ratio": 0.01,
    }


# ─── A1 / L1: Monday persists every scored name ───────────────────────────────


class TestScreenTickersTopN:
    def _screen(self, tickers, **kw):
        screener = StockScreener()
        criteria = load_criteria_from_yaml()  # truncation only runs when scoring rules exist
        criteria.top_n = 2
        with (
            patch.object(screener, "_fetch_stock_data", side_effect=lambda s: _mock_data(s)),
            patch.object(screener, "_get_cached_data", return_value=None),
            patch.object(screener, "_fetch_historical_data", return_value={}),
        ):
            return screener.screen_tickers(tickers, criteria, **kw)

    def test_default_keeps_criteria_top_n(self):
        assert len(self._screen(["A", "B", "C", "D"])) == 2

    def test_zero_returns_every_passing_name(self):
        assert len(self._screen(["A", "B", "C", "D"], top_n=0)) == 4

    def test_explicit_override(self):
        assert len(self._screen(["A", "B", "C", "D"], top_n=3)) == 3


class TestClearQualityScores:
    def test_clears_only_names_not_rescored(self, db):
        db.upsert_universe_stock("FRESH", source="finviz_screen", quality_score=55.0)
        db.upsert_universe_stock("STALE", source="finviz_screen", quality_score=98.5)
        db.upsert_universe_stock("NEVER", source="finviz_screen")
        cleared = db.clear_quality_scores_except(["FRESH"])
        assert cleared == 1
        assert db.get_universe_stock("FRESH")["quality_score"] == pytest.approx(55.0)
        assert db.get_universe_stock("STALE")["quality_score"] is None
        assert db.get_universe_stock("NEVER")["quality_score"] is None


# ─── B3 / L5: hard filters reach the paid queues ──────────────────────────────


class TestQueuesSkipUnscoredNames:
    def _haiku(self, db, ticker, passed=True, days=180):
        with _open(db.path) as conn:
            conn.execute(
                "INSERT INTO haiku_screens (ticker, screened_at, passed, moat_estimate, summary, expires_at)"
                " VALUES (?, ?, ?, 'WIDE', 's', ?)",
                (ticker, datetime.now().isoformat(), int(passed), (datetime.now() + timedelta(days=days)).isoformat()),
            )

    def test_unscreened_queue(self, db):
        db.upsert_universe_stock("SCORED", source="finviz_screen", quality_score=60.0)
        db.upsert_universe_stock("TOOHARD", source="finviz_screen")  # failed hard filters: no score
        q = db.get_unscreened_tickers()
        assert "SCORED" in q and "TOOHARD" not in q

    def test_expiring_queue(self, db):
        db.upsert_universe_stock("SCORED", source="finviz_screen", quality_score=60.0)
        db.upsert_universe_stock("TOOHARD", source="finviz_screen")
        self._haiku(db, "SCORED", days=10)
        self._haiku(db, "TOOHARD", days=10)
        q = db.get_expiring_haiku_tickers(within_days=30)
        assert "SCORED" in q and "TOOHARD" not in q

    def test_sonnet_queue(self, db):
        db.upsert_universe_stock("SCORED", source="finviz_screen", quality_score=60.0)
        db.upsert_universe_stock("TOOHARD", source="finviz_screen")
        self._haiku(db, "SCORED")
        self._haiku(db, "TOOHARD")
        q = db.get_haiku_passes_without_analysis(limit=10)
        assert "SCORED" in q and "TOOHARD" not in q


# ─── A2: the prompt carries the as-of date ────────────────────────────────────


class TestDbSummaryAsOf:
    def test_as_of_date_is_printed(self, db):
        from scripts.scheduler import _build_db_summary

        db.upsert_universe_stock("AAPL", company_name="Apple", sector="Technology", source="sp500_filter")
        db.save_fundamentals("AAPL", {"price": 180.0, "roe": 1.2}, as_of_date="2026-06-29")
        summary = _build_db_summary("AAPL", db)
        assert "as of 2026-06-29" in summary
        assert "Current Price" not in summary


# ─── A6: NaN oldest statement column ──────────────────────────────────────────


class TestTrendMetricsIgnoreNaNColumn:
    def _ticker(self, revenue, net_income):
        cols = pd.to_datetime(["2025-12-31", "2024-12-31", "2023-12-31", "2022-12-31", "2021-12-31"])
        fin = pd.DataFrame(
            {c: [r, n] for c, r, n in zip(cols, revenue, net_income)}, index=["Total Revenue", "Net Income"]
        )
        t = MagicMock()
        t.financials = fin
        t.balance_sheet = pd.DataFrame()
        t.cashflow = pd.DataFrame()
        return t

    def test_revenue_cagr_and_consistency_skip_nan(self):
        screener = StockScreener()
        t = self._ticker([200.0, 180.0, 150.0, 100.0, np.nan], [20.0, 18.0, 15.0, 10.0, np.nan])
        out = screener._fetch_historical_data("X", t)
        assert out["revenue_cagr"] == pytest.approx((200 / 100) ** (1 / 3) - 1)
        assert out["earnings_consistency"] == 3.0


# ─── A4 / L2: universe validation and the DB-driven weekly screen ─────────────


class TestUniverseValidation:
    def test_doubled_first_letter_list_is_rejected(self):
        from src.universe import looks_like_valid_universe

        assert not looks_like_valid_universe(["AAAPL", "AABBV", "AABNB", "UUSLM", "ZZ"])

    def test_normal_list_is_accepted(self):
        from src.universe import looks_like_valid_universe

        assert looks_like_valid_universe(["AAPL", "ABBV", "BRK-B", "CWEN-A", "BF.B", "AA", "KKR"])

    def test_garbage_is_rejected(self):
        from src.universe import looks_like_valid_universe

        assert not looks_like_valid_universe([])
        assert not looks_like_valid_universe(["<a href>", "123", "aapl", None, "MSFT"])

    def test_malformed_cache_is_ignored(self, tmp_path):
        import json

        from src import universe

        universe.set_cache_dir(tmp_path)
        (tmp_path / "stock_universe.json").write_text(
            json.dumps({"tickers": ["AAAPL", "AABBV", "AABNB"], "cached_at": datetime.now().isoformat()})
        )
        assert universe._load_cached_universe() is None


class TestWeeklyScreenUsesDbUniverse:
    def test_screens_db_universe_with_force_sets(self, db):
        from scripts.scheduler import _screen_db_universe

        for i in range(60):
            db.upsert_universe_stock(f"T{i:02d}", source="finviz_screen")
        db.upsert_universe_stock("V", source="conviction")
        db.upsert_universe_stock("AAPL", source="sp500_filter")
        screener = MagicMock()
        screener.screen_tickers.return_value = ["screened"]
        with patch("src.database.Database", return_value=db):
            out = _screen_db_universe(screener, criteria="crit")
        assert out == ["screened"]
        args, kwargs = screener.screen_tickers.call_args
        assert "V" in args[0] and "AAPL" in args[0] and len(args[0]) == 62
        assert kwargs["force_include"] == {"V"} and kwargs["force_large_cap"] == {"AAPL"}

    def test_falls_back_when_db_universe_is_small(self, db):
        from scripts.scheduler import _screen_db_universe

        db.upsert_universe_stock("ONLY", source="finviz_screen")
        with patch("src.database.Database", return_value=db):
            assert _screen_db_universe(MagicMock(), criteria="crit") is None


# ─── E5: snapshot plausibility gate ───────────────────────────────────────────


def _state(equity, cash=1000.0):
    from datetime import timezone

    from src.accounts.base import AccountState

    return AccountState(
        account_id="alpaca_paper",
        currency="USD",
        equity=equity,
        cash=cash,
        buying_power=cash,
        invested_value=equity - cash,
        invested_pct=(equity - cash) / equity if equity else 0.0,
        as_of=datetime.now(timezone.utc),
    )


def _pos(symbol, mv=10_000.0):
    from src.accounts.base import PositionState

    return PositionState(
        symbol=symbol,
        shares=10.0,
        avg_cost=100.0,
        price=mv / 10,
        market_value=mv,
        unrealized_pl=0.0,
        unrealized_pl_pct=0.0,
    )


def _backdate_latest_snapshot(db, days=1):
    with _open(db.path) as conn:
        conn.execute(
            "UPDATE portfolio_snapshots SET as_of = ? WHERE id = (SELECT MAX(id) FROM portfolio_snapshots)",
            ((datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S"),),
        )


class TestSnapshotAnomaly:
    def _seed(self, db, symbols, equity=100_000.0):
        db.save_snapshot(
            "alpaca_paper",
            currency="USD",
            equity=equity,
            cash=1000.0,
            positions=[{"symbol": s, "market_value": 10_000.0} for s in symbols],
        )
        _backdate_latest_snapshot(db)

    def test_no_history_is_plausible(self, db):
        from scripts.scheduler import _snapshot_anomaly

        assert _snapshot_anomaly(db, "alpaca_paper", _state(100_000.0), [_pos("A")]) is None

    def test_vanished_positions_without_a_sell_are_suspect(self, db):
        from scripts.scheduler import _snapshot_anomaly

        self._seed(db, ["AGM", "AIZ", "CCI", "CPRT"])
        reason = _snapshot_anomaly(db, "alpaca_paper", _state(58_000.0), [_pos("AGM"), _pos("CPRT")])
        assert reason and "AIZ" in reason and "CCI" in reason

    def test_vanished_position_with_a_journaled_sell_is_fine(self, db):
        from scripts.scheduler import _snapshot_anomaly

        self._seed(db, ["AGM", "AIZ"], equity=20_000.0)
        db.log_decision("AIZ", "sell", account_id="alpaca_paper", order_status="submitted")
        assert _snapshot_anomaly(db, "alpaca_paper", _state(19_500.0), [_pos("AGM")]) is None

    def test_equity_jump_without_orders_is_suspect(self, db):
        from scripts.scheduler import _snapshot_anomaly

        self._seed(db, ["AGM"], equity=100_000.0)
        reason = _snapshot_anomaly(db, "alpaca_paper", _state(58_000.0), [_pos("AGM")])
        assert reason and "equity moved" in reason

    def test_normal_day_is_plausible(self, db):
        from scripts.scheduler import _snapshot_anomaly

        self._seed(db, ["AGM"], equity=100_000.0)
        assert _snapshot_anomaly(db, "alpaca_paper", _state(101_500.0), [_pos("AGM")]) is None


class TestSuspectSnapshotsAreSkipped:
    def test_readers_skip_suspect_rows_by_default(self, db):
        db.save_snapshot("alpaca_paper", currency="USD", equity=100_000.0, cash=1.0)
        _backdate_latest_snapshot(db, days=2)
        db.save_snapshot("alpaca_paper", currency="USD", equity=58_000.0, cash=1.0, suspect_reason="3 of 8 positions")
        _backdate_latest_snapshot(db, days=1)
        db.save_snapshot("alpaca_paper", currency="USD", equity=101_000.0, cash=1.0)

        assert [s["equity"] for s in db.get_snapshots("alpaca_paper")] == [100_000.0, 101_000.0]
        assert [s["equity"] for s in db.get_snapshots("alpaca_paper", include_suspect=True)] == [
            100_000.0,
            58_000.0,
            101_000.0,
        ]
        assert db.get_latest_snapshots("alpaca_paper", limit=3)[1]["equity"] == 100_000.0
        assert [e["equity"] for e in db.get_equity_curve("alpaca_paper")] == [100_000.0, 101_000.0]
        flagged = db.get_latest_snapshots("alpaca_paper", limit=3, include_suspect=True)[1]
        assert flagged["suspect"] == 1 and flagged["suspect_reason"] == "3 of 8 positions"


# ─── D2: bold and bulleted labels ─────────────────────────────────────────────


class TestExtractFieldMarkdown:
    def test_bold_label(self):
        from src.analysis_parser import extract_field

        assert extract_field("**Durability:** MODERATE — strong brand", "Durability") == "MODERATE — strong brand"

    def test_bold_label_colon_outside(self):
        from src.analysis_parser import extract_field

        assert extract_field("**Type**: brand + switching costs", "Type") == "brand + switching costs"

    def test_bulleted_label_and_bold_value(self):
        from src.analysis_parser import extract_field

        assert extract_field("- Target Entry Price: **$80**", "Target Entry Price") == "$80"

    def test_plain_label_unchanged(self):
        from src.analysis_parser import extract_field

        assert extract_field("Durability: STRONG", "Durability") == "STRONG"


def _analysis_text(bold: bool) -> str:
    def L(k):
        return f"**{k}:**" if bold else f"{k}:"

    return f"""## MOAT CLASSIFICATION
{L("Type")} brand + switching costs
{L("Durability")} MODERATE
{L("Risks")} competition
## MANAGEMENT QUALITY
{L("Capital Allocation")} GOOD
{L("Insider Ownership")} 5%
{L("Summary")} fine
## BUSINESS DURABILITY
{L("Recession Resilience")} ok
{L("Existential Risks")} none
{L("10-Year Outlook")} larger
## BEAR CASE
{L("Customer Concentration")} LOW
{L("Switching Cost Test")} 4
{L("Regulatory/Tech Risk")} low
{L("Patent/IP Dependency")} NO
{L("Bear Case Summary")} meh
## CURRENCY EXPOSURE
{L("Domestic Revenue")} 60%
{L("International Revenue")} 40%
{L("Risk Level")} MODERATE
{L("Confidence")} HIGH
## FAIR VALUE ASSESSMENT
{L("Estimated Fair Value")} $100 - $120
{L("Target Entry Price")} $80
## CONVICTION LEVEL
MEDIUM - fine
## INVESTMENT SUMMARY
Good business.
## KEY RISKS
1. a
2. b
## THESIS-BREAKING RISKS
1. Operating margin reported below 12% for two consecutive quarters
2. Loss of the top customer announced
## TOTAL RETURN POTENTIAL
10%/yr
## DIVIDEND YIELD
1.2%
"""


class TestBoldAnalysisParsesLikePlain:
    def test_bold_and_plain_agree(self):
        from src.analysis_parser import parse_analysis

        plain = parse_analysis("T", "Test Co", _analysis_text(bold=False))
        bold = parse_analysis("T", "Test Co", _analysis_text(bold=True))
        for attr in (
            "moat_durability",
            "target_entry_price",
            "estimated_fair_value_low",
            "estimated_fair_value_high",
            "customer_concentration_risk",
            "switching_cost_rating",
        ):
            assert getattr(bold, attr) == getattr(plain, attr), attr
        assert bold.target_entry_price == 80.0
        assert bold.moat_rating.value == "narrow"


# ─── A5: the 10-K slicer picks the real section ───────────────────────────────


class TestEdgarSectionSlicer:
    def _filing(self):
        toc = (
            "table of contents item 1. business 3 item 1a. risk factors 10 item 1b. unresolved staff comments 20 "
            "item 2. properties 21 item 7. management's discussion and analysis 30 item 7a. quantitative and "
            "qualitative disclosures 40 item 8. financial statements and supplementary data 41 item 9. changes in "
            "and disagreements 80 item 9a. controls and procedures 80 item 9b. other information 81 item 10. "
            "directors 82 item 11. executive compensation 82 item 12. security ownership 82 item 13. certain "
            "relationships 83 item 14. principal accountant fees 83 item 15. exhibits 84 item 16. form 10-k summary 90 "
        )
        business = "Item 1. Business REALBUSINESS " + ("we sell widgets to everyone. " * 60)
        risk = "Item 1A. Risk Factors REALRISK " + ("competition could hurt us. " * 60)
        item1b = "Item 1B. Unresolved Staff Comments none. Item 2. Properties we own a plant. " + ("x " * 400)
        mda = "Item 7. Management's Discussion and Analysis REALMDA " + ("revenue grew nicely this year. " * 60)
        item7a = "Item 7A. Quantitative and Qualitative Disclosures about market risk " + ("y " * 400)
        notes = (
            "notes to the financial statements. as discussed in item 7. management's discussion and analysis "
            "of our annual report on form 10-k for the prior year, which was filed with the sec, " + ("z " * 400)
        )
        item8 = "Item 8. Financial Statements and Supplementary Data " + ("w " * 400)
        return toc + business + risk + item1b + mda + item7a + notes + item8

    def test_sections_start_at_the_real_headers(self):
        from src.edgar_fetcher import _extract_sections

        sections = _extract_sections(self._filing())
        assert sections["business"].startswith("Item 1. Business REALBUSINESS")
        assert sections["risk_factors"].startswith("Item 1A. Risk Factors REALRISK")
        assert sections["mda"].startswith("Item 7. Management's Discussion and Analysis REALMDA")
        assert "REALMDA" in sections["mda"] and "notes to the financial" not in sections["mda"]
