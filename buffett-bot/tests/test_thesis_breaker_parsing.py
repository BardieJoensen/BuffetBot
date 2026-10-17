"""
Thesis-breaker parsing hygiene (ruleset 3.0.0).

A pre-registered breaker is the only news event that can force a sell, so a
stored item must be one observable event: no markdown leftovers, no section
headings, no watch-list items smuggled in under a "monitor closely" heading.
"""

import json
import sqlite3

from scripts.clean_thesis_breakers import main as clean_main
from scripts.clean_thesis_breakers import plan_changes
from src.analysis_parser import (
    extract_list,
    extract_thesis_breakers,
    is_heading_like,
    normalize_thesis_breakers,
    parse_analysis,
)
from src.database import Database, _open
from tests.fixtures_analysis import well_formed_analysis


class TestExtractList:
    def test_bold_title_prefix_is_stripped_cleanly(self):
        """The production artifact: '**Dividend cut** - text' became 'Dividend cut** - text'."""
        items = extract_list("- **Dividend cut** - would kill the bull case\n- **Fraud**: restatement")
        assert items == ["Dividend cut - would kill the bull case", "Fraud: restatement"]

    def test_numbered_items_and_plain_items_unchanged(self):
        items = extract_list("1. First risk\n2. Second risk\n- third")
        assert items == ["First risk", "Second risk", "third"]


class TestHeadingDetection:
    def test_headings(self):
        for heading in ("Sell Immediately If:", "Events that would signal SELL IMMEDIATELY", "Monitor Closely:", ""):
            assert is_heading_like(heading), heading

    def test_events_are_not_headings(self):
        assert not is_heading_like("Operating margin reported below 12% for two consecutive quarters")
        assert not is_heading_like("Sell-side downgrade follows a 30% revenue miss reported in Q3")


class TestExtractThesisBreakers:
    SECTION = """
[What specific events would invalidate the thesis]
**Sell Immediately If:**
1. **HOKA growth falls below 8%** for two consecutive quarters while under $3B revenue
2. UGG revenue declines >15% year-over-year in a single quarter outside a recession
**Monitor Closely:**
3. Inventory days rise above 120, which would suggest demand softening
- Tariff headlines
"""

    def test_keeps_events_under_sell_heading_and_drops_watch_items(self):
        assert extract_thesis_breakers(self.SECTION) == [
            "HOKA growth falls below 8% for two consecutive quarters while under $3B revenue",
            "UGG revenue declines >15% year-over-year in a single quarter outside a recession",
        ]

    def test_resumes_after_a_later_sell_heading(self):
        text = "Monitor:\n- Inventory creeping up over time\nSell immediately:\n- Dividend suspended by the board"
        assert extract_thesis_breakers(text) == ["Dividend suspended by the board"]

    def test_drops_fragments(self):
        assert extract_thesis_breakers("1. Margin\n2. A credit rating downgrade below BB announced") == [
            "A credit rating downgrade below BB announced"
        ]

    def test_golden_fixture_still_yields_two_breakers(self):
        a = parse_analysis("AAPL", "Apple Inc", well_formed_analysis(), "Technology")
        assert len(a.thesis_risks) == 2
        assert all("**" not in item for item in a.thesis_risks)


class TestNormalizeStored:
    def test_strips_markup_and_drops_headings(self):
        stored = [
            "Sell Immediately If:**",
            "Dividend cut or buyback suspension** - Only thing supporting the stock",
            "Monitor Closely:**",
            "Margin",
            "Loss of a top-five insurer relationship announced",
        ]
        assert normalize_thesis_breakers(stored) == [
            "Dividend cut or buyback suspension - Only thing supporting the stock",
            "Loss of a top-five insurer relationship announced",
        ]

    def test_idempotent(self):
        clean = ["Loss of a top-five insurer relationship announced"]
        assert normalize_thesis_breakers(clean) == clean


class TestCleanerScript:
    def _seed(self, tmp_path):
        db = Database(db_path=tmp_path / "t.db")
        db.save_deep_analysis(
            "DECK",
            tier="B",
            thesis_breakers=["Sell Immediately If:**", "HOKA growth falls below 8%** for two consecutive quarters"],
        )
        db.save_deep_analysis("OK", tier="A", thesis_breakers=["Loss of the AT&T contract announced"])
        return db

    def test_dry_run_changes_nothing(self, tmp_path, capsys):
        db = self._seed(tmp_path)
        assert clean_main(["--db", str(db.path)]) == 0
        out = capsys.readouterr().out
        assert "rows that change: 1" in out and "DRY RUN" in out
        assert db.get_latest_deep_analysis("DECK")["thesis_breakers"][0] == "Sell Immediately If:**"

    def test_apply_rewrites_rows_and_backs_up(self, tmp_path):
        db = self._seed(tmp_path)
        assert clean_main(["--db", str(db.path), "--apply"]) == 0
        assert db.get_latest_deep_analysis("DECK")["thesis_breakers"] == [
            "HOKA growth falls below 8% for two consecutive quarters"
        ]
        assert db.get_latest_deep_analysis("OK")["thesis_breakers"] == ["Loss of the AT&T contract announced"]
        backups = list((tmp_path / "backups").glob("*.pre-clean-breakers.*.db"))
        assert len(backups) == 1
        conn = sqlite3.connect(str(backups[0]))
        raw = conn.execute("SELECT thesis_breakers FROM deep_analyses WHERE ticker='DECK'").fetchone()[0]
        conn.close()
        assert json.loads(raw)[0] == "Sell Immediately If:**"
        # Second apply is a no-op
        with _open(db.path) as conn:
            assert plan_changes(conn) == []
