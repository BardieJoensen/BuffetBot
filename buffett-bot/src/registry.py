"""
Studied Companies Registry

Persistent database of all deeply analyzed companies, with campaign tracking
for systematic coverage of the stock universe over time.

Storage: data/registry.json — single JSON file with atomic writes.
Also writes individual analysis files to data/analyses/ for backward compat.
"""

import json
import logging
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def _quarter_id(dt: Optional[datetime] = None) -> str:
    """Generate a quarter-based campaign ID like '2026-Q1'."""
    dt = dt or datetime.now()
    q = (dt.month - 1) // 3 + 1
    return f"{dt.year}-Q{q}"


class Registry:
    """
    Tracks all deeply studied companies and manages coverage campaigns.

    A campaign tracks which stocks have been Haiku-screened and deep-analyzed
    during a quarter. When >90% of the universe is screened, a new campaign
    starts. Studies persist across campaigns.
    """

    FILENAME = "registry.json"

    def __init__(self, data_dir: Optional[Path] = None):
        self.data_dir = data_dir or Path("./data")
        self._path = self.data_dir / self.FILENAME
        self._analyses_dir = self.data_dir / "analyses"
        self._analyses_dir.mkdir(parents=True, exist_ok=True)
        self._data = self._load()

    def _load(self) -> dict:
        """Load registry from disk or create empty."""
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text())
                if data.get("version") == 1:
                    return data
                logger.warning("Registry version mismatch, starting fresh")
            except (json.JSONDecodeError, KeyError) as exc:
                logger.warning(f"Corrupt registry, starting fresh: {exc}")
        return self._empty_registry()

    def _empty_registry(self) -> dict:
        """Create an empty registry with a new campaign."""
        return {
            "version": 1,
            "campaign": {
                "campaign_id": _quarter_id(),
                "started_at": datetime.now().isoformat(),
                "haiku_screened": [],
                "haiku_passed": [],
                "haiku_failed": {},  # {symbol: screened_at ISO string}
                "analyzed": [],
            },
            "studies": {},
        }

    def save(self) -> None:
        """Atomic write: write to temp file then rename."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        try:
            fd, tmp_path = tempfile.mkstemp(dir=str(self.data_dir), suffix=".tmp", prefix="registry_")
            with os.fdopen(fd, "w") as f:
                json.dump(self._data, f, indent=2, default=str)
            os.replace(tmp_path, str(self._path))
        except Exception:
            # Clean up temp file on failure
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    # ── Lookups ──────────────────────────────────────────────────

    def get_studied(self, symbol: str) -> Optional[dict]:
        """Return study entry for a symbol, or None."""
        return self._data["studies"].get(symbol)

    def get_all_studied(self) -> dict[str, dict]:
        """Return all study entries."""
        return dict(self._data["studies"])

    # ── Campaign management ──────────────────────────────────────

    @property
    def campaign(self) -> dict:
        return self._data["campaign"]

    def get_unstudied_symbols(self, all_symbols: list[str]) -> list[str]:
        """Symbols not yet Haiku-screened in the current campaign."""
        screened = set(self.campaign["haiku_screened"])
        return [s for s in all_symbols if s not in screened]

    def get_unanalyzed_haiku_passed(self) -> list[str]:
        """Symbols that passed Haiku but haven't been deep-analyzed this campaign."""
        analyzed = set(self.campaign["analyzed"])
        return [s for s in self.campaign["haiku_passed"] if s not in analyzed]

    def mark_haiku_screened(self, symbols: list[str], results: list[dict], min_score: int = 5) -> None:
        """
        Record Haiku screening results. Symbols scoring >= min_score
        go to haiku_passed, others to haiku_failed (with timestamp).
        """
        campaign = self.campaign
        screened_set = set(campaign["haiku_screened"])
        passed_set = set(campaign["haiku_passed"])
        # haiku_failed is a dict {symbol: screened_at}; migrate from old list format
        failed_dict = campaign["haiku_failed"]
        if isinstance(failed_dict, list):
            failed_dict = {s: campaign.get("started_at", datetime.now().isoformat()) for s in failed_dict}
            campaign["haiku_failed"] = failed_dict

        result_map = {r["symbol"]: r for r in results}
        now_iso = datetime.now().isoformat()

        for sym in symbols:
            if sym in screened_set:
                continue
            screened_set.add(sym)
            r = result_map.get(sym)
            if r and (r.get("moat_hint", 0) + r.get("quality_hint", 0)) >= min_score:
                passed_set.add(sym)
            else:
                failed_dict[sym] = now_iso

        campaign["haiku_screened"] = sorted(screened_set)
        campaign["haiku_passed"] = sorted(passed_set)
        campaign["haiku_failed"] = failed_dict

    def mark_analyzed(self, symbol: str) -> None:
        """Record that a symbol has been deep-analyzed this campaign."""
        analyzed = set(self.campaign["analyzed"])
        analyzed.add(symbol)
        self.campaign["analyzed"] = sorted(analyzed)

    def add_study(
        self,
        symbol: str,
        analysis,
        tier_assignment,
        haiku_result: Optional[dict] = None,
        score: float = 0.0,
        confidence: float = 0.0,
    ) -> None:
        """
        Add or update a study entry. Also writes backward-compat
        analysis JSON to data/analyses/{symbol}.json.
        """
        analysis_dict = analysis.to_dict() if hasattr(analysis, "to_dict") else {}

        entry = {
            "symbol": symbol,
            "company_name": getattr(analysis, "company_name", symbol) or symbol,
            "sector": getattr(analysis, "sector", ""),
            "analysis": analysis_dict,
            "tier": tier_assignment.tier,
            "tier_reason": tier_assignment.tier_reason,
            "target_entry_price": tier_assignment.target_entry_price,
            "current_price_at_analysis": tier_assignment.current_price,
            "analyzed_at": datetime.now().isoformat(),
            "haiku_result": haiku_result,
            "screener_score": score,
            "screener_confidence": confidence,
        }
        self._data["studies"][symbol] = entry

        # Backward compat: write individual analysis file
        try:
            analysis_path = self._analyses_dir / f"{symbol}.json"
            analysis_path.write_text(
                json.dumps(
                    {**analysis_dict, "analyzed_at": entry["analyzed_at"]},
                    indent=2,
                    default=str,
                )
            )
        except Exception as exc:
            logger.warning(f"Failed to write analysis file for {symbol}: {exc}")

    def get_campaign_progress(self, universe_size: int) -> dict:
        """Return progress metrics for the current campaign."""
        campaign = self.campaign
        screened = len(campaign["haiku_screened"])
        passed = len(campaign["haiku_passed"])
        analyzed = len(campaign["analyzed"])
        total_studied = len(self._data["studies"])
        coverage_pct = screened / universe_size if universe_size > 0 else 0.0

        # Estimate completion: how many runs to cover the rest at ~100/run
        remaining = max(0, universe_size - screened)
        est_runs_remaining = (remaining + 99) // 100 if remaining > 0 else 0

        return {
            "campaign_id": campaign["campaign_id"],
            "universe_size": universe_size,
            "haiku_screened": screened,
            "haiku_passed": passed,
            "haiku_failed": len(campaign.get("haiku_failed", {})),
            "deeply_analyzed": analyzed,
            "coverage_pct": coverage_pct,
            "est_runs_remaining": est_runs_remaining,
            "total_studied_all_time": total_studied,
        }

    def should_start_new_campaign(self, universe_size: int) -> bool:
        """True if >90% of universe has been Haiku-screened."""
        if universe_size <= 0:
            return False
        screened = len(self.campaign["haiku_screened"])
        return screened / universe_size > 0.90

    def start_new_campaign(self, carry_forward_days: int = 90) -> str:
        """
        Start a new campaign, keeping all studies. Returns new campaign_id.

        Recent Haiku failures (< carry_forward_days old) are carried into the
        new campaign so we don't waste money re-screening companies that just
        failed. Only truly stale failures get re-evaluated.
        """
        new_id = _quarter_id()
        # Avoid duplicate IDs
        if new_id == self.campaign["campaign_id"]:
            new_id = f"{new_id}b"

        # Carry forward recent failures from old campaign
        old_failed = self.campaign.get("haiku_failed", {})
        if isinstance(old_failed, list):
            old_failed = {s: self.campaign.get("started_at", "2000-01-01") for s in old_failed}

        cutoff = datetime.now() - timedelta(days=carry_forward_days)
        carried_failed = {}
        expired_count = 0
        for sym, screened_at_str in old_failed.items():
            try:
                screened_at = datetime.fromisoformat(screened_at_str)
                if screened_at > cutoff:
                    carried_failed[sym] = screened_at_str
                else:
                    expired_count += 1
            except (ValueError, TypeError):
                expired_count += 1

        carried_screened = sorted(carried_failed.keys())

        self._data["campaign"] = {
            "campaign_id": new_id,
            "started_at": datetime.now().isoformat(),
            "haiku_screened": carried_screened,
            "haiku_passed": [],
            "haiku_failed": carried_failed,
            "analyzed": [],
        }
        logger.info(
            f"Started new campaign: {new_id} (carried {len(carried_failed)} recent failures, expired {expired_count})"
        )
        return new_id

    def needs_refresh(self, symbol: str, max_age_days: int = 180) -> bool:
        """True if a study is older than max_age_days."""
        entry = self.get_studied(symbol)
        if not entry:
            return True
        try:
            analyzed_at = datetime.fromisoformat(entry["analyzed_at"])
            return (datetime.now() - analyzed_at) > timedelta(days=max_age_days)
        except (KeyError, ValueError):
            return True

    def get_stale_symbols(self, max_age_days: int = 180) -> list[str]:
        """Return symbols with analyses older than max_age_days."""
        stale = []
        for sym, entry in self._data["studies"].items():
            try:
                analyzed_at = datetime.fromisoformat(entry["analyzed_at"])
                age = (datetime.now() - analyzed_at).days
                if age > max_age_days:
                    stale.append(sym)
            except (KeyError, ValueError):
                stale.append(sym)
        return stale

    def get_tier_entries(self, tiers: list[int]) -> dict[str, dict]:
        """Return all study entries matching the given tier numbers."""
        return {sym: entry for sym, entry in self._data["studies"].items() if entry.get("tier") in tiers}
