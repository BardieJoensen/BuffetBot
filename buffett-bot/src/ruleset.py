"""
Ruleset versioning.

Every change to what the bot buys, sells or rates restarts the track-record
clock: a paper equity curve produced under two different rulesets is two
experiments, not one. Decisions, tier changes and validation runs are stamped
with RULESET_VERSION so performance can be reported per version instead of
blended across them.

Bump the version and append a changelog entry whenever a rule changes. A
config knob moving (e.g. MARGIN_OF_SAFETY_PCT) is a rule change too.
"""

from __future__ import annotations

RULESET_VERSION = "3.1.1"

# (version, effective date, what changed). Oldest first.
RULESET_CHANGELOG: list[tuple[str, str, str]] = [
    (
        "2.0",
        "2026-07-02",
        "Phase C regime-driven deployment; sells on any confirmed downgrade to C; LLM target entry price drives tiers.",
    ),
    (
        "3.0.0",
        "2026-10-18",
        "News checks pre-registered thesis breakers only (a sell needs a breaker to fire; "
        "re-analysis never forces a sell); deterministic valuation drives target entry; "
        "moat NONE / LOW conviction never buyable (defense in depth); monthly tier-cohort "
        "validation; Danish lot sizing and single-buy recommendation.",
    ),
    (
        "3.1.0",
        "2026-10-20",
        "S tier = WIDE moat with MEDIUM or HIGH conviction at/below target (was HIGH only, which the "
        "model emitted 7 times in 293 analyses, never on a wide-moat name at a good price); the "
        "analysis prompt gets an observable rubric for HIGH/MEDIUM/LOW. Also live since 2026-10-19: "
        "Monday scores the full universe, LLM queues skip names that fail the hard filters, held "
        "names are news-monitored at any tier, suspect broker reads are not traded on.",
    ),
    (
        "3.1.1",
        "2026-10-20",
        "Light model (Haiku quick screen, news red-flag check, Friday quick screen) moves from "
        "Claude Haiku 4.5 to Claude Haiku 5.5. A model change in the gate that decides what "
        "reaches Sonnet is a rule change; measured after the fact by the model benchmark.",
    ),
]


def changelog_text() -> str:
    """Human-readable changelog for briefings and reports."""
    lines = [f"  v{version} ({date}): {what}" for version, date, what in RULESET_CHANGELOG]
    return "\n".join(lines)
