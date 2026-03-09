"""
step5_triage.py - Triage scored candidates into action buckets.

Score ranges (configurable via .env):
  75-100  → auto_approve   (add as WatchSource immediately, spot-check 5%)
  50-74   → human_review   (queue for quick human verification)
  25-49   → deep_review    (queue for deeper investigation)
  0-24    → auto_reject    (discard, log for auditing)
"""

import logging
from config import Config
from models import VideoCandidate, Performance
import db

logger = logging.getLogger(__name__)


class TriageResult:
    """Container for triage output."""

    def __init__(self):
        self.auto_approved: list[VideoCandidate] = []
        self.human_review: list[VideoCandidate] = []
        self.auto_rejected: list[VideoCandidate] = []

    @property
    def summary(self) -> str:
        return (
            f"Auto-approved: {len(self.auto_approved)} | "
            f"Human review: {len(self.human_review)} | "
            f"Auto-rejected: {len(self.auto_rejected)}"
        )


def triage_candidates(
    candidates: list[VideoCandidate],
    performance: Performance,
    auto_approve_to_db: bool = True,
) -> TriageResult:
    """
    Sort scored candidates into action buckets and optionally auto-approve top matches.

    Args:
        candidates: Scored VideoCandidate list (from step4)
        performance: The performance these candidates are for
        auto_approve_to_db: If True, immediately insert auto-approved videos as WatchSources

    Returns:
        TriageResult with candidates sorted into buckets
    """
    result = TriageResult()

    for candidate in candidates:
        score = candidate.relevance_score

        if score >= Config.SCORE_AUTO_APPROVE:
            candidate.triage_action = "auto_approve"
            result.auto_approved.append(candidate)

            if auto_approve_to_db:
                _auto_approve(candidate, performance)

        elif score >= Config.SCORE_HUMAN_REVIEW:
            candidate.triage_action = "human_review"
            result.human_review.append(candidate)

        else:
            candidate.triage_action = "auto_reject"
            result.auto_rejected.append(candidate)

    # Save all candidates to DB (for auditing and review queue)
    db.insert_candidates(candidates)

    logger.info(f"  Triage for '{performance.artist_name}': {result.summary}")

    return result


def _auto_approve(candidate: VideoCandidate, performance: Performance):
    """
    Automatically add a high-scoring candidate as a WatchSource.

    Determines if it's from an official channel and if it's a full performance
    based on the scoring breakdown.
    """
    from step4_score_candidates import KNOWN_OFFICIAL_CHANNELS

    is_official = (
        candidate.channel_id in KNOWN_OFFICIAL_CHANNELS
        or candidate.channel_id in performance.official_channel_ids
    )

    # Check if it's a full performance based on duration match
    is_full = True
    if performance.duration_minutes and candidate.duration_minutes > 0:
        ratio = candidate.duration_minutes / performance.duration_minutes
        if ratio < 0.5:
            is_full = False  # Probably a clip

    quality = candidate.definition or "sd"

    db.insert_watch_source(
        performance_id=performance.id,
        video_id=candidate.video_id,
        is_official=is_official,
        is_full=is_full,
        quality=quality,
    )

    logger.info(
        f"    AUTO-APPROVED: [{candidate.relevance_score}] {candidate.title[:60]}... "
        f"(official={is_official}, full={is_full})"
    )


def triage_summary_table(result: TriageResult) -> str:
    """Pretty-print the triage results for logging/display."""
    lines = []
    lines.append("\n" + "=" * 80)
    lines.append("TRIAGE RESULTS")
    lines.append("=" * 80)

    if result.auto_approved:
        lines.append(f"\n✅ AUTO-APPROVED ({len(result.auto_approved)}):")
        for c in result.auto_approved:
            lines.append(f"   [{c.relevance_score:3d}] {c.title[:70]}")
            lines.append(f"         Channel: {c.channel_title} | Views: {c.view_count:,}")

    if result.human_review:
        lines.append(f"\n🔍 HUMAN REVIEW ({len(result.human_review)}):")
        for c in result.human_review:
            lines.append(f"   [{c.relevance_score:3d}] {c.title[:70]}")
            lines.append(f"         Channel: {c.channel_title} | Views: {c.view_count:,}")

    if result.auto_rejected:
        lines.append(f"\n❌ AUTO-REJECTED ({len(result.auto_rejected)}):")
        for c in result.auto_rejected:
            lines.append(f"   [{c.relevance_score:3d}] {c.title[:70]}")

    lines.append("\n" + "=" * 80)
    return "\n".join(lines)


# ---- Demo / test ----

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Initialize DB
    db.init_db()

    # Create a test performance
    from datetime import date
    perf = Performance(
        id="test-triage",
        artist_name="Test Artist",
        event_name="Test Event",
        performance_date=date(2024, 1, 1),
        duration_minutes=30,
    )
    db.insert_performance(perf)

    # Simulate scored candidates
    candidates = [
        VideoCandidate(
            video_id="high1", title="Test Artist - Test Event 2024 Full Performance Official",
            channel_title="Official", channel_id="x", published_at="2024-01-02",
            description="", duration_iso="PT30M", duration_minutes=30, definition="hd",
            view_count=1_000_000, like_count=50_000, thumbnail_url="",
            performance_id=perf.id, relevance_score=85,
        ),
        VideoCandidate(
            video_id="mid1", title="Test Artist live at Test Event",
            channel_title="FanChannel", channel_id="y", published_at="2024-01-02",
            description="", duration_iso="PT28M", duration_minutes=28, definition="hd",
            view_count=50_000, like_count=1_000, thumbnail_url="",
            performance_id=perf.id, relevance_score=55,
        ),
        VideoCandidate(
            video_id="low1", title="My Reaction to Test Artist!!",
            channel_title="ReactGuy", channel_id="z", published_at="2024-01-03",
            description="", duration_iso="PT15M", duration_minutes=15, definition="sd",
            view_count=500, like_count=10, thumbnail_url="",
            performance_id=perf.id, relevance_score=10,
        ),
    ]

    result = triage_candidates(candidates, perf, auto_approve_to_db=True)
    print(triage_summary_table(result))
