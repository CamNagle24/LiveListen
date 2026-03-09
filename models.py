"""
models.py - Data classes for the pipeline.
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Optional


@dataclass
class Performance:
    """A live performance from your database."""
    id: str
    artist_name: str
    event_name: Optional[str] = None
    venue_name: Optional[str] = None
    performance_date: Optional[date] = None
    duration_minutes: Optional[int] = None
    performance_type: str = "concert"  # concert, festival_set, tv_performance, halftime, session

    # Optional: known official YouTube channel IDs for this artist
    official_channel_ids: list[str] = field(default_factory=list)


@dataclass
class VideoCandidate:
    """A YouTube video that might match a performance."""
    video_id: str
    title: str
    channel_title: str
    channel_id: str
    published_at: str
    description: str
    duration_iso: str           # ISO 8601 duration (e.g., "PT1H23M45S")
    duration_minutes: float     # Parsed to minutes
    definition: str             # "hd" or "sd"
    view_count: int
    like_count: int
    thumbnail_url: str

    # Set by scoring pipeline
    relevance_score: int = 0
    score_breakdown: dict = field(default_factory=dict)

    # Set by triage
    triage_action: str = ""     # "auto_approve", "human_review", "auto_reject"

    # Set by human review
    review_decision: str = ""   # "approve_full", "approve_partial", "reject", "skip"
    reviewer_notes: str = ""

    # Which performance this is a candidate for
    performance_id: str = ""

    @property
    def youtube_url(self) -> str:
        return f"https://www.youtube.com/watch?v={self.video_id}"

    @property
    def is_official_channel(self) -> bool:
        """Check during scoring, not here - this is just a convenience."""
        return False  # Override in scoring


@dataclass
class PipelineStats:
    """Track quota usage and results across a pipeline run."""
    searches_made: int = 0
    quota_used: int = 0
    videos_found: int = 0
    videos_enriched: int = 0
    auto_approved: int = 0
    sent_to_review: int = 0
    auto_rejected: int = 0

    @property
    def quota_remaining(self) -> int:
        from config import Config
        return Config.YOUTUBE_DAILY_QUOTA_LIMIT - self.quota_used

    def can_search(self) -> bool:
        from config import Config
        return self.quota_remaining >= Config.SEARCH_COST

    def log_search(self):
        from config import Config
        self.searches_made += 1
        self.quota_used += Config.SEARCH_COST

    def log_detail_fetch(self, count: int):
        from config import Config
        self.quota_used += count * Config.DETAIL_COST
        self.videos_enriched += count
