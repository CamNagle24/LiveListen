"""
step3_enrich_results.py - Batch-fetch video details from YouTube API.

The search endpoint (Step 2) only returns basic snippets. This step uses the
videos.list endpoint to get duration, view count, like count, and HD status.

Key insight: videos.list costs only 1 quota unit (vs 100 for search), and
accepts up to 50 video IDs per call. So enriching 50 videos costs 1-3 units.
"""

import logging
import isodate
from googleapiclient.discovery import build

from config import Config
from models import VideoCandidate, PipelineStats

logger = logging.getLogger(__name__)


class VideoEnricher:
    """Fetch full video details and convert to VideoCandidate objects."""

    def __init__(self, stats: PipelineStats):
        self.youtube = build("youtube", "v3", developerKey=Config.YOUTUBE_API_KEY)
        self.stats = stats

    def enrich(
        self,
        video_ids: list[str],
        performance_id: str,
        search_results: list[dict] | None = None,
    ) -> list[VideoCandidate]:
        """
        Fetch detailed info for a list of video IDs and return VideoCandidate objects.

        Args:
            video_ids: List of YouTube video IDs (max 50 per batch)
            performance_id: Which performance these candidates are for
            search_results: Optional raw search results to extract thumbnails from

        Returns:
            List of fully-populated VideoCandidate objects (without scores yet)
        """
        if not video_ids:
            return []

        candidates = []

        # Process in batches of 50 (API limit)
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i:i + 50]
            batch_candidates = self._fetch_batch(batch, performance_id)
            candidates.extend(batch_candidates)

        logger.info(f"  Enriched {len(candidates)} videos for performance {performance_id}")
        return candidates

    def _fetch_batch(self, video_ids: list[str], performance_id: str) -> list[VideoCandidate]:
        """Fetch details for up to 50 videos in a single API call."""

        # Request snippet (title, channel), contentDetails (duration, definition),
        # and statistics (views, likes) in one call
        response = self.youtube.videos().list(
            id=",".join(video_ids),
            part="snippet,contentDetails,statistics"
        ).execute()

        # Track quota: 1 unit per part requested, but the API charges per call not per video
        # In practice: 1 call with 3 parts for up to 50 videos = ~3 units
        self.stats.log_detail_fetch(len(video_ids))

        candidates = []
        for item in response.get("items", []):
            try:
                candidate = self._parse_video(item, performance_id)
                candidates.append(candidate)
            except Exception as e:
                logger.warning(f"  Failed to parse video {item.get('id')}: {e}")

        return candidates

    def _parse_video(self, item: dict, performance_id: str) -> VideoCandidate:
        """Parse a single YouTube API video item into a VideoCandidate."""
        snippet = item["snippet"]
        details = item["contentDetails"]
        stats = item.get("statistics", {})

        # Parse ISO 8601 duration (e.g., "PT1H23M45S") to minutes
        duration_minutes = self._parse_duration(details["duration"])

        # Get the best thumbnail available
        thumbnails = snippet.get("thumbnails", {})
        thumbnail_url = (
            thumbnails.get("maxres", {}).get("url")
            or thumbnails.get("high", {}).get("url")
            or thumbnails.get("medium", {}).get("url")
            or thumbnails.get("default", {}).get("url", "")
        )

        return VideoCandidate(
            video_id=item["id"],
            title=snippet["title"],
            channel_title=snippet["channelTitle"],
            channel_id=snippet["channelId"],
            published_at=snippet["publishedAt"],
            description=snippet.get("description", "")[:500],
            duration_iso=details["duration"],
            duration_minutes=duration_minutes,
            definition=details.get("definition", "sd"),
            view_count=int(stats.get("viewCount", 0)),
            like_count=int(stats.get("likeCount", 0)),
            thumbnail_url=thumbnail_url,
            performance_id=performance_id,
        )

    @staticmethod
    def _parse_duration(iso_duration: str) -> float:
        """
        Parse ISO 8601 duration to minutes.

        Examples:
            "PT1H23M45S" -> 83.75
            "PT14M30S"   -> 14.5
            "PT3M"       -> 3.0
            "P0D"        -> 0.0 (livestream placeholder)
        """
        try:
            td = isodate.parse_duration(iso_duration)
            return td.total_seconds() / 60.0
        except Exception:
            logger.warning(f"  Could not parse duration: {iso_duration}")
            return 0.0


# ---- Demo / test ----

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    Config.validate()
    stats = PipelineStats()
    enricher = VideoEnricher(stats)

    # Test with known video IDs (replace with real ones)
    test_ids = ["dQw4w9WgXcQ"]  # Rick Astley - Never Gonna Give You Up
    candidates = enricher.enrich(test_ids, performance_id="test-001")

    for c in candidates:
        print(f"\nVideo: {c.title}")
        print(f"  Channel: {c.channel_title}")
        print(f"  Duration: {c.duration_minutes:.1f} min")
        print(f"  Definition: {c.definition}")
        print(f"  Views: {c.view_count:,}")
        print(f"  Likes: {c.like_count:,}")
        print(f"  URL: {c.youtube_url}")

    print(f"\nQuota used: {stats.quota_used}")
