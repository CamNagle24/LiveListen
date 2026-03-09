"""
step2_search_youtube.py - Execute YouTube Data API v3 searches.

Handles:
- API client initialization
- Search execution with configurable filters
- Quota tracking
- Rate limiting (polite 1-second delay between calls)
"""

import time
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config import Config
from models import PipelineStats

logger = logging.getLogger(__name__)


class YouTubeSearcher:
    """Wrapper around YouTube Data API v3 search."""

    def __init__(self, stats: PipelineStats):
        self.youtube = build("youtube", "v3", developerKey=Config.YOUTUBE_API_KEY)
        self.stats = stats

    def search(
        self,
        query: str,
        max_results: int = 10,
        video_duration: str | None = None,
        video_definition: str = "any",
        order: str = "relevance",
        channel_id: str | None = None,
    ) -> list[dict]:
        """
        Search YouTube for videos matching a query.

        Args:
            query: Search string (e.g., "Beyoncé Coachella 2018 full set")
            max_results: Number of results (1-50, default 10)
            video_duration: "short" (<4m), "medium" (4-20m), "long" (>20m), or None
            video_definition: "high" for HD only, "any" for all
            order: "relevance" (default) or "viewCount"
            channel_id: Restrict to a specific channel (optional)

        Returns:
            List of raw search result items from the API.
            Each item has: id.videoId, snippet.title, snippet.channelTitle, etc.

        Raises:
            QuotaExhaustedError: If daily quota would be exceeded.
        """
        # Check quota before making the call
        if not self.stats.can_search():
            raise QuotaExhaustedError(
                f"Daily quota exhausted. Used {self.stats.quota_used} / "
                f"{Config.YOUTUBE_DAILY_QUOTA_LIMIT} units. "
                f"Try again tomorrow or request a quota increase."
            )

        # Build the API request
        params = {
            "q": query,
            "part": "snippet",
            "type": "video",
            "maxResults": min(max_results, 50),
            "order": order,
        }

        if video_duration:
            params["videoDuration"] = video_duration
        if video_definition and video_definition != "any":
            params["videoDefinition"] = video_definition
        if channel_id:
            params["channelId"] = channel_id

        logger.info(f"Searching YouTube: \"{query}\" (duration={video_duration}, def={video_definition})")

        try:
            response = self.youtube.search().list(**params).execute()
            self.stats.log_search()

            items = response.get("items", [])
            self.stats.videos_found += len(items)

            logger.info(f"  Found {len(items)} results. Quota used: {self.stats.quota_used}")

            # Polite rate limiting - 1 second between searches
            time.sleep(1)

            return items

        except HttpError as e:
            if e.resp.status == 403:
                logger.error("YouTube API quota exceeded or API key invalid.")
                raise QuotaExhaustedError(f"API returned 403: {e}")
            else:
                logger.error(f"YouTube API error: {e}")
                raise

    def search_with_fallback(
        self,
        queries: list[dict],
        max_results_per_query: int = 10,
        min_results_needed: int = 3,
    ) -> list[dict]:
        """
        Search using a prioritized list of queries, stopping when we have enough results.

        This is the main entry point for Step 2 in the pipeline. It takes the
        queries built by Step 1 and executes them in order, falling back to
        broader queries only if earlier ones didn't return enough results.

        Args:
            queries: Output of step1_build_queries.build_queries()
                     Each dict has: query, priority, video_duration, description
            max_results_per_query: Max results per search call
            min_results_needed: Stop searching after this many total results

        Returns:
            Deduplicated list of raw search result items.
        """
        all_results = []
        seen_video_ids = set()

        # Sort by priority (most specific first)
        sorted_queries = sorted(queries, key=lambda q: q["priority"])

        for q in sorted_queries:
            if not self.stats.can_search():
                logger.warning("Quota exhausted, stopping search fallback chain.")
                break

            results = self.search(
                query=q["query"],
                max_results=max_results_per_query,
                video_duration=q.get("video_duration"),
            )

            # Deduplicate across queries
            for item in results:
                vid_id = item["id"].get("videoId")
                if vid_id and vid_id not in seen_video_ids:
                    seen_video_ids.add(vid_id)
                    all_results.append(item)

            logger.info(
                f"  After query [{q['priority']}]: {len(all_results)} unique results total"
            )

            # If we have enough results, don't waste quota on broader queries
            if len(all_results) >= min_results_needed:
                logger.info(f"  Got {len(all_results)} results, skipping remaining queries.")
                break

        return all_results

    def extract_video_ids(self, search_results: list[dict]) -> list[str]:
        """Pull video IDs from search results for use in Step 3 (enrichment)."""
        return [
            item["id"]["videoId"]
            for item in search_results
            if "videoId" in item.get("id", {})
        ]


class QuotaExhaustedError(Exception):
    """Raised when YouTube API daily quota is exceeded."""
    pass


# ---- Demo / test ----

if __name__ == "__main__":
    from models import PipelineStats

    logging.basicConfig(level=logging.INFO)

    Config.validate()
    stats = PipelineStats()
    searcher = YouTubeSearcher(stats)

    results = searcher.search("Shakira Jennifer Lopez Super Bowl LIV halftime show full", max_results=5)

    print(f"\nFound {len(results)} results:")
    for item in results:
        title = item["snippet"]["title"]
        channel = item["snippet"]["channelTitle"]
        vid_id = item["id"]["videoId"]
        print(f"  [{vid_id}] {title} (by {channel})")

    print(f"\nQuota used: {stats.quota_used} / {Config.YOUTUBE_DAILY_QUOTA_LIMIT}")
