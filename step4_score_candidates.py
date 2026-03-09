"""
step4_score_candidates.py - Score each video candidate for relevance to a performance.

Scoring is 0-100 based on weighted signals:
  +20  Title contains artist name (fuzzy match)
  +20  Title contains event name (fuzzy match)
  +10  Title contains correct year
  +25  Video is from an official/verified channel
  +10  Duration is within ±30% of expected
  +5   HD quality
  +5   High view count (>100K)
  +5   Title contains "full" (suggests complete performance)
  -20  Title contains "reaction", "review", "reacts", "commentary"
  -30  Title contains "cover", "tribute", "karaoke", "parody"
  -10  Title contains "audio only" (deprioritize for video pipeline)
  -15  Title contains "highlights", "best moments" (not full performance)
"""

import logging
from thefuzz import fuzz

from models import Performance, VideoCandidate

logger = logging.getLogger(__name__)


# Known official music channels (add more as you discover them)
# Format: YouTube channel ID -> channel name
KNOWN_OFFICIAL_CHANNELS = {
    "UCIwFjwMjI0y7PDBVEO9-bkQ": "NFL",                   # Official NFL channel
    "UCYLNGLIzMhRTi6ZOLjAPSmw": "NPR Music",             # Tiny Desk Concerts
    "UC-9-kyTW8ZkZNDHQJ6FgpwQ": "Music",                 # YouTube Music
    "UCJ5v_MCY6GNUBTO8-D3XoAg": "WWE",                   # For performance at events
    "UCVTyTA7-g9nopHeHbeuvpRA": "ESPN",
    "UCp0hYYBW6IMayGgR-WeoCVQ": "The Tonight Show",
    "UCi7GJNg51C3jgmYTUwqoUXA": "Saturday Night Live",
    "UCgRQHK8Ttr1j9xCEpCAlgbQ": "Grammys",
    # Add your own as you build the database
}

# Negative signals - words that indicate this is NOT the performance itself
NEGATIVE_SIGNALS = {
    "reaction": -20,
    "reacts": -20,
    "review": -20,
    "commentary": -20,
    "cover": -30,
    "tribute": -30,
    "karaoke": -30,
    "parody": -30,
    "remix": -15,
    "highlights": -15,
    "best moments": -15,
    "top 10": -15,
    "ranking": -15,
    "audio only": -10,
    "audio version": -10,
    "fan cam": -5,
    "fancam": -5,
}

# Positive signals
POSITIVE_SIGNALS = {
    "full performance": 8,
    "full show": 8,
    "full concert": 8,
    "full set": 8,
    "complete": 5,
    "official": 5,
    "hd": 3,
    "4k": 3,
    "remastered": 3,
}


def score_candidate(video: VideoCandidate, performance: Performance) -> VideoCandidate:
    """
    Score a single video candidate against a performance. Mutates and returns the candidate.

    Returns the same VideoCandidate with relevance_score and score_breakdown populated.
    """
    score = 0
    breakdown = {}
    title_lower = video.title.lower()
    desc_lower = video.description.lower()

    # --- Artist name match (fuzzy) ---
    artist_score = fuzz.partial_ratio(performance.artist_name.lower(), title_lower)
    if artist_score >= 80:
        points = 20
        score += points
        breakdown["artist_match"] = f"+{points} (fuzzy={artist_score}%)"
    elif artist_score >= 60:
        points = 10
        score += points
        breakdown["artist_match"] = f"+{points} (partial fuzzy={artist_score}%)"

    # --- Event name match (fuzzy) ---
    if performance.event_name:
        event_score = fuzz.partial_ratio(performance.event_name.lower(), title_lower)
        if event_score >= 70:
            points = 20
            score += points
            breakdown["event_match"] = f"+{points} (fuzzy={event_score}%)"
        elif event_score >= 50:
            # Check description too
            desc_event_score = fuzz.partial_ratio(performance.event_name.lower(), desc_lower)
            if desc_event_score >= 70:
                points = 10
                score += points
                breakdown["event_match"] = f"+{points} (in description, fuzzy={desc_event_score}%)"

    # --- Year match ---
    if performance.performance_date:
        year_str = str(performance.performance_date.year)
        if year_str in video.title:
            score += 10
            breakdown["year_match"] = "+10 (in title)"
        elif year_str in video.description:
            score += 5
            breakdown["year_match"] = "+5 (in description only)"

    # --- Official channel ---
    if video.channel_id in KNOWN_OFFICIAL_CHANNELS:
        score += 25
        breakdown["official_channel"] = f"+25 ({KNOWN_OFFICIAL_CHANNELS[video.channel_id]})"
    elif video.channel_id in performance.official_channel_ids:
        score += 25
        breakdown["official_channel"] = "+25 (artist's official channel)"

    # --- Duration match ---
    if performance.duration_minutes and video.duration_minutes > 0:
        ratio = video.duration_minutes / performance.duration_minutes
        if 0.7 <= ratio <= 1.3:
            score += 10
            breakdown["duration_match"] = f"+10 (video={video.duration_minutes:.0f}m, expected={performance.duration_minutes}m, ratio={ratio:.2f})"
        elif ratio < 0.3:
            # Very short compared to expected = probably a clip
            score -= 10
            breakdown["duration_match"] = f"-10 (likely a clip: {video.duration_minutes:.0f}m vs expected {performance.duration_minutes}m)"
        elif ratio > 2.0:
            # Much longer = might be a compilation
            score -= 5
            breakdown["duration_match"] = f"-5 (much longer: {video.duration_minutes:.0f}m vs expected {performance.duration_minutes}m)"
    else:
        breakdown["duration_match"] = "0 (no expected duration to compare)"

    # --- HD quality ---
    if video.definition == "hd":
        score += 5
        breakdown["quality"] = "+5 (HD)"

    # --- View count ---
    if video.view_count > 1_000_000:
        score += 5
        breakdown["views"] = f"+5 ({video.view_count:,} views)"
    elif video.view_count > 100_000:
        score += 3
        breakdown["views"] = f"+3 ({video.view_count:,} views)"

    # --- Positive title signals ---
    for signal, points in POSITIVE_SIGNALS.items():
        if signal in title_lower:
            score += points
            breakdown[f"positive_{signal}"] = f"+{points}"
            break  # Only count the best positive signal

    # --- Negative title signals ---
    for signal, penalty in NEGATIVE_SIGNALS.items():
        if signal in title_lower:
            score += penalty  # penalty is already negative
            breakdown[f"negative_{signal}"] = f"{penalty}"

    # Clamp to 0-100
    video.relevance_score = max(0, min(100, score))
    video.score_breakdown = breakdown

    return video


def score_candidates(
    candidates: list[VideoCandidate],
    performance: Performance,
) -> list[VideoCandidate]:
    """Score all candidates for a performance. Returns sorted by score descending."""
    scored = [score_candidate(c, performance) for c in candidates]
    scored.sort(key=lambda c: c.relevance_score, reverse=True)

    logger.info(
        f"  Scored {len(scored)} candidates for '{performance.artist_name}'. "
        f"Top score: {scored[0].relevance_score if scored else 'N/A'}"
    )

    return scored


# ---- Demo / test ----

if __name__ == "__main__":
    from datetime import date

    logging.basicConfig(level=logging.INFO)

    # Simulate a performance and some candidates
    perf = Performance(
        id="sb-liv",
        artist_name="Shakira & Jennifer Lopez",
        event_name="Super Bowl LIV",
        performance_date=date(2020, 2, 2),
        duration_minutes=14,
        performance_type="halftime",
    )

    fake_candidates = [
        VideoCandidate(
            video_id="test1", title="Shakira & J. Lo's FULL Pepsi Super Bowl LIV Halftime Show | NFL",
            channel_title="NFL", channel_id="UCIwFjwMjI0y7PDBVEO9-bkQ",
            published_at="2020-02-03", description="Watch the full halftime show 2020",
            duration_iso="PT14M", duration_minutes=14.0, definition="hd",
            view_count=250_000_000, like_count=2_000_000, thumbnail_url="",
            performance_id=perf.id,
        ),
        VideoCandidate(
            video_id="test2", title="Super Bowl LIV Halftime Show REACTION!! Shakira & Jennifer Lopez",
            channel_title="ReactBro", channel_id="fake-channel",
            published_at="2020-02-03", description="My reaction to the halftime show",
            duration_iso="PT22M", duration_minutes=22.0, definition="hd",
            view_count=500_000, like_count=10_000, thumbnail_url="",
            performance_id=perf.id,
        ),
        VideoCandidate(
            video_id="test3", title="Shakira - Waka Waka (Cover by SomeGuy)",
            channel_title="SomeGuy Music", channel_id="fake-channel2",
            published_at="2020-03-01", description="My cover of Waka Waka",
            duration_iso="PT4M", duration_minutes=4.0, definition="hd",
            view_count=50_000, like_count=500, thumbnail_url="",
            performance_id=perf.id,
        ),
    ]

    scored = score_candidates(fake_candidates, perf)

    for c in scored:
        print(f"\n[Score: {c.relevance_score}] {c.title}")
        print(f"  Channel: {c.channel_title} | Views: {c.view_count:,} | Duration: {c.duration_minutes:.0f}m")
        print(f"  Breakdown:")
        for key, val in c.score_breakdown.items():
            print(f"    {key}: {val}")
