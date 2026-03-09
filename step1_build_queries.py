"""
step1_build_queries.py - Build optimized YouTube search queries from performance metadata.

Strategy:
- Start with the most specific query (artist + event + year + "full performance")
- Fall back to broader queries only if needed
- Tailor query style to performance type (halftime vs. festival vs. TV session)
"""

from models import Performance


# Performance-type-specific suffixes that help YouTube return the right results
TYPE_SUFFIXES = {
    "concert":        ["full concert", "live concert", "full show"],
    "festival_set":   ["full set", "live set", "festival"],
    "halftime":       ["halftime show full", "halftime performance"],
    "tv_performance": ["live performance", "live TV"],
    "session":        ["full session", "live session"],
    "ceremony":       ["performance", "live"],
    "dj_set":         ["full set", "live set", "DJ set"],
    "virtual":        ["full concert", "virtual concert", "livestream"],
}


def build_queries(performance: Performance) -> list[dict]:
    """
    Build 1-3 search queries for a performance, ordered most → least specific.

    Returns a list of dicts with:
        - query: the search string
        - priority: 1 (most specific) to 3 (broadest)
        - video_duration: YouTube filter ("short", "medium", "long", or None)
        - description: human-readable explanation of this query's intent
    """
    artist = performance.artist_name
    event = performance.event_name or ""
    year = str(performance.performance_date.year) if performance.performance_date else ""
    ptype = performance.performance_type or "concert"

    suffixes = TYPE_SUFFIXES.get(ptype, ["live performance", "full"])

    queries = []

    # --- Query 1: Most specific (artist + event + year + type suffix) ---
    if event and year:
        q1 = f"{artist} {event} {year} {suffixes[0]}"
        queries.append({
            "query": q1.strip(),
            "priority": 1,
            "video_duration": _duration_filter(ptype, performance.duration_minutes),
            "description": f"Specific: artist + event + year + '{suffixes[0]}'"
        })

    # --- Query 2: Medium specificity (artist + event OR artist + year) ---
    if event:
        q2 = f"{artist} {event} {suffixes[0]}"
        queries.append({
            "query": q2.strip(),
            "priority": 2,
            "video_duration": _duration_filter(ptype, performance.duration_minutes),
            "description": f"Medium: artist + event + '{suffixes[0]}'"
        })
    elif year:
        q2 = f"{artist} live {year}"
        queries.append({
            "query": q2.strip(),
            "priority": 2,
            "video_duration": _duration_filter(ptype, performance.duration_minutes),
            "description": "Medium: artist + 'live' + year"
        })

    # --- Query 3: Broad fallback (artist + type suffix) ---
    fallback_suffix = suffixes[1] if len(suffixes) > 1 else suffixes[0]
    q3 = f"{artist} {fallback_suffix} {year}".strip()
    queries.append({
        "query": q3,
        "priority": 3,
        "video_duration": None,  # No duration filter on broad search
        "description": f"Broad: artist + '{fallback_suffix}' + year"
    })

    # Deduplicate (in case event is empty and queries overlap)
    seen = set()
    unique = []
    for q in queries:
        if q["query"] not in seen:
            seen.add(q["query"])
            unique.append(q)

    return unique


def _duration_filter(ptype: str, duration_minutes: int | None) -> str | None:
    """
    Pick YouTube's videoDuration filter based on performance type.

    YouTube only supports: "short" (<4 min), "medium" (4-20 min), "long" (>20 min)
    """
    if duration_minutes:
        if duration_minutes < 4:
            return "short"
        elif duration_minutes < 20:
            return "medium"
        else:
            return "long"

    # Defaults by type if duration unknown
    defaults = {
        "concert": "long",
        "festival_set": "long",
        "halftime": "medium",     # Halftimes are 12-30 min, "medium" catches most
        "tv_performance": "medium",
        "session": "medium",
        "ceremony": "medium",
        "dj_set": "long",
        "virtual": "long",
    }
    return defaults.get(ptype)


# ---- Demo / test ----

if __name__ == "__main__":
    from datetime import date

    demos = [
        Performance(
            id="sb-liv",
            artist_name="Shakira & Jennifer Lopez",
            event_name="Super Bowl LIV",
            performance_date=date(2020, 2, 2),
            duration_minutes=14,
            performance_type="halftime",
        ),
        Performance(
            id="bey-coachella",
            artist_name="Beyoncé",
            event_name="Coachella",
            performance_date=date(2018, 4, 14),
            duration_minutes=105,
            performance_type="festival_set",
        ),
        Performance(
            id="mac-tiny-desk",
            artist_name="Mac Miller",
            event_name="NPR Tiny Desk Concert",
            performance_date=date(2018, 8, 1),
            duration_minutes=18,
            performance_type="session",
        ),
    ]

    for p in demos:
        print(f"\n{'='*60}")
        print(f"Performance: {p.artist_name} - {p.event_name} ({p.performance_date})")
        print(f"{'='*60}")
        for q in build_queries(p):
            print(f"  [{q['priority']}] \"{q['query']}\"")
            print(f"      Duration filter: {q['video_duration']}")
            print(f"      {q['description']}")
