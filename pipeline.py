"""
pipeline.py - Main orchestrator that runs Steps 1→6 for a batch of performances.

Usage:
  python pipeline.py                     # Process all performances without watch sources
  python pipeline.py --limit 10          # Process up to 10 performances
  python pipeline.py --seed              # Load sample seed data first, then run
  python pipeline.py --dry-run           # Show what would be searched without calling API

The pipeline flow:
  1. Pull performances from DB that have no WatchSource links
  2. For each performance:
     a. Step 1: Build search queries
     b. Step 2: Search YouTube (with fallback queries)
     c. Step 3: Enrich results with video details
     d. Step 4: Score candidates
     e. Step 5: Triage (auto-approve, queue for review, or reject)
  3. Step 6: Human review runs separately via `python step6_review.py`
"""

import sys
import logging
import argparse
from datetime import datetime

from config import Config
from models import Performance, PipelineStats
import db

from step1_build_queries import build_queries
from step2_search_youtube import YouTubeSearcher, QuotaExhaustedError
from step3_enrich_results import VideoEnricher
from step4_score_candidates import score_candidates
from step5_triage import triage_candidates, triage_summary_table

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_pipeline(limit: int = 100, dry_run: bool = False):
    """
    Main pipeline entry point.

    Args:
        limit: Max number of performances to process in this run
        dry_run: If True, build queries but don't call the YouTube API
    """
    # Validate config
    if not dry_run:
        Config.validate()

    # Initialize database
    db.init_db()

    # Get performances that need watch source links
    performances = db.get_performances_without_sources(limit=limit)

    if not performances:
        logger.info("No performances without watch sources. Nothing to do.")
        logger.info("Load seed data with: python pipeline.py --seed")
        return

    logger.info(f"Found {len(performances)} performances to process (limit={limit})")

    # Track stats
    stats = PipelineStats()
    run_id = db.start_pipeline_run()

    # Initialize API clients
    if not dry_run:
        searcher = YouTubeSearcher(stats)
        enricher = VideoEnricher(stats)

    # Process each performance
    processed = 0
    for i, perf in enumerate(performances):
        logger.info(f"\n{'='*60}")
        logger.info(f"[{i+1}/{len(performances)}] {perf.artist_name}")
        if perf.event_name:
            logger.info(f"  Event: {perf.event_name}")
        if perf.performance_date:
            logger.info(f"  Date: {perf.performance_date}")
        logger.info(f"{'='*60}")

        # ---- Step 1: Build queries ----
        queries = build_queries(perf)
        logger.info(f"  Step 1: Built {len(queries)} search queries:")
        for q in queries:
            logger.info(f"    [{q['priority']}] \"{q['query']}\" (duration={q['video_duration']})")

        if dry_run:
            logger.info("  [DRY RUN] Skipping API calls.")
            processed += 1
            continue

        # Check quota before proceeding
        if not stats.can_search():
            logger.warning(f"Quota exhausted after {processed} performances. Stopping.")
            break

        try:
            # ---- Step 2: Search YouTube ----
            logger.info(f"  Step 2: Searching YouTube...")
            raw_results = searcher.search_with_fallback(
                queries,
                max_results_per_query=10,
                min_results_needed=5,
            )

            if not raw_results:
                logger.info(f"  No YouTube results found for this performance.")
                processed += 1
                continue

            # ---- Step 3: Enrich results ----
            video_ids = searcher.extract_video_ids(raw_results)
            logger.info(f"  Step 3: Enriching {len(video_ids)} videos...")
            candidates = enricher.enrich(video_ids, performance_id=perf.id)

            if not candidates:
                logger.info(f"  No valid candidates after enrichment.")
                processed += 1
                continue

            # ---- Step 4: Score candidates ----
            logger.info(f"  Step 4: Scoring {len(candidates)} candidates...")
            scored = score_candidates(candidates, perf)

            # ---- Step 5: Triage ----
            logger.info(f"  Step 5: Triaging...")
            triage_result = triage_candidates(scored, perf, auto_approve_to_db=True)

            # Update running stats
            stats.auto_approved += len(triage_result.auto_approved)
            stats.sent_to_review += len(triage_result.human_review)
            stats.auto_rejected += len(triage_result.auto_rejected)

            # Print triage summary
            print(triage_summary_table(triage_result))

        except QuotaExhaustedError as e:
            logger.warning(f"Quota exhausted: {e}")
            break
        except Exception as e:
            logger.error(f"Error processing '{perf.artist_name}': {e}", exc_info=True)
            continue

        processed += 1

    # ---- Finish ----
    db.finish_pipeline_run(run_id, {
        "performances_processed": processed,
        "searches_made": stats.searches_made,
        "quota_used": stats.quota_used,
        "videos_found": stats.videos_found,
        "auto_approved": stats.auto_approved,
        "sent_to_review": stats.sent_to_review,
        "auto_rejected": stats.auto_rejected,
    })

    # Print final summary
    print(f"\n{'='*60}")
    print(f"  PIPELINE RUN COMPLETE")
    print(f"{'='*60}")
    print(f"  Performances processed: {processed}")
    print(f"  YouTube searches made:  {stats.searches_made}")
    print(f"  API quota used:         {stats.quota_used} / {Config.YOUTUBE_DAILY_QUOTA_LIMIT}")
    print(f"  Videos found:           {stats.videos_found}")
    print(f"  Videos enriched:        {stats.videos_enriched}")
    print(f"  Auto-approved:          {stats.auto_approved}")
    print(f"  Sent to review:         {stats.sent_to_review}")
    print(f"  Auto-rejected:          {stats.auto_rejected}")
    print(f"{'='*60}")

    if stats.sent_to_review > 0:
        print(f"\n  ➡️  Run 'python step6_review.py' to review {stats.sent_to_review} candidates.")
    print()


# ============================================================
# SEED DATA
# ============================================================

def load_seed_data():
    """Load sample performances for testing the pipeline."""
    from datetime import date

    db.init_db()

    seed_performances = [
        # Super Bowl Halftime Shows
        Performance(id="sb-lviii", artist_name="Usher", event_name="Super Bowl LVIII",
                    performance_date=date(2024, 2, 11), duration_minutes=13,
                    performance_type="halftime"),
        Performance(id="sb-lvii", artist_name="Rihanna", event_name="Super Bowl LVII",
                    performance_date=date(2023, 2, 12), duration_minutes=13,
                    performance_type="halftime"),
        Performance(id="sb-lvi", artist_name="Dr. Dre, Snoop Dogg, Eminem, Mary J. Blige, Kendrick Lamar",
                    event_name="Super Bowl LVI",
                    performance_date=date(2022, 2, 13), duration_minutes=14,
                    performance_type="halftime"),
        Performance(id="sb-liv", artist_name="Shakira & Jennifer Lopez", event_name="Super Bowl LIV",
                    performance_date=date(2020, 2, 2), duration_minutes=14,
                    performance_type="halftime"),
        Performance(id="sb-liii", artist_name="Maroon 5", event_name="Super Bowl LIII",
                    performance_date=date(2019, 2, 3), duration_minutes=11,
                    performance_type="halftime"),
        Performance(id="sb-li", artist_name="Lady Gaga", event_name="Super Bowl LI",
                    performance_date=date(2017, 2, 5), duration_minutes=13,
                    performance_type="halftime"),
        Performance(id="sb-50", artist_name="Coldplay, Beyoncé, Bruno Mars", event_name="Super Bowl 50",
                    performance_date=date(2016, 2, 7), duration_minutes=13,
                    performance_type="halftime"),
        Performance(id="sb-xlix", artist_name="Katy Perry", event_name="Super Bowl XLIX",
                    performance_date=date(2015, 2, 1), duration_minutes=12,
                    performance_type="halftime"),
        Performance(id="sb-xlviii", artist_name="Bruno Mars", event_name="Super Bowl XLVIII",
                    performance_date=date(2014, 2, 2), duration_minutes=12,
                    performance_type="halftime"),
        Performance(id="sb-xlvii", artist_name="Beyoncé", event_name="Super Bowl XLVII",
                    performance_date=date(2013, 2, 3), duration_minutes=12,
                    performance_type="halftime"),

        # Iconic Festival Sets
        Performance(id="bey-coachella", artist_name="Beyoncé", event_name="Coachella",
                    performance_date=date(2018, 4, 14), duration_minutes=105,
                    performance_type="festival_set"),
        Performance(id="kanye-glastonbury", artist_name="Kanye West", event_name="Glastonbury",
                    performance_date=date(2015, 6, 27), duration_minutes=90,
                    performance_type="festival_set"),
        Performance(id="radiohead-glastonbury", artist_name="Radiohead", event_name="Glastonbury",
                    performance_date=date(2017, 6, 23), duration_minutes=100,
                    performance_type="festival_set"),
        Performance(id="daft-punk-alive", artist_name="Daft Punk", event_name="Alive 2007 Tour",
                    performance_date=date(2007, 6, 14), duration_minutes=90,
                    performance_type="concert"),
        Performance(id="kendrick-glastonbury", artist_name="Kendrick Lamar", event_name="Glastonbury",
                    performance_date=date(2022, 6, 26), duration_minutes=90,
                    performance_type="festival_set"),

        # Tiny Desk Concerts
        Performance(id="td-mac-miller", artist_name="Mac Miller",
                    event_name="NPR Tiny Desk Concert",
                    performance_date=date(2018, 8, 6), duration_minutes=18,
                    performance_type="session"),
        Performance(id="td-anderson-paak", artist_name="Anderson .Paak & The Free Nationals",
                    event_name="NPR Tiny Desk Concert",
                    performance_date=date(2016, 3, 7), duration_minutes=18,
                    performance_type="session"),
        Performance(id="td-tyler", artist_name="Tyler, The Creator",
                    event_name="NPR Tiny Desk Concert",
                    performance_date=date(2019, 11, 21), duration_minutes=20,
                    performance_type="session"),
        Performance(id="td-adele", artist_name="Adele", event_name="NPR Tiny Desk Concert",
                    performance_date=date(2015, 11, 20), duration_minutes=19,
                    performance_type="session"),
        Performance(id="td-paramore", artist_name="Paramore", event_name="NPR Tiny Desk Concert",
                    performance_date=date(2023, 4, 4), duration_minutes=15,
                    performance_type="session"),

        # Grammy / Award Performances
        Performance(id="grammy-billie-2020", artist_name="Billie Eilish",
                    event_name="Grammy Awards 2020",
                    performance_date=date(2020, 1, 26), duration_minutes=5,
                    performance_type="ceremony"),
        Performance(id="grammy-silk-sonic-2022", artist_name="Silk Sonic (Bruno Mars & Anderson .Paak)",
                    event_name="Grammy Awards 2022",
                    performance_date=date(2022, 4, 3), duration_minutes=5,
                    performance_type="ceremony"),
        Performance(id="vma-kanye-2010", artist_name="Kanye West",
                    event_name="MTV VMAs 2010",
                    performance_date=date(2010, 9, 12), duration_minutes=5,
                    performance_type="ceremony"),

        # Classic Concert Films
        Performance(id="sms-1984", artist_name="Talking Heads",
                    event_name="Stop Making Sense",
                    performance_date=date(1984, 1, 1), duration_minutes=88,
                    performance_type="concert"),
        Performance(id="last-waltz", artist_name="The Band",
                    event_name="The Last Waltz",
                    performance_date=date(1976, 11, 25), duration_minutes=117,
                    performance_type="concert"),
    ]

    db.insert_performances_bulk(seed_performances)
    logger.info(f"Loaded {len(seed_performances)} seed performances into the database.")


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="LiveScore YouTube Discovery Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py --seed --dry-run   # Load seed data and preview queries
  python pipeline.py --seed --limit 5   # Load seed data and process 5 performances
  python pipeline.py --limit 20         # Process next 20 performances in queue
  python pipeline.py                    # Process all queued performances

After running the pipeline:
  python step6_review.py                # Review candidates in terminal
  python step6_review.py --export       # Export review queue to CSV
        """,
    )
    parser.add_argument("--seed", action="store_true", help="Load sample seed data before running")
    parser.add_argument("--limit", type=int, default=100, help="Max performances to process (default: 100)")
    parser.add_argument("--dry-run", action="store_true", help="Build queries without calling YouTube API")

    args = parser.parse_args()

    if args.seed:
        load_seed_data()

    run_pipeline(limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
