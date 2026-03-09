"""
step6_review.py - Human review interface for video candidates.

Two modes:
  1. Terminal mode (default): Interactive CLI for reviewing candidates
  2. Export mode: Dump review queue to CSV for spreadsheet-based review

Usage:
  python step6_review.py              # Interactive terminal review
  python step6_review.py --export     # Export to CSV
  python step6_review.py --import review_decisions.csv  # Import decisions from CSV
"""

import sys
import csv
import json
import webbrowser
import logging
from pathlib import Path
from datetime import datetime

import db
from config import Config
from models import VideoCandidate

logger = logging.getLogger(__name__)


# ============================================================
# TERMINAL REVIEW MODE
# ============================================================

def review_terminal(batch_size: int = 50):
    """
    Interactive terminal-based review of video candidates.

    For each candidate, shows:
    - Performance metadata (artist, event, date)
    - Video details (title, channel, duration, views, score breakdown)
    - YouTube link (option to open in browser)

    Reviewer picks: (a)pprove full, (p)artial, (r)eject, (s)kip, (o)pen in browser, (q)uit
    """
    candidates = db.get_candidates_for_review(limit=batch_size)

    if not candidates:
        print("\n✅ No candidates in the review queue. You're all caught up!")
        return

    print(f"\n{'='*70}")
    print(f"  LIVESCORE VIDEO REVIEW QUEUE")
    print(f"  {len(candidates)} candidates to review")
    print(f"{'='*70}")

    reviewed = 0
    approved = 0
    rejected = 0

    for i, row in enumerate(candidates):
        video_id = row["video_id"]
        perf_id = row["performance_id"]
        url = f"https://www.youtube.com/watch?v={video_id}"

        print(f"\n{'─'*70}")
        print(f"  [{i+1}/{len(candidates)}]  Score: {row['relevance_score']}")
        print(f"{'─'*70}")

        # Performance info
        print(f"  🎵 Performance: {row['artist_name']}")
        if row.get("event_name"):
            print(f"     Event: {row['event_name']}")
        if row.get("performance_date"):
            print(f"     Date: {row['performance_date']}")

        # Video info
        print(f"\n  📹 Video: {row['title']}")
        print(f"     Channel: {row['channel_title']}")
        print(f"     Duration: {row['duration_minutes']:.0f} min | Quality: {row['definition'].upper()}")
        print(f"     Views: {row['view_count']:,} | Likes: {row['like_count']:,}")
        print(f"     URL: {url}")

        # Score breakdown
        if row.get("score_breakdown"):
            try:
                breakdown = json.loads(row["score_breakdown"])
                if breakdown:
                    print(f"\n  📊 Score Breakdown:")
                    for key, val in breakdown.items():
                        print(f"       {key}: {val}")
            except json.JSONDecodeError:
                pass

        # Get decision
        print(f"\n  Actions:")
        print(f"    [a] Approve (full performance)")
        print(f"    [p] Approve (partial/clip)")
        print(f"    [r] Reject (wrong content)")
        print(f"    [s] Skip (unsure, review later)")
        print(f"    [o] Open in browser first")
        print(f"    [q] Quit review session")

        while True:
            choice = input(f"\n  Decision: ").strip().lower()

            if choice == "o":
                webbrowser.open(url)
                print("  ↳ Opened in browser. Now choose a/p/r/s:")
                continue

            if choice == "a":
                decision = "approve_full"
                notes = input("  Notes (optional, press Enter to skip): ").strip()
                db.update_review_decision(video_id, perf_id, decision, notes)

                # Also add as a WatchSource
                db.insert_watch_source(
                    performance_id=perf_id,
                    video_id=video_id,
                    is_official=False,  # Human-reviewed, not auto-detected as official
                    is_full=True,
                    quality=row.get("definition", "sd"),
                )
                approved += 1
                print("  ✅ Approved (full performance) and added as WatchSource.")
                break

            elif choice == "p":
                decision = "approve_partial"
                notes = input("  Notes (optional): ").strip()
                db.update_review_decision(video_id, perf_id, decision, notes)

                db.insert_watch_source(
                    performance_id=perf_id,
                    video_id=video_id,
                    is_official=False,
                    is_full=False,
                    quality=row.get("definition", "sd"),
                )
                approved += 1
                print("  ✅ Approved (partial) and added as WatchSource.")
                break

            elif choice == "r":
                decision = "reject"
                notes = input("  Reason (optional): ").strip()
                db.update_review_decision(video_id, perf_id, decision, notes)
                rejected += 1
                print("  ❌ Rejected.")
                break

            elif choice == "s":
                print("  ⏭️  Skipped.")
                break

            elif choice == "q":
                print(f"\n{'='*70}")
                print(f"  Session complete: {reviewed} reviewed, {approved} approved, {rejected} rejected")
                print(f"{'='*70}")
                return

            else:
                print("  Invalid choice. Enter a, p, r, s, o, or q.")

        reviewed += 1

    print(f"\n{'='*70}")
    print(f"  Queue complete! {reviewed} reviewed, {approved} approved, {rejected} rejected")
    print(f"{'='*70}")


# ============================================================
# CSV EXPORT/IMPORT MODE
# ============================================================

def export_review_queue(output_path: str | None = None):
    """Export the review queue to CSV for spreadsheet-based review."""
    candidates = db.get_candidates_for_review(limit=500)

    if not candidates:
        print("No candidates in the review queue.")
        return

    if not output_path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"{Config.OUTPUT_DIR}/review_queue_{timestamp}.csv"

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "video_id", "performance_id", "relevance_score",
            "video_title", "channel", "duration_min", "views",
            "youtube_url", "artist_name", "event_name", "performance_date",
            "decision", "notes",
        ])

        for row in candidates:
            writer.writerow([
                row["video_id"],
                row["performance_id"],
                row["relevance_score"],
                row["title"],
                row["channel_title"],
                f"{row['duration_minutes']:.0f}",
                row["view_count"],
                f"https://www.youtube.com/watch?v={row['video_id']}",
                row["artist_name"],
                row.get("event_name", ""),
                row.get("performance_date", ""),
                "",  # decision column - reviewer fills this in
                "",  # notes column
            ])

    print(f"Exported {len(candidates)} candidates to: {output_path}")
    print(f"Fill in the 'decision' column with: approve_full, approve_partial, reject, or skip")
    print(f"Then run: python step6_review.py --import {output_path}")


def import_review_decisions(csv_path: str):
    """Import review decisions from a filled-in CSV."""
    imported = 0
    approved = 0
    rejected = 0

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            decision = row.get("decision", "").strip().lower()
            if not decision or decision == "skip":
                continue

            video_id = row["video_id"]
            perf_id = row["performance_id"]
            notes = row.get("notes", "")

            db.update_review_decision(video_id, perf_id, decision, notes)

            if decision in ("approve_full", "approve_partial"):
                is_full = decision == "approve_full"
                db.insert_watch_source(
                    performance_id=perf_id,
                    video_id=video_id,
                    is_official=False,
                    is_full=is_full,
                )
                approved += 1
            elif decision == "reject":
                rejected += 1

            imported += 1

    print(f"Imported {imported} decisions: {approved} approved, {rejected} rejected")


# ============================================================
# CLI ENTRY POINT
# ============================================================

def main():
    db.init_db()

    if len(sys.argv) > 1:
        if sys.argv[1] == "--export":
            output = sys.argv[2] if len(sys.argv) > 2 else None
            export_review_queue(output)
        elif sys.argv[1] == "--import":
            if len(sys.argv) < 3:
                print("Usage: python step6_review.py --import <csv_path>")
                sys.exit(1)
            import_review_decisions(sys.argv[2])
        else:
            print("Usage:")
            print("  python step6_review.py              # Interactive terminal review")
            print("  python step6_review.py --export     # Export queue to CSV")
            print("  python step6_review.py --import X   # Import decisions from CSV")
    else:
        review_terminal()


if __name__ == "__main__":
    main()
