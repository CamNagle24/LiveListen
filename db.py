"""
db.py - SQLite database for storing performances, candidates, and approved watch sources.

For production, swap SQLite for PostgreSQL by changing the DATABASE_URL in .env.
"""

import sqlite3
import json
import os
from datetime import date
from typing import Optional
from config import Config
from models import Performance, VideoCandidate


DB_PATH = Config.DATABASE_URL.replace("sqlite:///", "")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create tables if they don't exist."""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS performances (
            id TEXT PRIMARY KEY,
            artist_name TEXT NOT NULL,
            event_name TEXT,
            venue_name TEXT,
            performance_date TEXT,
            duration_minutes INTEGER,
            performance_type TEXT DEFAULT 'concert',
            official_channel_ids TEXT DEFAULT '[]',
            has_watch_source INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS video_candidates (
            video_id TEXT NOT NULL,
            performance_id TEXT NOT NULL,
            title TEXT,
            channel_title TEXT,
            channel_id TEXT,
            published_at TEXT,
            description TEXT,
            duration_iso TEXT,
            duration_minutes REAL,
            definition TEXT,
            view_count INTEGER DEFAULT 0,
            like_count INTEGER DEFAULT 0,
            thumbnail_url TEXT,
            relevance_score INTEGER DEFAULT 0,
            score_breakdown TEXT DEFAULT '{}',
            triage_action TEXT DEFAULT '',
            review_decision TEXT DEFAULT '',
            reviewer_notes TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (video_id, performance_id),
            FOREIGN KEY (performance_id) REFERENCES performances(id)
        );

        CREATE TABLE IF NOT EXISTS watch_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            performance_id TEXT NOT NULL,
            platform TEXT DEFAULT 'youtube',
            video_id TEXT,
            url TEXT NOT NULL,
            is_official INTEGER DEFAULT 0,
            is_full_performance INTEGER DEFAULT 1,
            quality TEXT DEFAULT 'hd',
            requires_subscription INTEGER DEFAULT 0,
            verified INTEGER DEFAULT 1,
            added_by TEXT DEFAULT 'pipeline',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (performance_id) REFERENCES performances(id)
        );

        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP,
            performances_processed INTEGER DEFAULT 0,
            searches_made INTEGER DEFAULT 0,
            quota_used INTEGER DEFAULT 0,
            videos_found INTEGER DEFAULT 0,
            auto_approved INTEGER DEFAULT 0,
            sent_to_review INTEGER DEFAULT 0,
            auto_rejected INTEGER DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_candidates_perf
            ON video_candidates(performance_id);
        CREATE INDEX IF NOT EXISTS idx_candidates_triage
            ON video_candidates(triage_action);
        CREATE INDEX IF NOT EXISTS idx_candidates_score
            ON video_candidates(relevance_score DESC);
        CREATE INDEX IF NOT EXISTS idx_watch_sources_perf
            ON watch_sources(performance_id);
    """)
    conn.commit()
    conn.close()
    print(f"Database initialized at: {DB_PATH}")


# ----- Performance CRUD -----

def insert_performance(p: Performance):
    conn = get_connection()
    conn.execute("""
        INSERT OR IGNORE INTO performances
        (id, artist_name, event_name, venue_name, performance_date,
         duration_minutes, performance_type, official_channel_ids)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        p.id, p.artist_name, p.event_name, p.venue_name,
        p.performance_date.isoformat() if p.performance_date else None,
        p.duration_minutes, p.performance_type,
        json.dumps(p.official_channel_ids),
    ))
    conn.commit()
    conn.close()


def insert_performances_bulk(performances: list[Performance]):
    conn = get_connection()
    conn.executemany("""
        INSERT OR IGNORE INTO performances
        (id, artist_name, event_name, venue_name, performance_date,
         duration_minutes, performance_type, official_channel_ids)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [(
        p.id, p.artist_name, p.event_name, p.venue_name,
        p.performance_date.isoformat() if p.performance_date else None,
        p.duration_minutes, p.performance_type,
        json.dumps(p.official_channel_ids),
    ) for p in performances])
    conn.commit()
    conn.close()


def get_performances_without_sources(limit: int = 100) -> list[Performance]:
    """Get performances that have no watch sources yet (the job queue)."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT * FROM performances
        WHERE has_watch_source = 0
        ORDER BY created_at ASC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()

    results = []
    for r in rows:
        results.append(Performance(
            id=r["id"],
            artist_name=r["artist_name"],
            event_name=r["event_name"],
            venue_name=r["venue_name"],
            performance_date=date.fromisoformat(r["performance_date"]) if r["performance_date"] else None,
            duration_minutes=r["duration_minutes"],
            performance_type=r["performance_type"],
            official_channel_ids=json.loads(r["official_channel_ids"] or "[]"),
        ))
    return results


# ----- Video Candidate CRUD -----

def insert_candidates(candidates: list[VideoCandidate]):
    conn = get_connection()
    conn.executemany("""
        INSERT OR REPLACE INTO video_candidates
        (video_id, performance_id, title, channel_title, channel_id,
         published_at, description, duration_iso, duration_minutes,
         definition, view_count, like_count, thumbnail_url,
         relevance_score, score_breakdown, triage_action,
         review_decision, reviewer_notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(
        c.video_id, c.performance_id, c.title, c.channel_title, c.channel_id,
        c.published_at, c.description, c.duration_iso, c.duration_minutes,
        c.definition, c.view_count, c.like_count, c.thumbnail_url,
        c.relevance_score, json.dumps(c.score_breakdown), c.triage_action,
        c.review_decision, c.reviewer_notes,
    ) for c in candidates])
    conn.commit()
    conn.close()


def get_candidates_for_review(limit: int = 50) -> list[dict]:
    """Get candidates in the human review queue, highest score first."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT vc.*, p.artist_name, p.event_name, p.performance_date
        FROM video_candidates vc
        JOIN performances p ON vc.performance_id = p.id
        WHERE vc.triage_action = 'human_review'
          AND vc.review_decision = ''
        ORDER BY vc.relevance_score DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_review_decision(video_id: str, performance_id: str, decision: str, notes: str = ""):
    conn = get_connection()
    conn.execute("""
        UPDATE video_candidates
        SET review_decision = ?, reviewer_notes = ?
        WHERE video_id = ? AND performance_id = ?
    """, (decision, notes, video_id, performance_id))
    conn.commit()
    conn.close()


# ----- Watch Source CRUD -----

def insert_watch_source(performance_id: str, video_id: str, is_official: bool,
                        is_full: bool = True, quality: str = "hd"):
    conn = get_connection()
    url = f"https://www.youtube.com/watch?v={video_id}"
    conn.execute("""
        INSERT OR IGNORE INTO watch_sources
        (performance_id, video_id, url, is_official, is_full_performance, quality)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (performance_id, video_id, url, int(is_official), int(is_full), quality))

    # Mark performance as having a source
    conn.execute("""
        UPDATE performances SET has_watch_source = 1 WHERE id = ?
    """, (performance_id,))
    conn.commit()
    conn.close()


# ----- Pipeline Run Tracking -----

def start_pipeline_run() -> int:
    conn = get_connection()
    cursor = conn.execute("INSERT INTO pipeline_runs DEFAULT VALUES")
    run_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return run_id


def finish_pipeline_run(run_id: int, stats: dict):
    conn = get_connection()
    conn.execute("""
        UPDATE pipeline_runs SET
            finished_at = CURRENT_TIMESTAMP,
            performances_processed = ?,
            searches_made = ?,
            quota_used = ?,
            videos_found = ?,
            auto_approved = ?,
            sent_to_review = ?,
            auto_rejected = ?
        WHERE id = ?
    """, (
        stats.get("performances_processed", 0),
        stats.get("searches_made", 0),
        stats.get("quota_used", 0),
        stats.get("videos_found", 0),
        stats.get("auto_approved", 0),
        stats.get("sent_to_review", 0),
        stats.get("auto_rejected", 0),
        run_id,
    ))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
    print("Database ready.")
