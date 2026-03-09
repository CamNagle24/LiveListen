# LiveScore YouTube Discovery Pipeline

Automated pipeline to discover, score, and verify YouTube videos of live musical performances.

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Add your YouTube API key to .env
#    Edit .env and replace YOUR_YOUTUBE_API_KEY_HERE with your actual key
#    Get one at: https://console.cloud.google.com
#      → Create project → Enable "YouTube Data API v3" → Create API key

# 3. Load seed data and preview queries (no API calls)
python pipeline.py --seed --dry-run

# 4. Run the pipeline for real (uses API quota)
python pipeline.py --seed --limit 5

# 5. Review candidates that need human verification
python step6_review.py
```

## Project Structure

```
livescore-youtube-pipeline/
├── .env                      # Your API key and config (EDIT THIS)
├── requirements.txt          # Python dependencies
├── config.py                 # Loads .env settings
├── models.py                 # Data classes (Performance, VideoCandidate)
├── db.py                     # SQLite database layer
├── step1_build_queries.py    # Build optimized YouTube search queries
├── step2_search_youtube.py   # Execute YouTube API searches
├── step3_enrich_results.py   # Batch-fetch video details (duration, views, HD)
├── step4_score_candidates.py # Automated 0-100 relevance scoring
├── step5_triage.py           # Sort into auto-approve / review / reject
├── step6_review.py           # Human review (terminal UI or CSV export)
├── pipeline.py               # Main orchestrator (runs steps 1-5)
└── README.md                 # This file
```

## How It Works

### Pipeline Flow

```
Performance DB  →  Step 1: Build Queries  →  Step 2: YouTube Search
                                                       ↓
Step 6: Human Review  ←  Step 5: Triage  ←  Step 4: Score  ←  Step 3: Enrich
       ↓                      ↓
  WatchSource DB         Auto-approve
                         to WatchSource DB
```

### Step-by-Step

| Step | What it does | API cost |
|------|-------------|----------|
| **Step 1** | Builds 1-3 search queries per performance (most specific → broadest) | Free |
| **Step 2** | Searches YouTube, stops early if enough results found | 100 units/search |
| **Step 3** | Batch-fetches duration, views, HD status for up to 50 videos | ~3 units/batch |
| **Step 4** | Scores 0-100 using fuzzy title matching, channel detection, duration check | Free |
| **Step 5** | Auto-approves ≥75, queues 50-74 for review, rejects <25 | Free |
| **Step 6** | Interactive terminal or CSV-based human verification | Free |

### Scoring System

Videos are scored 0-100 based on:

| Signal | Points |
|--------|--------|
| Title matches artist name (fuzzy) | +20 |
| Title matches event name (fuzzy) | +20 |
| Title contains correct year | +10 |
| From official/verified channel | +25 |
| Duration within ±30% of expected | +10 |
| HD quality | +5 |
| 100K+ views | +3-5 |
| Title contains "full performance" | +5-8 |
| Title contains "reaction" / "review" | -20 |
| Title contains "cover" / "tribute" | -30 |

### Quota Budget

Free tier: **10,000 units/day** = ~100 searches.

Each performance uses 1-3 searches (100-300 units) + 1 detail fetch (~3 units).  
Conservative estimate: **~100 units per performance**.  
Free tier processes: **~100 performances/day**.

Request a quota increase to 50K-100K units for faster processing.

## Usage Examples

```bash
# Load seed data (25 performances: Super Bowls, festivals, Tiny Desks)
python pipeline.py --seed --dry-run

# Process 5 performances with real API calls
python pipeline.py --limit 5

# Process everything in the queue
python pipeline.py

# Review candidates interactively (opens YouTube links in browser)
python step6_review.py

# Export review queue to CSV (for spreadsheet review)
python step6_review.py --export

# Import decisions from a reviewed CSV
python step6_review.py --import output/review_queue_20260218.csv

# Run individual steps for testing
python step1_build_queries.py    # Preview query generation
python step4_score_candidates.py # Test scoring with fake data
```

## Customization

### Adding More Seed Data

Edit `pipeline.py` → `load_seed_data()` to add your own performances.

### Adjusting Score Thresholds

Edit `.env`:
```
SCORE_AUTO_APPROVE=75    # Lower = more auto-approvals, less human review
SCORE_HUMAN_REVIEW=50    # Lower = more auto-rejections
SCORE_AUTO_REJECT=25     # Anything below this is discarded
```

### Adding Known Official Channels

Edit `step4_score_candidates.py` → `KNOWN_OFFICIAL_CHANNELS` dict.  
Add YouTube channel IDs for official artist/network channels.  
Videos from these channels get +25 points automatically.

### Connecting to PostgreSQL (Production)

Change `.env`:
```
DATABASE_URL=postgresql://user:pass@host:5432/livescore
```
Then update `db.py` to use `psycopg2` instead of `sqlite3`.

## Database

The pipeline uses SQLite by default (file: `livescore.db`).

**Tables:**
- `performances` — Your catalog of live performances
- `video_candidates` — All YouTube videos found and scored
- `watch_sources` — Approved video links (the final output)
- `pipeline_runs` — Audit log of each pipeline execution

Query examples:
```sql
-- See all approved watch sources
SELECT p.artist_name, p.event_name, ws.url, ws.quality
FROM watch_sources ws
JOIN performances p ON ws.performance_id = p.id;

-- See the review queue
SELECT * FROM video_candidates
WHERE triage_action = 'human_review' AND review_decision = '';

-- Pipeline run history
SELECT * FROM pipeline_runs ORDER BY started_at DESC;
```
