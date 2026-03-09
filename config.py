"""
config.py - Load environment variables and provide configuration.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(Path(__file__).parent / ".env")


class Config:
    # YouTube API
    YOUTUBE_API_KEY: str = os.getenv("YOUTUBE_API_KEY", "")
    YOUTUBE_DAILY_QUOTA_LIMIT: int = int(os.getenv("YOUTUBE_DAILY_QUOTA_LIMIT", "10000"))

    # Scoring thresholds
    SCORE_AUTO_APPROVE: int = int(os.getenv("SCORE_AUTO_APPROVE", "75"))
    SCORE_HUMAN_REVIEW: int = int(os.getenv("SCORE_HUMAN_REVIEW", "50"))
    SCORE_AUTO_REJECT: int = int(os.getenv("SCORE_AUTO_REJECT", "25"))

    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///livescore.db")

    # Output
    OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", "./output")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # Quota tracking (100 units per search, 1 unit per video detail fetch)
    SEARCH_COST: int = 100
    DETAIL_COST: int = 1

    @classmethod
    def validate(cls):
        """Check that required config is present."""
        if not cls.YOUTUBE_API_KEY or cls.YOUTUBE_API_KEY == "YOUR_YOUTUBE_API_KEY_HERE":
            raise ValueError(
                "Missing YOUTUBE_API_KEY. Edit your .env file and add your key.\n"
                "Get one at: https://console.cloud.google.com"
            )
        os.makedirs(cls.OUTPUT_DIR, exist_ok=True)
        return True
