"""
db/connection.py
-----------------
PostgreSQL connection factory.
Reads credentials from config.py (which itself reads .env or falls back to defaults).
"""

import logging
import psycopg2
import psycopg2.extras
from config import DB_CONFIG

logger = logging.getLogger(__name__)


def get_connection():
    """Return a new psycopg2 connection. Caller is responsible for closing it."""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def test_connection() -> bool:
    """Quick connectivity check. Returns True on success."""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        logger.info("Database connection OK.")
        return True
    except Exception as e:
        logger.error(f"Database connection FAILED: {e}")
        return False
