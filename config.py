"""
config.py — TradeIntel Central Configuration
=============================================
All runtime settings live here with sensible defaults.
Override anything via environment variables or a .env file.
"""

import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env if it exists (silently skipped if not present)
load_dotenv(Path(__file__).resolve().parent / ".env")

# ── Database ─────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.getenv("DB_HOST",     "localhost"),
    "port":            int(os.getenv("DB_PORT", 5432)),
    "dbname":          os.getenv("DB_NAME",     "tradeintel"),
    "user":            os.getenv("DB_USER",     "postgres"),
    "password":        os.getenv("DB_PASSWORD", "postgres"),
    "client_encoding": "utf8",
}

# ── Pipeline defaults ─────────────────────────────────────────────────────────
DEFAULT_EXCHANGE = os.getenv("DEFAULT_EXCHANGE", "NASDAQ")
SYMBOL_LIMIT     = int(os.getenv("SYMBOL_LIMIT", 0))   # 0 = all
RSS_WORKERS      = int(os.getenv("RSS_WORKERS",  8))
