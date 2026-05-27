"""
db_transfer.py — TradeIntel DB backup / restore
================================================
type = "save"  → pg_dump  → tradeintel_backup.sql  (transfer this file)
type = "load"  → pg_restore via psql from tradeintel_backup.sql

Usage:
    python db_transfer.py          # uses TYPE variable below
    TYPE=save python db_transfer.py
    TYPE=load python db_transfer.py

Or set type directly in the TYPE variable below.
"""

import os
import sys
import subprocess
import logging
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
# Change this to "save" or "load", or set TYPE env var
TYPE = os.getenv("TYPE", "save").lower()   # "save" | "load"

# Backup file location — same dir as this script
BACKUP_FILE = Path(__file__).resolve().parent / "tradeintel_backup.sql"

# ── Load DB creds from config ─────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DB_CONFIG

HOST     = DB_CONFIG["host"]
PORT     = str(DB_CONFIG["port"])
DBNAME   = DB_CONFIG["dbname"]
USER     = DB_CONFIG["user"]
PASSWORD = DB_CONFIG["password"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("db_transfer")

# pg_dump / psql need password via env
pg_env = {**os.environ, "PGPASSWORD": PASSWORD}

# ── Locate pg_dump / psql (Windows: PostgreSQL may not be in PATH) ─────────────
def _find_pg_bin(exe: str) -> str:
    """Return full path to a PostgreSQL binary, searching common install dirs."""
    import shutil
    found = shutil.which(exe)
    if found:
        return found
    # Windows: search PostgreSQL versioned installs
    search_roots = [
        Path("C:/Program Files/PostgreSQL"),
        Path("C:/Program Files (x86)/PostgreSQL"),
    ]
    for root in search_roots:
        if root.exists():
            candidates = sorted(root.iterdir(), reverse=True)  # newest version first
            for ver in candidates:
                candidate = ver / "bin" / (exe + ".exe")
                if candidate.exists():
                    return str(candidate)
    raise FileNotFoundError(
        f"Could not find '{exe}'. Add PostgreSQL bin dir to PATH or install PostgreSQL."
    )

PG_DUMP = _find_pg_bin("pg_dump")
PSQL    = _find_pg_bin("psql")


def save():
    log.info(f"Dumping '{DBNAME}' -> {BACKUP_FILE}")
    cmd = [
        PG_DUMP,
        "-h", HOST,
        "-p", PORT,
        "-U", USER,
        "-d", DBNAME,
        "--no-password",
        "--clean",          # DROP before CREATE — safe re-import
        "--if-exists",      # no error if objects don't exist yet
        "--encoding", "UTF8",
        "-f", str(BACKUP_FILE),
    ]
    result = subprocess.run(cmd, env=pg_env, capture_output=True, text=True)
    if result.returncode != 0:
        log.error(f"pg_dump failed:\n{result.stderr}")
        sys.exit(1)
    size_mb = BACKUP_FILE.stat().st_size / (1024 * 1024)
    log.info(f"Done. File: {BACKUP_FILE}  ({size_mb:.2f} MB)")
    log.info("Transfer this file to the target machine, then run: TYPE=load python db_transfer.py")


def load():
    if not BACKUP_FILE.exists():
        log.error(f"Backup file not found: {BACKUP_FILE}")
        sys.exit(1)
    size_mb = BACKUP_FILE.stat().st_size / (1024 * 1024)
    log.info(f"Restoring '{DBNAME}' from {BACKUP_FILE}  ({size_mb:.2f} MB)")
    # psql runs the .sql dump (handles --clean/--if-exists DROP statements)
    cmd = [
        PSQL,
        "-h", HOST,
        "-p", PORT,
        "-U", USER,
        "-d", DBNAME,
        "--no-password",
        "-f", str(BACKUP_FILE),
    ]
    result = subprocess.run(cmd, env=pg_env, capture_output=True, text=True)
    # psql exits 0 even on non-fatal errors — check stderr for real failures
    if result.returncode != 0:
        log.error(f"psql failed (exit {result.returncode}):\n{result.stderr}")
        sys.exit(1)
    if result.stderr:
        # Common non-fatal noise: "role does not exist", "already exists" — show as warnings
        for line in result.stderr.strip().splitlines():
            log.warning(f"psql: {line}")
    log.info("Restore complete.")


if __name__ == "__main__":
    if TYPE == "save":
        save()
    elif TYPE == "load":
        load()
    else:
        log.error(f"Unknown TYPE='{TYPE}'. Use 'save' or 'load'.")
        sys.exit(1)
