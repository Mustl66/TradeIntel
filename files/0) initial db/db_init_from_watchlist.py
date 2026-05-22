"""
db_init_from_watchlist.py
--------------------------
ONE-TIME initializer. Run this before anything else.

What it does:
  1. Clears the entire DB (rss_feeds -> symbols -> pipeline_runs)
  2. Loads watchlist_status.json
  3. Inserts every symbol into `symbols`
  4. Inserts every known RSS link into `rss_feeds` (one row per URL)

After this, main.py / universe_setup.py will only ADD new feeds,
never wipe existing ones.

Usage:
    python db_init_from_watchlist.py
    python db_init_from_watchlist.py --watchlist path/to/other.json
"""

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from db.connection import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
log = logging.getLogger("db_init")

DEFAULT_WATCHLIST = Path(__file__).parent / "watchlist_status.json"


# ── helpers ──────────────────────────────────────────────────────────────────

def clear_database(conn) -> None:
    """Truncate all tables in dependency order (FK-safe)."""
    log.info("Clearing database...")
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE rss_feeds, pipeline_runs, symbols RESTART IDENTITY CASCADE;")
    conn.commit()
    log.info("All tables cleared.")


def load_watchlist(path: Path) -> list[dict]:
    log.info(f"Loading watchlist from {path}")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    log.info(f"Loaded {len(data)} symbols.")
    return data


def insert_symbols(conn, symbols: list[dict]) -> dict[str, int]:
    """
    Insert all symbols. Returns {symbol_ticker: db_id} map.
    status field in JSON is the string 'true'/'false' -> convert to bool.
    """
    log.info(f"Inserting {len(symbols)} symbols...")
    sym_to_id: dict[str, int] = {}

    skipped = 0
    with conn.cursor() as cur:
        for item in symbols:
            ticker   = item["symbol"]
            # Skip garbage rows (metadata lines embedded in JSON)
            if " " in ticker or len(ticker) > 20:
                log.warning(f"Skipping bad symbol entry: {ticker!r}")
                skipped += 1
                continue
            name     = item.get("company_name", "")
            exchange = item.get("exchange", "")
            active   = str(item.get("status", "false")).lower() == "true"

            cur.execute(
                """
                INSERT INTO symbols (symbol, company_name, exchange, status)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE
                    SET company_name = EXCLUDED.company_name,
                        exchange     = EXCLUDED.exchange,
                        status       = EXCLUDED.status,
                        last_updated_at = NOW()
                RETURNING id
                """,
                (ticker, name, exchange, active),
            )
            sym_to_id[ticker] = cur.fetchone()[0]

    conn.commit()
    log.info(f"Symbols inserted/updated: {len(sym_to_id)} (skipped {skipped} bad entries)")
    return sym_to_id


def insert_rss_feeds(conn, symbols: list[dict], sym_to_id: dict[str, int]) -> None:
    """
    Insert all RSS links from watchlist into rss_feeds.
    One row per URL. Skips duplicates via ON CONFLICT DO NOTHING.
    """
    total_feeds = 0
    rows = []

    for item in symbols:
        ticker = item["symbol"]
        sym_id = sym_to_id.get(ticker)
        if sym_id is None:
            continue

        links = item.get("rss_links") or []
        # Handle legacy single-string field (just in case)
        if isinstance(links, str):
            links = [links]

        for url in links:
            url = url.strip()
            if not url or url == "#":
                continue
            # Detect feed type and source by URL pattern (best-effort)
            if "globenewswire" in url:
                feed_type = "atom" if "atomfeed" in url else "rss"
                source    = "globenewswire"
            else:
                feed_type = "rss"
                source    = "company_ir"

            rows.append((sym_id, url, feed_type, source))

    log.info(f"Inserting {len(rows)} RSS feed rows...")

    with conn.cursor() as cur:
        for sym_id, url, feed_type, source in rows:
            cur.execute(
                """
                INSERT INTO rss_feeds (symbol_id, feed_url, feed_type, source, is_active)
                VALUES (%s, %s, %s, %s, TRUE)
                ON CONFLICT (feed_url) DO NOTHING
                """,
                (sym_id, url, feed_type, source),
            )
            total_feeds += 1

    conn.commit()
    log.info(f"RSS feeds inserted: {total_feeds}")


def add_unique_constraint_if_missing(conn) -> None:
    """
    Ensure rss_feeds(symbol_id, feed_url) has a unique constraint
    so ON CONFLICT works. Idempotent.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name = 'rss_feeds'
              AND constraint_type = 'UNIQUE'
              AND constraint_name = 'uq_rss_feeds_symbol_url'
        """)
        if cur.fetchone() is None:
            log.info("Adding unique constraint on rss_feeds(symbol_id, feed_url)...")
            cur.execute("""
                ALTER TABLE rss_feeds
                ADD CONSTRAINT uq_rss_feeds_symbol_url UNIQUE (symbol_id, feed_url)
            """)
            conn.commit()
            log.info("Constraint added.")


def add_symbol_unique_if_missing(conn) -> None:
    """Ensure symbols(symbol) has a unique constraint."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name = 'symbols'
              AND constraint_type = 'UNIQUE'
              AND constraint_name = 'uq_symbols_symbol'
        """)
        if cur.fetchone() is None:
            log.info("Adding unique constraint on symbols(symbol)...")
            cur.execute("""
                ALTER TABLE symbols
                ADD CONSTRAINT uq_symbols_symbol UNIQUE (symbol)
            """)
            conn.commit()
            log.info("Constraint added.")


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Initialize TradeIntel DB from watchlist JSON.")
    parser.add_argument(
        "--watchlist",
        default=str(DEFAULT_WATCHLIST),
        help="Path to watchlist_status.json",
    )
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Skip clearing the DB (merge mode — adds missing rows only)",
    )
    args = parser.parse_args()

    conn = get_connection()

    try:
        # Ensure constraints exist (idempotent)
        add_symbol_unique_if_missing(conn)
        add_unique_constraint_if_missing(conn)

        if not args.no_clear:
            clear_database(conn)

        symbols  = load_watchlist(Path(args.watchlist))
        sym_map  = insert_symbols(conn, symbols)
        insert_rss_feeds(conn, symbols, sym_map)

        # Summary
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM symbols")
            n_sym = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM rss_feeds")
            n_feeds = cur.fetchone()[0]

        log.info(f"Done. DB state: {n_sym} symbols, {n_feeds} RSS feeds.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
