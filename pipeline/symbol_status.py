"""
pipeline/symbol_status.py
---------------------------
Step 1a: Universe Management
  - Fetches live symbols from TradingView for a given exchange.
  - Upserts them into the `symbols` table.
  - Marks delisted symbols (no longer in live feed) as status=FALSE.
  - Restores re-listed symbols to status=TRUE.
  - Logs the run in `pipeline_runs`.
"""

import sys
import logging
import requests
import urllib.parse
from datetime import datetime, timezone

import psycopg2.extras

from db.connection import get_connection

logger = logging.getLogger(__name__)


# ── TradingView Fetcher ──────────────────────────────────────────────────────

def build_gnw_search_url(company_name: str) -> str:
    first_encode  = urllib.parse.quote(company_name)
    double_encode = urllib.parse.quote(first_encode)
    return f"https://www.globenewswire.com/en/search/organization/{double_encode}"


def fetch_live_symbols(exchange: str) -> dict[str, str]:
    """
    Returns {ticker: company_name} for all live symbols on the exchange.
    Two-pass approach: raw list then chunk-detail queries.
    """
    try:
        import pandas as pd
        from tradingview_screener import Query, col
    except ImportError as e:
        logger.error(f"Missing dependency: {e}")
        sys.exit(1)

    exchange_upper = exchange.upper()
    logger.info("Fetching raw symbol list from TradingView...")

    try:
        url = "https://scanner.tradingview.com/america/scan"
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        all_symbols = [item["s"] for item in r.json().get("data", []) if "s" in item]
    except Exception as e:
        logger.error(f"Failed to fetch TradingView symbol list: {e}")
        return {}

    selected = [s for s in all_symbols if s.startswith(f"{exchange_upper}:")]
    logger.info(f"Found {len(selected)} raw symbols for {exchange_upper}.")

    if not selected:
        return {}

    result = {}
    chunk_size = 500

    for i in range(0, len(selected), chunk_size):
        chunk = selected[i: i + chunk_size]
        page  = (i // chunk_size) + 1
        total = (len(selected) + chunk_size - 1) // chunk_size
        logger.info(f"Fetching detail batch {page}/{total}...")

        try:
            q = Query().select("name", "description", "exchange").limit(chunk_size)
            try:
                q = q.set_tickers(*chunk)
            except Exception:
                try:
                    q = q.set_tickers(chunk)
                except Exception:
                    syms_only = [t.split(":")[1] for t in chunk]
                    q = q.where(col("name").isin(syms_only))

            res = q.get_scanner_data()
            if res is None:
                continue
            _, df = res
            if df is None or df.empty:
                continue

            for _, row in df.iterrows():
                ticker = row.get("name", "")
                name   = row.get("description", "")
                if ticker:
                    result[ticker] = name

        except Exception as e:
            logger.warning(f"Batch {page} failed: {e}")

    logger.info(f"Resolved details for {len(result)} symbols.")
    return result


# ── DB Operations ─────────────────────────────────────────────────────────────

def upsert_symbols(conn, live: dict[str, str], exchange: str) -> dict:
    """
    Upsert live symbols and mark delisted ones.
    Returns stats dict.
    """
    exchange_upper = exchange.upper()
    now = datetime.now(timezone.utc)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

        # ── 1. Load existing symbols for this exchange ────────────────────
        cur.execute(
            "SELECT id, symbol, status FROM symbols WHERE exchange = %s",
            (exchange_upper,)
        )
        existing = {row["symbol"]: row for row in cur.fetchall()}

        live_tickers = set(live.keys())
        existing_tickers = set(existing.keys())

        added           = 0
        restored        = 0
        delisted        = 0
        unchanged       = 0

        # ── 2. Upsert live symbols ───────────────────────────────────────
        for ticker, company_name in live.items():
            gnw_url = build_gnw_search_url(company_name)

            if ticker not in existing:
                # New symbol
                cur.execute(
                    """
                    INSERT INTO symbols
                        (symbol, exchange, company_name, status, gnw_search_url, last_updated_at)
                    VALUES (%s, %s, %s, TRUE, %s, %s)
                    ON CONFLICT (symbol, exchange) DO NOTHING
                    """,
                    (ticker, exchange_upper, company_name, gnw_url, now)
                )
                added += 1
                logger.info(f"  [NEW]      {ticker} – {company_name}")

            else:
                row = existing[ticker]
                if not row["status"]:
                    # Was delisted, now back
                    cur.execute(
                        """
                        UPDATE symbols
                        SET status = TRUE, company_name = %s, last_updated_at = %s
                        WHERE id = %s
                        """,
                        (company_name, now, row["id"])
                    )
                    restored += 1
                    logger.info(f"  [RESTORED] {ticker} back on {exchange_upper}.")
                else:
                    # Active and still active — refresh name/timestamp
                    cur.execute(
                        "UPDATE symbols SET company_name = %s, last_updated_at = %s WHERE id = %s",
                        (company_name, now, row["id"])
                    )
                    unchanged += 1

        # ── 3. Mark delisted symbols ─────────────────────────────────────
        for ticker, row in existing.items():
            if ticker not in live_tickers and row["status"]:
                cur.execute(
                    "UPDATE symbols SET status = FALSE, last_updated_at = %s WHERE id = %s",
                    (now, row["id"])
                )
                delisted += 1
                logger.info(f"  [DELISTED] {ticker} no longer on {exchange_upper}.")

    return {
        "live_count":   len(live_tickers),
        "added":        added,
        "restored":     restored,
        "delisted":     delisted,
        "unchanged":    unchanged,
    }


# ── Pipeline Entry Point ──────────────────────────────────────────────────────

def run(exchange: str, limit: int = 0) -> dict:
    """
    Main entry for the symbol_status step.
    limit=0 means all symbols; limit>0 is a dev/test cap.
    Returns stats dict.
    """
    exchange_upper = exchange.upper()
    conn = get_connection()
    run_id = None

    try:
        # ── Start audit record ────────────────────────────────────────────
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (step, exchange, status)
                VALUES ('symbol_status', %s, 'running')
                RETURNING id
                """,
                (exchange_upper,)
            )
            run_id = cur.fetchone()[0]
        conn.commit()

        # ── Fetch live data ───────────────────────────────────────────────
        live = fetch_live_symbols(exchange_upper)
        if not live:
            raise RuntimeError("No live symbols fetched from TradingView.")

        # Apply dev limit (just cap which NEW symbols get added)
        if limit > 0:
            logger.info(f"Dev limit active: capping at {limit} symbols.")
            live_keys = sorted(live.keys())[:limit]
            live = {k: live[k] for k in live_keys}

        # ── Upsert into DB ────────────────────────────────────────────────
        stats = upsert_symbols(conn, live, exchange_upper)
        conn.commit()

        # ── Update audit record ───────────────────────────────────────────
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_runs
                SET status = 'success',
                    finished_at = NOW(),
                    symbols_total   = %s,
                    symbols_added   = %s,
                    symbols_updated = %s
                WHERE id = %s
                """,
                (
                    stats["live_count"],
                    stats["added"],
                    stats["restored"] + stats["delisted"],
                    run_id,
                )
            )
        conn.commit()

        print(f"\n  Exchange:     {exchange_upper}")
        print(f"  Live symbols: {stats['live_count']}")
        print(f"  New added:    {stats['added']}")
        print(f"  Restored:     {stats['restored']}")
        print(f"  Delisted:     {stats['delisted']}")
        print(f"  Unchanged:    {stats['unchanged']}")

        return stats

    except Exception as e:
        conn.rollback()
        logger.error(f"symbol_status run failed: {e}")

        if run_id:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE pipeline_runs SET status='failed', finished_at=NOW(), error_message=%s WHERE id=%s",
                        (str(e), run_id)
                    )
                conn.commit()
            except Exception:
                pass

        raise

    finally:
        conn.close()
