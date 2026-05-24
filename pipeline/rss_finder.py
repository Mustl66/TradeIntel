"""
pipeline/rss_finder.py
-----------------------
Step 1b: RSS Feed Discovery
  - Reads active symbols from the `symbols` table that have no RSS feed yet.
  - Scrapes GlobeNewswire to find RSS/Atom feed URLs.
  - Stores discovered feeds in the `rss_feeds` table.
  - Skips symbols that already have feeds (unless --refresh).
  - Logs the run in `pipeline_runs`.
"""

import re
import sys
import logging
import os
import requests
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import psycopg2.extras

from db.connection import get_connection

logger = logging.getLogger(__name__)

GNW_BASE    = "https://www.globenewswire.com"
MAX_WORKERS = int(os.getenv("RSS_WORKERS", 8))
HEADERS     = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


# ── GlobeNewswire Scrapers ────────────────────────────────────────────────────

def build_gnw_search_url(company_name: str) -> str:
    first_encode  = urllib.parse.quote(company_name)
    double_encode = urllib.parse.quote(first_encode)
    return f"{GNW_BASE}/en/search/organization/{double_encode}"


def get_first_article_url(search_url: str) -> str | None:
    try:
        r = requests.get(search_url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        match = re.search(r'href="(/news-release/[^"]+\.html)"', r.text)
        if match:
            return GNW_BASE + match.group(1)
    except Exception as e:
        logger.debug(f"get_first_article_url error: {e}")
    return None


def extract_feeds_from_article(article_url: str) -> dict:
    """
    Returns { "rss": url|None, "atom": url|None, "org_id": int|None }
    """
    result = {"rss": None, "atom": None, "org_id": None}
    try:
        r = requests.get(article_url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        html = r.text

        # JSON blob: escaped variant
        rss_match = re.search(
            r'\\\"Key\\\":\\\"rss\\\"[^}]*\\\"Url\\\":\\\"(/rssfeed/organization/[^\\]+)\\\"',
            html,
        )
        if rss_match:
            result["rss"] = GNW_BASE + rss_match.group(1)

        atom_match = re.search(
            r'\\\"Key\\\":\\\"atom\\\"[^}]*\\\"Url\\\":\\\"(/atomfeed/organization/[^\\]+)\\\"',
            html,
        )
        if atom_match:
            result["atom"] = GNW_BASE + atom_match.group(1)

        # Unescaped fallbacks
        if not result["rss"]:
            m = re.search(r'href="(/rssfeed/organization/[^"]+)"', html)
            if m:
                result["rss"] = GNW_BASE + m.group(1)

        if not result["atom"]:
            m = re.search(r'href="(/atomfeed/organization/[^"]+)"', html)
            if m:
                result["atom"] = GNW_BASE + m.group(1)

        # Numeric org ID
        org_match = re.search(r"ContextOrgId\s*:\s*(\d+)", html)
        if org_match:
            result["org_id"] = int(org_match.group(1))

    except Exception as e:
        logger.debug(f"extract_feeds_from_article error: {e}")

    return result


def _classify_feed(url: str) -> tuple[str, str]:
    """Returns (feed_type, source) for a URL."""
    if "globenewswire.com" in url:
        source = "globenewswire"
        feed_type = "rss" if "/rssfeed/" in url else "atom" if "/atomfeed/" in url else "unknown"
    else:
        source = "company_ir"
        feed_type = "rss"
    return feed_type, source


# ── Per-Symbol Worker ─────────────────────────────────────────────────────────

def process_symbol(symbol_row: dict, refresh: bool) -> dict:
    """
    Find RSS/Atom feeds for one symbol row from the DB.
    Returns a result dict with lists of discovered feed URLs.
    """
    sym_id      = symbol_row["id"]
    ticker      = symbol_row["symbol"]
    company     = symbol_row["company_name"]
    has_rss     = symbol_row["has_rss"]
    gnw_url     = symbol_row["gnw_search_url"] or build_gnw_search_url(company)

    if has_rss and not refresh:
        logger.info(f"  [SKIP]   {ticker} – RSS already present.")
        return {"sym_id": sym_id, "ticker": ticker, "feeds": [], "org_id": None, "skipped": True}

    logger.info(f"  [SEARCH] {ticker} – {company}")

    article_url = get_first_article_url(gnw_url)
    if not article_url:
        logger.warning(f"  [NO ART] {ticker} – no articles on GlobeNewswire.")
        return {"sym_id": sym_id, "ticker": ticker, "feeds": [], "org_id": None, "skipped": False}

    feeds_raw = extract_feeds_from_article(article_url)
    found_urls = []

    if feeds_raw["rss"]:
        found_urls.append(feeds_raw["rss"])
        logger.info(f"  [RSS]    {ticker} – {feeds_raw['rss']}")
    if feeds_raw["atom"]:
        found_urls.append(feeds_raw["atom"])
        logger.info(f"  [ATOM]   {ticker} – {feeds_raw['atom']}")

    if not found_urls:
        logger.warning(f"  [NO RSS] {ticker} – no feed in article.")

    return {
        "sym_id": sym_id,
        "ticker": ticker,
        "feeds":  found_urls,
        "org_id": feeds_raw["org_id"],
        "skipped": False,
    }


# ── DB Operations ─────────────────────────────────────────────────────────────

def load_target_symbols(conn, exchange: str, limit: int) -> list[dict]:
    """
    Load active symbols for this exchange.
    Includes a flag 'has_rss' so we know whether to skip.
    """
    exchange_upper = exchange.upper()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                s.id,
                s.symbol,
                s.company_name,
                s.gnw_search_url,
                EXISTS(
                    SELECT 1 FROM rss_feeds f
                    WHERE f.symbol_id = s.id AND f.is_active = TRUE
                ) AS has_rss
            FROM symbols s
            WHERE s.exchange = %s
            ORDER BY s.symbol
            """ + ("LIMIT %s" if limit > 0 else ""),
            (exchange_upper, limit) if limit > 0 else (exchange_upper,)
        )
        return [dict(row) for row in cur.fetchall()]


def save_feeds(conn, results: list[dict]) -> int:
    """
    Upsert discovered feeds into rss_feeds. Returns count of new rows inserted.
    """
    now = datetime.now(timezone.utc)
    new_count = 0

    with conn.cursor() as cur:
        for res in results:
            if not res["feeds"]:
                continue

            # Update gnw_org_id if we found it
            if res.get("org_id"):
                cur.execute(
                    "UPDATE symbols SET gnw_org_id = %s, last_updated_at = %s WHERE id = %s AND gnw_org_id IS NULL",
                    (res["org_id"], now, res["sym_id"])
                )

            for url in res["feeds"]:
                feed_type, source = _classify_feed(url)
                cur.execute(
                    """
                    INSERT INTO rss_feeds (symbol_id, feed_url, feed_type, source, discovered_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (feed_url) DO UPDATE
                        SET is_active = TRUE,
                            last_checked_at = %s
                    """,
                    (res["sym_id"], url, feed_type, source, now, now)
                )
                if cur.rowcount == 1:
                    new_count += 1

    return new_count


# ── Pipeline Entry Point ──────────────────────────────────────────────────────

def run(exchange: str, limit: int = 0, refresh: bool = False) -> dict:
    """
    Main entry for the rss_finder step.
    limit=0 means process all active symbols for the exchange.
    """
    exchange_upper = exchange.upper()
    conn = get_connection()
    run_id = None

    try:
        # ── Start audit record ────────────────────────────────────────────
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pipeline_runs (step, exchange, status) VALUES ('rss_finder', %s, 'running') RETURNING id",
                (exchange_upper,)
            )
            run_id = cur.fetchone()[0]
        conn.commit()

        # ── Load targets ─────────────────────────────────────────────────
        targets = load_target_symbols(conn, exchange_upper, limit)
        logger.info(f"Processing {len(targets)} symbols for {exchange_upper}.")

        if not targets:
            logger.warning("No active symbols found. Run symbol_status first.")
            return {"checked": 0, "found": 0, "skipped": 0}

        # ── Parallel scraping ─────────────────────────────────────────────
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_symbol, row, refresh): row
                for row in targets
            }
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    row = futures[future]
                    logger.error(f"Worker failed for {row['symbol']}: {e}")

        # ── Persist results ───────────────────────────────────────────────
        new_feeds = save_feeds(conn, results)
        conn.commit()

        found   = sum(1 for r in results if r["feeds"])
        skipped = sum(1 for r in results if r["skipped"])
        no_rss  = len(results) - found - skipped

        # ── Update audit record ───────────────────────────────────────────
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_runs
                SET status = 'success', finished_at = NOW(),
                    symbols_total = %s, feeds_found = %s
                WHERE id = %s
                """,
                (len(targets), new_feeds, run_id)
            )
        conn.commit()

        print(f"\n  Exchange:      {exchange_upper}")
        print(f"  Symbols checked: {len(targets)}")
        print(f"  RSS found:       {found}")
        print(f"  New feeds in DB: {new_feeds}")
        print(f"  Already had RSS: {skipped}")
        print(f"  No RSS found:    {no_rss}")

        return {"checked": len(targets), "found": found, "new_feeds": new_feeds, "skipped": skipped}

    except Exception as e:
        conn.rollback()
        logger.error(f"rss_finder run failed: {e}")

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
