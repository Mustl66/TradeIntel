"""
migrate_feed_type.py — One-time feed classifier
================================================
Classifies every row in rss_feeds as 'rss', 'atom', or 'html'.

Strategy (in order):
  1. URL heuristic — if URL path contains /rss, /atom, .xml, /feed → almost certainly RSS/Atom.
     Stamp it and skip the HTTP call (saves bandwidth on 2000+ feeds).
  2. HTTP HEAD → Content-Type header. application/rss+xml or application/atom+xml → done.
  3. HTTP GET → attempt feedparser. If entries > 0 → rss/atom. Else → html.

Run once:
    python migrate_feed_type.py

Safe to re-run — skips rows already classified (feed_type != 'unknown').
Supports --reclassify flag to force re-check everything.
"""

import argparse
import logging
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import feedparser
import requests

sys.path.insert(0, ".")
from db.connection import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("migrate_feed_type")

# ── Constants ─────────────────────────────────────────────────────────────────
TIMEOUT       = 15
WORKERS       = 12
DELAY_MIN     = 0.5
DELAY_MAX     = 1.0

# Full browser header profiles — rotate to avoid bot detection
_HEADER_PROFILES = [
    {   # Chrome 124 / Windows
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
    },
    {   # Firefox 125 / Windows
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
    },
    {   # Safari 17 / macOS
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    },
    {   # Edge 124 / Windows
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    },
]


def _random_headers() -> dict:
    return random.choice(_HEADER_PROFILES)


def _random_delay():
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


# ── URL heuristic (no HTTP needed) ────────────────────────────────────────────

_RSS_URL_SIGNALS = (
    "/rss", "/atom", "/feed", "/feeds",
    "rss.xml", "atom.xml", "feed.xml", "news.xml",
    "rss/news", "rss/press", "atom/news",
)

# Query-param patterns used by IR platforms (Q4/Business Wire/Cision etc.)
_RSS_QUERY_SIGNALS = (
    "pagetemplate=rss",
    "format=rss",
    "format=feed",
    "type=atom",
    "type=rss",
    "feed=rss",
    "feed=atom",
    "output=rss",
)


def _url_heuristic(url: str) -> str | None:
    """
    Return 'rss', 'atom', or None (inconclusive) purely from URL structure.
    No HTTP call made.
    """
    low    = url.lower()
    parsed = urlparse(low)
    path   = parsed.path
    query  = parsed.query

    # Atom signals first
    if "atom" in path or "type=atom" in query or "format=atom" in query:
        return "atom"

    # Path signals
    for sig in _RSS_URL_SIGNALS:
        if sig in low:
            return "rss"

    # Query-param signals (IR platform RSS endpoints)
    for sig in _RSS_QUERY_SIGNALS:
        if sig in query:
            return "rss"

    # Common GlobeNewswire / company IR patterns
    if low.endswith(".xml"):
        return "rss"

    return None


# ── HTTP classification ────────────────────────────────────────────────────────

def _classify_via_http(url: str) -> str:
    """
    Returns 'rss', 'atom', or 'html'.
    Step 1: HEAD → Content-Type.
    Step 2: GET → feedparser probe.
    """
    headers = _random_headers()
    _random_delay()

    try:
        # Step 1 — HEAD request, fast
        head = requests.head(url, headers=headers, timeout=TIMEOUT,
                             allow_redirects=True)
        ct = head.headers.get("Content-Type", "").lower()
        if "rss" in ct:
            return "rss"
        if "atom" in ct:
            return "atom"
        if "xml" in ct:
            # Could be RSS — confirm with feedparser
            pass
        elif "html" in ct:
            return "html"

    except Exception:
        pass  # Fall through to GET probe

    # Step 2 — GET + feedparser
    _random_delay()
    try:
        resp = requests.get(url, headers=_random_headers(),
                            timeout=TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
        if parsed.entries:
            # Distinguish rss vs atom from feed version field
            ver = getattr(parsed, "version", "") or ""
            return "atom" if "atom" in ver.lower() else "rss"
        return "html"
    except Exception:
        # Can't reach it — keep as unknown, don't break classification
        return "unknown"


# ── Main classifier ────────────────────────────────────────────────────────────

def _classify_row(row: dict) -> tuple[int, str]:
    """Returns (feed_id, classified_type)."""
    url = row["feed_url"]

    # Fast path — URL heuristic (no network)
    guess = _url_heuristic(url)
    if guess:
        return row["id"], guess

    # Slow path — HTTP probe
    result = _classify_via_http(url)
    return row["id"], result


def run(reclassify: bool = False):
    conn = get_connection()

    with conn.cursor() as cur:
        if reclassify:
            cur.execute("SELECT id, feed_url FROM rss_feeds ORDER BY id")
        else:
            cur.execute("SELECT id, feed_url FROM rss_feeds WHERE feed_type = 'unknown' ORDER BY id")
        cols  = [d[0] for d in cur.description]
        rows  = [dict(zip(cols, r)) for r in cur.fetchall()]

    conn.close()

    if not rows:
        logger.info("No unclassified feeds — all done.")
        return

    logger.info(f"Classifying {len(rows)} feeds ({WORKERS} workers)...")

    counts = {"rss": 0, "atom": 0, "html": 0, "unknown": 0}
    updates: list[tuple[str, int]] = []

    total = len(rows)
    done  = 0

    with ThreadPoolExecutor(max_workers=WORKERS, thread_name_prefix="classify") as ex:
        futures = {ex.submit(_classify_row, r): r for r in rows}
        for fut in as_completed(futures):
            done += 1
            try:
                feed_id, feed_type = fut.result()
                counts[feed_type] = counts.get(feed_type, 0) + 1
                updates.append((feed_type, feed_id))
            except Exception as exc:
                logger.warning(f"Classify error: {exc}")

            if done % 100 == 0 or done == total:
                logger.info(
                    f"Progress: {done}/{total} — "
                    f"rss={counts['rss']} atom={counts['atom']} "
                    f"html={counts['html']} unknown={counts.get('unknown',0)}"
                )

    # Bulk update
    if updates:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.executemany(
                "UPDATE rss_feeds SET feed_type = %s WHERE id = %s",
                updates
            )
        conn.commit()
        conn.close()
        logger.info(f"Updated {len(updates)} rows.")

    logger.info(
        f"Classification complete — "
        f"rss={counts['rss']} atom={counts['atom']} "
        f"html={counts['html']} unknown={counts.get('unknown',0)}"
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="One-time feed type classifier")
    ap.add_argument("--reclassify", action="store_true",
                    help="Re-classify ALL feeds, not just unknowns")
    args = ap.parse_args()
    run(reclassify=args.reclassify)
