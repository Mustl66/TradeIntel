"""
pipeline/market_research_ingest.py — Phase 3: Market Research RSS Ingestion
=============================================================================
Fetches articles from market_research_feeds table (Research and Markets,
SNS Insider, MarketsandMarkets, FDA, etc.) and stores them in
market_research_articles. These articles are then processed by
macro_multiplier.py to derive sector growth multipliers.

Usage:
    python -m pipeline.market_research_ingest
    python -m pipeline.market_research_ingest --limit 10   # dev/test
"""

import hashlib
import logging
import random
import time
import warnings
from contextlib import contextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import feedparser
import requests

warnings.filterwarnings("ignore")

try:
    from curl_cffi import requests as curl_requests
    _HAS_CURL = True
except ImportError:
    _HAS_CURL = False

from db.connection import get_connection

logger = logging.getLogger(__name__)

# ── Browser header pool ───────────────────────────────────────────────────────
_HEADER_POOL = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
                      "Gecko/20100101 Firefox/125.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    },
]


def _headers() -> dict:
    return random.choice(_HEADER_POOL)


@contextmanager
def _suppress_stderr():
    import os, sys
    devnull = os.open(os.devnull, os.O_WRONLY)
    old_stderr = os.dup(2)
    os.dup2(devnull, 2)
    os.close(devnull)
    try:
        yield
    finally:
        os.dup2(old_stderr, 2)
        os.close(old_stderr)


# ── Feed fetching ─────────────────────────────────────────────────────────────

def _fetch_feed(url: str) -> feedparser.FeedParserDict | None:
    """Fetch RSS/Atom feed. Falls back to curl_cffi on TLS/403 errors."""
    try:
        resp = requests.get(url, headers=_headers(), timeout=20)
        if resp.status_code in (403, 429) and _HAS_CURL:
            raise requests.exceptions.ConnectionError("blocked")
        resp.raise_for_status()
        with _suppress_stderr():
            parsed = feedparser.parse(resp.text)
        if parsed.bozo and not parsed.entries:
            return None
        return parsed
    except Exception:
        if not _HAS_CURL:
            return None
        try:
            resp = curl_requests.get(
                url, headers=_headers(), timeout=20, impersonate="chrome124"
            )
            with _suppress_stderr():
                parsed = feedparser.parse(resp.text)
            return parsed if parsed.entries else None
        except Exception as e2:
            logger.debug(f"curl_cffi also failed for {url}: {e2}")
            return None


def _parse_date(entry) -> datetime:
    """Parse published date from feed entry. Falls back to now()."""
    for attr in ("published", "updated", "created"):
        val = getattr(entry, attr, None)
        if val:
            try:
                return parsedate_to_datetime(val)
            except Exception:
                pass
    return datetime.now(timezone.utc)


def _make_hash(url: str, title: str, published_at: datetime) -> str:
    raw = f"{url}|{title}|{published_at.isoformat()}"
    return hashlib.sha256(raw.encode()).hexdigest()


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_active_feeds(limit: int = 0) -> list[dict]:
    conn = get_connection()
    with conn.cursor() as cur:
        q = """
            SELECT id, feed_url, source_name
            FROM market_research_feeds
            WHERE is_active = TRUE
            ORDER BY id
        """
        if limit:
            q += f" LIMIT {limit}"
        cur.execute(q)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()
    return rows


def _insert_articles(articles: list[dict]) -> int:
    """Bulk insert. Returns count of newly inserted rows."""
    if not articles:
        return 0
    conn = get_connection()
    inserted = 0
    with conn.cursor() as cur:
        for a in articles:
            cur.execute("""
                INSERT INTO market_research_articles
                    (feed_id, article_hash, url, title, summary, published_at, source_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                a["feed_id"],
                a["article_hash"],
                a["url"],
                a["title"],
                a.get("summary", ""),
                a["published_at"],
                a.get("source_name", ""),
            ))
            if cur.rowcount:
                inserted += 1
        # Update last_checked_at
        feed_ids = list({a["feed_id"] for a in articles})
        cur.execute("""
            UPDATE market_research_feeds SET last_checked_at = NOW()
            WHERE id = ANY(%s)
        """, (feed_ids,))
    conn.commit()
    conn.close()
    return inserted


# ── Main orchestrator ─────────────────────────────────────────────────────────

def run(limit: int = 0) -> dict:
    """Fetch all active market research feeds and store articles."""
    started_at = datetime.now(timezone.utc)
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_runs (step, started_at, status)
            VALUES ('market_research_ingest', NOW(), 'running') RETURNING id
        """)
        run_id = cur.fetchone()[0]
    conn.commit()
    conn.close()

    feeds = _get_active_feeds(limit)
    logger.info(f"Processing {len(feeds)} market research feeds...")

    total_inserted = 0
    total_fetched  = 0

    for feed in feeds:
        parsed = _fetch_feed(feed["feed_url"])
        if not parsed or not parsed.entries:
            logger.warning(f"[MR] No entries from {feed['feed_url']}")
            continue

        articles = []
        for entry in parsed.entries:
            title = (entry.get("title") or "").strip()
            url   = (entry.get("link")  or "").strip()
            if not title or not url:
                continue
            pub   = _parse_date(entry)
            summary = entry.get("summary", "") or ""
            # strip HTML tags from summary
            import re
            summary = re.sub(r"<[^>]+>", " ", summary).strip()[:2000]

            articles.append({
                "feed_id":      feed["id"],
                "article_hash": _make_hash(url, title, pub),
                "url":          url,
                "title":        title,
                "summary":      summary,
                "published_at": pub,
                "source_name":  feed["source_name"],
            })

        total_fetched  += len(articles)
        inserted = _insert_articles(articles)
        total_inserted += inserted
        logger.info(
            f"[MR] {feed['source_name'] or feed['feed_url'][:50]} "
            f"→ {len(articles)} fetched, {inserted} new"
        )
        time.sleep(0.3)

    duration = (datetime.now(timezone.utc) - started_at).total_seconds()

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_runs
            SET status='success', finished_at=NOW(),
                symbols_total=%s, symbols_added=%s
            WHERE id=%s
        """, (total_fetched, total_inserted, run_id))
    conn.commit()
    conn.close()

    logger.info(
        f"Market research ingest done — {total_fetched} fetched, "
        f"{total_inserted} new, {duration:.1f}s"
    )
    return {"fetched": total_fetched, "inserted": total_inserted, "duration_s": round(duration, 1)}


if __name__ == "__main__":
    import argparse, sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
                        stream=sys.stderr)
    p = argparse.ArgumentParser()
    p.add_argument("--limit", "-l", type=int, default=0)
    args = p.parse_args()
    print(run(limit=args.limit))
