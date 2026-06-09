"""
pipeline/news_ingest.py — Step 2: News Ingestion
==================================================
Fetches and permanently stores full-text financial news articles
from every active RSS/Atom feed in the rss_feeds table.

Design contracts:
  - Append-only: existing rows are NEVER modified or deleted.
  - Crash-safe: ON CONFLICT (article_hash) DO NOTHING means safe to re-run.
  - Dual timestamps: published_at (from feed) + inserted_at (system time).
  - Dedup hash: SHA-256(url + title + published_at_isoformat).
  - Composite index on (symbol_id, published_at DESC) → newest-first queries.
  - Full-text scraping is best-effort; falls back to feed summary on failure.

Called by main.py via news_ingest_runner.py or directly:
    python -m pipeline.news_ingest
"""

import hashlib
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser

# curl_cffi — TLS fingerprint spoof for CDN-blocked feeds (Edgio, Cloudflare)
try:
    from curl_cffi import requests as cffi_requests
    _CFFI_AVAILABLE = True
except ImportError:
    _CFFI_AVAILABLE = False

from db.connection import get_connection

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
REQUEST_TIMEOUT   = 15       # seconds per HTTP request
MAX_FULL_TEXT_LEN = 100_000  # chars; truncate absurdly long pages
SCRAPE_DELAY      = 0.2      # seconds between article scrapes per worker
FEED_WORKERS      = 10       # parallel feed fetchers
ARTICLE_WORKERS   = 12       # parallel article scrapers (be polite)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; TradeIntel/2.0; "
        "+https://github.com/tradeintel)"
    )
}

# Rotating browser header profiles — used in _fetch_feed to avoid bot detection
import random as _random

_HEADER_PROFILES = [
    {   # Chrome 124 Windows
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        # NOTE: Do NOT set Accept-Encoding. requests adds it automatically and handles
        # gzip decompression. If we override with 'br' (Brotli), requests can't decompress
        # and feedparser receives raw binary garbage → bozo=True, 0 entries.
    },
    {   # Firefox 125 Windows
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    },
    {   # Safari 17 macOS
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    },
    {   # Edge 124 Windows
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
    },
]

def _random_headers() -> dict:
    return _random.choice(_HEADER_PROFILES)


# ── Hash ───────────────────────────────────────────────────────────────────────

def _article_hash(url: str, title: str, published_at: datetime) -> str:
    """Deterministic SHA-256 over the three fields that uniquely identify an article."""
    raw = f"{url}|{title}|{published_at.isoformat()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ── Full-text scraper ──────────────────────────────────────────────────────────

def _scrape_full_text(url: str) -> Optional[str]:
    """
    Best-effort HTML scrape → extract readable body text.
    Returns None on any failure (network, parse, encoding).

    Strategy: rotate to a real browser UA + try chrome-impersonation fallback
    (cffi_requests) for sites that block plain requests (nasdaq.com, etc.).
    """
    headers = _random_headers()
    resp = None
    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT + 15)
        resp.raise_for_status()
    except Exception as exc:
        # Fall back to curl_cffi w/ chrome fingerprint — defeats most bot walls.
        try:
            resp = cffi_requests.get(url, impersonate='chrome124',
                                     timeout=REQUEST_TIMEOUT + 15)
            if resp.status_code != 200:
                logger.debug(f"Full-text scrape cffi non-200 for {url}: {resp.status_code}")
                return None
        except Exception as exc2:
            logger.debug(f"Full-text scrape failed for {url}: {exc} | cffi: {exc2}")
            return None

    try:
        content = getattr(resp, "content", None) or resp.text.encode("utf-8", "ignore")
        soup = BeautifulSoup(content, "lxml")

        # Remove nav, footer, scripts, styles
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()

        # Try common article body selectors first
        for selector in ("article", '[class*="article-body"]', '[class*="news-body"]',
                         '[class*="press-release"]', '[class*="body__content"]',
                         '[data-testid*="content"]', "main", ".content", "#content"):
            node = soup.select_one(selector)
            if node:
                text = node.get_text(separator=" ", strip=True)
                if len(text) > 200:
                    return text[:MAX_FULL_TEXT_LEN]

        # Fallback: all paragraph text
        paras = soup.find_all("p")
        text = " ".join(p.get_text(strip=True) for p in paras if len(p.get_text(strip=True)) > 40)
        return text[:MAX_FULL_TEXT_LEN] if len(text) > 200 else None

    except Exception as exc:
        logger.debug(f"Full-text parse failed for {url}: {exc}")
        return None


# ── Feed parsing ───────────────────────────────────────────────────────────────

def _parse_published(entry) -> Optional[datetime]:
    """
    Extract a timezone-aware datetime from a feedparser entry.
    Tries published_parsed (struct_time) → published (raw string) → None.
    """
    # feedparser gives us a time.struct_time in UTC
    if getattr(entry, "published_parsed", None):
        try:
            return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        except Exception:
            pass

    # Fallback: raw string via dateutil
    raw = getattr(entry, "published", None) or getattr(entry, "updated", None)
    if raw:
        try:
            dt = dateutil_parser.parse(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass

    return None


def _fetch_feed(feed_row: dict) -> list[dict]:
    """
    Fetch and parse one RSS/Atom feed.
    Returns a list of article dicts ready for DB insertion.
    """
    feed_id   = feed_row["id"]
    symbol_id = feed_row["symbol_id"]
    symbol    = feed_row["symbol"]
    url       = feed_row["feed_url"]

    try:
        # Use requests for HTTP so we get a real timeout.
        # feedparser.parse(url) uses urllib with NO timeout —
        # one hung server stalls the worker thread forever.
        resp = requests.get(url, headers=_random_headers(), timeout=REQUEST_TIMEOUT)

        # 403/429 = CDN block (Edgio, Cloudflare). Retry with curl_cffi TLS spoof.
        if resp.status_code in (403, 429) and _CFFI_AVAILABLE:
            logger.debug(f"[{symbol}] HTTP {resp.status_code} — retrying with curl_cffi: {url}")
            resp = cffi_requests.get(url, impersonate='chrome124', timeout=REQUEST_TIMEOUT)

        resp.raise_for_status()
        parsed = feedparser.parse(resp.text)
    except requests.exceptions.Timeout:
        # TLS-level timeout — some CDNs (Edgio) drop requests connections entirely.
        # Retry once with curl_cffi before giving up.
        if _CFFI_AVAILABLE:
            try:
                logger.debug(f"[{symbol}] Timeout — retrying with curl_cffi: {url}")
                resp = cffi_requests.get(url, impersonate='chrome124', timeout=REQUEST_TIMEOUT)
                parsed = feedparser.parse(resp.text)
            except Exception as exc:
                logger.warning(f"[{symbol}] curl_cffi retry also failed ({url}): {exc}")
                return []
        else:
            logger.warning(f"[{symbol}] Feed timed out ({REQUEST_TIMEOUT}s): {url}")
            return []
    except requests.exceptions.RequestException as exc:
        logger.warning(f"[{symbol}] Feed fetch error ({url}): {exc}")
        return []
    except Exception as exc:
        logger.warning(f"[{symbol}] Feed parse error ({url}): {exc}")
        return []

    if parsed.bozo and not parsed.entries:
        logger.debug(f"[{symbol}] Bozo feed, no entries: {url}")
        return []

    articles = []
    for entry in parsed.entries:
        published_at = _parse_published(entry)
        if published_at is None:
            # Skip entries with no usable timestamp — they're unfit for decay model
            logger.debug(f"[{symbol}] Skipping entry with no timestamp: {getattr(entry, 'title', '?')}")
            continue

        title   = (getattr(entry, "title",   "") or "").strip()
        link    = (getattr(entry, "link",    "") or "").strip()
        summary = (getattr(entry, "summary", "") or getattr(entry, "description", "") or "").strip()
        author  = (getattr(entry, "author",  "") or "").strip() or None

        if not link or not title:
            continue

        art_hash = _article_hash(link, title, published_at)

        articles.append({
            "symbol_id":    symbol_id,
            "feed_id":      feed_id,
            "article_hash": art_hash,
            "url":          link,
            "title":        title,
            "summary":      summary or None,
            "full_text":    None,          # filled in scrape pass
            "published_at": published_at,
            "author":       author,
            "source_name":  parsed.feed.get("title", None),
        })

    logger.debug(f"[{symbol}] Feed returned {len(articles)} entries: {url}")
    return articles


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _get_active_feeds(conn, exchange: str, limit: int, symbol_limit=False) -> list[dict]:
    """Return all active feeds joined with symbol info."""
    from pipeline_config import SYMBOL_LIMIT as _SL
    _symbol_limit = symbol_limit if symbol_limit is not False else _SL

    sql = """
        SELECT
            rf.id          AS id,
            rf.symbol_id   AS symbol_id,
            rf.feed_url    AS feed_url,
            s.symbol       AS symbol
        FROM rss_feeds rf
        JOIN symbols   s  ON s.id = rf.symbol_id
        WHERE rf.is_active = TRUE
          AND s.exchange   = %s
          AND rf.feed_type IN ('rss', 'atom', 'unknown')
        ORDER BY s.symbol
    """
    if limit > 0:
        sql += f" LIMIT {limit}"

    with conn.cursor() as cur:
        cur.execute(sql, (exchange,))
        cols = [d[0] for d in cur.description]
        feeds = [dict(zip(cols, row)) for row in cur.fetchall()]

    if _symbol_limit and isinstance(_symbol_limit, int) and _symbol_limit > 0:
        seen = []
        for f in feeds:
            if f["symbol"] not in seen:
                seen.append(f["symbol"])
            if len(seen) >= _symbol_limit:
                break
        symbol_set = set(seen)
        feeds = [f for f in feeds if f["symbol"] in symbol_set]
        logger.info(f"[news_ingest] SYMBOL_LIMIT={_symbol_limit}: {len(feeds)} feeds for {len(symbol_set)} symbols")

    return feeds


def _known_hashes(conn, hashes: list[str]) -> set[str]:
    """Return the subset of hashes that already exist in the DB (fast batch check)."""
    if not hashes:
        return set()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT article_hash FROM news_articles WHERE article_hash = ANY(%s)",
            (hashes,)
        )
        return {row[0] for row in cur.fetchall()}


def _insert_articles(conn, articles: list[dict]) -> int:
    """
    Bulk-insert articles. Returns count of newly inserted rows.
    ON CONFLICT (article_hash) DO NOTHING — safe to call repeatedly.
    """
    if not articles:
        return 0

    sql = """
        INSERT INTO news_articles
            (symbol_id, feed_id, article_hash, url, title,
             summary, full_text, published_at, author, source_name)
        VALUES
            (%(symbol_id)s, %(feed_id)s, %(article_hash)s, %(url)s, %(title)s,
             %(summary)s, %(full_text)s, %(published_at)s, %(author)s, %(source_name)s)
        ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        cur.executemany(sql, articles)
        inserted = cur.rowcount  # -1 if executemany doesn't support it on this driver
    conn.commit()

    # psycopg2 executemany rowcount is unreliable; do a count diff approach
    return max(inserted, 0)


def _update_feed_checked(conn, feed_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE rss_feeds SET last_checked_at = NOW() WHERE id = %s",
            (feed_id,)
        )
    conn.commit()


# ── Orchestration ──────────────────────────────────────────────────────────────

def run(exchange: str = "NASDAQ", limit: int = 0, scrape_full_text: bool = True) -> dict:
    """
    Main entry point called by main.py.

    exchange         : NASDAQ | NYSE
    limit            : 0 = all feeds; >0 = dev cap on feed count
    scrape_full_text : if True, attempt to scrape article body from URL
                       (set False in tests or when behind a firewall)

    Returns summary dict with counts.
    """
    exchange = exchange.upper()
    logger.info(f"[news_ingest] Starting — exchange={exchange}, limit={limit}, "
                f"scrape={scrape_full_text}")

    conn = get_connection()
    feeds = _get_active_feeds(conn, exchange, limit)
    conn.close()

    if not feeds:
        logger.warning("[news_ingest] No active feeds found. Run universe_setup first.")
        return {"feeds": 0, "articles_new": 0, "articles_skipped": 0}

    logger.info(f"[news_ingest] {len(feeds)} active feeds to process")

    # ── Phase 1: Fetch all feeds in parallel ──────────────────────────────────
    all_articles: list[dict] = []
    feed_article_map: dict[int, list[dict]] = {}  # feed_id → articles

    total_feeds = len(feeds)
    with ThreadPoolExecutor(max_workers=FEED_WORKERS, thread_name_prefix="feed") as ex:
        futures = {ex.submit(_fetch_feed, f): f for f in feeds}
        done = 0
        for fut in as_completed(futures):
            feed = futures[fut]
            try:
                arts = fut.result()
                if arts:
                    feed_article_map[feed["id"]] = arts
                    all_articles.extend(arts)
            except Exception as exc:
                logger.warning(f"[news_ingest] Feed {feed['feed_url']} raised: {exc}")
            done += 1
            if done % 50 == 0 or done == total_feeds:
                logger.info(
                    f"[news_ingest] Feeds: {done}/{total_feeds} "
                    f"| articles collected so far: {len(all_articles)}"
                )

    logger.info(f"[news_ingest] Fetched {len(all_articles)} raw entries from feeds")

    if not all_articles:
        return {"feeds": len(feeds), "articles_new": 0, "articles_skipped": 0}

    # ── Phase 2: Dedup check — skip hashes already in DB ─────────────────────
    conn = get_connection()
    existing = _known_hashes(conn, [a["article_hash"] for a in all_articles])
    conn.close()

    new_articles   = [a for a in all_articles if a["article_hash"] not in existing]
    skip_count     = len(all_articles) - len(new_articles)
    logger.info(f"[news_ingest] {len(new_articles)} new articles "
                f"({skip_count} already in DB, skipped)")

    if not new_articles:
        return {"feeds": len(feeds), "articles_new": 0, "articles_skipped": skip_count}

    # ── Phase 3: Full-text scraping (best-effort, parallel) ───────────────────
    if scrape_full_text:
        total_arts = len(new_articles)
        logger.info(f"[news_ingest] Scraping full text for {total_arts} articles...")

        def _scrape_one(art: dict) -> dict:
            art["full_text"] = _scrape_full_text(art["url"])
            time.sleep(SCRAPE_DELAY)
            return art

        scraped_done = 0
        scraped_results = []
        with ThreadPoolExecutor(max_workers=ARTICLE_WORKERS, thread_name_prefix="scrape") as ex:
            futures = {ex.submit(_scrape_one, a): a for a in new_articles}
            # NO global timeout on as_completed — let every article finish or
            # time out individually. Global timeout killed batches of 400+ articles
            # after only 20 seconds (REQUEST_TIMEOUT + 5).
            for f in as_completed(futures):
                try:
                    scraped_results.append(f.result(timeout=REQUEST_TIMEOUT + 5))
                except Exception:
                    # timed-out or failed scrape — keep article without full_text
                    art = futures[f]
                    art["full_text"] = None
                    scraped_results.append(art)
                scraped_done += 1
                if scraped_done % 100 == 0 or scraped_done == total_arts:
                    logger.info(
                        f"[news_ingest] Scraped: {scraped_done}/{total_arts} articles"
                    )
        new_articles = scraped_results

        scraped_count = sum(1 for a in new_articles if a["full_text"])
        logger.info(f"[news_ingest] Full text scraped for {scraped_count}/{len(new_articles)} articles")

    # ── Phase 4: Persist to DB ────────────────────────────────────────────────
    conn = get_connection()
    try:
        inserted = _insert_articles(conn, new_articles)

        # Update last_checked_at for every feed we touched
        for feed_id in feed_article_map:
            _update_feed_checked(conn, feed_id)

    finally:
        conn.close()

    logger.info(
        f"[news_ingest] Done — feeds={len(feeds)}, "
        f"new={len(new_articles)}, skipped={skip_count}"
    )

    return {
        "feeds":            len(feeds),
        "articles_fetched": len(all_articles),
        "articles_new":     len(new_articles),
        "articles_skipped": skip_count,
    }


# ── Standalone execution ───────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
        stream=sys.stderr,
    )

    ap = argparse.ArgumentParser(description="Step 2 – News Ingestion")
    ap.add_argument("--exchange", "-e", default="NASDAQ")
    ap.add_argument("--limit",    "-l", type=int, default=0,
                    help="Cap feed count (dev mode). 0 = all.")
    ap.add_argument("--no-scrape", action="store_true",
                    help="Skip full-text scraping (summary only).")
    args = ap.parse_args()

    from db import create_tables, test_connection
    if not test_connection():
        logger.error("DB connection failed.")
        sys.exit(1)
    create_tables()

    result = run(
        exchange=args.exchange,
        limit=args.limit,
        scrape_full_text=not args.no_scrape,
    )
    print(result)
