"""
orchestrator.py — Phase 4: Parallel Background Worker Orchestrator
===================================================================
Runs as a SEPARATE process alongside admin.py.
Three concurrent workers on different intervals:

  Worker 1 (every 1 min):  GlobeNewswire live tracker — NASDAQ latest 50 articles
  Worker 2 (every 1 hour): Universal news pipeline    — RSS + HTML for all symbols
  Worker 3 (every 24 hrs): Macro & market research    — sector multiplier refresh

Start with:
    python orchestrator.py

or in background:
    python orchestrator.py &

All workers share a thread-safe DB connection pool via get_connection() (per-call).
Workers run in threads — if one crashes it restarts automatically after its interval.

Config (pipeline_config.py):
    WORKER1_INTERVAL = 60
    WORKER2_INTERVAL = 3600
    WORKER3_INTERVAL = 86400
"""

import hashlib
import json
import logging
import sys
import threading
import time
from datetime import datetime, timezone

import feedparser
import requests
import psycopg2.extras

from db.connection import get_conn
from pipeline_config import (
    WORKER1_INTERVAL,
    WORKER2_INTERVAL,
    WORKER3_INTERVAL,
    SYMBOL_LIMIT,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("orchestrator")


# ═══════════════════════════════════════════════════════════════════════════════
# Worker 1 — GlobeNewswire Live Tracker (every 1 minute)
# ═══════════════════════════════════════════════════════════════════════════════

# Official GNW RSS feed — returns last 20 NASDAQ press releases, refreshed ~every minute
_GNW_RSS_URL = "https://www.globenewswire.com/RssFeed/exchange/NASDAQ"
_GNW_RSS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}


def _fetch_gnw_articles() -> list[dict]:
    """
    Fetch GlobeNewswire NASDAQ RSS feed.
    Tickers are in <category domain="...rss/stock">Nasdaq:CWST</category> tags.
    Returns list of dicts with keys: title, url, tickers, pub_str.
    """
    try:
        resp = requests.get(_GNW_RSS_URL, headers=_GNW_RSS_HEADERS, timeout=15)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
    except Exception as e:
        logger.warning(f"[Worker1] GNW RSS fetch failed: {e}")
        return []

    if feed.bozo:
        logger.warning(f"[Worker1] GNW RSS parse warning: {feed.bozo_exception}")

    articles = []
    for entry in feed.entries:
        try:
            title = entry.get("title", "").strip()
            url   = entry.get("link", "").strip()
            if not title or not url:
                continue

            # Extract tickers from category tags with domain ending in /rss/stock
            tickers = []
            for tag in entry.get("tags", []):
                scheme = tag.get("scheme", "")
                term   = tag.get("term", "")
                if "rss/stock" in (scheme or "") and ":" in term:
                    # Format: "Nasdaq:CWST" -> "CWST"
                    ticker = term.split(":", 1)[1].strip().upper()
                    if ticker:
                        tickers.append(ticker)

            # Published date as ISO string
            pub_str = None
            if entry.get("published"):
                pub_str = entry["published"]

            articles.append({
                "title":   title,
                "url":     url,
                "tickers": tickers,
                "pub_str": pub_str,
            })
        except Exception:
            continue

    logger.info(f"[Worker1] GNW RSS returned {len(articles)} articles")
    return articles


def _resolve_symbol_id(conn, ticker: str) -> int | None:
    """Look up symbol_id from ticker string."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM symbols WHERE symbol = %s AND status = TRUE LIMIT 1",
            (ticker.upper(),)
        )
        row = cur.fetchone()
    return row[0] if row else None


def _article_hash(url: str, title: str) -> str:
    return hashlib.sha256(f"{url}|{title}".encode()).hexdigest()


def _insert_article_if_new(conn, symbol_id: int, title: str, url: str,
                            pub_str: str | None) -> int | None:
    """Insert article, return id if new, None if already existed."""
    from datetime import datetime, timezone
    try:
        pub_dt = datetime.fromisoformat(pub_str) if pub_str else datetime.now(timezone.utc)
    except Exception:
        pub_dt = datetime.now(timezone.utc)
    if pub_dt.tzinfo is None:
        pub_dt = pub_dt.replace(tzinfo=timezone.utc)

    h = _article_hash(url, title)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO news_articles
                (symbol_id, article_hash, url, title, published_at, source_name)
            VALUES (%s, %s, %s, %s, %s, 'GlobeNewswire')
            ON CONFLICT (article_hash) DO NOTHING
            RETURNING id
        """, (symbol_id, h, url, title, pub_dt))
        row = cur.fetchone()
    conn.commit()
    return row[0] if row else None


def worker1_tick():
    """One tick of Worker 1: fetch, resolve, insert new, score instantly."""
    from pipeline.sentiment_scoring import score_single_article

    articles = _fetch_gnw_articles()
    if not articles:
        return

    conn = get_conn()
    new_count = 0
    try:
        for art in articles:
            for ticker in art["tickers"]:
                symbol_id = _resolve_symbol_id(conn, ticker)
                if not symbol_id:
                    logger.debug(f"[Worker1] ticker {ticker} not in DB — skipping")
                    continue
                art_id = _insert_article_if_new(
                    conn, symbol_id, art["title"], art["url"], art["pub_str"]
                )
                if art_id:
                    new_count += 1
                    # Score immediately in background thread to not block the loop
                    threading.Thread(
                        target=score_single_article,
                        args=(art_id, symbol_id),
                        daemon=True,
                    ).start()
    finally:
        conn.close()

    logger.info(f"[Worker1] tick done — fetched={len(articles)} new={new_count} symbol_matches={new_count}")
    if new_count:
        logger.info(f"[Worker1] {new_count} new GNW articles inserted + queued for scoring")


# ═══════════════════════════════════════════════════════════════════════════════
# Worker 2 — Universal News Pipeline (every 1 hour)
# ═══════════════════════════════════════════════════════════════════════════════

def worker2_tick():
    """Run RSS + HTML ingest for symbols with new articles, then score them."""
    from news_ingest_runner import run as news_run
    from pipeline.sentiment_scoring import run as sentiment_run

    try:
        logger.info("[Worker2] Starting RSS + HTML ingest...")
        result = news_run(exchange="NASDAQ", limit=0, scrape_full_text=True)
        new_articles = result.get("articles_new", 0) + result.get("html_new", 0)
        logger.info(f"[Worker2] Ingest done — new={new_articles}")

        if new_articles > 0:
            logger.info("[Worker2] Running sentiment scoring on new articles...")
            score_result = sentiment_run(exchange="NASDAQ", limit=0)
            logger.info(f"[Worker2] Scoring done — {score_result}")
        else:
            logger.info("[Worker2] No new articles — skipping sentiment scoring")
    except Exception as e:
        logger.error(f"[Worker2] Error: {e}", exc_info=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Worker 3 — Macro & Market Research (every 24 hours)
# ═══════════════════════════════════════════════════════════════════════════════

def worker3_tick():
    """Sync market research feeds + recompute sector multipliers."""
    try:
        logger.info("[Worker3] Starting market research ingest...")
        from pipeline.market_research_ingest import run as mr_run
        mr_result = mr_run()
        logger.info(f"[Worker3] Market research ingest done — {mr_result}")

        logger.info("[Worker3] Running macro multiplier LLM analysis...")
        from pipeline.macro_multiplier import run as macro_run
        macro_result = macro_run()
        logger.info(f"[Worker3] Macro multiplier done — {macro_result}")
    except Exception as e:
        logger.error(f"[Worker3] Error: {e}", exc_info=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Generic worker loop wrapper
# ═══════════════════════════════════════════════════════════════════════════════

def _run_worker(name: str, interval: int, tick_fn):
    """
    Run tick_fn every `interval` seconds.
    Catches all exceptions — worker never dies permanently.
    """
    logger.info(f"[{name}] Started — interval={interval}s")
    while True:
        t0 = time.time()
        try:
            tick_fn()
        except Exception as e:
            logger.error(f"[{name}] Tick failed: {e}", exc_info=True)
        elapsed = time.time() - t0
        sleep_for = max(0, interval - elapsed)
        logger.debug(f"[{name}] Sleeping {sleep_for:.0f}s until next tick")
        time.sleep(sleep_for)


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    logger.info("=" * 60)
    logger.info("TradeIntel Orchestrator starting")
    logger.info(f"  Worker1 (GlobeNewswire live): every {WORKER1_INTERVAL}s")
    logger.info(f"  Worker2 (News pipeline):      every {WORKER2_INTERVAL}s")
    logger.info(f"  Worker3 (Macro research):     every {WORKER3_INTERVAL}s")
    logger.info("=" * 60)

    threads = [
        threading.Thread(
            target=_run_worker,
            args=("Worker1", WORKER1_INTERVAL, worker1_tick),
            daemon=True,
            name="Worker1-GNW",
        ),
        # Worker2 and Worker3 disabled — will be enabled in a future phase
        # threading.Thread(
        #     target=_run_worker,
        #     args=("Worker2", WORKER2_INTERVAL, worker2_tick),
        #     daemon=True,
        #     name="Worker2-News",
        # ),
        # threading.Thread(
        #     target=_run_worker,
        #     args=("Worker3", WORKER3_INTERVAL, worker3_tick),
        #     daemon=True,
        #     name="Worker3-Macro",
        # ),
    ]

    for t in threads:
        t.start()
        logger.info(f"Thread {t.name} started")

    # Keep main thread alive — workers are daemons so they die when main exits
    try:
        while True:
            time.sleep(60)
            alive = [t.name for t in threads if t.is_alive()]
            dead  = [t.name for t in threads if not t.is_alive()]
            if dead:
                logger.error(f"Dead threads detected: {dead} — restart orchestrator")
            logger.debug(f"Heartbeat — alive: {alive}")
    except KeyboardInterrupt:
        logger.info("Orchestrator stopped by user")


if __name__ == "__main__":
    main()
