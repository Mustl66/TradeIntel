"""
pipeline/edgar_ingest.py — SEC EDGAR 8-K Filing Ingestion
===========================================================
Fetches official press releases (8-K filings) from the SEC EDGAR API.
Covers ALL listed US companies — no JS rendering, no bot detection, no Cloudflare.

Strategy:
  1. Download bulk ticker→CIK map from SEC (cached for 24h)
  2. For each symbol in our DB: resolve CIK, fetch submissions JSON
  3. Filter for 8-K and 8-K/A filings (current reports / press releases)
  4. Fetch full HTML text of each filing
  5. Deduplicate via SHA-256 (same as news_ingest), insert into news_articles

Rate limit: SEC requests max 10 req/sec. We stay at ~5 req/sec with 0.2s delay.
User-Agent MUST be: "name email" format — SEC blocks generic agents.
"""

import hashlib
import json
import logging
import re
import time
import random
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
import sys

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from dateutil import parser as dateutil_parser
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

sys.path.insert(0, ".")
from db.connection import get_connection

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
EDGAR_USER_AGENT  = "TradeIntel research@tradeintel.com"
EDGAR_HEADERS     = {"User-Agent": EDGAR_USER_AGENT, "Accept-Encoding": "gzip, deflate"}
EDGAR_DELAY_MIN   = 0.15   # SEC rate limit: stay well under 10 req/sec
EDGAR_DELAY_MAX   = 0.35
EDGAR_TIMEOUT     = 20
BULK_MAP_CACHE    = Path("scratch/edgar_cik_map.json")
BULK_MAP_TTL_H    = 24     # re-download CIK map every 24 hours
MAX_FILINGS       = 40     # max 8-K filings to fetch per symbol per run
MAX_FULL_TEXT_LEN = 120_000
WORKERS           = 4      # conservative — EDGAR requests, not scraping

# 8-K item codes that are actual press releases (not just governance/exec changes)
# Item 2.02 = Results of Operations, Item 7.01/8.01 = press release attachments
# We include all 8-K for now — full content available anyway
PRESS_RELEASE_FORMS = {"8-K", "8-K/A"}


# ── CIK map ───────────────────────────────────────────────────────────────────

def _load_cik_map() -> dict[str, str]:
    """Load ticker→CIK mapping. Cache locally for 24h."""
    if BULK_MAP_CACHE.exists():
        age = time.time() - BULK_MAP_CACHE.stat().st_mtime
        if age < BULK_MAP_TTL_H * 3600:
            with open(BULK_MAP_CACHE) as f:
                return json.load(f)

    logger.info("[edgar] Downloading SEC bulk ticker→CIK map...")
    r = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers=EDGAR_HEADERS,
        timeout=30
    )
    r.raise_for_status()
    raw = r.json()
    mapping = {v["ticker"].upper(): str(v["cik_str"]).zfill(10) for v in raw.values()}
    BULK_MAP_CACHE.parent.mkdir(exist_ok=True)
    with open(BULK_MAP_CACHE, "w") as f:
        json.dump(mapping, f)
    logger.info(f"[edgar] CIK map loaded: {len(mapping)} companies")
    return mapping


# ── Hashing ───────────────────────────────────────────────────────────────────

def _article_hash(url: str, title: str, published_at: datetime) -> str:
    raw = f"{url}|{title}|{published_at.isoformat()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ── Date parsing ──────────────────────────────────────────────────────────────

def _parse_date(raw) -> Optional[datetime]:
    if not raw:
        return None
    try:
        if isinstance(raw, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", raw.strip()):
            dt = datetime.strptime(raw.strip(), "%Y-%m-%d")
            return dt.replace(tzinfo=timezone.utc)
        dt = dateutil_parser.parse(str(raw))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


# ── Filing text extraction ─────────────────────────────────────────────────────

def _extract_filing_text(url: str) -> str:
    """Fetch 8-K HTML document and extract clean text."""
    try:
        time.sleep(random.uniform(EDGAR_DELAY_MIN, EDGAR_DELAY_MAX))
        r = requests.get(url, headers=EDGAR_HEADERS, timeout=EDGAR_TIMEOUT)
        if r.status_code != 200:
            return ""
        # EDGAR files are HTML or plain text
        content_type = r.headers.get("content-type", "")
        if "html" in content_type or url.endswith((".htm", ".html")):
            soup = BeautifulSoup(r.content, "lxml")
            # Remove script/style/header junk
            for tag in soup(["script", "style", "nav", "footer", "head"]):
                tag.decompose()
            text = soup.get_text(separator="\n", strip=True)
        else:
            text = r.text
        # Trim
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        return text[:MAX_FULL_TEXT_LEN]
    except Exception as e:
        logger.debug(f"[edgar] Text fetch failed {url}: {e}")
        return ""


# ── Per-symbol filing fetch ────────────────────────────────────────────────────

def _fetch_symbol_filings(ticker: str, cik: str, seen_hashes: set) -> list[dict]:
    """Fetch 8-K filings for one ticker from EDGAR submissions API."""
    articles = []
    try:
        time.sleep(random.uniform(EDGAR_DELAY_MIN, EDGAR_DELAY_MAX))
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        r = requests.get(url, headers=EDGAR_HEADERS, timeout=EDGAR_TIMEOUT)
        if r.status_code != 200:
            logger.debug(f"[edgar] {ticker}: submissions fetch failed ({r.status_code})")
            return []

        d = r.json()
        recent = d.get("filings", {}).get("recent", {})
        forms   = recent.get("form", [])
        dates   = recent.get("filingDate", [])
        accs    = recent.get("accessionNumber", [])
        docs    = recent.get("primaryDocument", [])

        # Also check older filings pages if available
        # (SEC splits into pages of 40 — we fetch first page only for now)

        count = 0
        for i in range(len(forms)):
            if forms[i] not in PRESS_RELEASE_FORMS:
                continue
            if count >= MAX_FILINGS:
                break

            filing_date = _parse_date(dates[i]) if i < len(dates) else None
            if not filing_date:
                continue

            acc_clean = accs[i].replace("-", "") if i < len(accs) else ""
            doc       = docs[i] if i < len(docs) else ""
            if not doc or not acc_clean:
                continue

            cik_int   = int(cik)
            doc_url   = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/{doc}"
            # Build a stable title from form + date (actual title extracted from content)
            title_stub = f"{ticker} 8-K Filing {dates[i]}"

            art_hash = _article_hash(doc_url, title_stub, filing_date)
            if art_hash in seen_hashes:
                count += 1
                continue

            # Fetch full text
            full_text = _extract_filing_text(doc_url)
            if not full_text:
                count += 1
                continue

            # Try to extract a real title from the document
            title = _extract_title_from_text(full_text, title_stub)

            # Recompute hash with real title
            art_hash = _article_hash(doc_url, title, filing_date)
            if art_hash in seen_hashes:
                count += 1
                continue

            articles.append({
                "url":          doc_url,
                "title":        title,
                "published_at": filing_date,
                "full_text":    full_text,
                "article_hash": art_hash,
                "source":       "edgar_8k",
            })
            seen_hashes.add(art_hash)
            count += 1

    except Exception as e:
        logger.warning(f"[edgar] {ticker} error: {e}")

    return articles


def _extract_title_from_text(text: str, fallback: str) -> str:
    """Extract a meaningful title from 8-K text content."""
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    # Hard skip: EDGAR metadata boilerplate (first ~30 lines are junk)
    skip_exact = re.compile(
        r"^(false|true|\d{10}|\d{4}-\d{2}-\d{2}|UNITED STATES|SECURITIES AND EXCHANGE"
        r"|FORM 8-K|CURRENT REPORT|PURSUANT TO SECTION|Washington, D\.C\.|Date of Report"
        r"|Registrant'?s? telephone|Former name|Securities registered|Check the appropriate"
        r"|Indicate by check|Commission File|IRS Employer|State or other|Zip Code"
        r"|Suite \d+|One |Two |Three |Four |Five |\(\d{3}\)"
        r"|Soliciting material|Written communications|Pre-commencement|Post-commencement"
        r"|Emerging growth|Shell company|Amendment Flag|Document Type|Entity|Fiscal Year"
        r"|Item \d+\.|Exhibit \d+|N/A$"
        r"|If an emerging|indicate by check|simultaneously satisfy|filing obli"
        r"|intended to satisfy|filed.*Exchange Act|pursuant to Rule|under the Exchange"
        r"|17 CFR|Securities Act|Exchange Act|Registrant|registrant)",
        re.I
    )

    candidates = []
    for line in lines[5:]:   # skip first 5 lines (always EDGAR metadata)
        if len(line) < 20 or len(line) > 250:
            continue
        if skip_exact.match(line):
            continue
        # Skip lines that are ONLY uppercase letters/digits/punctuation (company names, addresses)
        if re.match(r"^[A-Z0-9\s\.\,\-\(\)\/]+$", line) and len(line) < 80:
            continue
        # Must contain at least one lowercase letter (real sentence)
        if not re.search(r"[a-z]", line):
            continue
        candidates.append(line)
        if len(candidates) >= 8:
            break

    # Prefer longer candidates (more descriptive titles)
    for c in sorted(candidates, key=len, reverse=True):
        if len(c) > 25:
            return c[:200]

    return fallback


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _load_symbols_without_rss(conn, limit: int = 0) -> list[tuple]:
    """
    Return symbols that have NO working RSS/atom feed.
    Includes:
    - Symbols with zero rss_feeds rows
    - Symbols with only html/unknown feeds (Nasdaq pages, Yahoo, etc.)
    These are the ones EDGAR should cover.
    Returns list of (symbol_id, symbol).
    """
    query = """
        SELECT s.id, s.symbol
        FROM symbols s
        WHERE s.exchange = 'NASDAQ'
          AND NOT EXISTS (
              SELECT 1 FROM rss_feeds r
              WHERE r.symbol_id = s.id
                AND r.feed_type IN ('rss', 'atom')
                AND r.is_active = true
          )
          AND NOT EXISTS (
              SELECT 1 FROM rss_feeds r
              WHERE r.symbol_id = s.id
                AND r.source = 'edgar_8k'
          )
        ORDER BY s.symbol
    """
    if limit:
        query += f" LIMIT {limit}"
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def _load_symbols_with_edgar_source(conn, limit: int = 0) -> list[tuple]:
    """
    Return symbols that have an edgar_8k entry in rss_feeds.
    For re-fetching new filings.
    """
    query = """
        SELECT DISTINCT s.id, s.symbol
        FROM symbols s
        JOIN rss_feeds r ON r.symbol_id = s.id
        WHERE r.source = 'edgar_8k'
          AND r.is_active = true
        ORDER BY s.symbol
    """
    if limit:
        query += f" LIMIT {limit}"
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def _get_seen_hashes(conn) -> set:
    """Load all existing article hashes to skip dupes."""
    with conn.cursor() as cur:
        cur.execute("SELECT article_hash FROM news_articles")
        return {row[0] for row in cur.fetchall()}


def _ensure_edgar_feed_row(conn, symbol_id: int, ticker: str) -> int:
    """
    Create or return the rss_feeds row for EDGAR source.
    We store one synthetic feed row per ticker so news_articles.feed_id links correctly.
    """
    url = f"https://data.sec.gov/submissions/CIK{{cik}}/edgar/{ticker}"
    with conn.cursor() as cur:
        # Check existing
        cur.execute(
            "SELECT id FROM rss_feeds WHERE symbol_id = %s AND source = 'edgar_8k'",
            (symbol_id,)
        )
        row = cur.fetchone()
        if row:
            return row[0]
        # Insert synthetic feed row
        cur.execute("""
            INSERT INTO rss_feeds (symbol_id, feed_url, feed_type, source, is_active, discovered_at)
            VALUES (%s, %s, 'api', 'edgar_8k', true, NOW())
            ON CONFLICT (feed_url) DO UPDATE SET is_active = true
            RETURNING id
        """, (symbol_id, f"https://data.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={ticker}&type=8-K"))
        conn.commit()
        return cur.fetchone()[0]


def _insert_articles(conn, feed_id: int, symbol_id: int, articles: list[dict]) -> int:
    """Batch insert articles. Returns count of newly inserted rows."""
    if not articles:
        return 0
    inserted = 0
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        for a in articles:
            cur.execute("""
                INSERT INTO news_articles
                    (symbol_id, feed_id, url, title, full_text, published_at, inserted_at, article_hash, source_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'edgar_8k')
                ON CONFLICT (article_hash) DO NOTHING
            """, (
                symbol_id,
                feed_id,
                a["url"],
                a["title"],
                a["full_text"],
                a["published_at"],
                now,
                a["article_hash"],
            ))
            if cur.rowcount:
                inserted += 1
    conn.commit()
    return inserted


# ── Main entry ────────────────────────────────────────────────────────────────

def run(limit: int = 0):
    """
    Main EDGAR ingestion entry point.
    limit=0 means all symbols.
    """
    conn = get_connection()
    try:
        cik_map = _load_cik_map()
        seen_hashes = _get_seen_hashes(conn)
        logger.info(f"[edgar] {len(seen_hashes)} articles already in DB")

        # Symbols to process:
        # 1) Those with no RSS/atom feed (primary target)
        # 2) Those already marked edgar_8k (for incremental updates)
        no_rss = _load_symbols_without_rss(conn, limit)
        edgar_existing = _load_symbols_with_edgar_source(conn)
        edgar_tickers = {t for _, t in edgar_existing}

        # Merge, no duplicates
        all_symbols = list(no_rss)
        for row in edgar_existing:
            if row not in all_symbols:
                all_symbols.append(row)

        if limit:
            all_symbols = all_symbols[:limit]

        # Filter to symbols that have a CIK
        resolvable = [(sid, t) for sid, t in all_symbols if t in cik_map]
        unresolvable = [(sid, t) for sid, t in all_symbols if t not in cik_map]

        logger.info(f"[edgar] {len(all_symbols)} symbols to process | {len(resolvable)} have CIK | {len(unresolvable)} unknown to SEC")

        total_inserted = 0
        total_skipped  = 0

        for idx, (symbol_id, ticker) in enumerate(resolvable, 1):
            cik = cik_map[ticker]
            filings = _fetch_symbol_filings(ticker, cik, seen_hashes)

            if not filings:
                total_skipped += 1
                if idx % 50 == 0:
                    logger.info(f"[edgar] Progress: {idx}/{len(resolvable)} | inserted: {total_inserted}")
                continue

            feed_id  = _ensure_edgar_feed_row(conn, symbol_id, ticker)
            inserted = _insert_articles(conn, feed_id, symbol_id, filings)
            total_inserted += inserted

            if idx % 50 == 0:
                logger.info(f"[edgar] Progress: {idx}/{len(resolvable)} | inserted: {total_inserted}")

        logger.info(f"[edgar] Done — {total_inserted} articles inserted | {len(unresolvable)} symbols had no SEC CIK")
        return {"inserted": total_inserted, "no_cik": len(unresolvable)}

    finally:
        conn.close()


def run_single(symbol: str, symbol_id: int) -> int:
    """Run EDGAR pipeline for one specific symbol. Returns count of inserted articles."""
    conn = get_connection()
    try:
        cik_map = _load_cik_map()
        if symbol not in cik_map:
            logger.warning(f"[edgar] {symbol} not found in SEC CIK map — skipping")
            return 0
        seen_hashes = _get_seen_hashes(conn)
        cik      = cik_map[symbol]
        filings  = _fetch_symbol_filings(symbol, cik, seen_hashes)
        if not filings:
            logger.info(f"[edgar] {symbol} — no new filings")
            return 0
        feed_id  = _ensure_edgar_feed_row(conn, symbol_id, symbol)
        inserted = _insert_articles(conn, feed_id, symbol_id, filings)
        logger.info(f"[edgar] {symbol} → {inserted} filings inserted")
        return inserted
    finally:
        conn.close()
