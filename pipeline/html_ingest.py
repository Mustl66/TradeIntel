"""
pipeline/html_ingest.py — Step 2b: HTML Press Release Ingestion
===============================================================
Handles feeds classified as feed_type='html' — IR pages, press release
pages, PRNewswire/Nasdaq/Yahoo pages that have no obvious RSS/Atom feed.

Strategy — four layers in order:

  Layer 1: RSS Autodiscovery
    Look for <link rel="alternate" type="...rss+xml"> in page <head>.
    If found, promote feed to RSS in DB and fetch it now.

  Layer 2: Platform-specific handlers
    Known IR platforms have predictable RSS/API endpoints:
    - Q4IR / Cision IR (investors.*.com + /press-releases/default.aspx)
      → probe /rss.xml on the same domain
    - Nasdaq press-release pages (nasdaq.com/market-activity/stocks/*/press-releases)
      → public JSON API: api.nasdaq.com/api/company/{SYMBOL}/pressreleases
    - PRNewswire company pages (prnewswire.com/news/*/rss-list.rss)
      → RSS endpoint pattern probe
    - mynewsdesk, businesswire, globe newswire listing pages
      → known RSS patterns

  Layer 3: JSON-LD
    Parse <script type="application/ld+json"> blocks.
    Works on modern IR pages that embed structured article data.

  Layer 4: Trafilatura
    Generic HTML article extractor — fallback for static pages.
    Output validated by LM Studio quality gate.

  Skip list: Yahoo Finance (full JS, no API), pure React shells with
    no RSS/API alternative.

Anti-bot: full browser header rotation + random 0.5-1.0s delay per request.
All articles land in news_articles (same table as RSS pipeline).
"""

import hashlib
import json
import logging
import os
import random
import re
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser

sys.path.insert(0, ".")
from db.connection import get_connection

# Suppress lxml encoding warnings (cosmetic noise from malformed byte sequences)
warnings.filterwarnings("ignore", category=UserWarning, module="lxml")
os.environ["PYTHONWARNINGS"] = "ignore"

@contextmanager
def _suppress_stderr():
    """Suppress lxml C-library stderr noise (encoding errors from malformed bytes).
    These bypass Python's warning system entirely and must be silenced at OS level."""
    import ctypes
    devnull = os.open(os.devnull, os.O_WRONLY)
    old_stderr = os.dup(2)
    os.dup2(devnull, 2)
    os.close(devnull)
    try:
        yield
    finally:
        os.dup2(old_stderr, 2)
        os.close(old_stderr)

def _make_soup(content, parser="lxml"):
    """BeautifulSoup wrapper that silences lxml C-level encoding noise."""
    with _suppress_stderr():
        return BeautifulSoup(content, parser)

try:
    import trafilatura
    _TRAFILATURA_AVAILABLE = True
except ImportError:
    _TRAFILATURA_AVAILABLE = False

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
REQUEST_TIMEOUT   = 15
SCRAPE_DELAY_MIN  = 0.5
SCRAPE_DELAY_MAX  = 1.0
HTML_WORKERS      = 6
MAX_FULL_TEXT_LEN = 100_000
LM_STUDIO_URL     = "http://127.0.0.1:1234/v1/chat/completions"
LM_STUDIO_MODEL   = "google/gemma-4-e4b"
LM_MAX_TOKENS     = 512
LM_TIMEOUT        = 30

# Platforms we skip entirely — JS-rendered with no public API
_SKIP_DOMAINS = {
    # Yahoo Finance handled by dedicated API handler below — not skipped anymore
}

# Yahoo Finance press release publishers — only ingest these, skip analyst noise
_YAHOO_PR_PUBLISHERS = {
    "Business Wire", "BusinessWire", "PR Newswire", "PRNewswire",
    "GlobeNewswire", "Globe Newswire", "Accesswire", "ACCESS Newswire",
    "Business Wire (BW)", "PR Newswire (PRN)",
}

# ── Browser header rotation ────────────────────────────────────────────────────
_HEADER_PROFILES = [
    {   # Chrome 124 / Windows
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",

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

        "Connection": "keep-alive",
    },
    {   # Edge 124 / Windows
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",

        "Connection": "keep-alive",
    },
]


def _headers() -> dict:
    return random.choice(_HEADER_PROFILES)


def _delay():
    time.sleep(random.uniform(SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX))


# ── Hashing ────────────────────────────────────────────────────────────────────

def _article_hash(url: str, title: str, published_at: datetime) -> str:
    raw = f"{url}|{title}|{published_at.isoformat()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ── Date parsing ──────────────────────────────────────────────────────────────

def _parse_date(raw) -> Optional[datetime]:
    if not raw:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    try:
        dt = dateutil_parser.parse(str(raw))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


# ── HTTP fetch ────────────────────────────────────────────────────────────────

def _fetch(url: str, extra_headers: Optional[dict] = None) -> Optional[requests.Response]:
    """Fetch URL with humanized headers + delay."""
    _delay()
    h = _headers()
    if extra_headers:
        h.update(extra_headers)
    try:
        resp = requests.get(url, headers=h, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        return resp
    except requests.exceptions.Timeout:
        logger.warning(f"[html_ingest] Timeout: {url}")
        return None
    except Exception as exc:
        logger.debug(f"[html_ingest] Fetch failed {url}: {exc}")
        return None


def _fetch_rss(url: str) -> list[dict]:
    """Fetch and parse an RSS/Atom URL. Returns list of raw entry dicts."""
    _delay()
    try:
        resp = requests.get(url, headers=_headers(), timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
        if not parsed.entries:
            return []
        return parsed.entries
    except Exception as exc:
        logger.debug(f"RSS fetch failed {url}: {exc}")
        return []


def _entries_to_articles(entries, symbol_id: int, feed_id: int,
                          source_name: str) -> list[dict]:
    """Convert feedparser entries to article dicts."""
    arts = []
    for entry in entries:
        pub_raw = (getattr(entry, "published", None) or
                   getattr(entry, "updated", None))
        pub_at  = _parse_date(pub_raw) or datetime.now(timezone.utc)
        title   = (getattr(entry, "title",   "") or "").strip()
        link    = (getattr(entry, "link",    "") or "").strip()
        summary = (getattr(entry, "summary", "") or "").strip()
        if not title or not link:
            continue
        arts.append({
            "symbol_id":    symbol_id,
            "feed_id":      feed_id,
            "article_hash": _article_hash(link, title, pub_at),
            "url":          link,
            "title":        title,
            "summary":      summary or None,
            "full_text":    None,
            "published_at": pub_at,
            "author":       None,
            "source_name":  source_name,
        })
    return arts


# ── Layer 1: RSS Autodiscovery ─────────────────────────────────────────────────

def _discover_rss_in_html(url: str, html: bytes) -> Optional[str]:
    """Find <link rel="alternate" type="...rss+xml"> in HTML. Returns absolute URL or None."""
    try:
        soup = _make_soup(html, "lxml")
        for link in soup.find_all("link", rel="alternate"):
            link_type = link.get("type", "").lower()
            if "rss" in link_type or "atom" in link_type:
                href = link.get("href", "").strip()
                if href:
                    return urljoin(url, href)
    except Exception:
        pass
    return None


# ── Layer 2: Platform-specific handlers ───────────────────────────────────────

def _handle_mynewsdesk(url: str, symbol: str, feed_id: int, symbol_id: int) -> list[dict]:
    """Scrape press releases from mynewsdesk.com company pages.
    Articles are server-rendered in <article> tags with /pressreleases/ links.
    Also checks for RSS autodiscovery as primary source.
    """
    try:
        resp = requests.get(url, headers=_headers(), timeout=20)
        if resp.status_code != 200:
            logger.warning(f"[{symbol}] mynewsdesk HTTP {resp.status_code}: {url}")
            return []

        soup = _make_soup(resp.text, "lxml")

        # Try RSS autodiscovery first
        rss_tag = soup.find("link", {"type": "application/rss+xml"}) or \
                  soup.find("link", {"type": "application/atom+xml"})
        if rss_tag and rss_tag.get("href"):
            rss_url = urljoin(url, rss_tag["href"])
            parsed = feedparser.parse(rss_url)
            if parsed.entries:
                logger.info(f"[{symbol}] mynewsdesk RSS autodiscovered: {rss_url}")
                articles = []
                for e in parsed.entries:
                    title = e.get("title", "").strip()
                    link  = e.get("link", "").strip()
                    if not title or not link:
                        continue
                    pub = None
                    if hasattr(e, "published_parsed") and e.published_parsed:
                        pub = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
                    raw  = title + link
                    ahash = hashlib.sha256(raw.encode()).hexdigest()
                    articles.append({
                        "symbol_id":    symbol_id,
                        "feed_id":      feed_id,
                        "title":        title,
                        "url":          link,
                        "published_at": pub,
                        "summary":      e.get("summary", "")[:500],
                        "full_text":    e.get("summary", ""),
                        "author":       e.get("author", ""),
                        "source_name":  "mynewsdesk",
                        "article_hash": ahash,
                    })
                return articles

        # Fallback: scrape article links from the page
        articles = []
        seen = set()
        for a in soup.select('a[href*="/pressreleases/"]'):
            href = a.get("href", "")
            if not href or href in seen:
                continue
            # Skip pagination/category links — only real articles have numeric IDs
            if not re.search(r'/pressreleases/[a-z0-9-]+-\d+$', href):
                continue
            seen.add(href)
            full_url = urljoin("https://www.mynewsdesk.com", href)
            title = a.get_text(strip=True)
            if not title:
                # title may be in a sibling span
                parent = a.find_parent()
                if parent:
                    title = parent.get_text(strip=True)[:200]
            if not title:
                continue
            # Date: look for sibling time tag
            parent = a.find_parent("article") or a.find_parent("li") or a.find_parent("div")
            pub = None
            if parent:
                time_tag = parent.find("time")
                if time_tag:
                    dt_str = time_tag.get("datetime") or time_tag.get_text(strip=True)
                    try:
                        pub = dateutil_parser.parse(dt_str).replace(tzinfo=timezone.utc)
                    except Exception:
                        pass
            ahash = hashlib.sha256((title + full_url).encode()).hexdigest()
            articles.append({
                "symbol_id":    symbol_id,
                "feed_id":      feed_id,
                "title":        title,
                "url":          full_url,
                "published_at": pub,
                "summary":      "",
                "full_text":    "",
                "author":       "",
                "source_name":  "mynewsdesk",
                "article_hash": ahash,
            })

        logger.info(f"[{symbol}] mynewsdesk scraped {len(articles)} articles from {url}")
        return articles

    except Exception as exc:
        logger.warning(f"[{symbol}] mynewsdesk error: {exc}")
        return []


def _handle_nasdaq_press_release_api(ticker: str, symbol: str, feed_id: int, symbol_id: int) -> list[dict]:
    """
    Nasdaq press-release API — works for any Nasdaq-listed ticker.
    Used as fallback when Yahoo Finance returns sparse results.
    GET https://api.nasdaq.com/api/news/topic/press_release
        ?q=symbol:{ticker}|assetclass:stocks&limit=100&offset=0
    """
    import trafilatura, hashlib as _hashlib

    api_url = (
        f"https://api.nasdaq.com/api/news/topic/press_release"
        f"?q=symbol:{ticker.lower()}|assetclass:stocks&limit=100&offset=0"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://www.nasdaq.com/",
    }
    try:
        _delay()
        resp = requests.get(api_url, headers=headers, timeout=20)
        if resp.status_code != 200:
            logger.warning(f"[{symbol}] Nasdaq PR API HTTP {resp.status_code}")
            return []
        data = resp.json()
    except Exception as e:
        logger.warning(f"[{symbol}] Nasdaq PR API error: {e}")
        return []

    rows = (data.get("data") or {}).get("rows") or []
    if not rows:
        return []

    articles = []
    for row in rows:
        title = (row.get("title") or "").strip()
        slug  = (row.get("url") or "").strip()
        if not title or not slug:
            continue

        link = slug if slug.startswith("http") else f"https://www.nasdaq.com{slug}"

        # Parse date string like "May 14, 2026"
        pub_dt = None
        date_str = row.get("created") or row.get("ago") or ""
        if date_str:
            try:
                from datetime import datetime, timezone as _tz
                pub_dt = datetime.strptime(date_str, "%b %d, %Y").replace(tzinfo=_tz.utc)
            except Exception:
                pass

        full_text = ""
        try:
            _delay()
            r_art = requests.get(link, headers=_headers(), timeout=12, allow_redirects=True)
            if r_art.status_code == 200:
                extracted = trafilatura.extract(r_art.text, include_comments=False, include_tables=False)
                full_text = (extracted or "")[:MAX_FULL_TEXT_LEN]
        except Exception as e:
            logger.debug(f"[{symbol}] Nasdaq PR trafilatura error on {link}: {e}")

        art_hash = _hashlib.sha256(f"{title}|{link}".encode()).hexdigest()
        articles.append({
            "symbol_id":    symbol_id,
            "feed_id":      feed_id,
            "title":        title,
            "url":          link,
            "published_at": pub_dt,
            "summary":      "",
            "author":       "",
            "full_text":    full_text,
            "source_name":  "nasdaq_pr_api",
            "article_hash": art_hash,
        })

    return articles


def _handle_yahoo_finance(url: str, symbol: str, feed_id: int, symbol_id: int) -> list[dict]:
    """
    Yahoo Finance press-release pages:
    https://finance.yahoo.com/quote/{TICKER}/press-releases/

    Strategy:
      1. Hit Yahoo Finance search API — free, no JS needed, returns up to 40 items
      2. Filter to only press release publishers (Business Wire, PRNewswire, GNW, etc.)
      3. If Yahoo returns <= 5 items (capped / page 404), fall back to Nasdaq press-release API
      4. Fetch full article text via trafilatura
      5. Return clean article dicts

    API: https://query2.finance.yahoo.com/v1/finance/search?q={ticker}&newsCount=40
    Fallback: https://api.nasdaq.com/api/news/topic/press_release?q=symbol:{ticker}|assetclass:stocks
    """
    import trafilatura

    ticker = symbol.upper()
    api_url = (
        f"https://query2.finance.yahoo.com/v1/finance/search"
        f"?q={ticker}&lang=en-US&region=US&quotesCount=0&newsCount=40"
        f"&enableFuzzyQuery=false"
    )

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://finance.yahoo.com/",
    }

    pr_items = []
    source = "yahoo"
    try:
        _delay()
        resp = requests.get(api_url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        news_items = data.get("news", [])
        pr_items = [n for n in news_items if n.get("publisher", "") in _YAHOO_PR_PUBLISHERS]
        logger.debug(f"[{symbol}] Yahoo search API: {len(news_items)} total, {len(pr_items)} press releases")
    except Exception as e:
        logger.warning(f"[{symbol}] Yahoo API error: {e}")

    # Fall back to Nasdaq press-release API if Yahoo returned too few results
    if len(pr_items) <= 5:
        logger.info(f"[{symbol}] Yahoo sparse ({len(pr_items)}), trying Nasdaq press-release API fallback")
        nasdaq_articles = _handle_nasdaq_press_release_api(ticker, symbol, feed_id, symbol_id)
        if nasdaq_articles:
            logger.info(f"[{symbol}] Nasdaq fallback: {len(nasdaq_articles)} articles")
            return nasdaq_articles
        # If Nasdaq also empty, proceed with whatever Yahoo gave us

    if not pr_items:
        logger.debug(f"[{symbol}] Yahoo: 0 press releases, no fallback results either")
        return []

    logger.info(f"[{symbol}] Yahoo: {len(pr_items)} press releases from API, fetching full text...")

    articles = []
    for item in pr_items:
        title     = item.get("title", "").strip()
        link      = item.get("link", "").strip()
        pub_ts    = item.get("providerPublishTime")
        publisher = item.get("publisher", "")

        if not link or not title:
            continue

        pub_dt = datetime.fromtimestamp(pub_ts, tz=timezone.utc) if pub_ts else None

        # Fetch full article text via trafilatura — bypasses consent wall
        full_text = ""
        try:
            _delay()
            resp_tf = requests.get(link, headers=_headers(), timeout=10, allow_redirects=True)
            if resp_tf.status_code == 200:
                extracted = trafilatura.extract(resp_tf.text, include_comments=False,
                                                include_tables=False)
                full_text = (extracted or "")[:MAX_FULL_TEXT_LEN]
        except Exception as e:
            logger.debug(f"[{symbol}] Yahoo trafilatura error on {link}: {e}")

        content_str = f"{title}|{link}|{pub_ts}"
        art_hash    = hashlib.sha256(content_str.encode()).hexdigest()

        articles.append({
            "symbol_id":    symbol_id,
            "feed_id":      feed_id,
            "title":        title,
            "url":          link,
            "published_at": pub_dt,
            "summary":      "",
            "author":       "",
            "full_text":    full_text,
            "source_name":  f"yahoo_finance/{publisher.lower().replace(' ', '_')}",
            "article_hash": art_hash,
        })

    logger.info(f"[{symbol}] Yahoo: built {len(articles)} articles with full text")
    return articles


def _handle_nasdaq_page(url: str, symbol: str, feed_id: int, symbol_id: int) -> list[dict]:
    """
    Nasdaq press-release pages:
    https://www.nasdaq.com/market-activity/stocks/{ticker}/press-releases

    Real API discovered from JS bundle:
    GET https://api.nasdaq.com/api/news/topic/press_release
        ?q=symbol:{ticker}|assetclass:stocks&limit=40&offset=0

    Returns JSON rows with title, url (/press-release/slug), created date.
    Full article body scraped from https://www.nasdaq.com{url}
    using selector: div.body__content
    """
    try:
        from curl_cffi import requests as cffi_req
        _USE_CFFI = True
    except ImportError:
        _USE_CFFI = False

    # Extract ticker from URL — two formats:
    # /market-activity/stocks/{ticker}/press-releases
    # /api/news/topic/press_release?q=symbol:{ticker}|...
    m = re.search(r"/stocks/([^/]+)/press-releases", url.lower())
    if not m:
        m = re.search(r"symbol:([^|&]+)", url.lower())
    ticker = (m.group(1) if m else symbol).lower()

    api_url = (
        f"https://api.nasdaq.com/api/news/topic/press_release"
        f"?q=symbol:{ticker}|assetclass:stocks&limit=40&offset=0"
    )
    api_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nasdaq.com/",
        "Origin": "https://www.nasdaq.com",
    }

    try:
        _delay()

        _delay()
        if _USE_CFFI:
            resp = cffi_req.get(api_url, headers=api_headers, timeout=REQUEST_TIMEOUT, impersonate="chrome124")
        else:
            resp = requests.get(api_url, headers=api_headers, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logger.warning(f"[{symbol}] Nasdaq API HTTP {resp.status_code}: {api_url}")
            return []
        data = resp.json()
    except Exception as exc:
        logger.warning(f"[{symbol}] Nasdaq API error: {exc}")
        return []

    rows = (data.get("data") or {}).get("rows") or []
    if not rows:
        logger.debug(f"[{symbol}] Nasdaq: 0 rows from API")
        return []

    articles = []
    for row in rows:
        title = (row.get("title") or "").strip()
        slug  = (row.get("url") or "").strip()
        date_str = row.get("date", "")
        if not title or not slug:
            continue
        art_url = f"https://www.nasdaq.com{slug}" if slug.startswith("/") else slug
        pub_dt = None
        try:
            pub_dt = dateutil_parser.parse(date_str).replace(tzinfo=timezone.utc)
        except Exception:
            pub_dt = datetime.now(timezone.utc)

        art_hash = hashlib.sha256(f"{art_url}|{title}".encode()).hexdigest()
        articles.append({
            "symbol_id":    symbol_id,
            "feed_id":      feed_id,
            "title":        title,
            "url":          art_url,
            "published_at": pub_dt,
            "summary":      "",
            "full_text":    "",
            "author":       "",
            "source_name":  "nasdaq",
            "article_hash": art_hash,
        })

    logger.info(f"[{symbol}] Nasdaq API -> {len(articles)} articles from {url}")
    return articles


def _handle_q4ir_api(url: str, symbol: str, feed_id: int, symbol_id: int) -> list:
    """Q4 Inc IR platform - default.aspx pages.
    Hidden API: GET {base}/feed/PressRelease.svc/GetPressReleaseList
                    ?bodyType=2&pageSize=-1&year=-1
    Returns full article list + bodies inline.
    """
    try:
        from curl_cffi import requests as cffi_req
        _USE_CFFI = True
    except ImportError:
        _USE_CFFI = False

    parsed = urlparse(url)
    base   = f"{parsed.scheme}://{parsed.netloc}"
    api_url = (
        f"{base}/feed/PressRelease.svc/GetPressReleaseList"
        f"?bodyType=2&pageSize=-1&year=-1&includeTags=true&tagList="
    )
    api_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": url,
        "X-Requested-With": "XMLHttpRequest",
    }

    try:
        _delay()
        if _USE_CFFI:
            resp = cffi_req.get(api_url, headers=api_headers, timeout=REQUEST_TIMEOUT, impersonate="chrome124")
        else:
            resp = requests.get(api_url, headers=api_headers, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logger.warning(f"[{symbol}] Q4 API HTTP {resp.status_code}: {api_url}")
            return []
        data = resp.json()
    except Exception as exc:
        logger.warning(f"[{symbol}] Q4 API error: {exc}")
        return []

    items = data if isinstance(data, list) else (data.get("items") or data.get("GetPressReleaseListResult") or [])
    if not items:
        logger.debug(f"[{symbol}] Q4 API: 0 items from {api_url}")
        return []

    articles = []
    for item in items:
        title     = (item.get("Headline") or item.get("title") or "").strip()
        slug      = (item.get("LinkToDetailPage") or item.get("url") or "").strip()
        body_html = (item.get("Body") or item.get("body") or "").strip()
        date_str  = item.get("PressReleaseDate") or item.get("date") or ""

        if not title:
            continue

        art_url = (f"{base}{slug}" if slug and slug.startswith("/") else slug) or f"{base}/press-releases"

        pub_dt = None
        try:
            pub_dt = dateutil_parser.parse(date_str).replace(tzinfo=timezone.utc)
        except Exception:
            pub_dt = datetime.now(timezone.utc)

        full_text = ""
        if body_html:
            try:
                soup = _make_soup(body_html, "lxml")
                full_text = soup.get_text(separator=" ", strip=True)[:MAX_FULL_TEXT_LEN]
            except Exception:
                full_text = body_html[:MAX_FULL_TEXT_LEN]

        art_hash = hashlib.sha256(f"{art_url}|{title}".encode()).hexdigest()
        articles.append({
            "symbol_id":    symbol_id,
            "feed_id":      feed_id,
            "title":        title,
            "url":          art_url,
            "published_at": pub_dt,
            "summary":      "",
            "full_text":    full_text,
            "author":       "",
            "source_name":  "q4ir",
            "article_hash": art_hash,
        })

    logger.info(f"[{symbol}] Q4 API -> {len(articles)} articles from {base}")
    return articles


def _detect_platform(url: str) -> str:
    u = url.lower()
    if "default.aspx" in u:
        return "q4ir"
    if "nasdaq.com" in u:
        return "nasdaq"
    if "finance.yahoo.com" in u:
        return "yahoo"
    if "mynewsdesk.com" in u:
        return "mynewsdesk"
    return "generic"


def _save_articles(articles: list, conn) -> int:
    if not articles:
        return 0
    cur = conn.cursor()
    inserted = 0
    for art in articles:
        try:
            cur.execute(
                """
                INSERT INTO news_articles
                    (symbol_id, feed_id, article_hash, url, title,
                     summary, full_text, published_at, author, source_name)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (
                    art["symbol_id"], art["feed_id"], art["article_hash"],
                    art["url"], art["title"], art.get("summary"),
                    art.get("full_text"), art.get("published_at"),
                    art.get("author"), art.get("source_name"),
                ),
            )
            if cur.rowcount:
                inserted += 1
        except Exception as exc:
            logger.debug(f"[html_ingest] insert error: {exc}")
            try:
                conn.rollback()
            except Exception:
                pass
    conn.commit()
    cur.close()
    return inserted


def _process_html_feed(feed: dict) -> tuple:
    symbol    = feed["symbol"]
    url       = feed["feed_url"]
    feed_id   = feed["feed_id"]
    symbol_id = feed["symbol_id"]
    platform  = _detect_platform(url)

    try:
        if platform == "q4ir":
            articles = _handle_q4ir_api(url, symbol, feed_id, symbol_id)
        elif platform == "nasdaq":
            articles = _handle_nasdaq_page(url, symbol, feed_id, symbol_id)
        elif platform == "yahoo":
            articles = _handle_yahoo_finance(url, symbol, feed_id, symbol_id)
        elif platform == "mynewsdesk":
            articles = _handle_mynewsdesk(url, symbol, feed_id, symbol_id)
        else:
            resp = _fetch(url)
            if resp is None:
                return symbol, 0
            rss_url = _discover_rss_in_html(url, resp.content)
            if rss_url:
                entries  = _fetch_rss(rss_url)
                articles = _entries_to_articles(entries, symbol_id, feed_id, "rss_autodiscovered")
            else:
                return symbol, 0

        if not articles:
            return symbol, 0

        conn = get_connection()
        n = _save_articles(articles, conn)
        conn.close()
        return symbol, n

    except BaseException as exc:
        logger.warning(f"[{symbol}] html_ingest error ({type(exc).__name__}): {exc}")
        return symbol, 0


def _get_html_feeds(exchange: str, limit: int, symbol_limit) -> list:
    conn = get_connection()
    cur  = conn.cursor()

    if exchange:
        cur.execute(
            """
            SELECT f.id, f.feed_url, s.id, s.symbol
            FROM rss_feeds f
            JOIN symbols s ON s.id = f.symbol_id
            WHERE f.feed_type = 'html'
              AND s.exchange = %s
            ORDER BY s.symbol
            """,
            (exchange,),
        )
    else:
        cur.execute(
            """
            SELECT f.id, f.feed_url, s.id, s.symbol
            FROM rss_feeds f
            JOIN symbols s ON s.id = f.symbol_id
            WHERE f.feed_type = 'html'
            ORDER BY s.symbol
            """
        )

    rows = cur.fetchall()
    conn.close()

    feeds = [{"feed_id": r[0], "feed_url": r[1], "symbol_id": r[2], "symbol": r[3]} for r in rows]

    if symbol_limit and isinstance(symbol_limit, int) and symbol_limit > 0:
        seen = []
        for f in feeds:
            if f["symbol"] not in seen:
                seen.append(f["symbol"])
            if len(seen) >= symbol_limit:
                break
        symbol_set = set(seen)
        feeds = [f for f in feeds if f["symbol"] in symbol_set]
        logger.info(f"[html_ingest] SYMBOL_LIMIT={symbol_limit}: {len(feeds)} feeds for {len(symbol_set)} symbols")

    if limit and limit > 0:
        feeds = feeds[:limit]

    return feeds


def run(exchange: str = "NASDAQ", limit: int = 0, scrape_full_text: bool = True) -> dict:
    from pipeline_config import SYMBOL_LIMIT

    feeds = _get_html_feeds(exchange, limit, SYMBOL_LIMIT)
    logger.info(f"[html_ingest] Starting -- exchange={exchange}, limit={limit}")
    logger.info(f"[html_ingest] {len(feeds)} HTML feeds to process")

    total_new = 0
    with ThreadPoolExecutor(max_workers=HTML_WORKERS) as pool:
        futures = {pool.submit(_process_html_feed, f): f for f in feeds}
        for fut in as_completed(futures):
            try:
                symbol, n = fut.result(timeout=REQUEST_TIMEOUT * 2)
                total_new += n
            except Exception as exc:
                feed = futures[fut]
                logger.warning(f"[{feed['symbol']}] future error: {exc}")

    logger.info(f"[html_ingest] Done -- feeds={len(feeds)}, new={total_new}")
    return {"feeds": len(feeds), "articles_new": total_new, "articles_skipped": 0}
