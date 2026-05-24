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


def _handle_yahoo_finance(url: str, symbol: str, feed_id: int, symbol_id: int) -> list[dict]:
    """
    Yahoo Finance press-release pages:
    https://finance.yahoo.com/quote/{TICKER}/press-releases/

    Strategy:
      1. Hit Yahoo Finance search API — free, no JS needed, returns 40 news items
      2. Filter to only press release publishers (Business Wire, PRNewswire, GNW, etc.)
      3. Fetch full article text via trafilatura (bypasses Yahoo consent wall)
      4. Return clean article dicts

    API: https://query2.finance.yahoo.com/v1/finance/search?q={ticker}&newsCount=40
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

    try:
        _delay()
        resp = requests.get(api_url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"[{symbol}] Yahoo API error: {e}")
        return []

    news_items = data.get("news", [])
    # Filter to press release publishers only — skip analyst commentary
    pr_items = [n for n in news_items if n.get("publisher", "") in _YAHOO_PR_PUBLISHERS]

    if not pr_items:
        logger.debug(f"[{symbol}] Yahoo: 0 press releases found (total news={len(news_items)})")
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
            downloaded = trafilatura.fetch_url(link)
            if downloaded:
                extracted = trafilatura.extract(downloaded, include_comments=False,
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
        if _USE_CFFI:
            r = cffi_req.get(api_url, headers=api_headers, impersonate="chrome124",
                             timeout=REQUEST_TIMEOUT)
        else:
            r = requests.get(api_url, headers=api_headers, timeout=REQUEST_TIMEOUT)

        if r.status_code != 200:
            logger.warning(f"[{symbol}] Nasdaq API returned {r.status_code}")
            return []

        data = r.json()
        rows = data.get("data", {}).get("rows", [])
        if not rows:
            logger.debug(f"[{symbol}] Nasdaq API — no rows for {ticker}")
            return []

        logger.info(f"[{symbol}] Nasdaq API — {len(rows)} press releases found")
        arts = []

        for row in rows:
            title = (row.get("title") or "").strip()
            rel_url = (row.get("url") or "").strip()
            created_str = (row.get("created") or row.get("ago") or "").strip()
            if not title or not rel_url:
                continue

            article_url = f"https://www.nasdaq.com{rel_url}"

            # Parse date
            pub_dt = None
            if created_str:
                try:
                    pub_dt = dateutil_parser.parse(created_str, fuzzy=True)
                    if pub_dt.tzinfo is None:
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    pub_dt = None

            # Fetch full article body
            full_text = ""
            try:
                _delay()
                art_headers = dict(api_headers)
                art_headers["Accept"] = "text/html,*/*"
                if _USE_CFFI:
                    ar = cffi_req.get(article_url, headers=art_headers,
                                      impersonate="chrome124", timeout=REQUEST_TIMEOUT)
                else:
                    ar = requests.get(article_url, headers=art_headers,
                                      timeout=REQUEST_TIMEOUT)
                if ar.status_code == 200:
                    soup = _make_soup(ar.text, "lxml")
                    body = soup.select_one("div.body__content")
                    if body:
                        full_text = body.get_text(separator="\n", strip=True)[:MAX_FULL_TEXT_LEN]
            except Exception as e:
                logger.debug(f"[{symbol}] Nasdaq article fetch failed: {e}")

            article_hash = hashlib.sha256(
                f"{symbol_id}:{article_url}".encode()
            ).hexdigest()

            arts.append({
                "symbol_id":    symbol_id,
                "feed_id":      feed_id,
                "title":        title,
                "url":          article_url,
                "published_at": pub_dt,
                "full_text":    full_text,
                "source_name":  "nasdaq_api",
                "summary":      "",
                "author":       "",
                "article_hash": article_hash,
            })

        return arts

    except Exception as e:
        logger.error(f"[{symbol}] Nasdaq API error: {e}", exc_info=True)
        return []


def _handle_q4ir_api(url: str, symbol: str,
                     feed_id: int, symbol_id: int) -> list[dict]:
    """
    Q4 Inc. / Cision IR platform — GetPressReleaseList JSON API.

    Every site on the Q4 platform (investors.*.com/press-releases/default.aspx,
    ir.*.com/news/default.aspx, etc.) exposes a single endpoint:
        {base}/feed/PressRelease.svc/GetPressReleaseList

    bodyType=2 returns full HTML body inline — no detail-page fetches needed.
    year=-1 returns ALL years in one call.
    """
    from curl_cffi import requests as cffi_requests

    parsed = urlparse(url)
    base   = f"{parsed.scheme}://{parsed.netloc}"
    api_url = f"{base}/feed/PressRelease.svc/GetPressReleaseList"
    params = {
        "LanguageId":            "1",
        "bodyType":              "2",       # full HTML body inline
        "pressReleaseDateFilter":"3",
        "categoryId":            "00000000-0000-0000-0000-000000000000",
        "pageSize":              "-1",      # all articles
        "pageNumber":            "0",
        "tagList":               "",
        "includeTags":           "true",
        "year":                  "-1",      # all years
        "excludeSelection":      "1",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept":     "application/json, text/javascript, */*; q=0.01",
        "Referer":    url,
    }

    _delay()
    try:
        r = cffi_requests.get(api_url, params=params, headers=headers,
                               impersonate="chrome124", timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logger.warning(f"[{symbol}] Q4 API HTTP {r.status_code}: {api_url}")
            return []
        data  = r.json()
        items = data.get("GetPressReleaseListResult", [])
        if not items:
            return []
    except Exception as e:
        logger.warning(f"[{symbol}] Q4 API error: {e}")
        return []

    arts = []
    for item in items:
        headline = (item.get("Headline") or item.get("Title") or "").strip()
        if not headline:
            continue

        link     = item.get("LinkToDetailPage", "") or ""
        full_url = (base + link) if link.startswith("/") else (link or url)

        # Parse date: "MM/DD/YYYY HH:MM:SS"
        raw_date = item.get("PressReleaseDate", "")
        published = None
        if raw_date:
            try:
                published = datetime.strptime(raw_date, "%m/%d/%Y %H:%M:%S").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                try:
                    published = dateutil_parser.parse(raw_date)
                except Exception:
                    pass

        # Full text from inline HTML body
        body_html = item.get("Body") or ""
        full_text = ""
        if body_html:
            full_text = _make_soup(body_html, "lxml").get_text(" ", strip=True)
        summary   = (item.get("ShortDescription") or item.get("ShortBody") or "").strip()

        art_hash = hashlib.sha256(
            f"{symbol_id}:{full_url}:{headline}".encode()
        ).hexdigest()

        arts.append({
            "symbol_id":    symbol_id,
            "feed_id":      feed_id,
            "title":        headline[:1000],
            "url":          full_url,
            "published_at": published,
            "full_text":    full_text[:MAX_FULL_TEXT_LEN],
            "summary":      summary[:500],
            "author":       "",
            "source_name":  "q4ir",
            "article_hash": art_hash,
        })

    logger.info(f"[{symbol}] Q4 API → {len(arts)} articles from {base}")
    return arts


def _handle_prnewswire(url: str, symbol: str,
                       feed_id: int, symbol_id: int) -> list[dict]:
    """
    PRNewswire company pages: fetch the listing page and extract article links,
    then scrape each article page.
    Pattern: https://www.prnewswire.com/news/{company-slug}/
    """
    resp = _fetch(url)
    if not resp:
        return []

    soup = _make_soup(resp.content, "lxml")
    arts = []
    seen = set()

    # PRNewswire uses class="newsreleaseconsolidatelink" on article anchors
    # href pattern: /news-releases/{slug}-NNNNNN.html  (no date in path)
    for a in soup.find_all("a", class_=re.compile(r"newsreleaseconsolidatelink", re.I)):
        href = a.get("href", "").strip()
        if not href or href in seen:
            continue
        seen.add(href)
        full_url = urljoin("https://www.prnewswire.com", href)

        # Title + date are both in the link text: "Nov 28, 2025, 09:01 ETAbove Food..."
        raw_text = a.get_text(strip=True)
        # Split on ET — date prefix is everything before "ET", title is after
        if "ET" in raw_text:
            parts = raw_text.split("ET", 1)
            date_part  = parts[0].strip()
            title_part = parts[1].strip() if len(parts) > 1 else raw_text
        else:
            date_part  = ""
            title_part = raw_text

        title = title_part if len(title_part) >= 10 else raw_text
        if len(title) < 10:
            continue

        pub_at = _parse_date(date_part) if date_part else datetime.now(timezone.utc)
        if pub_at is None:
            pub_at = datetime.now(timezone.utc)

        arts.append({
            "symbol_id":    symbol_id,
            "feed_id":      feed_id,
            "article_hash": _article_hash(full_url, title, pub_at),
            "url":          full_url,
            "title":        title,
            "summary":      None,
            "full_text":    None,
            "published_at": pub_at,
            "author":       None,
            "source_name":  "prnewswire",
        })

    # Alternate: PRNewswire has RSS for company search
    # Try: https://www.prnewswire.com/rss/news-releases-list.rss?name={slug}
    if not arts:
        slug_m = re.search(r"/news/([^/]+)/?$", url.lower())
        if slug_m:
            slug    = slug_m.group(1)
            rss_url = f"https://www.prnewswire.com/rss/news-releases-list.rss?name={slug}"
            _delay()
            try:
                rss_resp = requests.get(rss_url, headers=_headers(), timeout=REQUEST_TIMEOUT)
                rss_resp.raise_for_status()
                parsed = feedparser.parse(rss_resp.content)
                if parsed.entries:
                    arts = _entries_to_articles(parsed.entries, symbol_id, feed_id, "prnewswire_rss")
            except Exception:
                pass

    return arts


def _handle_businesswire(url: str, symbol: str,
                          feed_id: int, symbol_id: int) -> list[dict]:
    """
    BusinessWire company pages — they have RSS at:
    https://feed.businesswire.com/rss/home/?rss=G1&company={ticker}
    """
    ticker  = symbol.upper()
    rss_url = f"https://feed.businesswire.com/rss/home/?rss=G1&company={ticker}"
    entries = _fetch_rss(rss_url)
    if entries:
        return _entries_to_articles(entries, symbol_id, feed_id, "businesswire_rss")
    return []


def _handle_globenewswire_listing(url: str, symbol: str,
                                  feed_id: int, symbol_id: int) -> list[dict]:
    """
    GlobeNewswire search/organization pages.
    Pattern: https://www.globenewswire.com/en/search/organization/{Company+Name}
             https://www.globenewswire.com/en/search/keyword/{TICKER}
    Strategy: scrape article links from the search results page directly.
    Article links follow pattern: /news-release/YYYY/MM/DD/NNNNNN/...
    """
    # Single article page — skip, handled by full-text scraper elsewhere
    if re.search(r"globenewswire\.com/(?:en/)?news-release/\d+", url):
        return []

    resp = _fetch(url)
    if not resp:
        return []

    soup = _make_soup(resp.content, "lxml")
    arts = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not re.search(r"/news-release/\d{4}/", href):
            continue
        full_url = ("https://www.globenewswire.com" + href
                    if href.startswith("/") else href)
        if full_url in seen:
            continue
        seen.add(full_url)

        title = a.get_text(strip=True)
        if len(title) < 10:
            continue

        # Extract date from URL path: /news-release/YYYY/MM/DD/
        m = re.search(r"/news-release/(\d{4})/(\d{2})/(\d{2})/", href)
        if m:
            pub_at = datetime(int(m.group(1)), int(m.group(2)),
                              int(m.group(3)), tzinfo=timezone.utc)
        else:
            pub_at = datetime.now(timezone.utc)

        arts.append({
            "symbol_id":    symbol_id,
            "feed_id":      feed_id,
            "article_hash": _article_hash(full_url, title, pub_at),
            "url":          full_url,
            "title":        title,
            "summary":      None,
            "full_text":    None,
            "published_at": pub_at,
            "author":       None,
            "source_name":  "globenewswire_scrape",
        })

    return arts


# ── Layer 3: JSON-LD extraction ────────────────────────────────────────────────

_JSONLD_ARTICLE_TYPES = {
    "newsarticle", "article", "pressrelease", "blogposting",
    "financialarticle", "reportage"
}


def _extract_jsonld(html: bytes, base_url: str) -> list[dict]:
    """Parse <script type="application/ld+json"> for article objects."""
    articles = []
    try:
        soup = _make_soup(html, "lxml")
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
            except Exception:
                continue

            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict):
                    continue
                if "@graph" in item:
                    items = items + item["@graph"]
                    continue

                t = item.get("@type", "")
                if isinstance(t, list):
                    t = " ".join(t)
                if not any(at in t.lower() for at in _JSONLD_ARTICLE_TYPES):
                    continue

                url = item.get("url") or item.get("mainEntityOfPage", {})
                if isinstance(url, dict):
                    url = url.get("@id", "")
                url = str(url).strip()
                if not url.startswith("http"):
                    url = urljoin(base_url, url)

                title = (item.get("headline") or item.get("name") or "").strip()
                if not title or not url:
                    continue

                pub_at = _parse_date(
                    item.get("datePublished") or item.get("dateCreated")
                ) or datetime.now(timezone.utc)

                body = str(item.get("articleBody") or item.get("text") or "").strip()
                author_raw = item.get("author")
                author = None
                if isinstance(author_raw, dict):
                    author = author_raw.get("name")
                elif isinstance(author_raw, str):
                    author = author_raw

                articles.append({
                    "url": url, "title": title, "published_at": pub_at,
                    "full_text": body or None, "author": author,
                })
    except Exception as exc:
        logger.debug(f"JSON-LD parse error: {exc}")
    return articles


# ── Layer 4: Trafilatura + LM Studio ──────────────────────────────────────────

def _extract_trafilatura(html: bytes, url: str) -> Optional[dict]:
    if not _TRAFILATURA_AVAILABLE:
        return None
    try:
        result = trafilatura.extract(
            html, url=url, output_format="json",
            include_metadata=True, include_links=False,
            no_fallback=False, favor_precision=True,
        )
        if not result:
            return None
        data    = json.loads(result)
        title   = data.get("title", "").strip()
        text    = data.get("text", "").strip()
        pub_at  = _parse_date(data.get("date")) or datetime.now(timezone.utc)
        if not title or len(text) < 200:
            return None
        return {
            "url": url, "title": title, "published_at": pub_at,
            "full_text": text[:MAX_FULL_TEXT_LEN],
            "author": data.get("author"),
        }
    except Exception as exc:
        logger.debug(f"Trafilatura failed on {url}: {exc}")
        return None


_LM_SYSTEM = (
    "You are a financial news content validator. "
    "Given a page title and extracted text, determine if this is a genuine financial "
    "news article (earnings, M&A, product launch, regulatory filing, executive change, etc). "
    'Respond ONLY with JSON: {"valid": true/false, "reason": "one sentence", '
    '"cleaned_title": "...", "cleaned_body": "first 3 sentences"}'
)


def _lm_quality_gate(title: str, text: str) -> dict:
    """LM Studio validation. Fails open (returns valid=True) if LM is unreachable."""
    payload = {
        "model": LM_STUDIO_MODEL,
        "messages": [
            {"role": "system", "content": _LM_SYSTEM},
            {"role": "user",   "content": f"Title: {title}\n\nText:\n{text[:1000]}"},
        ],
        "max_tokens": LM_MAX_TOKENS,
        "temperature": 0.1,
    }
    try:
        resp = requests.post(LM_STUDIO_URL, json=payload, timeout=LM_TIMEOUT)
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"].strip()
        m = re.search(r"\{.*\}", content, re.DOTALL)
        if m:
            return json.loads(m.group())
    except Exception as exc:
        logger.debug(f"LM Studio gate failed (fail-open): {exc}")
    return {"valid": True, "cleaned_title": title, "cleaned_body": text}


# ── Platform detection ────────────────────────────────────────────────────────

def _detect_platform(url: str) -> Optional[str]:
    low = url.lower()
    if _SKIP_DOMAINS and any(d in low for d in _SKIP_DOMAINS):
        return "skip"
    if "mynewsdesk.com" in low:
        return "mynewsdesk"
    if "finance.yahoo.com" in low and ("press-releases" in low or "quote" in low):
        return "yahoo"
    if ("nasdaq.com/market-activity/stocks/" in low and "press-releases" in low) \
            or "nasdaq.com/api/news/topic/press_release" in low:
        return "nasdaq"
    if "prnewswire.com/news/" in low:
        return "prnewswire"
    if "businesswire.com" in low:
        return "businesswire"
    if "globenewswire.com" in low:
        return "globenewswire"
    # Q4IR / Cision: investors.*.com ending in default.aspx
    if "default.aspx" in low or (
        re.search(r"investors?\.", low) and "press-release" in low
    ):
        return "q4ir"
    return None


# ── Per-feed processor ────────────────────────────────────────────────────────

def _process_html_feed(feed_row: dict) -> dict:
    feed_id   = feed_row["id"]
    symbol_id = feed_row["symbol_id"]
    symbol    = feed_row["symbol"]
    url       = feed_row["feed_url"]

    result = {
        "feed_id":   feed_id,
        "symbol_id": symbol_id,
        "symbol":    symbol,
        "url":       url,
        "rss_found": None,
        "articles":  [],
        "method":    None,
    }

    # ── Skip list ──────────────────────────────────────────────────────────
    platform = _detect_platform(url)
    if platform == "skip":
        logger.debug(f"[{symbol}] Skipping JS-only platform: {url}")
        result["method"] = "skip"
        return result

    # ── Platform-specific handlers ─────────────────────────────────────────
    if platform == "mynewsdesk":
        arts = _handle_mynewsdesk(url, symbol, feed_id, symbol_id)
        if arts:
            result["articles"] = arts
            result["method"]   = "mynewsdesk"
            return result

    if platform == "yahoo":
        arts = _handle_yahoo_finance(url, symbol, feed_id, symbol_id)
        if arts:
            result["articles"] = arts
            result["method"]   = "yahoo_api"
            return result

    if platform == "nasdaq":
        arts = _handle_nasdaq_page(url, symbol, feed_id, symbol_id)
        if arts:
            result["articles"] = arts
            result["method"]   = "nasdaq_api"
            return result

    if platform == "prnewswire":
        arts = _handle_prnewswire(url, symbol, feed_id, symbol_id)
        if arts:
            result["articles"] = arts
            result["method"]   = "prnewswire"
            return result

    if platform == "businesswire":
        arts = _handle_businesswire(url, symbol, feed_id, symbol_id)
        if arts:
            result["articles"] = arts
            result["method"]   = "businesswire"
            return result

    if platform == "q4ir":
        arts = _handle_q4ir_api(url, symbol, feed_id, symbol_id)
        if arts:
            result["articles"] = arts
            result["method"]   = "q4ir_api"
            return result

    # ── Fetch page for generic layers ──────────────────────────────────────
    resp = _fetch(url)
    if resp is None:
        return result

    html = resp.content

    # ── Layer 1: RSS Autodiscovery ─────────────────────────────────────────
    rss_url = _discover_rss_in_html(url, html)
    if rss_url:
        logger.info(f"[{symbol}] RSS autodiscovered: {rss_url}")
        entries = _fetch_rss(rss_url)
        if entries:
            result["rss_found"] = rss_url
            result["method"]    = "rss_autodiscovery"
            result["articles"]  = _entries_to_articles(
                entries, symbol_id, feed_id, "autodiscovered_rss"
            )
            return result

    # ── Layer 3: JSON-LD ───────────────────────────────────────────────────
    jsonld_arts = _extract_jsonld(html, url)
    if jsonld_arts:
        result["method"] = "json-ld"
        for art in jsonld_arts:
            result["articles"].append({
                "symbol_id":    symbol_id,
                "feed_id":      feed_id,
                "article_hash": _article_hash(art["url"], art["title"], art["published_at"]),
                "url":          art["url"],
                "title":        art["title"],
                "summary":      None,
                "full_text":    art.get("full_text"),
                "published_at": art["published_at"],
                "author":       art.get("author"),
                "source_name":  "json-ld",
            })
        return result

    # ── Layer 4: Trafilatura + LM Studio ──────────────────────────────────
    traf = _extract_trafilatura(html, url)
    if traf:
        gate = _lm_quality_gate(traf["title"], traf["full_text"] or "")
        if not gate.get("valid", True):
            logger.debug(f"[{symbol}] LM gate rejected: {gate.get('reason')}")
            return result

        clean_title = gate.get("cleaned_title") or traf["title"]
        clean_body  = gate.get("cleaned_body")  or traf["full_text"]
        result["method"] = "trafilatura+lm"
        result["articles"].append({
            "symbol_id":    symbol_id,
            "feed_id":      feed_id,
            "article_hash": _article_hash(url, clean_title, traf["published_at"]),
            "url":          url,
            "title":        clean_title,
            "summary":      None,
            "full_text":    str(clean_body)[:MAX_FULL_TEXT_LEN] if clean_body else None,
            "published_at": traf["published_at"],
            "author":       traf.get("author"),
            "source_name":  "html_scrape",
        })

    return result


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_html_feeds(conn, exchange: str, limit: int) -> list[dict]:
    sql = """
        SELECT rf.id AS id, rf.symbol_id AS symbol_id,
               rf.feed_url AS feed_url, s.symbol AS symbol
        FROM rss_feeds rf
        JOIN symbols s ON s.id = rf.symbol_id
        WHERE rf.feed_type = 'html'
          AND rf.is_active = TRUE
          AND s.exchange   = %s
        ORDER BY s.symbol
    """
    if limit > 0:
        sql += f" LIMIT {limit}"
    with conn.cursor() as cur:
        cur.execute(sql, (exchange,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _known_hashes(conn, hashes: list[str]) -> set[str]:
    if not hashes:
        return set()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT article_hash FROM news_articles WHERE article_hash = ANY(%s)",
            (hashes,)
        )
        return {row[0] for row in cur.fetchall()}


def _insert_articles(conn, articles: list[dict]) -> int:
    if not articles:
        return 0
    sql = """
        INSERT INTO news_articles
            (symbol_id, feed_id, article_hash, url, title,
             summary, full_text, published_at, author, source_name)
        VALUES
            (%(symbol_id)s, %(feed_id)s, %(article_hash)s, %(url)s, %(title)s,
             %(summary)s, %(full_text)s, %(published_at)s, %(author)s, %(source_name)s)
        ON CONFLICT (article_hash) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.executemany(sql, articles)
    conn.commit()
    return len(articles)


def _promote_to_rss(conn, feed_id: int, rss_url: str) -> bool:
    """Register newly discovered RSS URL. Returns True if new row inserted."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM rss_feeds WHERE feed_url = %s", (rss_url,))
        if cur.fetchone():
            return False
        cur.execute("SELECT symbol_id, source FROM rss_feeds WHERE id = %s", (feed_id,))
        row = cur.fetchone()
        if not row:
            return False
        symbol_id, source = row
        cur.execute(
            """
            INSERT INTO rss_feeds (symbol_id, feed_url, feed_type, source, is_active)
            VALUES (%s, %s, 'rss', %s, TRUE)
            ON CONFLICT (feed_url) DO NOTHING
            """,
            (symbol_id, rss_url, source)
        )
    conn.commit()
    return True


# ── Main entry point ──────────────────────────────────────────────────────────

def run(exchange: str = "NASDAQ", limit: int = 0) -> dict:
    exchange = exchange.upper()
    logger.info(f"[html_ingest] Starting — exchange={exchange}, limit={limit}")

    conn  = get_connection()
    feeds = _get_html_feeds(conn, exchange, limit)
    conn.close()

    if not feeds:
        logger.info("[html_ingest] No html-type feeds found. Run migrate_feed_type.py first.")
        return {"html_feeds": 0, "articles_new": 0, "rss_promoted": 0}

    logger.info(f"[html_ingest] {len(feeds)} HTML feeds to process")

    total       = len(feeds)
    done        = 0
    all_results = []

    with ThreadPoolExecutor(max_workers=HTML_WORKERS, thread_name_prefix="html") as ex:
        futures = {ex.submit(_process_html_feed, f): f for f in feeds}
        for fut in as_completed(futures):
            done += 1
            try:
                all_results.append(fut.result())
            except Exception as exc:
                logger.warning(f"[html_ingest] Feed raised: {exc}")
            if done % 50 == 0 or done == total:
                collected = sum(len(r.get("articles", [])) for r in all_results)
                logger.info(
                    f"[html_ingest] Pages: {done}/{total} "
                    f"| articles collected: {collected}"
                )

    # Promote discovered RSS feeds
    conn         = get_connection()
    rss_promoted = 0
    for res in all_results:
        if res.get("rss_found"):
            if _promote_to_rss(conn, res["feed_id"], res["rss_found"]):
                rss_promoted += 1
                logger.info(
                    f"[html_ingest] [{res['symbol']}] RSS promoted: {res['rss_found']}"
                )

    # Dedup + insert
    all_articles = [a for res in all_results for a in res.get("articles", [])]
    logger.info(f"[html_ingest] {len(all_articles)} raw articles collected")

    inserted   = 0
    skip_count = 0
    if all_articles:
        existing   = _known_hashes(conn, [a["article_hash"] for a in all_articles])
        new_arts   = [a for a in all_articles if a["article_hash"] not in existing]
        skip_count = len(all_articles) - len(new_arts)
        logger.info(
            f"[html_ingest] {len(new_arts)} new "
            f"({skip_count} already in DB, skipped)"
        )
        inserted = _insert_articles(conn, new_arts)

    conn.close()

    # Method breakdown
    methods: dict[str, int] = {}
    for res in all_results:
        m = res.get("method") or "none"
        methods[m] = methods.get(m, 0) + 1

    logger.info(
        f"[html_ingest] Done — feeds={len(feeds)}, new={inserted}, "
        f"skipped={skip_count}, rss_promoted={rss_promoted}, methods={methods}"
    )

    return {
        "html_feeds":   len(feeds),
        "articles_new": inserted,
        "articles_skip": skip_count,
        "rss_promoted": rss_promoted,
        "methods":      methods,
    }


if __name__ == "__main__":
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
        stream=sys.stderr,
    )
    ap = argparse.ArgumentParser(description="Step 2b – HTML Press Release Ingest")
    ap.add_argument("--exchange", "-e", default="NASDAQ")
    ap.add_argument("--limit",    "-l", type=int, default=0)
    args = ap.parse_args()
    from db import create_tables, test_connection
    if not test_connection():
        sys.exit(1)
    create_tables()
    print(run(exchange=args.exchange, limit=args.limit))
