"""
rss_finder.py  –  GlobeNewswire RSS Feed Finder
================================================
Autonomous pipeline that:
  1. Reads watchlist_status.json.
  2. For each symbol (optionally limited by --limit), visits its
     GlobeNewswire search page to find the first news article.
  3. Fetches that article page and extracts the RSS and Atom feed URLs
     from the embedded JSON (both are stored, never deleted).
  4. Saves updated entries back to watchlist_status.json.

Feed extraction works by parsing the JSON blob embedded in the article HTML:
  {"Key":"rss",  "Url":"/rssfeed/organization/<id>"}
  {"Key":"atom", "Url":"/atomfeed/organization/<id>"}

Usage:
  python rss_finder.py                    # process first 5 in watchlist (test)
  python rss_finder.py --limit 0          # process all (production)
  python rss_finder.py --exchange NYSE --limit 10
  python rss_finder.py --refresh          # re-check even already-filled entries
"""

import sys
import json
import re
import logging
import argparse
import urllib.parse
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
WATCHLIST_FILE = Path(__file__).parent / "watchlist_status.json"
GNW_BASE = "https://www.globenewswire.com"
MAX_WORKERS = 5   # concurrent requests to GlobeNewswire

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


# ─── Helpers ─────────────────────────────────────────────────────────────────
def load_watchlist() -> list:
    if WATCHLIST_FILE.exists():
        try:
            with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Migrate rss_link to rss_links
                for entry in data:
                    if "rss_link" in entry:
                        val = entry.pop("rss_link")
                        if not val:
                            entry["rss_links"] = []
                        elif isinstance(val, str):
                            entry["rss_links"] = [val]
                        elif isinstance(val, list):
                            entry["rss_links"] = [x for x in val if x]
                        else:
                            entry["rss_links"] = []
                    elif "rss_links" not in entry:
                        entry["rss_links"] = []
                return data
        except Exception as e:
            logger.error(f"Failed to read {WATCHLIST_FILE}: {e}")
    return []


def save_watchlist(data: list) -> None:
    try:
        with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.info(f"watchlist_status.json saved ({len(data)} entries).")
    except Exception as e:
        logger.error(f"Failed to save watchlist: {e}")


def build_gnw_search_url(company_name: str) -> str:
    first_encode = urllib.parse.quote(company_name)
    double_encode = urllib.parse.quote(first_encode)
    return f"{GNW_BASE}/en/search/organization/{double_encode}"


def normalize_rss_links(existing) -> list:
    """Normalize rss_links field to a list (handles str, list, None)."""
    if not existing:
        return []
    if isinstance(existing, str):
        return [existing] if existing else []
    if isinstance(existing, list):
        return [x for x in existing if x]
    return []


def merge_rss_links(existing, new_links: list) -> list:
    """Add new_links to existing without duplicates. Returns list."""
    current = normalize_rss_links(existing)
    for link in new_links:
        if link and link not in current:
            current.append(link)
    return current


def has_rss(entry: dict) -> bool:
    """Return True if the entry already has at least one GlobeNewswire RSS link."""
    links = entry.get("rss_links", [])
    return any("globenewswire.com" in l for l in links)


# ─── GlobeNewswire Fetcher ───────────────────────────────────────────────────
def get_first_article_url(search_url: str) -> str | None:
    """
    Fetch the GNW search page and return the URL of the first news article.
    Matches: href="/news-release/..."
    """
    try:
        r = requests.get(search_url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        match = re.search(r'href="(/news-release/[^"]+\.html)"', r.text)
        if match:
            return GNW_BASE + match.group(1)
    except Exception as e:
        logger.debug(f"get_first_article_url error for {search_url}: {e}")
    return None


def extract_feeds_from_article(article_url: str) -> dict:
    """
    Fetch a GNW article page and extract RSS + Atom feed URLs
    from the embedded JSON data block.

    Returns:
        {
          "rss":  "https://www.globenewswire.com/rssfeed/organization/..."  or None,
          "atom": "https://www.globenewswire.com/atomfeed/organization/..." or None,
          "org_id": <int> or None,
        }
    """
    result = {"rss": None, "atom": None, "org_id": None}
    try:
        r = requests.get(article_url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        html = r.text

        # Extract RSS feed path from JSON blob (escaped in HTML)
        rss_match = re.search(
            r'\\"Key\\":\\"rss\\"[^}]*\\"Url\\":\\"(/rssfeed/organization/[^\\]+)\\"',
            html,
        )
        if rss_match:
            result["rss"] = GNW_BASE + rss_match.group(1)

        # Extract Atom feed path
        atom_match = re.search(
            r'\\"Key\\":\\"atom\\"[^}]*\\"Url\\":\\"(/atomfeed/organization/[^\\]+)\\"',
            html,
        )
        if atom_match:
            result["atom"] = GNW_BASE + atom_match.group(1)

        # Also try unescaped variants (some pages differ)
        if not result["rss"]:
            rss_plain = re.search(r'href="(/rssfeed/organization/[^"]+)"', html)
            if rss_plain:
                result["rss"] = GNW_BASE + rss_plain.group(1)

        if not result["atom"]:
            atom_plain = re.search(r'href="(/atomfeed/organization/[^"]+)"', html)
            if atom_plain:
                result["atom"] = GNW_BASE + atom_plain.group(1)

        # Extract ContextOrgId (the numeric organization ID)
        org_match = re.search(r"ContextOrgId\s*:\s*(\d+)", html)
        if org_match:
            result["org_id"] = int(org_match.group(1))

    except Exception as e:
        logger.debug(f"extract_feeds_from_article error for {article_url}: {e}")

    return result


def process_entry(entry: dict, refresh: bool) -> dict:
    """
    Look up RSS feeds for one watchlist entry and return updated entry.
    Skips entries that already have feeds unless --refresh is set.
    """
    sym = entry["symbol"]
    company = entry.get("company_name", "")

    # Skip if already has RSS and we're not refreshing
    if not refresh and has_rss(entry):
        logger.info(f"  [SKIP]  {sym} – RSS already present.")
        return entry

    # Get or build GNW search URL
    search_url = entry.get("gnw_search_url") or build_gnw_search_url(company)

    logger.info(f"  [SEARCH] {sym} – {company}")
    logger.debug(f"           search URL: {search_url}")

    article_url = get_first_article_url(search_url)
    if not article_url:
        logger.warning(f"  [NO ART] {sym} – no articles found on GlobeNewswire.")
        return entry

    logger.debug(f"  [ART]   {sym} – {article_url}")
    feeds = extract_feeds_from_article(article_url)

    new_links = []
    if feeds["rss"]:
        new_links.append(feeds["rss"])
        logger.info(f"  [RSS]   {sym} – {feeds['rss']}")
    if feeds["atom"]:
        new_links.append(feeds["atom"])
        logger.info(f"  [ATOM]  {sym} – {feeds['atom']}")

    if new_links:
        entry["rss_links"] = merge_rss_links(entry.get("rss_links"), new_links)
    else:
        logger.warning(f"  [NO RSS] {sym} – no RSS/Atom feed found in article.")

    if feeds["org_id"] and not entry.get("gnw_org_id"):
        entry["gnw_org_id"] = feeds["org_id"]

    # Ensure the search URL is stored for future runs
    if not entry.get("gnw_search_url"):
        entry["gnw_search_url"] = search_url

    return entry


# ─── Main Pipeline ────────────────────────────────────────────────────────────
def run(exchange: str, limit: int, refresh: bool) -> None:
    exchange_upper = exchange.upper()

    watchlist = load_watchlist()
    if not watchlist:
        logger.error("watchlist_status.json is empty or missing. Run symbol_status.py first.")
        sys.exit(1)

    # Filter to target exchange only
    targets = [e for e in watchlist if e.get("exchange", "").upper() == exchange_upper]

    if not targets:
        logger.warning(f"No entries found for exchange {exchange_upper}.")
        return

    # Apply limit (0 = all)
    if limit > 0:
        targets = targets[:limit]
        logger.info(f"Test mode: processing first {limit} entries for {exchange_upper}.")
    else:
        logger.info(f"Processing all {len(targets)} entries for {exchange_upper}.")

    # Build a fast lookup to update in-place
    watchlist_by_symbol = {e["symbol"]: e for e in watchlist}

    found_count = 0
    skipped_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_entry, entry, refresh): entry for entry in targets}
        for future in as_completed(futures):
            updated = future.result()
            sym = updated["symbol"]
            watchlist_by_symbol[sym].update(updated)
            if has_rss(updated):
                found_count += 1
            else:
                skipped_count += 1

    # Rebuild watchlist in original order
    updated_watchlist = list(watchlist_by_symbol.values())
    save_watchlist(updated_watchlist)

    print(f"\n Exchange:        {exchange_upper}")
    print(f" Entries checked: {len(targets)}")
    print(f" RSS found/had:   {found_count}")
    print(f" No RSS:          {skipped_count}")
    print(f" Saved to:        {WATCHLIST_FILE}")


# ─── Entry Point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Find and store GlobeNewswire RSS feeds in watchlist_status.json."
    )
    parser.add_argument(
        "--exchange",
        default="NASDAQ",
        help="Exchange name: NASDAQ or NYSE (default: NASDAQ)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of entries to process (0 = all, default: 5 for testing)",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-check RSS even for entries that already have a feed.",
    )
    args = parser.parse_args()
    run(args.exchange, args.limit, args.refresh)
