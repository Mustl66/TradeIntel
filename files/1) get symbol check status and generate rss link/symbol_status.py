"""
symbol_status.py  –  Exchange Symbol Status Tracker
=====================================================
Autonomous pipeline that:
  1. Fetches live symbols from TradingView for the given exchange.
  2. Loads watchlist_status.json.
  3. Marks symbols no longer on the exchange as status=false.
  4. Marks returned symbols as status=true.
  5. Adds brand-new symbols (not yet in watchlist) as status=true with no RSS.
  6. Saves watchlist_status.json.

Usage:
  python symbol_status.py                   # NASDAQ, first 5 symbols (test mode)
  python symbol_status.py --exchange NYSE   # NYSE, first 5
  python symbol_status.py --limit 0         # NASDAQ, all symbols (production)
  python symbol_status.py --exchange NYSE --limit 0
"""

import sys
import json
import logging
import argparse
import requests
import urllib.parse
from pathlib import Path

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
WATCHLIST_FILE = Path(__file__).parent / "watchlist_status.json"


# ─── Helpers ─────────────────────────────────────────────────────────────────
def build_gnw_search_url(company_name: str) -> str:
    """Build the double-encoded GlobeNewswire search URL for a company."""
    first_encode = urllib.parse.quote(company_name)
    double_encode = urllib.parse.quote(first_encode)
    return f"https://www.globenewswire.com/en/search/organization/{double_encode}"


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


# ─── TradingView Live Symbol Fetch ───────────────────────────────────────────
def fetch_live_symbols(exchange: str) -> dict:
    """
    Returns a dict of { ticker: company_name } for the given exchange.
    Fetches from TradingView scanner in two passes:
      1. Get raw symbol list  (fast, no details)
      2. Query details in chunks of 500
    """
    try:
        import pandas as pd
        from tradingview_screener import Query, col
    except ImportError as e:
        logger.error(f"Missing dependency: {e}. Run: pip install pandas tradingview-screener")
        sys.exit(1)

    exchange_upper = exchange.upper()
    logger.info("Fetching symbol list from TradingView...")

    # Step 1 – raw list
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

    # Step 2 – details in chunks
    result = {}
    chunk_size = 500
    for i in range(0, len(selected), chunk_size):
        chunk = selected[i : i + chunk_size]
        page = (i // chunk_size) + 1
        total_pages = (len(selected) + chunk_size - 1) // chunk_size
        logger.info(f"Fetching details batch {page}/{total_pages}...")
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
                name = row.get("description", "")
                if ticker:
                    result[ticker] = name
        except Exception as e:
            logger.warning(f"Batch {page} failed: {e}")

    logger.info(f"Resolved details for {len(result)} symbols.")
    return result


# ─── Main Pipeline ────────────────────────────────────────────────────────────
def run(exchange: str, limit: int) -> None:
    exchange_upper = exchange.upper()

    # 1. Fetch live symbols from TradingView
    live = fetch_live_symbols(exchange_upper)
    if not live:
        logger.error("No live symbols fetched. Aborting.")
        sys.exit(1)

    # Apply test limit ONLY to which symbols we'll add as new entries.
    # Status checks for EXISTING watchlist entries always run against the full live set.
    live_tickers = set(live.keys())

    # Determine the slice for new-symbol additions (first N alphabetically)
    if limit > 0:
        sorted_live = sorted(live.keys())
        new_candidate_tickers = set(sorted_live[:limit])
        logger.info(f"Test mode: limiting new-symbol additions to first {limit} symbols.")
    else:
        new_candidate_tickers = live_tickers

    # 2. Load existing watchlist
    watchlist = load_watchlist()
    watchlist_by_symbol = {entry["symbol"]: entry for entry in watchlist}

    added = 0
    updated_to_true = 0
    updated_to_false = 0

    # 3. Update status for existing entries
    for entry in watchlist:
        sym = entry["symbol"]
        sym_exchange = entry.get("exchange", "").upper()

        # Only touch entries that belong to this exchange
        if sym_exchange != exchange_upper:
            continue

        was_active = entry.get("status", "true") in (True, "true")

        if sym in live_tickers:
            if not was_active:
                entry["status"] = "true"
                updated_to_true += 1
                logger.info(f"  [RESTORED]  {sym} is back on {exchange_upper}.")
        else:
            if was_active:
                entry["status"] = "false"
                updated_to_false += 1
                logger.info(f"  [DELISTED]  {sym} no longer on {exchange_upper}.")

    # 4. Add new symbols (within limit) not yet in watchlist
    for ticker in sorted(new_candidate_tickers):
        if ticker in watchlist_by_symbol:
            continue  # Already tracked

        company_name = live[ticker]
        gnw_url = build_gnw_search_url(company_name)

        new_entry = {
            "symbol": ticker,
            "company_name": company_name,
            "rss_links": [],
            "gnw_search_url": gnw_url,
            "exchange": exchange_upper,
            "status": "true",
        }
        watchlist.append(new_entry)
        watchlist_by_symbol[ticker] = new_entry
        added += 1
        logger.info(f"  [NEW]       {ticker} – {company_name}")

    # 5. Save
    save_watchlist(watchlist)

    print(f"\n Exchange:        {exchange_upper}")
    print(f" Live symbols:    {len(live_tickers)}")
    print(f" Watchlist total: {len(watchlist)}")
    print(f" New added:       {added}")
    print(f" Restored true:   {updated_to_true}")
    print(f" Marked false:    {updated_to_false}")
    print(f" Saved to:        {WATCHLIST_FILE}")


# ─── Entry Point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Track NASDAQ/NYSE symbol status in watchlist_status.json."
    )
    parser.add_argument(
        "--exchange",
        default="NASDAQ",
        help="Exchange name: NASDAQ or NYSE (default: NASDAQ)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit new-symbol additions per run (0 = unlimited, default: 5 for testing)",
    )
    args = parser.parse_args()
    run(args.exchange, args.limit)
