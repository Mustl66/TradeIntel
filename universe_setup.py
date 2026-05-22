"""
universe_setup.py — Step 1: Universe Management
=================================================
Responsible for:
  1a. Syncing the symbol universe with TradingView  (symbol_status)
  1b. Discovering GlobeNewswire RSS/Atom feeds       (rss_finder)

Called by main.py. Can also be run standalone:
    python universe_setup.py
    python universe_setup.py --exchange NYSE
    python universe_setup.py --step symbol_status
    python universe_setup.py --limit 20
    python universe_setup.py --refresh
"""

import sys
import logging
import argparse

logger = logging.getLogger(__name__)


# ── Argument Parser ───────────────────────────────────────────────────────────

def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Step 1 – Universe Management: symbol sync + RSS discovery",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--exchange", "-e",
        default="NASDAQ",
        help="Exchange to process: NASDAQ or NYSE (default: NASDAQ)",
    )
    parser.add_argument(
        "--step", "-s",
        choices=["symbol_status", "rss_finder", "all"],
        default="all",
        help=(
            "Which sub-step to run:\n"
            "  symbol_status  – sync universe with TradingView\n"
            "  rss_finder     – find GlobeNewswire RSS/Atom feeds\n"
            "  all            – run both in order (default)"
        ),
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=0,
        help="Dev limit: only process first N symbols. 0 = all (default: 0)",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-check RSS feeds even for symbols that already have one.",
    )
    return parser.parse_args(argv)


# ── Main Entry ────────────────────────────────────────────────────────────────

def run(exchange: str = "NASDAQ", step: str = "all", limit: int = 0, refresh: bool = False):
    """
    Called by main.py (or standalone).
    exchange : NASDAQ | NYSE
    step     : symbol_status | rss_finder | all
    limit    : 0 = process all, >0 = dev/test cap
    refresh  : if True, re-check RSS even for symbols that already have feeds
    """
    exchange = exchange.upper()

    if limit > 0:
        logger.info(f"[universe_setup] Dev mode: capped at {limit} symbols.")

    if step in ("all", "symbol_status"):
        logger.info(f"[universe_setup] ── Step 1a: symbol_status [{exchange}] ──")
        from pipeline.symbol_status import run as run_symbol_status
        run_symbol_status(exchange=exchange, limit=limit)

    if step in ("all", "rss_finder"):
        logger.info(f"[universe_setup] ── Step 1b: rss_finder [{exchange}] ──")
        from pipeline.rss_finder import run as run_rss_finder
        run_rss_finder(exchange=exchange, limit=limit, refresh=refresh)

    logger.info("[universe_setup] Step 1 complete.")


# ── Standalone execution ──────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
        stream=sys.stderr,
    )

    # When run standalone, bootstrap DB first
    try:
        from db import create_tables, test_connection
        if not test_connection():
            logger.error(
                "Cannot connect to PostgreSQL. "
                "Check your .env file and ensure the DB is running."
            )
            sys.exit(1)
        create_tables()
    except EnvironmentError as e:
        logger.error(str(e))
        sys.exit(1)

    args = parse_args()
    run(
        exchange=args.exchange,
        step=args.step,
        limit=args.limit,
        refresh=args.refresh,
    )
