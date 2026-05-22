"""
news_ingest_runner.py — Step 2: News Ingestion Entry Point
===========================================================
Thin wrapper called by main.py.
All logic lives in pipeline/news_ingest.py.

Standalone usage:
    python news_ingest_runner.py
    python news_ingest_runner.py --exchange NYSE
    python news_ingest_runner.py --limit 10
    python news_ingest_runner.py --no-scrape
"""

import sys
import logging
import argparse

logger = logging.getLogger(__name__)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Step 2 – News Ingestion: fetch & store full-text articles",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--exchange", "-e",
        default="NASDAQ",
        help="Exchange to process: NASDAQ or NYSE (default: NASDAQ)",
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=0,
        help="Dev limit: only process first N feeds. 0 = all (default: 0)",
    )
    parser.add_argument(
        "--no-scrape",
        action="store_true",
        help="Skip full-text HTML scraping. Store feed summary only.",
    )
    return parser.parse_args(argv)


def run(exchange: str = "NASDAQ", limit: int = 0, scrape_full_text: bool = True):
    """
    Called by main.py.
    exchange         : NASDAQ | NYSE
    limit            : 0 = all feeds, >0 = dev cap
    scrape_full_text : fetch full article body from URL (default True)
    """
    from pipeline.news_ingest import run as _run
    result = _run(exchange=exchange, limit=limit, scrape_full_text=scrape_full_text)
    logger.info(f"[news_ingest_runner] Result: {result}")
    return result


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
        stream=sys.stderr,
    )

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
        limit=args.limit,
        scrape_full_text=not args.no_scrape,
    )
