"""
news_ingest_runner.py — Step 2: News Ingestion Entry Point
===========================================================
Orchestrates three ingestion pipelines:
  2a. pipeline/news_ingest.py   — RSS/Atom feeds  (feed_type IN ('rss','atom','unknown'))
  2b. pipeline/html_ingest.py   — HTML press pages (feed_type = 'html')
  2c. pipeline/edgar_ingest.py  — SEC EDGAR 8-K filings (symbols without RSS, bulletproof)

All pipelines write to the same news_articles table.
Run migrate_feed_type.py once before first use to stamp all feed rows.

Standalone usage:
    python news_ingest_runner.py
    python news_ingest_runner.py --exchange NYSE
    python news_ingest_runner.py --limit 10
    python news_ingest_runner.py --no-scrape
    python news_ingest_runner.py --rss-only      # skip HTML + EDGAR
    python news_ingest_runner.py --html-only     # skip RSS + EDGAR
    python news_ingest_runner.py --edgar-only    # skip RSS + HTML
"""

import sys
import logging
import argparse

logger = logging.getLogger(__name__)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Step 2 – News Ingestion: RSS + HTML + EDGAR pipelines",
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
        help="Dev limit: only process first N feeds/symbols per pipeline. 0 = all (default: 0)",
    )
    parser.add_argument(
        "--no-scrape",
        action="store_true",
        help="Skip full-text scraping in the RSS pipeline. Store feed summary only.",
    )
    parser.add_argument(
        "--rss-only",
        action="store_true",
        help="Run only the RSS/Atom pipeline. Skip HTML and EDGAR.",
    )
    parser.add_argument(
        "--html-only",
        action="store_true",
        help="Run only the HTML press release pipeline. Skip RSS and EDGAR.",
    )
    parser.add_argument(
        "--edgar-only",
        action="store_true",
        help="Run only the SEC EDGAR pipeline. Skip RSS and HTML.",
    )
    return parser.parse_args(argv)


def run(
    exchange: str = "NASDAQ",
    limit: int = 0,
    scrape_full_text: bool = True,
    rss_only: bool = False,
    html_only: bool = False,
    edgar_only: bool = False,
):
    """
    Called by main.py.
    Runs all three pipelines unless a --*-only flag is passed.
    Pipeline on/off is controlled by pipeline_config.py (active: True/False).
    CLI flags (--rss-only etc.) still override config when passed explicitly.
    """
    from pipeline_config import PIPELINES
    results = {}

    rss_enabled   = PIPELINES["rss"]["active"]
    html_enabled  = PIPELINES["html"]["active"]
    edgar_enabled = PIPELINES["edgar"]["active"]

    # ── 2a: RSS/Atom pipeline ─────────────────────────────────────────────────
    if rss_enabled and not html_only and not edgar_only:
        logger.info("[news_ingest_runner] Starting RSS/Atom pipeline...")
        from pipeline.news_ingest import run as _rss_run
        rss_result = _rss_run(
            exchange=exchange,
            limit=limit,
            scrape_full_text=scrape_full_text,
        )
        results["rss"] = rss_result
        logger.info(f"[news_ingest_runner] RSS result: {rss_result}")
    elif not rss_enabled:
        logger.info("[news_ingest_runner] RSS pipeline DISABLED (pipeline_config.py)")

    # ── 2b: HTML press release pipeline ──────────────────────────────────────
    if html_enabled and not rss_only and not edgar_only:
        logger.info("[news_ingest_runner] Starting HTML press release pipeline...")
        from pipeline.html_ingest import run as _html_run
        html_result = _html_run(
            exchange=exchange,
            limit=limit,
        )
        results["html"] = html_result
        logger.info(f"[news_ingest_runner] HTML result: {html_result}")
    elif not html_enabled:
        logger.info("[news_ingest_runner] HTML pipeline DISABLED (pipeline_config.py)")

    # ── 2c: SEC EDGAR 8-K pipeline ────────────────────────────────────────────
    if edgar_enabled and not rss_only and not html_only:
        logger.info("[news_ingest_runner] Starting SEC EDGAR 8-K pipeline...")
        from pipeline.edgar_ingest import run as _edgar_run
        edgar_result = _edgar_run(limit=limit)
        results["edgar"] = edgar_result
        logger.info(f"[news_ingest_runner] EDGAR result: {edgar_result}")
    elif not edgar_enabled:
        logger.info("[news_ingest_runner] EDGAR pipeline DISABLED (pipeline_config.py)")

    return results


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
        rss_only=args.rss_only,
        html_only=args.html_only,
        edgar_only=args.edgar_only,
    )
