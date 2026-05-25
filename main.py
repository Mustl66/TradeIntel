"""
main.py - TradeIntel Orchestrator
===================================
Pure orchestrator. Starts pipeline scripts in order.
No business logic lives here.

Usage:
    python main.py                        # Run full pipeline (NASDAQ)
    python main.py --exchange NYSE
    python main.py --limit 20             # Dev mode
    python main.py --only universe        # Run only one stage

Pipeline stages (in order):
    1. universe         - symbol universe sync + RSS feed discovery
    2. news             - fetch & store full-text news articles
    3. sector_map       - TradingView sector/industry mapping
    4. market_research  - market research RSS feed ingestion
    5. macro_multiplier - LLM sector growth multiplier extraction
    # 6. sentiment      - (Step 4)
    # 7. aggregation    - (Step 5)
    # 8. output         - (Step 6)
"""

import sys
import logging
import argparse

from pipeline_config import PIPELINES, START_FROM as _CONFIG_START_FROM

# -- Logging ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("tradeintel.main")


# -- Registered pipeline stages -----------------------------------------------
STAGES = [
    "universe",          # universe_setup.py                   -> Step 1
    "news",              # news_ingest_runner.py               -> Step 2
    "sector_map",        # pipeline/sector_map.py              -> Step 3a
    "market_research",   # pipeline/market_research_ingest.py  -> Step 3b
    "macro_multiplier",  # pipeline/macro_multiplier.py        -> Step 3c
    # "sentiment",       # pipeline/sentiment.py               -> Step 4
    # "aggregation",     # pipeline/aggregation.py             -> Step 5
    # "output",          # pipeline/output.py                  -> Step 6
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="TradeIntel - Automated Stock Sentiment Pipeline",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--exchange", "-e", default="NASDAQ",
                        help="Exchange to process: NASDAQ or NYSE (default: NASDAQ)")
    parser.add_argument("--only", choices=STAGES, default=None,
                        help="Run only a specific stage instead of the full pipeline.")
    parser.add_argument("--limit", "-l", type=int, default=0,
                        help="Dev limit: only process first N symbols per stage. 0 = all.")
    parser.add_argument("--refresh", action="store_true",
                        help="Pass --refresh flag down to stages that support it.")
    parser.add_argument("--dry-run", "-d", action="store_true",
                        help="Print LLM signals without writing to DB (macro_multiplier only).")
    parser.add_argument("--start-from", default=None, choices=STAGES,
                        help="Skip all stages before this one (overrides START_FROM in pipeline_config.py).")
    return parser.parse_args()


def bootstrap_db():
    """Verify DB connectivity and ensure schema is up to date."""
    logger.info("Bootstrapping database...")
    try:
        from db import create_tables, test_connection
        if not test_connection():
            logger.error(
                "Cannot connect to PostgreSQL. "
                "Check your .env file (copy from .env.example) and ensure PostgreSQL is running."
            )
            sys.exit(1)
        create_tables()
    except EnvironmentError as e:
        logger.error(str(e))
        sys.exit(1)


def _is_active(stage: str) -> bool:
    """Check pipeline_config toggle. Unknown stages default to active."""
    cfg = PIPELINES.get(stage)
    if cfg is None:
        return True
    return cfg.get("active", True)


def run_stage(name: str, args):
    """Dispatch to the correct stage script."""
    if not _is_active(name):
        logger.info(f"Stage '{name}' is disabled in pipeline_config.py - skipping.")
        return

    if name == "universe":
        from universe_setup import run
        run(exchange=args.exchange, limit=args.limit, refresh=args.refresh)

    elif name == "news":
        from news_ingest_runner import run
        run(exchange=args.exchange, limit=args.limit, scrape_full_text=True)

    elif name == "sector_map":
        from pipeline.sector_map import run
        run(exchange=args.exchange, limit=args.limit)

    elif name == "market_research":
        from pipeline.market_research_ingest import run
        run(limit=args.limit)

    elif name == "macro_multiplier":
        from pipeline.macro_multiplier import run
        run(limit=args.limit, dry_run=getattr(args, "dry_run", False))

    # elif name == "sentiment":
    #     from pipeline.sentiment import run
    #     run(exchange=args.exchange, limit=args.limit)

    else:
        logger.warning(f"Unknown stage '{name}' - skipped.")


def main():
    args = parse_args()
    bootstrap_db()

    # Resolve start_from: CLI flag > pipeline_config.py > None
    start_from = getattr(args, "start_from", None) or _CONFIG_START_FROM

    stages_to_run = [args.only] if args.only else STAGES

    if start_from and not args.only:
        if start_from not in STAGES:
            logger.error(f"Unknown start_from stage '{start_from}'. Valid: {STAGES}")
            sys.exit(1)
        skip_idx = STAGES.index(start_from)
        stages_to_run = STAGES[skip_idx:]
        logger.info(f"START_FROM='{start_from}' — skipping: {STAGES[:skip_idx]}")

    logger.info(f"Starting pipeline - exchange={args.exchange}, stages={stages_to_run}")
    for stage in stages_to_run:
        logger.info(f"=== Stage: {stage} ===")
        run_stage(stage, args)
    logger.info("Pipeline finished.")


if __name__ == "__main__":
    main()
