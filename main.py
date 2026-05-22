"""
main.py — TradeIntel Orchestrator
===================================
Pure orchestrator. Starts pipeline scripts in order.
No business logic lives here.

Usage:
    python main.py                        # Run full pipeline (NASDAQ)
    python main.py --exchange NYSE
    python main.py --limit 20             # Dev mode
    python main.py --only universe        # Run only one stage

Pipeline stages (in order):
    1. universe_setup   – symbol universe sync + RSS feed discovery
    2. news_ingest      – fetch & store full-text news articles
    # 3. sector_map     – (Step 3)
    # 4. sentiment      – (Step 4)
    # 5. aggregation    – (Step 5)
    # 6. output         – (Step 6)
"""

import sys
import logging
import argparse

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("tradeintel.main")


# ── Registered pipeline stages ────────────────────────────────────────────────
# Add a new entry here whenever a new stage script is created.
# Each entry: (stage_name, module_path, run_function)
STAGES = [
    "universe",     # universe_setup.py        → Step 1
    "news",         # news_ingest_runner.py     → Step 2
    # "sectors",    # sector_map.py             → Step 3
    # "sentiment",  # sentiment.py              → Step 4
    # "aggregation",# aggregation.py            → Step 5
    # "output",     # output.py                 → Step 6
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="TradeIntel – Automated Stock Sentiment Pipeline",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--exchange", "-e",
        default="NASDAQ",
        help="Exchange to process: NASDAQ or NYSE (default: NASDAQ)",
    )
    parser.add_argument(
        "--only",
        choices=STAGES,
        default=None,
        help="Run only a specific stage instead of the full pipeline.",
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=0,
        help="Dev limit: only process first N symbols per stage. 0 = all.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Pass --refresh flag down to stages that support it.",
    )
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


def run_stage(name: str, args):
    """Dispatch to the correct stage script."""
    if name == "universe":
        from universe_setup import run
        run(exchange=args.exchange, limit=args.limit, refresh=args.refresh)

    # Future stages — uncomment as they are built:
    elif name == "news":
        from news_ingest_runner import run
        run(
            exchange=args.exchange,
            limit=args.limit,
            scrape_full_text=True,
        )

    # elif name == "sectors":
    #     from sector_map import run
    #     run(exchange=args.exchange)
    #
    # elif name == "sentiment":
    #     from sentiment import run
    #     run(exchange=args.exchange, limit=args.limit)
    #
    # elif name == "aggregation":
    #     from aggregation import run
    #     run(exchange=args.exchange)
    #
    # elif name == "output":
    #     from output import run
    #     run(exchange=args.exchange)

    else:
        logger.warning(f"Unknown stage '{name}' — skipped.")


def main():
    args = parse_args()

    bootstrap_db()

    stages_to_run = [args.only] if args.only else STAGES

    logger.info(f"Starting pipeline — exchange={args.exchange}, stages={stages_to_run}")

    for stage in stages_to_run:
        logger.info(f"━━━ Stage: {stage} ━━━")
        run_stage(stage, args)

    logger.info("Pipeline finished.")


if __name__ == "__main__":
    main()
