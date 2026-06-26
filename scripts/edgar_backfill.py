"""
One-time historical backfill script for SEC EDGAR filings.

Fetches the following history for all active NASDAQ symbols:
  - 10-K  : 5 years
  - 10-Q  : 5 quarters
  - 8-K   : 24 months
  - S-3 / 424B : 36 months
  - Form 4 / 13D / 13G : 12 months

Filings are organised into three tiers:
  Tier 1 – Core Financials  : 10-K, 10-Q, 8-K
  Tier 2 – Capital/Dilution : S-3, 424B, NT filings
  Tier 3 – Ownership/Insiders: Form 4, 13D, 13G

After this backfill has run once, Worker 4 and Worker 5 inside
orchestrator.py take over and handle all incremental updates
automatically — this script does NOT need to be re-run.
"""

import sys
import os
import logging
import argparse

# Make the project root importable regardless of where the script is invoked from.
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from db import create_tables, test_connection                   # noqa: E402
from pipeline.edgar_ingest import run as edgar_run              # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(
        description="One-time SEC EDGAR historical backfill for TradeIntel.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=0,
        help="Only process first N symbols (0=all)",
    )
    parser.add_argument(
        "--tier",
        type=int,
        choices=[1, 2, 3],
        default=None,
        help="Only run a specific tier (1=Core Financials, 2=Capital & Dilution, 3=Ownership & Insiders)",
    )
    parser.add_argument(
        "--forms",
        nargs="+",
        default=None,
        help="Only run specific form types e.g. 10-K 10-Q",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would run without executing",
    )

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args()

    # ------------------------------------------------------------------
    # Banner
    # ------------------------------------------------------------------
    logger.info("=" * 70)
    logger.info("   TradeIntel SEC EDGAR Historical Backfill")
    logger.info("=" * 70)

    # ------------------------------------------------------------------
    # Log parameters
    # ------------------------------------------------------------------
    logger.info("Run parameters:")
    logger.info("  --limit   : %s", args.limit if args.limit else "all symbols")
    logger.info("  --tier    : %s", args.tier if args.tier is not None else "all tiers (1, 2, 3)")
    logger.info("  --forms   : %s", " ".join(args.forms) if args.forms else "all forms for selected tier(s)")
    logger.info("  --dry-run : %s", args.dry_run)

    # ------------------------------------------------------------------
    # DB setup
    # ------------------------------------------------------------------
    logger.info("Testing database connection …")
    test_connection()
    logger.info("Creating / verifying tables …")
    create_tables()

    # ------------------------------------------------------------------
    # Dry-run: show plan and exit
    # ------------------------------------------------------------------
    if args.dry_run:
        logger.info("[DRY RUN] The following phases would execute:")
        tiers_to_run = [args.tier] if args.tier is not None else [1, 2, 3]
        tier_labels = {
            1: "Tier 1 Core Financials  — 10-K (5yr), 10-Q (5qtr), 8-K (24mo)",
            2: "Tier 2 Capital/Dilution — S-3, 424B, NT filings (36mo)",
            3: "Tier 3 Ownership/Insiders — Form 4, 13D, 13G (12-36mo)",
        }
        for i, tier in enumerate(tiers_to_run, start=1):
            logger.info(
                "  Phase %d/%d: %s | limit=%s | forms=%s",
                i,
                len(tiers_to_run),
                tier_labels[tier],
                args.limit if args.limit else "all",
                " ".join(args.forms) if args.forms else "default",
            )
        logger.info("[DRY RUN] Exiting — no data was fetched or written.")
        return

    # ------------------------------------------------------------------
    # Phase 1 — Tier 1: Core Financials
    # ------------------------------------------------------------------
    logger.info("-" * 70)
    logger.info("Phase 1/3: Tier 1 Core Financials (10-K 5yr, 10-Q 5qtr, 8-K 24mo)")
    logger.info("-" * 70)

    tier_filter_p1 = [1] if not args.tier else [args.tier]
    result_p1 = edgar_run(
        limit=args.limit,
        tier_filter=tier_filter_p1,
        forms_only=args.forms,
    )
    inserted_p1 = result_p1.get("inserted", 0) if isinstance(result_p1, dict) else result_p1
    logger.info("Phase 1 result: %s records inserted.", inserted_p1)

    total_inserted = inserted_p1

    # ------------------------------------------------------------------
    # Phases 2 & 3 — only when running all tiers
    # ------------------------------------------------------------------
    if args.tier is None:

        # Phase 2 — Tier 2: Capital & Dilution
        logger.info("-" * 70)
        logger.info("Phase 2/3: Tier 2 Capital & Dilution (S-3, 424B, NT filings 36mo)")
        logger.info("-" * 70)

        result_p2 = edgar_run(
            limit=args.limit,
            tier_filter=[2],
            forms_only=args.forms,
        )
        inserted_p2 = result_p2.get("inserted", 0) if isinstance(result_p2, dict) else result_p2
        logger.info("Phase 2 result: %s records inserted.", inserted_p2)
        total_inserted += inserted_p2

        # Phase 3 — Tier 3: Ownership & Insiders
        logger.info("-" * 70)
        logger.info("Phase 3/3: Tier 3 Ownership & Insiders (Form4, 13D, 13G 12-36mo)")
        logger.info("-" * 70)

        result_p3 = edgar_run(
            limit=args.limit,
            tier_filter=[3],
            forms_only=args.forms,
        )
        inserted_p3 = result_p3.get("inserted", 0) if isinstance(result_p3, dict) else result_p3
        logger.info("Phase 3 result: %s records inserted.", inserted_p3)
        total_inserted += inserted_p3

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    logger.info("=" * 70)
    logger.info("Grand total records inserted across all phases: %s", total_inserted)
    logger.info("Backfill complete. Run orchestrator.py to start incremental updates.")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
