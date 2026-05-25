"""
pipeline/sector_map.py — Phase 3: Sector & Industry Mapping
=============================================================
Fetches sector/industry for every symbol via TradingView Screener,
inserts new industries into sectors_macro (multiplier=1.000 default),
then maps sector_id back onto every symbols row.

Usage:
    python -m pipeline.sector_map                  # all exchanges
    python -m pipeline.sector_map --exchange NASDAQ
    python -m pipeline.sector_map --limit 50       # dev/test
"""

import logging
import time
from datetime import datetime, timezone

from tradingview_screener import Query, Column
from db.connection import get_connection

logger = logging.getLogger(__name__)

# ── TradingView field names ───────────────────────────────────────────────────
TV_SECTOR   = "sector"
TV_INDUSTRY = "industry"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fetch_tv_sectors(symbols: list[str]) -> dict[str, dict]:
    """
    Query TradingView Screener for sector + industry for a batch of tickers.
    Returns {ticker: {"sector": ..., "industry": ...}}.
    Batches in chunks of 500 to stay within API limits.
    """
    result = {}
    chunk_size = 500

    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        try:
            _, df = (
                Query()
                .select(TV_SECTOR, TV_INDUSTRY)
                .where(Column("name").isin(chunk))
                .get_scanner_data()
            )
            for _, row in df.iterrows():
                ticker = row.get("name") or row.get("ticker", "")
                # TV returns "NASDAQ:AAPL" — strip exchange prefix
                if ":" in ticker:
                    ticker = ticker.split(":", 1)[1]
                sector   = (row.get(TV_SECTOR)   or "").strip()
                industry = (row.get(TV_INDUSTRY) or "").strip()
                if ticker and sector and industry:
                    result[ticker] = {"sector": sector, "industry": industry}
        except Exception as e:
            logger.warning(f"TradingView batch {i//chunk_size} failed: {e}")
        time.sleep(0.5)   # polite rate limit

    return result


def _upsert_industry(cur, sector: str, industry: str) -> int:
    """
    Insert industry if not present. Returns sectors_macro.id.
    Multiplier left at default 1.000 — macro_multiplier.py updates it later.
    """
    cur.execute("""
        INSERT INTO sectors_macro (sector_name, industry_name, macro_multiplier, updated_at)
        VALUES (%s, %s, 1.000, NOW())
        ON CONFLICT (sector_name, industry_name) DO NOTHING
        RETURNING id
    """, (sector, industry))
    row = cur.fetchone()
    if row:
        return row[0]
    # Already existed — fetch id
    cur.execute(
        "SELECT id FROM sectors_macro WHERE sector_name=%s AND industry_name=%s",
        (sector, industry)
    )
    return cur.fetchone()[0]


def _get_all_symbols(cur, exchange: str | None, limit: int) -> list[dict]:
    """Fetch all active symbols we want to map."""
    q = "SELECT id, symbol, exchange FROM symbols"
    args = []
    if exchange:
        q += " WHERE exchange = %s"
        args.append(exchange.upper())
    q += " ORDER BY symbol"
    if limit:
        q += f" LIMIT {limit}"
    cur.execute(q, args)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── Main orchestrator ─────────────────────────────────────────────────────────

def run(exchange: str | None = None, limit: int = 0) -> dict:
    """
    Full sector mapping run.
    Returns stats dict: {mapped, new_industries, missing, duration_s}
    """
    started_at = datetime.now(timezone.utc)
    conn = get_connection()

    # Log pipeline start
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_runs (step, exchange, started_at, status)
            VALUES ('sector_map', %s, NOW(), 'running') RETURNING id
        """, (exchange or "ALL",))
        run_id = cur.fetchone()[0]
    conn.commit()

    stats = {"mapped": 0, "new_industries": 0, "missing": 0, "duration_s": 0}

    try:
        with conn.cursor() as cur:
            symbols = _get_all_symbols(cur, exchange, limit)

        if not symbols:
            logger.warning("No symbols found to map.")
            return stats

        logger.info(f"Fetching sectors for {len(symbols)} symbols from TradingView...")
        tickers = [s["symbol"] for s in symbols]
        tv_data = _fetch_tv_sectors(tickers)
        logger.info(f"TradingView returned data for {len(tv_data)} tickers.")

        # Build symbol_id lookup
        sym_by_ticker = {s["symbol"]: s["id"] for s in symbols}

        with conn.cursor() as cur:
            for ticker, info in tv_data.items():
                sector   = info["sector"]
                industry = info["industry"]
                sym_id   = sym_by_ticker.get(ticker)
                if not sym_id:
                    continue

                # Upsert industry
                sector_id = _upsert_industry(cur, sector, industry)
                if sector_id:
                    stats["new_industries"] += 1   # crude — refined below

                # Map sector_id onto symbol
                cur.execute("""
                    UPDATE symbols SET sector_id = %s, last_updated_at = NOW()
                    WHERE id = %s AND (sector_id IS DISTINCT FROM %s)
                """, (sector_id, sym_id, sector_id))
                stats["mapped"] += 1  # count all TV-matched symbols, not just changed rows

            conn.commit()

        # Count truly new industries (inserted this run)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM sectors_macro
                WHERE last_llm_run_at IS NULL
            """)
            stats["new_industries"] = cur.fetchone()[0]

        stats["missing"] = len(symbols) - len(tv_data)
        logger.info(
            f"Sector map complete — mapped={stats['mapped']}, "
            f"new_industries={stats['new_industries']}, missing={stats['missing']}"
        )

    except Exception as e:
        logger.error(f"sector_map failed: {e}")
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE pipeline_runs SET status='failed', finished_at=NOW(),
                error_message=%s WHERE id=%s
            """, (str(e), run_id))
        conn.commit()
        raise

    finally:
        duration = (datetime.now(timezone.utc) - started_at).total_seconds()
        stats["duration_s"] = round(duration, 1)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE pipeline_runs
                SET status='success', finished_at=NOW(),
                    symbols_total=%s, symbols_mapped=%s,
                    meta=%s::jsonb
                WHERE id=%s
            """, (
                len(symbols),
                stats["mapped"],
                f'{{"new_industries":{stats["new_industries"]},"missing":{stats["missing"]}}}',
                run_id
            ))
        conn.commit()
        conn.close()

    return stats


if __name__ == "__main__":
    import argparse, sys, logging
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
                        stream=sys.stderr)
    p = argparse.ArgumentParser()
    p.add_argument("--exchange", "-e", default=None)
    p.add_argument("--limit",    "-l", type=int, default=0)
    args = p.parse_args()
    result = run(exchange=args.exchange, limit=args.limit)
    print(result)
