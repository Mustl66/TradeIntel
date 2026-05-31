"""
pipeline/sector_map.py — Phase 3: Sector & Industry Mapping + TV Data Refresh
=============================================================
Fetches sector/industry + all TradingView metrics for every symbol.
Inserts new industries into sectors_macro (multiplier=1.000 default),
maps sector_id back onto every symbols row, updates all TV metric columns,
and saves one daily snapshot per symbol into symbol_daily_snapshots.

Usage:
    python -m pipeline.sector_map                  # all exchanges
    python -m pipeline.sector_map --exchange NASDAQ
    python -m pipeline.sector_map --limit 50       # dev/test
"""

import json
import logging
from datetime import datetime, timezone, date

from tradingview_screener import Query
from db.connection import get_connection

logger = logging.getLogger(__name__)

# ── TradingView field names ───────────────────────────────────────────────────
TV_SECTOR   = "sector"
TV_INDUSTRY = "industry"

# TV field → DB column mapping for metrics
# NOTE: only include fields verified valid by TV screener API
TV_METRIC_FIELDS: dict[str, str] = {
    "close":                          "close_price",
    "change":                         "price_change",
    "price_earnings_ttm":             "price_earnings_ttm",
    "price_book_fq":                  "price_book_ratio",
    "earnings_per_share_diluted_ttm": "earnings_per_share_basic_ttm",
    "price_earnings_growth_ttm":      "price_earnings_growth_ttm",
    "total_revenue":                  "total_revenue",
    "net_income":                     "net_income",
    "gross_margin":                   "gross_margin",
    "operating_margin":               "operating_margin",
    "net_margin":                     "net_margin",
    "return_on_equity":               "return_on_equity",
    "debt_to_equity":                 "debt_to_equity",
    "current_ratio_mrq":              "current_ratio",
    "RSI":                            "rsi",
    "SMA200":                         "sma200",
    "High.52W":                       "price_52_week_high",
    "relative_volume_10d_calc":       "relative_volume_10d_calc",
    "average_volume_30d_calc":        "average_volume_30d_calc",
    "earnings_release_next_date":     "earnings_release_date",
    "Dividends.Yield.Current":        "dividend_yield_recent",
    "number_of_employees":            "number_of_employees",
    "market_cap_calc":                "market_cap_formatted",
}

ALL_TV_FIELDS = [TV_SECTOR, TV_INDUSTRY] + list(TV_METRIC_FIELDS.keys())


# ── Helpers ───────────────────────────────────────────────────────────────────

def _probe_valid_fields() -> list[str]:
    """
    Auto-detect which fields TV accepts. Start with ALL_TV_FIELDS, strip any
    field that causes a 400 error, retry until the request succeeds or no
    fields remain. Returns the validated field list.
    """
    fields = list(ALL_TV_FIELDS)
    while fields:
        try:
            Query().select(*fields).limit(1).get_scanner_data()
            logger.info(f"TV field probe OK — {len(fields)} valid fields")
            return fields
        except Exception as e:
            msg = str(e)
            # Extract the bad field name from TV error body
            import re
            m = re.search(r'Unknown field \\"([^\\"]+)\\"', msg)
            if m:
                bad = m.group(1)
                logger.warning(f"TV field rejected: '{bad}' — removing and retrying")
                fields = [f for f in fields if f != bad]
            else:
                # Unknown error format — abort probe
                logger.error(f"TV field probe failed with unexpected error: {e}")
                return []
    logger.error("TV field probe: no valid fields remain")
    return []


def _fetch_tv_data(symbols: list[str]) -> dict[str, dict]:
    """
    Query TradingView Screener for sector, industry, and all metrics.
    ONE request — fetch all US stocks, filter locally against our symbol set.
    Auto-probes valid fields before fetching so bad field names never block the run.
    Returns {ticker: {field: value, ...}}.
    """
    want = set(symbols)
    result = {}
    try:
        valid_fields = _probe_valid_fields()
        if not valid_fields:
            logger.error("No valid TV fields found — skipping TV fetch")
            return result
        _, df = (
            Query()
            .select(*valid_fields)
            .limit(25000)
            .get_scanner_data()
        )
        for _, row in df.iterrows():
            ticker = row.get("name") or row.get("ticker", "")
            if ":" in ticker:
                ticker = ticker.split(":", 1)[1]
            if ticker and ticker in want:
                result[ticker] = {col: row.get(col) for col in valid_fields}
    except Exception as e:
        logger.error(f"TradingView fetch failed: {e}")

    return result


def _upsert_industry(cur, sector: str, industry: str) -> int:
    """
    Insert industry if not present. Returns sectors_macro.id.
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
    cur.execute(
        "SELECT id FROM sectors_macro WHERE sector_name=%s AND industry_name=%s",
        (sector, industry)
    )
    return cur.fetchone()[0]


def _safe_numeric(v):
    """Convert TV value to float, or None."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _safe_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _safe_date(v):
    """Parse TV earnings date to date string or None."""
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)):
            # TV sometimes returns Unix timestamp in ms
            return datetime.fromtimestamp(v / 1000, tz=timezone.utc).isoformat()
        return str(v)
    except Exception:
        return None


def _sanitize_for_json(obj):
    """Recursively replace float NaN/Inf with None so PostgreSQL JSONB accepts it."""
    import math
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    return obj


def _upsert_daily_snapshot(cur, symbol_id: int, snap_date: date, data: dict):
    """Save/update daily TV snapshot (one row per symbol per day)."""
    # Clean: only store non-None values, sanitize NaN/Inf
    clean = _sanitize_for_json({k: v for k, v in data.items() if v is not None})
    cur.execute("""
        INSERT INTO symbol_daily_snapshots (symbol_id, snapshot_date, data)
        VALUES (%s, %s, %s::jsonb)
        ON CONFLICT (symbol_id, snapshot_date) DO UPDATE
            SET data = EXCLUDED.data, created_at = NOW()
    """, (symbol_id, snap_date, json.dumps(clean)))


def _get_all_symbols(cur, exchange: str | None, limit: int) -> list[dict]:
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
    Full sector mapping + TV data refresh run.
    """
    started_at = datetime.now(timezone.utc)
    today      = started_at.date()
    conn       = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS symbol_daily_snapshots (
                id            SERIAL PRIMARY KEY,
                symbol_id     INTEGER NOT NULL REFERENCES symbols(id) ON DELETE CASCADE,
                snapshot_date DATE    NOT NULL,
                data          JSONB   NOT NULL DEFAULT '{}',
                created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (symbol_id, snapshot_date)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_sds_symbol_date
            ON symbol_daily_snapshots (symbol_id, snapshot_date DESC)
        """)
        cur.execute("""
            INSERT INTO pipeline_runs (step, exchange, started_at, status)
            VALUES ('sector_map', %s, NOW(), 'running') RETURNING id
        """, (exchange or "ALL",))
        run_id = cur.fetchone()[0]
    conn.commit()

    stats = {"mapped": 0, "new_industries": 0, "missing": 0, "snapshots": 0, "duration_s": 0}

    try:
        with conn.cursor() as cur:
            symbols = _get_all_symbols(cur, exchange, limit)

        if not symbols:
            logger.warning("No symbols found to map.")
            return stats

        logger.info(f"Fetching TV data for {len(symbols)} symbols...")
        tickers  = [s["symbol"] for s in symbols]
        tv_data  = _fetch_tv_data(tickers)
        logger.info(f"TradingView returned data for {len(tv_data)} tickers.")

        sym_by_ticker = {s["symbol"]: s["id"] for s in symbols}

        with conn.cursor() as cur:
            for ticker, info in tv_data.items():
                sector   = (info.get(TV_SECTOR) or "").strip()
                industry = (info.get(TV_INDUSTRY) or "").strip()
                sym_id   = sym_by_ticker.get(ticker)
                if not sym_id:
                    continue

                # ── sector_id + industry column ───────────────────────────────
                sector_id = None
                if sector and industry:
                    sector_id = _upsert_industry(cur, sector, industry)

                # Build metric update dict
                metric_updates = {}
                for tv_col, db_col in TV_METRIC_FIELDS.items():
                    val = info.get(tv_col)
                    if db_col == "market_cap_formatted":
                        metric_updates[db_col] = str(val)[:50] if val is not None else None
                    elif db_col in ("number_of_employees",):
                        metric_updates[db_col] = _safe_int(val)
                    elif db_col == "earnings_release_date":
                        metric_updates[db_col] = _safe_date(val)
                    else:
                        metric_updates[db_col] = _safe_numeric(val)

                # Dynamic UPDATE for metrics
                set_parts = ["last_updated_at = NOW()"]
                vals = []
                if industry:
                    set_parts.append("industry = %s")
                    vals.append(industry)
                if sector_id:
                    set_parts.append("sector_id = %s")
                    vals.append(sector_id)
                for db_col, val in metric_updates.items():
                    if val is not None:
                        set_parts.append(f"{db_col} = %s")
                        vals.append(val)

                vals.append(sym_id)
                cur.execute(
                    f"UPDATE symbols SET {', '.join(set_parts)} WHERE id = %s",
                    vals
                )
                stats["mapped"] += 1

                # ── Daily snapshot ─────────────────────────────────────────────
                snap_data = {
                    "sector": sector, "industry": industry,
                    **{db_col: val for db_col, val in metric_updates.items() if val is not None}
                }
                _upsert_daily_snapshot(cur, sym_id, today, snap_data)
                stats["snapshots"] += 1

            conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM sectors_macro WHERE last_llm_run_at IS NULL")
            stats["new_industries"] = cur.fetchone()[0]

        stats["missing"] = len(symbols) - len(tv_data)
        logger.info(
            f"Sector map complete — mapped={stats['mapped']}, "
            f"snapshots={stats['snapshots']}, new_industries={stats['new_industries']}, "
            f"missing={stats['missing']}"
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
                json.dumps({"new_industries": stats["new_industries"],
                            "missing": stats["missing"],
                            "snapshots": stats["snapshots"]}),
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
