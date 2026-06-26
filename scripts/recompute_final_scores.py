"""
Recompute symbols.final_score using the NEW weighted-mean aggregation
with asymptotic multiplier + gravity dampening.

Reads existing news_articles.sentiment_score + published_at; never calls the LLM.
Uses ai_sector_multiplier already stored on each symbol (from the last full run).
Resolves macro_multiplier from sectors_macro by industry, just like the pipeline.

Formula:
    w_i          = decay_weight(published_at_i)                   # in [0, 1]
    weighted_avg = sum(raw_i * w_i) / sum(w_i)                    # true weighted mean
                   (only articles with |raw| >= MATERIALITY_THRESHOLD included)
    after_mult   = asymptotic_multiply(weighted_avg, macro*ai_mult)
    final_score  = gravity(after_mult, newest_material_pub)        # inactivity penalty

Idempotent: safe to re-run. Prints a per-symbol before/after diff.
Use --dry-run to preview without writing.
"""
from __future__ import annotations
import argparse
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

# Make project root importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2
import psycopg2.extras
from config import DB_CONFIG
from pipeline_config import (
    SENTIMENT_LAMBDA, DECAY_GRACE_MONTHS,
    NEUTRAL_SCORE_THRESHOLD, MATERIALITY_THRESHOLD,
    GRAVITY_GAMMA, GRAVITY_GRACE_DAYS, ASYMPTOTE_THRESHOLD,
)


GRACE_HOURS = DECAY_GRACE_MONTHS * 30.44 * 24


def decay_weight(published_at: datetime, now: datetime) -> float:
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    age_h = max(0.0, (now - published_at).total_seconds() / 3600.0)
    if age_h <= GRACE_HOURS:
        return 1.0
    return math.exp(-SENTIMENT_LAMBDA * (age_h - GRACE_HOURS))


def apply_asymptotic_multiplier(base: float, multiplier: float,
                                threshold: float = ASYMPTOTE_THRESHOLD) -> float:
    if base == 0.0:
        return 0.0
    sign = 1.0 if base > 0.0 else -1.0
    abs_base = abs(base)
    if abs_base <= threshold:
        return max(-1.0, min(1.0, base * multiplier))
    m = max(0.5, min(multiplier, 2.0))
    headroom = 1.0 - abs_base
    compressed = 1.0 - headroom * (2.0 - m)
    return sign * max(abs_base, min(1.0, compressed))


def apply_gravity(score: float, newest_pub: datetime | None, now: datetime) -> float:
    if not newest_pub or GRAVITY_GAMMA == 0.0 or score == 0.0:
        return score
    pub = newest_pub
    if pub.tzinfo is None:
        pub = pub.replace(tzinfo=timezone.utc)
    age_h = max(0.0, (now - pub).total_seconds() / 3600.0)
    grace_h = GRAVITY_GRACE_DAYS * 24.0
    if age_h <= grace_h:
        return score
    return round(score * math.exp(-GRAVITY_GAMMA * (age_h - grace_h)), 6)


def get_macro_mult(cur, industry: str) -> float:
    if not industry:
        return 1.000
    cur.execute(
        "SELECT COALESCE(MAX(macro_multiplier), 1.000) FROM sectors_macro "
        "WHERE industry_name ILIKE %s",
        (f"%{industry}%",),
    )
    row = cur.fetchone()
    return float(row[0]) if row else 1.000


def main(dry_run: bool, verbose: bool):
    now = datetime.now(timezone.utc)
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    updated = skipped_no_articles = 0
    diffs: list[tuple[str, float, float]] = []  # (symbol, old, new)

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, symbol, industry,
                       COALESCE(final_score, 0)::float        AS old_final,
                       COALESCE(ai_sector_multiplier, 1.0)::float AS ai_mult
                FROM symbols
                WHERE status = TRUE
                ORDER BY symbol
            """)
            symbols = cur.fetchall()

        for s in symbols:
            sid = s["id"]
            symbol = s["symbol"]
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT sentiment_score::float AS raw, published_at
                    FROM news_articles
                    WHERE symbol_id = %s
                      AND sentiment_score IS NOT NULL
                      AND published_at IS NOT NULL
                """, (sid,))
                rows = cur.fetchall()

            if not rows:
                skipped_no_articles += 1
                continue

            num = 0.0
            den = 0.0
            n_used = 0
            n_skipped = 0
            newest_material_pub = None
            for r in rows:
                if abs(r["raw"]) < MATERIALITY_THRESHOLD:
                    n_skipped += 1
                    continue
                w = decay_weight(r["published_at"], now)
                num += r["raw"] * w
                den += w
                n_used += 1
                pub = r["published_at"]
                if newest_material_pub is None or pub > newest_material_pub:
                    newest_material_pub = pub

            if den <= 0:
                skipped_no_articles += 1
                continue

            avg_weighted = num / den

            with conn.cursor() as cur:
                macro_mult = get_macro_mult(cur, s["industry"] or "")
            combined_mult = macro_mult * s["ai_mult"]
            after_mult = apply_asymptotic_multiplier(avg_weighted, combined_mult)
            after_gravity = apply_gravity(after_mult, newest_material_pub, now)
            new_final = round(max(-1.0, min(1.0, after_gravity)), 6)

            diffs.append((symbol, s["old_final"], new_final))

            if verbose:
                g_str = f" gravity→{after_gravity:+.4f}" if after_gravity != after_mult else ""
                print(f"{symbol:>6}  old={s['old_final']:+.4f}  new={new_final:+.4f}  "
                      f"Δ={new_final - s['old_final']:+.4f}  "
                      f"avg={avg_weighted:+.4f}  mult×{combined_mult:.3f}→{after_mult:+.4f}"
                      f"{g_str}  used={n_used} skip={n_skipped}")

            if not dry_run:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE symbols SET final_score = %s, score_updated_at = NOW() WHERE id = %s",
                        (new_final, sid),
                    )
            updated += 1

        if not dry_run:
            conn.commit()
        else:
            conn.rollback()

    finally:
        conn.close()

    # Summary
    diffs.sort(key=lambda x: abs(x[2] - x[1]), reverse=True)
    print("\n" + "=" * 60)
    print(f"Symbols recomputed: {updated}")
    print(f"Skipped (no scored articles): {skipped_no_articles}")
    print(f"Materiality threshold: |raw| >= {MATERIALITY_THRESHOLD}")
    print(f"Gravity gamma: {GRAVITY_GAMMA}/hr  grace: {GRAVITY_GRACE_DAYS}d")
    print(f"Asymptote threshold: {ASYMPTOTE_THRESHOLD}")
    print(f"{'DRY RUN — no writes' if dry_run else 'WRITTEN to symbols.final_score'}")
    print("=" * 60)

    if diffs:
        moved = [(s, o, n) for s, o, n in diffs if abs(n - o) > 0.01]
        print(f"\nTop 20 biggest swings (|Δ| > 0.01):  {len(moved)} symbols changed by more than 0.01")
        print(f"{'SYM':>6}  {'OLD':>10}  {'NEW':>10}  {'Δ':>10}")
        for sym, old, new in diffs[:20]:
            print(f"{sym:>6}  {old:+10.4f}  {new:+10.4f}  {new-old:+10.4f}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Recompute symbols.final_score with asymptotic + gravity math.")
    p.add_argument("--dry-run", action="store_true", help="Preview without writing.")
    p.add_argument("-v", "--verbose", action="store_true", help="Per-symbol log line.")
    args = p.parse_args()
    main(args.dry_run, args.verbose)
