"""
pipeline/macro_multiplier.py — Phase 3: LLM-Driven Macro Multiplier Extraction
================================================================================
Reads unprocessed market_research_articles, sends batches to the configured
LLM, extracts sector/industry growth signals, and updates sectors_macro.macro_multiplier.

Multiplier scale:
    1.000 = neutral / no signal
    1.010 = mild positive outlook
    1.025 = moderate growth forecast
    1.040 = strong growth indicators
    1.050 = exceptional — top 5% forecasts only (rare)

LLM is prompted to return strict JSON. Each article may map to one or more
industry tags (e.g. "AI Semiconductors", "GLP-1 Drugs", "Cybersecurity").

Usage:
    python -m pipeline.macro_multiplier             # process all pending
    python -m pipeline.macro_multiplier --limit 50  # dev/test batch
    python -m pipeline.macro_multiplier --dry-run   # print JSON, no DB write
"""

import json
import logging
import time
from datetime import datetime, timezone

from openai import OpenAI

from config import LLM_CONFIG, LLM_TYPE
from db.connection import get_connection

logger = logging.getLogger(__name__)

# ── LLM client ────────────────────────────────────────────────────────────────

def _get_client() -> OpenAI:
    return OpenAI(
        base_url=LLM_CONFIG["base_url"],
        api_key=LLM_CONFIG["api_key"],
    )


# ── Prompt ────────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """You are a financial market analyst specializing in macro sector growth forecasting.

Your task: analyze market research article titles and summaries, then extract structured growth signals.

For each article, identify ALL relevant industries, niches, products, technologies, diseases, medications,
or market segments mentioned. Assign each a growth_score from 0.0 to 1.0 based on the described
market outlook, CAGR forecasts, adoption curves, or analyst projections.

SCORING GUIDE:
  0.0 - 0.2 : Declining, shrinking, negative outlook
  0.2 - 0.4 : Flat, saturated, marginal growth
  0.4 - 0.6 : Moderate growth (5-10% CAGR)
  0.6 - 0.8 : Strong growth (10-20% CAGR, or large TAM expansion)
  0.8 - 1.0 : Exceptional growth (>20% CAGR, disruptive, transformative)

Be STRICT — 0.9+ should be rare. Reserve 1.0 for only the most extraordinary forecasts.

Return ONLY valid JSON in this exact format — no markdown, no explanation:
{
  "signals": [
    {
      "sector": "Healthcare",
      "industry": "GLP-1 Obesity Drugs",
      "growth_score": 0.87,
      "rationale": "Global GLP-1 market projected $130B by 2030, 24% CAGR",
      "year_horizon": 2030
    }
  ]
}

If no clear growth signal exists in the article, return: {"signals": []}
"""


def _build_user_prompt(articles: list[dict]) -> str:
    lines = []
    for i, a in enumerate(articles, 1):
        lines.append(f"[{i}] TITLE: {a['title']}")
        if a.get("summary"):
            lines.append(f"     SUMMARY: {a['summary'][:1000]}")
    return "\n".join(lines)


# ── Score → multiplier conversion ─────────────────────────────────────────────

def _score_to_multiplier(growth_score: float) -> float:
    """
    Map 0.0-1.0 growth_score to 1.000-1.050 multiplier.
    5% max premium. Curve is convex — high scores compress near top.
    """
    score = max(0.0, min(1.0, growth_score))
    # Only positive signals get a premium
    if score <= 0.4:
        return 1.000
    # Linear mapping from [0.4, 1.0] → [1.000, 1.050]
    premium = ((score - 0.4) / 0.6) * 0.050
    return round(1.000 + premium, 3)


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_pending_articles(limit: int = 0) -> list[dict]:
    conn = get_connection()
    with conn.cursor() as cur:
        q = """
            SELECT id, title, summary, source_name
            FROM market_research_articles
            WHERE llm_processed = FALSE
            ORDER BY published_at DESC
        """
        if limit:
            q += f" LIMIT {limit}"
        cur.execute(q)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()
    return rows


def _update_multiplier(cur, sector: str, industry: str,
                        new_score: float, rationale: str) -> bool:
    """
    Update sectors_macro multiplier — only increases, never decreases.
    This prevents one weak article from overwriting a strong established signal.
    Returns True if updated.
    """
    new_mult = _score_to_multiplier(new_score)
    cur.execute("""
        INSERT INTO sectors_macro
            (sector_name, industry_name, macro_multiplier, rationale, last_llm_run_at, updated_at)
        VALUES (%s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (sector_name, industry_name) DO UPDATE
            SET macro_multiplier = GREATEST(sectors_macro.macro_multiplier, EXCLUDED.macro_multiplier),
                rationale        = CASE
                    WHEN EXCLUDED.macro_multiplier >= sectors_macro.macro_multiplier
                    THEN EXCLUDED.rationale
                    ELSE sectors_macro.rationale
                END,
                last_llm_run_at  = NOW(),
                updated_at       = NOW()
        RETURNING (xmax = 0) AS was_inserted
    """, (sector, industry, new_mult, rationale))
    return True


def _mark_processed(cur, article_ids: list[int]):
    cur.execute("""
        UPDATE market_research_articles
        SET llm_processed = TRUE
        WHERE id = ANY(%s)
    """, (article_ids,))


# ── LLM call ─────────────────────────────────────────────────────────────────

def _call_llm(client: OpenAI, articles: list[dict]) -> list[dict]:
    """Call LLM with a batch of articles. Returns list of signal dicts."""
    prompt = _build_user_prompt(articles)
    kwargs = {
        "model":             LLM_CONFIG["model"],
        "temperature":       LLM_CONFIG["temperature"],
        "max_tokens":        LLM_CONFIG["max_tokens"],
        "top_p":             LLM_CONFIG["top_p"],
        "frequency_penalty": LLM_CONFIG["frequency_penalty"],
        "presence_penalty":  LLM_CONFIG["presence_penalty"],
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
    }
    # top_k only supported by some providers
    if LLM_TYPE == "ollama":
        kwargs["extra_body"] = {"top_k": LLM_CONFIG.get("top_k", 40)}

    try:
        response = client.chat.completions.create(**kwargs)
        raw = response.choices[0].message.content.strip()
        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        data = json.loads(raw)
        return data.get("signals", [])
    except json.JSONDecodeError as e:
        logger.warning(f"LLM returned invalid JSON: {e}")
        return []
    except Exception as e:
        logger.error(f"LLM call failed: {e}")
        return []


# ── Main orchestrator ─────────────────────────────────────────────────────────

BATCH_SIZE = 1   # articles per LLM call


def run(limit: int = 0, dry_run: bool = False) -> dict:
    """
    Process all pending market research articles through LLM.
    Updates sectors_macro multipliers.
    """
    started_at = datetime.now(timezone.utc)
    articles = _get_pending_articles(limit)

    if not articles:
        logger.info("No pending market research articles to process.")
        return {"processed": 0, "signals": 0, "industries_updated": 0}

    logger.info(f"Processing {len(articles)} articles through LLM ({LLM_TYPE}: {LLM_CONFIG['model']})...")
    client = _get_client()

    total_signals    = 0
    industries_updated = 0
    processed_ids    = []

    conn = get_connection()

    for i in range(0, len(articles), BATCH_SIZE):
        batch = articles[i:i + BATCH_SIZE]
        logger.info(f"LLM batch {i//BATCH_SIZE + 1}/{(len(articles)-1)//BATCH_SIZE + 1} "
                    f"({len(batch)} articles)...")

        signals = _call_llm(client, batch)
        total_signals += len(signals)

        if dry_run:
            for s in signals:
                mult = _score_to_multiplier(s.get("growth_score", 0))
                print(f"  {s.get('sector')} / {s.get('industry')} "
                      f"score={s.get('growth_score')} → mult={mult} | {s.get('rationale','')[:80]}")
        else:
            with conn.cursor() as cur:
                for s in signals:
                    sector   = (s.get("sector")   or "").strip()
                    industry = (s.get("industry") or "").strip()
                    score    = float(s.get("growth_score", 0))
                    rationale = s.get("rationale", "")[:500]
                    if not sector or not industry:
                        continue
                    _update_multiplier(cur, sector, industry, score, rationale)
                    industries_updated += 1

                batch_ids = [a["id"] for a in batch]
                _mark_processed(cur, batch_ids)
                processed_ids.extend(batch_ids)
            conn.commit()

        time.sleep(1.0)   # rate limit between batches

    conn.close()

    duration = (datetime.now(timezone.utc) - started_at).total_seconds()
    logger.info(
        f"Macro multiplier run complete — "
        f"processed={len(processed_ids)}, signals={total_signals}, "
        f"industries_updated={industries_updated}, duration={duration:.1f}s"
    )
    return {
        "processed":          len(processed_ids),
        "signals":            total_signals,
        "industries_updated": industries_updated,
        "duration_s":         round(duration, 1),
    }


if __name__ == "__main__":
    import argparse, sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
                        stream=sys.stderr)
    p = argparse.ArgumentParser()
    p.add_argument("--limit",   "-l", type=int, default=0)
    p.add_argument("--dry-run", "-d", action="store_true",
                   help="Print signals, do not write to DB")
    args = p.parse_args()
    print(run(limit=args.limit, dry_run=args.dry_run))
