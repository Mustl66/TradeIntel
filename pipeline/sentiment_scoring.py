"""
pipeline/sentiment_scoring.py — Phase 4: Unified Sentiment Engine
==================================================================
Two-stage cascading LLM pipeline per symbol:

  Stage 1 (gemma-4-e2b, fast):
    Cleans raw article text → structured extended_summary + extracted_facts
    Cached in news_articles.pre_summary_data (JSONB) — never re-run if cached.
    Bypassed if ENABLE_PRE_SUMMARIZATION=False or if Stage 1 fails (fallback).

  Stage 2 (main LLM, stateful):
    Reads rolling window of MAX_EVAL_ARTICLES newest articles (oldest→newest).
    Maintains rolling master_summary across the window.
    Outputs: sentiment_score, article_summary, key_events,
             updated_master_summary, forecast_until_earnings, score_rationale.

  Time-decay:
    weighted_sentiment = sentiment_score * exp(-lambda * t_hours)

  After window completes:
    symbols.symbol_master_summary  ← last updated_master_summary
    symbols.symbol_forecast_narrative ← last forecast_until_earnings
    symbols.final_score            ← avg(weighted_sentiment) * MAX(macro_multiplier)
    symbols.score_updated_at       ← NOW()

Usage:
    from pipeline.sentiment_scoring import run
    run(exchange="NASDAQ")          # score all symbols with unscored articles
    run(exchange="NASDAQ", limit=5) # dev test, first 5 symbols
"""

import json
import logging
import math
import time
from datetime import datetime, timezone
from typing import Optional

from openai import OpenAI

from config import LLM_CONFIG, LLM_TYPE
from db.connection import get_conn
from pipeline_config import (
    MAX_EVAL_ARTICLES,
    ENABLE_PRE_SUMMARIZATION,
    SUMMARY_LLM_MODEL,
    SENTIMENT_LAMBDA,
)

logger = logging.getLogger(__name__)

# ── LLM clients ───────────────────────────────────────────────────────────────

def _get_main_client() -> OpenAI:
    return OpenAI(base_url=LLM_CONFIG["base_url"], api_key=LLM_CONFIG["api_key"])


def _get_summary_client() -> OpenAI:
    """Stage 1 client — same endpoint, different model (gemma-4-e2b)."""
    return OpenAI(base_url=LLM_CONFIG["base_url"], api_key=LLM_CONFIG["api_key"])


# ── Stage 1: Pre-summarization prompt ─────────────────────────────────────────

_STAGE1_SYSTEM = """You are a financial news extraction engine. Your ONLY job is to process raw news text and return structured JSON.

Extract all hard business facts. Strip boilerplate, legal disclaimers, and website navigation noise.

Return ONLY valid JSON — no markdown, no explanation:
{
  "extended_summary": "Dense multi-sentence summary preserving ALL event trajectories, market dynamics, financial figures, and operational details.",
  "extracted_facts": {
    "contracts": "Specific contract terms, counter-parties, dollar amounts. Null if none.",
    "patents": "Patent numbers, descriptions, clinical phase clearings. Null if none.",
    "mergers_acquisitions": "Target companies, acquisition stakes, joint-venture intents. Null if none.",
    "sector_and_macro_tags": "Market conditions, supply chain shifts, regulatory changes. Null if none."
  }
}"""


def _call_stage1(client: OpenAI, text: str) -> Optional[dict]:
    """Fast pre-summarization. Returns dict or None on failure."""
    try:
        resp = client.chat.completions.create(
            model=SUMMARY_LLM_MODEL,
            temperature=0.05,
            max_tokens=1024,
            messages=[
                {"role": "system", "content": _STAGE1_SYSTEM},
                {"role": "user",   "content": f"Extract facts from this article:\n\n{text[:3000]}"},
            ],
        )
        raw = resp.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except Exception as e:
        logger.warning(f"[Stage1] Failed: {e}")
        return None


# ── Stage 2: Stateful sentiment prompt ────────────────────────────────────────

_STAGE2_SYSTEM = """You are a professional financial analyst AI. You evaluate news articles in chronological sequence for a specific stock symbol.

You receive:
- A TradingView snapshot with fundamental/technical metrics
- The current article (title + text)
- Previous state: rolling master_summary and last sentiment score

Your tasks:
1. Score the article sentiment from -1.00 (very negative) to +1.00 (very positive)
   - Consider the article in context of the master_summary history
   - Weight operational/fundamental catalysts heavily
   - Ignore routine press releases and promotional fluff
2. Write a 1-sentence article_summary
3. Extract key_events (contracts, patents, mergers, sector_impact) — null if none
4. Update the master_summary: a dense rolling narrative of all significant events seen so far
5. Write a forward-looking forecast until the next earnings date (if known)

Return ONLY valid JSON — no markdown, no explanation:
{
  "sentiment_score": 0.65,
  "article_summary": "One sentence summary.",
  "key_events": {
    "contracts": null,
    "patents": null,
    "mergers": null,
    "sector_impact": null
  },
  "updated_master_summary": "Rolling cumulative narrative of all significant events.",
  "forecast_until_earnings": "Forward outlook based on all evidence seen.",
  "score_rationale": "Brief explanation of the sentiment score."
}"""


def _build_stage2_prompt(
    symbol: str,
    tv_snapshot: dict,
    article: dict,
    master_summary: str,
    last_score: float,
    stage1_result: Optional[dict],
) -> str:
    # Article text: use Stage 1 output if available, else raw text
    if stage1_result:
        text_block = json.dumps(stage1_result, ensure_ascii=False)
    else:
        raw = (article.get("full_text") or article.get("summary") or "")[:3000]
        text_block = raw

    payload = {
        "symbol": symbol,
        "tradingview_snapshot": tv_snapshot,
        "current_article": {
            "title":        article["title"],
            "published_at": article["published_at"].isoformat() if hasattr(article["published_at"], "isoformat") else str(article["published_at"]),
            "text_snippet": text_block,
        },
        "previous_state": {
            "master_summary":    master_summary or "",
            "last_article_score": last_score,
        },
    }
    return json.dumps(payload, ensure_ascii=False)


def _call_stage2(client: OpenAI, prompt: str) -> Optional[dict]:
    """Main sentiment LLM call. Returns parsed dict or None."""
    kwargs = {
        "model":             LLM_CONFIG["model"],
        "temperature":       LLM_CONFIG["temperature"],
        "max_tokens":        min(LLM_CONFIG["max_tokens"], 2048),
        "top_p":             LLM_CONFIG["top_p"],
        "frequency_penalty": LLM_CONFIG["frequency_penalty"],
        "presence_penalty":  LLM_CONFIG["presence_penalty"],
        "messages": [
            {"role": "system", "content": _STAGE2_SYSTEM},
            {"role": "user",   "content": prompt},
        ],
    }
    if LLM_TYPE == "ollama":
        kwargs["extra_body"] = {"top_k": LLM_CONFIG.get("top_k", 40)}
    try:
        resp = client.chat.completions.create(**kwargs)
        raw = resp.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except Exception as e:
        logger.warning(f"[Stage2] Failed: {e}")
        return None


# ── Time-decay ────────────────────────────────────────────────────────────────

def _time_decay(score: float, published_at: datetime, lam: float = SENTIMENT_LAMBDA) -> float:
    now = datetime.now(timezone.utc)
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    t_hours = max(0.0, (now - published_at).total_seconds() / 3600.0)
    return round(score * math.exp(-lam * t_hours), 6)


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_symbols_with_unscored(conn, exchange: str, limit: int) -> list[dict]:
    """Return symbols that have at least one unscored article."""
    with conn.cursor() as cur:
        q = """
            SELECT DISTINCT s.id, s.symbol,
                s.industry, s.market_cap_formatted,
                s.close_price, s.price_change,
                s.price_earnings_ttm, s.price_sales_ratio, s.price_book_ratio,
                s.earnings_per_share_basic_ttm, s.price_earnings_growth_ttm,
                s.total_revenue, s.net_income,
                s.gross_margin, s.operating_margin, s.net_margin,
                s.return_on_equity, s.debt_to_equity, s.current_ratio,
                s.rsi, s.sma200, s.price_52_week_high,
                s.relative_volume_10d_calc, s.average_volume_30d_calc,
                s.earnings_release_date, s.dividend_yield_recent,
                s.number_of_employees,
                s.symbol_master_summary
            FROM symbols s
            WHERE s.exchange = %s
              AND s.status = TRUE
              AND EXISTS (
                  SELECT 1 FROM news_articles na
                  WHERE na.symbol_id = s.id AND na.sentiment_score IS NULL
              )
            ORDER BY s.symbol
        """
        if limit:
            q += f" LIMIT {limit}"
        cur.execute(q, (exchange,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _get_articles_for_symbol(conn, symbol_id: int) -> list[dict]:
    """Get rolling window: newest MAX_EVAL_ARTICLES, returned oldest→newest."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, title, summary, full_text, published_at,
                   sentiment_score, pre_summary_data
            FROM news_articles
            WHERE symbol_id = %s
            ORDER BY published_at DESC
            LIMIT %s
        """, (symbol_id, MAX_EVAL_ARTICLES))
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    # flip: oldest → newest for stateful processing
    return list(reversed(rows))


def _get_macro_multiplier(conn, industry: str) -> float:
    """Get MAX macro_multiplier for the given industry. Default 1.000."""
    if not industry:
        return 1.000
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(MAX(macro_multiplier), 1.000)
            FROM sectors_macro
            WHERE industry_name ILIKE %s
        """, (f"%{industry}%",))
        row = cur.fetchone()
    return float(row[0]) if row else 1.000


def _save_article_result(cur, article_id: int, result: dict,
                         master_snapshot: str, pre_summary: Optional[dict],
                         published_at: datetime):
    score = float(result.get("sentiment_score", 0.0))
    score = max(-1.0, min(1.0, score))
    weighted = _time_decay(score, published_at)

    updates = {
        "sentiment_score":         score,
        "weighted_sentiment":      weighted,
        "article_summary":         (result.get("article_summary") or "")[:500],
        "master_summary_snapshot": master_snapshot,
        "key_events":              json.dumps(result.get("key_events") or {}),
    }
    if pre_summary:
        updates["pre_summary_data"] = json.dumps(pre_summary)

    cur.execute("""
        UPDATE news_articles SET
            sentiment_score         = %(sentiment_score)s,
            weighted_sentiment      = %(weighted_sentiment)s,
            article_summary         = %(article_summary)s,
            master_summary_snapshot = %(master_summary_snapshot)s,
            key_events              = %(key_events)s::jsonb,
            pre_summary_data        = COALESCE(%(pre_summary_data)s::jsonb, pre_summary_data)
        WHERE id = %(id)s
    """, {**updates, "id": article_id,
          "pre_summary_data": json.dumps(pre_summary) if pre_summary else None})


def _save_symbol_scores(cur, symbol_id: int, master_summary: str,
                        forecast: str, weighted_scores: list[float],
                        macro_multiplier: float):
    if not weighted_scores:
        return
    avg_weighted = sum(weighted_scores) / len(weighted_scores)
    final_score  = round(avg_weighted * macro_multiplier, 6)
    cur.execute("""
        UPDATE symbols SET
            symbol_master_summary      = %s,
            symbol_forecast_narrative  = %s,
            final_score                = %s,
            score_updated_at           = NOW()
        WHERE id = %s
    """, (master_summary[:4000] if master_summary else None,
          forecast[:2000] if forecast else None,
          final_score, symbol_id))


# ── Per-symbol processor ──────────────────────────────────────────────────────

def _process_symbol(
    conn,
    main_client: OpenAI,
    summary_client: OpenAI,
    sym: dict,
) -> dict:
    symbol   = sym["symbol"]
    sym_id   = sym["id"]
    industry = sym.get("industry") or ""

    articles = _get_articles_for_symbol(conn, sym_id)
    if not articles:
        return {"symbol": symbol, "scored": 0, "skipped": 0}

    # TradingView snapshot (only non-None fields)
    tv_fields = [
        "close_price", "price_change", "price_earnings_ttm", "price_sales_ratio",
        "price_book_ratio", "earnings_per_share_basic_ttm", "price_earnings_growth_ttm",
        "total_revenue", "net_income", "gross_margin", "operating_margin", "net_margin",
        "return_on_equity", "debt_to_equity", "current_ratio", "rsi", "sma200",
        "price_52_week_high", "relative_volume_10d_calc", "average_volume_30d_calc",
        "earnings_release_date", "dividend_yield_recent", "number_of_employees",
        "industry", "market_cap_formatted",
    ]
    tv_snapshot = {k: sym[k] for k in tv_fields
                   if sym.get(k) is not None}
    if "earnings_release_date" in tv_snapshot and hasattr(tv_snapshot["earnings_release_date"], "isoformat"):
        tv_snapshot["earnings_release_date"] = tv_snapshot["earnings_release_date"].isoformat()

    macro_mult   = _get_macro_multiplier(conn, industry)
    master_summary = sym.get("symbol_master_summary") or ""
    last_score     = 0.0
    weighted_scores = []
    last_forecast  = ""
    scored = 0
    skipped = 0

    for article in articles:
        art_id      = article["id"]
        published_at = article["published_at"]

        # Already scored — use its weighted score for final calc but skip LLM
        if article["sentiment_score"] is not None:
            weighted_scores.append(float(article["sentiment_score"]) *
                                   math.exp(-SENTIMENT_LAMBDA *
                                            max(0, (datetime.now(timezone.utc) - (
                                                published_at if published_at.tzinfo
                                                else published_at.replace(tzinfo=timezone.utc)
                                            )).total_seconds() / 3600)))
            skipped += 1
            continue

        # Stage 1: pre-summarization
        stage1_result = None
        if ENABLE_PRE_SUMMARIZATION:
            # Use cached pre_summary_data if available
            if article.get("pre_summary_data"):
                stage1_result = article["pre_summary_data"]
            else:
                raw_text = (article.get("full_text") or article.get("summary") or "")
                if raw_text.strip():
                    stage1_result = _call_stage1(summary_client, raw_text)

        # Stage 2: stateful scoring
        prompt = _build_stage2_prompt(
            symbol, tv_snapshot, article,
            master_summary, last_score, stage1_result
        )
        result = _call_stage2(main_client, prompt)

        if result is None:
            # Fallback: neutral score, preserve master_summary
            logger.warning(f"[{symbol}] Stage2 failed for article {art_id}, using neutral fallback")
            result = {
                "sentiment_score":       0.0,
                "article_summary":       article["title"][:200],
                "key_events":            {},
                "updated_master_summary": master_summary,
                "forecast_until_earnings": last_forecast,
                "score_rationale":       "LLM fallback — neutral score assigned",
            }

        raw_score = float(result.get("sentiment_score", 0.0))
        raw_score = max(-1.0, min(1.0, raw_score))
        weighted  = _time_decay(raw_score, published_at)

        # Save to DB
        with conn.cursor() as cur:
            _save_article_result(cur, art_id, result,
                                 master_summary, stage1_result, published_at)
        conn.commit()

        # Advance state
        master_summary = result.get("updated_master_summary") or master_summary
        last_forecast  = result.get("forecast_until_earnings") or last_forecast
        last_score     = raw_score
        weighted_scores.append(weighted)
        scored += 1

        time.sleep(0.3)  # brief pause between articles

    # Save symbol-level scores
    if scored > 0:
        with conn.cursor() as cur:
            _save_symbol_scores(cur, sym_id, master_summary,
                                last_forecast, weighted_scores, macro_mult)
        conn.commit()

    logger.info(f"[{symbol}] scored={scored} skipped={skipped} "
                f"macro_mult={macro_mult:.3f} final_score="
                f"{round(sum(weighted_scores)/len(weighted_scores)*macro_mult, 4) if weighted_scores else 'N/A'}")
    return {"symbol": symbol, "scored": scored, "skipped": skipped,
            "macro_multiplier": macro_mult}


# ── Public run() ─────────────────────────────────────────────────────────────

def run(exchange: str = "NASDAQ", limit: int = 0) -> dict:
    """
    Score all symbols with unscored articles.
    limit=0 → all symbols. limit=N → first N symbols only.
    """
    started_at = datetime.now(timezone.utc)
    conn = get_conn()

    symbols = _get_symbols_with_unscored(conn, exchange, limit)
    if not symbols:
        logger.info("[sentiment_scoring] No symbols with unscored articles.")
        conn.close()
        return {"symbols_processed": 0, "articles_scored": 0}

    logger.info(f"[sentiment_scoring] {len(symbols)} symbols to score "
                f"(pre_summarization={'ON' if ENABLE_PRE_SUMMARIZATION else 'OFF'})")

    main_client    = _get_main_client()
    summary_client = _get_summary_client()

    total_scored = 0
    results = []
    for sym in symbols:
        try:
            r = _process_symbol(conn, main_client, summary_client, sym)
            results.append(r)
            total_scored += r["scored"]
        except Exception as e:
            logger.error(f"[{sym['symbol']}] Unhandled error: {e}", exc_info=True)

    conn.close()
    duration = (datetime.now(timezone.utc) - started_at).total_seconds()
    logger.info(f"[sentiment_scoring] Done — symbols={len(results)}, "
                f"articles_scored={total_scored}, duration={duration:.1f}s")
    return {
        "symbols_processed": len(results),
        "articles_scored":   total_scored,
        "duration_s":        round(duration, 1),
    }


# ── Single-article fast path (used by Worker 1) ───────────────────────────────

def score_single_article(article_id: int, symbol_id: int) -> bool:
    """
    Score one article immediately using the last saved master_summary.
    Used by the GlobeNewswire live tracker (Worker 1).
    Returns True on success.
    """
    conn = get_conn()
    try:
        # Load symbol
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, symbol, industry, market_cap_formatted,
                       close_price, price_change, price_earnings_ttm,
                       earnings_release_date, rsi, sma200,
                       symbol_master_summary
                FROM symbols WHERE id = %s
            """, (symbol_id,))
            row = cur.fetchone()
            if not row:
                return False
            cols = [d[0] for d in cur.description]
            sym = dict(zip(cols, row))

        # Load article
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, title, summary, full_text, published_at,
                       sentiment_score, pre_summary_data
                FROM news_articles WHERE id = %s
            """, (article_id,))
            row = cur.fetchone()
            if not row:
                return False
            cols = [d[0] for d in cur.description]
            article = dict(zip(cols, row))

        if article["sentiment_score"] is not None:
            return True  # already scored

        main_client    = _get_main_client()
        summary_client = _get_summary_client()
        macro_mult     = _get_macro_multiplier(conn, sym.get("industry") or "")
        master_summary = sym.get("symbol_master_summary") or ""

        tv_snapshot = {k: sym[k] for k in
                       ["close_price", "price_change", "price_earnings_ttm",
                        "rsi", "sma200", "earnings_release_date", "industry",
                        "market_cap_formatted"]
                       if sym.get(k) is not None}

        stage1_result = None
        if ENABLE_PRE_SUMMARIZATION:
            raw_text = (article.get("full_text") or article.get("summary") or "")
            if raw_text.strip():
                stage1_result = _call_stage1(summary_client, raw_text)

        prompt = _build_stage2_prompt(
            sym["symbol"], tv_snapshot, article,
            master_summary, 0.0, stage1_result
        )
        result = _call_stage2(main_client, prompt)
        if result is None:
            return False

        with conn.cursor() as cur:
            _save_article_result(cur, article_id, result,
                                 master_summary, stage1_result, article["published_at"])
            # Update symbol master_summary
            new_master = result.get("updated_master_summary") or master_summary
            cur.execute("""
                UPDATE symbols SET
                    symbol_master_summary     = %s,
                    symbol_forecast_narrative = %s,
                    score_updated_at          = NOW()
                WHERE id = %s
            """, (new_master[:4000], result.get("forecast_until_earnings", "")[:2000], symbol_id))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"[score_single_article] Error: {e}", exc_info=True)
        return False
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse, sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        stream=sys.stderr,
    )
    p = argparse.ArgumentParser()
    p.add_argument("--exchange", default="NASDAQ")
    p.add_argument("--limit", "-l", type=int, default=0)
    args = p.parse_args()
    print(run(exchange=args.exchange, limit=args.limit))
