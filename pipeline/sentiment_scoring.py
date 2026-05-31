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
from decimal import Decimal
import math
import platform
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from openai import OpenAI

from config import LLM_CONFIG, LLM_TYPE, GPU_VRAM_GB
from db.connection import get_conn
from pipeline_config import (  # noqa — patched below
    MAX_EVAL_ARTICLES,
    ENABLE_PRE_SUMMARIZATION,
    SUMMARY_LLM_MODEL,
    SENTIMENT_LAMBDA,
    DECAY_GRACE_MONTHS,
)

# ── JSON helper — handles Decimal / NaN from psycopg2 ─────────────────────────

class _SafeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)

    def iterencode(self, o, _one_shot=False):
        # replace NaN/Inf floats with None before encoding
        def _clean(v):
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return None
            if isinstance(v, dict):
                return {k: _clean(x) for k, x in v.items()}
            if isinstance(v, list):
                return [_clean(x) for x in v]
            return v
        return super().iterencode(_clean(o), _one_shot)


def _json_dumps(obj, **kwargs):
    return json.dumps(obj, cls=_SafeEncoder, ensure_ascii=False, **kwargs)


# ── Load instruction JSONs ────────────────────────────────────────────────────

_CONFIG_DIR = Path(__file__).parent.parent / "config"


def _load_instruction(filename: str) -> str:
    """Load a JSON instruction file and return its content as a formatted string."""
    path = _CONFIG_DIR / filename
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return json.dumps(data, indent=2, ensure_ascii=False)
    except Exception as e:
        logging.getLogger(__name__).error(f"[instructions] Failed to load {filename}: {e}")
        return "{}"


_STAGE1_INSTRUCTION_JSON = _load_instruction("stage1_instruction.json")
_STAGE2_INSTRUCTION_JSON = _load_instruction("stage2_instruction.json")

logger = logging.getLogger(__name__)

# ── LLM clients ───────────────────────────────────────────────────────────────

def _get_main_client() -> OpenAI:
    return OpenAI(base_url=LLM_CONFIG["base_url"], api_key=LLM_CONFIG["api_key"])


def _get_summary_client() -> OpenAI:
    """Stage 1 client — same endpoint, different model (gemma-4-e2b)."""
    return OpenAI(base_url=LLM_CONFIG["base_url"], api_key=LLM_CONFIG["api_key"])


def _warmup_models(main_client: OpenAI, summary_client: OpenAI) -> None:
    """Send a tiny request to both models so LM Studio loads both into VRAM before processing starts.
    Skipped for API mode — wastes tokens and the model is always available remotely."""
    if LLM_TYPE in ("api", "anthropic"):
        logger.info("[warmup] API mode — skipping warmup (no local model to load)")
        return
    for client, model, label in [
        (summary_client, SUMMARY_LLM_MODEL,    "summary (e2b)"),
        (main_client,    LLM_CONFIG["model"],  "main (e4b)"),
    ]:
        try:
            client.chat.completions.create(
                model=model,
                max_tokens=1,
                messages=[{"role": "user", "content": "hi"}],
            )
            logger.info(f"[warmup] {label} loaded OK")
        except Exception as e:
            logger.warning(f"[warmup] {label} warmup failed (non-fatal): {e}")


# ── Stage 1: Pre-summarization prompt ─────────────────────────────────────────

_STAGE1_SYSTEM = (
    "You are a financial news extraction engine operating under the following instruction schema.\n\n"
    + _STAGE1_INSTRUCTION_JSON
    + "\n\nReturn ONLY valid JSON matching the output_schema above. No markdown, no explanation."
)


def _extract_json(raw: str) -> Optional[dict]:
    """Robust JSON extraction: strip markdown fences, find outermost { }, parse."""
    from json_repair import repair_json

    raw = raw.strip()

    # strip ```json ... ``` or ``` ... ```
    if raw.startswith("```"):
        inner = raw.split("```")
        for part in inner[1:]:
            candidate = part.lstrip("json").lstrip("\n").strip()
            if candidate.startswith("{"):
                raw = candidate
                break

    # find outermost { ... } — skip if no closing } (truncated output)
    start = raw.find("{")
    end   = raw.rfind("}")
    if start != -1 and end != -1 and end > start:
        raw = raw[start:end + 1]
    elif start != -1:
        # truncated: no closing } — take from { onward and let json_repair fix it
        raw = raw[start:]

    # pass 1: standard json.loads (fast path)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # pass 2: json_repair as string -> json.loads (handles truncation, open strings, trailing commas)
    try:
        repaired_str = repair_json(raw)
        if repaired_str:
            parsed = json.loads(repaired_str)
            if isinstance(parsed, dict) and parsed:
                return parsed
    except Exception:
        pass

    # pass 3: json_repair with return_objects=True (direct dict, no re-parse)
    try:
        repaired = repair_json(raw, return_objects=True)
        if isinstance(repaired, dict) and repaired:
            return repaired
    except Exception:
        pass

    # pass 4: manual brace-patch then json.loads (last resort)
    try:
        opens  = raw.count("{") - raw.count("}")
        aopens = raw.count("[") - raw.count("]")
        patched = raw.rstrip(",\n\r\t ") + ("]" * max(0, aopens)) + ("}" * max(0, opens))
        return json.loads(patched)
    except Exception:
        pass

    logger.warning(f"[_extract_json] All parse attempts failed. Raw (first 400): {raw[:400]!r}")
    return None


def _call_stage1(client: OpenAI, text: str, model_override: str = None) -> Optional[dict]:
    """Fast pre-summarization. Returns dict or None on failure."""
    model = model_override or SUMMARY_LLM_MODEL
    try:
        kwargs1 = dict(
            model=model,
            temperature=0.05,
            max_tokens=4096,          # was 2048 — too small, caused truncated JSON
            messages=[
                {"role": "system", "content": _STAGE1_SYSTEM},
                {"role": "user",   "content": f"Extract facts from this article:\n\n{text[:6000]}"},
            ],
        )
        if LLM_TYPE == "ollama":
            kwargs1["extra_body"] = {
                "num_ctx": LLM_CONFIG.get("context_size", 16384),
                "top_k":   LLM_CONFIG.get("top_k", 40),
                "format":  "json",
            }
        elif LLM_TYPE in ("api", "anthropic"):
            kwargs1["response_format"] = {"type": "json_object"}
        resp = client.chat.completions.create(**kwargs1)
        raw = resp.choices[0].message.content.strip()
        return _extract_json(raw)
    except Exception as e:
        logger.warning(f"[Stage1] Failed: {e}")
        return None


# ── Stage 2: Stateful sentiment prompt ────────────────────────────────────────

_STAGE2_SYSTEM = (
    "You are a professional financial analyst AI operating under the following instruction schema.\n\n"
    + _STAGE2_INSTRUCTION_JSON
    + "\n\nReturn ONLY valid JSON matching the output_schema above. No markdown, no explanation."
)


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
        text_block = _json_dumps(stage1_result)
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
    return _json_dumps(payload)


def _call_stage2(client: OpenAI, prompt: str, retries: int = 3) -> Optional[dict]:
    """Main sentiment LLM call. Returns parsed dict or None. Retries on bad JSON."""
    kwargs = {
        "model":             LLM_CONFIG["model"],
        "temperature":       LLM_CONFIG["temperature"],
        "max_tokens":        LLM_CONFIG["max_tokens"],
        "top_p":             LLM_CONFIG["top_p"],
        "frequency_penalty": LLM_CONFIG["frequency_penalty"],
        "presence_penalty":  LLM_CONFIG["presence_penalty"],
        "messages": [
            {"role": "system", "content": _STAGE2_SYSTEM},
            {"role": "user",   "content": prompt},
        ],
    }
    if LLM_TYPE == "ollama":
        kwargs["extra_body"] = {
            "num_ctx": LLM_CONFIG.get("context_size", 16384),
            "top_k":   LLM_CONFIG.get("top_k", 40),
            "format":  "json",   # force valid JSON output
        }
    elif LLM_TYPE in ("api", "anthropic"):
        kwargs["response_format"] = {"type": "json_object"}
    last_raw = None
    for attempt in range(1, retries + 1):
        try:
            # On retry: inject corrective turn showing model what it did wrong
            if attempt > 1 and last_raw is not None:
                kwargs["messages"] = [
                    kwargs["messages"][0],  # system
                    kwargs["messages"][1],  # original user prompt
                    {"role": "assistant", "content": last_raw},
                    {"role": "user", "content": (
                        "Your previous response was NOT valid JSON. "
                        "You returned markdown/prose. "
                        "Return ONLY a raw JSON object — no markdown fences, no explanation, no preamble. "
                        "Start your response with { and end with }."
                    )},
                ]
            resp = client.chat.completions.create(**kwargs)
            raw = resp.choices[0].message.content.strip()
            result = _extract_json(raw)
            if result is not None:
                return result
            last_raw = raw
            logger.warning(f"[Stage2] Attempt {attempt}/{retries}: could not parse JSON (len={len(raw)})")
            logger.warning(f"[Stage2] Attempt {attempt} raw (first 300): {raw[:300]!r}")
        except Exception as e:
            logger.warning(f"[Stage2] Attempt {attempt}/{retries}: {e}")
    logger.warning(f"[Stage2] All {retries} attempts failed — using neutral fallback")
    return None


# ── Time-decay ────────────────────────────────────────────────────────────────

def _time_decay(score: float, published_at: datetime, lam: float = SENTIMENT_LAMBDA) -> float:
    """
    Grace period: no decay for articles younger than DECAY_GRACE_MONTHS.
    After grace period: exponential decay kicks in, measured from grace cutoff.
    Lambda=0.001/hr → half-life ~29 days after grace ends.
    """
    now = datetime.now(timezone.utc)
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    age_hours = max(0.0, (now - published_at).total_seconds() / 3600.0)
    grace_hours = DECAY_GRACE_MONTHS * 30.44 * 24  # avg days per month
    if age_hours <= grace_hours:
        return round(score, 6)  # no decay inside grace window
    t_hours = age_hours - grace_hours  # decay only from grace cutoff onward
    return round(score * math.exp(-lam * t_hours), 6)


# ── Cross-language dedup ───────────────────────────────────────────────────────

def _dedup_languages(conn) -> int:
    """
    Before scoring, remove cross-language duplicate articles.
    Groups by (symbol_id, 5-min bucket, source_name).
    Keeps: English URL (/0/en/) > highest sentiment_score > lowest id.
    Scored articles are never deleted (sentinel: scored articles are always kept
    as the 'winner' if present, so we never discard already-scored work).
    Returns number of rows deleted.
    """
    import psycopg2.extras as _extras
    try:
        with conn.cursor(cursor_factory=_extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    symbol_id,
                    DATE_TRUNC('hour', published_at)            AS hour_bucket,
                    EXTRACT(MINUTE FROM published_at)::int / 5  AS min_bucket,
                    source_name,
                    ARRAY_AGG(id ORDER BY
                        CASE WHEN sentiment_score IS NOT NULL THEN 0 ELSE 1 END,
                        CASE WHEN url ~ '/0/en/' THEN 0 ELSE 1 END,
                        COALESCE(sentiment_score, -99) DESC,
                        id ASC
                    ) AS ids
                FROM news_articles
                WHERE published_at IS NOT NULL
                GROUP BY symbol_id, hour_bucket, min_bucket, source_name
                HAVING COUNT(*) > 1
            """)
            groups = cur.fetchall()

        ids_to_delete = []
        for g in groups:
            ids_to_delete.extend(g["ids"][1:])  # first id is the keeper

        deleted = 0
        if ids_to_delete:
            with conn.cursor() as cur2:
                cur2.execute(
                    "DELETE FROM news_articles WHERE id = ANY(%s)",
                    (ids_to_delete,)
                )
                deleted = cur2.rowcount
                conn.commit()

        if deleted:
            logger.info(f"[dedup_languages] Removed {deleted} cross-language duplicates "
                        f"across {len(groups)} groups before scoring.")
        else:
            logger.info("[dedup_languages] No cross-language duplicates found.")
        return deleted
    except Exception as e:
        logger.warning(f"[dedup_languages] Failed (non-fatal): {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0


# ── VRAM detection & worker sizing ────────────────────────────────────────────

def _get_free_vram_gb() -> tuple[float, float]:
    """
    Returns (total_gb, free_gb).
    1. If GPU_VRAM_GB is set in .env, use it directly (remote Ollama server case).
    2. Otherwise tries nvidia-smi (local GPU), then macOS unified memory.
    Returns (0, 0) on failure.
    """
    # Manual override — required when Ollama is on a remote machine
    if GPU_VRAM_GB > 0:
        logger.info(f"[vram] Using GPU_VRAM_GB override: {GPU_VRAM_GB:.1f} GB")
        return GPU_VRAM_GB, GPU_VRAM_GB

    # NVIDIA via nvidia-smi
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.total,memory.free",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5,
        )
        if out.returncode == 0 and out.stdout.strip():
            line = out.stdout.strip().splitlines()[0]
            total_mb, free_mb = [float(p.strip()) for p in line.split(",")]
            return total_mb / 1024.0, free_mb / 1024.0
    except Exception:
        pass

    # macOS unified memory (no true free_gb — report 85% of total as usable)
    if platform.system() == "Darwin":
        try:
            out = subprocess.run(
                ["system_profiler", "SPHardwareDataType"],
                capture_output=True, text=True, timeout=10,
            )
            for line in out.stdout.splitlines():
                if "Memory:" in line:
                    parts = line.strip().split()
                    val = float(parts[1])
                    unit = parts[2].upper() if len(parts) > 2 else "GB"
                    if unit == "TB":
                        val *= 1024
                    return val, val * 0.85
        except Exception:
            pass

    return 0.0, 0.0


def _get_ollama_model_size_gb(model_name: str) -> float:
    """
    Query Ollama /api/show for the on-disk/VRAM size of a model.
    Returns size in GB, or 0.0 on failure.
    """
    base_url = LLM_CONFIG.get("base_url", "http://localhost:11434/v1")
    # Strip /v1 suffix to get Ollama root
    ollama_root = base_url.rstrip("/")
    for suffix in ("/v1", "/api"):
        if ollama_root.endswith(suffix):
            ollama_root = ollama_root[: -len(suffix)]
            break
    try:
        resp = requests.post(
            f"{ollama_root}/api/show",
            json={"name": model_name},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            size_bytes = data.get("size", 0)
            if size_bytes:
                return size_bytes / (1024 ** 3)
            # Fallback: estimate from parameter count (fp16 = 2 bytes/param)
            params = (data.get("model_info") or {}).get("general.parameter_count", 0)
            if params:
                return (params * 2) / (1024 ** 3)
    except Exception as e:
        logger.warning(f"[vram] Could not fetch model size for '{model_name}': {e}")
    return 0.0


def _compute_worker_count() -> int:
    """
    Detect free VRAM, query both model sizes via Ollama, compute N workers.
    Formula: floor((free_vram * 0.90) / (stage1_gb + stage2_gb)), min 1.
    For API mode: skip VRAM entirely, use API_PARALLEL_WORKERS from config.
    Prints a clear startup banner to console.
    """
    # API mode — no local GPU, parallelism limited by rate limits not VRAM
    if LLM_TYPE in ("api", "anthropic"):
        workers = LLM_CONFIG.get("api_parallel_workers", 5)
        provider = "Anthropic Claude" if LLM_TYPE == "anthropic" else "OpenAI / hosted API"
        print("\n┌─ GPU WORKER SIZING ──────────────────────────────────┐")
        print(f"│  Mode: API ({provider:<29})│")
        print(f"│  Workers: {workers:<4d}  (set API_PARALLEL_WORKERS in .env)  │")
        print("│  No VRAM detection needed — model runs remotely      │")
        print("└──────────────────────────────────────────────────────┘\n")
        logger.info(f"[vram] API mode — using {workers} parallel workers (no VRAM sizing)")
        return workers, False

    total_gb, free_gb = _get_free_vram_gb()

    if free_gb == 0.0:
        logger.warning("[vram] VRAM detection failed — defaulting to 1 worker")
        print("\n┌─ GPU WORKER SIZING ────────────────────────────┐")
        print("│  VRAM detection failed                         │")
        print("│  Workers: 1 (safe default)                     │")
        print("└────────────────────────────────────────────────┘\n")
        return 1, False

    stage1_gb = _get_ollama_model_size_gb(SUMMARY_LLM_MODEL)
    stage2_gb = _get_ollama_model_size_gb(LLM_CONFIG["model"])

    if stage1_gb == 0.0 or stage2_gb == 0.0:
        logger.warning(
            f"[vram] Model size unknown (stage1={stage1_gb:.2f}GB stage2={stage2_gb:.2f}GB) "
            f"— defaulting to 1 worker"
        )
        print("\n┌─ GPU WORKER SIZING ────────────────────────────┐")
        print(f"│  GPU Total : {total_gb:6.1f} GB                        │")
        print(f"│  GPU Free  : {free_gb:6.1f} GB                        │")
        print("│  Model sizes unknown (Ollama /api/show failed) │")
        print("│  Workers: 1 (safe default)                     │")
        print("└────────────────────────────────────────────────┘\n")
        return 1, False

    usable_gb     = free_gb * 0.90
    per_worker_gb = stage1_gb + stage2_gb
    workers       = max(1, int(usable_gb / per_worker_gb))

    # If only 1 worker fits both models, check if single-model mode fits more workers
    single_model_workers = max(1, int(usable_gb / stage2_gb))
    use_single_model = False  # Always load both stage-1 and stage-2 models

    if use_single_model:
        workers = single_model_workers
        headroom_gb = free_gb - (workers * stage2_gb)
        print("\n┌─ GPU WORKER SIZING ──────────────────────────────────┐")
        print(f"│  GPU Total      : {total_gb:6.1f} GB                          │")
        print(f"│  GPU Free       : {free_gb:6.1f} GB  (10% headroom reserved) │")
        print(f"│  Usable VRAM    : {usable_gb:6.2f} GB                          │")
        print(f"│  NOTE: Both models don't fit — using stage-2 only    │")
        print(f"│  Model          : {stage2_gb:6.2f} GB  ({LLM_CONFIG['model']}){'':>5}│")
        print(f"│  Per-worker     : {stage2_gb:6.2f} GB                          │")
        print(f"│  ── WORKERS     :   {workers:<4d} (headroom {headroom_gb:.2f} GB)         │")
        print("└──────────────────────────────────────────────────────┘\n")
        logger.info(
            f"[vram] single-model mode: total={total_gb:.1f}GB free={free_gb:.1f}GB "
            f"stage2={stage2_gb:.2f}GB workers={workers}"
        )
        return workers, True   # (workers, single_model_mode)

    headroom_gb   = free_gb - (workers * per_worker_gb)

    print("\n┌─ GPU WORKER SIZING ──────────────────────────────────┐")
    print(f"│  GPU Total      : {total_gb:6.1f} GB                          │")
    print(f"│  GPU Free       : {free_gb:6.1f} GB  (10% headroom reserved) │")
    print(f"│  Usable VRAM    : {usable_gb:6.2f} GB                          │")
    print(f"│  Stage-1 model  : {stage1_gb:6.2f} GB  ({SUMMARY_LLM_MODEL}){'':>5}│")
    print(f"│  Stage-2 model  : {stage2_gb:6.2f} GB  ({LLM_CONFIG['model']}){'':>5}│")
    print(f"│  Per-worker     : {per_worker_gb:6.2f} GB                          │")
    print(f"│  ── WORKERS     :   {workers:<4d} (headroom {headroom_gb:.2f} GB)         │")
    print("└──────────────────────────────────────────────────────┘\n")

    logger.info(
        f"[vram] total={total_gb:.1f}GB free={free_gb:.1f}GB "
        f"stage1={stage1_gb:.2f}GB stage2={stage2_gb:.2f}GB "
        f"workers={workers}"
    )
    return workers, False  # (workers, single_model_mode)


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
        """
        if limit:
            q += f" LIMIT {limit}"
        cur.execute(q, (exchange,))
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    import random
    random.shuffle(rows)
    # Pull priority queue and move those symbols to the front (preserving their rank order)
    try:
        with conn.cursor() as pq_cur:
            pq_cur.execute("SELECT symbol_id FROM priority_queue ORDER BY rank ASC")
            priority_ids = [r[0] for r in pq_cur.fetchall()]
        if priority_ids:
            prio_map = {sid: i for i, sid in enumerate(priority_ids)}
            priority_rows = sorted([r for r in rows if r["id"] in prio_map], key=lambda r: prio_map[r["id"]])
            normal_rows   = [r for r in rows if r["id"] not in prio_map]
            rows = priority_rows + normal_rows
    except Exception:
        pass  # priority_queue table may not exist yet on older installs
    return rows


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
                         published_at: datetime, stage2_prompt: str = ""):
    score = float(result.get("sentiment_score", 0.0))
    score = max(-1.0, min(1.0, score))
    weighted = _time_decay(score, published_at)

    def _to_str(v, limit):
        if v is None:
            return ""
        if isinstance(v, (dict, list)):
            v = json.dumps(v)
        return str(v)[:limit]

    updates = {
        "sentiment_score":         score,
        "weighted_sentiment":      weighted,
        "article_summary":         _to_str(result.get("article_summary"), 500),
        "master_summary_snapshot": master_snapshot,
        "key_events":              json.dumps(result.get("key_events") or {}),
        "score_rationale":         _to_str(result.get("score_rationale"), 1000),
        "forecast_until_earnings": _to_str(result.get("forecast_until_earnings"), 2000),
        "stage2_prompt":           stage2_prompt,
        "company_connections":     json.dumps(result.get("company_connections") or {"competitors": [], "partners": [], "suppliers": []}),
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
            pre_summary_data        = COALESCE(%(pre_summary_data)s::jsonb, pre_summary_data),
            score_rationale         = %(score_rationale)s,
            forecast_until_earnings = %(forecast_until_earnings)s,
            stage2_prompt           = %(stage2_prompt)s,
            company_connections     = %(company_connections)s::jsonb
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

def _get_daily_snapshot(conn, symbol_id: int, article_date) -> dict:
    """
    Get the best TV snapshot for a given article date.
    Looks for snapshot on same day, then searches backwards (most recent ≤ article_date).
    Falls back to most recent snapshot available. Returns empty dict if none.
    """
    try:
        snap_date = article_date.date() if hasattr(article_date, "date") else article_date
        with conn.cursor() as cur:
            # Best: same day or earlier (most recent ≤ article date)
            cur.execute("""
                SELECT data FROM symbol_daily_snapshots
                WHERE symbol_id = %s AND snapshot_date <= %s
                ORDER BY snapshot_date DESC
                LIMIT 1
            """, (symbol_id, snap_date))
            row = cur.fetchone()
            if row:
                data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                return {**data, "_snapshot_date": str(snap_date)}
    except Exception as e:
        logger.debug(f"[snapshot] Could not fetch daily snapshot: {e}")
    return {}


def _process_symbol(
    conn,
    main_client: OpenAI,
    summary_client: OpenAI,
    sym: dict,
    single_model_mode: bool = False,
) -> dict:
    symbol   = sym["symbol"]
    sym_id   = sym["id"]
    industry = sym.get("industry") or ""

    articles = _get_articles_for_symbol(conn, sym_id)
    if not articles:
        return {"symbol": symbol, "scored": 0, "skipped": 0}

    macro_mult     = _get_macro_multiplier(conn, industry)
    master_summary = sym.get("symbol_master_summary") or ""
    last_score     = 0.0
    weighted_scores = []
    last_forecast  = ""
    scored = 0
    skipped = 0

    # Fallback TV snapshot from symbols row (used when no daily snapshot exists)
    tv_fields_fallback = [
        "close_price", "price_change", "price_earnings_ttm", "price_sales_ratio",
        "price_book_ratio", "earnings_per_share_basic_ttm", "price_earnings_growth_ttm",
        "total_revenue", "net_income", "gross_margin", "operating_margin", "net_margin",
        "return_on_equity", "debt_to_equity", "current_ratio", "rsi", "sma200",
        "price_52_week_high", "relative_volume_10d_calc", "average_volume_30d_calc",
        "earnings_release_date", "dividend_yield_recent", "number_of_employees",
        "industry", "market_cap_formatted",
    ]
    tv_snapshot_fallback = {k: sym[k] for k in tv_fields_fallback if sym.get(k) is not None}
    if "earnings_release_date" in tv_snapshot_fallback and hasattr(tv_snapshot_fallback["earnings_release_date"], "isoformat"):
        tv_snapshot_fallback["earnings_release_date"] = tv_snapshot_fallback["earnings_release_date"].isoformat()

    for article in articles:
        art_id      = article["id"]
        published_at = article["published_at"]

        # Get date-matched daily snapshot for this article; fall back to symbols row
        daily_snap = _get_daily_snapshot(conn, sym_id, published_at)
        tv_snapshot = daily_snap if daily_snap else tv_snapshot_fallback

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
        t_s1 = 0.0
        if ENABLE_PRE_SUMMARIZATION:
            # Use cached pre_summary_data if available
            if article.get("pre_summary_data"):
                stage1_result = article["pre_summary_data"]
            else:
                raw_text = (article.get("full_text") or article.get("summary") or "")
                if raw_text.strip():
                    _t0 = time.time()
                    stage1_result = _call_stage1(
                        summary_client, raw_text,
                        model_override=LLM_CONFIG["model"] if single_model_mode else None
                    )
                    t_s1 = round(time.time() - _t0, 1)

        # Stage 2: stateful scoring
        prompt = _build_stage2_prompt(
            symbol, tv_snapshot, article,
            master_summary, last_score, stage1_result
        )
        _t0 = time.time()
        result = _call_stage2(main_client, prompt)
        t_s2 = round(time.time() - _t0, 1)

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
                                 master_summary, stage1_result, published_at,
                                 stage2_prompt=prompt)
        conn.commit()
        master_summary = result.get("updated_master_summary") or master_summary
        last_forecast  = result.get("forecast_until_earnings") or last_forecast
        last_score     = raw_score
        weighted_scores.append(weighted)
        scored += 1

        title_short = (article.get("title") or "")[:60]
        logger.info(
            f"[{symbol}] article={art_id} score={raw_score:+.2f} "
            f"s1={t_s1}s s2={t_s2}s | {title_short}"
        )

        time.sleep(0.3)  # brief pause between articles

    # Save symbol-level scores
    if weighted_scores:
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
    Worker count is auto-sized from free VRAM at startup.
    """
    started_at = datetime.now(timezone.utc)
    conn = get_conn()

    # ── Auto-dedup cross-language articles before scoring ──────────────────────
    _dedup_languages(conn)

    symbols = _get_symbols_with_unscored(conn, exchange, limit)
    if not symbols:
        logger.info("[sentiment_scoring] No symbols with unscored articles.")
        conn.close()
        return {"symbols_processed": 0, "articles_scored": 0}

    logger.info(f"[sentiment_scoring] {len(symbols)} symbols to score "
                f"(pre_summarization={'ON' if ENABLE_PRE_SUMMARIZATION else 'OFF'})")

    # ── VRAM-based worker sizing ───────────────────────────────────────────────
    n_workers, single_model_mode = _compute_worker_count()

    # Warmup models before spawning threads
    main_client    = _get_main_client()
    if single_model_mode:
        summary_client = main_client  # reuse same client, same model, no load/unload
        logger.info("[vram] Single-model mode: stage-1 will use stage-2 model (no swap)")
    else:
        summary_client = _get_summary_client()
    _warmup_models(main_client, summary_client)

    # ── Semaphore caps concurrent Ollama requests ──────────────────────────────
    semaphore = threading.Semaphore(n_workers)

    # Progress tracking
    total_symbols  = len(symbols)
    done_count     = [0]  # mutable counter shared across threads
    done_lock      = threading.Lock()

    def _worker(sym: dict) -> dict:
        with semaphore:
            # Each worker gets its own DB connection (thread-safe)
            w_conn = get_conn()
            try:
                result = _process_symbol(w_conn, main_client, summary_client, sym, single_model_mode)
            except Exception as e:
                logger.error(f"[{sym['symbol']}] Unhandled error: {e}", exc_info=True)
                try:
                    w_conn.rollback()
                except Exception:
                    pass
                result = {"symbol": sym["symbol"], "scored": 0, "skipped": 0}
            finally:
                w_conn.close()

            with done_lock:
                done_count[0] += 1
                pct = done_count[0] / total_symbols * 100
                print(
                    f"  [{done_count[0]:>{len(str(total_symbols))}}/{total_symbols}] "
                    f"({pct:5.1f}%)  {sym['symbol']:10s}  "
                    f"scored={result.get('scored',0)}  "
                    f"skipped={result.get('skipped',0)}",
                    flush=True,
                )
            return result

    conn.close()  # main conn no longer needed — workers use their own

    print(f"\n  Scoring {total_symbols} symbols with {n_workers} parallel worker(s)...\n")

    results      = []
    total_scored = 0

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {pool.submit(_worker, sym): sym for sym in symbols}
        for future in as_completed(futures):
            try:
                r = future.result()
                results.append(r)
                total_scored += r.get("scored", 0)
            except Exception as e:
                logger.error(f"[run] Future raised: {e}", exc_info=True)

    duration = (datetime.now(timezone.utc) - started_at).total_seconds()
    print(f"\n  Done — symbols={len(results)}, articles_scored={total_scored}, "
          f"duration={duration:.1f}s\n")
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
                                 master_summary, stage1_result, article["published_at"],
                                 stage2_prompt=prompt)
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
