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
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import requests
from openai import OpenAI
import httpx

from config import LLM_CONFIG, LLM_TYPE, GPU_VRAM_GB
from db.connection import get_conn
from pipeline_config import (  # noqa — patched below
    MAX_EVAL_ARTICLES,
    ENABLE_PRE_SUMMARIZATION,
    SUMMARY_LLM_MODEL,
    SENTIMENT_LAMBDA,
    DECAY_GRACE_MONTHS,
    NEUTRAL_SCORE_THRESHOLD,
    MATERIALITY_THRESHOLD,
    GRAVITY_GAMMA,
    GRAVITY_GRACE_DAYS,
    ASYMPTOTE_THRESHOLD,
    SYMBOL_LIMIT,
    STAGE1_PARALLEL_WORKERS,
    STAGE2_PARALLEL_WORKERS,
    SKIP_WORKER_COUNT_DETECTION,
    SKILLS_ENABLED,
    INCLUDE_TV_SNAPSHOT,
)

# ── Pause / tier-reload controls ──────────────────────────────────────────────

class ScoringPaused(Exception):
    """Raised when the pause flag is detected — mid-stream or between articles."""
    pass


# In-process pause event — set = paused, clear = running.
# Updated by _pause_poller() background thread; checked cheaply in hot paths.
_PAUSE_EVENT = threading.Event()
_POLLER_STARTED = False   # guard: only one poller per process


def _is_paused() -> bool:
    """Fast in-process pause check (no DB hit — uses _PAUSE_EVENT)."""
    return _PAUSE_EVENT.is_set()


def _start_pause_poller() -> None:
    """Start background thread that syncs _PAUSE_EVENT with scoring_control.paused every 2 s.
    Called once at the top of run(). Safe to call multiple times — only one thread starts."""
    global _POLLER_STARTED
    if _POLLER_STARTED:
        return
    _POLLER_STARTED = True

    def _poll():
        while True:
            try:
                c = get_conn()
                with c.cursor() as cur:
                    cur.execute("SELECT paused FROM scoring_control WHERE id = 1")
                    row = cur.fetchone()
                c.close()
                if row and row[0]:
                    _PAUSE_EVENT.set()
                else:
                    _PAUSE_EVENT.clear()
            except Exception:
                pass  # DB blip — keep previous state
            time.sleep(2.0)

    t = threading.Thread(target=_poll, name="pause-poller", daemon=True)
    t.start()
    logger.debug("[pause_poller] started")


def _reload_tier(tier_num: int) -> None:
    """Hot-reload LLM_CONFIG + worker counts from config._TIERS[tier_num] in-place.

    Called at the top of run() when scoring_control.active_tier differs from the
    currently loaded tier — allows tier switches while paused without restarting.
    """
    global LLM_TYPE, STAGE1_PARALLEL_WORKERS, STAGE2_PARALLEL_WORKERS, SUMMARY_LLM_MODEL
    try:
        import config as _cfg
        t = _cfg._TIERS.get(tier_num)
        if not t or not _cfg._is_configured(t):
            logger.warning(f"[tier_reload] Tier {tier_num} not configured — keeping current")
            return
        merged = {**_cfg._SHARED_PARAMS, **t}
        LLM_CONFIG.update(merged)
        LLM_TYPE = (t.get("llm_type") or "local").lower()
        STAGE1_PARALLEL_WORKERS = int(t.get("stage1_workers", 4))
        STAGE2_PARALLEL_WORKERS = int(t.get("stage2_workers", 1))
        SUMMARY_LLM_MODEL = str(
            merged.get("summary_model") or merged.get("model") or SUMMARY_LLM_MODEL
        )
        logger.info(
            f"[tier_reload] → TIER={tier_num}  model={t['model']}  "
            f"s1_workers={STAGE1_PARALLEL_WORKERS}  s2_workers={STAGE2_PARALLEL_WORKERS}"
        )
    except Exception as e:
        logger.warning(f"[tier_reload] Failed to reload tier {tier_num}: {e}")


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


# ── Skills loader ─────────────────────────────────────────────────────────────

def _load_skills() -> str:
    """Load all .md files from the project skills/ folder.

    Rules:
    - Files prefixed with _ are skipped (easy per-file disable).
    - Files are loaded in alphabetical order.
    - Returns empty string if SKILLS_ENABLED=False or no files found.
    """
    if not SKILLS_ENABLED:
        return ""
    skills_dir = Path(__file__).parent.parent / "skills"
    if not skills_dir.is_dir():
        return ""
    parts = []
    for md_path in sorted(skills_dir.glob("*.md")):
        if md_path.stem.startswith("_") or md_path.name == "README.md":
            continue
        try:
            text = md_path.read_text(encoding="utf-8").strip()
            if text:
                parts.append(
                    f"[SKILL: {md_path.stem}]\n"
                    f"The following skill is GLOBALLY ACTIVE and must be followed STRICTLY "
                    f"for every response in this session:\n\n{text}"
                )
        except Exception as e:
            logging.getLogger(__name__).warning(f"[skills] Failed to load {md_path.name}: {e}")
    if not parts:
        return ""
    return "\n\n" + "\n\n---\n\n".join(parts)


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

# ── SEC per-form instruction files ────────────────────────────────────────────
# Maps form_type → config filename. Loaded lazily on first use.
_FORM_TYPE_TO_INSTRUCTION: dict[str, str] = {
    "10-K":     "sec_10k_instruction.json",
    "10-K/A":   "sec_10k_instruction.json",
    "10-Q":     "sec_10q_instruction.json",
    "10-Q/A":   "sec_10q_instruction.json",
    "8-K":      "sec_8k_instruction.json",
    "8-K/A":    "sec_8k_instruction.json",
    "S-3":      "sec_s3_424b_instruction.json",
    "S-3/A":    "sec_s3_424b_instruction.json",
    "424B1":    "sec_s3_424b_instruction.json",
    "424B3":    "sec_s3_424b_instruction.json",
    "424B4":    "sec_s3_424b_instruction.json",
    "424B5":    "sec_s3_424b_instruction.json",
    "NT 10-K":  "sec_nt_instruction.json",
    "NT 10-Q":  "sec_nt_instruction.json",
    "4":        "sec_form4_instruction.json",
    "SC 13D":   "sec_13d_instruction.json",
    "SC 13D/A": "sec_13d_instruction.json",
    "SC 13G":   "sec_13g_instruction.json",
    "SC 13G/A": "sec_13g_instruction.json",
}
# Cache: fname → fully-built system prompt string
_SEC_SYSTEM_CACHE: dict[str, str] = {}

logger = logging.getLogger(__name__)

# ── LLM clients ───────────────────────────────────────────────────────────────

def _get_main_client() -> OpenAI:
    # connect=60: gives Ollama time to load/swap model into VRAM before first byte.
    # read=600:   long generation (5120 tokens) can take several minutes on e4b.
    # max_retries=1: SDK retries waste time on server-load stalls — outer
    #   _call_stage2 loop handles retries with corrective prompts instead.
    return OpenAI(
        base_url=LLM_CONFIG["base_url"],
        api_key=LLM_CONFIG["api_key"],
        timeout=httpx.Timeout(connect=60.0, read=600.0, write=120.0, pool=60.0),
        max_retries=1,
    )


def _get_summary_client() -> OpenAI:
    """Stage 1 client — uses stage1_base_url if configured, otherwise shares stage2 endpoint."""
    base_url = LLM_CONFIG.get("stage1_base_url") or LLM_CONFIG["base_url"]
    api_key  = LLM_CONFIG.get("stage1_api_key")  or LLM_CONFIG["api_key"]
    return OpenAI(
        base_url=base_url,
        api_key=api_key,
        timeout=httpx.Timeout(connect=60.0, read=300.0, write=120.0, pool=60.0),
        max_retries=1,
    )


def _lmstudio_unload_model(root: str, instance_id: str) -> None:
    """Unload a specific model instance so we can reload with correct settings."""
    try:
        resp = requests.post(
            f"{root}/api/v1/models/unload",
            json={"instance_id": instance_id},
            timeout=30,
        )
        if resp.status_code == 200:
            logger.info(f"[lmstudio_unload] '{instance_id}' unloaded OK")
        else:
            logger.debug(f"[lmstudio_unload] '{instance_id}' HTTP {resp.status_code}")
    except Exception as e:
        logger.debug(f"[lmstudio_unload] '{instance_id}' error (non-fatal): {e}")


def _lmstudio_load_model(base_url: str, model_id: str, context_length: int,
                         parallel_slots: int = 1) -> bool:
    """Force-load a model via LM Studio /api/v1/models/load (LM Studio >= 0.4.0).

    Steps:
      1. Check if model already loaded with correct context — skip if so.
      2. Unload any existing instances that have wrong context.
      3. Load fresh with context_length from config + N parallel inference slots
         so ONE instance handles N concurrent chat completions (instead of
         LM Studio's JIT spawning N duplicate instances).

    LM Studio 0.4.x auto-maximizes GPU layers — no gpu_offload param needed.
    Returns True on success."""
    root = base_url.rstrip("/")
    if root.endswith("/v1"):
        root = root[:-3]

    # Check current state via /api/v1/models
    try:
        r = requests.get(f"{root}/api/v1/models", timeout=10)
        if r.status_code == 200:
            for m in r.json().get("models", []):
                if m.get("key") == model_id:
                    for inst in m.get("loaded_instances", []):
                        cfg = inst.get("config", {})
                        inst_ctx = cfg.get("context_length", 0)
                        inst_par = cfg.get("parallel", 1) or 1
                        inst_id  = inst.get("id", model_id)
                        if inst_ctx == context_length and inst_par == parallel_slots:
                            logger.info(
                                f"[lmstudio_load] '{model_id}' already loaded "
                                f"ctx={context_length} parallel={parallel_slots} — skipping"
                            )
                            return True
                        else:
                            logger.info(
                                f"[lmstudio_load] Unloading '{inst_id}' "
                                f"(ctx={inst_ctx}/par={inst_par} → "
                                f"need ctx={context_length}/par={parallel_slots})"
                            )
                            _lmstudio_unload_model(root, inst_id)
    except Exception as e:
        logger.debug(f"[lmstudio_load] state check failed (non-fatal): {e}")

    # Load with desired context_length + parallel slots so a SINGLE instance
    # serves all concurrent worker requests. Without this LM Studio's JIT
    # auto-load spawns one fresh instance per concurrent request, multiplying
    # VRAM use by N.
    #
    # LM Studio's /api/v1/models/load is strict — unknown keys are REJECTED
    # with HTTP 400 'unrecognized_keys'. As of LM Studio 0.4.x the accepted
    # keys for our use case are exactly: model, context_length, parallel, ttl.
    payload = {
        "model": model_id,
        "context_length": context_length,
        "parallel": parallel_slots,
        "ttl": -1,
    }
    try:
        resp = requests.post(
            f"{root}/api/v1/models/load",
            json=payload,
            timeout=120,
        )
        if resp.status_code in (200, 201):
            logger.info(
                f"[lmstudio_load] '{model_id}' loaded — ctx={context_length} "
                f"parallel_slots={parallel_slots}"
            )
            return True
        else:
            logger.warning(
                f"[lmstudio_load] '{model_id}' failed: "
                f"HTTP {resp.status_code} — {resp.text[:300]}"
            )
            return False
    except Exception as e:
        logger.warning(f"[lmstudio_load] '{model_id}' request error: {e}")
        return False


def _warmup_models(main_client: OpenAI, summary_client: OpenAI) -> None:
    """Load both models with guaranteed GPU offload + context size before processing starts.
    LM Studio (local): uses /api/v0/models/load to force settings from code, not UI.
    Ollama: sends a tiny chat ping to preload into VRAM.
    Skipped entirely for API/Anthropic."""
    if LLM_TYPE in ("api", "anthropic"):
        logger.info("[warmup] API mode — skipping warmup (no local model to load)")
        return

    ctx_size       = LLM_CONFIG.get("context_size", 50000)
    base_url       = LLM_CONFIG["base_url"]
    stage1_base_url = LLM_CONFIG.get("stage1_base_url") or base_url

    if LLM_TYPE == "local":
        # LM Studio: force-load both models via REST API with correct settings.
        # parallel_slots = worker count so a SINGLE instance serves all workers
        # instead of LM Studio JIT-spawning N duplicate instances.
        s1_slots = max(1, STAGE1_PARALLEL_WORKERS)
        s2_slots = max(1, STAGE2_PARALLEL_WORKERS)
        ok1 = _lmstudio_load_model(stage1_base_url, SUMMARY_LLM_MODEL,  ctx_size,
                                   parallel_slots=s1_slots)
        ok2 = _lmstudio_load_model(base_url,        LLM_CONFIG["model"], ctx_size,
                                   parallel_slots=s2_slots)
        if ok1 and ok2:
            logger.info("[warmup] Both models loaded via LM Studio API — GPU auto-max, "
                        "ctx=%d, s1_slots=%d s2_slots=%d",
                        ctx_size, s1_slots, s2_slots)
            return
        # Fallback: /api/v0/models/load unavailable — fall through to chat ping
        logger.warning("[warmup] LM Studio load API failed — falling back to chat ping (GPU/ctx from UI)")

    # Ollama or LM Studio fallback: plain chat ping
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
            logger.info(f"[warmup] {label} loaded OK (chat ping)")
        except Exception as e:
            logger.warning(f"[warmup] {label} warmup failed (non-fatal): {e}")


# ── Stage 1: Pre-summarization prompt ─────────────────────────────────────────

_STAGE1_SYSTEM = (
    "You are a financial news extraction engine operating under the following instruction schema.\n\n"
    + _STAGE1_INSTRUCTION_JSON
    + "\n\nReturn ONLY valid JSON matching the output_schema above. No markdown, no explanation."
    + _load_skills()
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
    if start == -1:
        # no JSON object at all — give up immediately
        return None
    if end != -1 and end > start:
        raw = raw[start:end + 1]
    else:
        # truncated: no closing } — take from { onward and let json_repair fix it
        raw = raw[start:]

    # pass 1: standard json.loads (fast path)
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict) and parsed:
            return parsed
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
    """Fast pre-summarization. Returns dict or None on failure.

    Determinism: temperature near zero + top_p=1 + fixed seed (when supported)
    so the same article yields the same JSON across runs and across models
    that honor these knobs.
    """
    model = model_override or SUMMARY_LLM_MODEL
    try:
        kwargs1 = dict(
            model=model,
            temperature=0.0,
            top_p=1.0,
            max_tokens=4096,
            seed=42,
            messages=[
                {"role": "system", "content": _STAGE1_SYSTEM},
                {"role": "user",   "content": f"Extract facts from this article:\n\n{text}"},
            ],
        )
        if LLM_TYPE in ("ollama", "local"):
            kwargs1["extra_body"] = {
                "num_ctx":     LLM_CONFIG.get("context_size", 16384),
                "top_k":       1,        # greedy decoding for determinism
                "num_predict": 4096,
                "seed":        42,
                "format":      "json",
            }
            # ollama/local typically doesn't honor top-level seed; the
            # extra_body version is what reaches the runtime.
            kwargs1.pop("seed", None)
        elif LLM_TYPE in ("api", "anthropic"):
            kwargs1["response_format"] = {"type": "json_object"}
        resp = client.chat.completions.create(**kwargs1)
        raw = ""
        if resp and resp.choices:
            msg = resp.choices[0].message
            raw = (getattr(msg, "content", None) or "").strip()
        if not raw:
            logger.warning("[Stage1] Empty content from LLM")
            return None
        return _extract_json(raw)
    except Exception as e:
        logger.warning(f"[Stage1] Failed: {e}")
        return None


# ── Stage 2: Stateful sentiment prompt ────────────────────────────────────────

_STAGE2_SYSTEM = (
    "You are a professional financial analyst AI operating under the following instruction schema.\n\n"
    + _STAGE2_INSTRUCTION_JSON
    + "\n\nReturn ONLY valid JSON matching the output_schema above. No markdown, no explanation."
    + _load_skills()
)


def _get_stage2_system(form_type: Optional[str]) -> str:
    """Return the correct Stage 2 system prompt for this article.

    News articles (form_type=None) → existing _STAGE2_SYSTEM (unchanged behaviour).
    SEC filings → load the tailor-made per-form instruction JSON, build system
    prompt once, cache it for the lifetime of this process.
    """
    if not form_type:
        return _STAGE2_SYSTEM  # news / press release — no change

    fname = _FORM_TYPE_TO_INSTRUCTION.get(form_type)
    if not fname:
        logger.debug(f"[sec_router] Unknown form_type '{form_type}' — falling back to news prompt")
        return _STAGE2_SYSTEM

    if fname not in _SEC_SYSTEM_CACHE:
        instr = _load_instruction(fname)
        if instr == "{}":
            logger.warning(f"[sec_router] Instruction file '{fname}' missing — falling back to news prompt")
            _SEC_SYSTEM_CACHE[fname] = _STAGE2_SYSTEM
        else:
            _SEC_SYSTEM_CACHE[fname] = (
                "You are a professional financial analyst AI operating under the following "
                "SEC filing instruction schema.\n\n"
                + instr
                + "\n\nReturn ONLY valid JSON matching the output_schema above. "
                  "No markdown, no explanation."
                + _load_skills()
            )
        logger.debug(f"[sec_router] Loaded SEC instruction: {fname} for form_type={form_type}")

    return _SEC_SYSTEM_CACHE[fname]


def _build_stage2_prompt(
    symbol: str,
    tv_snapshot: dict,
    article: dict,
    master_summary: str,
    last_score: float,
    stage1_result: Optional[dict],
    previous_article_summary: Optional[str] = None,
) -> str:
    # Article body fed to stage 2:
    #   • Stage 1 ON  → use the extracted-facts JSON (compact, structured)
    #   • Stage 1 OFF → use raw article text. Strip HTML, pick the longer
    #     of full_text / RSS summary so stage 2 always sees the richest body
    #     available. If both empty stage 2 scores from title alone.
    def _strip_html(s: str) -> str:
        if not s: return ""
        import re as _re, html as _html
        # decode entities, drop tags, collapse whitespace
        s = _html.unescape(s)
        s = _re.sub(r"<[^>]+>", " ", s)
        s = _re.sub(r"\s+", " ", s).strip()
        return s

    raw_full    = _strip_html(article.get("full_text") or "")
    raw_summary = _strip_html(article.get("summary") or "")
    if stage1_result:
        text_block = _json_dumps(stage1_result)
        current_article = {
            "title":           article["title"],
            "published_at":    article["published_at"].isoformat() if hasattr(article["published_at"], "isoformat") else str(article["published_at"]),
            "stage1_facts":    text_block,
            "_pre_summarization": "ON",
        }
    else:
        # Stage 1 disabled — give scorer the LONGEST clean text we have.
        # Some sources stuff the full body into `summary`, others into `full_text`.
        body = raw_full if len(raw_full) >= len(raw_summary) else raw_summary
        secondary = raw_summary if body is raw_full else raw_full
        current_article = {
            "title":             article["title"],
            "published_at":      article["published_at"].isoformat() if hasattr(article["published_at"], "isoformat") else str(article["published_at"]),
            "raw_full_text":     body,
            "rss_summary":       secondary if (secondary and secondary != body) else None,
            "_pre_summarization": "OFF",
            "_note":             "Stage 1 pre-summarization disabled — score directly from raw text above.",
        }
        # Strip Nones for clean prompt
        current_article = {k: v for k, v in current_article.items() if v is not None}

    payload = {
        "symbol": symbol,
        "master_summary": master_summary or "",
        "tradingview_snapshot": (tv_snapshot or {}) if INCLUDE_TV_SNAPSHOT else {},
        "previous_state": {
            "last_article_score": last_score,
            "last_article_summary": previous_article_summary or None,
        },
        "current_article": current_article,
    }
    return _json_dumps(payload)


def _validate_stage2_result(result: dict, prev_master_summary: str = "") -> list[str]:
    """Return a list of validation failures. Empty list = result is acceptable.

    Checks enforced:
      1. updated_master_summary must be present and non-empty (Part G).
         If a previous master_summary exists, the output must be at least as long
         as the input (no collapsing history).
      2. sentiment_score must be a finite float in [-1, 1].
      3. score_calibration must be present if sentiment_score != 0.0.
      4. forecast_until_earnings must be present and non-empty.
    """
    failures: list[str] = []
    # -- updated_master_summary --
    ums = result.get("updated_master_summary")
    if not ums or not str(ums).strip():
        failures.append(
            "CRITICAL: updated_master_summary is empty or null. "
            "You MUST carry forward the previous master_summary and weave in new facts. "
            "If no previous summary exists, write a 150–300 word foundation narrative."
        )
    elif prev_master_summary and len(str(ums).strip()) < max(100, len(prev_master_summary) // 4):
        failures.append(
            f"CRITICAL: updated_master_summary is too short ({len(str(ums).strip())} chars vs "
            f"previous {len(prev_master_summary)} chars). You collapsed or deleted history. "
            "Carry forward ALL quantified facts from the previous summary."
        )
    # -- sentiment_score --
    raw_score = result.get("sentiment_score")
    try:
        s = float(raw_score)
        if not (-1.0 <= s <= 1.0):
            failures.append(f"sentiment_score {s} is outside [-1, 1].")
        import math as _math
        if _math.isnan(s) or _math.isinf(s):
            failures.append("sentiment_score is NaN or Inf.")
    except (TypeError, ValueError):
        failures.append(f"sentiment_score is not a valid float: {raw_score!r}.")
    # -- score_calibration --
    try:
        if float(result.get("sentiment_score", 0.0)) != 0.0:
            sc = result.get("score_calibration")
            if not sc or not isinstance(sc, dict):
                failures.append(
                    "score_calibration block is missing or not an object. "
                    "Provide anchor_band_base, negative_counter_weights, final_calculated_raw."
                )
    except (TypeError, ValueError):
        pass
    # -- forecast_until_earnings --
    fcast = result.get("forecast_until_earnings")
    if not fcast or not str(fcast).strip():
        failures.append("forecast_until_earnings is empty. Provide bias + confidence analysis.")
    return failures


def _call_stage2(client: OpenAI, prompt: str, retries: int = 3,
                 prev_master_summary: str = "",
                 system_prompt: Optional[str] = None) -> Optional[dict]:
    """Main sentiment LLM call. Streams tokens and checks pause flag every chunk.

    system_prompt: if provided, overrides the default _STAGE2_SYSTEM.
                   Used to inject SEC-specific instruction JSONs per form type.
    """
    sys_msg = system_prompt if system_prompt is not None else _STAGE2_SYSTEM
    kwargs = {
        "model":             LLM_CONFIG["model"],
        "temperature":       0.0,
        "top_p":             1.0,
        "max_tokens":        LLM_CONFIG["max_tokens"],
        "frequency_penalty": LLM_CONFIG["frequency_penalty"],
        "presence_penalty":  LLM_CONFIG["presence_penalty"],
        "seed":              42,
        "stream":            True,   # ← streaming for interruptibility
        "messages": [
            {"role": "system", "content": sys_msg},
            {"role": "user",   "content": prompt},
        ],
    }
    if LLM_TYPE in ("ollama", "local"):
        kwargs["extra_body"] = {
            "num_ctx":     LLM_CONFIG.get("context_size", 16384),
            "num_predict": LLM_CONFIG.get("max_tokens", 4096),
            "top_k":       1,
            "seed":        42,
            "format":      "json",
        }
        kwargs.pop("seed", None)
    elif LLM_TYPE in ("api", "anthropic"):
        kwargs["response_format"] = {"type": "json_object"}

    last_raw: Optional[str] = None

    for attempt in range(1, retries + 1):
        # Inject corrective turn on retry
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

        try:
            chunks: list[str] = []
            stream = client.chat.completions.create(**kwargs)
            try:
                for chunk in stream:
                    # ── Hard-stop check every token ───────────────────────────
                    if _PAUSE_EVENT.is_set():
                        try:
                            stream.close()
                        except Exception:
                            pass
                        raise ScoringPaused("Pause detected mid-stream — article stays unscored")

                    delta = chunk.choices[0].delta if chunk.choices else None
                    text  = (getattr(delta, "content", None) or "") if delta else ""
                    if text:
                        chunks.append(text)
            except ScoringPaused:
                raise   # propagate immediately — do not fall into retry logic
            finally:
                try:
                    stream.close()
                except Exception:
                    pass

            raw = "".join(chunks).strip()
            if not raw:
                last_raw = ""
                logger.warning(f"[Stage2] Attempt {attempt}/{retries}: empty response from LLM")
                continue

            result = _extract_json(raw)
            if result is not None:
                failures = _validate_stage2_result(result, prev_master_summary)
                if not failures:
                    return result
                repair_issues = "\n".join(f"  - {f}" for f in failures)
                logger.warning(
                    f"[Stage2] Attempt {attempt}/{retries}: schema validation failed "
                    f"({len(failures)} issue(s)):\n{repair_issues}"
                )
                last_raw = raw
                kwargs["messages"] = [
                    kwargs["messages"][0],
                    kwargs["messages"][1],
                    {"role": "assistant", "content": raw},
                    {"role": "user", "content": (
                        "Your previous response has the following critical schema violations. "
                        "Return a corrected JSON object that fixes ALL issues below. "
                        "Keep all other fields unchanged. Start with { and end with }.\n\n"
                        "VIOLATIONS TO FIX:\n"
                        + repair_issues
                    )},
                ]
                continue

            last_raw = raw
            logger.warning(f"[Stage2] Attempt {attempt}/{retries}: could not parse JSON (len={len(raw)})")
            logger.warning(f"[Stage2] Attempt {attempt} raw (first 300): {raw[:300]!r}")

        except ScoringPaused:
            raise   # never swallow — let _process_symbol handle it
        except Exception as e:
            logger.warning(f"[Stage2] Attempt {attempt}/{retries}: {e}")

    logger.warning(f"[Stage2] All {retries} attempts failed — using neutral fallback")
    return None




# ── Time-decay ────────────────────────────────────────────────────────────────

def _decay_weight(published_at: datetime, lam: float = SENTIMENT_LAMBDA) -> float:
    """
    Decay weight in [0, 1]. 1.0 inside the grace window, then exp(-lam * t_hours).
    Use this as the weight in a weighted mean — DO NOT also multiply the raw
    score by it for aggregation, or decay gets double-applied.
    """
    now = datetime.now(timezone.utc)
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    age_hours = max(0.0, (now - published_at).total_seconds() / 3600.0)
    grace_hours = DECAY_GRACE_MONTHS * 30.44 * 24
    if age_hours <= grace_hours:
        return 1.0
    return math.exp(-lam * (age_hours - grace_hours))


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
    """Return symbols that have at least one unscored article.

    Priority-queue symbols are ALWAYS included (regardless of exchange filter
    and LIMIT) so they actually get scored. Non-priority symbols obey
    exchange + LIMIT.
    """
    SELECT_COLS = """
        s.id, s.symbol,
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
        s.symbol_master_summary,
        s.ai_sector_pick
    """

    # ── 1) Priority symbols (no exchange/limit filter — user explicitly queued)
    priority_ids: list[int] = []
    priority_rows: list[dict] = []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol_id FROM priority_queue ORDER BY rank ASC")
            priority_ids = [r[0] for r in cur.fetchall()]
        if priority_ids:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT {SELECT_COLS}
                    FROM symbols s
                    WHERE s.id = ANY(%s)
                      AND EXISTS (
                          SELECT 1 FROM news_articles na
                          WHERE na.symbol_id = s.id AND na.sentiment_score IS NULL
                      )
                """, (priority_ids,))
                cols = [d[0] for d in cur.description]
                rows_by_id = {r[0]: dict(zip(cols, r)) for r in cur.fetchall()}
            # Preserve priority rank order
            priority_rows = [rows_by_id[sid] for sid in priority_ids if sid in rows_by_id]
    except Exception as e:
        logger.warning(f"[priority_queue] read failed: {e}")
        priority_ids = []

    # ── 2) Normal symbols (exchange + limit)
    with conn.cursor() as cur:
        q = f"""
            SELECT DISTINCT {SELECT_COLS}
            FROM symbols s
            WHERE s.exchange = %s
              AND s.status = TRUE
              AND EXISTS (
                  SELECT 1 FROM news_articles na
                  WHERE na.symbol_id = s.id AND na.sentiment_score IS NULL
              )
        """
        params: list = [exchange]
        if priority_ids:
            q += " AND NOT (s.id = ANY(%s))"
            params.append(priority_ids)
        if limit:
            q += f" LIMIT {limit}"
        cur.execute(q, tuple(params))
        cols = [d[0] for d in cur.description]
        normal_rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    import random
    random.shuffle(normal_rows)

    rows = priority_rows + normal_rows
    # Hard cap total worklist size to the user's SYMBOL_LIMIT. Priority rows are kept first.
    if limit and len(rows) > limit:
        rows = rows[:limit]
    if priority_rows:
        logger.debug(f"[priority_queue] {len(priority_rows)} priority symbol(s) at head of run: "
                     f"{[r['symbol'] for r in priority_rows]}")
    return rows


def _get_articles_for_symbol(conn, symbol_id: int) -> list[dict]:
    """Get rolling window for scoring: newest MAX_EVAL_ARTICLES articles, oldest→newest.

    SEC-reserved slot strategy:
      - Always include most recent 10-K (if ≤18 months old) — up to 1 slot
      - Always include most recent 3 10-Qs (if ≤15 months old) — up to 3 slots
      - Always include most recent Tier-2/3 SEC filings in last 90 days — up to 2 slots
      - Remaining slots filled with standard news (current behaviour)

    This ensures fundamental filings always influence scoring even when there
    are many recent news articles that would otherwise crowd them out.
    """
    from pipeline_config import EDGAR_SEC_RESERVED_SLOTS, EDGAR_10K_STALENESS_MONTHS, EDGAR_10Q_STALENESS_MONTHS
    now = datetime.now(timezone.utc)
    cutoff_10k = now - timedelta(days=EDGAR_10K_STALENESS_MONTHS * 30.44)
    cutoff_10q = now - timedelta(days=EDGAR_10Q_STALENESS_MONTHS * 30.44)
    cutoff_t23 = now - timedelta(days=90)

    SEC_COLS = "id, title, summary, full_text, published_at, sentiment_score, pre_summary_data, form_type, sec_source_weight"

    # ── 1. Most recent 10-K (1 slot, max staleness 18 months) ────────────────
    reserved: list[dict] = []
    reserved_ids: set[int] = set()

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT {SEC_COLS}
            FROM news_articles
            WHERE symbol_id = %s
              AND form_type IN ('10-K', '10-K/A')
              AND published_at >= %s
            ORDER BY published_at DESC
            LIMIT 1
        """, (symbol_id, cutoff_10k))
        cols = [d[0] for d in cur.description]
        for r in cur.fetchall():
            row = dict(zip(cols, r))
            reserved.append(row)
            reserved_ids.add(row["id"])

    # ── 2. Most recent 3 10-Qs (up to 3 slots, max staleness 15 months) ──────
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT {SEC_COLS}
            FROM news_articles
            WHERE symbol_id = %s
              AND form_type IN ('10-Q', '10-Q/A')
              AND published_at >= %s
            ORDER BY published_at DESC
            LIMIT 3
        """, (symbol_id, cutoff_10q))
        cols = [d[0] for d in cur.description]
        for r in cur.fetchall():
            row = dict(zip(cols, r))
            if row["id"] not in reserved_ids:
                reserved.append(row)
                reserved_ids.add(row["id"])

    # ── 3. Most recent Tier-2/3 events in last 90 days (up to 2 slots) ───────
    tier23_forms = ("S-3","S-3/A","424B1","424B3","424B4","424B5",
                    "NT 10-K","NT 10-Q","SC 13D","SC 13D/A","SC 13G","SC 13G/A","4")
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT {SEC_COLS}
            FROM news_articles
            WHERE symbol_id = %s
              AND form_type = ANY(%s)
              AND published_at >= %s
            ORDER BY published_at DESC
            LIMIT 2
        """, (symbol_id, list(tier23_forms), cutoff_t23))
        cols = [d[0] for d in cur.description]
        for r in cur.fetchall():
            row = dict(zip(cols, r))
            if row["id"] not in reserved_ids:
                reserved.append(row)
                reserved_ids.add(row["id"])

    # ── 4. News articles fill remaining slots ─────────────────────────────────
    news_limit = max(1, MAX_EVAL_ARTICLES - len(reserved))
    excl = list(reserved_ids) if reserved_ids else [-1]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT {SEC_COLS}
            FROM news_articles
            WHERE symbol_id = %s
              AND (form_type IS NULL OR form_type NOT IN (
                  '10-K','10-K/A','10-Q','10-Q/A',
                  'S-3','S-3/A','424B1','424B3','424B4','424B5',
                  'NT 10-K','NT 10-Q','SC 13D','SC 13D/A','SC 13G','SC 13G/A','4',
                  '8-K','8-K/A'
              ))
              AND id != ALL(%s)
            ORDER BY published_at DESC
            LIMIT %s
        """, (symbol_id, excl, news_limit))
        cols = [d[0] for d in cur.description]
        news_rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    # Also grab recent 8-K filings not in reserved (they fit in the news slots)
    eight_k_limit = max(0, news_limit - len(news_rows))
    if eight_k_limit > 0:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT {SEC_COLS}
                FROM news_articles
                WHERE symbol_id = %s
                  AND form_type IN ('8-K', '8-K/A')
                  AND id != ALL(%s)
                ORDER BY published_at DESC
                LIMIT %s
            """, (symbol_id, excl, eight_k_limit))
            cols = [d[0] for d in cur.description]
            for r in cur.fetchall():
                row = dict(zip(cols, r))
                if row["id"] not in reserved_ids:
                    news_rows.append(row)

    # ── 5. Merge: reserved (SEC) first, then news, sorted oldest→newest ───────
    all_rows = reserved + news_rows
    all_rows.sort(key=lambda x: x["published_at"])
    return all_rows


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


def _load_all_sectors(conn) -> list[dict]:
    """Load all sectors_macro rows for AI sector picking. Returns list of dicts."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sector_name, industry_name, macro_multiplier
            FROM sectors_macro
            ORDER BY sector_name, industry_name
        """)
        rows = cur.fetchall()
    return [{"sector_name": r[0], "industry_name": r[1], "macro_multiplier": float(r[2])} for r in rows]


def _call_ai_sector_pick(main_client, symbol: str, master_summary: str,
                          sectors: list, hint_candidates: list[str] | None = None) -> str:
    """
    Two-stage sector picker that survives a 672-entry catalog at 16k ctx.

    Stage A: pick the sector_name from the ~164 distinct sectors (small list, fast).
    Stage B: pick the industry_name from only the industries inside that sector
             (typically 1-200 entries, but bounded to a single sector).

    `hint_candidates` is an optional list of per-article `extracted_facts.
    ai_sector_pick_hint` values from Stage 2. Most common hint is shown as
    a soft guide in both stages.

    Returns 'sector_name | industry_name' verbatim from the catalog, or
    empty string on failure.
    """
    if not sectors or not master_summary:
        return ""

    # Aggregate hints (top-3 most common, used in both stages).
    hint_lines = ""
    if hint_candidates:
        from collections import Counter
        cnt = Counter(h.strip() for h in hint_candidates if h and h.strip())
        if cnt:
            top = cnt.most_common(3)
            hint_lines = ("\nHints from per-article extraction (soft guide, final must be from list):\n"
                          + "\n".join(f"  - {label} (×{n})" for label, n in top) + "\n")

    # Distinct sector list (preserves insertion order via dict)
    sector_index: dict[str, list[dict]] = {}
    for s in sectors:
        sector_index.setdefault(s["sector_name"], []).append(s)
    sector_names = list(sector_index.keys())

    def _ollama_kwargs(prompt: str, max_out: int) -> dict:
        kw = dict(
            model=LLM_CONFIG["model"],
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            top_p=1.0,
            max_tokens=max_out,
            seed=42,
        )
        if LLM_TYPE in ("ollama", "local"):
            kw["extra_body"] = {
                "num_ctx":     LLM_CONFIG.get("context_size", 16384),
                "num_predict": max_out,
                "top_k":       1,
                "seed":        42,
                "format":      "json",
            }
            kw.pop("seed", None)
        elif LLM_TYPE in ("api", "anthropic"):
            kw["response_format"] = {"type": "json_object"}
        return kw

    # Cap master_summary so the prompt fits even when sector list is long.
    ms = (master_summary or "")[:4000]

    # ── Stage A: pick the sector_name ────────────────────────────────────────
    sec_list = "\n".join(f"- {n}" for n in sector_names)
    prompt_a = (
        f"You are a financial sector classifier. Pick the SINGLE best matching "
        f"sector for the company below.\n\n"
        f"Symbol: {symbol}\n\n"
        f"Company Summary:\n{ms}\n\n"
        f"Sectors (pick ONE):\n{sec_list}\n"
        f"{hint_lines}\n"
        f"Return ONLY JSON: {{\"sector\": \"<sector_name>\"}}\n"
        f"Rules: exact verbatim string from list, no explanation, no markdown."
    )
    chosen_sector: str = ""
    try:
        # Gemma reasoning models consume ~500-1000 hidden tokens before output —
        # need a generous num_predict or the JSON never gets emitted.
        resp = main_client.chat.completions.create(**_ollama_kwargs(prompt_a, 1536))
        raw = (resp.choices[0].message.content or "").strip()
        parsed = _extract_json(raw) or {}
        chosen_sector = (parsed.get("sector") or "").strip()
    except Exception as e:
        logger.warning(f"[{symbol}] ai_sector_pick stageA failed: {e}")

    # Snap stage A output to a real sector_name
    if chosen_sector:
        # exact, then case-insensitive, then substring
        lookup = {n.lower(): n for n in sector_names}
        sec_lower = chosen_sector.lower()
        if sec_lower in lookup:
            chosen_sector = lookup[sec_lower]
        else:
            hits = [n for n in sector_names if sec_lower in n.lower() or n.lower() in sec_lower]
            chosen_sector = hits[0] if hits else ""

    # Fallback: try the hint's sector half if stage A produced nothing.
    if not chosen_sector and hint_candidates:
        for h in hint_candidates:
            head = (h.split("|", 1)[0] if "|" in h else h).strip()
            for n in sector_names:
                if head.lower() == n.lower() or head.lower() in n.lower():
                    chosen_sector = n
                    break
            if chosen_sector:
                break

    if not chosen_sector:
        logger.warning(f"[{symbol}] ai_sector_pick stageA returned no sector — skipping")
        return ""

    industries_in_sector = sector_index[chosen_sector]

    # ── Stage B: pick the industry_name within the chosen sector ──────────────
    if len(industries_in_sector) == 1:
        only = industries_in_sector[0]
        pick = f"{only['sector_name']} | {only['industry_name']}"
        logger.info(f"[{symbol}] ai_sector_pick → '{pick}' (single industry in sector)")
        return pick

    ind_list = "\n".join(f"[{i+1}] {s['industry_name']}" for i, s in enumerate(industries_in_sector))
    prompt_b = (
        f"Within the '{chosen_sector}' sector, pick the SINGLE best matching "
        f"industry for the company below.\n\n"
        f"Symbol: {symbol}\n\n"
        f"Company Summary:\n{ms}\n\n"
        f"Industries inside '{chosen_sector}' (pick ONE):\n{ind_list}\n"
        f"{hint_lines}\n"
        f"Return ONLY JSON: {{\"industry\": \"<industry_name>\"}}\n"
        f"Rules: exact verbatim string, no leading '[N]' index, no explanation."
    )
    chosen_industry: str = ""
    try:
        resp = main_client.chat.completions.create(**_ollama_kwargs(prompt_b, 1536))
        raw = (resp.choices[0].message.content or "").strip()
        parsed = _extract_json(raw) or {}
        chosen_industry = (parsed.get("industry") or "").strip()
    except Exception as e:
        logger.warning(f"[{symbol}] ai_sector_pick stageB failed: {e}")

    # Snap stage B output to a real industry_name inside chosen_sector
    pick = ""
    if chosen_industry:
        import re as _re
        chosen_industry = _re.sub(r"^\[\d+\]\s*", "", chosen_industry).strip("\"'")
        lookup = {s["industry_name"].lower(): s["industry_name"] for s in industries_in_sector}
        ci_lower = chosen_industry.lower()
        snapped = lookup.get(ci_lower)
        if not snapped:
            # substring within this sector only
            hits = [s["industry_name"] for s in industries_in_sector
                    if ci_lower in s["industry_name"].lower() or s["industry_name"].lower() in ci_lower]
            if hits:
                snapped = max(hits, key=len)
        if snapped:
            pick = f"{chosen_sector} | {snapped}"

    # Fallback: if stage B failed, take the shortest industry name in this
    # sector (usually the most generic, e.g. just the sector again).
    if not pick:
        fallback = min(industries_in_sector, key=lambda s: len(s["industry_name"]))
        pick = f"{chosen_sector} | {fallback['industry_name']}"
        logger.info(f"[{symbol}] ai_sector_pick stageB miss — using shortest industry '{pick}'")
    else:
        logger.info(f"[{symbol}] ai_sector_pick → '{pick}'")

    return pick


def _snap_sector_pick(raw_pick: str, sectors: list) -> str:
    """
    Force-validate the LLM's sector pick against the actual sectors_macro catalog.

    Accepts a variety of separators ('|', '/', '-', '>', ':', ',') and snaps
    to the catalog entry whose 'sector_name | industry_name' best matches.

    Returns the canonical 'sector_name | industry_name' string from the
    catalog, or empty string if no plausible match.
    """
    if not raw_pick or not sectors:
        return ""
    import re as _re

    # Strip JSON fences, quotes, leading "[N]" indices
    raw = raw_pick.strip()
    raw = _re.sub(r"^\[\d+\]\s*", "", raw)
    raw = raw.strip(" \t\r\n\"'")

    # Build canonical labels and case-folded lookup
    canon = [f"{s['sector_name']} | {s['industry_name']}" for s in sectors]
    canon_lower = {c.lower(): c for c in canon}

    # Direct verbatim hit
    if raw.lower() in canon_lower:
        return canon_lower[raw.lower()]

    # Normalize separators: replace any of `/`, `>`, `-`, `:`, `,` between
    # the two parts with a pipe.
    norm = _re.sub(r"\s*[|/>:,\-]\s*", " | ", raw, count=1)
    if norm.lower() in canon_lower:
        return canon_lower[norm.lower()]

    # Split on the first separator, try (sector, industry) exact match
    parts = _re.split(r"\s*[|/>:,\-]\s*", raw, maxsplit=1)
    sector = parts[0].strip().lower() if parts else ""
    industry = parts[1].strip().lower() if len(parts) > 1 else ""

    if sector and industry:
        for s in sectors:
            if (s["sector_name"].lower() == sector
                    and s["industry_name"].lower() == industry):
                return f"{s['sector_name']} | {s['industry_name']}"

    # Industry-only exact match (LLM dropped sector)
    if industry or sector:
        needle = industry or sector
        for s in sectors:
            if s["industry_name"].lower() == needle:
                return f"{s['sector_name']} | {s['industry_name']}"

    # Substring match on industry_name (most-specific wins)
    if industry or sector:
        needle = industry or sector
        cand = [s for s in sectors if needle in s["industry_name"].lower()]
        if not cand and sector:
            cand = [s for s in sectors if sector in s["sector_name"].lower()]
        if cand:
            # Prefer longest industry_name (more specific)
            best = max(cand, key=lambda s: len(s["industry_name"]))
            return f"{best['sector_name']} | {best['industry_name']}"

    return ""


def _resolve_ai_sector_multiplier(conn, ai_sector_pick: str) -> float:
    """
    Given 'sector_name | industry_name' string from LLM, find the matching
    macro_multiplier. Falls back to 1.000 if not found.
    """
    if not ai_sector_pick:
        return 1.000
    parts = [p.strip() for p in ai_sector_pick.split("|")]
    sector = parts[0] if len(parts) > 0 else ""
    industry = parts[1] if len(parts) > 1 else ""
    with conn.cursor() as cur:
        # Try exact match on both
        cur.execute("""
            SELECT macro_multiplier FROM sectors_macro
            WHERE sector_name ILIKE %s AND industry_name ILIKE %s
            LIMIT 1
        """, (sector, industry))
        row = cur.fetchone()
        if row:
            return float(row[0])
        # Fallback: match sector_name only, take MAX
        if sector:
            cur.execute("""
                SELECT COALESCE(MAX(macro_multiplier), 1.000) FROM sectors_macro
                WHERE sector_name ILIKE %s
            """, (f"%{sector}%",))
            row = cur.fetchone()
            if row:
                return float(row[0])
    return 1.000


def _save_article_result(cur, article_id: int, result: dict,
                         master_snapshot: str, pre_summary: Optional[dict],
                         published_at: datetime, stage2_prompt: str = ""):
    # Relevance gate: LLM may set is_relevant=false for off-topic articles.
    # When false: force score to 0, weighted to 0, so downstream aggregation
    # ignores it. Article row still saved (audit trail), but excluded from
    # weighted-mean and counted as filtered in the viewer.
    is_relevant_raw = result.get("is_relevant")
    if isinstance(is_relevant_raw, str):
        is_relevant = is_relevant_raw.strip().lower() in ("true", "1", "yes")
    elif is_relevant_raw is None:
        is_relevant = True  # default: legacy responses without the field stay in
    else:
        is_relevant = bool(is_relevant_raw)
    relevance_reason = str(result.get("relevance_reason") or "")[:1000]

    score = float(result.get("sentiment_score", 0.0))
    outlook_bonus = float(result.get("outlook_bonus", 0.0))
    # Hard cap at 0.03: outlook_bonus is a small forward-looking nudge only.
    # The original 0.15 ceiling caused ALL positive articles to cluster at 0.999
    # because any score ≥ 0.969 (STRONG+) + even a small bonus hit the cap.
    # At 0.03 the bonus stays within one band step and cannot cross band ceilings.
    outlook_bonus = max(0.0, min(0.03, outlook_bonus))

    # ── Band-ceiling enforcement ───────────────────────────────────────────────
    # The outlook_bonus MUST NOT push the score past the top of the LLM's chosen
    # band.  Without this, a VERY_STRONG article (ceiling 0.984) + 0.01 bonus
    # silently lands in EXCEPTIONAL territory (0.985+), violating the regulatory
    # ladder (BLA acceptance → max STRONG, not EXCEPTIONAL).
    #
    # Band ceilings (positive side) — must stay in sync with stage2_instruction.json.
    # ORDER MATTERS: longer/more-specific labels before shorter ones so substring
    # matching works correctly (e.g. WEAK_POSITIVE before POSITIVE, VERY_STRONG
    # before STRONG, SLIGHTLY_POSITIVE before POSITIVE).
    _BAND_CEILINGS = [
        ("EPOCH_DEFINING",   1.000),
        ("TRANSFORMATIVE",   0.999),
        ("EXCEPTIONAL",      0.994),
        ("VERY_STRONG",      0.984),
        ("CLEARLY_POSITIVE", 0.939),
        ("STRONG",           0.969),
        ("SLIGHTLY_POSITIVE",0.799),
        ("WEAK_POSITIVE",    0.599),
        ("POSITIVE",         0.899),
        ("NEUTRAL",          0.000),
    ]
    # Parse anchor band from score_rationale (e.g. "VERY_STRONG band; ...")
    _rationale = str(result.get("score_rationale") or "").upper()
    _band_ceiling = None
    for _band_label, _ceil in _BAND_CEILINGS:
        if _band_label in _rationale:
            _band_ceiling = _ceil
            break
    # ── ──────────────────────────────────────────────────────────────────────

    if is_relevant:
        if score < 1.0:
            # Outlook bonus can improve the score but must not silently push a
            # sub-EPOCH_DEFINING article to 1.0 via arithmetic overflow.
            # Only the model may output 1.0 directly (after passing the
            # EPOCH_DEFINING litmus test in the instruction).  Post-processing
            # caps at 0.999 so the distinction is preserved in the DB.
            score = min(0.999, score + outlook_bonus)
            # Also enforce band ceiling so bonus never crosses into a higher band.
            if _band_ceiling is not None and score > 0.0:
                score = min(score, _band_ceiling)
        # score == 1.0 → model explicitly chose EPOCH_DEFINING; keep as-is.
        score = max(-1.0, score)
        weighted = _time_decay(score, published_at)
    else:
        score = 0.0
        weighted = 0.0

    def _to_str(v, limit):
        if v is None:
            return ""
        if isinstance(v, (dict, list)):
            v = json.dumps(v)
        return str(v)[:limit]

    updates = {
        "sentiment_score":         score,
        "weighted_sentiment":      weighted,
        "outlook_bonus":           outlook_bonus,
        "is_relevant":             is_relevant,
        "relevance_reason":        relevance_reason,
        "article_summary":         _to_str(result.get("article_summary"), 500),
        "master_summary_snapshot": master_snapshot,
        "key_events":              json.dumps(result.get("key_events") or {}),
        "extracted_facts":         json.dumps(result.get("extracted_facts") or {}),
        "ai_sector_pick_hint":     _to_str((result.get("extracted_facts") or {}).get("ai_sector_pick_hint"), 200),
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
            outlook_bonus           = %(outlook_bonus)s,
            is_relevant             = %(is_relevant)s,
            relevance_reason        = %(relevance_reason)s,
            article_summary         = %(article_summary)s,
            master_summary_snapshot = %(master_summary_snapshot)s,
            key_events              = %(key_events)s::jsonb,
            extracted_facts         = %(extracted_facts)s::jsonb,
            ai_sector_pick_hint     = %(ai_sector_pick_hint)s,
            pre_summary_data        = COALESCE(%(pre_summary_data)s::jsonb, pre_summary_data),
            score_rationale         = %(score_rationale)s,
            forecast_until_earnings = %(forecast_until_earnings)s,
            stage2_prompt           = %(stage2_prompt)s,
            company_connections     = %(company_connections)s::jsonb
        WHERE id = %(id)s
    """, {**updates, "id": article_id,
          "pre_summary_data": json.dumps(pre_summary) if pre_summary else None})

    # Log scoring event (resource manager throughput tracking)
    try:
        cur.execute(
            "INSERT INTO scoring_events (kind, symbol_id) "
            "SELECT 'article', symbol_id FROM news_articles WHERE id = %s",
            (article_id,)
        )
    except Exception:
        pass

    # Remove from active_article (this article is now finished)
    try:
        cur.execute("DELETE FROM active_article WHERE article_id = %s", (article_id,))
        cur.execute("NOTIFY active_article_changed")
    except Exception:
        pass


def _get_sec_modifier(conn, symbol_id: int) -> float:
    """Sum all active sec_signals modifiers for this symbol (last 180 days).
    Returns a value capped at ±0.20. Returns 0.0 if no signals or table missing.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(SUM(score_modifier), 0.0)
                FROM sec_signals
                WHERE symbol_id = %s
                  AND is_active  = TRUE
                  AND filed_at  > NOW() - INTERVAL '180 days'
            """, (symbol_id,))
            row = cur.fetchone()
        return round(float(row[0]) if row else 0.0, 4)
    except Exception as e:
        logger.debug(f"[sec_modifier] read failed (non-fatal): {e}")
        return 0.0


def _apply_asymptotic_multiplier(base: float, multiplier: float,
                                  threshold: float = ASYMPTOTE_THRESHOLD) -> float:
    """Apply sector/macro multiplier with asymptotic compression near ±1.

    For |base| <= threshold: plain linear scaling (multiplier*base) — no distortion
    for mid-range scores.  For |base| > threshold: the remaining headroom to ±1 is
    compressed so the multiplier can never push the score into hard-clip territory.

    Formula (positive side):
        headroom = 1.0 - base
        effective = 1.0 - headroom * (2.0 - multiplier)
        i.e.  multiplier=1.042 compresses 0.9923 → ~0.9950 instead of clipping 1.034.

    Symmetric for negative side.  Falls back to linear for |base| <= threshold.
    Always returns a value in [-1.0, 1.0].
    """
    if base == 0.0:
        return 0.0

    sign = 1.0 if base > 0.0 else -1.0
    abs_base = abs(base)

    if abs_base <= threshold:
        # Linear region — clamp to avoid overflow with extreme multipliers
        return max(-1.0, min(1.0, base * multiplier))

    # Clamp multiplier so we can't accidentally invert direction
    # (multiplier >2.0 would flip the headroom term negative)
    m = max(0.5, min(multiplier, 2.0))
    headroom = 1.0 - abs_base
    compressed = 1.0 - headroom * (2.0 - m)
    return sign * max(abs_base, min(1.0, compressed))


def _apply_gravity(score: float, newest_material_pub: Optional[datetime],
                   gamma: float = GRAVITY_GAMMA,
                   grace_days: float = GRAVITY_GRACE_DAYS) -> float:
    """Apply inactivity gravity: penalty based on age of the newest material article.

    Inside grace_days: no penalty (symbol is active).
    Beyond grace_days: score *= exp(-gamma * hours_past_grace).
    gamma=0.0005/hr → half-life ≈ 58 days after grace window.

    Score sign is preserved; zero-score symbols unchanged.
    """
    if not newest_material_pub or gamma == 0.0 or score == 0.0:
        return score
    now = datetime.now(timezone.utc)
    pub = newest_material_pub
    if pub.tzinfo is None:
        pub = pub.replace(tzinfo=timezone.utc)
    age_hours = max(0.0, (now - pub).total_seconds() / 3600.0)
    grace_hours = grace_days * 24.0
    if age_hours <= grace_hours:
        return score
    return round(score * math.exp(-gamma * (age_hours - grace_hours)), 6)


def _save_symbol_scores(cur, symbol_id: int, master_summary: str,
                        forecast: str, weighted_scores: list[float],
                        macro_multiplier: float, ai_sector_pick: str = "",
                        ai_sector_multiplier: float = 1.000,
                        raw_weight_pairs: list[tuple[float, float]] | None = None,
                        newest_material_pub: Optional[datetime] = None,
                        conn=None):
    """Compute and store final_score using selectivity-enhanced aggregation.

    Math pipeline:
      1. Weighted mean:  avg = Σ(raw_i * w_i) / Σ(w_i)
      2. Asymptotic multiplier application (prevents hard-clip at ±1 from sector boosts)
      3. Gravity penalty (inactivity: score decays if newest catalyst is weeks old)
      4. Clamp [-1, 1]
    """
    if not weighted_scores:
        return
    # ── Step 1: weighted mean ──────────────────────────────────────────────────
    if raw_weight_pairs:
        total_w = sum(w for _, w in raw_weight_pairs)
        if total_w > 0:
            avg_weighted = sum(r * w for r, w in raw_weight_pairs) / total_w
        else:
            avg_weighted = sum(weighted_scores) / len(weighted_scores)
    else:
        avg_weighted = sum(weighted_scores) / len(weighted_scores)

    # ── Step 2: asymptotic sector × macro multiplier ───────────────────────────
    combined_mult = macro_multiplier * ai_sector_multiplier
    final_score = _apply_asymptotic_multiplier(avg_weighted, combined_mult)

    # ── Step 3: gravity — inactivity penalty on symbol level ──────────────────
    final_score = _apply_gravity(final_score, newest_material_pub)

    # ── Step 3b: SEC deterministic signal modifier ─────────────────────────────
    # Additive nudge from rule-based signals: NT delays, activist entry,
    # insider buying, dilution events etc. Capped at ±0.20 in sec_signals table.
    sec_mod = _get_sec_modifier(conn, symbol_id)
    if sec_mod != 0.0:
        final_score = max(-1.0, min(1.0, final_score + sec_mod))
        logger.debug(f"[save_scores] symbol_id={symbol_id} sec_modifier={sec_mod:+.4f} → {final_score:.4f}")

    # ── Step 4: clamp ─────────────────────────────────────────────────────────
    final_score = round(max(-1.0, min(1.0, final_score)), 6)

    cur.execute("""
        UPDATE symbols SET
            symbol_master_summary      = %s,
            symbol_forecast_narrative  = %s,
            final_score                = %s,
            ai_sector_pick             = %s,
            ai_sector_multiplier       = %s,
            score_updated_at           = NOW()
        WHERE id = %s
    """, (master_summary[:4000] if master_summary else None,
          forecast[:2000] if forecast else None,
          final_score,
          ai_sector_pick or None,
          ai_sector_multiplier,
          symbol_id))

    # Log scoring event (resource manager throughput tracking)
    try:
        cur.execute(
            "INSERT INTO scoring_events (kind, symbol_id) VALUES ('symbol', %s)",
            (symbol_id,)
        )
    except Exception:
        pass


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
    s1_pool: "ThreadPoolExecutor | None" = None,
) -> dict:
    symbol   = sym["symbol"]
    sym_id   = sym["id"]
    industry = sym.get("industry") or ""

    articles = _get_articles_for_symbol(conn, sym_id)
    if not articles:
        return {"symbol": symbol, "scored": 0, "skipped": 0}

    macro_mult          = _get_macro_multiplier(conn, industry)
    master_summary      = sym.get("symbol_master_summary") or ""
    last_score          = sym.get("final_score") or 0.0
    last_article_summary = None
    weighted_scores     = []
    raw_weight_pairs: list[tuple[float, float]] = []  # (raw_score, decay_weight) — for true weighted mean
    ai_sector_hints: list[str] = []  # per-article extracted_facts.ai_sector_pick_hint values
    last_forecast       = ""
    scored  = 0
    skipped = 0
    newest_material_pub: Optional[datetime] = None  # pub date of newest article above MATERIALITY_THRESHOLD

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

    # ── Stage 1 prefetch pool ─────────────────────────────────────────────────
    # Pipeline pattern: S1[n+1] runs in background while S2[n] scores.
    # All S1 futures are submitted upfront; S2 loop calls .result() in order
    # (returns immediately if S1 already done, blocks briefly if still running).
    # Stage 2 stays sequential — master_summary chain requires ordered processing.
    # Use shared pool if provided (so total S1 concurrency = STAGE1_PARALLEL_WORKERS
    # globally, not per-symbol). Fall back to per-symbol pool for back-compat.
    _own_pool = s1_pool is None
    _s1_pool = s1_pool or ThreadPoolExecutor(max_workers=max(1, STAGE1_PARALLEL_WORKERS))

    def _submit_s1(article):
        """Submit one article's Stage 1 task. Returns Future[(result, elapsed)]."""
        # Mark as Stage 1 active (yellow in admin view)
        try:
            with conn.cursor() as _ac:
                _ac.execute(
                    "INSERT INTO active_article (article_id, symbol_id, stage) VALUES (%s, %s, 1) "
                    "ON CONFLICT (article_id) DO UPDATE SET stage = 1, started_at = NOW()",
                    (article["id"], sym_id)
                )
                _ac.execute("NOTIFY active_article_changed")
            conn.commit()
        except Exception as _e:
            logger.warning(f"[active_article] S1 mark failed for art={article.get('id')}: {_e}")
            try: conn.rollback()
            except Exception: pass

        # Reuse cached pre_summary ONLY when stage 1 is enabled. When the user
        # disables ENABLE_PRE_SUMMARIZATION we want the raw article text to
        # go directly to stage 2 — ignore any stale cache from prior runs.
        if ENABLE_PRE_SUMMARIZATION and article.get("pre_summary_data"):
            return _s1_pool.submit(lambda c=article["pre_summary_data"]: (c, 0.0))
        if ENABLE_PRE_SUMMARIZATION:
            raw_text = (article.get("full_text") or article.get("summary") or "")
            if raw_text.strip():
                def _run(text=raw_text):
                    t0 = time.time()
                    r = _call_stage1(
                        summary_client, text,
                        model_override=LLM_CONFIG["model"] if single_model_mode else None,
                    )
                    return r, round(time.time() - t0, 1)
                return _s1_pool.submit(_run)
        return _s1_pool.submit(lambda: (None, 0.0))

    stage1_futures = {a["id"]: _submit_s1(a) for a in articles}
    if _own_pool:
        _s1_pool.shutdown(wait=False)  # no new submissions; running tasks continue in background

    # ── Stage 2 scoring (sequential — stateful master_summary chain) ──────────
    for idx, article in enumerate(articles):
        art_id      = article["id"]
        published_at = article["published_at"]

        # Get date-matched daily snapshot for this article; fall back to symbols row
        daily_snap = _get_daily_snapshot(conn, sym_id, published_at)
        tv_snapshot = daily_snap if daily_snap else tv_snapshot_fallback

        # Already scored — use its weighted score for final calc but skip LLM.
        # IMPORTANT: still propagate this article's stored master_summary_snapshot
        # and forecast into the running tracker so the symbol-level save at the
        # end of the loop ends up with real content (instead of NULL when ALL
        # articles in this run were already scored).
        if article["sentiment_score"] is not None:
            _raw_prev = float(article["sentiment_score"])
            # Pull cached snapshot fields so the tracker doesn't stay empty.
            try:
                with conn.cursor() as _cur:
                    _cur.execute(
                        "SELECT master_summary_snapshot, forecast_until_earnings, "
                        "article_summary "
                        "FROM news_articles WHERE id = %s",
                        (art_id,),
                    )
                    _row = _cur.fetchone()
                if _row:
                    _ms, _fc, _as = _row
                    if _ms:
                        master_summary = _ms
                    if _fc:
                        last_forecast = _fc if isinstance(_fc, str) else str(_fc)
                    if _as:
                        last_article_summary = _as
                    last_score = _raw_prev
            except Exception:
                pass
            if abs(_raw_prev) < MATERIALITY_THRESHOLD:
                skipped += 1
                continue  # noise filter — below materiality threshold, exclude from mean
            _w_prev = _decay_weight(published_at)
            weighted_scores.append(_raw_prev * _w_prev)
            raw_weight_pairs.append((_raw_prev, _w_prev))
            # Track newest material pub for gravity calculation (articles oldest→newest)
            if newest_material_pub is None or published_at > newest_material_pub:
                newest_material_pub = published_at
            skipped += 1
            continue

        stage1_result, t_s1 = stage1_futures[art_id].result()  # wait if S1 still running

        # ── Pause check — BEFORE calling Stage 2 ──────────────────────────────
        # Checked after S1 completes (S1 is fast / cached) but before the slow S2
        # LLM call, so the article stays unscored and will be re-evaluated on resume.
        if _is_paused():
            logger.info(f"[{symbol}] Scoring paused — stopping before article {art_id}")
            raise ScoringPaused(f"Paused before article {art_id}")

        # Previous article's DB summary (article_summary from last scored article)
        prev_art_summary = last_article_summary if idx > 0 else None

        # Stage 2: stateful scoring
        prompt = _build_stage2_prompt(
            symbol, tv_snapshot, article,
            master_summary, last_score, stage1_result,
            previous_article_summary=prev_art_summary,
        )
        # Upgrade to Stage 2 active (green in admin view)
        try:
            with conn.cursor() as _ac:
                _ac.execute(
                    "INSERT INTO active_article (article_id, symbol_id, stage) VALUES (%s, %s, 2) "
                    "ON CONFLICT (article_id) DO UPDATE SET stage = 2, started_at = NOW()",
                    (art_id, sym_id)
                )
                _ac.execute("NOTIFY active_article_changed")
            conn.commit()
        except Exception:
            try: conn.rollback()
            except Exception: pass

        _t0 = time.time()
        result = _call_stage2(main_client, prompt,
                              prev_master_summary=master_summary,
                              system_prompt=_get_stage2_system(article.get("form_type")))
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
        _ob = float(result.get("outlook_bonus", 0.0))
        _ob = max(0.0, min(0.15, _ob))
        raw_score = max(-1.0, min(1.0, raw_score + _ob))

        # Relevance gate matches _save_article_result. Off-topic articles get
        # saved with is_relevant=false but excluded from the aggregate score.
        _rel_raw = result.get("is_relevant")
        if isinstance(_rel_raw, str):
            _is_relevant = _rel_raw.strip().lower() in ("true", "1", "yes")
        elif _rel_raw is None:
            _is_relevant = True
        else:
            _is_relevant = bool(_rel_raw)

        weighted  = _time_decay(raw_score, published_at) if _is_relevant else 0.0
        decay_w   = _decay_weight(published_at)

        # Save to DB
        with conn.cursor() as cur:
            _save_article_result(cur, art_id, result,
                                 master_summary, stage1_result, published_at,
                                 stage2_prompt=prompt)
        conn.commit()
        master_summary      = result.get("updated_master_summary") or master_summary if _is_relevant else master_summary
        _fc = result.get("forecast_until_earnings")
        last_forecast       = str(_fc) if _fc and not isinstance(_fc, str) else (_fc or last_forecast)
        last_score          = raw_score
        last_article_summary = result.get("article_summary") or None
        if _is_relevant:
            weighted_scores.append(weighted)
            _hint = (result.get("extracted_facts") or {}).get("ai_sector_pick_hint")
            if _hint and isinstance(_hint, str):
                ai_sector_hints.append(_hint)
            if abs(raw_score) >= MATERIALITY_THRESHOLD:
                # Apply sec_source_weight so Tier-1 filings (10-K=2.5×, 10-Q=2.0×)
                # count proportionally more in the final weighted mean.
                _sec_w = float(article.get("sec_source_weight") or 1.0)
                raw_weight_pairs.append((raw_score, decay_w * _sec_w))
                # Track newest material pub for gravity calculation
                if newest_material_pub is None or published_at > newest_material_pub:
                    newest_material_pub = published_at
        scored += 1

        title_short = (article.get("title") or "")[:60]
        _rel_tag = "" if _is_relevant else " IRRELEVANT"
        logger.info(
            f"[{symbol}] article={art_id} score={raw_score:+.2f} bonus={_ob:+.2f} "
            f"s1={t_s1}s s2={t_s2}s{_rel_tag} | {title_short}"
        )

        time.sleep(0.3)  # brief pause between articles

    # Save symbol-level scores
    if weighted_scores:
        # Dedicated AI sector pick — runs once after all articles scored
        all_sectors = _load_all_sectors(conn)
        ai_sector_pick = _call_ai_sector_pick(main_client, symbol, master_summary,
                                              all_sectors, hint_candidates=ai_sector_hints)
        ai_sector_mult = _resolve_ai_sector_multiplier(conn, ai_sector_pick)
        with conn.cursor() as cur:
            _save_symbol_scores(cur, sym_id, master_summary,
                                last_forecast, weighted_scores, macro_mult,
                                ai_sector_pick=ai_sector_pick,
                                ai_sector_multiplier=ai_sector_mult,
                                raw_weight_pairs=raw_weight_pairs,
                                newest_material_pub=newest_material_pub,
                                conn=conn)
        conn.commit()
        avg_w = sum(weighted_scores) / len(weighted_scores)
        logger.info(f"[{symbol}] scored={scored} skipped={skipped} "
                    f"macro_mult={macro_mult:.3f} ai_sector='{ai_sector_pick}' "
                    f"ai_sector_mult={ai_sector_mult:.3f} final_score="
                    f"{round(avg_w * macro_mult * ai_sector_mult, 4)}")
    else:
        logger.info(f"[{symbol}] scored={scored} skipped={skipped} — no weighted scores")
    return {"symbol": symbol, "scored": scored, "skipped": skipped,
            "macro_multiplier": macro_mult}


# ── Public run() ─────────────────────────────────────────────────────────────

def run(exchange: str = "NASDAQ", limit: int = 0) -> dict:
    """
    Score all symbols with unscored articles.

    Effective limit (in order of precedence):
      1. explicit `limit` argument > 0  → use it
      2. otherwise SYMBOL_LIMIT from pipeline_config (int N → first N symbols,
         False / 0 / None → all)
    Worker count is auto-sized from free VRAM at startup.
    """
    # Resolve effective cap from pipeline_config when caller didn't pin one.
    effective_limit = limit
    if not effective_limit:
        # SYMBOL_LIMIT may be False, 0, None, or a positive int.
        try:
            _sl = int(SYMBOL_LIMIT) if SYMBOL_LIMIT else 0
        except (TypeError, ValueError):
            _sl = 0
        effective_limit = _sl
    if effective_limit and effective_limit > 0:
        logger.info(f"[sentiment_scoring] SYMBOL_LIMIT active — capping run to {effective_limit} symbols")

    # ── Start pause poller (syncs _PAUSE_EVENT from DB every 2 s) ────────────────
    _start_pause_poller()

    # ── Reload tier from scoring_control (set via admin while paused) ────────────
    try:
        _sc = get_conn()
        with _sc.cursor() as _cur:
            _cur.execute("SELECT active_tier FROM scoring_control WHERE id = 1")
            _sc_row = _cur.fetchone()
        _sc.close()
        if _sc_row and _sc_row[0]:
            _reload_tier(int(_sc_row[0]))
    except Exception as _sc_e:
        logger.debug(f"[scoring_control] tier check failed (non-fatal): {_sc_e}")

    started_at = datetime.now(timezone.utc)
    conn = get_conn()

    # ── Auto-dedup cross-language articles before scoring
    _dedup_languages(conn)

    # Ensure active_processing table exists + clear stale rows from prior crashes
    try:
        with conn.cursor() as _ac:
            _ac.execute("""
                CREATE TABLE IF NOT EXISTS active_processing (
                    symbol_id  INT PRIMARY KEY REFERENCES symbols(id) ON DELETE CASCADE,
                    worker_id  TEXT,
                    started_at TIMESTAMP DEFAULT NOW()
                )
            """)
            _ac.execute("TRUNCATE active_processing")
            try:
                _ac.execute("TRUNCATE active_article")
            except Exception:
                pass
        conn.commit()
    except Exception as e:
        logger.warning(f"[active_processing] init failed: {e}")
        conn.rollback()

    symbols = _get_symbols_with_unscored(conn, exchange, effective_limit)
    if not symbols:
        logger.info("[sentiment_scoring] No symbols with unscored articles.")
        conn.close()
        return {"symbols_processed": 0, "articles_scored": 0}

    logger.info(f"[sentiment_scoring] {len(symbols)} symbols to score "
                f"(pre_summarization={'ON' if ENABLE_PRE_SUMMARIZATION else 'OFF'})")

    # ── VRAM-based worker sizing ───────────────────────────────────────────────
    if SKIP_WORKER_COUNT_DETECTION:
        # n_workers = symbol-level (Stage 2) concurrency.
        # Stage 1 concurrency is enforced by the SHARED _shared_s1_pool below.
        n_workers       = max(1, STAGE2_PARALLEL_WORKERS)
        single_model_mode = False
        print(f"\n[worker sizing] SKIP_WORKER_COUNT_DETECTION=True — "
              f"stage1={STAGE1_PARALLEL_WORKERS} stage2/symbol={n_workers} "
              f"single_model_mode=False\n")
        logger.info(f"[vram] detection skipped — stage1={STAGE1_PARALLEL_WORKERS} "
                    f"stage2={n_workers} (from active tier in config.py)")
    else:
        n_workers, single_model_mode = _compute_worker_count()

    # Warmup models before spawning threads
    main_client    = _get_main_client()
    if single_model_mode:
        summary_client = main_client  # reuse same client, same model, no load/unload
        logger.info("[vram] Single-model mode: stage-1 will use stage-2 model (no swap)")
    else:
        summary_client = _get_summary_client()
    _warmup_models(main_client, summary_client)

    # Mutable containers so _drain_worker can hot-swap clients after a tier change
    _clients = {"main": main_client, "summary": summary_client}
    _active_tier_at_start = LLM_CONFIG.get("model", "")  # track for change detection

    # ── Semaphore caps concurrent symbol-level (Stage 2) work ─────────────────
    semaphore = threading.Semaphore(n_workers)

    # ── Shared Stage 1 pool ───────────────────────────────────────────────────
    # ONE pool across all symbols → enforces global STAGE1_PARALLEL_WORKERS cap.
    # Previously each symbol created its own pool → effective S1 = N_symbols × S1.
    shared_s1_pool = ThreadPoolExecutor(
        max_workers=max(1, STAGE1_PARALLEL_WORKERS),
        thread_name_prefix="s1",
    )

    # Progress tracking
    total_symbols  = [len(symbols)]  # list so workers see updates
    done_count     = [0]  # mutable counter shared across threads
    done_lock      = threading.Lock()

    def _worker(sym: dict) -> dict:
        with semaphore:
            # Each worker gets its own DB connection (thread-safe)
            w_conn = get_conn()
            # Mark symbol as actively processing (best-effort; ignore if table missing)
            try:
                with w_conn.cursor() as _ac:
                    _ac.execute(
                        "INSERT INTO active_processing (symbol_id, worker_id, started_at) "
                        "VALUES (%s, %s, NOW()) "
                        "ON CONFLICT (symbol_id) DO UPDATE SET started_at = NOW()",
                        (sym["id"], threading.current_thread().name),
                    )
                w_conn.commit()
            except Exception as e:
                logger.debug(f"[active_processing] insert failed: {e}")
                w_conn.rollback()
            try:
                result = _process_symbol(
                    w_conn, _clients["main"], _clients["summary"], sym,
                    single_model_mode, s1_pool=shared_s1_pool,
                )
            except ScoringPaused as _sp:
                logger.info(f"[{sym['symbol']}] Paused mid-symbol: {_sp}")
                result = {"symbol": sym["symbol"], "scored": 0, "skipped": 0, "paused": True}
            except Exception as e:
                logger.error(f"[{sym['symbol']}] Unhandled error: {e}", exc_info=True)
                try:
                    w_conn.rollback()
                except Exception:
                    pass
                result = {"symbol": sym["symbol"], "scored": 0, "skipped": 0}
            finally:
                # Clear active marker + remove from priority_queue if it was queued
                try:
                    with w_conn.cursor() as _ac:
                        _ac.execute("DELETE FROM active_processing WHERE symbol_id = %s", (sym["id"],))
                        _ac.execute("DELETE FROM priority_queue   WHERE symbol_id = %s", (sym["id"],))
                    w_conn.commit()
                except Exception as e:
                    logger.debug(f"[active_processing] cleanup failed: {e}")
                    try: w_conn.rollback()
                    except Exception: pass
                w_conn.close()

            with done_lock:
                done_count[0] += 1
                pct = done_count[0] / total_symbols[0] * 100
                print(
                    f"  [{done_count[0]:>{len(str(total_symbols[0]))}}/{total_symbols[0]}] "
                    f"({pct:5.1f}%)  {sym['symbol']:10s}  "
                    f"scored={result.get('scored',0)}  "
                    f"skipped={result.get('skipped',0)}",
                    flush=True,
                )
            return result

    conn.close()  # main conn no longer needed — workers use their own

    print(f"\n  Scoring {total_symbols[0]} symbols with {n_workers} parallel worker(s)...\n")

    results      = []
    total_scored = [0]
    results_lock = threading.Lock()

    # Build initial worklist from the priority-aware fetcher
    worklist: list[dict] = list(symbols)
    worklist_lock = threading.Lock()
    seen_ids: set[int] = {s["id"] for s in worklist}
    work_done = threading.Event()

    def _next_symbol() -> dict | None:
        """Pop the highest-priority symbol off the worklist.

        Priority order: any symbol present in `priority_queue` table (by rank),
        then everything else. Re-reads priority_queue each call so freshly
        rescored symbols jump the line.
        """
        with worklist_lock:
            if not worklist:
                return None
            # Read priority order from DB
            try:
                _c = get_conn()
                with _c.cursor() as _cur:
                    _cur.execute("SELECT symbol_id FROM priority_queue ORDER BY rank ASC")
                    prio_order = [r[0] for r in _cur.fetchall()]
                _c.close()
            except Exception:
                prio_order = []
            prio_set = set(prio_order)
            # Find best symbol
            best_idx = None
            best_rank = float("inf")
            for i, s in enumerate(worklist):
                if s["id"] in prio_set:
                    r = prio_order.index(s["id"])
                    if r < best_rank:
                        best_rank = r
                        best_idx = i
            if best_idx is None:
                # No priority hits → return first non-priority
                return worklist.pop(0)
            return worklist.pop(best_idx)

    def _refill():
        """Re-read DB for newly-queued symbols (rescore additions)."""
        try:
            poll_conn = get_conn()
            with poll_conn.cursor() as _pc:
                _pc.execute("SELECT symbol_id FROM priority_queue ORDER BY rank ASC")
                pq_ids = {r[0] for r in _pc.fetchall()}
            new_syms = _get_symbols_with_unscored(poll_conn, exchange, 0)
            poll_conn.close()
        except Exception as e:
            logger.debug(f"[priority_queue] refill failed: {e}")
            return 0

        added = 0
        with worklist_lock:
            worklist_ids = {s["id"] for s in worklist}
            for s in new_syms:
                sid = s["id"]
                # Skip if currently queued in this run's worklist
                if sid in worklist_ids:
                    continue
                # Skip if already submitted AND not a fresh priority entry
                if sid in seen_ids and sid not in pq_ids:
                    continue
                # Respect SYMBOL_LIMIT for non-priority refills.
                # Priority-queue entries always jump the line (user explicitly queued them).
                if effective_limit and effective_limit > 0 \
                        and sid not in pq_ids \
                        and len(seen_ids) >= effective_limit:
                    continue
                # Rescore case: previously processed but now re-queued via priority.
                # Only re-add if it actually has unscored articles AND isn't running.
                if sid in seen_ids and sid in pq_ids:
                    seen_ids.discard(sid)
                worklist.append(s)
                worklist_ids.add(sid)
                seen_ids.add(sid)
                added += 1
        if added:
            total_symbols[0] = len(seen_ids)
            logger.debug(f"[priority_queue] +{added} symbol(s) added mid-run "
                         f"(worklist now {len(worklist)})")
        return added

    # Background refill poller — fires every 3s
    def _poller():
        while not work_done.is_set():
            time.sleep(3.0)
            if work_done.is_set():
                break
            _refill()

    poller_thread = threading.Thread(target=_poller, name="pq-poller", daemon=True)
    poller_thread.start()

    # Worker loop — N workers pull symbols off worklist until empty + drained
    def _drain_worker():
        _was_paused = False
        while True:
            # Stop pulling new symbols when paused — wait until unpaused
            if _is_paused():
                _was_paused = True
                time.sleep(2.0)
                continue

            # Just unpaused — check if tier changed and rebuild clients if so
            if _was_paused:
                _was_paused = False
                try:
                    _sc_conn = get_conn()
                    with _sc_conn.cursor() as _sc_cur:
                        _sc_cur.execute("SELECT active_tier FROM scoring_control WHERE id = 1")
                        _sc_row = _sc_cur.fetchone()
                    _sc_conn.close()
                    if _sc_row and _sc_row[0]:
                        new_tier = int(_sc_row[0])
                        _reload_tier(new_tier)
                        # Rebuild clients with new base_url / api_key from updated LLM_CONFIG
                        _clients["main"]    = _get_main_client()
                        _clients["summary"] = _get_summary_client()
                        logger.info(
                            f"[tier_switch] Clients rebuilt → "
                            f"model={LLM_CONFIG['model']} "
                            f"base_url={LLM_CONFIG['base_url']}"
                        )
                        _warmup_models(_clients["main"], _clients["summary"])
                except Exception as _te:
                    logger.warning(f"[tier_switch] Failed to reload tier on resume: {_te}")

            sym = _next_symbol()
            if sym is None:
                # Worklist empty; wait briefly in case poller adds more
                time.sleep(2.0)
                sym = _next_symbol()
                if sym is None:
                    return
            r = _worker(sym)
            with results_lock:
                results.append(r)
                total_scored[0] += r.get("scored", 0)

    drain_threads = []
    for i in range(n_workers):
        t = threading.Thread(target=_drain_worker, name=f"drain-{i}", daemon=False)
        t.start()
        drain_threads.append(t)

    for t in drain_threads:
        t.join()

    work_done.set()
    shared_s1_pool.shutdown(wait=True)

    duration = (datetime.now(timezone.utc) - started_at).total_seconds()
    print(f"\n  Done — symbols={len(results)}, articles_scored={total_scored[0]}, "
          f"duration={duration:.1f}s\n")
    logger.info(f"[sentiment_scoring] Done — symbols={len(results)}, "
                f"articles_scored={total_scored[0]}, duration={duration:.1f}s")
    return {
        "symbols_processed": len(results),
        "articles_scored":   total_scored[0],
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
