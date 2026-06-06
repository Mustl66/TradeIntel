"""
config.py — TradeIntel Central Configuration
=============================================
All runtime settings live here with sensible defaults.
Override anything via environment variables or a .env file.

LLM tier system
---------------
TIER = 0  →  auto-probe tiers 1..5 in order, use first one whose endpoints respond.
TIER = N  →  force tier N (skip probing).

Each tier defines: base_url, api_key, model, stage1_base_url, stage1_api_key,
summary_model, plus shared sampling params.

Empty (None / "") fields in tiers 2..5 mark them as "not configured" → skipped
during auto-probe. Fill them in to enable.
"""

import os
import socket
import logging
from urllib.parse import urlparse
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).resolve().parent / ".env")

_log = logging.getLogger("config")

# ── Database ─────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.getenv("DB_HOST",     "localhost"),
    "port":            int(os.getenv("DB_PORT", 5432)),
    "dbname":          os.getenv("DB_NAME",     "tradeintel"),
    "user":            os.getenv("DB_USER",     "postgres"),
    "password":        os.getenv("DB_PASSWORD", "postgres"),
    "client_encoding": "utf8",
}

# ── Pipeline defaults ─────────────────────────────────────────────────────────
DEFAULT_EXCHANGE = os.getenv("DEFAULT_EXCHANGE", "NASDAQ")
SYMBOL_LIMIT     = int(os.getenv("SYMBOL_LIMIT", 0))   # 0 = all
RSS_WORKERS      = int(os.getenv("RSS_WORKERS",  8))

# ── LLM Tier system ──────────────────────────────────────────────────────────
# TIER = 0 → auto-probe in order. TIER = 1..5 → force that tier.
TIER = int(os.getenv("TIER", "2"))

# Shared sampling defaults — same across all tiers unless overridden in the tier dict
_SHARED_PARAMS = {
    "temperature":       0.1,
    "context_size":      50000,
    "max_tokens":        12228,
    "top_p":             0.9,
    "top_k":             40,
    "frequency_penalty": 0.2,
    "presence_penalty":  0.1,
    "reasoning_mode":    False,
}

_TIERS = {
    # ── Tier 1 ───────────────────────────────────────────────────────────────
    1: {
        "base_url":        "http://10.11.12.163:8000/v1",
        "api_key":         "ollama",
        "model":           "gemma-4-26B-A4B-it-oQ8-fp16",
        "stage1_base_url": "http://10.11.12.163:8000/v1",
        "stage1_api_key":  "ollama",
        "summary_model":   "gemma-4-E4B-it-oQ8",
        "stage1_workers":  15,
        "stage2_workers":  15,
    },

    # ── Tier 2 ───────────────────────────────────────────────────────────────
    2: {
        "base_url":        "http://10.11.12.163:1234/v1",
        "api_key":         "lmstudio",
        "model":           "gemma-4-26b-a4b-it-oq8",
        "stage1_base_url": "http://10.11.12.163:1234/v1",
        "stage1_api_key":  "lmstudio",
        "summary_model":   "gemma-4-26b-a4b-it-oq8",
        "stage1_workers":  15,
        "stage2_workers":  15,
    },

    # ── Tier 3 ───────────────────────────────────────────────────────────────
    3: {
        "base_url":        "http://10.11.12.163:1234/v1",
        "api_key":         "lmstudio",
        "model":           "gemma-4-26b-a4b-it-oq8",
        "stage1_base_url": "http://10.11.12.8:11434/v1",
        "stage1_api_key":  "ollama",
        "summary_model":   "gemma4:e4b-ctx16k",
        "stage1_workers":  2,
        "stage2_workers":  30,
    },

    # ── Tier 4 ───────────────────────────────────────────────────────────────
    4: {
        "base_url":        "http://10.11.12.8:11434/v1",
        "api_key":         "ollama",
        "model":           "gemma4:e4b-ctx16k",
        "stage1_base_url": "http://10.11.12.8:11434/v1",
        "stage1_api_key":  "ollama",
        "summary_model":   "gemma4:e4b-ctx16k",
        "stage1_workers":  4,
        "stage2_workers":  1,
    },

    # ── Tier 5 ───────────────────────────────────────────────────────────────
    5: {
        "base_url":        "",
        "api_key":         "ollama",
        "model":           "",
        "stage1_base_url": "",
        "stage1_api_key":  "ollama",
        "summary_model":   "",
        "stage1_workers":  4,
        "stage2_workers":  1,
    },
}


def _is_configured(tier: dict) -> bool:
    """A tier is usable if base_url, model, stage1_base_url, summary_model all set."""
    required = ("base_url", "model", "stage1_base_url", "summary_model")
    return all(tier.get(k) for k in required)


def _probe(url: str, timeout: float = 1.5) -> bool:
    """TCP-connect probe. Returns True if host:port reachable."""
    if not url:
        return False
    try:
        p = urlparse(url)
        host = p.hostname
        port = p.port or (443 if p.scheme == "https" else 80)
        if not host:
            return False
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout, ValueError):
        return False


def _resolve_tier() -> tuple[int, dict]:
    """Return (tier_num, tier_dict). Honors TIER env var; falls back to auto-probe."""
    # Forced tier
    if TIER and TIER in _TIERS:
        t = _TIERS[TIER]
        if not _is_configured(t):
            raise RuntimeError(
                f"TIER={TIER} forced but not fully configured. "
                f"Set base_url / model / stage1_base_url / summary_model in config.py."
            )
        _log.info(f"[tier] forced TIER={TIER} → base={t['base_url']} stage1={t['stage1_base_url']}")
        return TIER, t

    # Auto-probe 1..5
    for n in sorted(_TIERS.keys()):
        t = _TIERS[n]
        if not _is_configured(t):
            continue
        ok_main = _probe(t["base_url"])
        ok_s1   = _probe(t["stage1_base_url"])
        if ok_main and ok_s1:
            _log.info(f"[tier] auto-selected TIER={n} → base={t['base_url']} stage1={t['stage1_base_url']}")
            return n, t
        _log.info(f"[tier] skip TIER={n}: main={ok_main} stage1={ok_s1}")

    raise RuntimeError(
        "No usable LLM tier. None of the configured tiers responded. "
        "Check connectivity or set TIER=N to force, then fix that tier."
    )


try:
    _ACTIVE_TIER, _ACTIVE = _resolve_tier()
except RuntimeError as _e:
    # Don't break tools like admin.py that don't need LLM live at import time.
    # Fall back to tier 1 as a placeholder; LLM calls will fail later if truly down.
    _log.warning(f"[tier] {_e} — falling back to TIER=1 placeholder, LLM calls may fail.")
    _ACTIVE_TIER, _ACTIVE = 1, _TIERS[1]
ACTIVE_TIER: int = _ACTIVE_TIER

# Build LLM_CONFIG — merge shared params with the active tier, allow env overrides
LLM_CONFIG: dict = {
    **_SHARED_PARAMS,
    **_ACTIVE,
    # Per-field env overrides (highest precedence) — keep parity with old behavior
    "base_url":        os.getenv("LLM_BASE_URL",      _ACTIVE["base_url"]),
    "api_key":         os.getenv("LLM_API_KEY",       _ACTIVE["api_key"]),
    "model":           os.getenv("LLM_MODEL",         _ACTIVE["model"]),
    "stage1_base_url": os.getenv("STAGE1_BASE_URL",   _ACTIVE["stage1_base_url"]),
    "stage1_api_key":  os.getenv("STAGE1_API_KEY",    _ACTIVE["stage1_api_key"]),
    "summary_model":   os.getenv("SUMMARY_LLM_MODEL", _ACTIVE["summary_model"]),
}

# Tier-specific worker counts (env vars override)
STAGE1_PARALLEL_WORKERS: int = int(os.getenv("STAGE1_WORKERS", str(_ACTIVE.get("stage1_workers", 4))))
STAGE2_PARALLEL_WORKERS: int = int(os.getenv("STAGE2_WORKERS", str(_ACTIVE.get("stage2_workers", 1))))

# Back-compat: some scripts still read LLM_TYPE
LLM_TYPE = os.getenv("LLM_TYPE", "local").lower()

# Optional: set GPU_VRAM_GB=16 in .env to skip local nvidia-smi detection
GPU_VRAM_GB: float = float(os.getenv("GPU_VRAM_GB", "0"))
