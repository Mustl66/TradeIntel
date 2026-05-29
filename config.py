"""
config.py — TradeIntel Central Configuration
=============================================
All runtime settings live here with sensible defaults.
Override anything via environment variables or a .env file.
"""

import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env if it exists (silently skipped if not present)
load_dotenv(Path(__file__).resolve().parent / ".env")

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

# ── LLM Provider ─────────────────────────────────────────────────────────────
# Set LLM_TYPE to one of: "local", "ollama", "api"
# local  → LM Studio at localhost:1234 (OpenAI-compatible)
# ollama → Remote Ollama server (e.g. http://10.11.12.8:11434/v1)
# api    → OpenAI / Anthropic / other hosted API

LLM_TYPE = os.getenv("LLM_TYPE", "ollama").lower()   # local | ollama | api

_LLM_PROFILES = {
    "local": {
        "base_url":          os.getenv("LLM_BASE_URL",   "http://127.0.0.1:1234/v1"),
        "api_key":           os.getenv("LLM_API_KEY",    "lm-studio"),
        "model":             os.getenv("LLM_MODEL",      "google/gemma-4-e4b"),
        "summary_model":     os.getenv("SUMMARY_LLM_MODEL", "google/gemma-4-e2b"),
        "temperature":       0.1,
        "context_size":      16384,
        "max_tokens":        12228,
        "top_p":             0.9,
        "top_k":             40,
        "frequency_penalty": 0.2,
        "presence_penalty":  0.1,
        "reasoning_mode":    False,
    },
    "ollama": {
        "base_url":          os.getenv("LLM_BASE_URL",   "http://10.11.12.8:11434/v1"),
        "api_key":           os.getenv("LLM_API_KEY",    "ollama"),
        "model":             os.getenv("LLM_MODEL",      "gemma4:e4b"),
        "summary_model":     os.getenv("SUMMARY_LLM_MODEL", "gemma4:e2b"),
        "temperature":       0.1,
        "context_size":      16384,
        "max_tokens":        12228,
        "top_p":             0.9,
        "top_k":             40,
        "frequency_penalty": 0.2,
        "presence_penalty":  0.1,
        "num_gpu_layers":    40,
        "reasoning_mode":    True,
    },
    "api": {
        "base_url":          os.getenv("LLM_BASE_URL",   "https://api.openai.com/v1"),
        "api_key":           os.getenv("LLM_API_KEY",    ""),   # set in .env
        "model":             os.getenv("LLM_MODEL",      "gpt-4o-mini"),
        "temperature":       0.1,
        "context_size":      16384,
        "max_tokens":        12228,
        "top_p":             0.9,
        "top_k":             40,
        "frequency_penalty": 0.2,
        "presence_penalty":  0.1,
        "reasoning_mode":    False,
    },
}

LLM_CONFIG: dict = _LLM_PROFILES.get(LLM_TYPE, _LLM_PROFILES["local"])

# Optional: set GPU_VRAM_GB=16 in .env to skip local nvidia-smi detection
# (required when Ollama runs on a remote server)
GPU_VRAM_GB: float = float(os.getenv("GPU_VRAM_GB", "0"))
