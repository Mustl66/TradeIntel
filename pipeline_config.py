"""
pipeline_config.py — Master on/off switches for every ingestion pipeline.

Set active=True to enable, active=False to disable.
Set START_FROM to skip all stages before a given stage name.

main.py + news_ingest_runner.py both read from here.

Stage order:
  rss -> html -> edgar -> sector_map -> market_research -> macro_multiplier

Example: to skip rss+html and start at sector_map:
  START_FROM = "sector_map"
"""

# Set to a stage name to skip everything before it. None = run all active stages.
START_FROM = None   # e.g. "html"  or  "sector_map"

# Limit symbols processed per run. False = all symbols. Integer = first N symbols only.
# Useful for quick test runs without waiting for the full 2800-symbol pipeline.
SYMBOL_LIMIT = False   # e.g. 100  or  False

PIPELINES = {
    "rss": {
        "active": True,
        "description": "RSS/Atom feeds (GlobeNewswire, company IR, etc.)",
    },
    "html": {
        "active": True,
        "description": "HTML press release pages (Nasdaq, PRNewswire, etc.)",
    },
    "edgar": {
        "active": True,
        "description": "SEC EDGAR: 10-K/10-Q/8-K/Form4/13D/13G/S-3/424B (all tiers)",
        "tiers_enabled": [1, 2, 3],
    },
    "sector_map": {
        "active": True,
        "description": "TradingView sector/industry mapping onto symbols",
    },
    "market_research": {
        "active": True,
        "description": "Market research RSS feeds (SNS Insider, Research and Markets, etc.)",
    },
    "macro_multiplier": {
        "active": True,
        "description": "LLM analysis of market research articles → sector growth multipliers",
    },
}

# Ordered stage list — controls execution sequence
STAGE_ORDER = ["rss", "html", "edgar", "sector_map", "market_research", "macro_multiplier", "sentiment"]

from config import LLM_CONFIG

# ── LLM Skills ────────────────────────────────────────────────────────────────
# Drop .md files into the skills/ folder to inject extra instructions into the
# Stage 2 LLM system prompt.  Files prefixed with _ are skipped.
# Set SKILLS_ENABLED = False to disable all skills globally.
SKILLS_ENABLED = False

# ── Phase 4: Sentiment Scoring ────────────────────────────────────────────────
MAX_EVAL_ARTICLES        = 12      # rolling window per symbol (newest N articles)
ENABLE_PRE_SUMMARIZATION = False    # Stage 1 fast summarizer before main LLM
SUMMARY_LLM_MODEL        = LLM_CONFIG.get("summary_model", LLM_CONFIG["model"])   # Stage 1 model
SENTIMENT_LAMBDA         = 0.0007   # time-decay lambda (per hour) — applied only AFTER grace period
DECAY_GRACE_MONTHS       = 1       # no decay for articles younger than this many months
NEUTRAL_SCORE_THRESHOLD  = 0.1    # legacy alias — kept for viewer/recompute compat; use MATERIALITY_THRESHOLD internally
MATERIALITY_THRESHOLD    = 0.25   # skip articles where |sentiment_score| < this from aggregation
                                   # 0.25 = only real signal articles included (was 0.1 — PR fluff crept in)

# ── Symbol-level aggregation selectivity ──────────────────────────────────────
# Gravity: exponential penalty on final_score based on age of NEWEST material
# article. If the most recent real catalyst is weeks old, symbol drifts → 0.
# Applied on top of per-article time-decay.
GRAVITY_GAMMA            = 0.0005  # per hour; half-life ≈ 58 days after grace (was 0.0003)
GRAVITY_GRACE_DAYS       = 14      # no gravity within this many days of newest material article

# Asymptote threshold: above this |score|, linear multiplier application is
# replaced by asymptotic compression so sector/macro boosts never hard-clip.
# Below threshold, plain linear scaling unchanged (no distortion of mid scores).
ASYMPTOTE_THRESHOLD      = 0.85   # scores above this get asymptotic treatment

# ── Stage 1 parallel workers (gemma-4-e2b pre-summarization) ──────────────────
# Stage 1 is stateless per article — safe to run in parallel.
# Set >1 only if LM Studio can handle concurrent requests for the fast model.
# Set to 1 to disable parallelism (safe default).
# ── TradingView snapshot in LLM context ───────────────────────────────────────
# When True: tv_snapshot (market cap, sector, earnings date, etc.) is included
# in every Stage 2 prompt so the LLM can scale scores by company size.
# When False: tv_snapshot is omitted — faster prompts, less context, blind scoring.
INCLUDE_TV_SNAPSHOT      = False    # set False to disable TV data in LLM prompts

STAGE1_PARALLEL_WORKERS  = 4      # e.g. 3 to test parallel Stage 1
# Tier override: config.py sets per-tier counts. Pull active tier value.
try:
    from config import STAGE1_PARALLEL_WORKERS as _S1_TIER
    STAGE1_PARALLEL_WORKERS = _S1_TIER
except ImportError:
    pass

# ── Stage 2 parallel workers (main LLM scoring) ───────────────────────────────
# Stage 2 is stateful WITHIN a symbol (needs rolling master_summary) — only
# parallelised ACROSS symbols. Each worker takes a different symbol.
# Set >1 only if LM Studio handles concurrent main-model requests.
# Set to 1 to disable (recommended until tested).
STAGE2_PARALLEL_WORKERS  = 1       # e.g. 2 to test parallel Stage 2 across symbols
# Tier override
try:
    from config import STAGE2_PARALLEL_WORKERS as _S2_TIER
    STAGE2_PARALLEL_WORKERS = _S2_TIER
except ImportError:
    pass

# ── Worker count detection bypass ─────────────────────────────────────────────
# When True: skip _compute_worker_count() VRAM probe entirely.
# Uses STAGE1_PARALLEL_WORKERS and STAGE2_PARALLEL_WORKERS directly.
# single_model_mode is forced False (both stages use their own models).
# When False (default): VRAM is probed and workers are computed automatically.
SKIP_WORKER_COUNT_DETECTION = True

# ── Phase 4: Worker 2 ─────────────────────────────────────────────────────────
WORKER2_SUBWORKERS       = 5       # parallel symbol batches for RSS+HTML ingest
RSS_DELAY_RANGE          = (0.2, 1.0)   # seconds between RSS requests (per worker)
HTML_DELAY_RANGE         = (1.0, 5.0)  # seconds between HTML page fetches (per worker)

# ── Phase 4: Orchestrator Intervals (seconds) ─────────────────────────────────
WORKER1_INTERVAL = 60       # GlobeNewswire live tracker — every 1 minute
WORKER2_INTERVAL = 3600     # Universal news pipeline   — every 1 hour
WORKER3_INTERVAL = 86400    # Macro / market research   — every 24 hours
WORKER4_INTERVAL = 21600    # EDGAR live: 8-K, Form4, S-3, NT — every 6 hours
WORKER5_INTERVAL = 86400    # EDGAR daily: 10-K, 10-Q — every 24 hours
WORKER6_INTERVAL = 300      # LLM scoring: score all unscored articles — every 5 minutes

# ── SEC EDGAR ingestion controls ──────────────────────────────────────────────
EDGAR_SEC_RESERVED_SLOTS   = 4      # slots in MAX_EVAL_ARTICLES always held for SEC filings
EDGAR_10K_STALENESS_MONTHS = 18     # skip 10-K older than this for scoring (still stored)
EDGAR_10Q_STALENESS_MONTHS = 15     # skip 10-Q older than this for scoring

# ── SEC tier depth ─────────────────────────────────────────────────────────────
# Controls which SEC filing tiers are fetched and scored.
# Acts as a MAX tier — selecting a tier implicitly includes all tiers below it:
#   EDGAR_MAX_TIER = 1  →  Tier 1 only  (10-K, 10-Q, 8-K)
#   EDGAR_MAX_TIER = 2  →  Tier 1 + 2  (+ S-3, 424B, NT filings)
#   EDGAR_MAX_TIER = 3  →  All tiers   (+ Form 4, SC 13D/G)
EDGAR_MAX_TIER = 3
