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
        "active": False,
        "description": "SEC EDGAR 8-K filings (future step — keep off for now)",
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

# ── Phase 4: Sentiment Scoring ────────────────────────────────────────────────
MAX_EVAL_ARTICLES        = 30      # rolling window per symbol (newest N articles)
ENABLE_PRE_SUMMARIZATION = True    # Stage 1 fast summarizer before main LLM
SUMMARY_LLM_MODEL        = "google/gemma-4-e2b"   # Stage 1 model (fast, small)
SENTIMENT_LAMBDA         = 0.02    # time-decay lambda (per hour)

# ── Phase 4: Orchestrator Intervals (seconds) ─────────────────────────────────
WORKER1_INTERVAL = 60       # GlobeNewswire live tracker — every 1 minute
WORKER2_INTERVAL = 3600     # Universal news pipeline   — every 1 hour
WORKER3_INTERVAL = 86400    # Macro / market research   — every 24 hours
