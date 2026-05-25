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
SYMBOL_LIMIT = 15   # e.g. 100  or  False

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
STAGE_ORDER = ["rss", "html", "edgar", "sector_map", "market_research", "macro_multiplier"]
