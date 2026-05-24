"""
pipeline_config.py — Master on/off switches for every ingestion pipeline.

Set active=True to enable, active=False to disable.
main.py + news_ingest_runner.py both read from here.
"""

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
}
