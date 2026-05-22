"""
db/schema.py
-------------
Defines and creates all TradeIntel database tables.

Design principles:
  - Each logical domain gets its own table (extensible for future steps).
  - Use TIMESTAMPTZ everywhere (timezone-aware).
  - Enum-style text columns with CHECK constraints (easy to extend vs. DB enums).
  - Indexes on the columns most likely to be filtered/joined.

Tables in this module (Step 1 scope):
  - symbols          : master universe of tracked tickers
  - rss_feeds        : one row per RSS/Atom feed URL per symbol
  - pipeline_runs    : audit log of every pipeline execution

Future tables (placeholder comments for Steps 2-6):
  - news_articles, insider_transactions, social_signals
  - sector_mappings, macro_weights
  - sentiment_scores, aggregate_scores
"""

import logging
from db.connection import get_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL Statements
# ---------------------------------------------------------------------------

_DDL = """
-- ── symbols ────────────────────────────────────────────────────────────────
-- Master table: one row per ticker. Single source of truth for universe.
CREATE TABLE IF NOT EXISTS symbols (
    id              SERIAL PRIMARY KEY,
    symbol          VARCHAR(20)  NOT NULL,
    exchange        VARCHAR(20)  NOT NULL,
    company_name    TEXT         NOT NULL DEFAULT '',
    status          BOOLEAN      NOT NULL DEFAULT TRUE,
    gnw_search_url  TEXT,
    gnw_org_id      INTEGER,
    first_seen_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    -- Natural key: one ticker per exchange
    CONSTRAINT uq_symbol_exchange UNIQUE (symbol, exchange)
);

CREATE INDEX IF NOT EXISTS idx_symbols_exchange ON symbols (exchange);
CREATE INDEX IF NOT EXISTS idx_symbols_status   ON symbols (status);

-- ── rss_feeds ──────────────────────────────────────────────────────────────
-- One row per feed URL per symbol. Supports multiple feeds per ticker
-- (e.g. GNW Atom + company IR RSS).
CREATE TABLE IF NOT EXISTS rss_feeds (
    id              SERIAL PRIMARY KEY,
    symbol_id       INTEGER      NOT NULL REFERENCES symbols(id) ON DELETE CASCADE,
    feed_url        TEXT         NOT NULL,
    feed_type       VARCHAR(20)  NOT NULL DEFAULT 'unknown'
                    CHECK (feed_type IN ('rss', 'atom', 'unknown')),
    source          VARCHAR(50)  NOT NULL DEFAULT 'globenewswire'
                    CHECK (source IN ('globenewswire', 'company_ir', 'other')),
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    discovered_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_checked_at TIMESTAMPTZ,
    CONSTRAINT uq_feed_url UNIQUE (feed_url)
);

CREATE INDEX IF NOT EXISTS idx_rss_feeds_symbol_id ON rss_feeds (symbol_id);
CREATE INDEX IF NOT EXISTS idx_rss_feeds_active    ON rss_feeds (is_active);

-- ── pipeline_runs ──────────────────────────────────────────────────────────
-- Audit log: every time a pipeline step runs, record it here.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              SERIAL PRIMARY KEY,
    step            VARCHAR(50)  NOT NULL,   -- 'symbol_status', 'rss_finder', etc.
    exchange        VARCHAR(20),
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          VARCHAR(20)  NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'success', 'failed', 'partial')),
    symbols_total   INTEGER,
    symbols_added   INTEGER,
    symbols_updated INTEGER,
    feeds_found     INTEGER,
    error_message   TEXT,
    meta            JSONB        DEFAULT '{}'::JSONB
);

-- ── news_articles ───────────────────────────────────────────────────────────
-- Permanent cumulative archive of full-text financial news articles.
-- Never truncated or purged — append-only by design.
--
-- Dedup strategy: article_hash = SHA-256(url + title + published_at).
-- Ingestion always uses ON CONFLICT (article_hash) DO NOTHING.
--
-- Dual timestamps:
--   published_at  — original publication time from the source feed
--                   (used by the time-decay sentiment model)
--   inserted_at   — system time when this row was written to the DB
--                   (used for pipeline tracking / lag diagnostics)
CREATE TABLE IF NOT EXISTS news_articles (
    id              BIGSERIAL    PRIMARY KEY,

    -- Source linkage
    symbol_id       INTEGER      NOT NULL REFERENCES symbols(id) ON DELETE CASCADE,
    feed_id         INTEGER      REFERENCES rss_feeds(id) ON DELETE SET NULL,

    -- Identity / dedup
    article_hash    CHAR(64)     NOT NULL,   -- SHA-256 hex digest
    url             TEXT         NOT NULL,
    title           TEXT         NOT NULL,

    -- Content
    summary         TEXT,                   -- raw feed summary/description
    full_text       TEXT,                   -- scraped full article body (best-effort)

    -- Timestamps
    published_at    TIMESTAMPTZ  NOT NULL,  -- from feed entry (used for decay model)
    inserted_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),  -- when we wrote the row

    -- Author / source metadata
    author          TEXT,
    source_name     TEXT,                   -- e.g. "GlobeNewswire", "PR Newswire"

    CONSTRAINT uq_article_hash UNIQUE (article_hash)
);

-- Primary query pattern: "give me AAPL news newest-first"
-- This index guarantees ultra-fast lookups as the table grows to millions of rows.
CREATE INDEX IF NOT EXISTS idx_news_symbol_published
    ON news_articles (symbol_id, published_at DESC);

-- Secondary: look up by feed (for feed health diagnostics)
CREATE INDEX IF NOT EXISTS idx_news_feed_id
    ON news_articles (feed_id);

-- ── Future tables (placeholders) ────────────────────────────────────────────
-- CREATE TABLE IF NOT EXISTS insider_transactions ( ... );
-- CREATE TABLE IF NOT EXISTS social_signals ( ... );
-- CREATE TABLE IF NOT EXISTS sector_mappings ( ... );
-- CREATE TABLE IF NOT EXISTS macro_weights ( ... );
-- CREATE TABLE IF NOT EXISTS sentiment_scores ( ... );
-- CREATE TABLE IF NOT EXISTS aggregate_scores ( ... );
"""


def create_tables() -> None:
    """Run all DDL statements. Safe to call on every startup (idempotent)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()
        logger.info("Database schema verified / created successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Schema creation failed: {e}")
        raise
    finally:
        conn.close()
