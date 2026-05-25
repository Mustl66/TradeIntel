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
                    CHECK (feed_type IN ('rss', 'atom', 'html', 'unknown')),
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

-- ── sectors_macro ───────────────────────────────────────────────────────────
-- One row per industry. Macro growth multiplier applied in Phase 5 scoring.
-- multiplier range: 1.00 (neutral) → 1.05 (top 5% growth forecast).
-- Set by macro_multiplier.py via LLM analysis of market research articles.
CREATE TABLE IF NOT EXISTS sectors_macro (
    id                  SERIAL PRIMARY KEY,
    sector_name         VARCHAR(100) NOT NULL,
    industry_name       VARCHAR(100) NOT NULL,
    macro_multiplier    NUMERIC(4,3) NOT NULL DEFAULT 1.000,
    rationale           TEXT,                  -- LLM explanation for the multiplier
    last_llm_run_at     TIMESTAMPTZ,           -- when LLM last updated this row
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_sector_industry UNIQUE (sector_name, industry_name)
);

CREATE INDEX IF NOT EXISTS idx_sectors_macro_industry ON sectors_macro (industry_name);

-- ── market_research_feeds ────────────────────────────────────────────────────
-- RSS/Atom feeds for market research sources (Research and Markets, SNS Insider, etc.)
-- Separate from rss_feeds — these are market-wide, not ticker-specific.
CREATE TABLE IF NOT EXISTS market_research_feeds (
    id              SERIAL PRIMARY KEY,
    feed_url        TEXT         NOT NULL,
    source_name     VARCHAR(100) NOT NULL DEFAULT '',
    description     TEXT,
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    discovered_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_checked_at TIMESTAMPTZ,
    CONSTRAINT uq_market_research_url UNIQUE (feed_url)
);

-- ── market_research_articles ─────────────────────────────────────────────────
-- Articles from market research feeds. LLM reads these to derive multipliers.
CREATE TABLE IF NOT EXISTS market_research_articles (
    id              BIGSERIAL    PRIMARY KEY,
    feed_id         INTEGER      REFERENCES market_research_feeds(id) ON DELETE SET NULL,
    article_hash    CHAR(64)     NOT NULL,
    url             TEXT         NOT NULL,
    title           TEXT         NOT NULL,
    summary         TEXT,
    full_text       TEXT,
    published_at    TIMESTAMPTZ  NOT NULL,
    inserted_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    source_name     TEXT,
    llm_processed   BOOLEAN      NOT NULL DEFAULT FALSE,
    CONSTRAINT uq_mr_article_hash UNIQUE (article_hash)
);

CREATE INDEX IF NOT EXISTS idx_mr_articles_feed      ON market_research_articles (feed_id);
CREATE INDEX IF NOT EXISTS idx_mr_articles_published ON market_research_articles (published_at DESC);
CREATE INDEX IF NOT EXISTS idx_mr_articles_llm       ON market_research_articles (llm_processed) WHERE llm_processed = FALSE;

-- ── Future tables (placeholders) ────────────────────────────────────────────
-- CREATE TABLE IF NOT EXISTS insider_transactions ( ... );
-- CREATE TABLE IF NOT EXISTS social_signals ( ... );
-- CREATE TABLE IF NOT EXISTS sentiment_scores ( ... );
-- CREATE TABLE IF NOT EXISTS aggregate_scores ( ... );
"""


def create_tables() -> None:
    """Run all DDL statements. Safe to call on every startup (idempotent)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(_DDL)
            # Idempotent ALTER — add sector_id to symbols if not present
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name='symbols' AND column_name='sector_id'
                    ) THEN
                        ALTER TABLE symbols
                            ADD COLUMN sector_id INTEGER REFERENCES sectors_macro(id) ON DELETE SET NULL;
                        CREATE INDEX IF NOT EXISTS idx_symbols_sector_id ON symbols (sector_id);
                    END IF;
                END$$;
            """)
        conn.commit()
        logger.info("Database schema verified / created successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Schema creation failed: {e}")
        raise
    finally:
        conn.close()
