# TradeIntel – Changelog & Architecture Reference

> This file is the living reference for all changes, decisions, architecture notes,
> and future ideas. Update it every time a meaningful change is made to the project.

---

## Project Vision

An automated financial data engineering and analysis pipeline for the Nasdaq/NYSE universe.
The system ingests multidimensional data, scores it through an LLM/NLP sentiment engine,
and produces macro-weighted actionable scores per ticker.

Full pipeline (6 steps):
1. Universe Management — keep the symbol list clean and current
2. Multi-Source Data Ingestion — news, SEC Form 4, social media, earnings calls
3. Industry & Macro Mapping — sector tags + growth-factor multipliers
4. Sentiment & Time-Decay Scoring — LLM/NLP + e^(−λt) decay
5. Aggregation & Macro Synthesis — base score × industry multiplier
6. Output / Visualization — time-series DB, Streamlit dashboard, backtesting

---

## Changelog

### [0.4.0] – 2026-05-22 — Admin Panel (admin.py)

Standalone FastAPI + HTMX admin panel for manual DB management. Completely
independent from main.py — never touches the pipeline.

#### New File: admin.py
- Run with: `python admin.py` — opens browser automatically at http://localhost:8055
- Left panel: full symbol list, search by ticker or company name
- Right panel — two tabs per symbol:
  - **RSS Feeds tab**: list all feeds, toggle active/inactive, edit URL (validates
    live before saving), delete feed, add new feed with source type selector
  - **News tab**: paginated article list (20/page), newest first, shows title
    (clickable link), published date, feed source chip, 3-line text preview
- Feed validation: paste URL → Validate button hits the URL, shows feed title,
  description and article count before committing to DB
- Active/inactive toggle: inactive feeds are skipped by main.py pipeline
- No dependencies beyond what was already installed

---

### [0.3.2] – 2026-05-22 — Feedparser Timeout Fix

#### Bug: Pipeline stalling silently at ~feed 300
`feedparser.parse(url)` does its own HTTP with no timeout. One hung server
occupied a worker thread indefinitely. With 6 workers, a cluster of slow
servers caused a complete stall with zero log output.

#### Fix (pipeline/news_ingest.py)
- Replaced `feedparser.parse(url)` with `requests.get(timeout=15)` + `feedparser.parse(raw_bytes)`
- feedparser now only parses bytes — never touches the network
- Hung/unreachable feeds log a warning and return `[]` immediately
- Feed workers bumped 6→10, scrape workers bumped 4→6

---

### [0.3.1] – 2026-05-22 — Progress Logging for News Ingest

#### Bug: No visibility during long runs
After "3096 active feeds to process" the log went silent for 10+ minutes.
No way to tell if it was running or hung.

#### Fix (pipeline/news_ingest.py)
- Feed fetch phase: progress line every 50 feeds showing count + articles collected so far
- Scrape phase: progress line every 100 articles showing count + total to scrape
- Example output:
  ```
  [news_ingest] Feeds: 50/3096 | articles collected so far: 629
  [news_ingest] Feeds: 100/3096 | articles collected so far: 1042
  [news_ingest] Scraped: 100/18200 articles
  [news_ingest] Scraped: 200/18200 articles
  ```

---

### [0.3.0] – 2026-05-22 — Phase 2: News Ingestion Pipeline

Full-text financial news ingestion with permanent dedup-safe storage.

#### New Table: news_articles (db/schema.py)
- `article_hash CHAR(64) UNIQUE` — SHA-256(url + title + published_at), dedup key
- `published_at TIMESTAMPTZ` — original publication time (used by time-decay model)
- `inserted_at TIMESTAMPTZ DEFAULT NOW()` — when our pipeline wrote the row
- `full_text TEXT` — scraped full article body
- `summary TEXT` — raw feed description (always available, even if scrape fails)
- Composite index on `(symbol_id, published_at DESC)` for fast per-ticker queries
- Secondary index on `feed_id`
- `ON CONFLICT DO NOTHING` on article_hash — crash-safe, resume-safe

#### New File: pipeline/news_ingest.py
Three-phase pipeline:
1. Parallel RSS fetch (10 workers) — feedparser reads all active feeds
2. Batch hash dedup — all hashes checked against DB in one query; existing articles never scraped
3. Parallel full-text scrape (6 workers) — BeautifulSoup extracts article body; only new articles

#### New File: news_ingest_runner.py
Thin entry point, mirrors universe_setup.py pattern. Can run standalone:
`python news_ingest_runner.py --limit 50 --no-scrape`

#### main.py wired
"news" added to STAGES list. Run with: `python main.py --only news`

#### Dedup guarantee
- SHA-256 batch pre-check removes known articles before any HTTP scraping
- ON CONFLICT DO NOTHING on insert catches race conditions
- Re-running pipeline: existing articles cost zero scrape bandwidth, DB untouched

---

### [0.2.2] – 2026-05-22 — Centralized Config + DB Init Script

#### New File: config.py
Single source of truth for all runtime settings. Replaces scattered os.getenv
calls across modules. Key sections:
- `DB_CONFIG` — host, port, dbname (tradeintel), user, password, client_encoding
- `PIPELINE` — exchange (NASDAQ), default limits, worker counts
- db/connection.py updated to import from config instead of raw os.getenv
- client_encoding: utf8 added to fix Windows locale encoding error on connect

#### New File: db_init_from_watchlist.py
One-shot initialization script. Run ONCE to build the baseline DB from
watchlist_status.json (the validated RSS feed list).

What it does:
1. Wipes all tables (TRUNCATE CASCADE) for a clean slate
2. Reads watchlist_status.json
3. Inserts all symbols (skips corrupt entries: spaces in ticker, length > 20)
4. Inserts all known RSS feed URLs (skips garbage `#` URLs)
5. Uses ON CONFLICT DO NOTHING for idempotency

Results on first run: 3795 symbols, 3605 RSS feeds, 77 duplicates skipped,
24 garbage URLs dropped, 1 corrupt symbol row skipped.

Flags:
- `--no-clear` — merge without wiping (add a second watchlist file)

After init, main.py run preserves all baseline feeds and adds new discoveries
as additional rows (multiple feed URLs per symbol supported).

#### Multiple RSS feeds per symbol
rss_feeds table: one row per URL, multiple rows per symbol supported.
All active feed URLs for a symbol are used in news ingestion — not just the first.
Schema supports this natively via FK to symbols.

### [0.2.1] – 2025-05-22 — Orchestrator Pattern: main.py Split

#### Key Changes

- **`main.py` is now a pure orchestrator.** Zero business logic. It only:
  1. Bootstraps the DB (connectivity check + `CREATE TABLE IF NOT EXISTS`)
  2. Iterates the `STAGES` list and dispatches each to its script
  3. Passes `--exchange`, `--limit`, `--refresh` down to each stage

- **`universe_setup.py` (new)** holds everything that was in `main.py`:
  - Parses its own `--step` arg (symbol_status | rss_finder | all)
  - Can be run standalone: `python universe_setup.py --limit 20`
  - `run()` function called by main.py without argument parsing overhead

- **Adding future stages is one-line in main.py:**
  1. Create `news_ingest.py` with a `run(exchange, limit)` function
  2. Add `"news"` to the `STAGES` list in `main.py`
  3. Uncomment the elif block in `run_stage()`

- **Stage naming convention:** flat scripts at project root, named by domain:
  `universe_setup.py`, `news_ingest.py`, `sector_map.py`, `sentiment.py`,
  `aggregation.py`, `output.py`

---

### [0.2.0] – 2025-05-22 — PostgreSQL Migration & Architecture Refactor

#### Key Changes

- **Replaced JSON file storage** (`watchlist_status.json`) with PostgreSQL.
  All symbol and feed data now lives in a proper relational DB.

- **New project layout:**
  ```
  TradeIntel/
  ├── main.py               ← single entry point, starts all steps
  ├── .env                  ← DB credentials (not committed)
  ├── .env.example          ← template
  ├── db/
  │   ├── __init__.py
  │   ├── connection.py     ← psycopg2 connection factory, .env loader
  │   └── schema.py         ← all DDL, idempotent (CREATE TABLE IF NOT EXISTS)
  ├── pipeline/
  │   ├── __init__.py
  │   ├── symbol_status.py  ← Step 1a: TradingView universe sync
  │   └── rss_finder.py     ← Step 1b: GlobeNewswire RSS discovery
  └── files/                ← original scripts (kept as reference)
  ```

- **`main.py` is now the single entry point.** It:
  1. Verifies DB connectivity
  2. Runs `CREATE TABLE IF NOT EXISTS` (safe on every startup)
  3. Dispatches to pipeline steps via `--step` arg
  4. Supports `--exchange`, `--limit` (dev mode), `--refresh`

- **Database schema** (`db/schema.py`):
  - `symbols` table: master universe with UNIQUE constraint on (symbol, exchange)
  - `rss_feeds` table: one row per URL, FK to symbols, UNIQUE on feed_url
  - `pipeline_runs` table: audit log of every pipeline execution with stats
  - All timestamps are TIMESTAMPTZ (timezone-aware)
  - Placeholder comments for future tables (news_articles, sentiment_scores, etc.)

- **`pipeline/symbol_status.py`** (migrated from `files/symbol_status.py`):
  - Fetches live symbols from TradingView (same two-pass approach)
  - Upserts into `symbols` table (INSERT ON CONFLICT DO NOTHING / UPDATE)
  - Marks delisted symbols status=FALSE, restores re-listed ones to TRUE
  - Records run in `pipeline_runs` with stats + error capture

- **`pipeline/rss_finder.py`** (migrated from `files/rss_finder.py`):
  - Reads active symbols from DB (instead of JSON)
  - Parallel GlobeNewswire scraping (ThreadPoolExecutor, configurable via RSS_WORKERS env)
  - Upserts feeds into `rss_feeds` table with ON CONFLICT for idempotency
  - Classifies feed type (rss/atom) and source (globenewswire/company_ir) automatically

#### Dependencies Added
- `psycopg2-binary` — PostgreSQL adapter
- `python-dotenv`   — .env file loading

#### How to Set Up
1. Install PostgreSQL and create a database: `CREATE DATABASE tradeintel;`
2. Copy `.env.example` to `.env` and fill in your credentials
3. Run: `python main.py --limit 20` (test mode, 20 symbols)
4. Run: `python main.py` (full production run, NASDAQ)

---

### [0.1.0] – Initial State (pre-refactor)

- `main.py` — fetched exchange symbols from TradingView, saved to `.txt` files
- `files/symbol_status.py` — synced symbol status to `watchlist_status.json`
- `files/rss_finder.py` — found GlobeNewswire RSS feeds, updated `watchlist_status.json`
- Storage: flat JSON file (`watchlist_status.json`)

---

## Architecture Decisions

### Why PostgreSQL over JSON files?

| Concern | JSON file | PostgreSQL |
|---|---|---|
| Concurrent access | Race conditions | ACID transactions |
| Partial updates (crashes) | File corruption risk | Transactional, safe |
| Query / filter | Load entire file | SQL WHERE, indexes |
| Scale (100k+ symbols) | Memory pressure | Row-level access |
| Future joins (news ↔ symbols) | Manual dict merging | Native FK joins |
| Audit trail | None | pipeline_runs table |

### Why `pipeline/` folder (not monolithic scripts)?

Each pipeline step is isolated in its own module with a `run()` function.
`main.py` is a pure dispatcher. This allows:
- Running a single step independently during development
- Easy addition of new steps (just add a new file + call in main.py)
- Future parallelism between steps if needed
- Clean testing per module

### Why `db/schema.py` with CREATE TABLE IF NOT EXISTS?

- Safe to call on every startup — zero extra code for first-run vs. subsequent runs
- All DDL is in one file — easy to audit the full schema at a glance
- Future migrations: when a column needs adding, document it here and add an
  `ALTER TABLE IF NOT EXISTS col ...` below the original CREATE.

### Status field: BOOLEAN not TEXT

Original JSON used `"status": "true"/"false"` (strings). DB uses proper BOOLEAN.
Cleaner queries, proper indexing, type safety.

---

## Ideas & Future Enhancements

*(These are architectural proposals — discuss before implementing)*

### Near-term (Step 2 prep)
- [ ] **RSS feed health checker**: periodic job that hits each `rss_feeds.feed_url`,
      marks unreachable feeds `is_active=FALSE`, alerts on high miss rate.
      Saves scraping time in Step 2.
- [ ] **GNW org_id direct feed construction**: once we have `gnw_org_id`,
      we can build the feed URL deterministically without scraping.
      Would make RSS discovery ~10x faster.
- [ ] **Exchange expansion**: the schema already supports multi-exchange.
      Just call `main.py --exchange NYSE` when ready.

### Step 2 ideas
- [ ] **Feed prioritization**: not all RSS feeds have the same quality.
      Company IR feeds (non-GNW) often have richer body text.
      Rank feeds by source quality when ingesting articles.
- [ ] **SEC EDGAR webhook**: instead of polling, subscribe to EDGAR's EDGAR Online
      API for real-time Form 4 filings. Much lower latency than polling.
- [ ] **StockTwits spike detection**: maintain a rolling 7-day baseline of
      message volume per ticker. Flag when current volume > 2σ above baseline.
      Correlate spikes with price movements.

### Step 4 ideas (Scoring)
- [ ] **Decay function tuning**: λ value should be calibrated per sector.
      Biotech news decays faster (FDA decisions are binary) than industrials.
      Consider a per-sector λ table in `macro_weights`.
- [ ] **Local LLM via LM Studio**: the user runs LM Studio at localhost:1234.
      For sentiment scoring, we can hit the local OpenAI-compatible endpoint
      instead of a paid API. Cost = $0, latency = ~1s/article on good hardware.
      Batch sentiment scoring in chunks of 10 articles per LLM call.
- [ ] **Insider trading as leading indicator**: if insider buys on Day 0,
      weight any positive news from Day 1-14 with a +1.2x multiplier.
      The correlation of insider buys → positive news is well-documented.

### Step 6 ideas (Output)
- [ ] **Streamlit dashboard**: live leaderboard of top-scoring symbols.
      Show score, decay curve, top 3 headline drivers.
      Color-code by sector for quick macro view.
- [ ] **Backtesting hook**: store `aggregate_scores` with timestamps.
      Feed into `vectorbt` or `backtrader` to test "buy top-10 by score weekly."
- [ ] **Alert system**: Telegram/Discord webhook when a symbol crosses a
      configurable score threshold. Integrates naturally with Hermes Agent gateway.

---

## Database Tables — Quick Reference

| Table | What lives here | Key columns |
|---|---|---|
| `symbols` | Master list of every tracked ticker (NASDAQ/NYSE) | `symbol`, `exchange`, `company_name`, `status` |
| `rss_feeds` | One row per RSS/Atom feed URL, linked to a symbol | `feed_url`, `source`, `is_active`, `last_checked_at` |
| `pipeline_runs` | Audit log — every time any pipeline step runs | `step`, `started_at`, `finished_at`, `status`, `error_message` |
| `news_articles` | **Fetched news lives here.** Full-text, permanent, append-only archive | `title`, `url`, `full_text`, `published_at`, `inserted_at`, `article_hash` |

### news_articles detail

- `symbol_id` — FK to `symbols.id` (which ticker this article belongs to)
- `feed_id` — FK to `rss_feeds.id` (which feed it came from)
- `article_hash` — SHA-256(url + title + published_at) — dedup key, UNIQUE constraint
- `published_at` — original publication time from the RSS feed (used by time-decay model)
- `inserted_at` — timestamp when OUR pipeline wrote the row (pipeline lag diagnostics)
- `full_text` — scraped full article body (best-effort HTML scrape)
- `summary` — raw feed description/summary (always available, even if scrape fails)

Query newest articles for a ticker:
```sql
SELECT title, published_at, full_text
FROM news_articles na
JOIN symbols s ON s.id = na.symbol_id
WHERE s.symbol = 'AAPL'
ORDER BY published_at DESC;
```

---

## Notes for AI Agent Sessions

- Always run via project venv: `.venv/Scripts/python.exe`
- DB credentials in `.env` (never commit this file)
- `main.py --limit 20` for fast dev/test iteration
- `pipeline_runs` table is your audit trail — check it when debugging
- Schema changes: edit `db/schema.py`, add `ALTER TABLE` statements below the original DDL
- New pipeline steps: add `pipeline/<step>.py` with `run()` function, register in `main.py`
