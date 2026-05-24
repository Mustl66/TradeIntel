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

### [0.9.0] – 2026-05-24 — RSS/Atom Ingest Bug Fixes + CDN 403 Bypass

#### Problems Fixed

**Bug 1 — news_ingest processing html/api feeds through feedparser**
`_get_active_feeds()` had no `feed_type` filter. It pulled all 3890 active feeds
including 850 html and 775 api feeds into feedparser. feedparser returned 0 entries
for html/api feeds silently — wasted CPU, logged nothing, articles never inserted.

Fix: Added `AND rf.feed_type IN ('rss', 'atom', 'unknown')` to the query.
Result: 2455 feeds processed instead of 3890 — 37% reduction in wasted work.

**Bug 2 — CDN TLS-level blocks (Edgio, Cloudflare) returning timeout or 403**
URLs like `ir.abivax.com/rss.xml` are behind Edgio CDN which drops the TCP connection
at TLS handshake level when it detects non-browser TLS fingerprints. `requests` library
times out completely — never gets to HTTP layer. Other CDNs return 403 directly.

Fix: Two-layer fallback in `_fetch_feed`:
- On HTTP 403/429: retry immediately with `curl_cffi impersonate='chrome124'`
- On `ReadTimeout`: retry with `curl_cffi` before giving up

`curl_cffi` spoofs the TLS fingerprint of real Chrome — CDNs see a legitimate browser.

**Bug 3 — Static bot User-Agent in news_ingest HEADERS constant**
The original UA `Mozilla/5.0 (compatible; TradeIntel/2.0)` is immediately identifiable
as a bot by most CDNs and rate-limiters. All feeds using this UA were potentially
degraded in service quality.

Fix: Added `_HEADER_PROFILES` pool (Chrome 124, Firefox 125, Safari 17, Edge 124)
with full Accept/Accept-Language/Accept-Encoding headers per profile.
`_random_headers()` picks a random profile per feed request.

#### Files Changed
- `pipeline/news_ingest.py` — feed_type filter, curl_cffi fallback, header rotation

### [0.8.0] – 2026-05-24 — Q4 Inc. / default.aspx IR Platform Handler

#### Problem
IR pages ending in `/default.aspx` (e.g. `investors.airbnb.com/press-releases/default.aspx`,
`ir.archgroup.com/news/default.aspx`) were routed to the old `q4ir` handler which only probed
for `/rss.xml`. That probe always returned 0 results — these Q4-hosted IR pages don't expose RSS.
Result: every `default.aspx` feed silently produced 0 articles.

#### Root Cause Discovery
Playwright network interception revealed the Q4 platform makes a hidden API call during page load:
```
GET {base}/feed/PressRelease.svc/GetPressReleaseList
    ?bodyType=2&pageSize=-1&year=-1&...
```
This endpoint is IDENTICAL across every Q4-hosted IR site — same path, same params, different base domain.
`bodyType=2` returns the full press release HTML body inline — no article detail-page fetches needed.
`year=-1` returns ALL years in one call.

#### Transport
`curl_cffi` with `impersonate='chrome124'` — same as Nasdaq. Regular `requests` gets blocked.

#### Fix (`pipeline/html_ingest.py`)
- Replaced `_handle_q4ir_rss_probe()` with `_handle_q4ir_api()`.
- Single API call per company, returns full article list + bodies inline.
- Date parsed from `PressReleaseDate` field (`MM/DD/YYYY HH:MM:SS` format).
- `_process_html_feed` updated to call `_handle_q4ir_api`, method tagged `q4ir_api`.

#### Verified Live
| Symbol | IR Domain | Articles |
|--------|-----------|---------|
| ABNB | investors.airbnb.com | 63 ✓ |
| BLFY | investor.fultonbank.com | 514 ✓ |
| ACGL | ir.archgroup.com | 175 ✓ |
| ADV | ir.youradv.com | 122 ✓ |

Note: some Q4 sites return 403 (server-side block, not our transport). These are unchanged — 0 articles, expected.

#### Coverage
Covers all `default.aspx` URLs in `rss_feeds` regardless of subdomain (`investors.`, `ir.`, `investor.`).
`_detect_platform` already tagged all of these as `q4ir`.

---

### [0.7.0] – 2026-05-24 — Yahoo Finance Scraper + Admin SEC Filings Tab


#### Yahoo Finance Press Releases (pipeline/html_ingest.py)

Problem: `finance.yahoo.com/quote/{ticker}/press-releases` pages are full JS-rendered and blocked by a GDPR consent wall. `requests`, BeautifulSoup, and Playwright all failed.

Fix — two-layer approach:

Layer 1 — Yahoo internal search API (no JS, no consent wall):
```
GET https://query2.finance.yahoo.com/v1/finance/search?q={ticker}&newsCount=40
```
Returns JSON with title, publisher, timestamp, article link. Filter by publisher set (Business Wire, PRNewswire, GlobeNewswire, Accesswire) to keep only press releases, discard analyst commentary and general news.

Layer 2 — Trafilatura for full text:
`trafilatura.fetch_url()` bypasses the Yahoo consent wall entirely and extracts clean article body from Yahoo article URLs.

Verified live (ACTG):
- "Acacia Research to Release First Quarter 2026 Financial Results on May 7, 2026" — exact match
- "Acacia Research Corporation Reports Fourth Quarter and Year End 2025 Financial Results" — exact match

#### Admin Panel: SEC Filings Tab

- Added third tab "SEC Filings" next to RSS Feeds and News
- `GET /symbol/{id}/sec` — serves only articles with `source_name='edgar_8k'`
- Keyword search box (300ms debounce) with pagination
- Displays SEC 8-K chip instead of feed URL chip
- Empty state guides user to enable EDGAR pipeline in `pipeline_config.py`
- News tab now EXCLUDES edgar articles (`source_name != 'edgar_8k'`) — clean separation
- Live tested on ACTG (66 edgar filings) — all routes 200 OK

---

### [0.6.0] – 2026-05-24 — Pipeline Toggle System + Admin Search Upgrade

#### New File: pipeline_config.py
Single master switch file for enabling/disabling entire pipelines without touching code.

```python
PIPELINES = {
    "rss":   {"active": True,  "description": "RSS/Atom feed ingestion"},
    "html":  {"active": True,  "description": "HTML page scraping (Nasdaq, PRNewswire...)"},
    "edgar": {"active": False, "description": "SEC EDGAR 8-K filings — future step"},
}
```

Change `active: False` → `active: True` and run `main.py`. No CLI flags needed.
CLI flags (`--rss-only`, `--html-only`, `--edgar-only`) still override for testing.

#### Admin: Symbol Search Now Matches Feed URLs
- `GET /symbols?q=globenewswire` now returns symbols that have GlobeNewswire feeds even if ticker/company name don't contain the word
- SQL: added `OR f.feed_url ILIKE %s` to the WHERE clause
- Works for: `nasdaq`, `prnewswire`, `globenewswire`, `businesswire`, any domain fragment
- 14 TDD tests — all GREEN

#### Admin: News Tab Keyword Search
- `GET /symbol/{id}/news?q=FDA` filters by `title ILIKE` OR `full_text ILIKE`
- Live search box rendered at top of every news tab (300ms HTMX debounce)
- Empty state: "No articles matching FDA" when no hits
- Pagination carries `q=` so paging doesn't reset the filter

---

### [0.5.0] – 2026-05-24 — Nasdaq Press Release Scraper (FIXED + VERIFIED)

#### Problem
Nasdaq press-release pages (`/market-activity/stocks/{ticker}/press-releases`) are
fully JS-rendered shells. `requests`, Playwright, and all standard scrapers return
a 15KB React skeleton with zero article content. The old fallback (BusinessWire RSS)
returned RANDOM unrelated news, not ticker-specific articles.

#### Root Cause Analysis
Three separate bugs combined to produce silent failure:

1. **Wrong data source** — BusinessWire RSS is a global feed, not ticker-filtered.
   GYRO was getting peptide-market reports instead of Gyrodyne press releases.

2. **`content_hash` vs `article_hash` key mismatch** — The Nasdaq handler built
   article dicts with key `content_hash` but `_insert_articles()` expected `article_hash`.
   Every article hit a KeyError silently — zero DB inserts, no error logged.

3. **BNBX stored as `feed_type='rss'`** — feedparser consumed the Nasdaq API URL
   instead of routing it to html_ingest. The API URL
   (`api.nasdaq.com/api/news/topic/press_release?q=symbol:BNBX|...`)
   was classified as RSS by the URL heuristic, bypassing the HTML pipeline entirely.

#### Fix: Nasdaq Internal JSON API

Discovered via JS bundle inspection. Nasdaq's own page loads articles from:
```
GET https://api.nasdaq.com/api/news/topic/press_release
    ?q=symbol:{TICKER}|assetclass:stocks&limit=40&offset=0
```
Returns clean JSON: title, date, relative URL. No JS rendering needed.

Transport: `curl_cffi` with `impersonate='chrome124'` — bypasses Nasdaq's HTTP/2
TLS fingerprint block that kills both `requests` and Playwright.

Article body: fetched from `https://www.nasdaq.com/press-release/{slug}` using
`div.body__content` CSS selector.

#### What Changed (pipeline/html_ingest.py)
- `_handle_nasdaq_page()` — complete rewrite using the internal API
- `_detect_platform()` — now recognises both URL formats:
  - `/market-activity/stocks/{ticker}/press-releases`
  - `api.nasdaq.com/api/news/topic/press_release?q=symbol:{ticker}|...`
- Ticker extraction handles both URL formats
- `article_hash` key fixed (was `content_hash`) — articles now insert correctly
- Added `api.nasdaq.com` URL format to `platform_config.py` detection

#### DB Fix
- BNBX and similar symbols with API-style Nasdaq URLs: `feed_type` updated
  `rss` → `html` in DB so html_ingest picks them up correctly

#### False Article Cleanup
- 182 wrongly-linked GlobeNewswire articles deleted from symbols that only had
  Nasdaq HTML feeds (they were inserted by the old broken BusinessWire RSS fallback)

#### Verified Live (exact title match)
| Symbol | Expected (from Nasdaq page) | Result |
|--------|---------------------------|--------|
| GYRO | "Gyrodyne Announces Agreement with Star Equity Fund" (Oct 17 2025) | ✅ Inserted |
| GYRO | "GYRODYNE ANNOUNCES CLOSING OF SUCCESSFUL RIGHTS OFFERING" | ✅ Inserted |
| BNBX | "BNB Plus Corp. Completes Historic $1.2 Million LineaDNA™ Order" (Apr 23 2026) | ✅ Inserted |
| BNBX | "BNB Plus Corp. Announces Review of Strategic Alternatives" (Apr 20 2026) | ✅ Inserted |
| BLDP | Ballard Power / Weichai articles | ✅ Inserted |

#### Human-Like Request Behaviour
- Full browser header rotation: Chrome / Firefox / Safari / Edge fingerprint pools
- Random delay 0.5–1.0s between every HTTP request
- `curl_cffi` TLS impersonation for Nasdaq-specific endpoints

---

### [0.4.1] – 2026-05-24 — HTML Ingest Pipeline + feed_type Migration

#### Problem
849 feeds stored in `rss_feeds` with HTML press-release page URLs were being
silently ignored. feedparser returned 0 entries on HTML pages, no error logged.

#### New File: pipeline/html_ingest.py
Multi-layer HTML scraping pipeline for non-RSS sources:

| Layer | Method | Coverage |
|-------|--------|----------|
| 1 | RSS autodiscovery (`<link rel="alternate">` in page `<head>`) | Promotes to RSS permanently |
| 2 | JSON-LD structured data | Modern IR pages |
| 3 | Trafilatura full-text extraction | Generic fallback |
| 4 | LM Studio quality gate (gemma-4-e4b @ localhost:1234) | Validates Trafilatura output |

Platform-specific handlers (bypass generic scraping):
- **Nasdaq** — internal JSON API (see v0.5.0 above)
- **PRNewswire** — `class="newsreleaseconsolidatelink"` anchor selector on listing pages
- **GlobeNewswire HTML listings** — scrapes `/news-release/YYYY/MM/DD/` links directly from search results page
- **BusinessWire** — RSS endpoint fallback
- **Q4IR / Airbnb IR** — attempts `/rss.xml` probe first

#### New File: pipeline/migrate_feed_type.py
One-time classifier. Probes all feeds via URL heuristic + HTTP.
Results: rss=679, atom=2041, html=849, unknown=39.
Run with `--reclassify` to re-probe existing feeds.

#### DB Schema Updates
- `rss_feeds.feed_type` CHECK now allows: `rss`, `atom`, `html`, `unknown`, `api`
- `rss_feeds.source` CHECK now allows: `globenewswire`, `company_ir`, `other`, `edgar_8k`
- `news_articles.source_type` column added

#### news_ingest_runner.py
Now orchestrates all three pipelines with flags:
- `--rss-only` — only RSS/Atom feeds
- `--html-only` — only HTML page feeds
- `--edgar-only` — only EDGAR 8-K (currently inactive)

---

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
