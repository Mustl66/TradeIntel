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

### [3.0.3] – 2026-05-28 — LLM Instruction JSONs + Full Scoring Schema

#### config/stage1_instruction.json
Full extraction schema for Stage 1 (gemma-4-e2b). Replaces hardcoded system prompt.
Fields now extracted per article:
- extended_summary (4-8 dense analyst sentences)
- financial_figures: revenue, EPS, guidance, deal_value
- contracts: counterparty, value, duration, significance
- patents_and_ip: patent numbers, clinical phase, regulatory status
- mergers_and_acquisitions: type, target, stake, deal_value, expected_close
- partnerships_and_collaborations: partner, purpose, revenue_share, exclusivity
- management_changes: name, role, change_type, effective_date
- legal_and_regulatory: type, authority, amount, status
- product_and_technology: product_name, launch_status, target_market
- connected_companies: all named companies for future relationship mapping
- earnings_and_dates: next_earnings_date, dividend, buyback
- industry_and_market: primary_industry, sub_sector, geographic_markets, macro_tags
- sentiment_signals: tone, urgency, materiality, flags

#### config/stage2_instruction.json
Full scoring rubric + stateful analyst schema for Stage 2 (main LLM). Replaces hardcoded system prompt.
Scoring rubric with explicit score ranges:
- very_positive [0.70, 1.00]: earnings beat >10%, major acquisition, FDA approval, large contract
- positive [0.30, 0.69]: in-line earnings + guidance, patent granted, Phase 3 trial success
- mildly_positive [0.05, 0.29]: minor MOU, routine product update, small contract
- neutral [-0.04, 0.04]: routine filings, boilerplate IR, no material content
- mildly_negative [-0.29, -0.05]: guidance slight miss, minor delay, small fine
- negative [-0.69, -0.30]: 5-15% miss, CEO departure, SEC investigation, product recall
- very_negative [-1.00, -0.70]: >15% miss, bankruptcy, fraud, regulatory ban
Adjustment factors: context_weight, materiality_weight, valuation_context, earnings_proximity
Expanded key_events output: + management_change, legal_regulatory, product_launch, earnings_signal

#### pipeline/sentiment_scoring.py
- Instruction JSONs loaded at startup from config/ via _load_instruction()
- _STAGE1_SYSTEM and _STAGE2_SYSTEM now built from JSON content
- Hot-reloadable: edit JSON files, restart orchestrator — no code change needed
- score_rationale now required to reference rubric category and adjustments applied

### [3.0.2] – 2026-05-28 — Worker 2 Implementation + Per-Article Score Detail

#### Worker 2: Full RSS + HTML + Scoring Pipeline
Worker 2 is now live. Architecture: two threads per W2 lifecycle.

- `Worker2-Scheduler` thread fires every 1 hour via `_run_worker`. It only enqueues
  a timestamp token into `_w2_queue` — it never blocks.
- `Worker2-Consumer` thread blocks on `_w2_queue.get()`. Processes one full run at a time.
  If the 1-hour tick fires while a run is still in progress, the token queues up (FIFO).
  Next run starts immediately after the current one finishes — no tick is ever lost.

Worker 2 run sequence (3 steps):
1. `pipeline.news_ingest.run(exchange="NASDAQ", limit=0)` — RSS/Atom feeds
2. Random delay `RSS_DELAY_RANGE=(0.2, 1.0)s` between stages (humanization)
3. `pipeline.html_ingest.run(exchange="NASDAQ", limit=0)` — HTML feeds
4. `pipeline.sentiment_scoring.run(exchange="NASDAQ", limit=0)` — score ALL unscored articles

Overlap with W1 is OK — `article_hash` dedupe prevents double-inserts.
EDGAR + market research explicitly excluded (those belong to W3).

#### Humanization
- RSS stage: `random.uniform(*RSS_DELAY_RANGE)` — 0.2–1.0s between stage 1 and 2
- HTML stage delay lives inside `html_ingest.py` (`HTML_DELAY_RANGE` configurable)
- `pipeline_config.py` exposes `RSS_DELAY_RANGE` and `HTML_DELAY_RANGE` as tuples

#### Per-Article Score Detail — New DB Columns
Two new columns added to `news_articles`:
- `score_rationale TEXT` — brief LLM explanation of why that sentiment score was assigned
- `forecast_until_earnings TEXT` — per-article forward outlook extracted from Stage 2

Previously these fields were returned by Stage 2 but only `forecast_until_earnings` was
saved to `symbols.symbol_forecast_narrative` (symbol-level, not per-article).
Now both are persisted per article, enabling future per-article drill-down in the admin UI.

`_save_article_result()` in `sentiment_scoring.py` updated to write both columns.

#### Files Changed
- `orchestrator.py` — `worker2_tick` enqueues to `_w2_queue`; `_worker2_consumer` runs pipeline;
  W2 scheduler + consumer threads added to `main()`; `queue` imported;
  `RSS_DELAY_RANGE` + `HTML_DELAY_RANGE` imported from `pipeline_config`
- `pipeline/sentiment_scoring.py` — `_save_article_result` saves `score_rationale` + `forecast_until_earnings`
- `db/schema.py` — `score_rationale TEXT` + `forecast_until_earnings TEXT` columns added with
  idempotent `ALTER TABLE IF NOT EXISTS` guards

---

### [3.0.1] – 2026-05-27 — Worker 1 GNW RSS Fix + Logging

#### Problem
Worker 1 was completely silent after startup. Two root causes:

1. `_GNW_URL` pointed to a broken HTML page. The old scraper used guessed CSS selectors
   (`article.oc-repeater-item`, `li.article`) that never matched real GlobeNewswire HTML.
   Result: 0 articles fetched on every tick, silently.

2. Key log lines were at `DEBUG` level — never shown at default `INFO` logging.
   Worker 1 ticks produced zero output even when running correctly.

3. `scheme = None` crash: when feedparser returns a link with no scheme,
   `"rss/stock" in scheme` threw `TypeError: argument of type 'NoneType' is not iterable`.
   Caught silently by `except Exception: continue` — every entry skipped.

#### Fix: Official GNW RSS Feed
Replaced the entire HTML scraper with the official GlobeNewswire NASDAQ RSS feed:
```
https://www.globenewswire.com/RssFeed/exchange/NASDAQ
```
Returns 20 real press releases per poll. Tickers are embedded as `<category>Nasdaq:CWST</category>`.
Exchange prefix stripped before DB lookup (`CWST` not `Nasdaq:CWST`).
No browser / Playwright / CSS selectors needed — pure feedparser.

#### Fix: scheme=None TypeError
```python
# Before (crashes when scheme=None):
if "rss/stock" in scheme:
# After:
if "rss/stock" in (scheme or ""):
```

#### Fix: Logging bumped to INFO
Worker 1 now logs at INFO level on every tick:
```
[Worker1] GNW RSS returned 20 articles
[Worker1] tick done — fetched=20 new=5 symbol_matches=12
[Worker1] ticker CWST not found in DB symbols (skipped)
```

#### Workers 2 + 3 disabled
Workers 2 and 3 are commented out in `orchestrator.py` `main()`.
Code preserved — not deleted. Will be re-enabled in a later phase.

#### Files Changed
- `pipeline/orchestrator.py` — GNW URL replaced, feedparser RSS ingest, scheme fix, INFO logs
- `pipeline/orchestrator.py` — worker2_tick + worker3_tick calls removed from `main()`

---

### [3.0.5] – 2026-05-31 — Score Math Fixes, Context Window, Viewer Detail Page, Company Connections

#### Bug Fix: `if scored > 0` → `if weighted_scores`
When all articles for a symbol were already scored in a previous run, `scored=0` caused
`_save_symbol_scores` to be skipped entirely. Time-decay was recalculated correctly but
the result was thrown away. Fix: guard now checks `if weighted_scores` — symbol final_score
refreshes on every run regardless of whether new LLM calls were made.
- `pipeline/sentiment_scoring.py`

#### Bug Fix: JSON Parse Failures (Stage 2 returning markdown prose)
Model ignored `format: json` and returned markdown headers/emoji instead of JSON.
Three root causes fixed:
1. `json_repair` promoted to pass 2 (runs before manual brace patching)
2. On retry 2+, conversation now shows the model its own bad response with explicit correction
3. Debug log promoted to WARNING — raw output (first 400 chars) now always visible on failure
- `pipeline/sentiment_scoring.py`

#### Bug Fix: Truncated JSON (open string, no closing brace)
Model output ending mid-sentence (no closing `}`) caused `rfind("}")` to return -1,
silently skipping the repair path. `json_repair` with `return_objects=True` then returned
a non-dict and was rejected. Fix: `repair_json` now called as string output then re-parsed
via `json.loads` — handles open strings and truncated values reliably.
- `pipeline/sentiment_scoring.py`

#### Time-Decay Overhaul — Grace Period + Gentler Lambda
Old: λ=0.02/hr → half-life 34.7 hours. Earnings from 3 months ago decayed to ~0.
New: grace period (default 6 months) where NO decay is applied. After grace, gentler
λ=0.001/hr (half-life ~29 days). The older the news, the more it decays — but recent
same-year reports retain full weight.
New config vars in `pipeline_config.py`:
  DECAY_GRACE_MONTHS = 6      # no decay inside this window
  SENTIMENT_LAMBDA = 0.001    # decay rate after grace period
- `pipeline_config.py`, `pipeline/sentiment_scoring.py`

#### Ollama Context: Persistent Modelfiles (16k)
Ollama defaults context to 4096 tokens (silently truncating inputs).
Created persistent Modelfiles on remote Debian server (10.11.12.8):
  gemma4:e2b-ctx16k — Stage 1 model, num_ctx=16384
  gemma4:e4b-ctx16k — Stage 2 model, num_ctx=16384
`config.py` STAGE1_MODEL + STAGE2_MODEL updated to point to ctx16k variants.
- `config.py`

#### single_model_mode Disabled
VRAM check was sometimes falling back to single-model mode (stage1 reuses stage2 model).
`use_single_model` hardcoded to False — both models always loaded independently.
- `pipeline/sentiment_scoring.py`

#### Per-Article Timing Log
Each article now logs: symbol, article_id, score, s1 time, s2 time, headline.
Example: `[AAPL] article=1234 score=+0.72 s1=4.2s s2=9.1s | Apple Reports Record Q2...`
s1=0.0s means Stage 1 was cached (pre_summary_data already in DB).
- `pipeline/sentiment_scoring.py`

#### Company Connections Extracted by Stage 2 LLM
Stage 2 now extracts structured company relationships per article:
  {"competitors": ["ABBV","MRK"], "partners": ["RHHBY"], "suppliers": []}
Stored in new `news_articles.company_connections JSONB` column.
Aggregated across all articles on the viewer detail page.
- `pipeline/stage2_instruction.json`, `pipeline/sentiment_scoring.py`, `db/schema.py`

#### Viewer: Full Symbol Detail Page
Clicking a symbol in the leaderboard opens a full detail page /symbol/{id}:
- Score hero + TV screener grid (price, volume, margins, ratios, next earnings date)
- Symbol forecast narrative (full text)
- Master summary
- Company connections (aggregated: competitors, partners, suppliers)
- Sector macro context + market research articles
- All scored articles with expandable LLM input (collapsible): master_summary,
  pre_summary_data, TV data, stage2_prompt, key_events, forecast, rationale
- Industry pulled from symbol_daily_snapshots
- `viewer.py`

#### Admin: Intelligence Tab
New 🧠 Intelligence tab on every symbol panel:
- Industry/sector from symbol_daily_snapshots
- TradingView data grid
- Full forecast narrative
- Master summary
- Company connections (competitors/partners/suppliers)
- Sector macro rationale
- `admin.py`

#### Files Changed
- `pipeline/sentiment_scoring.py` — decay fix, JSON parse, timing log, connections save, single_model forced off
- `pipeline_config.py` — DECAY_GRACE_MONTHS, SENTIMENT_LAMBDA updated
- `pipeline/stage2_instruction.json` — company_connections added to output schema
- `db/schema.py` — company_connections JSONB column migration
- `config.py` — STAGE1_MODEL + STAGE2_MODEL → ctx16k
- `viewer.py` — symbol detail page
- `admin.py` — Intelligence tab

---

### [3.0.4] – 2026-05-28 — Ollama ctx fix, LLM debug tab, stage2_prompt saved

#### Ollama `num_ctx` override
Ollama defaults context window to 2048 tokens — silently truncating inputs.
Both Stage 1 and Stage 2 now pass `num_ctx` via `extra_body` when `LLM_TYPE="ollama"`:
```python
extra_body = {"num_ctx": LLM_CONFIG["context_size"], "top_k": LLM_CONFIG["top_k"]}
```
Stage 1 article input limit also raised 3000 → 6000 chars. Stage 1 `max_tokens` 1024 → 2048.
Stage 2 removed artificial `min(..., 2048)` cap — now uses full `LLM_CONFIG["max_tokens"]`.

#### stage2_prompt saved per article
New column `news_articles.stage2_prompt TEXT` — the exact JSON blob sent to the main LLM.
Both `_process_symbol` and `score_single_article` now pass `stage2_prompt=prompt` to `_save_article_result`.
Articles scored before v3.0.4 show "Not saved — scored before v3.0.4" in the debug view.

#### Admin: 🔬 LLM Debug tab
New tab on every symbol detail panel. Two views:
- List view: all articles sorted newest first, score (+/-/unscored) color-coded, S1✓/PROMPT✓ chips
- Detail view (click any article): full breakdown of every field:
  - Sentiment score + weighted score + source
  - Article summary, score rationale, forecast until earnings
  - Key events JSON (Stage 2 output)
  - Stage 1 pre_summary_data JSON
  - Master summary snapshot (context at time of scoring)
  - Full Stage 2 prompt (exact input sent to LLM)
  - Raw article full_text (up to 5000 chars)

#### Files Changed
- `pipeline/sentiment_scoring.py` — Ollama num_ctx, Stage1 text limit, Stage2 max_tokens cap removed, stage2_prompt saved
- `db/schema.py` (migration run) — `news_articles.stage2_prompt TEXT`
- `admin.py` — import json added, 🔬 LLM Debug tab + route `/symbol/{id}/debug`

---


### [3.0.0] – 2026-05-27 — Phase 4: Live Orchestrator + Sentiment Scoring Foundation

#### Architecture: Separate Orchestrator Process
`orchestrator.py` introduced as a long-running background process, separate from `main.py`.
`main.py` remains the one-shot batch pipeline. `orchestrator.py` is the live daemon.

Three workers defined (W2+W3 disabled at launch — see v3.0.1):

| Worker | Interval | Task |
|--------|----------|------|
| Worker 1 | 60s | GlobeNewswire live press releases → instant sentiment score |
| Worker 2 | 1hr | RSS + HTML ingest → score unscored articles |
| Worker 3 | 24hr | Market research ingest + macro multiplier LLM rerun |

Worker 1 runs `score_single_article(art_id, symbol_id)` in a daemon thread
immediately after each new article is inserted — sub-minute latency from publish to score.

#### DB Schema (db/schema.py)
New columns on `news_articles`:
- `sentiment_score NUMERIC(5,3)` — raw LLM score (-1.000 to +1.000)
- `weighted_sentiment NUMERIC(5,3)` — score × e^(−λt) time-decay
- `article_summary TEXT` — Stage 1 LLM pre-summary (cached)
- `master_summary_snapshot TEXT` — rolling symbol-level context snapshot at score time
- `key_events JSONB` — structured events extracted by LLM
- `pre_summary_data JSONB` — gemma pre-summary cache (avoids re-summarizing on rescore)

New columns on `symbols`:
- `final_score NUMERIC(8,4)` — aggregated weighted score across all articles
- `score_updated_at TIMESTAMPTZ`
- `symbol_master_summary TEXT` — latest rolling narrative for the symbol
- `symbol_forecast_narrative TEXT` — forward-looking LLM output

Partial index on `news_articles WHERE sentiment_score IS NULL` for fast unscored queries.

#### pipeline/sentiment_scoring.py
Two-stage LLM pipeline:
- Stage 1: gemma-4b (fast, cheap) — pre-summarize article, cache in `pre_summary_data`
- Stage 2: main LLM — stateful scoring with rolling `master_summary` context per symbol
- Time-decay: `weighted = score × exp(−λ × t_hours)` where λ from `pipeline_config.py`
- Public API: `run()` (batch all unscored), `score_single_article(art_id, symbol_id)` (fast path)

#### pipeline_config.py additions
```python
WORKER1_INTERVAL = 60      # seconds
WORKER2_INTERVAL = 3600
WORKER3_INTERVAL = 86400
MAX_EVAL_ARTICLES = 50
ENABLE_PRE_SUMMARIZATION = True
SUMMARY_LLM_MODEL = "google/gemma-4-e2b"
SENTIMENT_LAMBDA = 0.05    # decay rate
```

#### Files Changed
- `db/schema.py` — sentiment columns on news_articles + symbols, partial index
- `pipeline/sentiment_scoring.py` — new file
- `pipeline/orchestrator.py` — new file
- `pipeline_config.py` — worker intervals + scoring config
- `main.py` — `sentiment` stage wired in
- `requirements.txt` — created

---

### [2.1.0] – 2026-05-25 — Bug Fixes, Dev Controls, Market Scores Admin Panel

#### Bug Fixes

**news_ingest.py — Rogue newline in User-Agent header**
`HEADERS["User-Agent"]` had a literal `\n` in the string. `requests` raises
`InvalidHeader` on any newline in a header — every `_scrape_full_text()` call
silently returned `None`. Result: 0/426 full text scraped. Fixed: removed the `\n`.

**html_ingest.py — File corruption (line numbers baked into source)**
A write_file call with line numbers embedded (`1|"""`) corrupted the file.
The file was also truncated at exactly 500 lines — the missing ~300 lines
(`_handle_nasdaq_page` completion, `_handle_q4ir_api`, `_detect_platform`,
`_process_html_feed`, `_get_html_feeds`, `_save_articles`, `run()`) were
reconstructed from session history and appended. File now compiles cleanly.

**html_ingest.py — Accept-Encoding: gzip, deflate, br in all 4 header profiles**
Same Brotli bug as news_ingest (documented in v1.0.0). All 4 `_HEADER_PROFILES`
in html_ingest had `Accept-Encoding: gzip, deflate, br`. Brotli responses came
back but `requests` can't decompress them → pipeline stalled at ACGL every run.
Fixed: removed `Accept-Encoding` from all 4 profiles.

**html_ingest.py — BaseException not caught in futures loop**
Worker futures caught `except Exception` only. `SystemExit` (a `BaseException`)
propagated straight through `fut.result()` and killed the main process after
the first bad worker. Fixed: changed to `except BaseException`, logs type name,
continues.

**html_ingest.py — s.ticker column doesn't exist**
`_get_html_feeds()` used `s.ticker` in 4 places but the column is `s.symbol`.
Every html_ingest run crashed with `UndefinedColumn: Spalte s.ticker existiert nicht`.
Fixed: all 4 occurrences changed to `s.symbol`.

**sector_map.py — Scanner import error**
`from tradingview_screener import Scanner, Query, Column` — `Scanner` doesn't
exist in the installed version. Only `Query` and `Column` are exported.
Fixed: removed `Scanner` from import.

**sector_map.py — pipeline_runs missing symbols_mapped / symbols_total columns**
`UPDATE pipeline_runs SET symbols_total=..., symbols_mapped=...` crashed with
`UndefinedColumn`. Fixed: added both columns via
`ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS` in schema.py.

**sector_map.py — TradingView returns NASDAQ:AAPL, DB has AAPL → 0 mapped**
TV ticker format is `{EXCHANGE}:{TICKER}`. `sym_by_ticker` dict keyed on plain
ticker never matched → `mapped=0` even though TV returned 400 rows.
Fixed: strip exchange prefix before lookup. `mapped` counter also fixed to read
final count from DB rather than tracking deltas (re-runs showed 0 even when data
was already written).

**admin.py — get_connection not defined in market scores routes**
New `/market-scores` routes used `get_connection()` but the project uses `get_conn()`.
All 6 call sites in the new routes updated.

**admin.py — Market Scores panel showing only 22 of 389 rows**
HTMX was truncating large HTML responses. Removed LIMIT/OFFSET pagination,
added `max-height: 65vh; overflow-y: auto` scrollable container instead.
All 389 rows load at once.

---

#### New Features

**SYMBOL_LIMIT — pipeline_config.py**
Cap how many symbols are processed without touching code:
```python
SYMBOL_LIMIT = False   # all symbols (production)
SYMBOL_LIMIT = 100     # first 100 symbols (dev/test)
```
Applied in both `news_ingest.py` and `html_ingest.py`. Limit is per symbol
(not per feed) — a symbol with 3 feeds still gets all 3 of its feeds.

**START_FROM — skip-to stage control**
Two ways to start the pipeline mid-way without rerunning earlier stages:

CLI flag (one-off):
```
python main.py --start-from html
python main.py --start-from sector_map
```

Config (persistent):
```python
START_FROM = "html"   # in pipeline_config.py
```
Valid values: `universe`, `news`, `html`, `sector_map`, `market_research`,
`macro_multiplier`. CLI flag overrides the config value.

**Admin — 📈 Market Scores panel**
New green button in admin header opens the `sectors_macro` table as a full panel:
- Sector tag chip + Industry name
- Visual multiplier bar (color-coded: gray=neutral, amber=mild, green=strong,
  bright green=exceptional) + numeric `1.035×` label
- LLM rationale text
- Last LLM run timestamp
- Delete button per row (inline, no reload)
- "Delete All Rankings" button at top (confirm dialog) for fresh LLM re-run
- Scrollable table, all rows loaded at once

**market_research_sources.csv imported**
23 market research feeds bulk-imported into `market_research_feeds` table:
- 18 GlobeNewswire org feeds (SNS Insider, Grand View Research, MarketsandMarkets, etc.)
- 5 FDA RSS feeds (drug approvals, safety alerts, etc.)
1 duplicate (SNS Insider) skipped. Pipeline now has real feeds to process.

---

#### How-To Reference

**Change LLM type** — edit `config.py` or set env var:
```
LLM_TYPE=local    # LM Studio at 127.0.0.1:1234 (default)
LLM_TYPE=ollama   # Ollama at 10.11.12.8:11434
LLM_TYPE=api      # OpenAI-compatible, set OPENAI_API_KEY in .env
```

**Change LLM batch size** — edit `pipeline/macro_multiplier.py` line:
```python
BATCH_SIZE = 20   # articles per LLM call — increase for fewer calls, decrease for more precision
```

**Change article text sent to LLM** — edit `pipeline/macro_multiplier.py`:
```python
(a.get('full_text') or a.get('summary') or '')[:3000]   # current: summary[:500]
```
Use `full_text` for richer analysis of scraped market research articles.

**Scoring architecture decision (2026-05-25)**
Macro multiplier is applied to the FINAL aggregated raw score, not per-article.
Multi-sector companies: use MAX(multiplier) across all sectors the company belongs to.
Formula: `final_score = raw_score × MAX(macro_multiplier of company's sectors)`
Rationale: per-news multiplier is noisy (one bad article in a hot sector inflates score);
final multiplier is clean, transparent, and explainable.

---

### [2.0.0] – 2026-05-25 — Phase 3: Industry & Macro Mapping

#### Architecture
Phase 3 fully implemented. Three new pipeline modules + schema + admin UI.

#### DB Schema (`db/schema.py`)
- `sectors_macro`: sector_name, industry_name, macro_multiplier (NUMERIC 4,3 range 1.000-1.050),
  rationale (LLM text), last_llm_run_at. Unique on (sector_name, industry_name).
- `market_research_feeds`: market-wide RSS feeds, not ticker-specific. source_name, description.
- `market_research_articles`: articles from above. llm_processed flag + partial index on FALSE.
- `symbols.sector_id` FK → sectors_macro(id). Added via idempotent DO $$ ALTER block.

#### `config.py` — LLM provider system
- LLM_TYPE env var: "local" | "ollama" | "api". Default: "local".
- local: LM Studio 127.0.0.1:1234, model=google/gemma-4-e4b
- ollama: 10.11.12.8:11434/v1, model=gemma4:e4b, reasoning_mode=True, num_gpu_layers=40
- api: OpenAI-compatible, api_key blank by default (set in .env)
- All profiles: temp=0.1, context=16384, max_tokens=12228, freq_penalty=0.2, pres_penalty=0.1

#### `pipeline/sector_map.py`
- TradingView Screener Query().select(sector, industry). Batches of 500, 0.5s rate limit.
- Upserts sectors_macro (multiplier=1.000 default). Maps sector_id onto symbols.
- Logs to pipeline_runs step='sector_map'.

#### `pipeline/market_research_ingest.py`
- Fetches market_research_feeds RSS/Atom. Stores to market_research_articles.
- curl_cffi fallback, browser headers, _suppress_stderr — same hardening as news_ingest.

#### `pipeline/macro_multiplier.py`
- Reads llm_processed=FALSE articles in batches of 20. Calls LLM via OpenAI client.
- Extracts signals: sector/industry/niche/product/disease/medication + growth_score 0.0-1.0.
- Score → multiplier: [0.4,1.0] → [1.000,1.050] linear. Below 0.4 = 1.000 (neutral).
- GREATEST() on UPDATE — multiplier only increases, never decreases from weak articles.
- --dry-run flag: prints signals without writing DB.

#### Admin UI
- "Market Research" button in header → full panel in right pane.
- Add/delete market research feeds with source name + description.
- Article count + pending LLM badges. "Run LLM Analysis" button (50-article batch).

#### `pipeline_config.py` + `main.py`
- New toggles: sector_map, market_research, macro_multiplier (all active=True).
- STAGES order: universe → news → sector_map → market_research → macro_multiplier.

---

### [1.4.0] – 2026-05-24 — Per-Symbol Fetch Button in Admin Panel

#### Feature: Run Pipeline from Admin UI
Every symbol row now has a ▶ button on the right side.
Clicking it runs the full pipeline (RSS + HTML + EDGAR if enabled) for that single
symbol and streams the log output directly into the right panel — no terminal needed.

- Color-coded output: green = inserted/ok, red = error, gray = info
- Before/after article count shown at top so you see instantly what was inserted
- Back to feeds button returns to normal feeds view after the run
- `event.stopPropagation()` prevents the row's hx-get from firing when clicking ▶

Backend: `POST /symbol/{id}/fetch` — calls `test_symbol.run_symbol()` and captures
stdout + stderr via `subprocess`. Returns HTML with colored pre-formatted log.

#### Files Changed
- `admin.py` — fetch button in symbol row, CSS for button, `/symbol/{id}/fetch` POST route

---

### [1.3.0] – 2026-05-24 — Feed Type Selector in Admin Edit + Add Forms

#### Problem
Feeds incorrectly stored as `feed_type='rss'` (Nasdaq API URLs, Yahoo Finance URLs)
had to be fixed via raw SQL. The admin edit form only had a URL input — no way to
change `feed_type` from the UI.

#### Fix (admin.py)
- Edit form: added `feed_type` dropdown (rss / atom / html / api / unknown) pre-filled
  with the feed's current value. Current type chip visible on every feed card.
- Add form: same dropdown, defaults to `unknown`.
- `saveEdit` JS: for `html` and `api` types, skips feedparser validation entirely
  (feedparser always fails on non-RSS URLs). rss/atom/unknown still validate first.
- PUT + POST backend: both now accept `feed_type` from the form and use it as-is.
  Previously the backend silently re-detected and overwrote the type on every save.

#### Workflow
Find broken symbol via 0-articles filter → feeds tab → chip says `rss` but URL is
Nasdaq → Edit → change to `html` → Save → run test_symbol.py to verify immediately.

---

### [1.2.0] – 2026-05-24 — Admin Panel Filter + Sort System

#### Feature: Article Count Filtering + Sorting
The left symbol list now has a filter bar and sort bar for rapid triage of broken symbols.

Filter bar (5 buttons, instant HTMX, no page reload):
- All — default view
- 0 articles — symbols with zero news (red badge) — primary fix target
- Low <5 — symbols with 1-4 articles (orange badge) — likely partially broken
- OK — symbols with 5+ articles (green badge)
- No feed — symbols with no feed URL at all

Sort bar:
- A-Z — default alphabetical
- Fewest first — broken symbols pushed to top, best for triage
- Most first — best-covered symbols first

Article count badge on every row: red=0, orange=1-4, green=5+.

Workflow: click "0 articles" then "Fewest first" → instantly see every broken symbol
sorted worst-first. Click one → go to feeds tab → debug URL/type.

#### Files Changed
- `admin.py` — filter/sort query params in `/symbols` route, CSS badge styles,
  filter bar HTML rendered in left panel head

---

### [1.1.0] – 2026-05-24 — test_symbol.py CLI Diagnostic Tool

#### New File: test_symbol.py
Run the full pipeline for a single symbol and see results immediately.
No more waiting for the full 3800-symbol run to check if one symbol works.

Usage:
```
python test_symbol.py GYRO
python test_symbol.py BNBX
python test_symbol.py ABNB
```

What it does per symbol:
1. Shows symbol info + all feeds with active/inactive status and feed_type
2. Shows article count BEFORE run
3. Runs RSS pipeline for all rss/atom feeds belonging to that symbol
4. Runs HTML pipeline (Nasdaq, Q4 IR, Yahoo, PRNewswire, mynewsdesk etc.)
5. Runs EDGAR pipeline if enabled in pipeline_config.py
6. Shows exact count: X fetched, Y new inserted per feed
7. Shows total article count BEFORE and AFTER at bottom

Saves results to DB — identical to what main.py does, just scoped to one symbol.

---

### [1.0.0] – 2026-05-24 — Platform Handler: mynewsdesk + Brotli Bug Fix + status Filter Bug

#### Bug 1 — s.status = TRUE blocking 562 symbols from all pipelines
Root cause: `status = FALSE` in the symbols table means "not on active watchlist"
NOT "delisted". But all three pipelines had `AND s.status = TRUE` in their feed
queries — silently dropping 562 real listed companies with valid feeds.

Affected files: `news_ingest.py`, `html_ingest.py`, `rss_finder.py`
Fix: removed `AND s.status = TRUE` from all three. Feed's own `is_active` column
is the correct gate.

Result: 350 previously-invisible RSS/atom feeds + unknown HTML feeds now process
correctly. Confirmed on ABVX, AAPG, ABP — all had valid feeds but 0 articles.

#### Bug 2 — Brotli compression causing feedparser to get binary garbage
Root cause: `_HEADER_PROFILES` in `news_ingest.py` included `Accept-Encoding: gzip, deflate, br`.
Servers returned Brotli-compressed responses. `requests` decompresses gzip natively
but NOT Brotli (requires the `brotli` package). `resp.text` was binary garbage.
feedparser got garbage → `bozo=True` → 0 entries. Silent failure.

Affected feed type: `adapthealth.com/blogs/latest-news.atom` and any Brotli-capable server.
Fix: removed `Accept-Encoding` from ALL header profiles. `requests` adds its own
`Accept-Encoding: gzip, deflate` automatically and handles decompression correctly.

#### Bug 3 — Nasdaq + Yahoo URLs stored as feed_type='rss'
Bulk of Nasdaq API URLs and Yahoo Finance URLs were classified as `rss` by the
URL heuristic at import time. feedparser consumed them, returned 0. html_ingest
never saw them.
Fix: bulk UPDATE in DB (`feed_type='html'` for these URL patterns) + `_detect_platform`
updated to catch `nasdaq.com/api/news/topic` URL format.

Affected symbols: ADSE and all others with Nasdaq API-style or Yahoo press-release URLs.

#### New Platform: mynewsdesk.com
`_handle_mynewsdesk()` added to `html_ingest.py`.
- RSS autodiscovery first (many mynewsdesk pages have `/newsroom/{company}/rss`)
- Falls back to scraping `a[href*="/pressreleases/"]` anchor links from server-rendered page
- Trafilatura extracts full text from each article URL

Verified on ADSE: 12 mynewsdesk articles + 10 Yahoo + 40 Nasdaq = 62 total.

#### Files Changed
- `pipeline/news_ingest.py` — removed status filter, removed Accept-Encoding
- `pipeline/html_ingest.py` — removed status filter, mynewsdesk handler, Nasdaq URL pattern fix
- `pipeline/rss_finder.py` — removed status filter

---

### [0.9.1] – 2026-05-24 — lxml stderr Encoding Error Suppression

#### Problem
lxml's C parser writes directly to file descriptor 2 (stderr) when it encounters
malformed or compressed bytes in HTML bodies — completely bypassing Python's
`warnings` module. Output was 4 identical error lines per article for many Q4 API
symbols:
```
encoding error : input conversion failed due to input error, bytes 0x8D 0x89 0xBF 0x0F
```

`warnings.filterwarnings` has zero effect on C-level fd writes.

#### Fix (pipeline/html_ingest.py)
Added `_suppress_stderr()` context manager that redirects file descriptor 2 to
`os.devnull` at OS level before every BeautifulSoup call and restores it after.
All 6 `BeautifulSoup(` call sites replaced with `_make_soup()` wrapper.

Data is unaffected — lxml still parses correctly, it just stops printing about
malformed bytes. Zero articles lost.

---

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
