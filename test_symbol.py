"""
Run the full pipeline for a single symbol and save to DB.
Usage:  python test_symbol.py ABVX
"""

import sys, os, logging
sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s"
)

if len(sys.argv) < 2:
    print("Usage: python test_symbol.py <SYMBOL>")
    print("Example: python test_symbol.py ABVX")
    sys.exit(1)

symbol = sys.argv[1].upper().strip()
print(f"\n{'='*55}")
print(f"  TradeIntel pipeline test → {symbol}")
print(f"{'='*55}\n")

# ── DB lookup ─────────────────────────────────────────────────────────────────
import psycopg2, psycopg2.extras
from db.connection import get_connection as get_conn

conn = get_conn()
with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    cur.execute("SELECT id, symbol, company_name, status FROM symbols WHERE symbol = %s", (symbol,))
    sym = cur.fetchone()

if not sym:
    print(f"[ERROR] Symbol '{symbol}' not found in DB.")
    conn.close()
    sys.exit(1)

print(f"Found: {sym['symbol']} — {sym['company_name']} (id={sym['id']}, status={sym['status']})\n")

# ── Feeds for this symbol ─────────────────────────────────────────────────────
with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    cur.execute("""
        SELECT f.id, f.feed_url, f.feed_type, f.source, f.is_active,
               s.id AS symbol_id, s.symbol
        FROM rss_feeds f
        JOIN symbols s ON s.id = f.symbol_id
        WHERE f.symbol_id = %s
        ORDER BY f.feed_type
    """, (sym['id'],))
    feeds = cur.fetchall()

conn.close()

if not feeds:
    print("[WARNING] No feeds found for this symbol.")
else:
    print(f"Feeds ({len(feeds)}):")
    for f in feeds:
        status = "✓" if f['is_active'] else "✗"
        print(f"  [{status}] {f['feed_type']:8s}  {f['source']:15s}  {f['feed_url']}")
    print()

# ── Count articles before ─────────────────────────────────────────────────────
conn = get_conn()
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM news_articles WHERE symbol_id = %s", (sym['id'],))
    before = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM news_articles WHERE symbol_id = %s AND source_name = 'edgar_8k'", (sym['id'],))
    before_edgar = cur.fetchone()[0]
conn.close()

print(f"Articles in DB before run: {before} news + {before_edgar} SEC filings\n")

# ── Pipeline config ───────────────────────────────────────────────────────────
from pipeline_config import PIPELINES

rss_feeds  = [f for f in feeds if f['feed_type'] in ('rss', 'atom', 'unknown') and f['is_active']]
html_feeds = [
    {**dict(f), "feed_id": f["id"]}
    for f in feeds
    if f['feed_type'] in ('html', 'api') and f['is_active']
]

# ── RSS pipeline ──────────────────────────────────────────────────────────────
if PIPELINES['rss']['active'] and rss_feeds:
    print(f"── RSS pipeline ({len(rss_feeds)} feed(s)) ──────────────────────────────")
    from pipeline.news_ingest import _fetch_feed, _insert_articles
    conn = get_conn()
    for feed in rss_feeds:
        print(f"  Fetching: {feed['feed_url']}")
        try:
            articles = _fetch_feed(feed)
            if articles:
                inserted = _insert_articles(conn, articles)
                print(f"  → {len(articles)} fetched, {inserted} new inserted")
            else:
                print(f"  → 0 entries returned")
        except Exception as e:
            print(f"  [ERROR] {e}")
    conn.close()
    print()
else:
    if not PIPELINES['rss']['active']:
        print("── RSS pipeline DISABLED in pipeline_config.py ──\n")
    elif not rss_feeds:
        print("── No RSS/atom feeds for this symbol ──\n")

# ── HTML pipeline ─────────────────────────────────────────────────────────────
if PIPELINES['html']['active'] and html_feeds:
    print(f"── HTML pipeline ({len(html_feeds)} feed(s)) ─────────────────────────────")
    from pipeline.html_ingest import _process_html_feed
    for feed in html_feeds:
        print(f"  Fetching: {feed['feed_url']}")
        try:
            # _process_html_feed returns (symbol, n_inserted) — saves internally
            result = _process_html_feed(feed)
            _, inserted = result if isinstance(result, tuple) else (feed['symbol'], 0)
            print(f"  → {inserted} new inserted")
        except Exception as e:
            print(f"  [ERROR] {e}")
    print()
else:
    if not PIPELINES['html']['active']:
        print("── HTML pipeline DISABLED in pipeline_config.py ──\n")
    elif not html_feeds:
        print("── No HTML feeds for this symbol ──\n")

# ── EDGAR pipeline ────────────────────────────────────────────────────────────
if PIPELINES['edgar']['active']:
    print("── EDGAR pipeline ───────────────────────────────────────────────")
    from pipeline.edgar_ingest import EdgarIngestor
    ingestor = EdgarIngestor()
    try:
        count = ingestor.run_single(symbol=sym['symbol'], symbol_id=sym['id'])
        print(f"  → {count} SEC filings inserted\n")
    except Exception as e:
        print(f"  [ERROR] {e}\n")
else:
    print("── EDGAR pipeline DISABLED in pipeline_config.py ──\n")

# ── Count articles after ──────────────────────────────────────────────────────
conn = get_conn()
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM news_articles WHERE symbol_id = %s", (sym['id'],))
    after = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM news_articles WHERE symbol_id = %s AND source_name = 'edgar_8k'", (sym['id'],))
    after_edgar = cur.fetchone()[0]
conn.close()

new_news  = (after - after_edgar) - (before - before_edgar)
new_edgar = after_edgar - before_edgar

print(f"{'='*55}")
print(f"  DONE — {symbol}")
print(f"  News   : {after - after_edgar} total  (+{new_news} new)")
print(f"  SEC    : {after_edgar} total  (+{new_edgar} new)")
print(f"{'='*55}\n")
