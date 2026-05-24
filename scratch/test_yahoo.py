import sys
sys.path.insert(0, ".")
from pipeline.html_ingest import _handle_yahoo_finance, _insert_articles
from db.connection import get_connection

conn = get_connection()
with conn.cursor() as cur:
    cur.execute("""
        SELECT s.id, s.symbol, f.id as feed_id, f.feed_url, f.feed_type
        FROM symbols s
        JOIN rss_feeds f ON f.symbol_id = s.id
        WHERE s.symbol = 'ACTG'
    """)
    rows = cur.fetchall()
conn.close()

print("All ACTG feeds:")
for r in rows:
    print(f"  feed_id={r[2]} type={r[4]} url={r[3]}")

# Try with the yahoo URL directly regardless of feed_type
yahoo_row = next((r for r in rows if "yahoo" in (r[3] or "").lower()), None)
if not yahoo_row:
    # Try direct test with constructed URL
    print("\nNo yahoo feed stored — testing with constructed URL directly")
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM symbols WHERE symbol='ACTG'")
        sym_id = cur.fetchone()[0]
    conn.close()
    url = "https://finance.yahoo.com/quote/ACTG/press-releases/"
    arts = _handle_yahoo_finance(url, "ACTG", -1, sym_id)
else:
    sym_id, sym, feed_id, feed_url, _ = yahoo_row
    print(f"\nTesting: {feed_url}")
    arts = _handle_yahoo_finance(feed_url, sym, feed_id, sym_id)

print(f"\nArticles found: {len(arts)}")
for a in arts[:5]:
    print(f"  {a['published_at']} | {a['title'][:75]}")

if arts and yahoo_row:
    sym_id, sym, feed_id, feed_url, _ = yahoo_row
    inserted = _insert_articles(get_connection(), arts)
    print(f"\nInserted into DB: {inserted}")
