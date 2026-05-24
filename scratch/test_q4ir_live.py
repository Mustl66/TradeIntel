import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import psycopg2
import psycopg2.extras
from pipeline.html_ingest import _handle_q4ir_api, _insert_articles
from db.connection import get_connection

conn = get_connection()

# Find ABNB, BLFY, ADTN in DB — all have default.aspx feeds
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("""
    SELECT s.symbol, s.id AS symbol_id, f.id AS feed_id, f.feed_url
    FROM rss_feeds f JOIN symbols s ON s.id = f.symbol_id
    WHERE f.feed_url ILIKE '%default.aspx%'
      AND s.symbol IN ('ABNB','BLFY','ADTN','ACGL','ADV')
    LIMIT 5
""")
rows = cur.fetchall()
cur.close()

print(f'Testing {len(rows)} symbols with default.aspx feeds\n')

for row in rows:
    sym   = row['symbol']
    url   = row['feed_url']
    print(f'=== {sym}: {url}')
    arts  = _handle_q4ir_api(url, sym, row['feed_id'], row['symbol_id'])
    print(f'  → {len(arts)} articles fetched')
    if arts:
        print(f'  First: {arts[0]["published_at"]} | {arts[0]["title"][:80]}')
        print(f'  Last:  {arts[-1]["published_at"]} | {arts[-1]["title"][:80]}')
        n = _insert_articles(conn, arts)
        print(f'  DB: {n} inserted/updated (dedup handled by ON CONFLICT)')
    print()

conn.close()
