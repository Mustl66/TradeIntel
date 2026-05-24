import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import psycopg2, psycopg2.extras

conn = psycopg2.connect(dbname="tradeintel", user="postgres", password="postgres", host="localhost")
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# 1. What feed_type are those GNW atomfeed URLs?
cur.execute("""
    SELECT feed_url, feed_type, source, is_active
    FROM rss_feeds
    WHERE feed_url ILIKE '%atomfeed/organization%'
    LIMIT 10
""")
rows = cur.fetchall()
print("=== GNW atomfeed/organization feed_types ===")
for r in rows:
    print(f"  {r['feed_type']:8} | {r['source']:15} | {r['feed_url'][-50:]}")

# 2. How many feeds are rss/atom type vs html?
cur.execute("""
    SELECT feed_type, COUNT(*) as cnt
    FROM rss_feeds
    WHERE is_active = TRUE
    GROUP BY feed_type ORDER BY cnt DESC
""")
print("\n=== Active feeds by type ===")
for r in cur.fetchall():
    print(f"  {r['feed_type']:10} : {r['cnt']}")

# 3. Does news_ingest filter by feed_type? Check with a sample
cur.execute("""
    SELECT COUNT(*) as total_active_feeds
    FROM rss_feeds rf
    JOIN symbols s ON s.id = rf.symbol_id
    WHERE rf.is_active = TRUE AND s.status = TRUE AND s.exchange = 'NASDAQ'
""")
print(f"\n=== Total feeds news_ingest would process (no type filter): {cur.fetchone()['total_active_feeds']}")

cur.execute("""
    SELECT COUNT(*) as rss_only
    FROM rss_feeds rf
    JOIN symbols s ON s.id = rf.symbol_id
    WHERE rf.is_active = TRUE AND s.status = TRUE AND s.exchange = 'NASDAQ'
    AND rf.feed_type IN ('rss', 'atom')
""")
print(f"=== RSS/Atom only feeds: {cur.fetchone()['rss_only']}")

# 4. Sample of html-type feeds being wrongly pulled into news_ingest
cur.execute("""
    SELECT rf.feed_url, rf.feed_type, s.symbol
    FROM rss_feeds rf JOIN symbols s ON s.id = rf.symbol_id
    WHERE rf.is_active = TRUE AND rf.feed_type = 'html'
    LIMIT 5
""")
print("\n=== Sample html feeds wrongly in news_ingest scope ===")
for r in cur.fetchall():
    print(f"  [{r['symbol']}] {r['feed_url'][:70]}")

conn.close()
