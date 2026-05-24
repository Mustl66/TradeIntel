"""
Clean old wrong Nasdaq articles and run html_ingest on Nasdaq feeds only.
"""
import sys, logging
sys.path.insert(0, r'C:\Users\Mustafa\PycharmProjects\TradeIntel')
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')

import psycopg2
from db.connection import get_connection

conn = get_connection()
cur = conn.cursor()

# Delete articles that came from old wrong BusinessWire/GNW fallback for Nasdaq feeds
cur.execute("""
    DELETE FROM news_articles na
    WHERE na.source_name IN ('businesswire_rss', 'globenewswire_listing')
      AND EXISTS (
        SELECT 1 FROM rss_feeds rf
        WHERE rf.id = na.feed_id
          AND rf.feed_url ILIKE '%nasdaq.com/market-activity%'
      )
""")
deleted = cur.rowcount
conn.commit()
print(f"Deleted {deleted} wrong fallback articles from Nasdaq feeds")

# Delete old nasdaq_api articles to get fresh clean set
cur.execute("DELETE FROM news_articles WHERE source_name = 'nasdaq_api'")
deleted2 = cur.rowcount
conn.commit()
print(f"Deleted {deleted2} old nasdaq_api articles (re-fetching fresh)")

cur.close()
conn.close()

# Now run html_ingest for ALL Nasdaq HTML feeds (no limit = process all)
from pipeline.html_ingest import run
print("\nRunning HTML ingest on all Nasdaq HTML feeds...")
result = run(exchange='NASDAQ', limit=0)
print(f"\nResult: {result}")
