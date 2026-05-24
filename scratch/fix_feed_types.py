import sys
sys.path.insert(0, r'C:\Users\Mustafa\PycharmProjects\TradeIntel')
from db.connection import get_connection as get_conn

conn = get_conn()
cur = conn.cursor()

# Fix Nasdaq market-activity URLs stored as rss → html
cur.execute("""
    UPDATE rss_feeds SET feed_type='html'
    WHERE feed_url ILIKE '%nasdaq.com/market-activity%'
    AND feed_type='rss'
""")
print(f"Nasdaq fixed: {cur.rowcount} rows")

# Fix Yahoo quote press-release URLs stored as rss → html
cur.execute("""
    UPDATE rss_feeds SET feed_type='html'
    WHERE feed_url ILIKE '%yahoo.com/quote%'
    AND feed_type='rss'
""")
print(f"Yahoo fixed: {cur.rowcount} rows")

conn.commit()
conn.close()
print("Done.")
