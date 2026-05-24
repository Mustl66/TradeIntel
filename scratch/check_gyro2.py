import sys; sys.path.insert(0, '.')
from db.connection import get_connection
conn = get_connection()
cur = conn.cursor()

# Check what's in DB for GYRO now
cur.execute("""
    SELECT na.source_name, COUNT(*)
    FROM news_articles na
    JOIN symbols s ON s.id = na.symbol_id
    WHERE s.symbol = 'GYRO'
    GROUP BY na.source_name
""")
rows = cur.fetchall()
print("GYRO by source:", rows)

# Check what symbol_id GYRO has
cur.execute("SELECT id, symbol FROM symbols WHERE symbol = 'GYRO'")
print("GYRO symbol:", cur.fetchall())

# Check the edgar feed row for GYRO
cur.execute("""
    SELECT rf.id, rf.feed_url, rf.source, rf.feed_type
    FROM rss_feeds rf
    JOIN symbols s ON s.id = rf.symbol_id
    WHERE s.symbol = 'GYRO'
""")
print("GYRO feeds:", cur.fetchall())

# Check if GYRO was in the 5 symbols processed
cur.execute("""
    SELECT s.id, s.symbol
    FROM symbols s
    WHERE s.exchange = 'NASDAQ'
      AND NOT EXISTS (
          SELECT 1 FROM rss_feeds r
          WHERE r.symbol_id = s.id
            AND r.feed_type IN ('rss', 'atom')
            AND r.is_active = true
      )
      AND NOT EXISTS (
          SELECT 1 FROM rss_feeds r
          WHERE r.symbol_id = s.id
            AND r.source = 'edgar_8k'
      )
    AND s.symbol = 'GYRO'
""")
print("GYRO in no-RSS list:", cur.fetchall())

# Check all news with edgar_8k source for recent entries
cur.execute("""
    SELECT s.symbol, na.title, na.published_at
    FROM news_articles na
    JOIN symbols s ON s.id = na.symbol_id
    WHERE na.source_name = 'edgar_8k'
    ORDER BY na.inserted_at DESC
    LIMIT 10
""")
print("\nMost recent EDGAR inserts:")
for sym, title, pub in cur.fetchall():
    print(f"  {sym} | {pub.date() if pub else 'N/A'} | {title[:60]}")

conn.close()
