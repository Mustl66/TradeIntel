"""
Fix: delete false GlobeNewswire articles for symbols whose feed is a Nasdaq HTML page.
These were inserted by the old broken BusinessWire RSS fallback.
Then add GYRO to EDGAR pipeline by including symbols with html-only feeds.
"""
import sys; sys.path.insert(0, '.')
from db.connection import get_connection
conn = get_connection()
cur = conn.cursor()

# How many false articles came from Nasdaq HTML pages via GNW scrape?
cur.execute("""
    SELECT COUNT(*)
    FROM news_articles na
    JOIN rss_feeds rf ON rf.id = na.feed_id
    WHERE rf.feed_url LIKE '%nasdaq.com/market-activity/stocks/%/press-releases'
      AND na.source_name = 'globenewswire_scrape'
""")
count = cur.fetchone()[0]
print(f"False GNW articles from Nasdaq HTML feeds: {count}")

# Delete them
cur.execute("""
    DELETE FROM news_articles
    WHERE id IN (
        SELECT na.id
        FROM news_articles na
        JOIN rss_feeds rf ON rf.id = na.feed_id
        WHERE rf.feed_url LIKE '%nasdaq.com/market-activity/stocks/%/press-releases'
          AND na.source_name = 'globenewswire_scrape'
    )
""")
deleted = cur.rowcount
print(f"Deleted: {deleted} false articles")
conn.commit()

# Also check yahoo false articles
cur.execute("""
    SELECT COUNT(*)
    FROM news_articles na
    JOIN rss_feeds rf ON rf.id = na.feed_id
    WHERE rf.feed_url LIKE '%yahoo.com%'
""")
print(f"Yahoo-sourced articles: {cur.fetchone()[0]}")

conn.close()
