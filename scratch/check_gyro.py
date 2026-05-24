import sys; sys.path.insert(0, '.')
from db.connection import get_connection
conn = get_connection()
cur = conn.cursor()

# Check ALL news for GYRO by source
cur.execute("""
    SELECT na.source_name, COUNT(*), MIN(na.published_at)::date, MAX(na.published_at)::date
    FROM news_articles na
    JOIN symbols s ON s.id = na.symbol_id
    WHERE s.symbol = 'GYRO'
    GROUP BY na.source_name
    ORDER BY COUNT(*) DESC
""")
print("GYRO articles by source:")
for src, cnt, min_d, max_d in cur.fetchall():
    print(f"  {src}: {cnt} articles ({min_d} to {max_d})")

# Check EDGAR articles for GYRO
cur.execute("""
    SELECT na.title, na.published_at, na.source_name
    FROM news_articles na
    JOIN symbols s ON s.id = na.symbol_id
    WHERE s.symbol = 'GYRO' AND na.source_name = 'edgar_8k'
    ORDER BY na.published_at DESC
    LIMIT 10
""")
rows = cur.fetchall()
print(f"\nGYRO EDGAR articles: {len(rows)}")
for title, pub, src in rows:
    print(f"  {pub.date() if pub else 'N/A'} | {title[:80]}")

# Check false articles - why is GNW scraping random articles for GYRO?
cur.execute("""
    SELECT na.title, na.url, rf.feed_url
    FROM news_articles na
    JOIN symbols s ON s.id = na.symbol_id
    JOIN rss_feeds rf ON rf.id = na.feed_id
    WHERE s.symbol = 'GYRO' AND na.source_name = 'globenewswire_scrape'
    LIMIT 3
""")
print("\nFalse GNW articles for GYRO (feed URL causing them):")
for title, url, feed_url in cur.fetchall():
    print(f"  Feed: {feed_url}")
    print(f"  Title: {title[:60]}")
    print(f"  Art URL: {url[:80]}")

conn.close()
