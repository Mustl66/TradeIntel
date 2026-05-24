import sys
sys.path.insert(0, r'C:\Users\Mustafa\PycharmProjects\TradeIntel')
from config import DB_CONFIG
import psycopg2

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# Yahoo feeds
cur.execute("""
SELECT rf.feed_url, COUNT(na.id) as articles
FROM rss_feeds rf
LEFT JOIN news_articles na ON na.feed_id = rf.id
WHERE rf.feed_url ILIKE '%yahoo%'
GROUP BY rf.feed_url
LIMIT 10
""")
print('--- Yahoo feeds ---')
for row in cur.fetchall():
    print(row)

# Nasdaq feeds
cur.execute("""
SELECT rf.feed_url, COUNT(na.id) as articles
FROM rss_feeds rf
LEFT JOIN news_articles na ON na.feed_id = rf.id
WHERE rf.feed_url ILIKE '%nasdaq%'
GROUP BY rf.feed_url
LIMIT 10
""")
print('\n--- Nasdaq feeds ---')
for row in cur.fetchall():
    print(row)

# PRNewswire feeds
cur.execute("""
SELECT rf.feed_url, COUNT(na.id) as articles
FROM rss_feeds rf
LEFT JOIN news_articles na ON na.feed_id = rf.id
WHERE rf.feed_url ILIKE '%prnewswire%'
GROUP BY rf.feed_url
LIMIT 10
""")
print('\n--- PRNewswire feeds ---')
for row in cur.fetchall():
    print(row)

# HTML feeds with 0 articles
cur.execute("""
SELECT COUNT(*) FROM rss_feeds rf
WHERE rf.feed_type = 'html'
AND NOT EXISTS (SELECT 1 FROM news_articles na WHERE na.feed_id = rf.id)
""")
print('\nHTML feeds with 0 articles:', cur.fetchone()[0])

# Domains of zero-article html feeds
cur.execute("""
SELECT SUBSTRING(rf.feed_url FROM 'https?://([^/]+)') as domain, COUNT(*)
FROM rss_feeds rf
WHERE rf.feed_type = 'html'
AND NOT EXISTS (SELECT 1 FROM news_articles na WHERE na.feed_id = rf.id)
GROUP BY domain
ORDER BY COUNT(*) DESC
LIMIT 20
""")
print('\n--- Zero-article HTML feed domains ---')
for row in cur.fetchall():
    print(row)

conn.close()
