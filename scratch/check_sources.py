import sys
sys.path.insert(0, "C:/Users/Mustafa/PycharmProjects/TradeIntel")
from admin import get_conn
import psycopg2.extras

conn = get_conn()
with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    cur.execute("""
        SELECT na.source_name, rf.source, rf.feed_type, COUNT(*) as cnt
        FROM news_articles na
        LEFT JOIN rss_feeds rf ON rf.id = na.feed_id
        GROUP BY na.source_name, rf.source, rf.feed_type
        ORDER BY cnt DESC LIMIT 20
    """)
    for row in cur.fetchall():
        print(dict(row))
conn.close()
