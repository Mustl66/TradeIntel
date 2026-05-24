import sys
sys.path.insert(0, ".")
from db.connection import get_connection

conn = get_connection()
with conn.cursor() as cur:
    cur.execute("ALTER TABLE rss_feeds DROP CONSTRAINT IF EXISTS rss_feeds_feed_type_check")
    cur.execute("ALTER TABLE rss_feeds ADD CONSTRAINT rss_feeds_feed_type_check CHECK (feed_type IN ('rss','atom','html','unknown'))")
conn.commit()
conn.close()
print("Migration done: feed_type CHECK now includes 'html'")
