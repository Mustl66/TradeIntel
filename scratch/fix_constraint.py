import sys; sys.path.insert(0, '.')
from db.connection import get_connection
conn = get_connection()
cur = conn.cursor()
# Add 'api' to feed_type CHECK
cur.execute("ALTER TABLE rss_feeds DROP CONSTRAINT IF EXISTS rss_feeds_feed_type_check")
cur.execute("ALTER TABLE rss_feeds ADD CONSTRAINT rss_feeds_feed_type_check CHECK (feed_type IN ('rss','atom','html','unknown','api'))")
conn.commit()
print("feed_type constraint updated OK")
conn.close()
