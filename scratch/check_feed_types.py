import sys
sys.path.insert(0, ".")
from db.connection import get_connection

conn = get_connection()
with conn.cursor() as cur:
    cur.execute("SELECT feed_type, COUNT(*) FROM rss_feeds GROUP BY feed_type ORDER BY feed_type")
    for row in cur.fetchall():
        print(row)
conn.close()
