import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import psycopg2, psycopg2.extras
from config import DB_CONFIG

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

for sym in ["GYRO", "BNBX", "BLDP"]:
    cur.execute("SELECT id FROM symbols WHERE symbol=%s", (sym,))
    row = cur.fetchone()
    if not row:
        print(f"{sym}: NOT IN SYMBOLS TABLE")
        continue
    sid = row["id"]
    cur.execute("SELECT id, feed_url, feed_type, source FROM rss_feeds WHERE symbol_id=%s", (sid,))
    feeds = cur.fetchall()
    print(f"\n{sym} (id={sid}) feeds:")
    for f in feeds:
        print(f"  feed_id={f['id']} type={f['feed_type']} source={f['source']}")
        print(f"  url={f['feed_url']}")
    cur.execute("SELECT COUNT(*) as c FROM news_articles WHERE symbol_id=%s", (sid,))
    print(f"  articles in db: {cur.fetchone()['c']}")
    cur.execute("SELECT title, published_at FROM news_articles WHERE symbol_id=%s ORDER BY published_at DESC LIMIT 3", (sid,))
    for a in cur.fetchall():
        print(f"  >> {a['published_at']} | {a['title'][:80]}")

conn.close()
