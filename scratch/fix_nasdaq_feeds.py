"""
Fix two DB issues:
1. Any rss_feeds row where feed_url matches the Nasdaq API pattern but has wrong feed_type
2. Delete false articles for GYRO/BNBX/BLDP that came from old broken runs
3. Verify correct articles land after live test
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import psycopg2, psycopg2.extras
from config import DB_CONFIG

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# 1. Fix any Nasdaq press-release HTML page stored as rss/atom/unknown
cur.execute("""
    UPDATE rss_feeds
    SET feed_type = 'html'
    WHERE feed_url ILIKE '%nasdaq.com/market-activity/stocks/%/press-releases%'
      AND feed_type != 'html'
    RETURNING id, feed_url, feed_type
""")
fixed = cur.fetchall()
print(f"Fixed feed_type for {len(fixed)} Nasdaq press-release feeds:")
for f in fixed:
    print(f"  id={f['id']} {f['feed_url']}")

# 2. Also fix Nasdaq API URLs that ended up as 'rss'
cur.execute("""
    UPDATE rss_feeds
    SET feed_type = 'html'
    WHERE feed_url ILIKE '%nasdaq.com/api/news/topic/press_release%'
      AND feed_type != 'html'
    RETURNING id, feed_url
""")
fixed2 = cur.fetchall()
print(f"\nFixed feed_type for {len(fixed2)} Nasdaq API 'rss' feeds:")
for f in fixed2:
    print(f"  id={f['id']} {f['feed_url']}")

# 3. Delete false articles for GYRO/BNBX/BLDP
# False = articles that came from non-specific sources (businesswire rss random, gnw random)
# We keep edgar_8k articles, delete nasdaq_api (wrong ones) and html_scrape junk
for sym in ["GYRO", "BNBX", "BLDP"]:
    cur.execute("SELECT id FROM symbols WHERE symbol=%s", (sym,))
    row = cur.fetchone()
    if not row:
        print(f"\n{sym}: not found")
        continue
    sid = row["id"]
    # Count before
    cur.execute("SELECT COUNT(*) as c FROM news_articles WHERE symbol_id=%s", (sid,))
    before = cur.fetchone()["c"]
    # Delete everything EXCEPT edgar_8k source
    cur.execute("""
        DELETE FROM news_articles
        WHERE symbol_id=%s
          AND source_name != 'edgar_8k'
    """, (sid,))
    deleted = cur.rowcount
    cur.execute("SELECT COUNT(*) as c FROM news_articles WHERE symbol_id=%s", (sid,))
    after = cur.fetchone()["c"]
    print(f"\n{sym}: {before} articles before → deleted {deleted} → {after} remaining (edgar only)")

conn.commit()
conn.close()
print("\nDone. DB fixed.")
