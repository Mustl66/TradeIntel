"""Live end-to-end test: GYRO, BNBX, BLDP through html_ingest pipeline"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from db.connection import get_connection
from pipeline.html_ingest import _process_html_feed, _insert_articles, _known_hashes
import psycopg2.extras

conn = get_connection()
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

for sym in ["GYRO", "BNBX", "BLDP"]:
    cur.execute("SELECT id FROM symbols WHERE symbol=%s", (sym,))
    row = cur.fetchone()
    if not row:
        print(f"{sym}: not in DB"); continue
    sid = row["id"]

    cur.execute("""
        SELECT rf.id, rf.feed_url, rf.feed_type, rf.source, s.symbol
        FROM rss_feeds rf JOIN symbols s ON s.id=rf.symbol_id
        WHERE rf.symbol_id=%s AND rf.feed_type='html' AND rf.is_active=TRUE
    """, (sid,))
    feeds = cur.fetchall()

    if not feeds:
        print(f"\n{sym}: NO html-type feeds found")
        continue

    for feed in feeds:
        print(f"\n{sym} — processing: {feed['feed_url']}")
        feed_row = {"id": feed["id"], "symbol_id": sid, "symbol": sym, "feed_url": feed["feed_url"]}
        result = _process_html_feed(feed_row)
        arts = result.get("articles", [])
        method = result.get("method", "none")
        print(f"  method={method}  raw_articles={len(arts)}")

        if arts:
            existing = _known_hashes(conn, [a["article_hash"] for a in arts])
            new_arts = [a for a in arts if a["article_hash"] not in existing]
            inserted = _insert_articles(conn, new_arts)
            print(f"  inserted={inserted}  skipped(dup)={len(arts)-len(new_arts)}")
            for a in arts[:3]:
                print(f"  >> {a.get('published_at','?')} | {a['title'][:80]}")
        else:
            print(f"  ERROR or 0 articles — rss_found={result.get('rss_found')}")

    # Final count
    cur.execute("SELECT COUNT(*) as c FROM news_articles WHERE symbol_id=%s", (sid,))
    print(f"  TOTAL in DB now: {cur.fetchone()['c']}")

conn.close()
