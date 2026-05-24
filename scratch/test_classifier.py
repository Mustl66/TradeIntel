"""
Quick test: run feed classifier on 20 feeds and show what changes.
"""
import sys
sys.path.insert(0, ".")
from migrate_feed_type import _url_heuristic, _classify_via_http
from db.connection import get_connection

conn = get_connection()
with conn.cursor() as cur:
    # Grab 20 company_ir feeds — most likely to have HTML imposters
    cur.execute("""
        SELECT id, feed_url, feed_type, source
        FROM rss_feeds
        WHERE source = 'company_ir'
        ORDER BY id
        LIMIT 20
    """)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
conn.close()

print(f"Testing {len(rows)} company_ir feeds...\n")
for row in rows:
    url      = row["feed_url"]
    current  = row["feed_type"]
    heuristic = _url_heuristic(url)
    if not heuristic:
        http_type = _classify_via_http(url)
    else:
        http_type = heuristic

    flag = " *** MISMATCH" if http_type != current else ""
    print(f"[{row['id']}] {current} -> {http_type}{flag}")
    print(f"    {url[:90]}")
    print()
