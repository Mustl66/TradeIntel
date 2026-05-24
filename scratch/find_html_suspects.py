"""
Find company_ir feeds that look like HTML pages (no rss/atom/xml/feed in URL).
"""
import sys
sys.path.insert(0, ".")
from migrate_feed_type import _url_heuristic
from db.connection import get_connection

conn = get_connection()
with conn.cursor() as cur:
    cur.execute("""
        SELECT id, feed_url, feed_type
        FROM rss_feeds
        WHERE source = 'company_ir'
        ORDER BY id
    """)
    rows = cur.fetchall()
conn.close()

html_suspects = []
for row in rows:
    fid, url, ftype = row
    guess = _url_heuristic(url)
    if not guess:
        html_suspects.append((fid, url, ftype))

print(f"Total company_ir: {len(rows)}")
print(f"HTML suspects (no rss/atom/xml signal in URL): {len(html_suspects)}")
print("\nFirst 20 suspects:")
for fid, url, ftype in html_suspects[:20]:
    print(f"  [{fid}] {ftype} — {url}")
