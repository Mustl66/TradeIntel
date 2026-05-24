"""
Debug: test one html feed manually through each layer.
"""
import sys
sys.path.insert(0, ".")
import random
import requests
from db.connection import get_connection

# Get first 5 html feeds
conn = get_connection()
with conn.cursor() as cur:
    cur.execute("""
        SELECT rf.id, rf.feed_url, s.symbol
        FROM rss_feeds rf
        JOIN symbols s ON s.id = rf.symbol_id
        WHERE rf.feed_type = 'html' AND rf.is_active = TRUE
        AND s.exchange = 'NASDAQ'
        ORDER BY rf.id
        LIMIT 5
    """)
    rows = cur.fetchall()
conn.close()

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

for fid, url, symbol in rows:
    print(f"\n[{symbol}] {url}")
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=15, allow_redirects=True)
        print(f"  status={resp.status_code} content-type={resp.headers.get('Content-Type','?')}")
        print(f"  content-length={len(resp.content)} bytes")

        # Try RSS autodiscovery
        from pipeline.html_ingest import _discover_rss, _extract_jsonld, _extract_trafilatura
        rss = _discover_rss(url, resp.content)
        print(f"  rss_autodiscovered={rss}")

        # JSON-LD
        arts = _extract_jsonld(resp.content, url)
        print(f"  json-ld articles={len(arts)}")
        if arts:
            print(f"    first title: {arts[0]['title'][:80]}")

        # Trafilatura
        traf = _extract_trafilatura(resp.content, url)
        if traf:
            traf_str = "yes (" + traf["title"][:60] + ")"
        else:
            traf_str = "none"
        print(f"  trafilatura={traf_str}")

    except Exception as e:
        print(f"  ERROR: {e}")
