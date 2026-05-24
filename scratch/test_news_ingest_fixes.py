import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import psycopg2, psycopg2.extras, feedparser, requests, time
from curl_cffi import requests as cffi_requests

# Test 1: verify _get_active_feeds now excludes html/api feeds
from db.connection import get_connection
conn = get_connection()
with conn.cursor() as cur:
    cur.execute("""
        SELECT feed_type, COUNT(*) FROM rss_feeds rf
        JOIN symbols s ON s.id = rf.symbol_id
        WHERE rf.is_active = TRUE AND s.status = TRUE AND s.exchange = 'NASDAQ'
        AND rf.feed_type IN ('rss','atom','unknown')
        GROUP BY feed_type
    """)
    rows = cur.fetchall()
    print("=== feeds that news_ingest will now process ===")
    total = 0
    for r in rows:
        print(f"  {r[0]:10}: {r[1]}")
        total += r[1]
    print(f"  TOTAL    : {total}  (was 3890 before fix)")
conn.close()

# Test 2: ir.abivax.com — 403 + curl_cffi retry
print("\n=== ir.abivax.com 403 + curl_cffi retry ===")
url = 'https://ir.abivax.com/rss.xml'
from pipeline.news_ingest import _random_headers
r = requests.get(url, headers=_random_headers(), timeout=15)
print(f"  requests → HTTP {r.status_code}")
if r.status_code == 403:
    r2 = cffi_requests.get(url, impersonate='chrome124', timeout=15)
    f = feedparser.parse(r2.content)
    print(f"  curl_cffi retry → HTTP {r2.status_code} | entries: {len(f.entries)}")
    if f.entries:
        print(f"  First: {f.entries[0].get('title','?')[:80]}")

# Test 3: run news_ingest with limit=30
print("\n=== running news_ingest limit=30 ===")
from pipeline.news_ingest import run
result = run(exchange='NASDAQ', limit=30, scrape_full_text=False)
print(f"  result: {result}")
