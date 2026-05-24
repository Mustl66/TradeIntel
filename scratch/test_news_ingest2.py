import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# Test 1: ir.abivax direct curl_cffi (bypass requests entirely)
from curl_cffi import requests as cffi_requests
import feedparser

print("=== ir.abivax.com direct curl_cffi ===")
r = cffi_requests.get('https://ir.abivax.com/rss.xml', impersonate='chrome124', timeout=20)
print(f"  HTTP: {r.status_code} | size: {len(r.content)}")
f = feedparser.parse(r.content)
print(f"  entries: {len(f.entries)}")
if f.entries:
    print(f"  First: {f.entries[0].get('title','?')[:80]}")

# Test 2: news_ingest run limited
print("\n=== news_ingest limit=50 ===")
from pipeline.news_ingest import run
result = run(exchange='NASDAQ', limit=50, scrape_full_text=False)
print(f"  {result}")
