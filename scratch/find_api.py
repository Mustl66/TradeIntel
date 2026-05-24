"""
Download the main JS bundle and search for the press-releases API endpoint.
"""
import requests, re
from curl_cffi import requests as cffi_req

# Get the page, find JS bundle URLs
r = cffi_req.get(
    'https://www.nasdaq.com/market-activity/stocks/bnbx/press-releases',
    impersonate='chrome124', timeout=15
)

# Find JS bundle URLs from page
js_urls = re.findall(r'src=["\'](https?://[^"\']+\.js[^"\']*)["\']', r.text)
js_urls += re.findall(r'src=["\']([^"\']+\.js[^"\']*)["\']', r.text)

print(f"JS URLs found: {len(js_urls)}")
for u in js_urls:
    print(" ", u[:120])

# Also search page HTML for any endpoint pattern
endpoints = re.findall(r'["\']([^"\']*(?:pressrelease|press-release|press_release|article-list)[^"\']*)["\']', r.text, re.IGNORECASE)
print(f"\nPress release endpoints in HTML: {len(endpoints)}")
for e in endpoints[:20]:
    print(" ", e[:150])

# look for /api/ paths
api_paths = re.findall(r'["\'](/api/[^"\'\\s<>]+)["\']', r.text)
print(f"\n/api/ paths in HTML:")
for a in set(api_paths):
    print(" ", a)
