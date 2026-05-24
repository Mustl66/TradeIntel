import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import feedparser, requests

url = 'https://www.globenewswire.com/atomfeed/organization/3xXRSRenvuBUxdKVumy31w=='

# Test 1: bot UA (current news_ingest UA)
bot_ua = {
    'User-Agent': 'Mozilla/5.0 (compatible; TradeIntel/2.0; +https://github.com/tradeintel)'
}
r1 = requests.get(url, headers=bot_ua, timeout=15)
f1 = feedparser.parse(r1.content)
print(f"BOT UA    → HTTP {r1.status_code} | entries: {len(f1.entries)} | bozo: {f1.bozo}")

# Test 2: full browser UA
browser_ua = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
}
r2 = requests.get(url, headers=browser_ua, timeout=15)
f2 = feedparser.parse(r2.content)
print(f"BROWSER UA → HTTP {r2.status_code} | entries: {len(f2.entries)} | bozo: {f2.bozo}")

# Test 3: ir.abivax.com with curl_cffi
try:
    from curl_cffi import requests as cffi_requests
    r3 = cffi_requests.get('https://ir.abivax.com/rss.xml', impersonate='chrome124', timeout=15)
    f3 = feedparser.parse(r3.content)
    print(f"CFFI ABIVAX → HTTP {r3.status_code} | entries: {len(f3.entries)} | bozo: {f3.bozo}")
    if f3.entries:
        print(f"  First: {f3.entries[0].get('title','?')}")
except Exception as e:
    print(f"CFFI ABIVAX → ERROR: {e}")
