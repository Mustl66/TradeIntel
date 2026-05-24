import requests, re, json

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
}

r = requests.get('https://www.nasdaq.com/market-activity/stocks/bnbx/press-releases', headers=headers, timeout=15)
text = r.text

# find all script tags with src
import re
scripts = re.findall(r'<script[^>]*src=["\']([^"\']+)["\'][^>]*>', text)
print("External scripts:")
for s in scripts[:20]:
    print(" ", s)

# find inline JS blocks that mention article or press
blocks = re.findall(r'<script[^>]*>(.*?)</script>', text, re.DOTALL)
print(f"\nTotal inline script blocks: {len(blocks)}")
for i, b in enumerate(blocks):
    if any(k in b.lower() for k in ['press', 'article', 'endpoint', 'url', 'fetch', 'xhr', 'ajax']):
        print(f"\n--- block {i} (len={len(b)}) ---")
        print(b[:2000])
