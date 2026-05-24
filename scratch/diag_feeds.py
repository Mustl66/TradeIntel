import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import feedparser
import requests

urls = [
    'https://www.globenewswire.com/atomfeed/organization/3xXRSRenvuBUxdKVumy31w==',
    'https://www.globenewswire.com/atomfeed/organization/BqyXItfI5PuQ1AB4QOBBaQ==',
    'https://ir.abivax.com/rss.xml',
    'https://www.globenewswire.com/atomfeed/organization/i6XHctpC3BUnkUkq2SCDIg==',
]

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/rss+xml, application/atom+xml, text/xml, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
}

for url in urls:
    print(f"\n--- {url} ---")
    try:
        r = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        ct = r.headers.get('Content-Type', '?')
        print(f"  HTTP: {r.status_code}  CT: {ct}  Size: {len(r.content)}")
        print(f"  First 300 chars: {r.text[:300]}")

        feed = feedparser.parse(r.content)
        print(f"  feedparser entries: {len(feed.entries)}")
        print(f"  feedparser bozo: {feed.bozo}")
        if feed.bozo:
            print(f"  bozo_exception: {feed.bozo_exception}")
        if feed.entries:
            e = feed.entries[0]
            print(f"  First entry title: {e.get('title','?')}")
            print(f"  First entry link:  {e.get('link','?')}")
    except Exception as ex:
        print(f"  EXCEPTION: {ex}")
