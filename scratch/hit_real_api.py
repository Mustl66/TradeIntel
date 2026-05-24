"""
Hit the real Nasdaq API endpoint discovered from JS bundle.
URL: https://www.nasdaq.com/api/news/topic/press_release?q=symbol:bnbx|assetclass:stocks&limit=10&offset=0
"""
import json
from curl_cffi import requests as cffi_req

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nasdaq.com/market-activity/stocks/bnbx/press-releases',
    'Origin': 'https://www.nasdaq.com',
}

# Try both bnbx (lower) and BNBX (upper), both base URLs
candidates = [
    'https://www.nasdaq.com/api/news/topic/press_release?q=symbol:bnbx|assetclass:stocks&limit=10&offset=0',
    'https://www.nasdaq.com/api/news/topic/press_release?q=symbol:BNBX|assetclass:stocks&limit=10&offset=0',
    'https://api.nasdaq.com/api/news/topic/press_release?q=symbol:bnbx|assetclass:stocks&limit=10&offset=0',
    # also try GYRO
    'https://www.nasdaq.com/api/news/topic/press_release?q=symbol:gyro|assetclass:stocks&limit=10&offset=0',
]

for url in candidates:
    try:
        r = cffi_req.get(url, headers=headers, impersonate='chrome124', timeout=15)
        print(f"\n{url}")
        print(f"  STATUS: {r.status_code}  CT: {r.headers.get('content-type','')[:60]}")
        if r.status_code == 200:
            try:
                data = r.json()
                print(f"  KEYS: {list(data.keys()) if isinstance(data, dict) else type(data)}")
                print(f"  PREVIEW: {json.dumps(data, indent=2)[:800]}")
            except:
                print(f"  TEXT: {r.text[:400]}")
    except Exception as e:
        print(f"  ERROR: {e}")
