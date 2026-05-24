"""
Hit the discovered Nasdaq API endpoint for press releases.
Pattern from JS bundle: press_release?q=symbol:${e}|assetclass:${s}&limit=${a}&offset=${r}
Base: https://api.nasdaq.com/api
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

# Test with BNBX, assetclass=stocks
candidates = [
    'https://api.nasdaq.com/api/press_release?q=symbol:BNBX|assetclass:stocks&limit=10&offset=0',
    'https://api.nasdaq.com/api/press-releases?q=symbol:BNBX|assetclass:stocks&limit=10&offset=0',
    'https://www.nasdaq.com/api/press_release?q=symbol:BNBX|assetclass:stocks&limit=10&offset=0',
    'https://qcapi.nasdaq.com/api/press_release?q=symbol:BNBX|assetclass:stocks&limit=10&offset=0',
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
                print(f"  PREVIEW: {str(data)[:500]}")
            except:
                print(f"  TEXT: {r.text[:300]}")
    except Exception as e:
        print(f"  ERROR: {e}")
