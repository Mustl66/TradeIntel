import requests, json

# Try known Nasdaq API patterns for press releases
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nasdaq.com/market-activity/stocks/bnbx/press-releases',
    'Origin': 'https://www.nasdaq.com',
}

candidates = [
    'https://api.nasdaq.com/api/company/BNBX/pressreleases?limit=10&offset=0',
    'https://api.nasdaq.com/api/quote/BNBX/press-releases?limit=10&offset=0',
    'https://www.nasdaq.com/api/company/BNBX/press-releases',
    'https://api.nasdaq.com/api/news/pressrelease?symbol=BNBX&limit=10',
    'https://api.nasdaq.com/api/company/BNBX/press-releases?limit=10',
    'https://api.nasdaq.com/api/press-releases/BNBX?limit=10',
]

for url in candidates:
    try:
        r = requests.get(url, headers=headers, timeout=10)
        print(f"\n{url}")
        print(f"  STATUS: {r.status_code}  CT: {r.headers.get('content-type','')[:60]}")
        if r.status_code == 200 and 'json' in r.headers.get('content-type',''):
            data = r.json()
            print(f"  KEYS: {list(data.keys()) if isinstance(data, dict) else type(data)}")
            print(f"  PREVIEW: {str(data)[:300]}")
    except Exception as e:
        print(f"  ERROR: {e}")
