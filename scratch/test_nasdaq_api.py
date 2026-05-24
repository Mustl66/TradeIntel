import sys
sys.path.insert(0, r'C:\Users\Mustafa\PycharmProjects\TradeIntel')
import requests, json

headers_nasdaq = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nasdaq.com/",
    "Origin": "https://www.nasdaq.com",
}

# Try Nasdaq search API
endpoints = [
    "https://api.nasdaq.com/api/news/companyquery?symbol=AAPL&limit=10&offset=0",
    "https://api.nasdaq.com/api/quote/AAPL/info?assetClass=stocks",
    "https://api.nasdaq.com/api/company/AAPL/secfilings?limit=10&offset=0",
    "https://efts.nasdaq.com/LATEST/search-index?q=AAPL&dateRange=custom&startdate=2024-01-01&enddate=2025-12-31&assetClass=Press+Release&exchange=NASDAQ",
    "https://api.nasdaq.com/api/company/AAPL/pressreleases?limit=10&offset=0&type=pressRelease",
    "https://api.nasdaq.com/api/company/AAPL/news?limit=10&offset=0",
]

for ep in endpoints:
    try:
        r = requests.get(ep, headers=headers_nasdaq, timeout=10)
        print(f"[{r.status_code}] {ep[:80]}")
        if r.status_code == 200:
            try:
                d = r.json()
                print("  Keys:", list(d.keys())[:8])
                data = d.get("data") or {}
                if isinstance(data, dict):
                    print("  data keys:", list(data.keys())[:8])
                    rows = data.get("rows") or data.get("results") or []
                    print("  rows:", len(rows))
                    if rows:
                        print("  sample:", rows[0])
            except:
                print("  non-JSON")
    except Exception as e:
        print(f"  ERROR: {e}")
