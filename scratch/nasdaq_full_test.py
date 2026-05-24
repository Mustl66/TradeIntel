"""
Check pagination + full article body fetch for Nasdaq press releases.
"""
import json
from curl_cffi import requests as cffi_req

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://www.nasdaq.com/market-activity/stocks/bnbx/press-releases',
}

# Full data response — check totalrecords
r = cffi_req.get(
    'https://api.nasdaq.com/api/news/topic/press_release?q=symbol:bnbx|assetclass:stocks&limit=20&offset=0',
    headers=headers, impersonate='chrome124', timeout=15
)
data = r.json()
print("TOTAL RECORDS:", data['data'].get('totalrecords'))
print("ROWS COUNT:", len(data['data'].get('rows', [])))
print("ALL KEYS in row:", list(data['data']['rows'][0].keys()) if data['data'].get('rows') else [])

# Now fetch full article text for one article
sample_url = data['data']['rows'][0]['url']
full_url = f"https://www.nasdaq.com{sample_url}"
print(f"\nFetching article: {full_url}")

r2 = cffi_req.get(full_url, headers={
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,*/*',
    'Referer': 'https://www.nasdaq.com/',
}, impersonate='chrome124', timeout=15)

print(f"Article STATUS: {r2.status_code}, SIZE: {len(r2.text)}")
from bs4 import BeautifulSoup
soup = BeautifulSoup(r2.text, 'lxml')

# try different body selectors
for sel in ['div.body__content', 'article', 'div.jupiter22-c-article__body', 
            'div[class*="article__body"]', 'div[class*="press-release"]',
            'div.prcontent', 'div#pr-content']:
    el = soup.select_one(sel)
    if el:
        text = el.get_text(separator='\n', strip=True)
        print(f"\nSELECTOR '{sel}' -> {len(text)} chars")
        print(text[:300])
        break
else:
    # fallback: main content area
    main = soup.find('main')
    if main:
        text = main.get_text(separator='\n', strip=True)
        print(f"\nMAIN tag -> {len(text)} chars")
        print(text[:400])
