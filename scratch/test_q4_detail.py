import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup
import json, time
from urllib.parse import urlparse

# Check raw date fields in API response
r = cffi_requests.get(
    'https://investors.airbnb.com/feed/PressRelease.svc/GetPressReleaseList',
    params={
        'LanguageId': '1', 'bodyType': '0', 'pressReleaseDateFilter': '3',
        'categoryId': '00000000-0000-0000-0000-000000000000',
        'pageSize': '5', 'pageNumber': '0', 'tagList': '',
        'includeTags': 'true', 'year': '-1', 'excludeSelection': '1',
    },
    headers={'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'},
    impersonate='chrome124', timeout=15
)
data = r.json()
items = data.get('GetPressReleaseListResult', [])
print('First item keys:', list(items[0].keys()) if items else 'empty')
print()
# Show date fields
for item in items[:3]:
    print('Headline:', item.get('Headline'))
    print('DateDisplay:', repr(item.get('DateDisplay')))
    print('Date:', repr(item.get('Date')))
    print('PressReleaseDate:', repr(item.get('PressReleaseDate')))
    print('LinkToDetailPage:', item.get('LinkToDetailPage'))
    print()

# Now fetch full text from one article detail page
detail_url = 'https://investors.airbnb.com' + items[0].get('LinkToDetailPage', '')
print(f'Fetching detail: {detail_url}')
time.sleep(0.7)
r2 = cffi_requests.get(detail_url, impersonate='chrome124', timeout=15,
    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
print('Detail status:', r2.status_code)
soup = BeautifulSoup(r2.text, 'lxml')
# Q4 article body selectors
for sel in ['.evergreen-body', '.body__content', '.press-release-body',
            '.evergreen-news-body', 'article .content', '.news-body',
            '.evergreen-article-body', '.pane--content .body']:
    el = soup.select_one(sel)
    if el:
        print(f'Found via selector {sel}: {len(el.get_text())} chars')
        print(el.get_text()[:500])
        break
else:
    print('No selector matched — checking all divs with content')
    # find div with most text
    all_divs = soup.find_all('div')
    best = max(all_divs, key=lambda d: len(d.get_text(strip=True)), default=None)
    if best:
        print('Largest div class:', best.get('class'))
        print(best.get_text()[:500])
