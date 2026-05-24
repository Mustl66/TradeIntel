import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup
import re

r = cffi_requests.get(
    'https://investors.airbnb.com/press-releases/default.aspx',
    impersonate='chrome124',
    timeout=15
)
print('Status:', r.status_code, 'Size:', len(r.text))

soup = BeautifulSoup(r.text, 'lxml')

# Year options
years = [o.get('value', o.text.strip()) for o in soup.select('select option')]
print('Years:', years[:10])

# Articles
articles = soup.select('a.evergreen-item-title')
print('Articles:', len(articles))
for a in articles[:4]:
    parent = a.find_parent(class_=lambda c: c and 'evergreen-item' in c)
    date_el = parent.select_one('.evergreen-news-date, .evergreen-item-date-time') if parent else None
    print(' -', date_el.get_text(strip=True) if date_el else 'nodate', '|', a.get_text(strip=True), '|', a.get('href',''))

# Check for Q4 API config in page
api_key = re.search(r'apiKey["\s:=]+([a-z0-9\-]+)', r.text)
print('API key found:', api_key.group(0) if api_key else 'none')

# Check data-feed-url attributes
for tag in soup.find_all(attrs={'data-feed-url': True})[:3]:
    print('data-feed-url:', tag.get('data-feed-url'))

# Look for JSON endpoint hints
for pattern in ['GetNewsList', 'api/news', 'feed/News', 'NewsRelease', 'press-release/list']:
    if pattern.lower() in r.text.lower():
        idx = r.text.lower().index(pattern.lower())
        print(f'Pattern "{pattern}" at:', r.text[max(0,idx-100):idx+200])
