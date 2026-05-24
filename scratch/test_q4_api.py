import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from curl_cffi import requests as cffi_requests
import json
from urllib.parse import urlparse

def test_q4_api(ir_url):
    parsed = urlparse(ir_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    api_url = f"{base}/feed/PressRelease.svc/GetPressReleaseList"
    params = {
        'LanguageId': '1', 'bodyType': '0', 'pressReleaseDateFilter': '3',
        'categoryId': '00000000-0000-0000-0000-000000000000',
        'pageSize': '-1', 'pageNumber': '0', 'tagList': '',
        'includeTags': 'true', 'year': '-1', 'excludeSelection': '1',
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json, */*',
        'Referer': ir_url,
    }
    try:
        r = cffi_requests.get(api_url, params=params, headers=headers, impersonate='chrome124', timeout=15)
        print(f'\n=== {ir_url} ===')
        print(f'Status: {r.status_code}')
        if r.status_code == 200:
            data = r.json()
            # Result is a flat list
            items = data.get('GetPressReleaseListResult', [])
            if isinstance(items, dict):
                # older Q4 format nests it
                items = items.get('PressReleases', {}).get('PressRelease', [])
            print(f'Articles: {len(items)}')
            for item in items[:4]:
                headline = item.get('Headline', '') or item.get('Title', '')
                date = item.get('DateDisplay', '')
                link = item.get('LinkToDetailPage', '')
                full_link = base + link if link.startswith('/') else link
                print(f'  {date} | {headline}')
                print(f'    -> {full_link}')
    except Exception as e:
        print(f'ERROR: {e}')

test_q4_api('https://investors.airbnb.com/press-releases/default.aspx')

# Find real default.aspx URLs from our DB
import psycopg2
conn = psycopg2.connect(dbname='tradeintel', user='postgres', password='postgres', host='localhost')
cur = conn.cursor()
cur.execute("""
    SELECT s.symbol, f.feed_url
    FROM rss_feeds f JOIN symbols s ON s.id = f.symbol_id
    WHERE f.feed_url ILIKE '%default.aspx%'
    LIMIT 5
""")
rows = cur.fetchall()
conn.close()
print(f'\nFound {len(rows)} default.aspx URLs in DB:')
for sym, url in rows:
    print(f'  {sym}: {url}')
    test_q4_api(url)
