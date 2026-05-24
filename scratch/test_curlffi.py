"""
curl-cffi mimics real browser TLS fingerprint — bypasses Cloudflare/H2 blocks.
If not installed, fall back to finding the API endpoint in JS bundles.
"""
import requests, re, json

# First: try curl_cffi which spoofs Chrome TLS fingerprint properly
try:
    from curl_cffi import requests as cffi_req
    r = cffi_req.get(
        'https://www.nasdaq.com/market-activity/stocks/bnbx/press-releases',
        impersonate='chrome124',
        timeout=15
    )
    print(f"curl_cffi STATUS: {r.status_code}")
    if r.status_code == 200:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, 'lxml')
        ul = soup.select_one('div.jupiter22-c-article-list ul')
        if ul:
            items = ul.find_all('li')
            print(f"Articles found: {len(items)}")
            for li in items[:3]:
                span = li.select_one('.jupiter22-c-article-list__item_title')
                date = li.select_one('.jupiter22-c-article-list__item_timeline')
                a = li.select_one('a.jupiter22-c-article-list__item_title_wrapper')
                print(f"  TITLE: {span.get_text(strip=True) if span else ''}")
                print(f"  DATE:  {date.get_text(strip=True) if date else ''}")
                print(f"  HREF:  {a.get('href','') if a else ''}")
        else:
            print("Article list empty — still JS shell")
            # look for API URLs in the rendered page
            apis = re.findall(r'https?://[^\s"\'<>]+(?:press|article|news|release)[^\s"\'<>]*', r.text)
            print("API-like URLs in page:", apis[:10])
except ImportError:
    print("curl_cffi not installed")
except Exception as e:
    print(f"curl_cffi error: {e}")
