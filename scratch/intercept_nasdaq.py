"""
Intercept network requests on Nasdaq press-releases page to find the real data API.
"""
import json, re
from playwright.sync_api import sync_playwright

intercepted = []

def handle_request(request):
    url = request.url
    if any(k in url for k in ['press', 'article', 'news', 'api', 'json', 'feed']):
        intercepted.append(url)

def handle_response(response):
    url = response.url
    ct = response.headers.get('content-type', '')
    if 'json' in ct and any(k in url for k in ['press', 'article', 'news', 'api']):
        try:
            body = response.json()
            print(f"\n=== JSON RESPONSE ===\nURL: {url}\n{json.dumps(body, indent=2)[:2000]}")
        except:
            pass

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    
    page.on('request', handle_request)
    page.on('response', handle_response)
    
    print("Loading page...")
    page.goto('https://www.nasdaq.com/market-activity/stocks/bnbx/press-releases', wait_until='networkidle', timeout=30000)
    
    # wait extra for lazy-loaded content
    page.wait_for_timeout(3000)
    
    print("\n=== ALL INTERCEPTED URLs ===")
    for u in intercepted:
        print(" ", u)
    
    # also try to get the rendered HTML
    html = page.content()
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'lxml')
    ul = soup.select_one('div.jupiter22-c-article-list ul')
    if ul:
        items = ul.find_all('li')
        print(f"\n=== RENDERED ARTICLES: {len(items)} ===")
        for li in items[:5]:
            a = li.select_one('a.jupiter22-c-article-list__item_title_wrapper')
            span = li.select_one('.jupiter22-c-article-list__item_title')
            date = li.select_one('.jupiter22-c-article-list__item_timeline')
            if span:
                print(f"  TITLE: {span.get_text(strip=True)}")
                print(f"  HREF:  {a.get('href','') if a else ''}")
                print(f"  DATE:  {date.get_text(strip=True) if date else ''}")
    
    browser.close()
