"""
Try Playwright with HTTP/1.1 and stealth args.
"""
import json
from playwright.sync_api import sync_playwright

intercepted_urls = []
json_responses = []

with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=True,
        args=[
            '--disable-http2',
            '--no-sandbox',
            '--disable-blink-features=AutomationControlled',
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        ]
    )
    
    context = browser.new_context(
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        locale='en-US',
        timezone_id='America/New_York',
        viewport={'width': 1920, 'height': 1080},
        extra_http_headers={
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        }
    )
    
    page = context.new_page()
    
    def on_response(response):
        url = response.url
        ct = response.headers.get('content-type', '')
        if 'json' in ct:
            intercepted_urls.append(url)
            try:
                body = response.body()
                txt = body.decode('utf-8', errors='replace')
                if any(k in txt.lower() for k in ['press', 'article', 'title', 'release']):
                    json_responses.append((url, txt[:2000]))
            except:
                pass
    
    page.on('response', on_response)
    
    print("Navigating (domcontentloaded)...")
    try:
        page.goto('https://www.nasdaq.com/market-activity/stocks/bnbx/press-releases',
                  wait_until='domcontentloaded', timeout=20000)
        print("DOM loaded, waiting for network...")
        page.wait_for_timeout(5000)
    except Exception as e:
        print(f"Navigation error: {e}")
    
    print(f"\nJSON responses intercepted: {len(json_responses)}")
    for url, body in json_responses[:10]:
        print(f"\nURL: {url}")
        print(f"BODY: {body[:500]}")
    
    print(f"\nAll JSON URLs: {len(intercepted_urls)}")
    for u in intercepted_urls[:20]:
        print(" ", u)
    
    browser.close()
