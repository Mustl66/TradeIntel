import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import asyncio
from playwright.async_api import async_playwright

async def main():
    api_calls = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        )
        page = await ctx.new_page()

        # Intercept all requests
        async def log_request(req):
            url = req.url
            if any(x in url for x in ['api', 'feed', 'news', 'press', 'json', 'release', 'ajax', 'GetNews']):
                api_calls.append({'url': url, 'method': req.method})

        page.on('request', log_request)

        await page.goto('https://investors.airbnb.com/press-releases/default.aspx', wait_until='networkidle', timeout=30000)

        # Wait for articles
        try:
            await page.wait_for_selector('a.evergreen-item-title', timeout=10000)
        except:
            print('Selector not found in time')

        # Get articles from DOM
        articles = await page.eval_on_selector_all(
            'a.evergreen-item-title',
            'els => els.map(el => {const p = el.closest("[class*=evergreen-item]"); const d = p ? p.querySelector("[class*=news-date],[class*=item-date]") : null; return {href: el.href, title: el.textContent.trim(), date: d ? d.textContent.trim() : ""}})'
        )
        print(f'Articles found: {len(articles)}')
        for a in articles[:5]:
            print(f'  {a["date"]} | {a["title"]} | {a["href"]}')

        # Year options
        years = await page.eval_on_selector_all(
            'select option',
            'els => els.map(e => e.value || e.textContent.trim())'
        )
        print('Years:', years[:10])

        print(f'\nAPI calls intercepted: {len(api_calls)}')
        for c in api_calls[:20]:
            print(f'  [{c["method"]}] {c["url"]}')

        await browser.close()

asyncio.run(main())
