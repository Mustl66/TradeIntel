import requests, re, json

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
}

r = requests.get('https://www.nasdaq.com/market-activity/stocks/bnbx/press-releases', headers=headers, timeout=15)
text = r.text

# dump ALL blocks with length > 1000 to see which ones have drupalSettings
blocks = re.findall(r'<script[^>]*>(.*?)</script>', text, re.DOTALL)
for i, b in enumerate(blocks):
    if len(b) > 1000:
        has_ds = 'drupalSettings' in b
        print(f"Block {i}: len={len(b)} drupalSettings={has_ds}")
        if has_ds:
            idx = b.find('drupalSettings')
            print("  Context:", b[idx:idx+500])
