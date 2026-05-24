import requests, re, json

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
}

r = requests.get('https://www.nasdaq.com/market-activity/stocks/bnbx/press-releases', headers=headers, timeout=15)
text = r.text

# Find the block with drupalSettings
blocks = re.findall(r'<script[^>]*>(.*?)</script>', text, re.DOTALL)
for i, b in enumerate(blocks):
    if 'drupalSettings' in b:
        print(f"Block {i} has drupalSettings, len={len(b)}")
        # extract just the relevant part
        # look for jupiter22, article-list, endpoint, component config
        for pattern in ['jupiter22', 'article', 'endpoint', 'press', 'component', 'nsdq_data']:
            hits = re.findall(rf'"{pattern}[^"]*"\s*:[^\n]{{0,200}}', b, re.IGNORECASE)
            if hits:
                print(f"\n  === {pattern} ===")
                for h in hits[:5]:
                    print("  ", h[:200])
