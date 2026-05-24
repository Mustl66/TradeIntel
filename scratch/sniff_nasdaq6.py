import requests, re

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
}

r = requests.get('https://www.nasdaq.com/market-activity/stocks/bnbx/press-releases', headers=headers, timeout=15)
text = r.text

# Block 15 is 18KB — dump it fully
blocks = re.findall(r'<script[^>]*>(.*?)</script>', text, re.DOTALL)
b = blocks[15]
print(b[:8000])
