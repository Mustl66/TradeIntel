import requests, re, sys

url = "https://www.nasdaq.com/market-activity/stocks/gyro/press-releases"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

r = requests.get(url, headers=headers, timeout=20)
html = r.text
print(f"Status: {r.status_code}, Size: {len(html)} bytes")

# Look for press release links
links = re.findall(r'href="(/press-release/[^"]+)"', html)
print(f"PR links found: {len(links)}")
for l in links[:10]:
    print(l)

# Look for Gyrodyne mentions
gyro_ctx = [(m.start(), html[max(0,m.start()-50):m.start()+150]) for m in re.finditer("Gyrodyne|GYRODYNE|Agreement|Announces", html, re.I)]
print(f"\nGyrodyne mentions: {len(gyro_ctx)}")
for pos, ctx in gyro_ctx[:5]:
    print(f"  pos={pos}: {ctx.strip()}")

# Check for __NEXT_DATA__
nd = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
print(f"\n__NEXT_DATA__ present: {nd is not None}")
if nd:
    print(nd.group(1)[:1000])

# Check for application/ld+json
ld = re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, re.DOTALL)
print(f"\nLD+JSON blocks: {len(ld)}")
for block in ld[:3]:
    print(block[:300])
