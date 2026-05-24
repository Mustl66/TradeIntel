import requests, re, json

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
}

r = requests.get('https://www.nasdaq.com/market-activity/stocks/bnbx/press-releases', headers=headers, timeout=15)
text = r.text

# API paths
apis = re.findall(r'["\'](/api/[^"\'\\s<>]+)["\']', text)
print("API paths:")
for a in sorted(set(apis)):
    print(" ", a)

# look for drupalSettings or window.__data
m = re.search(r'drupalSettings\s*=\s*({.+?});', text, re.DOTALL)
if m:
    print("\ndrupalSettings found, length:", len(m.group(1)))
    try:
        ds = json.loads(m.group(1))
        print(json.dumps(ds, indent=2)[:3000])
    except:
        print(m.group(1)[:2000])

# look for data-module or data-symbol attributes
syms = re.findall(r'data-symbol=["\']([^"\']+)["\']', text)
print("\ndata-symbol values:", syms[:10])

mods = re.findall(r'data-module=["\']([^"\']+)["\']', text)
print("data-module values:", list(set(mods))[:10])

# jupiter22 component configs
comps = re.findall(r'jupiter22[^"\']{0,200}', text[:80000])
print("\njupiter22 refs (first 5):")
for c in comps[:5]:
    print(" ", c[:200])
