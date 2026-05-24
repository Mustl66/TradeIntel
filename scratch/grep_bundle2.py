"""
Search second bundle and look for the data fetch endpoint.
"""
import re
from curl_cffi import requests as cffi_req

# Try the second bundle
bundle2 = 'https://www.nasdaq.com/sites/acquia.prod/files/js/js_kYGnDqIe6PFarrK9-cJMNBeLvam7Siin2gjB3cTxxeo.js?scope=footer&delta=2&language=en&theme=nsdq'

print("Downloading bundle 2...")
r = cffi_req.get(bundle2, impersonate='chrome124', timeout=30)
print(f"STATUS: {r.status_code}, SIZE: {len(r.text)}")
js = r.text

patterns = [
    r'press.?release[^\s"\']{0,150}',
    r'/api/[^\s"\'<>,;)]{5,80}',
    r'endpoint[^;]{0,150}',
    r'fetch\([^)]{0,100}',
    r'article.list[^\s"\']{0,100}',
    r'jupiter22[^;]{0,200}',
]

for pat in patterns:
    hits = re.findall(pat, js, re.IGNORECASE)
    if hits:
        print(f"\n=== {pat} ===")
        for h in hits[:5]:
            print(" ", h[:200])

# Also dump any URL-like strings with 'api' in them
api_urls = re.findall(r'https?://[^\s"\'<>]{10,100}', js)
api_urls = [u for u in api_urls if any(k in u.lower() for k in ['api', 'press', 'article', 'news'])]
print(f"\nAPI-like full URLs: {len(api_urls)}")
for u in api_urls[:20]:
    print(" ", u)
