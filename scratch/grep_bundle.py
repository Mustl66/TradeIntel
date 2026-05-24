"""
Download the main JS bundle and grep for the press-releases API endpoint.
"""
import re
from curl_cffi import requests as cffi_req

# The main bundle URL from earlier
bundle_url = 'https://www.nasdaq.com/sites/acquia.prod/files/js/js_ojSVofdQwGNdKaMXkow2tUFuDn5_WacPeDwvENQ19mc.js?scope=footer&delta=0&language=en&theme=nsdq'

print("Downloading main bundle...")
r = cffi_req.get(bundle_url, impersonate='chrome124', timeout=30)
print(f"STATUS: {r.status_code}, SIZE: {len(r.text)}")

js = r.text

# search for article-list endpoint
patterns = [
    r'press.release[s]?[^\s"\']{0,100}',
    r'article.list[^\s"\']{0,100}',
    r'/api/[^\s"\'<>]{3,80}',
    r'endpoint[^\s"\']{0,150}',
    r'pressrelease[^\s"\']{0,100}',
]

for pat in patterns:
    hits = re.findall(pat, js, re.IGNORECASE)
    if hits:
        print(f"\n=== {pat} ===")
        for h in hits[:10]:
            print(" ", h[:200])
