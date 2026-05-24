"""
Look more carefully at the JS bundle — find the full API base URL + path for press_release.
"""
import re
from curl_cffi import requests as cffi_req

bundle2 = 'https://www.nasdaq.com/sites/acquia.prod/files/js/js_kYGnDqIe6PFarrK9-cJMNBeLvam7Siin2gjB3cTxxeo.js?scope=footer&delta=2&language=en&theme=nsdq'
r = cffi_req.get(bundle2, impersonate='chrome124', timeout=30)
js = r.text

# Extract the full context around 'press_release'
hits = [(m.start(), m.end()) for m in re.finditer(r'press_release', js, re.IGNORECASE)]
print(f"Found {len(hits)} occurrences of press_release\n")

for start, end in hits:
    ctx_start = max(0, start - 300)
    ctx_end = min(len(js), end + 300)
    print("=" * 60)
    print(js[ctx_start:ctx_end])
    print()
