"""
Inspect PRNewswire company page for article links.
"""
import sys
sys.path.insert(0, ".")
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# PRNewswire company page
url = "https://www.prnewswire.com/news/above-food-ingredients-inc./"
resp = requests.get(url, headers=_HEADERS, timeout=15)
soup = BeautifulSoup(resp.content, "lxml")

print("=== All <a> with /news-releases/ in href ===")
for a in soup.find_all("a", href=True):
    h = a["href"]
    t = a.get_text(strip=True)
    # Look for actual press release article links (have date patterns or long slugs)
    if "/news-releases/" in h and len(h) > 30 and t:
        print(f"  {h[:100]}")
        print(f"  -> {t[:80]}")
        print()

# Also check if there's a JSON endpoint
print("\n=== Script tags with JSON/API ===")
for s in soup.find_all("script"):
    t = s.get_text()
    if "newsItems" in t or "articles" in t or "releases" in t.lower():
        print(t[:500])
        print("---")
