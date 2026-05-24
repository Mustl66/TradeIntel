"""
Inspect specific HTML pages to understand their structure.
"""
import sys
sys.path.insert(0, ".")
import requests
from bs4 import BeautifulSoup

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

urls = [
    ("PRNewswire",   "https://www.prnewswire.com/news/above-food-ingredients-inc./"),
    ("Airbnb Q4IR",  "https://investors.airbnb.com/press-releases/default.aspx"),
    ("Yahoo Finance","https://finance.yahoo.com/quote/ACTG/press-releases/"),
    ("Nasdaq PR",    "https://www.nasdaq.com/market-activity/stocks/afjk/press-releases"),
]

for name, url in urls:
    print(f"\n{'='*60}")
    print(f"{name}: {url}")
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=15, allow_redirects=True)
        soup = BeautifulSoup(resp.content, "lxml")

        # Find all links with typical article patterns
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if any(p in href.lower() for p in ["/press-release", "/news/", "/newsroom/", "releases", "artikel"]):
                if len(text) > 10:
                    links.append((href[:80], text[:60]))

        print(f"  Article-like links found: {len(links)}")
        for h, t in links[:5]:
            print(f"    href={h}")
            print(f"    text={t}")

        # Check for any hidden RSS in head
        for link in soup.find_all("link", rel="alternate"):
            print(f"  ALTERNATE LINK: type={link.get('type')} href={link.get('href','')[:80]}")

        # Check for JSON APIs (script tags with fetch URLs)
        scripts = soup.find_all("script")
        api_hints = []
        for s in scripts:
            t = s.get_text()
            if "api" in t.lower() and ("json" in t.lower() or "fetch" in t.lower()):
                api_hints.append(t[:200])
        if api_hints:
            print(f"  API hints in scripts: {len(api_hints)}")

    except Exception as e:
        print(f"  ERROR: {e}")
