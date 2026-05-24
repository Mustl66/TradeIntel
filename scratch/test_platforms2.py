import sys
sys.path.insert(0, r'C:\Users\Mustafa\PycharmProjects\TradeIntel')
import requests, json, re
from bs4 import BeautifulSoup
import feedparser

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
}

# --- PRNewswire: inspect actual HTML structure ---
print("=== PRNEWSWIRE HTML STRUCTURE ===")
r = requests.get("https://www.prnewswire.com/news/above-food-ingredients-inc./", headers=headers, timeout=15)
soup = BeautifulSoup(r.content, "lxml")
# Print all hrefs containing "news-release" or the company name
all_links = [(a.get_text(strip=True)[:50], a["href"][:100]) for a in soup.find_all("a", href=True)
             if any(x in a["href"].lower() for x in ["news-release", "above-food", "press"])]
print("Relevant links found:", len(all_links))
for t, h in all_links[:10]:
    print(f"  [{t}] {h}")

# Check card structure
cards = soup.find_all("div", class_=re.compile("card|release|article|news-item", re.I))
print("\nCards:", len(cards))
if cards:
    print("First card HTML:", str(cards[0])[:300])

# --- Nasdaq: find correct endpoint ---
print("\n=== NASDAQ CORRECT ENDPOINT ===")
# Try different endpoints
endpoints = [
    "https://api.nasdaq.com/api/company/AAPL/press-releases?limit=10",
    "https://www.nasdaq.com/api/v1/company/AAPL/press-releases",
    "https://api.nasdaq.com/api/quote/AAPL/press-releases?limit=10&offset=0",
]
for ep in endpoints:
    try:
        r2 = requests.get(ep, headers={**headers, "Referer": "https://www.nasdaq.com/"}, timeout=10)
        print(f"  {ep[:70]} → {r2.status_code}")
        if r2.status_code == 200:
            try:
                d = r2.json()
                print("  Keys:", list(d.keys())[:5])
            except:
                print("  Non-JSON response")
    except Exception as e:
        print(f"  {ep[:70]} → ERROR: {e}")

# --- GlobeNewswire search page ---
print("\n=== GLOBENEWSWIRE SEARCH PAGE ===")
gnw_url = "https://www.globenewswire.com/en/search/organization/Alliance%20Entertainment"
r3 = requests.get(gnw_url, headers=headers, timeout=15)
print("Status:", r3.status_code)
soup3 = BeautifulSoup(r3.content, "lxml")
# Look for article links
links3 = [(a.get_text(strip=True)[:50], a["href"][:100]) for a in soup3.find_all("a", href=True)
          if "news-release" in a["href"].lower() or "article" in a["href"].lower()]
print("Article links:", len(links3))
for t, h in links3[:5]:
    print(f"  [{t}] {h}")
# Check for JSON-LD
scripts = soup3.find_all("script", type="application/ld+json")
print("JSON-LD blocks:", len(scripts))
if scripts:
    try:
        data = json.loads(scripts[0].string)
        print("JSON-LD type:", data.get("@type"))
    except: pass

# Check GNW RSS - the proper feed URL pattern
print("\nGNW RSS probe:")
gnw_rss = "https://www.globenewswire.com/RssFeed/company/Alliance-Entertainment"
r4 = requests.get(gnw_rss, headers=headers, timeout=10)
print("Status:", r4.status_code, "Content-Type:", r4.headers.get("content-type",""))
parsed = feedparser.parse(r4.content)
print("Entries:", len(parsed.entries))
