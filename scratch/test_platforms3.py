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

# --- PRNewswire: find the actual article links inside cards ---
print("=== PRNEWSWIRE CARD LINKS ===")
r = requests.get("https://www.prnewswire.com/news/above-food-ingredients-inc./", headers=headers, timeout=15)
soup = BeautifulSoup(r.content, "lxml")
# Find newsreleaseconsolidatelink anchors
news_links = soup.find_all("a", class_=re.compile("newsreleaseconsolidatelink", re.I))
print("newsreleaseconsolidatelink anchors:", len(news_links))
for a in news_links[:5]:
    print(f"  href={a.get('href','')[:80]}  text={a.get_text(strip=True)[:50]}")

# Also check all links inside .newsCards
news_cards = soup.find("div", class_="newsCards")
if news_cards:
    card_links = news_cards.find_all("a", href=True)
    print(f"\nLinks inside .newsCards: {len(card_links)}")
    for a in card_links[:10]:
        print(f"  href={a['href'][:80]}  text={a.get_text(strip=True)[:40]}")

# --- Nasdaq: try scraping the HTML press-releases page directly ---
print("\n=== NASDAQ HTML PAGE ===")
nasdaq_url = "https://www.nasdaq.com/market-activity/stocks/aapl/press-releases"
r2 = requests.get(nasdaq_url, headers=headers, timeout=15)
print("Status:", r2.status_code)
soup2 = BeautifulSoup(r2.content, "lxml")
print("Title:", soup2.title.text if soup2.title else "none")
# Check for __NEXT_DATA__ (Next.js page with embedded JSON)
next_data = soup2.find("script", id="__NEXT_DATA__")
if next_data:
    print("Found __NEXT_DATA__")
    try:
        data = json.loads(next_data.string)
        # drill into props
        props = data.get("props", {}).get("pageProps", {})
        print("pageProps keys:", list(props.keys())[:10])
    except Exception as e:
        print("Parse error:", e)
else:
    print("No __NEXT_DATA__")
    # Any article links?
    links = [(a.get_text(strip=True)[:50], a["href"][:80]) for a in soup2.find_all("a", href=True)
             if "press-release" in a["href"].lower() or "news" in a["href"].lower()]
    print("News links:", len(links))
    for t, h in links[:5]:
        print(f"  [{t}] {h}")

# --- GlobeNewswire: extract article links from search page ---
print("\n=== GLOBENEWSWIRE ARTICLE EXTRACTION ===")
gnw_url = "https://www.globenewswire.com/en/search/organization/Alliance%20Entertainment"
r3 = requests.get(gnw_url, headers=headers, timeout=15)
soup3 = BeautifulSoup(r3.content, "lxml")
article_links = []
for a in soup3.find_all("a", href=True):
    href = a["href"]
    if re.search(r"/news-release/\d{4}/", href):
        full_url = "https://www.globenewswire.com" + href if href.startswith("/") else href
        title = a.get_text(strip=True)
        if title and len(title) > 10:
            article_links.append((title, full_url))

print(f"Article links: {len(article_links)}")
for t, u in article_links[:3]:
    print(f"  [{t[:60]}]\n   {u}")

# also check for date info near links
print("\nChecking for date elements near article links:")
for a in soup3.find_all("a", href=True)[:50]:
    href = a["href"]
    if re.search(r"/news-release/\d{4}/", href):
        parent = a.find_parent()
        if parent:
            date_el = parent.find(class_=re.compile("date|time|published", re.I))
            if date_el:
                print(f"  date: {date_el.get_text(strip=True)}")
                break
