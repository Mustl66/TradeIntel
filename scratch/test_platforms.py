import sys
sys.path.insert(0, r'C:\Users\Mustafa\PycharmProjects\TradeIntel')
import requests, json, re
from bs4 import BeautifulSoup

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
}

# --- TEST 1: Nasdaq API ---
print("=== NASDAQ API ===")
try:
    r = requests.get(
        "https://api.nasdaq.com/api/company/AAPL/pressreleases",
        headers={**headers, "Referer": "https://www.nasdaq.com/"},
        timeout=15
    )
    print("Status:", r.status_code)
    data = r.json()
    rows = (data.get("data") or {}).get("rows") or []
    print("Rows returned:", len(rows))
    if rows:
        print("Sample row:", rows[0])
except Exception as e:
    print("ERROR:", e)

# --- TEST 2: PRNewswire listing ---
print("\n=== PRNEWSWIRE ===")
try:
    r = requests.get(
        "https://www.prnewswire.com/news/above-food-ingredients-inc./",
        headers=headers, timeout=15
    )
    print("Status:", r.status_code)
    soup = BeautifulSoup(r.content, "lxml")
    link_re = re.compile(r"/news-releases/\d{4}/\d{2}/\d{2}/[^\"' ]+\.html")
    links = [a["href"] for a in soup.find_all("a", href=True) if link_re.search(a["href"])]
    print("Article links found:", len(links))
    if links:
        print("Sample:", links[0])
    # also check for any article cards
    cards = soup.find_all("div", class_=re.compile("card|release|article", re.I))
    print("Cards found:", len(cards))
except Exception as e:
    print("ERROR:", e)

# --- TEST 3: PRNewswire RSS endpoint ---
print("\n=== PRNEWSWIRE RSS ===")
try:
    import feedparser
    rss_url = "https://www.prnewswire.com/rss/news-releases-list.rss?name=above-food-ingredients-inc."
    r = requests.get(rss_url, headers=headers, timeout=15)
    print("Status:", r.status_code)
    parsed = feedparser.parse(r.content)
    print("Entries:", len(parsed.entries))
    if parsed.entries:
        print("Sample:", parsed.entries[0].get("title"))
except Exception as e:
    print("ERROR:", e)

# --- TEST 4: GlobeNewswire HTML feed ---
print("\n=== GLOBENEWSWIRE HTML ===")
try:
    # These are html-tagged gnw feeds - what do they look like?
    from config import DB_CONFIG
    import psycopg2
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT feed_url FROM rss_feeds
        WHERE feed_type = 'html' AND source = 'globenewswire'
        LIMIT 5
    """)
    for row in cur.fetchall():
        print("GNW HTML feed:", row[0])
    conn.close()
except Exception as e:
    print("ERROR:", e)

# --- TEST 5: Yahoo Finance - confirm truly blocked ---
print("\n=== YAHOO FINANCE ===")
try:
    r = requests.get(
        "https://finance.yahoo.com/quote/AAPL/press-releases/",
        headers=headers, timeout=15
    )
    print("Status:", r.status_code)
    soup = BeautifulSoup(r.content, "lxml")
    print("Page title:", soup.title.text if soup.title else "none")
    print("Content length:", len(r.content))
    # Check for any article links
    links = [a.get("href","") for a in soup.find_all("a", href=True) if "press" in a.get("href","").lower()]
    print("Press links:", links[:3])
except Exception as e:
    print("ERROR:", e)
