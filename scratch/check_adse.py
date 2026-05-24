import sys
sys.path.insert(0, r'C:\Users\Mustafa\PycharmProjects\TradeIntel')
from db.connection import get_connection as get_conn
import psycopg2.extras

conn = get_conn()
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

cur.execute("SELECT COUNT(*) as c FROM rss_feeds WHERE feed_url ILIKE '%nasdaq.com/market-activity%' AND feed_type='rss'")
print('Nasdaq URLs stored as rss type:', cur.fetchone()['c'])

cur.execute("SELECT COUNT(*) as c FROM rss_feeds WHERE feed_url ILIKE '%yahoo.com/quote%' AND feed_type='rss'")
print('Yahoo press-release URLs as rss type:', cur.fetchone()['c'])

cur.execute("SELECT COUNT(*) as c FROM rss_feeds WHERE feed_url ILIKE '%mynewsdesk%'")
print('mynewsdesk feeds total:', cur.fetchone()['c'])

cur.execute("SELECT feed_url, feed_type FROM rss_feeds WHERE feed_url ILIKE '%mynewsdesk%' LIMIT 5")
for row in cur.fetchall():
    print(f"  {row['feed_type']}  {row['feed_url']}")

# check mynewsdesk page structure
import requests
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36'}
resp = requests.get('https://www.mynewsdesk.com/adstec-energy/latest_news', headers=headers, timeout=15)
print(f"\nmynewsdesk status: {resp.status_code}, size: {len(resp.text)}")
# look for article links
from bs4 import BeautifulSoup
soup = BeautifulSoup(resp.text, 'lxml')
# try common selectors
items = soup.select('article') or soup.select('.latest-news-item') or soup.select('a[href*="/pressreleases/"]')
print(f"article tags found: {len(soup.select('article'))}")
pr_sel = 'a[href*="/pressreleases/"]'
print(f"pressrelease links: {len(soup.select(pr_sel))}")
for a in soup.select(pr_sel)[:3]:
    href = a.get('href')
    txt = a.get_text(strip=True)[:80]
    print(f"  {href} | {txt}")

# also check if mynewsdesk has an RSS feed
rss_link = soup.find('link', {'type': 'application/rss+xml'}) or soup.find('link', {'type': 'application/atom+xml'})
print(f"\nRSS autodiscovery: {rss_link}")

conn.close()
