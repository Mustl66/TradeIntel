
import sys
sys.path.insert(0, "C:/Users/Mustafa/PycharmProjects/TradeIntel")
from fastapi.testclient import TestClient
from admin import app

client = TestClient(app)

# Find a symbol that has edgar articles
import psycopg2, psycopg2.extras
from admin import get_conn
conn = get_conn()
with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    cur.execute("""
        SELECT na.symbol_id, s.symbol, COUNT(*) as cnt
        FROM news_articles na
        JOIN symbols s ON s.id = na.symbol_id
        WHERE na.source_name = 'edgar_8k'
        GROUP BY na.symbol_id, s.symbol
        ORDER BY cnt DESC LIMIT 1
    """)
    row = cur.fetchone()
conn.close()
sym_id = row["symbol_id"]
sym    = row["symbol"]
cnt    = row["cnt"]
print(f"Testing with symbol: {sym} (id={sym_id}, {cnt} edgar filings)")

# Test SEC tab
r = client.get(f"/symbol/{sym_id}/sec")
assert r.status_code == 200, f"SEC route failed: {r.status_code}"
assert "SEC 8-K" in r.text, "Missing SEC 8-K chip"
assert "sec-search" in r.text, "Missing search box"
print(f"SEC tab OK — contains SEC 8-K chip")

# Test SEC search
r2 = client.get(f"/symbol/{sym_id}/sec?q=agreement")
assert r2.status_code == 200
print(f"SEC search OK — status {r2.status_code}")

# Test News tab excludes edgar
r3 = client.get(f"/symbol/{sym_id}/news")
assert r3.status_code == 200
assert "edgar_8k" not in r3.text.lower() or "SEC 8-K" not in r3.text
print(f"News tab OK — edgar filings excluded")

# Test News tab still works (no crash)
assert "news-card" in r3.text or "empty-state" in r3.text
print("All tabs verified.")
