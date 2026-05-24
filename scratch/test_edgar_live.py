"""
Live test: EDGAR pipeline on GYRO specifically.
Verifies correct articles are fetched and stored.
"""
import sys, logging
sys.path.insert(0, '.')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s - %(message)s')

from pipeline.edgar_ingest import _load_cik_map, _fetch_symbol_filings, run
from db.connection import get_connection

# Step 1: Resolve GYRO CIK
cik_map = _load_cik_map()
gyro_cik = cik_map.get('GYRO')
print(f"GYRO CIK: {gyro_cik}")

# Step 2: Fetch filings directly
seen = set()
filings = _fetch_symbol_filings('GYRO', gyro_cik, seen)
print(f"\nFilings fetched: {len(filings)}")
for f in filings[:8]:
    print(f"  {f['published_at'].date()} | {f['title'][:80]}")
    print(f"    URL: {f['url']}")
    print(f"    Text: {len(f['full_text'])} chars")

# Step 3: Run full pipeline with limit=5 symbols
print("\n--- Running edgar_ingest.run(limit=5) ---")
result = run(limit=5)
print(f"Result: {result}")

# Step 4: Check DB for GYRO news
conn = get_connection()
cur = conn.cursor()
cur.execute("""
    SELECT na.title, na.published_at, na.url, na.source_name
    FROM news_articles na
    JOIN symbols s ON s.id = na.symbol_id
    WHERE s.symbol = 'GYRO'
    ORDER BY na.published_at DESC
    LIMIT 10
""")
rows = cur.fetchall()
print(f"\nGYRO articles in DB: {len(rows)}")
for title, pub, url, src in rows:
    print(f"  {pub.date() if pub else 'N/A'} | {src} | {title[:70]}")
conn.close()
