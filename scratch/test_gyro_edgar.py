"""Quick GYRO-only EDGAR test."""
import sys, logging
sys.path.insert(0, '.')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s - %(message)s')

from pipeline.edgar_ingest import _load_cik_map, _fetch_symbol_filings, _ensure_edgar_feed_row, _insert_articles, _get_seen_hashes
from db.connection import get_connection

conn = get_connection()
cik_map = _load_cik_map()
seen = _get_seen_hashes(conn)

# GYRO
gyro_cik = cik_map['GYRO']
filings = _fetch_symbol_filings('GYRO', gyro_cik, seen)
print(f"\nGYRO filings: {len(filings)}")
for f in filings[:6]:
    print(f"  {f['published_at'].date()} | {f['title'][:80]}")

if filings:
    # Get GYRO symbol_id
    cur = conn.cursor()
    cur.execute("SELECT id FROM symbols WHERE symbol='GYRO'")
    sym_id = cur.fetchone()[0]
    feed_id = _ensure_edgar_feed_row(conn, sym_id, 'GYRO')
    inserted = _insert_articles(conn, feed_id, sym_id, filings)
    print(f"\nInserted: {inserted}")

    # Verify
    cur.execute("""
        SELECT title, published_at FROM news_articles 
        WHERE symbol_id=%s AND source_name='edgar_8k'
        ORDER BY published_at DESC LIMIT 10
    """, (sym_id,))
    rows = cur.fetchall()
    print(f"GYRO EDGAR articles in DB: {len(rows)}")
    for title, pub in rows:
        print(f"  {pub.date()} | {title[:75]}")

conn.close()
