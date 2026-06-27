"""
Diagnostic script — check BMRA's exact state in the DB.
Run: python scripts\debug_bmra.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from db import get_connection

conn = get_connection()
cur  = conn.cursor()

print("=" * 70)
print("BMRA — Full DB Diagnostic")
print("=" * 70)

# 1. Symbol row
cur.execute("""
    SELECT id, symbol, status, final_score, sec_score_modifier,
           last_10k_filed, last_10q_filed
    FROM symbols WHERE symbol = 'BMRA'
""")
row = cur.fetchone()
if not row:
    print("❌ BMRA NOT in symbols table at all!")
    sys.exit(1)
sym_id, sym, status, score, modifier, last10k, last10q = row
print(f"\n[symbols] id={sym_id}  status={status}  final_score={score}  "
      f"sec_modifier={modifier}  last_10k={last10k}  last_10q={last10q}")

# 2. All news_articles for BMRA
cur.execute("""
    SELECT id, source_name, form_type, filing_tier, published_at::date,
           sentiment_score, LEFT(url, 80) as url_short
    FROM news_articles
    WHERE symbol_id = %s
    ORDER BY published_at DESC
    LIMIT 40
""", (sym_id,))
rows = cur.fetchall()
print(f"\n[news_articles] Total rows for BMRA: {len(rows)}")
print(f"{'id':>8}  {'source':16}  {'form_type':12}  {'tier':4}  {'date':12}  {'score':8}  url")
print("-"*100)
for r in rows:
    art_id, src, ft, tier, dt, score, url = r
    score_s = f"{score:.3f}" if score is not None else "NULL"
    ft_s    = ft or "NULL"
    tier_s  = str(tier) if tier else "NULL"
    print(f"{art_id:>8}  {(src or 'NULL'):16}  {ft_s:12}  {tier_s:4}  {str(dt):12}  {score_s:8}  {url}")

# 3. Count by form_type
cur.execute("""
    SELECT form_type, COUNT(*) as cnt
    FROM news_articles
    WHERE symbol_id = %s
    GROUP BY form_type ORDER BY cnt DESC
""", (sym_id,))
print("\n[news_articles] Count by form_type:")
for r in cur.fetchall():
    print(f"  form_type={r[0]}  count={r[1]}")

# 4. Count by source_name
cur.execute("""
    SELECT source_name, COUNT(*) as cnt
    FROM news_articles
    WHERE symbol_id = %s
    GROUP BY source_name ORDER BY cnt DESC
""", (sym_id,))
print("\n[news_articles] Count by source_name:")
for r in cur.fetchall():
    print(f"  source={r[0]}  count={r[1]}")

# 5. sec_signals
cur.execute("""
    SELECT signal_type, signal_value, filed_at::date, score_modifier
    FROM sec_signals WHERE symbol_id = %s ORDER BY filed_at DESC LIMIT 10
""", (sym_id,))
sigs = cur.fetchall()
print(f"\n[sec_signals] rows={len(sigs)}")
for s in sigs:
    print(f"  {s}")

conn.close()
print("\n" + "=" * 70)
