import sys; sys.path.insert(0, '.')
from db.connection import get_connection
conn = get_connection()
cur = conn.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='symbols' ORDER BY ordinal_position")
print('symbols columns:', [r[0] for r in cur.fetchall()])
cur.execute("SELECT * FROM symbols LIMIT 2")
print('sample:', cur.fetchall())
conn.close()
