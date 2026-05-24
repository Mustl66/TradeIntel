import sys; sys.path.insert(0, '.')
from db.connection import get_connection
conn = get_connection()
cur = conn.cursor()
cur.execute("SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='rss_feeds'::regclass AND contype='c'")
for name, defn in cur.fetchall():
    print(f"{name}: {defn}")
conn.close()
