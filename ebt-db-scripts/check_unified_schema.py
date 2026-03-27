import sqlite3
import os

db_path = r"C:\Users\ariha\Documents\ebt-translations\DB"
unified_db = os.path.join(db_path, "EBT_Unified.db")

conn_dst = sqlite3.connect(unified_db)

# Check sutta_master schema
print("=== sutta_master schema ===")
cur = conn_dst.cursor()
cur.execute("PRAGMA table_info(sutta_master)")
for col in cur.fetchall():
    print(f"  {col}")

# Check dt_dn schema
print("\n=== dt_dn schema ===")
cur.execute("PRAGMA table_info(dt_dn)")
for col in cur.fetchall():
    print(f"  {col}")

# Check dt_dn data
print("\n=== dt_dn data ===")
cur.execute("SELECT * FROM dt_dn LIMIT 5")
for row in cur.fetchall():
    print(f"  {row}")

conn_dst.close()
