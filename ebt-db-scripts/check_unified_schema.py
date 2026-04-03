import sqlite3

from ebt_translations.paths import UNIFIED_DB_PATH, ensure_data_directories


def main():
    ensure_data_directories()
    conn_dst = sqlite3.connect(UNIFIED_DB_PATH)
    cur = conn_dst.cursor()

    # Check sutta_master schema
    print("=== sutta_master schema ===")
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
    try:
        cur.execute("SELECT * FROM dt_dn LIMIT 5")
    except sqlite3.OperationalError as exc:
        print(f"  unavailable: {exc}")
    else:
        for row in cur.fetchall():
            print(f"  {row}")

    conn_dst.close()


if __name__ == "__main__":
    main()
