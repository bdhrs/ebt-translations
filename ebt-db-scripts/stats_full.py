"""Comprehensive statistics for EBT_Unified.db"""

import sqlite3


DB_PATH = "data/db/EBT_Unified (1).db"


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("")
    print("=" * 70)
    print("EBT UNIFIED DATABASE - COMPREHENSIVE STATISTICS")
    print("=" * 70)
    
    # STEP 1: Total per nikaya
    print("\n1. TOTALS PER NIKAYA")
    print("-" * 40)
    cursor.execute("SELECT nikaya, COUNT(*) FROM sutta_master GROUP BY nikaya ORDER BY nikaya")
    totals = {}
    for row in cursor.fetchall():
        totals[row[0]] = row[1]
        print("  {}: {}".format(row[0], row[1]))
    total_suttas = sum(totals.values())
    print("  TOTAL: {}".format(total_suttas))
    
    # STEP 2: Coverage per source x nikaya
    print("\n2. COVERAGE TABLE")
    print("-" * 70)
    print("{:10} {:8} {:8} {:8} {:8} {:8}".format(
        "Source", "Nikaya", "Total", "Extr", "Miss", "%"))
    print("-" * 70)
    
    cursor.execute("""
        SELECT sa.source_id, sm.nikaya, COUNT(DISTINCT sa.sutta_number)
        FROM source_availability sa
        JOIN sutta_master sm ON sm.sutta_number = sa.sutta_number
        GROUP BY sa.source_id, sm.nikaya
        ORDER BY sa.source_id, sm.nikaya
    """)
    
    for row in cursor.fetchall():
        src, nk, cnt = row
        total_nk = totals.get(nk, 0)
        missing_nk = total_nk - cnt
        pct = (cnt / total_nk * 100) if total_nk > 0 else 0
        print("{:10} {:8} {:8} {:8} {:8} {:7.1f}%".format(
            src, nk, total_nk, cnt, missing_nk, pct))
    
    # STEP 3: Source totals
    print("\n3. SOURCE TOTALS")
    print("-" * 40)
    cursor.execute("""
        SELECT sa.source_id, COUNT(DISTINCT sa.sutta_number)
        FROM source_availability sa
        JOIN sutta_master sm ON sm.sutta_number = sa.sutta_number
        GROUP BY sa.source_id
        ORDER BY COUNT(*) DESC
    """)
    
    for row in cursor.fetchall():
        src, cnt = row
        pct = (cnt / total_suttas * 100) if total_suttas > 0 else 0
        print("{:10} {:8} {:7.1f}%".format(src, cnt, pct))
    
    # STEP 4: Multi-source coverage
    print("\n4. MULTI-SOURCE COVERAGE")
    print("-" * 70)
    
    cursor.execute("""
        SELECT sm.nikaya,
               COUNT(*) AS total,
               SUM(CASE WHEN cnt >= 1 THEN 1 ELSE 0 END) AS c1,
               SUM(CASE WHEN cnt >= 2 THEN 1 ELSE 0 END) AS c2,
               SUM(CASE WHEN cnt >= 3 THEN 1 ELSE 0 END) AS c3,
               SUM(CASE WHEN cnt >= 4 THEN 1 ELSE 0 END) AS c4
        FROM (
            SELECT sm.sutta_number, sm.nikaya, COUNT(DISTINCT sa.source_id) AS cnt
            FROM sutta_master sm
            LEFT JOIN source_availability sa ON sm.sutta_number = sa.sutta_number
            GROUP BY sm.sutta_number
        ) t
        JOIN sutta_master sm ON sm.sutta_number = t.sutta_number
        GROUP BY sm.nikaya
    """)
    
    print("{:8} {:8} {:10} {:10} {:10} {:10}".format(
        "Nikaya", "Total", "1+ src", "2+ src", "3+ src", "4+ src"))
    print("-" * 70)
    for row in cursor.fetchall():
        nikaya, total, c1, c2, c3, c4 = row
        print("{:8} {:8} {:10} {:10} {:10} {:10}".format(
            nikaya, total, c1, c2, c3, c4))
    
    # STEP 5: TPK-only count
    print("\n5. TPK-ONLY SUTTAS")
    print("-" * 40)
    cursor.execute("""
        SELECT COUNT(*)
        FROM tpk_sn
        WHERE sutta_number NOT IN (SELECT sutta_number FROM sutta_master)
    """)
    tpk_only = cursor.fetchone()[0]
    print("  Not in sutta_master: {}".format(tpk_only))
    
    cursor.execute("SELECT COUNT(*) FROM tpk_sn")
    tpk_total = cursor.fetchone()[0]
    print("  Total in tpk_sn: {}".format(tpk_total))
    
    # STEP 6: Data quality
    print("\n6. DATA QUALITY CHECKS")
    print("-" * 40)
    
    # Orphan availability
    cursor.execute("""
        SELECT COUNT(*)
        FROM source_availability sa
        LEFT JOIN sutta_master sm ON sm.sutta_number = sa.sutta_number
        WHERE sm.sutta_number IS NULL
    """)
    orphans = cursor.fetchone()[0]
    print("  Orphan availability rows: {}".format(orphans))
    
    # Duplicate availability
    cursor.execute("""
        SELECT sutta_number, source_id, COUNT(*) c
        FROM source_availability
        GROUP BY sutta_number, source_id
        HAVING c > 1
    """)
    dups = cursor.fetchall()
    print("  Duplicate availability: {}".format(len(dups)))
    
    # References
    cursor.execute("SELECT COUNT(*) FROM tpk_references")
    refs = cursor.fetchone()[0]
    print("  tpk_references total: {}".format(refs))
    
    cursor.execute("""
        SELECT reference_type, COUNT(*) 
        FROM tpk_references 
        GROUP BY reference_type
    """)
    print("  References by type:")
    for row in cursor.fetchall():
        print("    {}: {}".format(row[0], row[1]))
    
    conn.close()
    print("")
    print("=" * 70)
    print("STATISTICS COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()