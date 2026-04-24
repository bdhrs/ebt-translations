"""Comprehensive gap analysis for EBT database."""

import sqlite3


DB_PATH = "data/db/EBT_Unified (1).db"


def run():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    print("=" * 60)
    print("COMPREHENSIVE GAP ANALYSIS")
    print("=" * 60)
    
    # STEP 1: Weak suttas (only 1 source)
    print("\n1. WEAK SUTTAS (1 source only)")
    print("-" * 40)
    c.execute("""
        SELECT sutta_number, source_id
        FROM source_availability
        WHERE sutta_number IN (
            SELECT sutta_number
            FROM source_availability
            GROUP BY sutta_number
            HAVING COUNT(DISTINCT source_id) = 1
        )
    """)
    weak = c.fetchall()
    print("  Count: {}".format(len(weak)))
    
    # Breakdown by single source
    c.execute("""
        SELECT source_id, COUNT(*) as cnt
        FROM source_availability
        WHERE sutta_number IN (
            SELECT sutta_number
            FROM source_availability
            GROUP BY sutta_number
            HAVING COUNT(DISTINCT source_id) = 1
        )
        GROUP BY source_id
    """)
    print("  By source:")
    for row in c.fetchall():
        print("    {}: {}".format(row[0], row[1]))
    
    # STEP 2: Source gap per nikaya
    print("\n2. SOURCE GAP PER NIKAYA")
    print("-" * 40)
    for src in ['sc', 'tbw', 'dt', 'ati', 'tpk']:
        c.execute("""
            SELECT sm.nikaya, COUNT(DISTINCT sa.sutta_number) as cnt
            FROM source_availability sa
            JOIN sutta_master sm ON sa.sutta_number = sm.sutta_number
            WHERE sa.source_id = ?
            GROUP BY sm.nikaya
            ORDER BY sm.nikaya
        """, (src,))
        print("  {}:".format(src))
        for row in c.fetchall():
            print("    {}: {}".format(row[0], row[1]))
    
    # STEP 3: Recoverable gaps (in SC but missing from others)
    print("\n3. RECOVERABLE GAPS")
    print("-" * 40)
    for src in ['dt', 'ati', 'tbw']:
        c.execute("""
            SELECT COUNT(*)
            FROM source_availability
            WHERE source_id = 'sc'
            AND sutta_number NOT IN (
                SELECT sutta_number FROM source_availability WHERE source_id = ?
            )
        """, (src,))
        missing = c.fetchone()[0]
        print("  {} to add {}: {}".format(
            src.upper(), src.upper(), missing))
    
    # STEP 4: Multi-source distribution
    print("\n4. MULTI-SOURCE DISTRIBUTION")
    print("-" * 40)
    c.execute("""
        SELECT cnt, COUNT(*) as suttas
        FROM (
            SELECT sutta_number, COUNT(DISTINCT source_id) as cnt
            FROM source_availability
            GROUP BY sutta_number
        )
        GROUP BY cnt
        ORDER BY cnt
    """)
    for row in c.fetchall():
        print("  {} sources: {} suttas".format(row[0], row[1]))
    
    # STEP 5: Quality metrics
    print("\n5. QUALITY METRICS")
    print("-" * 40)
    c.execute("""
        SELECT source_id,
               SUM(CASE WHEN has_translation = 1 THEN 1 ELSE 0 END) as with_trans,
               SUM(CASE WHEN has_pali = 1 THEN 1 ELSE 0 END) as with_pali,
               SUM(CASE WHEN is_complete = 1 THEN 1 ELSE 0 END) as complete,
               COUNT(*) as total
        FROM source_availability
        GROUP BY source_id
    """)
    for row in c.fetchall():
        src, trans, pali, complete, total = row
        trans_pct = (trans / total * 100) if total else 0
        pali_pct = (pali / total * 100) if total else 0
        complete_pct = (complete / total * 100) if total else 0
        print("  {}:".format(src))
        print("    translations: {} ({:.0f}%)".format(trans, trans_pct))
        print("    pali: {} ({:.0f}%)".format(pali, pali_pct))
        print("    complete: {} ({:.0f}%)".format(complete, complete_pct))
    
    conn.close()
    print("\n" + "=" * 60)


if __name__ == "__main__":
    run()