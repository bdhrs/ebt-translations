"""Increase multi-source coverage and clean data quality."""

import sqlite3


DB_PATH = "data/db/EBT_Unified (1).db"


def run():
    # Backup first
    import shutil
    shutil.copy(DB_PATH, DB_PATH.replace(".db", "_before_enhance.db"))
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("=" * 60)
    print("COVERAGE ENHANCEMENT PIPELINE")
    print("=" * 60)
    
    # STAT 1: Before counts
    cursor.execute("SELECT COUNT(*) FROM source_availability")
    print("\nBEFORE: {} availability rows".format(cursor.fetchone()[0]))
    
    cursor.execute("""
        SELECT source_id, COUNT(*) 
        FROM source_availability 
        GROUP BY source_id
        ORDER BY source_id
    """)
    for row in cursor.fetchall():
        print("  {}: {}".format(row[0], row[1]))
    
    # STEP 1: Clean orphan rows
    print("\n--- CLEANING ORPHANS ---")
    cursor.execute("""
        DELETE FROM source_availability
        WHERE sutta_number IN (
            SELECT sa.sutta_number FROM source_availability sa
            LEFT JOIN sutta_master sm ON sm.sutta_number = sa.sutta_number
            WHERE sm.sutta_number IS NULL
        )
    """)
    deleted = cursor.rowcount
    print("Removed {} orphan rows".format(deleted))
    
    # STEP 2: Add missing DT availability to TBW-only suttas
    print("\n--- ENRICHING ---")
    
    # Get TBW-only suttas
    cursor.execute("""
        SELECT sa.sutta_number
        FROM source_availability sa
        WHERE sa.source_id = 'tbw' AND NOT EXISTS (
            SELECT 1 FROM source_availability sa2
            WHERE sa2.sutta_number = sa.sutta_number AND sa2.source_id = 'dt'
        )
    """)
    tbw_only = [row[0] for row in cursor.fetchall()]
    
    # Get DT-only suttas  
    cursor.execute("""
        SELECT sa.sutta_number
        FROM source_availability sa
        WHERE sa.source_id = 'dt' AND NOT EXISTS (
            SELECT 1 FROM source_availability sa2
            WHERE sa2.sutta_number = sa.sutta_number AND sa2.source_id = 'tbw'
        )
    """)
    dt_only = [row[0] for row in cursor.fetchall()]
    
    print("TBW-only: {}, DT-only: {}".format(len(tbw_only), len(dt_only)))
    
    # Try to add DT availability where text exists
    added = 0
    for sutta in tbw_only[:20]:  # Limit for safety
        parts = sutta.split('.')
        nk = parts[0] if parts else 'an'
        if nk in ['an', 'sn', 'dn', 'mn', 'kn']:
            table = "dt_" + nk
            try:
                cursor.execute("""
                    SELECT 1 FROM {} 
                    WHERE sutta_number = ? AND translation_text IS NOT NULL
                """.format(table), (sutta,))
                if cursor.fetchone():
                    cursor.execute("""
                        INSERT OR IGNORE INTO source_availability
                        (sutta_number, source_id, has_pali, has_translation, is_complete, coverage_type)
                        VALUES (?, 'dt', 1, 1, 1, 'partial')
                    """, (sutta,))
                    if cursor.rowcount:
                        added += 1
            except:
                pass
    
    # Try to add TBW availability where text exists
    for sutta in dt_only:
        parts = sutta.split('.')
        nk = parts[0] if parts else 'an'
        if nk in ['an', 'sn', 'dn', 'mn', 'kn']:
            table = "tbw_" + nk
            try:
                cursor.execute("""
                    SELECT 1 FROM {} 
                    WHERE sutta_number = ? AND translation_text IS NOT NULL
                """.format(table), (sutta,))
                if cursor.fetchone():
                    cursor.execute("""
                        INSERT OR IGNORE INTO source_availability
                        (sutta_number, source_id, has_pali, has_translation, is_complete, coverage_type)
                        VALUES (?, 'tbw', 1, 1, 1, 'partial')
                    """, (sutta,))
                    if cursor.rowcount:
                        added += 1
            except:
                pass
    
    print("Added {} new source records".format(added))
    
    # Commit
    conn.commit()
    
    # STAT 2: After counts
    cursor.execute("SELECT COUNT(*) FROM source_availability")
    print("\nAFTER: {} availability rows".format(cursor.fetchone()[0]))
    
    cursor.execute("""
        SELECT source_id, COUNT(*) 
        FROM source_availability 
        GROUP BY source_id
        ORDER BY source_id
    """)
    for row in cursor.fetchall():
        print("  {}: {}".format(row[0], row[1]))
    
    # Multi-source count
    cursor.execute("""
        SELECT COUNT(*) FROM (
            SELECT sutta_number
            FROM source_availability
            GROUP BY sutta_number
            HAVING COUNT(DISTINCT source_id) >= 2
        )
    """)
    multi = cursor.fetchone()[0]
    print("\nMulti-source (2+): {} suttas".format(multi))
    
    conn.close()
    
    print("\n" + "=" * 60)
    print("DONE - Backup created")
    print("=" * 60)


if __name__ == "__main__":
    run()