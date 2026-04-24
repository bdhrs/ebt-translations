"""Compute exact source coverage."""

import sqlite3


DB_PATH = "data/db/EBT_Unified (1).db"


def run():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("=" * 60)
    print("EXACT SOURCE COVERAGE")
    print("=" * 60)
    
    # Get counts from source_availability
    cursor.execute("""
        SELECT source_id, 
               COUNT(DISTINCT sutta_number) as suttas,
               SUM(CASE WHEN has_translation = 1 THEN 1 ELSE 0 END) as with_trans,
               SUM(CASE WHEN has_pali = 1 THEN 1 ELSE 0 END) as with_pali,
               SUM(is_complete) as complete
        FROM source_availability
        GROUP BY source_id
    """)
    
    for row in cursor.fetchall():
        src, suttas, with_trans, with_pali, complete = row
        pct = (suttas / 6837.0) * 100
        print("\n{}:".format(src))
        print("  Suttas: {}".format(suttas))
        print("  With translation: {}".format(with_trans))
        print("  With pali: {}".format(with_pali))
        print("  Coverage: {:.1f}%".format(pct))
    
    # TPK special - full text in combined
    cursor.execute("SELECT SUM(char_count) FROM tpk_sn")
    tpk_chars = cursor.fetchone()[0] or 0
    print("\nTPK commentary: {:,} chars total".format(tpk_chars))
    print("  (This is commentary, not individual sutta texts)")
    
    # Total unique suttas
    cursor.execute("SELECT COUNT(DISTINCT sutta_number) FROM source_availability")
    total = cursor.fetchone()[0]
    print("\nTotal unique suttas: {}".format(total))
    
    # Multi-source breakdown
    print("\nMulti-source coverage:")
    cursor.execute("""
        SELECT COUNT(DISTINCT sutta_number)
        FROM source_availability
        GROUP BY sutta_number
        HAVING COUNT(DISTINCT source_id) = 2
    """)
    two = cursor.fetchone()[0]
    cursor.execute("""
        SELECT COUNT(DISTINCT sutta_number)
        FROM source_availability
        GROUP BY sutta_number
        HAVING COUNT(DISTINCT source_id) = 3
    """)
    three = cursor.fetchone()[0]
    cursor.execute("""
        SELECT COUNT(DISTINCT sutta_number)
        FROM source_availability
        GROUP BY sutta_number
        HAVING COUNT(DISTINCT source_id) >= 4
    """)
    four = cursor.fetchone()[0]
    print("  2 sources: {}".format(two))
    print("  3 sources: {}".format(three))  
    print("  4+ sources: {}".format(four))
    
    conn.close()
    print("\n" + "=" * 60)


if __name__ == "__main__":
    run()