import sqlite3
import sys
import os
from datetime import datetime

OLD_DB_PATH = r"C:\Users\ariha\Documents\ebt-translations\data\db\EBT_Suttas.db"
UNIFIED_DB_PATH = r"C:\Users\ariha\Documents\ebt-translations\data\db\EBT_Unified (1).db"

def prnt(msg):
    try:
        sys.stdout.buffer.write((str(msg) + "\n").encode("utf-8"))
    except:
        print(msg)

def merge_source_table(source_db, unified_db, source_code, nikaya):
    unified_conn = sqlite3.connect(unified_db)
    unified_cur = unified_conn.cursor()
    
    source_conn = sqlite3.connect(source_db)
    source_cur = source_conn.cursor()
    
    table_name = f"{source_code}_{nikaya}"
    
    prnt(f"\n=== Merging {table_name} ===")
    
    try:
        source_cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not source_cur.fetchone():
            prnt(f"  Table {table_name} does not exist in source DB")
            source_conn.close()
            unified_conn.close()
            return 0, 0
    except Exception as e:
        prnt(f"  Error checking table: {e}")
        source_conn.close()
        unified_conn.close()
        return 0, 0
    
    try:
        source_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        total = source_cur.fetchone()[0]
        prnt(f"  Source records: {total}")
    except Exception as e:
        prnt(f"  Error: {e}")
        source_conn.close()
        unified_conn.close()
        return 0, 0
    
    merged = 0
    skipped = 0
    
    try:
        all_records = source_cur.execute(f"SELECT * FROM {table_name}").fetchall()
    except Exception as e:
        prnt(f"  Error fetching: {e}")
        source_conn.close()
        unified_conn.close()
        return 0, 0
    
    for record in all_records:
        try:
            if len(record) >= 3:
                sutta_id = str(record[1]).strip().lower() if record[1] else None
                english_text = record[2] if record[2] else None
            else:
                skipped += 1
                continue
            
            if not sutta_id:
                skipped += 1
                continue
            
            if english_text and len(english_text) > 100:
                char_count = len(english_text)
            else:
                skipped += 1
                continue
            
            try:
                unified_cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE sutta_number = ?", (sutta_id,))
                exists = unified_cur.fetchone()[0]
            except:
                exists = 0
            
            if exists > 0:
                skipped += 1
                continue
            
            unified_cur.execute(f"""
                INSERT INTO {table_name}
                (sutta_number, translation_text, char_count, is_complete, last_updated)
                VALUES (?, ?, ?, 1, ?)
            """, (sutta_id, english_text, char_count, datetime.now().isoformat()))
            
            try:
                unified_cur.execute("""
                    UPDATE sutta_master SET
                        has_english = CASE
                            WHEN has_english IS NULL OR has_english = '' THEN ?
                            WHEN instr(has_english, ?) > 0 THEN has_english
                            ELSE has_english || ',' || ?
                        END
                    WHERE sutta_number = ?
                """, (source_code, source_code, source_code, sutta_id))
                
                unified_cur.execute("""
                    INSERT OR IGNORE INTO source_availability
                    (sutta_number, source_id, has_translation, is_complete)
                    VALUES (?, ?, 1, 1)
                """, (sutta_id, source_code))
            except Exception as e:
                pass
            
            merged += 1
            
            if merged % 20 == 0:
                unified_conn.commit()
                prnt(f"    Progress: {merged} merged")
        
        except Exception as e:
            skipped += 1
            continue
    
    unified_conn.commit()
    prnt(f"  Result: {merged} NEW, {skipped} skipped")
    
    source_conn.close()
    unified_conn.close()
    
    return merged, skipped

def main():
    prnt("=" * 70)
    prnt("MERGE DT DATA INTO UNIFIED DB (SAFE - ADD ONLY)")
    prnt("=" * 70)
    
    prnt(f"\nSource DB: {OLD_DB_PATH}")
    prnt(f"Unified DB: {UNIFIED_DB_PATH}")
    
    if not os.path.exists(OLD_DB_PATH):
        prnt("\nERROR: EBT_Suttas.db not found!")
        return
    
    if not os.path.exists(UNIFIED_DB_PATH):
        prnt(f"\nERROR: Unified DB not found!")
        return
    
    prnt("\n" + "=" * 70)
    prnt("Merging DT (Dhamma Talks)")
    prnt("=" * 70)
    
    total_merged = 0
    total_skipped = 0
    
    for nikaya in ['dn', 'mn', 'sn', 'an']:
        m, s = merge_source_table(OLD_DB_PATH, UNIFIED_DB_PATH, 'dt', nikaya)
        total_merged += m
        total_skipped += s
    
    prnt("\n" + "=" * 70)
    prnt("FINAL STATS")
    prnt("=" * 70)
    prnt(f"  New records merged: {total_merged}")
    prnt(f"  Records skipped (existing): {total_skipped}")
    prnt(f"\nUnified DB: {UNIFIED_DB_PATH}")
    prnt("Done!")

if __name__ == "__main__":
    main()
