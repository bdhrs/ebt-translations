import sqlite3
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from ebt_translations.paths import OLD_DB_PATH, UNIFIED_DB_PATH, DB_DIR, ensure_data_directories


def prnt(msg):
    try:
        sys.stdout.buffer.write((str(msg) + "\n").encode("utf-8"))
    except:
        print(msg)


def get_source_id_mapping():
    return {
        'dt': 'dt',
        'tpk': 'tpk',
        'cst': 'cst',
    }


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
            return 0
    except Exception as e:
        prnt(f"  Error checking table: {e}")
        source_conn.close()
        unified_conn.close()
        return 0
    
    try:
        source_cur.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in source_cur.fetchall()]
        prnt(f"  Source columns: {columns}")
    except Exception as e:
        prnt(f"  Error getting schema: {e}")
        source_conn.close()
        unified_conn.close()
        return 0
    
    try:
        source_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        total = source_cur.fetchone()[0]
        prnt(f"  Total records in source: {total}")
    except Exception as e:
        prnt(f"  Error counting: {e}")
        source_conn.close()
        unified_conn.close()
        return 0
    
    merged = 0
    skipped = 0
    
    try:
        all_records = source_cur.execute(f"SELECT * FROM {table_name}").fetchall()
    except Exception as e:
        prnt(f"  Error fetching records: {e}")
        source_conn.close()
        unified_conn.close()
        return 0
    
    unified_conn_backup = unified_conn
    
    for record in all_records:
        try:
            if source_code == 'dt':
                if len(record) >= 3:
                    sutta_id = record[1] if len(record) > 1 else None
                    english_text = record[2] if len(record) > 2 else None
                else:
                    continue
            elif 'english_text' in columns:
                idx = columns.index('english_text')
                english_text = record[idx] if idx < len(record) else None
                idx = columns.index('sutta_id')
                sutta_id = record[idx] if idx < len(record) else None
            else:
                english_text = None
                sutta_id = None
            
            if not sutta_id:
                skipped += 1
                continue
            
            sutta_id = sutta_id.strip().lower()
            
            if english_text and len(english_text) > 100:
                char_count = len(english_text)
            else:
                char_count = 0
            
            unified_cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE sutta_number = ?", (sutta_id,))
            exists = unified_cur.fetchone()[0]
            
            if exists > 0:
                skipped += 1
                continue
            
            translation_text = english_text if english_text else ""
            source_url = ""
            if len(record) > 3:
                source_url = str(record[3]) if record[3] else ""
            
            unified_cur.execute(f"""
                INSERT INTO {table_name}
                (sutta_number, translation_text, source_url, char_count, is_complete, last_updated)
                VALUES (?, ?, ?, ?, 1, ?)
            """, (sutta_id, translation_text, source_url, char_count, datetime.now()))
            
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
            
            merged += 1
            
            if merged % 50 == 0:
                unified_conn.commit()
                prnt(f"    Progress: {merged} merged, {skipped} skipped")
        
        except Exception as e:
            skipped += 1
            continue
    
    unified_conn.commit()
    prnt(f"  Result: {merged} NEW records merged, {skipped} skipped/existing")
    
    source_conn.close()
    unified_conn.close()
    
    return merged


def main():
    ensure_data_directories()
    
    prnt("=" * 70)
    prnt("MERGE NEW DATA INTO UNIFIED DB (SAFE - ADD ONLY)")
    prnt("=" * 70)
    
    prnt(f"\nSource DB: {OLD_DB_PATH}")
    prnt(f"Unified DB: {UNIFIED_DB_PATH}")
    
    if not os.path.exists(OLD_DB_PATH):
        prnt("\nERROR: EBT_Suttas.db not found!")
        prnt("Please run scrapers first:")
        prnt("  uv run python ebt-db-scripts/scrape_dt.py")
        prnt("  uv run python ebt-db-scripts/scrape_tipitaka.py")
        prnt("  uv run python ebt-db-scripts/scrape_tp.py")
        return
    
    if not os.path.exists(UNIFIED_DB_PATH):
        prnt(f"\nERROR: Unified DB not found at {UNIFIED_DB_PATH}")
        return
    
    prnt("\n" + "=" * 70)
    prnt("STEP 1: MERGING DT (Dhamma Talks)")
    prnt("=" * 70)
    total_dt = 0
    for nikaya in ['dn', 'mn', 'sn', 'an']:
        count = merge_source_table(OLD_DB_PATH, UNIFIED_DB_PATH, 'dt', nikaya)
        total_dt += count
    
    prnt("\n" + "=" * 70)
    prnt("STEP 2: MERGING TPK (Tipitaka Pali)")
    prnt("=" * 70)
    total_tpk = 0
    for nikaya in ['dn', 'mn', 'sn', 'an', 'kn']:
        count = merge_source_table(OLD_DB_PATH, UNIFIED_DB_PATH, 'tpk', nikaya)
        total_tpk += count
    
    prnt("\n" + "=" * 70)
    prnt("FINAL STATS")
    prnt("=" * 70)
    prnt(f"  DT new records: {total_dt}")
    prnt(f"  TPK new records: {total_tpk}")
    prnt(f"\nUnified DB: {UNIFIED_DB_PATH}")
    prnt("\nDone!")


if __name__ == "__main__":
    main()
