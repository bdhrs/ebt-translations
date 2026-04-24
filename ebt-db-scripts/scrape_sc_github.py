import requests
import json
import sqlite3
import os
import re
from datetime import datetime

SC_ROOT = "https://api.github.com/repos/suttacentral/sc-data/contents"
PAI_TEXT_BASE = "https://raw.githubusercontent.com/suttacentral/sc-data/main/sc_bilara_data/root/pli/ms/sutta"
TRANS_BASE = "https://raw.githubusercontent.com/suttacentral/sc-data/main/sc_bilara_data/translation/en/sujato/sutta"

DB_PATH = "data/db/EBT_Unified (1).db"

NIKAYA_FOLDERS = {
    # "dn": "dn",
    # "mn": "mn", 
    "sn": "sn",
    # "an": "an",
    # "kn": "kn"
}

def normalize_sutta_id(sc_id, nikaya):
    sc_id = sc_id.strip().lower()
    
    if nikaya == "dn":
        match = re.match(r'dn(\d+)', sc_id)
        if match:
            num = int(match.group(1))
            return f"dn{num:03d}"
    
    elif nikaya == "mn":
        match = re.match(r'mn(\d+)', sc_id)
        if match:
            num = int(match.group(1))
            return f"mn{num:03d}"
    
    elif nikaya == "sn":
        match = re.match(r'sn(\d+)\.?(\d*)', sc_id)
        if match:
            main = int(match.group(1))
            sub = match.group(2)
            if sub:
                return f"sn{main:04d}.{sub.zfill(4)}"
            else:
                return f"sn{main:04d}.0001"
    
    elif nikaya == "an":
        match = re.match(r'an(\d+)\.?(\d*)', sc_id)
        if match:
            main = int(match.group(1))
            sub = match.group(2)
            if sub:
                return f"an{main:04d}.{sub.zfill(4)}"
            else:
                return f"an{main:04d}.0001"
    
    elif nikaya == "kn":
        prefix_map = {
            "dhp": "dhp", "ud": "ud", "snp": "snp", "iti": "iti",
            "thag": "thag", "thig": "thig", "kp": "kp", "kv": "kv",
            "pe": "pe", "pt": "pt", "ap": "ap", "bv": "bv", "cp": "cp"
        }
        for prefix, code in prefix_map.items():
            if sc_id.startswith(prefix):
                num_match = re.search(r'\d+', sc_id[len(prefix):])
                if num_match:
                    num = int(num_match.group())
                    return f"{code}{num}"
        return sc_id
    
    return sc_id

def get_folder_contents(path):
    url = f"{SC_ROOT}/{path}"
    try:
        resp = requests.get(url, timeout=30)
        print(f"    API Response: {resp.status_code}")
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"    ERROR: {resp.status_code} - {resp.text[:100]}")
    except Exception as e:
        print(f"    Exception: {e}")
    return []

def download_json(raw_url):
    try:
        resp = requests.get(raw_url, timeout=30)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        pass
    return None

def parse_bilara_json(json_data):
    if not json_data:
        return ""
    
    text_segments = []
    for segment_id, content in json_data.items():
        if content and content.strip():
            text_segments.append(content.strip())
    
    return "\n\n".join(text_segments)

def get_existing_suttas(conn, source_id):
    cursor = conn.cursor()
    cursor.execute("SELECT sutta_number FROM source_availability WHERE source_id = ?", (source_id,))
    return set(row[0] for row in cursor.fetchall())

def insert_sutta(conn, source, nikaya, sutta_number, pali_text, translation_text):
    cursor = conn.cursor()
    table_name = f"{source}_{nikaya}"
    
    pali_len = len(pali_text) if pali_text else 0
    trans_len = len(translation_text) if translation_text else 0
    
    try:
        cursor.execute(f"""
            INSERT OR IGNORE INTO {table_name}
            (sutta_number, pali_text, translation_text, char_count, is_complete, last_updated)
            VALUES (?, ?, ?, ?, 1, ?)
        """, (sutta_number, pali_text, translation_text, pali_len + trans_len, datetime.now()))
        
        cursor.execute("""
            INSERT OR IGNORE INTO source_availability
            (sutta_number, source_id, has_pali, has_translation, is_complete)
            VALUES (?, ?, ?, ?, 1)
        """, (sutta_number, source, 1 if pali_text else 0, 1 if translation_text else 0))
        
        return True
    except Exception as e:
        print(f"  INSERT ERROR: {sutta_number} - {e}")
        return False

def main():
    print("=" * 70)
    print("SC GITHUB DATA EXTRACTION")
    print("=" * 70)
    
    print(f"\nDB Path: {DB_PATH}")
    print(f"DB exists: {os.path.exists(DB_PATH)}")
    
    if not os.path.exists(DB_PATH):
        print(f"ERROR: DB not found at {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    conn.text_factory = lambda b: b.decode('utf-8', errors='replace')
    
    stats = {"sn": 0, "an": 0, "kn": 0}
    
    for nikaya, folder in NIKAYA_FOLDERS.items():
        print(f"\n=== Processing {nikaya.upper()} ===")
        
        existing_suttas = get_existing_suttas(conn, "sc")
        print(f"  Already in DB: {len(existing_suttas)} suttas")
        
        folder_path = f"sc_bilara_data/root/pli/ms/sutta/{folder}"
        print(f"  Fetching: {SC_ROOT}/{folder_path}")
        items = get_folder_contents(folder_path)
        
        print(f"  API returned: {type(items)}, len={len(items) if items else 0}")
        
        folder_path = f"sc_bilara_data/root/pli/ms/sutta/{folder}"
        print(f"  Fetching: {SC_ROOT}/{folder_path}")
        items = get_folder_contents(folder_path)
        
        if not items:
            print(f"  ERROR: No items found in {folder_path}")
            continue
        
        print(f"  Found {len(items)} subfolders")
        
        processed = 0
        added = 0
        
        for item in items:
            if item.get("type") != "dir":
                continue
            
            subfolder = item["name"]
            subfolder_path = f"sc_bilara_data/root/pli/ms/sutta/{folder}/{subfolder}"
            
            sub_items = get_folder_contents(subfolder_path)
            
            for sub_item in sub_items:
                if not sub_item.get("name", "").endswith("_root-pli-ms.json"):
                    continue
                
                json_filename = sub_item["name"]
                sc_id = json_filename.replace("_root-pli-ms.json", "")
                sutta_number = normalize_sutta_id(sc_id, nikaya)
                
                if sutta_number in existing_suttas:
                    continue
                
                pali_url = f"{PAI_TEXT_BASE}/{folder}/{subfolder}/{json_filename}"
                trans_filename = json_filename.replace('_root-pli-ms', '_translation-en-sujato')
                trans_url = f"{TRANS_BASE}/{folder}/{subfolder}/{trans_filename}"
                
                pali_json = download_json(pali_url)
                trans_json = download_json(trans_url)
                
                pali_text = parse_bilara_json(pali_json) if pali_json else ""
                trans_text = parse_bilara_json(trans_json) if trans_json else ""
                
                if pali_text or trans_text:
                    if insert_sutta(conn, "sc", nikaya, sutta_number, pali_text, trans_text):
                        added += 1
                
                processed += 1
                
                if processed % 50 == 0:
                    print(f"    Progress: {processed} processed, {added} added")
        
        conn.commit()
        stats[nikaya] = added
        print(f"  Added {added} new suttas to {nikaya}")
    
    conn.close()
    
    print("\n" + "=" * 70)
    print("EXTRACTION COMPLETE")
    print("=" * 70)
    print(f"SN added: {stats.get('sn', 0)}")
    print(f"AN added: {stats.get('an', 0)}")
    print(f"KN added: {stats.get('kn', 0)}")

if __name__ == "__main__":
    main()