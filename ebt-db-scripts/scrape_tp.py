import sqlite3
import requests
import time
import re
from bs4 import BeautifulSoup

from ebt_translations.paths import OLD_DB_PATH, ensure_data_directories

BASE_URL = "https://tipitaka.paauksociety.org"

BOOK_RANGES = {
    'dn': (2101, 2303),
    'mn': (3101, 3303),
    'sn': (4101, 4305),
    'an': (5101, 5304),
    'kn': (6101, 6324)
}

NIKAYA_NAMES = {
    'dn': 'Dīghanikāya',
    'mn': 'Majjhimanikāya', 
    'sn': 'Saṃyuttanikāya',
    'an': 'Aṅguttaranikāya',
    'kn': 'Khuddakanikāya'
}

def get_sutta_id(nikaya, number):
    return f"{nikaya}{number}"

def fetch_sutta_content(bookno, max_retries=3):
    for attempt in range(max_retries):
        try:
            url = f"{BASE_URL}/index_palitext(ajax).php?bookno={bookno}&section=1"
            response = requests.get(url, timeout=60)
            
            if response.status_code == 200:
                if 'Database connection failed' in response.text:
                    print(f"  Book {bookno}: Database error, retrying...")
                    time.sleep(5)
                    continue
                return response.text
            else:
                print(f"  Book {bookno}: HTTP {response.status_code}")
        except Exception as e:
            print(f"  Book {bookno}: Error - {str(e)[:50]}")
            time.sleep(5)
    return None

def parse_sutta_content(html_content, bookno):
    if not html_content:
        return None, None
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        pali_text = ""
        english_text = ""
        hindi_text = ""
        
        for p in soup.find_all('p'):
            cls = p.get('class', [])
            text = p.get_text(strip=True)
            
            if not text:
                continue
                
            if 'r' in cls or 'rbld' in cls or 'rbodytext' in cls:
                english_text += text + "\n\n"
            elif 'h' in cls or 'hbld' in cls or 'hbodytext' in cls:
                hindi_text += text + "\n\n"
            elif 'm' in cls or 'mbld' in cls or 'mbodytext' in cls:
                pali_text += text + "\n\n"
        
        return pali_text.strip(), english_text.strip(), hindi_text.strip()
    except Exception as e:
        print(f"  Parse error: {e}")
        return None, None, None

def find_sutta_number_in_content(html_content, bookno):
    if not html_content:
        return None
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        title = soup.find('p', class_='mtitle')
        if title:
            text = title.get_text()
            match = re.search(r'(MN|DN|SN|AN|KN|Ud|Thag|Thig|Dhp)\s*(\d+)', text, re.IGNORECASE)
            if match:
                return int(match.group(2))
        
        for p in soup.find_all(['p', 'div']):
            text = p.get_text()
            match = re.search(r'(?:sutta|section)\s*(\d+)', text, re.IGNORECASE)
            if match:
                return int(match.group(1))
                
    except Exception:
        pass
    
    return None

def import_tp_data():
    ensure_data_directories()
    conn = sqlite3.connect(OLD_DB_PATH)
    cur = conn.cursor()
    
    print("Starting TP data import...")
    print("Target tables: tp_dn, tp_mn, tp_sn, tp_an, tp_kn (English)")
    print("Target tables: tp_dn_hin, tp_mn_hin, tp_sn_hin, tp_an_hin, tp_kn_hin (Hindi)")
    print()
    
    for nikaya, (start, end) in BOOK_RANGES.items():
        print(f"Processing {NIKAYA_NAMES[nikaya]} ({nikaya.upper()})...")
        
        table_en = f"tp_{nikaya}"
        table_hin = f"tp_{nikaya}_hin"
        
        imported_en = 0
        imported_hin = 0
        
        for bookno in range(start, end + 1):
            if (bookno - start) % 10 == 0:
                print(f"  Progress: {bookno - start}/{end - start + 1}")
            
            html = fetch_sutta_content(bookno)
            if not html:
                continue
            
            pali, eng, hin = parse_sutta_content(html, bookno)
            sutta_num = find_sutta_number_in_content(html, bookno)
            
            if sutta_num and pali:
                sutta_id = get_sutta_id(nikaya, sutta_num)
                
                try:
                    cur.execute(f"""
                        INSERT OR REPLACE INTO {table_en} (sutta_id, sutta_number, pali_text, english_text)
                        VALUES (?, ?, ?, ?)
                    """, (sutta_id, str(sutta_num), pali, eng))
                    imported_en += 1
                except Exception as e:
                    print(f"    English insert error: {e}")
                
                if hin:
                    try:
                        cur.execute(f"""
                            INSERT OR REPLACE INTO {table_hin} (sutta_id, sutta_number, pali_text, hindi_text)
                            VALUES (?, ?, ?, ?)
                        """, (sutta_id, str(sutta_num), pali, hin))
                        imported_hin += 1
                    except Exception as e:
                        print(f"    Hindi insert error: {e}")
            
            time.sleep(0.5)
        
        conn.commit()
        print(f"  Imported: {imported_en} English, {imported_hin} Hindi")
        print()
    
    print("Import complete!")
    
    print("\nFinal counts:")
    for nikaya in BOOK_RANGES.keys():
        for suffix in ['', '_hin']:
            table = f"tp_{nikaya}{suffix}"
            count = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count}")
    
    conn.close()

if __name__ == "__main__":
    import_tp_data()
