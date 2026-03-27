import requests
from bs4 import BeautifulSoup
import sqlite3
import os
import re
import time

db_path = r"C:\Users\ariha\Documents\ebt-translations\DB"
unified_db = os.path.join(db_path, "EBT_Unified.db")

print("=" * 100)
print("COMPLETING MISSING DATA - ALL METHODS")
print("=" * 100)

conn = sqlite3.connect(unified_db)
cur = conn.cursor()

# ===== METHOD 1: SCRAPE MORE DHAMMATALKS SN/AN =====
print("\n" + "=" * 100)
print("METHOD 1: COMPLETE DHAMMATALKS SN/AN SCRAPING")
print("=" * 100)

def scrape_dt_suttas(nikaya, max_suttas=500):
    """Scrape suttas from Dhammatalks"""
    url = f"https://www.dhammatalks.org/suttas/{nikaya}/"
    
    try:
        resp = requests.get(url, timeout=30)
        soup = BeautifulSoup(resp.text, 'html.parser')
        links = soup.find_all('a', href=True)
        
        files = set()
        for link in links:
            href = link.get('href', '')
            if f'/suttas/{nikaya}/' in href and href.endswith('.html'):
                filename = href.split('/')[-1]
                files.add(filename)
        
        return files
    except:
        return set()

def convert_dt_filename(filename, nikaya):
    """Convert DT filename to sutta_number"""
    name = filename.replace('.html', '')
    parts = name.split('_')
    if len(parts) == 2:
        book = parts[0].replace(nikaya.upper(), '')
        sutta = parts[1]
        return f"{nikaya.lower()}{book}.{sutta}"
    return None

# Get DT tables
for nik in ['sn', 'an']:
    dt_table = f"dt_{nik}"
    
    # Ensure table exists
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {dt_table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sutta_number TEXT UNIQUE,
            translation_text TEXT,
            source_url TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Get suttas from index
    files = scrape_dt_suttas(nik.upper())
    print(f"\n{nik.upper()}: Found {len(files)} suttas")
    
    # Process all suttas (batch of 50 at a time)
    processed = 0
    inserted = 0
    batch_size = 20
    
    sorted_files = sorted(files)
    
    for i, filename in enumerate(sorted_files):
        sutta_number = convert_dt_filename(filename, nik.upper())
        
        if sutta_number:
            url = f"https://www.dhammatalks.org/suttas/{nik.upper()}/{filename}"
            
            try:
                resp = requests.get(url, timeout=10)
                soup = BeautifulSoup(resp.text, 'html.parser')
                content = soup.find('div', {'id': 'content'})
                
                if content:
                    text = content.get_text(separator=' ', strip=True)
                    if len(text) > 100:
                        cur.execute(f"""
                            INSERT OR IGNORE INTO {dt_table} (sutta_number, translation_text, source_url)
                            VALUES (?, ?, ?)
                        """, (sutta_number, text, url))
                        inserted += 1
                        
            except:
                pass
            
            processed += 1
        
        # Commit every batch
        if processed % batch_size == 0:
            conn.commit()
            print(f"  {nik.upper()}: Processed {processed}/{len(files)}...")
    
    conn.commit()
    
    # Check count
    cur.execute(f"SELECT COUNT(*) FROM {dt_table} WHERE translation_text IS NOT NULL AND length(translation_text) > 100")
    count = cur.fetchone()[0]
    print(f"  {nik.upper()}: {count} translations now")

print("\n" + "=" * 100)
print("METHOD 2: TRY ACCESS TO INSIGHT FOR DN/MN")
print("=" * 100)

# ATI might have translations
ati_urls = [
    "https://www.accesstoinsight.org/tipitaka/kn/index.html",
    "https://www.accesstoinsight.org/tipitaka/dn/index.html",
    "https://www.accesstoinsight.org/tipitaka/mn/index.html",
]

for url in ati_urls:
    print(f"\nChecking: {url}")
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            print(f"  Status: {resp.status_code}")
    except Exception as e:
        print(f"  Error: {str(e)[:50]}")

print("\n" + "=" * 100)
print("METHOD 3: CHECK DIGITAL PALI READER")
print("=" * 100)

# DPR might have API
dpr_url = "https://www.digitalpalireader.online/"
try:
    resp = requests.get(dpr_url, timeout=10)
    print(f"DPR: {resp.status_code}")
except Exception as e:
    print(f"DPR: {str(e)[:50]}")

print("\n" + "=" * 100)
print("METHOD 4: SUTTACENTRAL API")
print("=" * 100)

# SC might have an API
sc_api = "https://suttacentral.net/api/"
try:
    resp = requests.get(sc_api, timeout=10)
    print(f"SC API: {resp.status_code}")
except Exception as e:
    print(f"SC API: {str(e)[:50]}")

conn.close()

print("\n" + "=" * 100)
print("SCRAPING COMPLETE")
print("=" * 100)
