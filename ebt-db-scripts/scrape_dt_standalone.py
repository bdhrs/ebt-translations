import sqlite3
import re
import time
from html import unescape
from urllib.request import urlopen
import os

BASE_URL = "https://www.dhammatalks.org/suttas"
DATA_DIR = r"C:\Users\ariha\Documents\ebt-translations\data"
DB_DIR = os.path.join(DATA_DIR, "db")
OLD_DB_PATH = os.path.join(DB_DIR, "EBT_Suttas.db")

os.makedirs(DB_DIR, exist_ok=True)

nikayas = {
    'dn': {'folder': 'DN', 'prefix': 'DN', 'table': 'dt_dn'},
    'mn': {'folder': 'MN', 'prefix': 'MN', 'table': 'dt_mn'},
    'sn': {'folder': 'SN', 'prefix': 'SN', 'table': 'dt_sn'},
    'an': {'folder': 'AN', 'prefix': 'AN', 'table': 'dt_an'},
}

def extract_sutta_text(html_content):
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def convert_sutta_id(prefix, filename):
    num = re.search(r'(\d+)', filename)
    if num:
        num = num.group(1)
    else:
        num = filename.replace(prefix, '').replace('.html', '')
    
    prefix_map = {'DN': 'dn', 'MN': 'mn', 'SN': 'sn', 'AN': 'an'}
    return f"{prefix_map.get(prefix, prefix.lower())}{num}"

def main():
    conn = sqlite3.connect(OLD_DB_PATH)
    c = conn.cursor()

    total_imported = 0

    for nikaya, config in nikayas.items():
        print(f"\nProcessing {nikaya.upper()}...")

        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {config['table']} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sutta_id TEXT,
                sutta_number TEXT,
                english_text TEXT
            )
        ''')

        url = f"{BASE_URL}/{config['folder']}/"

        try:
            response = urlopen(url, timeout=30)
            html = response.read().decode('utf-8')

            pattern = rf'/suttas/{config["folder"]}/{config["prefix"]}(\d+)\.html'
            matches = re.findall(pattern, html)

            sutta_nums = list(set(matches))
            print(f"  Found {len(sutta_nums)} sutta numbers")

            count = 0
            for num in sutta_nums:
                try:
                    filename = f"{config['prefix']}{num}.html"
                    full_url = f"{BASE_URL}/{config['folder']}/{filename}"

                    sutta_response = urlopen(full_url, timeout=30)
                    sutta_html = sutta_response.read().decode('utf-8')

                    text = extract_sutta_text(sutta_html)

                    sutta_id = f"{nikaya}{num}"

                    if text and len(text) > 100:
                        c.execute(f'''
                            INSERT OR REPLACE INTO {config['table']}
                            (sutta_id, sutta_number, english_text)
                            VALUES (?, ?, ?)
                        ''', (sutta_id, num, text))
                        count += 1

                except Exception as e:
                    pass

                if count % 10 == 0 and count > 0:
                    conn.commit()
                    print(f"  Progress: {count}")

                time.sleep(0.2)

            conn.commit()
            print(f"  Completed: {count} suttas")
            total_imported += count

        except Exception as e:
            print(f"  Error: {e}")

    print(f"\nTotal imported: {total_imported}")

    print("\n=== DT Summary ===")
    for nikaya in ['dn', 'mn', 'sn', 'an']:
        c.execute(f"SELECT COUNT(*) FROM dt_{nikaya}")
        print(f"  dt_{nikaya}: {c.fetchone()[0]}")

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
