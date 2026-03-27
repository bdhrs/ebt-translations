import sqlite3
import os
import re
import requests
from bs4 import BeautifulSoup
import time
import json

DB = r"C:\Users\ariha\Documents\ebt-translations\DB\EBT_Suttas.db"

def get_tree_json(script):
    url = f"https://tipitaka.org/{script}/tree.json"
    try:
        resp = requests.get(url, timeout=30)
        content = resp.content
        
        for enc in ['utf-16', 'utf-16-le', 'utf-8']:
            try:
                text = content.decode(enc)
                return json.loads(text)
            except:
                continue
        
        return json.loads(content.decode('utf-8', errors='ignore'))
    except Exception as e:
        print(f"Error fetching tree: {e}")
        return None

def extract_sutta_info(text):
    try:
        num_match = re.match(r'(\d+)\.\s*(.+)', text)
        if num_match:
            return num_match.group(1), num_match.group(2).strip()
    except:
        pass
    return None, None

def download_sutta(url):
    try:
        resp = requests.get(url, timeout=60)
        content = resp.content
        
        for enc in ['utf-16', 'utf-16-le', 'utf-8', 'latin-1']:
            try:
                text = content.decode(enc)
                break
            except:
                continue
        else:
            text = content.decode('utf-8', errors='ignore')
        
        soup = BeautifulSoup(text, 'xml')
        body = soup.find('body')
        if body:
            text = body.get_text(separator='\n', strip=True)
            text = re.sub(r'\n+', '\n', text)
            return text
    except Exception as e:
        print(f"Error downloading: {e}")
    return None

def scrape_tipitaka():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    
    total_downloaded = 0
    
    for script in ['romn']:
        print(f"\n{'='*60}")
        print(f"Processing {script.upper()} script")
        print(f"{'='*60}")
        
        tree = get_tree_json(script)
        if not tree:
            print(f"Failed to get tree")
            continue
        
        results = []
        nikaya = None
        
        def traverse(node, current_nikaya):
            nonlocal nikaya
            
            if not isinstance(node, dict):
                return current_nikaya
            
            text = node.get('text', '')
            href_attr = node.get('a_attr', {})
            href = href_attr.get('href', '') if isinstance(href_attr, dict) else ''
            
            if not current_nikaya:
                tl = text.lower()
                if 'dīgha' in tl or 'digha' in tl:
                    current_nikaya = 'dn'
                elif 'majjhima' in tl:
                    current_nikaya = 'mn'
                elif 'saṃyutta' in tl or 'samyutta' in tl:
                    current_nikaya = 'sn'
                elif 'aṅguttara' in tl or 'anguttara' in tl:
                    current_nikaya = 'an'
                elif 'khuddaka' in tl:
                    current_nikaya = 'kn'
            
            if href and href.endswith('.xml') and current_nikaya:
                sutta_num, title = extract_sutta_info(text)
                if sutta_num:
                    url = f"https://tipitaka.org/{script}/{href}"
                    results.append({
                        'nikaya': current_nikaya,
                        'sutta_num': sutta_num,
                        'title': title,
                        'url': url
                    })
            
            children = node.get('children', [])
            if isinstance(children, list):
                for child in children:
                    traverse(child, current_nikaya)
            
            return current_nikaya
        
        if isinstance(tree, list):
            for item in tree:
                traverse(item, None)
        else:
            traverse(tree, None)
        
        print(f"Found {len(results)} suttas")
        
        nikaya_stats = {}
        
        for i, item in enumerate(results):
            if i % 20 == 0:
                print(f"  Downloading {i+1}/{len(results)}...")
            
            text = download_sutta(item['url'])
            
            if text:
                sutta_id = f"{item['nikaya']}{item['sutta_num']}"
                table_name = f"tpk_{item['nikaya']}_{script}"
                
                try:
                    c.execute(f'''
                        INSERT OR REPLACE INTO {table_name}
                        (sutta_id, sutta_number, pali_text)
                        VALUES (?, ?, ?)
                    ''', (sutta_id, item['sutta_num'], text))
                    
                    nikaya_stats[item['nikaya']] = nikaya_stats.get(item['nikaya'], 0) + 1
                    total_downloaded += 1
                    
                except Exception as e:
                    pass
            
            time.sleep(0.3)
        
        conn.commit()
        print(f"\nScript {script} stats:")
        for n, count in nikaya_stats.items():
            print(f"  {n}: {count}")
    
    print(f"\n{'='*60}")
    print(f"Total downloaded: {total_downloaded}")
    print(f"{'='*60}")
    
    conn.close()

if __name__ == "__main__":
    scrape_tipitaka()
