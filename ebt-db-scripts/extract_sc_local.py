import zipfile
import sqlite3
import json
from datetime import datetime

ZIP_PATH = 'sc-data.zip'
DB_PATH = 'data/db/EBT_Unified (1).db'

def main():
    print('SC Local Zip - KN Extraction')
    print('='*50)
    
    z = zipfile.ZipFile(ZIP_PATH, 'r')
    
    # Build index for KN
    print('Building KN index...')
    root_index = {}
    trans_index = {}
    
    files = z.namelist()
    for f in files:
        if '/sutta/kn/' in f and f.endswith('.json'):
            parts = f.split('/')
            if len(parts) >= 4:
                collection = parts[-2]  # dhp, ud, snp, iti, thag, thig, kp
                fname = parts[-1]
                
                if 'root' in f:
                    sc_id = fname.replace('_root-pli-ms.json', '')
                    root_index[(collection, sc_id)] = f
                elif 'translation' in f:
                    sc_id = fname.replace('_translation-en-sujato.json', '')
                    trans_index[(collection, sc_id)] = f
    
    print('Indexed: {} root, {} trans'.format(len(root_index), len(trans_index)))
    
    # Get existing in DB
    conn = sqlite3.connect(DB_PATH)
    conn.text_factory = lambda b: b.decode('utf-8', errors='replace')
    cur = conn.cursor()
    
    cur.execute('SELECT sutta_number FROM source_availability WHERE source_id = "sc"')
    existing = set(row[0] for row in cur.fetchall())
    
    # Find and extract new
    total_added = 0
    
    for (collection, sc_id), zip_path in root_index.items():
        if sc_id in existing:
            continue
        
        try:
            # Read Pali
            content = z.read(zip_path)
            pali_data = json.loads(content)
            pali_text = '\n\n'.join(v for k,v in pali_data.items() if v)
            
            # Read translation
            if (collection, sc_id) in trans_index:
                t_path = trans_index[(collection, sc_id)]
                t_content = z.read(t_path)
                trans_data = json.loads(t_content)
                trans_text = '\n\n'.join(v for k,v in trans_data.items() if v)
            else:
                trans_text = ''
            
            if pali_text or trans_text:
                cur.execute('''
                    INSERT OR IGNORE INTO sc_kn
                    (sutta_number, sutta_title, pali_text, translation_text, char_count, is_complete, last_updated)
                    VALUES (?, ?, ?, ?, ?, 1, ?)
                ''', (sc_id, collection, pali_text, trans_text, len(pali_text)+len(trans_text), datetime.now()))
                
                cur.execute('''
                    INSERT OR IGNORE INTO source_availability
                    (sutta_number, source_id, has_pali, has_translation, is_complete)
                    VALUES (?, ?, ?, ?, 1)
                ''', (sc_id, 'sc', 1 if pali_text else 0, 1 if trans_text else 0))
                
                total_added += 1
                
                if total_added % 20 == 0:
                    print('  Added: {}'.format(total_added))
                    
        except Exception as e:
            print('Error: {}'.format(e))
    
    conn.commit()
    
    # Check final stats
    print('\n=== RESULTS ===')
    for nikaya in ['dn', 'mn', 'sn', 'an', 'kn']:
        cur.execute('SELECT COUNT(*) FROM sc_{}'.format(nikaya))
        print('sc_{}: {}'.format(nikaya, cur.fetchone()[0]))
    
    conn.close()
    print('\nTotal added: {}'.format(total_added))

if __name__ == '__main__':
    main()