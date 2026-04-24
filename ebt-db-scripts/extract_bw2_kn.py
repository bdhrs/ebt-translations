import os
import sqlite3
from datetime import datetime

DB_PATH = 'data/db/EBT_Unified (1).db'
BW2_PATH = 'data/bw2_20260118'

def extract_bw2_kn():
    print('BW2 KN Data Extraction')
    print('='*50)
    
    conn = sqlite3.connect(DB_PATH)
    conn.text_factory = lambda b: b.decode('utf-8', errors='replace')
    cur = conn.cursor()
    
    # Get existing sc_kn suttas
    cur.execute('SELECT sutta_number FROM source_availability WHERE source_id = "sc" AND (sutta_number LIKE "dhp%" OR sutta_number LIKE "snp%" OR sutta_number LIKE "ud%" OR sutta_number LIKE "thi%" OR sutta_number LIKE "kp%")')
    existing = set(row[0] for row in cur.fetchall())
    print('Existing KN in SC: {}'.format(len(existing)))
    
    # Process each KN subcollection
    kn_folders = {
        'dhp': 'dhp',
        'snp': 'snp', 
        'ud': 'ud',
        'thi': 'thi',
        'kp': 'kp'
    }
    
    total_added = 0
    
    for folder, prefix in kn_folders.items():
        print('\nProcessing {}...'.format(folder))
        folder_path = os.path.join(BW2_PATH, folder)
        
        if not os.path.exists(folder_path):
            print('  Folder not found')
            continue
        
        files = [f for f in os.listdir(folder_path) if f.endswith('.html') and not f.startswith('index')]
        print('  Files: {}'.format(len(files)))
        
        added = 0
        for fname in files:
            # Parse sutta number from filename
            # e.g., dhp1-20.html -> dhp1-20
            sutta_id = fname.replace('.html', '')
            
            if sutta_id in existing:
                continue
            
            # Read HTML file
            try:
                fpath = os.path.join(folder_path, fname)
                with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                    html = f.read()
                
                # Extract text (simple approach - title + body)
                title = ''
                text = ''
                
                # Get title
                if '<title>' in html:
                    tstart = html.find('<title>') + 7
                    tend = html.find('</title>', tstart)
                    if tstart > 6 and tend > tstart:
                        title = html[tstart:tend].strip()
                
                # Get body text
                if '<body' in html:
                    bstart = html.find('<body')
                    bend = html.find('</body>', bstart)
                    if bstart > 0 and bend > bstart:
                        body = html[bstart:bend]
                        # Remove HTML tags
                        import re
                        text = re.sub(r'<[^>]+>', ' ', body)
                        text = re.sub(r'\s+', ' ', text).strip()
                
                if text and len(text) > 50:
                    cur.execute('''
                        INSERT OR IGNORE INTO sc_kn
                        (sutta_number, sutta_title, translation_text, char_count, is_complete, last_updated)
                        VALUES (?, ?, ?, ?, 1, ?)
                    ''', (sutta_id, folder, text, len(text), datetime.now()))
                    
                    cur.execute('''
                        INSERT OR IGNORE INTO source_availability
                        (sutta_number, source_id, has_translation, is_complete)
                        VALUES (?, ?, 1, 1)
                    ''', (sutta_id, 'sc'))
                    
                    added += 1
                    existing.add(sutta_id)
                    
            except Exception as e:
                print('  Error: {} - {}'.format(fname, e))
        
        conn.commit()
        print('  Added: {}'.format(added))
        total_added += added
    
    # Final stats
    print('\n=== FINAL STATS ===')
    cur.execute('SELECT COUNT(*) FROM sc_kn')
    print('sc_kn total: {}'.format(cur.fetchone()[0]))
    
    conn.close()
    print('\nTotal added: {}'.format(total_added))

if __name__ == '__main__':
    extract_bw2_kn()