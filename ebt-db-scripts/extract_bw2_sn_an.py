import os
import sqlite3
import re
from datetime import datetime

BW2_PATH = 'data/bw2_20260118'
DB_PATH = 'data/db/EBT_Unified (1).db'

def extract_bw2_sn_an():
    print('BW2 SN/AN Extraction')
    print('='*50)
    
    conn = sqlite3.connect(DB_PATH)
    conn.text_factory = lambda b: b.decode('utf-8', errors='replace')
    cur = conn.cursor()
    
    # Get existing
    cur.execute('SELECT sutta_number FROM source_availability WHERE source_id = "tbw"')
    existing = set(row[0] for row in cur.fetchall())
    print('Existing TBW: {}'.format(len(existing)))
    
    stats = {'sn': 0, 'an': 0}
    
    # Process SN
    print('\nProcessing SN...')
    sn_path = os.path.join(BW2_PATH, 'sn')
    sn_files = [f for f in os.listdir(sn_path) if f.endswith('.html') and not f.startswith('index')]
    print('  Files: {}'.format(len(sn_files)))
    
    added = 0
    for fname in sn_files:
        sutta_id = fname.replace('.html', '')
        
        if sutta_id in existing:
            continue
        
        try:
            fpath = os.path.join(sn_path, fname)
            with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                html = f.read()
            
            # Extract text
            text = ''
            if '<body' in html:
                bstart = html.find('<body')
                bend = html.find('</body>', bstart)
                if bstart > 0 and bend > bstart:
                    body = html[bstart:bend]
                    text = re.sub(r'<[^>]+>', ' ', body)
                    text = re.sub(r'\s+', ' ', text).strip()
            
            if text and len(text) > 50:
                cur.execute('''
                    INSERT OR IGNORE INTO tbw_sn
                    (sutta_number, translation_text, char_count, is_complete, last_updated)
                    VALUES (?, ?, ?, 1, ?)
                ''', (sutta_id, text, len(text), datetime.now()))
                
                cur.execute('''
                    INSERT OR IGNORE INTO source_availability
                    (sutta_number, source_id, has_translation, is_complete)
                    VALUES (?, ?, 1, 1)
                ''', (sutta_id, 'tbw'))
                
                added += 1
                existing.add(sutta_id)
                
        except Exception as e:
            pass
    
    conn.commit()
    stats['sn'] = added
    print('  Added: {}'.format(added))
    
    # Process AN
    print('\nProcessing AN...')
    an_path = os.path.join(BW2_PATH, 'an')
    an_files = [f for f in os.listdir(an_path) if f.endswith('.html') and not f.startswith('index')]
    print('  Files: {}'.format(len(an_files)))
    
    added = 0
    for fname in an_files:
        sutta_id = fname.replace('.html', '')
        
        if sutta_id in existing:
            continue
        
        try:
            fpath = os.path.join(an_path, fname)
            with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                html = f.read()
            
            text = ''
            if '<body' in html:
                bstart = html.find('<body')
                bend = html.find('</body>', bstart)
                if bstart > 0 and bend > bstart:
                    body = html[bstart:bend]
                    text = re.sub(r'<[^>]+>', ' ', body)
                    text = re.sub(r'\s+', ' ', text).strip()
            
            if text and len(text) > 50:
                cur.execute('''
                    INSERT OR IGNORE INTO tbw_an
                    (sutta_number, translation_text, char_count, is_complete, last_updated)
                    VALUES (?, ?, ?, 1, ?)
                ''', (sutta_id, text, len(text), datetime.now()))
                
                cur.execute('''
                    INSERT OR IGNORE INTO source_availability
                    (sutta_number, source_id, has_translation, is_complete)
                    VALUES (?, ?, 1, 1)
                ''', (sutta_id, 'tbw'))
                
                added += 1
                existing.add(sutta_id)
                
        except Exception as e:
            pass
    
    conn.commit()
    stats['an'] = added
    print('  Added: {}'.format(added))
    
    # Final stats
    print('\n=== FINAL ===')
    for nikaya in ['dn', 'mn', 'sn', 'an', 'kn']:
        cur.execute('SELECT COUNT(*) FROM tbw_{}'.format(nikaya))
        print('tbw_{}: {}'.format(nikaya, cur.fetchone()[0]))
    
    conn.close()
    print('\nTotal added: SN={}, AN={}'.format(stats['sn'], stats['an']))

if __name__ == '__main__':
    extract_bw2_sn_an()