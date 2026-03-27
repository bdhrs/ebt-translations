import sqlite3
import pandas as pd
import sys
from datetime import datetime

def prnt(msg):
    sys.stdout.buffer.write((str(msg) + '\n').encode('utf-8'))

OLD_DB = r'C:\Users\ariha\Documents\ebt-translations\DB\EBT_Suttas.db'
NEW_DB = r'C:\Users\ariha\Documents\ebt-translations\DB\EBT_Unified.db'
EXCEL_FILE = r'C:\Users\ariha\Documents\ebt-translations\DB\Massive Table of Sutta Data.xlsx'

old_conn = sqlite3.connect(OLD_DB)
new_conn = sqlite3.connect(NEW_DB)
old_cur = old_conn.cursor()
new_cur = new_conn.cursor()

prnt('='*70)
prnt('BUILDING EBT UNIFIED DATABASE')
prnt('='*70)

# =============================================================================
# 1. POPULATE METADATA TABLES
# =============================================================================

prnt('\n[1/6] Populating metadata...')

# Sources
sources_data = [
    ('sc', 'sc', 'SuttaCentral', 'Various', 'en', 'human', 1, 1, 'https://suttacentral.net', 'CC BY-NC-SA 4.0', 0),
    ('tbw', 'tbw', "The Buddha's Words", 'Bhikkhu Bodhi', 'en', 'human', 1, 1, 'https://www.budsas.org', 'Public Domain', 0),
    ('dt', 'dt', 'Dhamma Talks', 'Thanissaro Bhikkhu', 'en', 'human', 1, 1, 'https://www.dhammatalks.org/suttas', 'CC BY-NC-ND 4.0', 0),
    ('cst', 'cst', 'Chaṭṭha Saṅgāyana Tipiṭaka', '6th Council', 'pli', 'human', 1, 0, 'https://tipitaka.org', 'Public Domain', 0),
    ('tpk', 'tpk', 'Tipitaka Pali', 'Pa Auk Society', 'pli', 'human', 1, 0, 'https://tipitaka.paauksociety.org', 'Restricted', 0),
    ('pau', 'pau', 'Pa Auk AI Translation', 'AI + Human', 'en', 'ai', 1, 1, 'https://tipitaka.paauksociety.org', 'Various', 0),
    ('epi', 'epi', 'ePitaka', 'AI Translation', 'en', 'ai', 1, 1, 'https://epitaka.org', 'Various', 0),
]

new_cur.executemany('''
    INSERT OR REPLACE INTO sources 
    (source_id, source_code, source_name, translator, language, translation_type, has_pali, has_translation, url, license, completeness_pct)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''', sources_data)

# Nikayas
nikayas_data = [
    ('dn', 'Digha Nikaya', 'long', 34, 3, 'Long discourses (Suttas 1-34)'),
    ('mn', 'Majjhima Nikaya', 'middle', 152, 15, 'Middle-length discourses (Suttas 1-152)'),
    ('sn', 'Samyutta Nikaya', 'linked', 2309, 56, 'Linked/grouped discourses by topic'),
    ('an', 'Anguttara Nikaya', 'numerical', 2300, 11, 'Numerical discourses (1-11 factors)'),
    ('kn', 'Khuddaka Nikaya', 'minor', 18, 18, 'Minor collections (15+ texts)'),
]

new_cur.executemany('''
    INSERT OR REPLACE INTO nikayas 
    (nikaya_code, nikaya_name, division_type, expected_suttas, vagga_count, description)
    VALUES (?, ?, ?, ?, ?, ?)
''', nikayas_data)

# KN Subcollections
kn_subs = [
    ('dhp', 'Dhammapada', 423, 'kn'),
    ('iti', 'Itivuttaka', 112, 'kn'),
    ('snp', 'Sutta Nipata', 77, 'kn'),
    ('thag', 'Theragatha', 264, 'kn'),
    ('thig', 'Therigatha', 73, 'kn'),
    ('ud', 'Udana', 88, 'kn'),
    ('kp', 'Khuddakapatha', 9, 'kn'),
    ('ps', 'Patisambhida', 357, 'kn'),
    ('ap', 'Apadana', 547, 'kn'),
    ('bv', 'Buddhavamsa', 24, 'kn'),
    ('cp', 'Cariyapitaka', 35, 'kn'),
]

new_cur.executemany('''
    INSERT OR REPLACE INTO kn_subcollections 
    (sub_code, sub_name, expected_count, parent_nikaya)
    VALUES (?, ?, ?, ?)
''', kn_subs)

new_conn.commit()
prnt('  Metadata populated')

# =============================================================================
# 2. IMPORT FROM EXCEL - Get all sutta numbers
# =============================================================================

prnt('\n[2/6] Importing sutta numbers from Excel...')

try:
    df_excel = pd.read_excel(EXCEL_FILE)
    prnt(f'  Excel loaded: {len(df_excel)} rows')
    
    # Extract all unique sutta UIDs from various columns
    all_suttas = set()
    
    # From SC codes
    if 'sc_code' in df_excel.columns:
        sc_codes = df_excel['sc_code'].dropna().unique()
        for code in sc_codes:
            if isinstance(code, str) and len(code) > 0:
                all_suttas.add(code.strip().lower())
    
    # From CST codes
    if 'cst_code' in df_excel.columns:
        cst_codes = df_excel['cst_code'].dropna().unique()
        for code in cst_codes:
            if isinstance(code, str) and len(code) > 0:
                # Convert to standard format (dn1.1 -> dn1)
                normalized = code.strip().lower().split('.')[0]
                if len(normalized) >= 2:
                    all_suttas.add(normalized)
    
    # From DPD codes
    if 'dpd_code' in df_excel.columns:
        dpd_codes = df_excel['dpd_code'].dropna().unique()
        for code in dpd_codes:
            if isinstance(code, str) and len(code) > 0:
                normalized = code.strip().lower().replace('_', '')
                all_suttas.add(normalized)
    
    prnt(f'  Found {len(all_suttas)} unique sutta numbers from Excel')
    
except Exception as e:
    prnt(f'  Excel import error: {e}')
    all_suttas = set()

# =============================================================================
# 3. CREATE SUTTA MASTER FROM OLD DB
# =============================================================================

prnt('\n[3/6] Creating sutta master from existing data...')

# Get all existing sutta IDs from EBT_Suttas
sutta_count = 0

# SC data
for nikaya in ['dn', 'mn', 'sn', 'an']:
    table = f'sc_{nikaya}'
    try:
        old_cur.execute(f'SELECT sutta_id FROM {table}')
        for (uid,) in old_cur.fetchall():
            if uid:
                normalized = uid.strip().lower()
                if normalized not in all_suttas:
                    all_suttas.add(normalized)
                sutta_count += 1
    except:
        pass

# SC KN
try:
    old_cur.execute('SELECT sutta_id, sub_collection FROM sc_kn')
    for (uid, sub) in old_cur.fetchall():
        if uid:
            all_suttas.add(uid.strip().lower())
            sutta_count += 1
except:
    pass

# TBW data
for nikaya in ['dn', 'mn', 'sn', 'an']:
    table = f'tb_{nikaya}'
    try:
        old_cur.execute(f'SELECT sutta_id FROM {table}')
        for (uid,) in old_cur.fetchall():
            if uid:
                all_suttas.add(uid.strip().lower())
                sutta_count += 1
    except:
        pass

# DT data
for nikaya in ['dn', 'mn', 'sn']:
    table = f'dt_{nikaya}'
    try:
        old_cur.execute(f'SELECT sutta_id FROM {table}')
        for (uid,) in old_cur.fetchall():
            if uid:
                all_suttas.add(uid.strip().lower())
    except:
        pass

prnt(f'  Collected {len(all_suttas)} unique sutta numbers')
prnt(f'  Processed {sutta_count} records from old DB')

# Categorize and insert into sutta_master
dn_suttas = sorted([s for s in all_suttas if s.startswith('dn')])
mn_suttas = sorted([s for s in all_suttas if s.startswith('mn')])
sn_suttas = sorted([s for s in all_suttas if s.startswith('sn')])
an_suttas = sorted([s for s in all_suttas if s.startswith('an')])

# KN sub-collections
kn_suttas = {sub: [] for sub, _, _, _ in kn_subs}
for s in all_suttas:
    if s.startswith('dhp'): kn_suttas['dhp'].append(s)
    elif s.startswith('iti'): kn_suttas['iti'].append(s)
    elif s.startswith('snp'): kn_suttas['snp'].append(s)
    elif s.startswith('thag'): kn_suttas['thag'].append(s)
    elif s.startswith('thig'): kn_suttas['thig'].append(s)
    elif s.startswith('ud'): kn_suttas['ud'].append(s)
    elif s.startswith('kp'): kn_suttas['kp'].append(s)
    elif s.startswith(('kn', 'pli', 'vin')): pass  # Skip other KN codes
    elif s.startswith(('dn', 'mn', 'sn', 'an')): pass  # Already handled
    else:
        # Unknown - might be KN
        if s not in dn_suttas + mn_suttas + sn_suttas + an_suttas:
            kn_suttas['dhp'].append(s)  # Default to dhp

# Insert main nikayas
def insert_suttas(nikaya_code, suttas, sub_coll=None):
    count = 0
    for s in suttas:
        try:
            new_cur.execute('''
                INSERT OR IGNORE INTO sutta_master (sutta_number, nikaya, sub_collection, last_updated)
                VALUES (?, ?, ?, ?)
            ''', (s, nikaya_code, sub_coll, datetime.now()))
            count += 1
        except:
            pass
    return count

prnt('  Inserting sutta numbers...')
insert_suttas('dn', dn_suttas)
insert_suttas('mn', mn_suttas)
insert_suttas('sn', sn_suttas)
insert_suttas('an', an_suttas)

# Insert KN
for sub, suttas in kn_suttas.items():
    if suttas:
        insert_suttas('kn', suttas, sub)

new_conn.commit()

# Count total
new_cur.execute('SELECT COUNT(*) FROM sutta_master')
total = new_cur.fetchone()[0]
prnt(f'  sutta_master populated: {total} suttas')

# =============================================================================
# 4. CREATE SOURCE TABLES
# =============================================================================

prnt('\n[4/6] Creating source tables...')

def create_source_table(conn, cur, source_code, nikaya_code):
    table_name = f'{source_code}_{nikaya_code}'
    try:
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sutta_number TEXT NOT NULL UNIQUE,
                sutta_title TEXT,
                pali_text TEXT,
                translation_text TEXT,
                source_url TEXT,
                char_count INTEGER,
                is_complete BOOLEAN DEFAULT 1,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sutta_number) REFERENCES sutta_master(sutta_number)
            )
        ''')
        conn.commit()
        return True
    except Exception as e:
        prnt(f'    Error creating {table_name}: {e}')
        return False

# Create tables for all sources x nikayas
sources_list = ['sc', 'tbw', 'dt', 'cst', 'tpk', 'pau']
nikayas_list = ['dn', 'mn', 'sn', 'an', 'kn']

tables_created = 0
for src in sources_list:
    for nik in nikayas_list:
        if create_source_table(new_conn, new_cur, src, nik):
            tables_created += 1

prnt(f'  Created {tables_created} source tables')

# =============================================================================
# 5. IMPORT DATA FROM OLD DB
# =============================================================================

prnt('\n[5/6] Importing data from EBT_Suttas.db...')

def import_source_data(src_old, src_new, nikaya, sub_coll=None):
    table_old = f'{src_old}_{nikaya}'
    table_new = f'{src_new}_{nikaya}'
    
    try:
        old_cur.execute(f'SELECT sutta_id, pali_text, translation_markdown FROM {table_old}')
        rows = old_cur.fetchall()
        imported = 0
        
        for sutta_id, pali, trans in rows:
            if not sutta_id:
                continue
            sutta_num = sutta_id.strip().lower()
            
            # Calculate char counts
            pali_len = len(pali) if pali else 0
            trans_len = len(trans) if trans else 0
            
            try:
                new_cur.execute(f'''
                    INSERT OR REPLACE INTO {table_new} 
                    (sutta_number, pali_text, translation_text, char_count, is_complete)
                    VALUES (?, ?, ?, ?, ?)
                ''', (sutta_num, pali, trans, pali_len + trans_len, 1))
                
                # Update sutta_master
                new_cur.execute(f'''
                    UPDATE sutta_master SET 
                        has_pali = COALESCE(has_pali || ',{src_new}', '{src_new}'),
                        has_english = CASE WHEN ? THEN COALESCE(has_english || ',{src_new}', '{src_new}') ELSE has_english END
                    WHERE sutta_number = ?
                ''', (1 if trans else 0, sutta_num))
                
                # Update availability
                new_cur.execute(f'''
                    INSERT OR REPLACE INTO source_availability 
                    (sutta_number, source_id, has_pali, has_translation, is_complete)
                    VALUES (?, ?, ?, ?, 1)
                ''', (sutta_num, src_new, 1, 1 if trans else 0))
                
                imported += 1
            except:
                pass
        
        return imported
    except Exception as e:
        return 0

# Import SC
prnt('  Importing SuttaCentral (sc)...')
for nikaya in ['dn', 'mn', 'sn', 'an']:
    count = import_source_data('sc', 'sc', nikaya)
    prnt(f'    sc_{nikaya}: {count} imported')

# Import SC KN
try:
    old_cur.execute('SELECT sutta_id, sub_collection, pali_text, translation_markdown FROM sc_kn')
    rows = old_cur.fetchall()
    imported = 0
    for sutta_id, sub, pali, trans in rows:
        if not sutta_id:
            continue
        sutta_num = sutta_id.strip().lower()
        try:
            new_cur.execute(f'''
                INSERT OR REPLACE INTO sc_kn 
                (sutta_number, sutta_title, pali_text, translation_text, is_complete)
                VALUES (?, ?, ?, ?, ?)
            ''', (sutta_num, sub, pali, trans, 1))
            
            new_cur.execute('''
                UPDATE sutta_master SET sub_collection = ? WHERE sutta_number = ?
            ''', (sub, sutta_num))
            imported += 1
        except:
            pass
    prnt(f'    sc_kn: {imported} imported')
except Exception as e:
    prnt(f'    sc_kn import error: {e}')

# Import TBW
prnt('  Importing The Buddha\'s Words (tbw)...')
for nikaya in ['dn', 'mn', 'sn', 'an']:
    count = import_source_data('tb', 'tbw', nikaya)
    prnt(f'    tbw_{nikaya}: {count} imported')

# Import DT
prnt('  Importing Dhamma Talks (dt)...')
for nikaya in ['dn', 'mn', 'sn']:
    count = import_source_data('dt', 'dt', nikaya)
    prnt(f'    dt_{nikaya}: {count} imported')

new_conn.commit()

# =============================================================================
# 6. UPDATE COMPLETENESS & FINALIZE
# =============================================================================

prnt('\n[6/6] Finalizing database...')

# Update completeness percentages
for src in ['sc', 'tbw', 'dt']:
    new_cur.execute(f'SELECT COUNT(*) FROM {src}_dn')
    dn_count = new_cur.fetchone()[0]
    new_cur.execute(f'SELECT expected_suttas FROM nikayas WHERE nikaya_code = ?', ('dn',))
    expected = new_cur.fetchone()[0] or 34
    pct = min(100, int(dn_count * 100 / expected))
    new_cur.execute('UPDATE sources SET completeness_pct = ? WHERE source_id = ?', (pct, src))

# Add available columns for tracking
try:
    new_cur.execute('''
        ALTER TABLE sutta_master ADD COLUMN has_sc_pali INTEGER DEFAULT 0
    ''')
    new_cur.execute('''
        ALTER TABLE sutta_master ADD COLUMN has_sc_english INTEGER DEFAULT 0
    ''')
except:
    pass

new_conn.commit()

# =============================================================================
# SUMMARY
# =============================================================================

prnt('\n' + '='*70)
prnt('DATABASE BUILD COMPLETE')
prnt('='*70)

new_cur.execute('SELECT COUNT(*) FROM sutta_master')
total = new_cur.fetchone()[0]
prnt(f'\nSutta Master: {total} suttas')

prnt('\nNikaya counts:')
for nikaya, name in [('dn','Digha'), ('mn','Majjhima'), ('sn','Samyutta'), ('an','Anguttara'), ('kn','Khuddaka')]:
    new_cur.execute(f'SELECT COUNT(*) FROM sutta_master WHERE nikaya = ?', (nikaya,))
    cnt = new_cur.fetchone()[0]
    prnt(f'  {nikaya} ({name}): {cnt}')

prnt('\nSource table counts:')
for src in ['sc', 'tbw', 'dt']:
    total_src = 0
    for nik in ['dn', 'mn', 'sn', 'an']:
        try:
            new_cur.execute(f'SELECT COUNT(*) FROM {src}_{nik}')
            total_src += new_cur.fetchone()[0]
        except:
            pass
    prnt(f'  {src.upper()}: {total_src} suttas')

prnt(f'\nDatabase: {NEW_DB}')

old_conn.close()
new_conn.close()

prnt('\nDone!')
