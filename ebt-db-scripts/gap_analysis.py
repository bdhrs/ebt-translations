"""Gap detection and filling for EBT_Unified.db"""

import sqlite3
import re


DB_PATH = "data/db/EBT_Unified (1).db"

# Normalization rules from multiple formats
NORM_PATTERNS = [
    # (pattern, replacement)
    (r'^sn0+(\d+)$', r'sn\1'),      # sn01 -> sn1
    (r'^an0+(\d+)$', r'an\1'),      # an01 -> an1
    (r'^mn0+(\d+)$', r'mn\1'),      # mn01 -> mn1
    (r'^dn0+(\d+)$', r'dn\1'),      # dn01 -> dn1
    (r'^sn(\d+)\.(\d+)$', r'sn\1.\2'),  # sn1.1 stays
    (r'^an(\d+)\.(\d+)$', r'an\1.\2'),  # an1.1 stays
]


def normalize_sutta_id(sutta_id):
    """Normalize sutta ID to master format."""
    if not sutta_id:
        return None
    
    sutta_id = str(sutta_id).strip()
    
    # Try patterns
    for pattern, replacement in NORM_PATTERNS:
        result = re.sub(pattern, replacement, sutta_id, flags=re.IGNORECASE)
        if result != sutta_id:
            return result
    
    return sutta_id


def analyze_gaps():
    """Analyze actual gaps between tables and availability."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    print("=" * 60)
    print("GAP ANALYSIS")
    print("=" * 60)
    
    results = {}
    
    for src in ['sc', 'tbw', 'dt', 'ati']:
        print("\n{}:".format(src))
        
        # Get unique suttas in table
        c.execute('SELECT DISTINCT sutta_number FROM {}_sn'.format(src))
        table_suttas = set(normalize_sutta_id(r[0]) for r in c.fetchall())
        
        # Get suttas in availability
        c.execute('SELECT sutta_number FROM source_availability WHERE source_id = ?', (src,))
        avail_suttas = set(normalize_sutta_id(r[0]) for r in c.fetchall())
        
        # In table but not in avail (potential expansion)
        expand = table_suttas - avail_suttas
        
        # In both (valid multi-source)
        valid_multi = table_suttas & avail_suttas
        
        # Format differences
        format_diff = avail_suttas - table_suttas
        
        print('  Valid: {}'.format(len(valid_multi)))
        print('  Could expand: {}'.format(len(expand)))
        print('  Format diff: {}'.format(len(format_diff)))
        
        if format_diff and len(format_diff) < 10:
            print('  Format issues: {}'.format(list(format_diff)[:5]))
        
        results[src] = {
            'valid': len(valid_multi),
            'expand': len(expand),
            'format_diff': len(format_diff)
        }
    
    conn.close()
    return results


def fix_format_gaps():
    """Fix format differences in availability."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    print("\n" + "=" * 60)
    print("FIXING FORMAT GAPS")
    print("=" * 60)
    
    fixed = 0
    
    for src in ['sc', 'tbw', 'dt', 'ati', 'tpk']:
        print("\n{}:".format(src))
        
        # Find suttas in availability not matching table
        for nikaya in ['sn', 'an', 'dn', 'mn', 'kn']:
            table = "{}_{}".format(src, nikaya)
            
            # Get sutta IDs from availability that don't match
            c.execute('''
                SELECT DISTINCT sa.sutta_number
                FROM source_availability sa
                LEFT JOIN {table} t ON sa.sutta_number = t.sutta_number
                WHERE sa.source_id = ? AND t.sutta_number IS NULL
            '''.format(table=table), (src,))
            
            orphan_ids = [r[0] for r in c.fetchall()]
            
            if orphan_ids:
                print('  {}: {} orphan IDs'.format(nikaya, len(orphan_ids)))
                
                # Try to normalize and find match
                for old_id in orphan_ids[:5]:
                    new_id = normalize_sutta_id(old_id)
                    if new_id != old_id:
                        # Check if normalized exists in table
                        c.execute('''
                            SELECT 1 FROM {table} 
                            WHERE sutta_number = ?
                        '''.format(table=table), (new_id,))
                        
                        if c.fetchone():
                            # Update availability
                            c.execute('''
                                UPDATE source_availability 
                                SET sutta_number = ?
                                WHERE source_id = ? AND sutta_number = ?
                            ''', (new_id, src, old_id))
                            fixed += 1
                            print('    {} -> {}'.format(old_id, new_id))
    
    conn.commit()
    conn.close()
    
    print('\nFixed: {} records'.format(fixed))
    return fixed


if __name__ == "__main__":
    analyze_gaps()
    fix_format_gaps()
    analyze_gaps()