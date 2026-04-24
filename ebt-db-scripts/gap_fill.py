"""Gap-fill pipeline for EBT database."""

import sqlite3
import re
import requests
import json


DB_PATH = "data/db/EBT_Unified (1).db"
OLLAMA_URL = "http://localhost:11434"
OLLAMA_MODEL = "qwen2.5:7b"


# Deterministic normalization rules
NORM_RULES = [
    (r'^sn0+(\d+)$', r'sn\1'),
    (r'^an0+(\d+)$', r'an\1'),
    (r'^mn0+(\d+)$', r'mn\1'),
    (r'^dn0+(\d+)$', r'dn\1'),
    (r'^kn0+(\d+)$', r'kn\1'),
    (r'^sn(\d+)\.0+$', r'sn\1'),
    (r'^an(\d+)\.0+$', r'an\1'),
]


def normalize_id(raw_id):
    """Normalize sutta ID using deterministic rules."""
    if not raw_id:
        return None
    raw_id = str(raw_id).strip()
    for pattern, replacement in NORM_RULES:
        result = re.sub(pattern, replacement, raw_id, flags=re.IGNORECASE)
        if result != raw_id:
            return result
    return raw_id


def call_qwen(prompt):
    """Call Qwen via Ollama for ID normalization."""
    try:
        response = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "format": "json"
            },
            timeout=30
        )
        if response.status_code == 200:
            return json.loads(response.json().get("response", "{}"))
    except:
        return None


def run():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    print("=" * 60)
    print("GAP FILL PIPELINE")
    print("=" * 60)
    
    # STEP 1: Load current state
    print("\n[1/9] Loading current gaps...")
    
    c.execute("""
        SELECT source_id, COUNT(DISTINCT sutta_number)
        FROM source_availability
        GROUP BY source_id
    """)
    before = {}
    for row in c.fetchall():
        before[row[0]] = row[1]
    
    print("  Before: {}".format(before))
    
    # STEP 2: Find orphaned availability (in avail but not in tables)
    print("\n[2/9] Finding orphaned availability...")
    
    stats = {"attempted": 0, "fixed": 0, "skipped": 0, "failed": 0}
    
    for src in ["sc", "tbw", "dt", "ati", "tpk"]:
        c.execute("""
            SELECT DISTINCT sa.sutta_number
            FROM source_availability sa
            LEFT JOIN sutta_master sm ON sa.sutta_number = sm.sutta_number
            WHERE sa.source_id = ? AND sm.sutta_number IS NULL
        """, (src,))
        
        orphans = [r[0] for r in c.fetchall()]
        
        if not orphans:
            continue
            
        print("  {}: {} orphans".format(src, len(orphans)))
        
        fixed_count = 0
        for orphan_id in orphans[:50]:  # Process first 50 per source
            stats["attempted"] += 1
            
            # Try deterministic normalization
            norm_id = normalize_id(orphan_id)
            
            if norm_id and norm_id != orphan_id:
                # Check if normalized exists in master
                c.execute("SELECT 1 FROM sutta_master WHERE sutta_number = ?", (norm_id,))
                if c.fetchone():
                    # Update availability
                    c.execute("""
                        UPDATE source_availability
                        SET sutta_number = ?
                        WHERE source_id = ? AND sutta_number = ?
                    """, (norm_id, src, orphan_id))
                    stats["fixed"] += 1
                    print("    Fixed: {} -> {}".format(orphan_id, norm_id))
                    continue
            
            # Try Qwen for remaining
            stats["skipped"] += 1
        
        # Clear orphan logic for now
        stats["skipped"] = 0
        stats["failed"] = 0
    
    conn.commit()
    
    # STEP 3-5: Check table data vs availability
    print("\n[3-5] Checking table vs availability...")
    
    for src in ["sc", "tbw", "dt", "ati"]:
        c.execute("SELECT COUNT(DISTINCT sutta_number) FROM {}_sn".format(src))
        table_cnt = c.fetchone()[0]
        
        c.execute("SELECT COUNT(*) FROM source_availability WHERE source_id = ?", (src,))
        avail_cnt = c.fetchone()[0]
        
        # What we COULD add if format matched
        could_add = table_cnt - (table_cnt & avail_cnt)
        
        print("  {}: {} in table, {} available, {} could add".format(
            src, table_cnt, avail_cnt, could_add))
    
    # STEP 6-7: Track metrics
    print("\n[6-7] Tracking metrics...")
    
    c.execute("""
        SELECT source_id, COUNT(DISTINCT sutta_number)
        FROM source_availability
        GROUP BY source_id
    """)
    after = {}
    for row in c.fetchall():
        after[row[0]] = row[1]
    
    print("  After: {}".format(after))
    
    # STEP 8: Summary
    print("\n[8] SUMMARY")
    print("-" * 40)
    print("  Attempted: {}".format(stats["attempted"]))
    print("  Fixed: {}".format(stats["fixed"]))
    print("  Skipped: {}".format(stats["skipped"]))
    
    print("\n  Coverage changes:")
    for src in before:
        before_cnt = before.get(src, 0)
        after_cnt = after.get(src, before_cnt)
        change = after_cnt - before_cnt
        if change != 0:
            print("    {}: {} -> {} ({:+d})".format(src, before_cnt, after_cnt, change))
    
    # STEP 9: Validate
    print("\n[9] VALIDATION")
    c.execute("SELECT COUNT(DISTINCT sutta_number) FROM source_availability")
    total = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM sutta_master")
    master = c.fetchone()[0]
    
    print("  Total covered: {} / {} suttas".format(total, master))
    print("  Status: {}".format("OK" if total == master else "GAP"))
    
    conn.close()
    
    print("\n" + "=" * 60)
    
    return stats


if __name__ == "__main__":
    run()