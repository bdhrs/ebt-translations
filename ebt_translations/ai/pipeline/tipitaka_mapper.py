"""Deterministic Tipitaka sutta mapping from source_table pattern."""

import sqlite3
from typing import Dict, List, Tuple
from dataclasses import dataclass
from collections import defaultdict

from .. import config

NIKAYA_MAP = {
    "s": "sn",
    "e": "an",
    "m": "mn",
    "d": "dn",
    "k": "kn",
}

VALID_NIKAYAS = {"sn", "an", "mn", "dn", "kn"}


@dataclass
class MappingResult:
    """Result of mapping source_table to sutta."""
    source_table: str
    derived_sutta: str
    exists_in_master: bool
    mapped_method: str


def is_valid_sutta_pattern(sutta_number: str) -> bool:
    """Check if sutta matches valid pattern (e.g., sn1.1)."""
    if not sutta_number:
        return False
    import re
    pattern = r"^(sn|an|mn|dn|kn)\d+\.\d+$"
    return bool(re.match(pattern, sutta_number))


def parse_source_table(tbl: str) -> str:
    """Parse source_table to sutta_number.
    
    Example: s0101a_att -> sn1.1
    """
    if not tbl or len(tbl) < 5:
        return None
    
    prefix = tbl[0].lower()
    nikaya = NIKAYA_MAP.get(prefix)
    if not nikaya:
        return None
    
    try:
        s_num = int(tbl[1:3])
        v_num = int(tbl[3:5])
        return f"{nikaya}{s_num}.{v_num}"
    except ValueError:
        return None


def group_segments(rows: List[Tuple]) -> Dict[str, str]:
    """Group segments by source_table, combine text."""
    groups = defaultdict(list)
    for row in rows:
        if len(row) >= 2 and row[1]:
            groups[row[0]].append(row[1])
    
    return {tbl: " ".join(texts) for tbl, texts in groups.items()}


def validate_sutta(conn: sqlite3.Connection, sutta_number: str) -> bool:
    """Check if sutta exists in sutta_master."""
    if not sutta_number:
        return False
    cursor = conn.execute(
        "SELECT 1 FROM sutta_master WHERE sutta_number = ? LIMIT 1",
        (sutta_number,)
    )
    return cursor.fetchone() is not None


def run_mapper(dry_run: bool = True) -> Dict:
    """Run deterministic mapping."""
    db_path = config.DB_PATH
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT source_table, english_translation 
        FROM tpk_segments 
        WHERE english_translation IS NOT NULL AND english_translation != ''
    """)
    rows = cursor.fetchall()
    conn.close()
    
    groups = group_segments(rows)
    
    conn = sqlite3.connect(db_path)
    results = []
    
    for source_table, text in groups.items():
        derived = parse_source_table(source_table)
        if not derived:
            continue
        
        exists = validate_sutta(conn, derived)
        
        if exists:
            method = "auto"
        elif is_valid_sutta_pattern(derived):
            method = "tpk_only"
        else:
            method = "invalid"
        
        results.append(MappingResult(
            source_table=source_table,
            derived_sutta=derived,
            exists_in_master=exists,
            mapped_method=method
        ))
    
    conn.close()
    
    auto_mapped = [r for r in results if r.mapped_method == "auto"]
    tpk_only = [r for r in results if r.mapped_method == "tpk_only"]
    invalid = [r for r in results if r.mapped_method == "invalid"]
    
    output = {
        "total_source_tables": len(results),
        "auto_mapped": len(auto_mapped),
        "tpk_only": len(tpk_only),
        "invalid": len(invalid),
        "results": results,
    }
    
    print(f"Total source_tables: {output['total_source_tables']}")
    print(f"Auto mapped: {output['auto_mapped']}")
    print(f"TPK-only: {output['tpk_only']}")
    print(f"Invalid: {output['invalid']}")
    print()
    
    if tpk_only:
        unique_tpk_only = sorted(set(r.derived_sutta for r in tpk_only))
        print(f"Unique TPK-only suttas ({len(unique_tpk_only)}):")
        for s in unique_tpk_only:
            print(f"  {s}")
    print()
    print("Sample mappings (source_table -> sutta -> status):")
    for r in results[:10]:
        status = "OK" if r.mapped_method == "auto" else "TPK" if r.mapped_method == "tpk_only" else "INV"
        print(f"  {r.source_table} -> {r.derived_sutta} [{status}]")
    
    return output


if __name__ == "__main__":
    run_mapper()