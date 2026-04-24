"""Unified query for all sources per sutta."""

import sqlite3
from typing import Dict, List, Optional


SOURCE_PRIORITY = ["sc", "tbw", "dt", "ati", "tpk"]


def get_unified_sutta(
    db_path: str, 
    sutta_number: str, 
    max_chars: int = 500,
    include_full_text: bool = False
) -> Dict:
    """Get all available text for a sutta from all sources.
    
    Args:
        db_path: Path to SQLite database
        sutta_number: Suttas ID (e.g., "sn1.1")
        max_chars: Maximum chars for text preview
        include_full_text: Include full text (can be large)
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    result = {
        "sutta_number": sutta_number,
        "sources": {},
        "references": []
    }
    
    # Get nikaya from sutta_master
    cursor.execute(
        "SELECT nikaya FROM sutta_master WHERE sutta_number = ?", 
        (sutta_number,)
    )
    row = cursor.fetchone()
    result["nikaya"] = row[0] if row else None
    
    # Map nikaya to table suffix
    nikaya_to_table = {
        "sn": "sn", "an": "an", "dn": "dn", "mn": "mn", "kn": "kn"
    }
    suffix = nikaya_to_table.get(result["nikaya"], "sn")
    
    # Get text from each source (in priority order)
    for src in SOURCE_PRIORITY:
        table = f"{src}_{suffix}"
        
        try:
            cursor.execute(f"""
                SELECT translation_text, sutta_title 
                FROM {table} 
                WHERE sutta_number = ? 
                LIMIT 1
            """, (sutta_number,))
            row = cursor.fetchone()
            if row and row[0]:
                full_text = row[0]
                is_truncated = len(full_text) > 10000
                
                # Create preview (max_chars chars)
                text_preview = full_text[:max_chars]
                if len(full_text) > max_chars:
                    text_preview += "..."
                
                # Build source data
                source_data = {
                    "text": text_preview,
                    "title": row[1],
                    "is_truncated": is_truncated,
                    "char_count": len(full_text)
                }
                
                # Optionally include full text
                if include_full_text:
                    source_data["full_text"] = full_text
                
                result["sources"][src] = source_data
        except sqlite3.OperationalError:
            pass
    
    # Get references from TPK (deduplicated)
    cursor.execute("""
        SELECT DISTINCT referenced_sutta, reference_type
        FROM tpk_references
        WHERE source_sutta = ?
    """, (sutta_number,))
    refs = cursor.fetchall()
    
    # Set-based deduplication
    ref_seen = set()
    for ref_sutta, ref_type in refs:
        if ref_sutta not in ref_seen:
            ref_seen.add(ref_sutta)
            result["references"].append({
                "sutta": ref_sutta,
                "type": ref_type
            })
    
    # Add available sources list
    result["available_sources"] = list(result["sources"].keys())
    
    # Add best source (highest priority with text)
    result["best_source"] = None
    result["best_source_text"] = None
    result["best_source_full_text"] = None
    
    for src in SOURCE_PRIORITY:
        if src in result["sources"]:
            result["best_source"] = src
            result["best_source_text"] = result["sources"][src]["text"]
            if include_full_text and "full_text" in result["sources"][src]:
                result["best_source_full_text"] = result["sources"][src]["full_text"]
            break
    
    conn.close()
    return result


def query_all_suttas(db_path: str, limit: int = None) -> List[Dict]:
    """Query all suttas with unified data."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT sutta_number FROM sutta_master ORDER BY sutta_number")
    suttas = [row[0] for row in cursor.fetchall()]
    
    if limit:
        suttas = suttas[:limit]
    
    conn.close()
    
    results = []
    for sutta in suttas:
        results.append(get_unified_sutta(db_path, sutta))
    
    return results


def print_unified(db_path: str, sutta_number: str):
    """Print unified sutta data in readable format."""
    data = get_unified_sutta(db_path, sutta_number)
    
    print("")
    print("=" * 60)
    print(f"SUTTA: {data['sutta_number']} ({data['nikaya']})")
    print("=" * 60)
    
    print("\nAVAILABLE SOURCES:", ", ".join(data["available_sources"]))
    print("BEST SOURCE:", data["best_source"])
    
    print("\nSOURCE PREVIEWS:")
    for src in SOURCE_PRIORITY:
        if src in data["sources"]:
            info = data["sources"][src]
            preview = info["text"][:100] + "..." if len(info["text"]) > 100 else info["text"]
            print(f"  {src.upper()}: {info['char_count']} chars" +
                  (info["is_truncated"] and " [truncated]" or ""))
            print(f"    {preview}")
    
    print(f"\nREFERENCES ({len(data['references'])}):")
    for ref in data["references"][:10]:
        print(f"  {ref['sutta']} [{ref['type']}]")
    if len(data['references']) > 10:
        print(f"  ... and {len(data['references']) - 10} more")
    
    return data


if __name__ == "__main__":
    db_path = "data/db/EBT_Unified (1).db"
    
    # Print sample suttas
    for sutta in ["sn1.1", "mn1", "dn1"]:
        print_unified(db_path, sutta)