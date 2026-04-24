"""Main AI pipeline for sutta extraction."""

import sqlite3
from typing import List, Dict, Optional
from dataclasses import asdict

from . import config
from .chunker import TextChunker, TextChunk
from .regex_detector import RegexDetector
from .ai_detector import AIDetector
from .aggregator import Aggregator, SuttaMapping
from .validator import Validator


def run_ai_pipeline(limit: int = 100, dry_run: bool = True) -> List[Dict]:
    """Run the AI pipeline on unmapped segments.
    
    Args:
        limit: Number of segments to process
        dry_run: If True, don't write to DB
    
    Returns:
        List of detected mappings
    """
    conn = sqlite3.connect(config.DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, source_table, segment_text, sutta_number, mapped
        FROM tpk_segments 
        WHERE mapped = 0 OR mapped IS NULL
        LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        print("No unmapped segments found")
        return []
    
    print(f"Processing {len(rows)} segments...")
    
    chunker = TextChunker()
    regex_detector = RegexDetector()
    ai_detector = AIDetector()
    aggregator = Aggregator()
    validator = Validator()
    
    all_chunks = []
    for row in rows:
        segment_id, source_table, text = row[0], row[1], row[2]
        chunks = chunker.chunkify(text, source_table, segment_id)
        all_chunks.extend(chunks)
    
    print(f"Created {len(all_chunks)} chunks")
    
    regex_matches = {}
    for chunk in all_chunks:
        matches = regex_detector.detect(chunk.text)
        if matches:
            regex_matches[chunk.segment_id] = matches
    
    print(f"Regex detected in {len(regex_matches)} segments")
    
    ai_detections = {}
    detected_count = 0
    for chunk in all_chunks:
        if chunk.segment_id not in regex_matches:
            detected = ai_detector.detect(chunk)
            ai_detections[chunk.segment_id] = detected
            if detected:
                detected_count += 1
    
    print(f"AI detected in {detected_count} segments")
    
    mappings = aggregator.aggregate(all_chunks, regex_matches, ai_detections)
    print(f"Aggregated to {len(mappings)} mappings")
    
    valid_mappings = validator.validate_mappings(mappings)
    print(f"Valid mappings: {len(valid_mappings)}")
    
    if not dry_run:
        conn = sqlite3.connect(config.DB_PATH)
        cursor = conn.cursor()
        for m in valid_mappings:
            cursor.execute("""
                UPDATE tpk_segments 
                SET sutta_number = ?, mapped = 1
                WHERE id = ?
            """, (m.sutta_id, m.segment_id))
        conn.commit()
        conn.close()
        print("Wrote mappings to DB")
    
    return [asdict(m) for m in valid_mappings]


def check_ollama() -> bool:
    """Check if Ollama is available."""
    from .ollama_client import OllamaClient
    client = OllamaClient()
    return client.is_available()


if __name__ == "__main__":
    available = check_ollama()
    print(f"Ollama available: {available}")
    
    results = run_ai_pipeline(limit=10)
    print(f"Results: {results}")