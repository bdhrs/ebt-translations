"""Aggregate chunk-level detections into sutta-level mappings."""

from typing import List, Dict, Set, Optional
from collections import defaultdict
from dataclasses import dataclass

from .chunker import TextChunk
from .regex_detector import SuttaMatch


@dataclass
class SuttaMapping:
    """A mapping from source segment to sutta."""
    sutta_id: str
    source_table: str
    segment_id: int
    detection_method: str
    confidence: float


class Aggregator:
    """Aggregate chunk detections into final sutta mappings."""
    
    def __init__(self, confidence_threshold: float = 0.8):
        self.confidence_threshold = confidence_threshold
    
    def aggregate(
        self, 
        chunks: List[TextChunk], 
        regex_matches: Dict[int, List[SuttaMatch]],
        ai_detections: Dict[int, Optional[str]]
    ) -> List[SuttaMapping]:
        """Aggregate chunk-level detections into sutta mappings."""
        mappings = []
        
        for chunk in chunks:
            regex_detected = regex_matches.get(chunk.segment_id, [])
            ai_detected = ai_detections.get(chunk.segment_id)
            
            best_sutta, method, confidence = self._select_best(
                chunk, regex_detected, ai_detected
            )
            
            if best_sutta:
                mappings.append(SuttaMapping(
                    sutta_id=best_sutta,
                    source_table=chunk.source_table,
                    segment_id=chunk.segment_id,
                    detection_method=method,
                    confidence=confidence
                ))
        
        return mappings
    
    def group_by_sutta(self, mappings: List[SuttaMapping]) -> Dict[str, List[SuttaMapping]]:
        """Group mappings by sutta ID."""
        groups = defaultdict(list)
        for m in mappings:
            groups[m.sutta_id].append(m)
        return dict(groups)
    
    def _select_best(
        self, 
        chunk: TextChunk, 
        regex_matches: List[SuttaMatch],
        ai_detected: Optional[str]
    ) -> tuple:
        """Select best sutta ID from regex and AI detections."""
        if regex_matches:
            best = max(regex_matches, key=lambda x: x.confidence)
            if best.confidence >= self.confidence_threshold:
                return best.sutta_id, "regex", best.confidence
        
        if ai_detected:
            return ai_detected, "ai", 0.7
        
        if regex_matches:
            best = max(regex_matches, key=lambda x: x.confidence)
            return best.sutta_id, "regex_fallback", best.confidence
        
        return None, "none", 0.0


def create_source_table_map(chunks: List[TextChunk]) -> Dict[str, Set[int]]:
    """Create mapping from source table to segment IDs."""
    table_map = defaultdict(set)
    for chunk in chunks:
        table_map[chunk.source_table].add(chunk.segment_id)
    return dict(table_map)