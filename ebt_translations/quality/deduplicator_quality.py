"""Quality deduplicator for EBT quality pipeline.

Detect:
- exact duplicates → hash(text)
- near duplicates → similarity (optional)

Rules:
- same text across sources → keep one canonical version
- duplicates in same source → remove
"""

import hashlib
import logging
import re
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class DuplicateResult:
    """Duplicate detection result."""
    is_duplicate: bool
    duplicate_of: Optional[str] = None
    similarity: Optional[float] = None
    reason: str = ""


class QualityDeduplicator:
    """Detect and handle duplicates in quality pipeline."""
    
    def __init__(self, similarity_threshold: float = 0.95):
        self.similarity_threshold = similarity_threshold
        self._text_hashes: dict[str, str] = {}
        self._exact_matches: dict[str, list[tuple]] = {}  # hash -> [(source, sutta)]
        self._stats = {
            "exact_duplicates": 0,
            "near_duplicates": 0,
            "unique_texts": 0,
        }
    
    def register(self, sutta_number: str, source_id: str, text: str):
        """Register a text for duplicate checking."""
        normalized = self._normalize_for_hash(text)
        text_hash = self._compute_hash(normalized)
        
        if text_hash not in self._exact_matches:
            self._exact_matches[text_hash] = []
            self._stats["unique_texts"] += 1
        
        self._exact_matches[text_hash].append((source_id, sutta_number))
        self._text_hashes[f"{source_id}:{sutta_number}"] = text_hash
    
    def check(self, sutta_number: str, source_id: str, text: str) -> DuplicateResult:
        """Check if text is duplicate."""
        normalized = self._normalize_for_hash(text)
        text_hash = self._compute_hash(normalized)
        
        if text_hash in self._exact_matches:
            existing = self._exact_matches[text_hash]
            
            if len(existing) > 1:
                self._stats["exact_duplicates"] += 1
                
                # Find original
                for src, sutta in existing:
                    if src != source_id or sutta != sutta_number:
                        return DuplicateResult(
                            is_duplicate=True,
                            duplicate_of=f"{src}:{sutta}",
                            reason="exact_match",
                        )
        
        return DuplicateResult(is_duplicate=False, reason="unique")
    
    def check_exact(self, text1: str, text2: str) -> bool:
        """Check if two texts are exact duplicates."""
        norm1 = self._normalize_for_hash(text1)
        norm2 = self._normalize_for_hash(text2)
        
        return self._compute_hash(norm1) == self._compute_hash(norm2)
    
    def compute_similarity(self, text1: str, text2: str) -> float:
        """Compute similarity between two texts (Jaccard)."""
        if not text1 or not text2:
            return 0.0
        
        # Tokenize
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())
        
        if not words1 or not words2:
            return 0.0
        
        # Jaccard similarity
        intersection = len(words1 & words2)
        union = len(words1 | words2)
        
        return intersection / union if union > 0 else 0.0
    
    def check_near_duplicate(
        self,
        sutta_number: str,
        source_id: str,
        text: str,
    ) -> Optional[DuplicateResult]:
        """Check for near duplicates using similarity."""
        normalized = self._normalize_for_hash(text)
        
        # Compare with existing
        for key, existing_hash in self._text_hashes.items():
            if key.startswith(f"{source_id}:"):
                continue
            
            # This is a simplified check - full near-duplicate needs index
            text2 = self._get_text_for_hash(existing_hash)
            if text2:
                sim = self.compute_similarity(normalized, text2)
                if sim >= self.similarity_threshold:
                    self._stats["near_duplicates"] += 1
                    return DuplicateResult(
                        is_duplicate=True,
                        similarity=sim,
                        reason="near_match",
                    )
        
        return None
    
    def deduplicate_batch(
        self,
        translations: list[dict],
    ) -> list[dict]:
        """Remove duplicates from list of translations.
        
        Args:
            translations: [{"sutta_number", "source_id", "text"}, ...]
            
        Returns:
            Deduplicated list
        """
        seen_hashes: dict[str, dict] = {}  # hash -> translation
        unique = []
        
        for trans in translations:
            text = trans.get("text", "")
            if not text:
                continue
            
            normalized = self._normalize_for_hash(text)
            text_hash = self._compute_hash(normalized)
            
            if text_hash in seen_hashes:
                existing = seen_hashes[text_hash]
                
                # Keep the one with better source priority
                better = self._choose_better(existing, trans)
                if better is trans:
                    seen_hashes[text_hash] = trans
            else:
                seen_hashes[text_hash] = trans
                unique.append(trans)
        
        self._stats["exact_duplicates"] = len(translations) - len(unique)
        
        return unique
    
    def _choose_better(self, trans1: dict, trans2: dict) -> dict:
        """Choose better translation (higher quality source)."""
        priority = {"sc": 3, "tbw": 2, "dt": 1, "ati": 1, "tpk": 1, "pau": 1}
        
        src1 = trans1.get("source_id", "")
        src2 = trans2.get("source_id", "")
        
        p1 = priority.get(src1, 0)
        p2 = priority.get(src2, 0)
        
        if p1 > p2:
            return trans1
        elif p2 > p1:
            return trans2
        else:
            # Keep longer
            return trans1 if len(trans1.get("text", "")) > len(trans2.get("text", "")) else trans2
    
    def _normalize_for_hash(self, text: str) -> str:
        """Normalize text for hashing."""
        if not text:
            return ""
        
        # Lowercase
        text = text.lower()
        
        # Normalize whitespace
        text = " ".join(text.split())
        
        # Remove punctuation for comparison
        text = re.sub(r"[^\w\s]", "", text)
        
        return text.strip()
    
    def _compute_hash(self, text: str) -> str:
        """Compute hash of text."""
        return hashlib.md5(text.encode("utf-8")).hexdigest()
    
    def _get_text_for_hash(self, text_hash: str) -> str:
        """Get text for hash (simplified)."""
        for key, h in self._text_hashes.items():
            if h == text_hash:
                return key
        return ""
    
    def get_stats(self) -> dict:
        """Get deduplication statistics."""
        return self._stats.copy()
    
    def get_duplicate_groups(self) -> dict[str, list[tuple]]:
        """Get groups of duplicate texts."""
        groups = {}
        
        for text_hash, entries in self._exact_matches.items():
            if len(entries) > 1:
                groups[text_hash] = entries
        
        return groups


def remove_exact_duplicates(translations: list[dict]) -> list[dict]:
    """Quick function to remove duplicates."""
    dedup = QualityDeduplicator()
    return dedup.deduplicate_batch(translations)