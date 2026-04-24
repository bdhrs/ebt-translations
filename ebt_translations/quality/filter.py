"""Filtering module for EBT quality pipeline.

Remove:
- empty texts
- very short texts (<200 chars)
- malformed IDs
- low-quality translations
"""

import logging
import re
import sqlite3
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class FilterResult:
    """Filter result."""
    kept: bool
    reason: str


class QualityFilter:
    """Filter translations based on quality criteria."""
    
    MIN_TEXT_LENGTH = 200
    MIN_WORD_COUNT = 30
    
    def __init__(self, min_length: int = MIN_TEXT_LENGTH):
        self.min_length = min_length
        self.stats = {
            "empty_removed": 0,
            "short_removed": 0,
            "malformed_removed": 0,
            "low_quality_removed": 0,
            "kept": 0,
        }
    
    def filter(
        self,
        sutta_number: str,
        text: str,
        score: Optional[float] = None,
    ) -> FilterResult:
        """Filter a translation.
        
        Args:
            sutta_number: Sutta number
            text: Translation text
            score: Optional quality score
            
        Returns:
            FilterResult
        """
        # Check empty
        if not text or not text.strip():
            self.stats["empty_removed"] += 1
            return FilterResult(kept=False, reason="empty_text")
        
        # Check too short
        if len(text) < self.min_length:
            self.stats["short_removed"] += 1
            return FilterResult(kept=False, reason="too_short")
        
        # Check word count
        word_count = len(text.split())
        if word_count < self.MIN_WORD_COUNT:
            self.stats["short_removed"] += 1
            return FilterResult(kept=False, reason="too_few_words")
        
        # Check malformed ID
        if self._is_malformed_id(sutta_number):
            self.stats["malformed_removed"] += 1
            return FilterResult(kept=False, reason="malformed_id")
        
        # Check low quality (if score provided)
        if score is not None and score < 30:
            self.stats["low_quality_removed"] += 1
            return FilterResult(kept=False, reason="low_quality")
        
        self.stats["kept"] += 1
        return FilterResult(kept=True, reason="passed")
    
    def _is_malformed_id(self, sutta_id: str) -> bool:
        """Check if sutta ID is malformed."""
        if not sutta_id:
            return True
        
        clean = sutta_id.strip().lower()
        
        valid_patterns = [
            r"^dn\d+$",
            r"^mn\d+$", 
            r"^sn\d+\.\d+$",
            r"^an\d+\.\d+$",
            r"^dhp\d+$",
            r"^iti\d+$",
            r"^snp\d+$",
            r"^thag\d+$",
            r"^thig\d+$",
            r"^ud\d+$",
            r"^kp\d+$",
        ]
        
        for pattern in valid_patterns:
            if re.match(pattern, clean):
                return False
        
        return True
    
    def filter_batch(
        self,
        translations: list[dict],
    ) -> list[dict]:
        """Filter multiple translations.
        
        Args:
            translations: [{"sutta_number", "source_id", "text", "score?"}, ...]
            
        Returns:
            Filtered list
        """
        filtered = []
        
        for trans in translations:
            result = self.filter(
                sutta_number=trans.get("sutta_number", ""),
                text=trans.get("text", ""),
                score=trans.get("score"),
            )
            
            if result.kept:
                filtered.append(trans)
        
        return filtered
    
    def get_stats(self) -> dict:
        """Get filtering statistics."""
        total = sum(self.stats.values())
        
        return {
            **self.stats,
            "total": total,
            "keep_rate": round(self.stats["kept"] / max(1, total) * 100, 1),
        }


class NikayaFilter:
    """Filter by nikaya."""
    
    NIKAYA_MAP = {
        "dn": ["dn"],
        "mn": ["mn"],
        "sn": ["sn"],
        "an": ["an"],
        "kn": ["dhp", "iti", "snp", "thag", "thig", "ud", "kp"],
    }
    
    def filter_by_nikaya(
        self,
        translations: list[dict],
        nikaya: str,
    ) -> list[dict]:
        """Filter translations by nikaya."""
        allowed = self.NIKAYA_MAP.get(nikaya, [])
        
        filtered = []
        for trans in translations:
            sutta = trans.get("sutta_number", "").lower()
            if any(sutta.startswith(prefix) for prefix in allowed):
                filtered.append(trans)
        
        return filtered
    
    def split_by_nikaya(
        self,
        translations: list[dict],
    ) -> dict[str, list[dict]]:
        """Split translations by nikaya."""
        result = {nikaya: [] for nikaya in self.NIKAYA_MAP}
        
        for trans in translations:
            sutta = trans.get("sutta_number", "").lower()
            
            for nikaya, prefixes in self.NIKAYA_MAP.items():
                if any(sutta.startswith(p) for p in prefixes):
                    result[nikaya].append(trans)
                    break
        
        return result


def filter_translations(translations: list[dict]) -> list[dict]:
    """Quick filter function."""
    filt = QualityFilter()
    return filt.filter_batch(translations)