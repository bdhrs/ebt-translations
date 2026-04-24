"""Regex-based sutta ID detection."""

import re
from typing import List, Optional, Set
from dataclasses import dataclass


@dataclass
class SuttaMatch:
    """A regex-detected sutta match."""
    sutta_id: str
    pattern: str
    start_pos: int
    end_pos: int
    confidence: float


SUTTA_PATTERNS = [
    (r'dhp\s*(\d+)', 'dhp', 0.9),
    (r'sn\s*(\d+)[.\-]?(\d+)', 'sn', 0.95),
    (r'an\s*(\d+)[.\-]?(\d+)', 'an', 0.95),
    (r'mn\s*(\d+)', 'mn', 0.95),
    (r'dn\s*(\d+)', 'dn', 0.95),
    (r'jv\s*(\d+)', 'jv', 0.9),
    (r'ud\s*(\d+)', 'ud', 0.9),
    (r'it[iɪ]?\s*(\d+)', 'iti', 0.9),
    (r'snp\s*(\d+)', 'snp', 0.9),
    (r'thag\s*(\d+)', 'thag', 0.9),
    (r'thg\s*(\d+)', 'thg', 0.9),
    (r'vp\s*(\d+)', 'vp', 0.85),
]

COMPILED_PATTERNS = [(re.compile(p, re.IGNORECASE), abbrev, conf) for p, abbrev, conf in SUTTA_PATTERNS]


class RegexDetector:
    """Detect sutta IDs using regex patterns."""
    
    def __init__(self, patterns: List[tuple] = None):
        self.patterns = patterns or COMPILED_PATTERNS
    
    def detect(self, text: str) -> List[SuttaMatch]:
        """Detect all sutta IDs in text."""
        if not text:
            return []
        
        matches = []
        for pattern, abbrev, conf in self.patterns:
            for m in pattern.finditer(text):
                sutta_id = self._normalize_id(abbrev, m.groups())
                matches.append(SuttaMatch(
                    sutta_id=sutta_id,
                    pattern=m.group(0),
                    start_pos=m.start(),
                    end_pos=m.end(),
                    confidence=conf
                ))
        
        matches.sort(key=lambda x: x.start_pos)
        return matches
    
    def detect_best(self, text: str) -> Optional[str]:
        """Get the highest confidence sutta ID."""
        matches = self.detect(text)
        if not matches:
            return None
        
        best = max(matches, key=lambda x: x.confidence)
        return best.sutta_id
    
    def _normalize_id(self, abbrev: str, groups: tuple) -> str:
        """Normalize sutta ID from regex groups."""
        if abbrev == 'sn' and len(groups) >= 2:
            return f"sn{groups[0]}.{groups[1]}"
        elif abbrev == 'an' and len(groups) >= 2:
            return f"an{groups[0]}.{groups[1]}"
        elif abbrev == 'dhp':
            return f"dhp{groups[0]}"
        else:
            return f"{abbrev}{groups[0]}"


def extract_unique_suttas(matches: List[SuttaMatch]) -> Set[str]:
    """Extract unique sutta IDs from matches."""
    return {m.sutta_id for m in matches}