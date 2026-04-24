"""Deduplication for EBT ingestion pipeline.

Prevents duplicates using:
- Primary key: (sutta_number, source_id)
- Text hash detection
- Malformed ID detection
"""

import hashlib
import logging
import re
import sqlite3
from dataclasses import dataclass
from typing import Optional, Set

logger = logging.getLogger(__name__)


@dataclass
class DeduplicationResult:
    """Result of duplicate check."""
    is_duplicate: bool
    reason: str
    existing_id: Optional[int] = None


class Deduplicator:
    """Detect and prevent duplicate insertions."""
    
    def __init__(self, db_path: Optional[sqlite3.Connection] = None):
        if isinstance(db_path, sqlite3.Connection):
            self.conn = db_path
            self._owns_connection = False
        else:
            self.conn = None
            self._owns_connection = True
        
        self._cache: Set[tuple] = set()
        self._text_hashes: dict[str, str] = {}
    
    def connect(self, db_path: str):
        """Connect to database."""
        if self.conn is None and self._owns_connection:
            self.conn = sqlite3.connect(db_path)
        return self.conn
    
    def close(self):
        """Close database connection."""
        if self.conn and self._owns_connection:
            self.conn.close()
            self.conn = None
    
    def load_existing(self, source_id: Optional[str] = None):
        """Load existing suttas into cache for comparison."""
        if not self.conn:
            return
        
        cur = self.conn.cursor()
        
        if source_id:
            # Load from specific source tables
            for nikaya in ["dn", "mn", "sn", "an", "kn"]:
                table_name = f"{source_id}_{nikaya}"
                try:
                    cur.execute(f"SELECT sutta_number FROM {table_name}")
                    for (sutta_number,) in cur.fetchall():
                        if sutta_number:
                            self._cache.add((sutta_number.lower(), source_id))
                except sqlite3.OperationalError:
                    pass
        else:
            # Load from source_availability
            try:
                cur.execute("SELECT DISTINCT sutta_number, source_id FROM source_availability")
                for sutta_number, src in cur.fetchall():
                    if sutta_number and src:
                        self._cache.add((sutta_number.lower(), src))
            except sqlite3.OperationalError:
                pass
        
        # Load text hashes
        try:
            cur.execute("""
                SELECT sutta_number, source_id, translation_text 
                FROM sc_dn UNION ALL
                SELECT sutta_number, source_id, translation_text 
                FROM tbw_dn
            """)
            for sutta_number, src, text in cur.fetchall():
                if text and sutta_number and src:
                    key = (sutta_number.lower(), src)
                    self._text_hashes[key] = self._hash_text(text)
        except sqlite3.OperationalError:
            pass
        
        logger.info(f"Loaded {len(self._cache)} existing suttas into cache")
    
    def check_duplicate(
        self,
        sutta_number: str,
        source_id: str,
        text: Optional[str] = None,
    ) -> DeduplicationResult:
        """Check if sutta would be a duplicate.
        
        Args:
            sutta_number: Normalized sutta number
            source_id: Source identifier
            text: Optional text for hash comparison
            
        Returns:
            DeduplicationResult
        """
        key = (sutta_number.lower(), source_id)
        
        # Check by key
        if key in self._cache:
            return DeduplicationResult(
                is_duplicate=True,
                reason="duplicate_key",
            )
        
        # Check by text hash
        if text:
            text_hash = self._hash_text(text)
            for existing_hash in self._text_hashes.values():
                if text_hash == existing_hash:
                    return DeduplicationResult(
                        is_duplicate=True,
                        reason="duplicate_text",
                    )
        
        return DeduplicationResult(
            is_duplicate=False,
            reason="unique",
        )
    
    def check_db_duplicate(
        self,
        sutta_number: str,
        source_id: str,
    ) -> bool:
        """Check if sutta already exists in database.
        
        Args:
            sutta_number: Sutta number
            source_id: Source identifier
            
        Returns:
            True if duplicate
        """
        if not self.conn:
            return False
        
        cur = self.conn.cursor()
        
        try:
            # Check source_availability first
            cur.execute("""
                SELECT 1 FROM source_availability 
                WHERE sutta_number = ? AND source_id = ?
            """, (sutta_number.lower(), source_id))
            if cur.fetchone():
                return True
            
            # Check source tables
            for nikaya in ["dn", "mn", "sn", "an", "kn"]:
                table = f"{source_id}_{nikaya}"
                try:
                    cur.execute(f"""
                        SELECT 1 FROM {table} 
                        WHERE sutta_number = ?
                    """, (sutta_number.lower(),))
                    if cur.fetchone():
                        return True
                except sqlite3.OperationalError:
                    pass
        except sqlite3.OperationalError:
            pass
        
        return False
    
    def is_malformed_id(self, sutta_id: str) -> bool:
        """Check if sutta ID is malformed.
        
        Args:
            sutta_id: Sutta ID to check
            
        Returns:
            True if malformed
        """
        if not sutta_id:
            return True
        
        clean = sutta_id.strip().lower()
        
        # Valid patterns
        valid_patterns = [
            r"^dn\d+$",           # dn1, dn2, dn34
            r"^mn\d+$",           # mn1, mn2, dn152
            r"^sn\d+\.\d+$",      # sn1.1, sn22.85
            r"^an\d+\.\d+$",      # an1.1, an10.176
            r"^dhp\d+$",          # dhp1
            r"^iti\d+$",          # iti1
            r"^snp\d+$",         # snp1
            r"^thag\d+$",        # thag1
            r"^thig\d+$",        # thig1
            r"^ud\d+$",          # ud1
            r"^kp\d+$",          # kp1
        ]
        
        for pattern in valid_patterns:
            if re.match(pattern, clean):
                return False
        
        return True
    
    def find_duplicates(
        self,
        suttas: list[dict],
    ) -> list[dict]:
        """Find duplicates in a list of suttas.
        
        Args:
            suttas: List of dicts with 'sutta_number' and 'source_id'
            
        Returns:
            List of duplicate records
        """
        seen: dict[tuple, list[int]] = {}
        duplicates = []
        
        for i, sutta in enumerate(suttas):
            key = (
                sutta.get("sutta_number", "").lower(),
                sutta.get("source_id", "").lower()
            )
            
            if key[0] and key[1]:
                if key in seen:
                    seen[key].append(i)
                else:
                    seen[key] = [i]
        
        for key, indices in seen.items():
            if len(indices) > 1:
                for i in indices:
                    duplicates.append({
                        "sutta_number": key[0],
                        "source_id": key[1],
                        "index": i,
                    })
        
        return duplicates
    
    def _hash_text(self, text: str) -> str:
        """Create hash of text content."""
        if not text:
            return ""
        
        # Normalize whitespace
        normalized = " ".join(text.split())
        
        return hashlib.md5(normalized.encode("utf-8")).hexdigest()
    
    def get_stats(self) -> dict:
        """Get deduplication statistics."""
        return {
            "cached_suttas": len(self._cache),
            "text_hashes": len(self._text_hashes),
        }