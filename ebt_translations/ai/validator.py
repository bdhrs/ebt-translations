"""Validate sutta IDs against sutta_master table."""

import sqlite3
from typing import List, Set, Dict, Optional
from dataclasses import dataclass

from . import config
from .aggregator import SuttaMapping


@dataclass
class ValidationResult:
    """Result of validating a sutta ID."""
    sutta_id: str
    is_valid: bool
    exists_in_master: bool


class Validator:
    """Validate detected suttas against sutta_master."""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or config.DB_PATH
    
    def validate(self, sutta_id: str) -> ValidationResult:
        """Validate a single sutta ID."""
        exists = self._exists_in_master(sutta_id)
        return ValidationResult(
            sutta_id=sutta_id,
            is_valid=exists,
            exists_in_master=exists
        )
    
    def validate_batch(self, sutta_ids: List[str]) -> Dict[str, ValidationResult]:
        """Validate multiple sutta IDs."""
        results = {}
        for sid in sutta_ids:
            results[sid] = self.validate(sid)
        return results
    
    def validate_mappings(self, mappings: List[SuttaMapping]) -> List[SuttaMapping]:
        """Filter mappings to only valid suttas."""
        valid_suttas = self._get_all_valid_suttas()
        return [m for m in mappings if m.sutta_id in valid_suttas]
    
    def _exists_in_master(self, sutta_id: str) -> bool:
        """Check if sutta exists in sutta_master."""
        normalized = self._normalize_id(sutta_id)
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute(
                "SELECT 1 FROM sutta_master WHERE sutta_number = ? LIMIT 1",
                (normalized,)
            )
            return cursor.fetchone() is not None
        finally:
            conn.close()
    
    def _get_all_valid_suttas(self) -> Set[str]:
        """Get all valid sutta IDs from master."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute("SELECT sutta_number FROM sutta_master")
            return {row[0] for row in cursor.fetchall()}
        finally:
            conn.close()
    
    def _normalize_id(self, sutta_id: str) -> str:
        """Normalize sutta ID format."""
        sutta_id = sutta_id.lower().strip()
        return sutta_id


def load_sutta_master(db_path: str = None) -> Set[str]:
    """Load all sutta IDs from master table."""
    validator = Validator(db_path)
    return validator._get_all_valid_suttas()