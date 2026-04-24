"""Validation for EBT ingestion pipeline.

Validates before insert:
- Sutta exists in sutta_master
- Text length > threshold
- No empty or junk HTML
"""

import logging
import re
import sqlite3
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of validation check."""
    is_valid: bool
    reason: str
    issues: list[str] = None
    
    def __post_init__(self):
        if self.issues is None:
            self.issues = []


class Validator:
    """Validate sutta data before insertion."""
    
    MIN_TEXT_LENGTH = 50
    MIN_WORDS = 10
    
    # Junk patterns that indicate invalid content
    JUNK_PATTERNS = [
        r"^[\s\n]*$",
        r"^<p>\s*</p>$",
        r"^[\s]+$",
    ]
    
    def __init__(
        self,
        db_path: Optional[str] = None,
        min_length: int = MIN_TEXT_LENGTH,
    ):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.min_length = min_length
        self._master_suttas: set = set()
    
    def connect(self, db_path: str):
        """Connect to database."""
        if self.conn is None:
            self.conn = sqlite3.connect(db_path)
        return self.conn
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def load_master_suttas(self):
        """Load sutta_master for validation."""
        if not self.conn:
            return
        
        cur = self.conn.cursor()
        
        try:
            cur.execute("SELECT sutta_number FROM sutta_master")
            self._master_suttas = {row[0].lower() for row in cur.fetchall() if row[0]}
        except sqlite3.OperationalError as e:
            logger.warning(f"Could not load sutta_master: {e}")
    
    def validate(
        self,
        sutta_number: str,
        text: str,
    ) -> ValidationResult:
        """Validate sutta data.
        
        Args:
            sutta_number: Normalized sutta number
            text: Text content to validate
            
        Returns:
            ValidationResult
        """
        issues = []
        
        # Check text length
        if not text or len(text.strip()) < self.min_length:
            issues.append(f"text_too_short ({len(text) if text else 0} chars)")
        
        # Check word count
        if text:
            word_count = len(text.split())
            if word_count < self.MIN_WORDS:
                issues.append(f"word_count_too_low ({word_count} words)")
        
        # Check for junk content
        if self._is_junk(text):
            issues.append("junk_content")
        
        # Check for HTML junk
        if self._is_html_junk(text):
            issues.append("html_junk")
        
        # Check sutta_master (if loaded)
        if self._master_suttas and sutta_number.lower() not in self._master_suttas:
            issues.append("not_in_master")
        
        is_valid = len(issues) == 0
        
        return ValidationResult(
            is_valid=is_valid,
            reason="valid" if is_valid else "; ".join(issues),
            issues=issues,
        )
    
    def validate_batch(
        self,
        suttas: list[dict],
    ) -> list[ValidationResult]:
        """Validate multiple suttas."""
        
        results = []
        for sutta in suttas:
            result = self.validate(
                sutta.get("sutta_number", ""),
                sutta.get("text", ""),
            )
            results.append(result)
        
        return results
    
    def _is_junk(self, text: str) -> bool:
        """Check if text is junk/empty."""
        if not text:
            return True
        
        for pattern in self.JUNK_PATTERNS:
            if re.match(pattern, text.strip()):
                return True
        
        return False
    
    def _is_html_junk(self, text: str) -> bool:
        """Check if HTML content is junk."""
        if not text or not "<" in text:
            return False
        
        # Check for empty tags
        empty_tag_patterns = [
            r"<p>\s*</p>",
            r"<div>\s*</div>",
            r"<span>\s*</span>",
            r"<br\s*/>",
        ]
        
        for pattern in empty_tag_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        
        # Check if mostly HTML tags with no text
        if len(re.sub(r"<[^>]+>", "", text)) < self.min_length:
            return True
        
        return False
    
    def check_master_exists(self, sutta_number: str) -> bool:
        """Check if sutta exists in sutta_master."""
        
        if self._master_suttas:
            return sutta_number.lower() in self._master_suttas
        
        if not self.conn:
            return True  # Allow if no DB connected
        
        cur = self.conn.cursor()
        
        try:
            cur.execute(
                "SELECT 1 FROM sutta_master WHERE sutta_number = ?",
                (sutta_number,)
            )
            return cur.fetchone() is not None
        except sqlite3.OperationalError:
            return True  # Allow if table doesn't exist
    
    def get_stats(self) -> dict:
        """Get validation statistics."""
        return {
            "master_suttas_loaded": len(self._master_suttas),
            "min_text_length": self.min_length,
        }