"""Database inserter for EBT ingestion pipeline.

Inserts into:
- dt_*
- ati_*
- tbw_*
- pau_*
- tpk_*

Updates source_availability.
"""

import logging
import sqlite3
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class Inserter:
    """Insert sutta data into database."""
    
    # Nikaya lookup for each sutta
    NIKAYA_MAP = {
        "dn": "dn", "mn": "mn", "sn": "sn", "an": "an", "kn": "kn",
        "dhp": "kn", "iti": "kn", "snp": "kn", "thag": "kn",
        "thig": "kn", "ud": "kn", "kp": "kn",
    }
    
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.insert_count = 0
    
    def connect(self, db_path: str):
        """Connect to database."""
        if self.conn is None:
            self.conn = sqlite3.connect(db_path)
            self.conn.row_factory = sqlite3.Row
        return self.conn
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def insert(
        self,
        source_id: str,
        sutta_number: str,
        text: str,
        title: Optional[str] = None,
        pali_text: Optional[str] = None,
    ) -> bool:
        """Insert sutta into appropriate table.
        
        Args:
            source_id: Source identifier (sc, tbw, dt, etc.)
            sutta_number: Normalized sutta number
            text: Translation text
            title: Optional title
            pali_text: Optional Pali text
            
        Returns:
            True if inserted
        """
        if not self.conn:
            logger.error("No database connection")
            return False
        
        # Determine nikaya
        nikaya = self._get_nikaya(sutta_number)
        
        # Get table name
        table_name = f"{source_id}_{nikaya}"
        
        # If KN, use source_kn
        if nikaya == "kn":
            table_name = f"{source_id}_kn"
        
        cur = self.conn.cursor()
        
        try:
            # Insert into source table
            cur.execute(f"""
                INSERT OR REPLACE INTO {table_name}
                (sutta_number, sutta_title, translation_text, pali_text, 
                 char_count, is_complete, last_updated)
                VALUES (?, ?, ?, ?, ?, 1, ?)
            """, (
                sutta_number.lower(),
                title,
                text,
                pali_text,
                len(text) + (len(pali_text) if pali_text else 0),
                datetime.now(),
            ))
            
            # Update source_availability
            has_trans = 1 if text else 0
            has_pali = 1 if pali_text else 0
            
            cur.execute("""
                INSERT OR REPLACE INTO source_availability
                (sutta_number, source_id, has_translation, has_pali, is_complete)
                VALUES (?, ?, ?, ?, 1)
            """, (sutta_number.lower(), source_id, has_trans, has_pali))
            
            self.conn.commit()
            self.insert_count += 1
            
            return True
            
        except sqlite3.IntegrityError:
            logger.debug(f"Duplicate key: {sutta_number} from {source_id}")
            return False
        except sqlite3.OperationalError as e:
            logger.error(f"Insert error: {e}")
            return False
    
    def insert_batch(
        self,
        source_id: str,
        suttas: list[dict],
    ) -> int:
        """Insert multiple suttas.
        
        Args:
            source_id: Source identifier
            suttas: List of sutta dicts
            
        Returns:
            Number inserted
        """
        inserted = 0
        
        for sutta in suttas:
            success = self.insert(
                source_id=source_id,
                sutta_number=sutta.get("sutta_number", ""),
                text=sutta.get("text", ""),
                title=sutta.get("title"),
                pali_text=sutta.get("pali_text"),
            )
            if success:
                inserted += 1
        
        return inserted
    
    def update_source_availability(
        self,
        sutta_number: str,
        source_id: str,
        has_pali: bool = False,
        has_translation: bool = True,
    ) -> bool:
        """Update source_availability record.
        
        Args:
            sutta_number: Sutta number
            source_id: Source ID
            has_pali: Has Pali text
            has_translation: Has translation
            
        Returns:
            True if updated
        """
        if not self.conn:
            return False
        
        cur = self.conn.cursor()
        
        try:
            cur.execute("""
                INSERT OR REPLACE INTO source_availability
                (sutta_number, source_id, has_pali, has_translation, is_complete)
                VALUES (?, ?, ?, ?, 1)
            """, (
                sutta_number.lower(),
                source_id,
                1 if has_pali else 0,
                1 if has_translation else 0,
            ))
            self.conn.commit()
            return True
        except sqlite3.OperationalError as e:
            logger.error(f"Update error: {e}")
            return False
    
    def _get_nikaya(self, sutta_number: str) -> str:
        """Determine nikaya from sutta number."""
        
        number = sutta_number.lower()
        
        # Check prefix
        for prefix, nikaya in self.NIKAYA_MAP.items():
            if number.startswith(prefix):
                return nikaya
        
        # Default to first nikaya found
        if number.startswith("dn"):
            return "dn"
        elif number.startswith("mn"):
            return "mn"
        elif number.startswith("sn"):
            return "sn"
        elif number.startswith("an"):
            return "an"
        
        return "kn"  # Default to KN
    
    def get_stats(self) -> dict:
        """Get insertion statistics."""
        return {
            "total_inserted": self.insert_count,
        }