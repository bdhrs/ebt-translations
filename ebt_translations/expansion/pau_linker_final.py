"""PAU Linker Final for EBT.

Links PAU translations into EBT_Unified database.

Process:
1. Load data from pau_an, pau_sn  
2. Map sequential IDs (e247 → an247, s56 → sn56)
3. Validate against sutta_master
4. Insert translations
5. Update source_availability
6. Track operations
"""

import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class PauLinker:
    """Link PAU translations to EBT database."""

    SOURCE_ID = "pau"
    MIN_TEXT_LENGTH = 200

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.conn: Optional[sqlite3.Connection] = None
        self.stats = {
            "processed": 0,
            "inserted": 0,
            "skipped": 0,
            "failed": 0,
            "duplicates": 0,
            "not_found": 0,
            "text_short": 0,
        }

    def connect(self):
        if self.conn is None:
            self.conn = sqlite3.connect(str(self.db_path))
            self.conn.row_factory = sqlite3.Row
        return self.conn

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def run(self, limit: int = 0, dry_run: bool = False) -> dict:
        """Run PAU linking."""
        conn = self.connect()
        cur = conn.cursor()

        coverage_before = self._get_coverage(cur)

        print("Processing pau_an...")
        self._process_table("pau_an", "an", cur, dry_run, limit)
        
        print("Processing pau_sn...")
        self._process_table("pau_sn", "sn", cur, dry_run, limit)

        coverage_after = self._get_coverage(cur)

        result = {
            **self.stats,
            "coverage_before": coverage_before,
            "coverage_after": coverage_after,
        }

        return result

    def _process_table(
        self,
        table: str,
        nikaya: str,
        cur: sqlite3.Cursor,
        dry_run: bool,
        limit: int,
    ):
        """Process all rows in a PAU table."""
        prefix = "e" if nikaya == "an" else "s"
        
        # First, get entries that already have standard IDs
        cur.execute(f"""
            SELECT sutta_number, translation_text, char_count
            FROM {table}
            WHERE translation_text IS NOT NULL
            AND LENGTH(translation_text) > ?
            AND sutta_number LIKE '{nikaya}%'
            ORDER BY char_count DESC
        """, (self.MIN_TEXT_LENGTH,))

        for row in cur.fetchall():
            raw_id = row[0]
            text = row[1]
            
            self.stats["processed"] += 1
            
            if limit and self.stats["processed"] >= limit:
                break

            # Already in standard format
            mapped_id = raw_id.strip().lower()

            # Verify exists in sutta_master
            cur.execute("SELECT 1 FROM sutta_master WHERE sutta_number = ?", (mapped_id,))
            if not cur.fetchone():
                self.stats["not_found"] += 1
                continue

            # Check duplicate
            cur.execute("""
                SELECT 1 FROM source_availability 
                WHERE sutta_number = ? AND source_id = ?
            """, (mapped_id, self.SOURCE_ID))
            if cur.fetchone():
                self.stats["duplicates"] += 1
                continue

            # Insert
            if dry_run:
                self.stats["inserted"] += 1
                continue

            try:
                target_table = f"pau_{nikaya}"
                cur.execute(f"""
                    INSERT OR REPLACE INTO {target_table}
                    (sutta_number, translation_text, char_count, is_complete, last_updated)
                    VALUES (?, ?, ?, 1, ?)
                """, (mapped_id, text, len(text), datetime.now().isoformat()))

                cur.execute("""
                    INSERT OR IGNORE INTO source_availability
                    (sutta_number, source_id, has_translation, is_complete, coverage_type)
                    VALUES (?, ?, 1, 1, 'full')
                """, (mapped_id, self.SOURCE_ID))

                self.conn.commit()
                self.stats["inserted"] += 1

            except Exception as e:
                logger.debug(f"Insert error: {e}")
                self.stats["failed"] += 1

    def _map_sequential_id(
        self,
        raw_id: str,
        nikaya: str,
        prefix: str,
    ) -> Optional[str]:
        """Map sequential PAU ID to EBT format."""
        if not raw_id:
            return None

        raw = raw_id.strip().lower()
        if not raw.startswith(prefix):
            return None

        try:
            num = int(raw[1:])
            return f"{nikaya}{num}"
        except ValueError:
            return None

    def _normalize_sutta_id(self, raw_id: str) -> str:
        """Normalize sutta ID."""
        s = raw_id.strip().lower()
        s = re.sub(r'\s+', '', s)
        s = re.sub(r'^0+', '', s)
        return s

    def _get_nikaya(self, sutta_number: str) -> str:
        """Get nikaya code."""
        s = sutta_number.lower()
        if s.startswith("dn"):
            return "dn"
        elif s.startswith("mn"):
            return "mn"
        elif s.startswith("sn"):
            return "sn"
        elif s.startswith("an"):
            return "an"
        return "kn"

    def _get_coverage(self, cur: sqlite3.Cursor) -> dict:
        """Get PAU coverage."""
        cur.execute("""
            SELECT COUNT(DISTINCT sutta_number) FROM source_availability
            WHERE source_id = ? AND has_translation = 1
        """, (self.SOURCE_ID,))
        return {"pau": cur.fetchone()[0] or 0}


def run_pau_linker(db_path: str, limit: int = 0, dry_run: bool = False) -> dict:
    """Run PAU linker."""
    linker = PauLinker(db_path)
    result = linker.run(limit=limit, dry_run=dry_run)
    linker.close()
    return result