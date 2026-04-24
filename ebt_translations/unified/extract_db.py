"""Database Extractor - Extract translations from unified DB for SC, DT, ATI."""

import logging
import sqlite3
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class DBExtractor:
    """Extract translations from SQLite database."""

    SOURCES = {
        "sc": ["dn", "mn", "sn", "an", "kn"],
        "dt": ["dn", "mn", "sn"],
        "ati": ["dn", "mn", "sn", "an", "kn"],
    }

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._extracted = []
        self._stats = {"sc": 0, "dt": 0, "ati": 0}

    def extract(self, source: str) -> list[dict]:
        """Extract translations for a specific source."""
        self._extracted = []
        
        if source not in self.SOURCES:
            logger.warning(f"Unknown source: {source}")
            return self._extracted
        
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()
        
        for nikaya in self.SOURCES[source]:
            table_name = f"{source}_{nikaya}"
            count = self._extract_table(cur, table_name, source, nikaya)
            logger.info(f"  {source}_{nikaya}: {count}")
            self._stats[source] += count
        
        conn.close()
        return self._extracted

    def _extract_table(
        self, 
        cur: sqlite3.Cursor, 
        table_name: str, 
        source: str, 
        nikaya: str
    ) -> int:
        """Extract translations from a single table."""
        try:
            cur.execute(f"SELECT * FROM {table_name}")
            columns = [desc[0] for desc in cur.description]
        except sqlite3.OperationalError:
            return 0
        
        count = 0
        for row in cur.fetchall():
            row_data = dict(zip(columns, row))
            
            sutta_id = row_data.get("sutta_number") or row_data.get("sutta_id")
            if not sutta_id:
                continue
            
            text = row_data.get("translation_text") or row_data.get("translation_markdown")
            if not text or len(text.strip()) < 50:
                continue
            
            self._extracted.append({
                "source_id": source,
                "sutta_number": str(sutta_id).strip().lower(),
                "text": text.strip(),
                "nikaya": nikaya,
                "is_certain": True,
            })
            count += 1
        
        return count

    def extract_all(self) -> list[dict]:
        """Extract all supported sources."""
        all_extracted = []
        
        for source in self.SOURCES:
            logger.info(f"Extracting {source.upper()}...")
            extracted = self.extract(source)
            all_extracted.extend(extracted)
            logger.info(f"  Total: {len(extracted)}")
        
        return all_extracted

    def get_stats(self) -> dict:
        """Get extraction statistics."""
        return self._stats.copy()


def extract_from_db(db_path: Path, source: str) -> list[dict]:
    """Quick function to extract from DB."""
    extractor = DBExtractor(db_path)
    return extractor.extract(source)


def extract_all_from_db(db_path: Path) -> list[dict]:
    """Quick function to extract all sources from DB."""
    extractor = DBExtractor(db_path)
    return extractor.extract_all()