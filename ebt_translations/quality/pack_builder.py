"""Translation pack builder for EBT quality pipeline.

For each sutta:
- gather all cleaned translations
- remove duplicates
- attach quality score
- sort by score

Output:
{
  sutta_number,
  translations: [
    {source, text, score}
  ]
}
"""

import logging
import sqlite3
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class TranslationPack:
    """Translation pack for a single sutta."""
    sutta_number: str
    translations: list[dict]
    
    def __post_init__(self):
        if self.translations is None:
            self.translations = []


class TranslationPackBuilder:
    """Build translation packs per sutta."""
    
    SOURCES = ["sc", "tbw", "dt", "tpk", "pau"]
    
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
    
    def connect(self, db_path: str):
        """Connect to database."""
        if self.conn is None:
            self.conn = sqlite3.connect(db_path)
        return self.conn
    
    def close(self):
        """Close connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def build_packs(
        self,
        sources: Optional[list[str]] = None,
    ) -> list[TranslationPack]:
        """Build translation packs for all suttas.
        
        Args:
            sources: Optional list of sources to include
            
        Returns:
            List of TranslationPacks
        """
        if sources is None:
            sources = self.SOURCES
        
        conn = self.connect(self.db_path)
        cur = conn.cursor()
        
        packs = []
        
        for source in sources:
            try:
                for nikaya in ["dn", "mn", "sn", "an"]:
                    table = f"{source}_{nikaya}"
                    
                    try:
                        cur.execute(f"""
                            SELECT sutta_number, translation_text 
                            FROM {table}
                            WHERE translation_text IS NOT NULL 
                            AND LENGTH(translation_text) > 100
                        """)
                    except sqlite3.OperationalError:
                        continue
                    
                    for sutta_number, text in cur.fetchall():
                        if not sutta_number or not text:
                            continue
                        
                        packs.append(TranslationPack(
                            sutta_number=sutta_number,
                            translations=[{
                                "source_id": source,
                                "text": text,
                                "nikaya": nikaya,
                            }]
                        ))
            except Exception as e:
                logger.debug(f"Error building packs: {e}")
        
        self.close()
        
        logger.info(f"Built {len(packs)} translation packs")
        return packs
    
    def build_packs_grouped(self) -> dict[str, dict]:
        """Build packs grouped by sutta number.
        
        Returns:
            {sutta_number: {sources: {...}, translations: [...]}}
        """
        conn = self.connect(self.db_path)
        cur = conn.cursor()
        
        suttas: dict[str, dict] = {}
        
        for source in self.SOURCES:
            for nikaya in ["dn", "mn", "sn", "an"]:
                table = f"{source}_{nikaya}"
                
                try:
                    cur.execute(f"""
                        SELECT sutta_number, translation_text 
                        FROM {table}
                        WHERE translation_text IS NOT NULL 
                        AND LENGTH(translation_text) > 100
                    """)
                except sqlite3.OperationalError:
                    continue
                
                for sutta_number, text in cur.fetchall():
                    if not sutta_number or not text:
                        continue
                    
                    sutta_number = sutta_number.lower()
                    
                    if sutta_number not in suttas:
                        suttas[sutta_number] = {
                            "sutta_number": sutta_number,
                            "sources": {},
                            "translations": [],
                        }
                    
                    if source not in suttas[sutta_number]["sources"]:
                        suttas[sutta_number]["sources"][source] = 0
                    
                    suttas[sutta_number]["sources"][source] += 1
                    suttas[sutta_number]["translations"].append({
                        "source_id": source,
                        "text": text[:500],  # First 500 chars for preview
                        "nikaya": nikaya,
                    })
        
        self.close()
        
        return suttas
    
    def get_stats(self) -> dict:
        """Get statistics."""
        conn = self.connect(self.db_path)
        cur = conn.cursor()
        
        stats = {
            "total_suttas": 0,
            "by_source": {},
            "by_nikaya": {},
        }
        
        # Total unique suttas
        cur.execute("SELECT COUNT(DISTINCT sutta_number) FROM sutta_master")
        stats["total_suttas"] = cur.fetchone()[0] or 0
        
        # By source
        for source in self.SOURCES:
            count = 0
            for nikaya in ["dn", "mn", "sn", "an"]:
                table = f"{source}_{nikaya}"
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count += cur.fetchone()[0] or 0
                except sqlite3.OperationalError:
                    pass
            stats["by_source"][source] = count
        
        self.close()
        
        return stats


def build_translation_packs(db_path: str) -> dict[str, dict]:
    """Quick function to build translation packs."""
    builder = TranslationPackBuilder(db_path=db_path)
    return builder.build_packs_grouped()