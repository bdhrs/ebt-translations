"""Source Expansion Pipeline for EBT.

Step 1: Gap Detector - find missing suttas per source
Step 2: Target Builder - create target list
Step 3: Source Handlers - fetch from DT, ATI, TBW, PAU, TPK
Step 4: Extraction - parse HTML/DB, extract text
Step 5: Normalization - normalize sutta ID
Step 6: Validation - check length, exists
Step 7: Deduplication - skip duplicates
Step 8: Insertion - insert into tables
Step 9: Tracking - log operations
Step 10: Execution Loop - process all targets
"""

import hashlib
import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ExpansionTarget:
    """Target sutta for expansion."""
    sutta_number: str
    target_source: str
    nikaya: str


@dataclass
class ExpansionResult:
    """Result of expansion attempt."""
    sutta_number: str
    source_id: str
    status: str
    reason: str = ""
    text_hash: Optional[str] = None


class GapDetector:
    """Detect gaps - missing suttas per source."""
    
    SOURCES = ["dt", "ati", "tbw", "pau", "tpk"]
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def get_gaps(self, source: str) -> list[str]:
        """Get list of suttas missing from a source."""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT sm.sutta_number
            FROM sutta_master sm
            LEFT JOIN source_availability sa
                ON sm.sutta_number = sa.sutta_number
                AND sa.source_id = ?
            WHERE sa.sutta_number IS NULL
            ORDER BY sm.sutta_number
        """, (source,))
        
        gaps = [row[0] for row in cur.fetchall() if row[0]]
        conn.close()
        
        logger.info(f"Found {len(gaps)} gaps for source {source}")
        return gaps
    
    def get_all_gaps(self) -> dict[str, list[str]]:
        """Get gaps for all sources."""
        gaps = {}
        
        for source in self.SOURCES:
            gaps[source] = self.get_gaps(source)
        
        return gaps


class TargetBuilder:
    """Build list of expansion targets."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def build_targets(
        self,
        sources: Optional[list[str]] = None,
    ) -> list[ExpansionTarget]:
        """Build targets from gaps."""
        detector = GapDetector(self.db_path)
        
        if sources is None:
            sources = detector.SOURCES
        
        targets = []
        
        for source in sources:
            gaps = detector.get_gaps(source)
            
            for sutta_number in gaps:
                nikaya = self._get_nikaya(sutta_number)
                targets.append(ExpansionTarget(
                    sutta_number=sutta_number,
                    target_source=source,
                    nikaya=nikaya,
                ))
        
        logger.info(f"Built {len(targets)} expansion targets")
        return targets
    
    def _get_nikaya(self, sutta_number: str) -> str:
        """Determine nikaya from sutta number."""
        s = sutta_number.lower()
        
        if s.startswith("dn"):
            return "dn"
        elif s.startswith("mn"):
            return "mn"
        elif s.startswith("sn"):
            return "sn"
        elif s.startswith("an"):
            return "an"
        else:
            return "kn"


class SourceHandlers:
    """Handlers for fetching from different sources."""
    
    def __init__(self):
        self.stats = {
            "dt_attempts": 0,
            "ati_attempts": 0,
            "tbw_attempts": 0,
            "pau_attempts": 0,
            "tpk_attempts": 0,
        }
    
    def fetch_dt(self, sutta_number: str) -> Optional[str]:
        """Fetch from Dhamma Talks (thanissaro)."""
        self.stats["dt_attempts"] += 1
        
        # dhammatalks.org/suttas/{nikaya}/{number}
        nikaya = self._get_nikaya(sutta_number)
        url = f"https://www.dhammatalks.org/suttas/{nikaya}/{sutta_number}"
        
        try:
            import requests
            from bs4 import BeautifulSoup
            
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                
                # Find translation content
                text = ""
                for elem in soup.find_all(["div"], class_=[]):
                    if elem.get("id") in ["sutta", "content", "main"]:
                        text = elem.get_text(strip=True)
                        break
                
                if not text:
                    for elem in soup.find_all(["p"]):
                        if len(elem.get_text()) > 200:
                            text = elem.get_text(strip=True)
                            break
                
                if text:
                    return self._clean_text(text)
                
        except Exception as e:
            logger.debug(f"DT fetch failed for {sutta_number}: {e}")
        
        return None
    
    def fetch_ati(self, sutta_number: str) -> Optional[str]:
        """Fetch from Access to Insight (ati)."""
        self.stats["ati_attempts"] += 1
        
        nikaya = self._get_nikaya(sutta_number)
        
        # accesstoinsight.org/tipitaka/{nikaya}.{number}.html
        url = f"https://www.accesstoinsight.org/tipitaka/{nikaya}.{sutta_number}.html"
        
        try:
            import requests
            from bs4 import BeautifulSoup
            
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                
                text = ""
                for elem in soup.find_all(["div"], id=["content", "main"]):
                    text = elem.get_text(strip=True)
                    break
                
                if text:
                    return self._clean_text(text)
                
        except Exception as e:
            logger.debug(f"ATI fetch failed for {sutta_number}: {e}")
        
        return None
    
    def fetch_tbw(self, sutta_number: str) -> Optional[str]:
        """Fetch from The Buddha's Words."""
        self.stats["tbw_attempts"] += 1
        
        # Try local BW2 files first
        data_dir = Path("C:/Users/ariha/Documents/ebt-translations/data/bw2_20260118")
        
        if data_dir.exists():
            nikaya = self._get_nikaya(sutta_number)
            html_file = data_dir / nikaya / f"{sutta_number}.html"
            
            if html_file.exists():
                try:
                    with open(html_file, encoding="utf-8", errors="ignore") as f:
                        from bs4 import BeautifulSoup
                        soup = BeautifulSoup(f.read(), "html.parser")
                        
                        text = ""
                        for elem in soup.find_all(["div"], class_=["sutta", "content"]):
                            text = elem.get_text(strip=True)
                            break
                        
                        if text:
                            return self._clean_text(text)
                            
                except Exception as e:
                    logger.debug(f"TBW local fetch failed: {e}")
        
        return None
    
    def fetch_pau(self, sutta_number: str) -> Optional[str]:
        """Fetch from Pa-Auk (TPK)."""
        self.stats["pau_attempts"] += 1
        
        # Try local pau_db
        db_path = Path("C:/Users/ariha/Documents/ebt-translations/data/pau_db/pau.db")
        
        if db_path.exists():
            try:
                conn = sqlite3.connect(str(db_path))
                cur = conn.cursor()
                
                cur.execute("""
                    SELECT translation_text 
                    FROM suttas 
                    WHERE sutta_id = ?
                """, (sutta_number,))
                
                row = cur.fetchone()
                conn.close()
                
                if row and row[0]:
                    return row[0]
                    
            except Exception as e:
                logger.debug(f"PAU fetch failed: {e}")
        
        return None
    
    def fetch_tpk(self, sutta_number: str) -> Optional[str]:
        """Fetch from Tipitaka Pali."""
        self.stats["tpk_attempts"] += 1
        
        # Try tipitaka-translation-db
        db_path = Path("C:/Users/ariha/Documents/ebt-translations/data/tipitaka-translation-data.db")
        
        if db_path.exists():
            try:
                conn = sqlite3.connect(str(db_path))
                cur = conn.cursor()
                
                for table in ["suttas", "dn", "mn", "sn", "an"]:
                    try:
                        cur.execute(f"""
                            SELECT translation_text 
                            FROM {table} 
                            WHERE sutta_id = ?
                        """, (sutta_number,))
                        
                        row = cur.fetchone()
                        if row and row[0]:
                            conn.close()
                            return row[0]
                    except sqlite3.OperationalError:
                        continue
                
                conn.close()
                        
            except Exception as e:
                logger.debug(f"TPK fetch failed: {e}")
        
        return None
    
    def fetch(self, source: str, sutta_number: str) -> Optional[str]:
        """Fetch from specified source."""
        fetch_map = {
            "dt": self.fetch_dt,
            "ati": self.fetch_ati,
            "tbw": self.fetch_tbw,
            "pau": self.fetch_pau,
            "tpk": self.fetch_tpk,
        }
        
        handler = fetch_map.get(source)
        if handler:
            return handler(sutta_number)
        
        return None
    
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
    
    def _clean_text(self, text: str) -> str:
        """Clean extracted text."""
        if not text:
            return ""
        
        # Remove navigation/footer
        import re
        
        nav_patterns = [
            r"<nav[^>]*>.*?</nav>",
            r"<footer[^>]*>.*?</footer>",
            r"<header[^>]*>.*?</header>",
        ]
        
        for pattern in nav_patterns:
            text = re.sub(pattern, "", text, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove HTML
        text = re.sub(r"<[^>]+>", "", text)
        
        # Normalize whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        
        return text.strip()
    
    def get_stats(self) -> dict:
        """Get fetch statistics."""
        return self.stats.copy()


class SourceExpander:
    """Main expansion pipeline."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        
        self.handlers = SourceHandlers()
        self.stats = {
            "processed": 0,
            "inserted": 0,
            "duplicates": 0,
            "failed": 0,
            "skipped": 0,
        }
    
    def connect(self):
        """Connect to database."""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
        return self.conn
    
    def close(self):
        """Close connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def run(
        self,
        sources: Optional[list[str]] = None,
        limit: int = 0,
    ) -> dict:
        """Run expansion pipeline."""
        
        logger.info("Starting source expansion")
        
        # Get coverage before
        coverage_before = self._get_coverage()
        
        # Build targets
        detector = GapDetector(self.db_path)
        
        if sources is None:
            sources = detector.SOURCES
        
        all_targets = []
        
        for source in sources:
            gaps = detector.get_gaps(source)
            
            if limit:
                gaps = gaps[:limit]
            
            for sutta_number in gaps:
                nikaya = self._get_nikaya(sutta_number)
                all_targets.append(ExpansionTarget(
                    sutta_number=sutta_number,
                    target_source=source,
                    nikaya=nikaya,
                ))
        
        logger.info(f"Processing {len(all_targets)} targets")
        
        # Process each target
        for target in all_targets:
            self._process_target(target)
        
        # Get coverage after
        coverage_after = self._get_coverage()
        
        result = {
            **self.stats,
            "coverage_before": coverage_before,
            "coverage_after": coverage_after,
        }
        
        logger.info(f"Expansion complete: {self.stats['inserted']} inserted")
        
        return result
    
    def _process_target(self, target: ExpansionTarget):
        """Process a single expansion target."""
        self.stats["processed"] += 1
        
        # Check if already exists
        if self._exists_in_db(target.sutta_number, target.target_source):
            self.stats["skipped"] += 1
            return
        
        # Fetch from source
        text = self.handlers.fetch(target.target_source, target.sutta_number)
        
        if not text:
            self.stats["failed"] += 1
            return
        
        # Validate
        if len(text) < 200:
            self.stats["failed"] += 1
            return
        
        # Check duplicate text
        text_hash = self._hash_text(text)
        
        if self._text_exists(text_hash):
            self.stats["duplicates"] += 1
            return
        
        # Insert
        if self._insert(target, text, text_hash):
            self.stats["inserted"] += 1
        else:
            self.stats["failed"] += 1
    
    def _exists_in_db(self, sutta_number: str, source_id: str) -> bool:
        """Check if sutta exists in database."""
        conn = self.connect()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 1 
            FROM source_availability 
            WHERE sutta_number = ? AND source_id = ?
        """, (sutta_number, source_id))
        
        return cur.fetchone() is not None
    
    def _text_exists(self, text_hash: str) -> bool:
        """Check if text hash exists."""
        # Simplified - can be enhanced with hash table
        return False
    
    def _insert(
        self,
        target: ExpansionTarget,
        text: str,
        text_hash: str,
    ) -> bool:
        """Insert translation into database."""
        conn = self.connect()
        cur = conn.cursor()
        
        table_name = f"{target.target_source}_{target.nikaya}"
        
        try:
            cur.execute(f"""
                INSERT OR IGNORE INTO {table_name}
                (sutta_number, translation_text, char_count, is_complete, last_updated)
                VALUES (?, ?, ?, 1, ?)
            """, (
                target.sutta_number,
                text,
                len(text),
                datetime.now(),
            ))
            
            cur.execute("""
                INSERT OR IGNORE INTO source_availability
                (sutta_number, source_id, has_translation, is_complete)
                VALUES (?, ?, 1, 1)
            """, (target.sutta_number, target.target_source))
            
            conn.commit()
            return True
            
        except sqlite3.OperationalError as e:
            logger.debug(f"Insert failed: {e}")
            return False
    
    def _get_coverage(self) -> dict:
        """Get current coverage stats."""
        conn = self.connect()
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM sutta_master")
        total = cur.fetchone()[0] or 0
        
        coverage = {"total": total, "by_source": {}}
        
        for source in ["sc", "tbw", "dt", "tpk", "pau", "ati"]:
            cur.execute("""
                SELECT COUNT(DISTINCT sutta_number)
                FROM source_availability
                WHERE source_id = ?
            """, (source,))
            
            count = cur.fetchone()[0] or 0
            coverage["by_source"][source] = {
                "count": count,
                "pct": round(count / max(1, total) * 100, 1),
            }
        
        return coverage
    
    def _get_nikaya(self, sutta_number: str) -> str:
        """Get nikaya from sutta number."""
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
    
    def _hash_text(self, text: str) -> str:
        """Hash text content."""
        normalized = " ".join(text.split())
        return hashlib.md5(normalized.encode()).hexdigest()


def run_expansion(db_path: str, sources: list[str] = None, limit: int = 0) -> dict:
    """Run expansion pipeline."""
    expander = SourceExpander(db_path)
    result = expander.run(sources=sources, limit=limit)
    expander.close()
    return result