"""Coverage Analyzer - Core analysis for EBT coverage tracking."""

import logging
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class SourceCoverage:
    """Coverage metrics for a single source."""
    source_id: str
    total_expected: int = 0
    total_scraped: int = 0
    with_translation: int = 0
    with_pali: int = 0
    is_complete: int = 0
    missing_suttas: list[str] = field(default_factory=list)
    
    @property
    def coverage_percent(self) -> float:
        """Calculate coverage percentage."""
        if self.total_expected == 0:
            return 0.0
        return round((self.total_scraped / self.total_expected) * 100, 1)


@dataclass
class NikayaCoverage:
    """Coverage breakdown by nikaya."""
    nikaya: str
    expected: int
    scraped: int
    
    @property
    def coverage_percent(self) -> float:
        if self.expected == 0:
            return 0.0
        return round((self.scraped / self.expected) * 100, 1)


class CoverageAnalyzer:
    """Analyze coverage metrics for all sources."""
    
    SOURCES = ["sc", "tbw", "dt", "ati", "tpk", "pau", "cst", "epi"]
    NIKAYAS = ["dn", "mn", "sn", "an", "kn"]
    
    # Expected sutta counts per nikaya
    NIKAYA_EXPECTED = {
        "dn": 34,
        "mn": 152,
        "sn": 2309,
        "an": 2300,
        "kn": 1000,  # Approximate for KN
    }
    
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or self._find_db_path()
        self.conn: Optional[sqlite3.Connection] = None
        self._master_suttas: set[str] = set()
    
    def _find_db_path(self) -> str:
        """Find the unified database."""
        possible_paths = [
            Path("data/db/EBT_Unified (1).db"),
            Path("data/db/EBT_Unified.db"),
            Path("data/db/EBT_Suttas.db"),
        ]
        
        base = Path(".")
        for p in possible_paths:
            full_path = base / p
            if full_path.exists():
                logger.debug(f"Found DB at: {full_path}")
                return str(full_path)
        
        return str(possible_paths[0])
    
    def connect(self) -> sqlite3.Connection:
        """Connect to database."""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
        return self.conn
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def load_master_list(self) -> set[str]:
        """Load master sutta list from sutta_master table."""
        conn = self.connect()
        cur = conn.cursor()
        
        cur.execute("SELECT sutta_number FROM sutta_master")
        suttas = {row[0].lower() for row in cur.fetchall() if row[0]}
        
        self._master_suttas = suttas
        logger.info(f"Loaded {len(suttas)} master suttas")
        return suttas
    
    def analyze_source(self, source_id: str) -> SourceCoverage:
        """Analyze coverage for a single source."""
        conn = self.connect()
        cur = conn.cursor()
        
        # Load master if not loaded
        if not self._master_suttas:
            self.load_master_list()
        
        total_expected = len(self._master_suttas)
        
        # Get counts from source_availability
        cur.execute("""
            SELECT 
                COUNT(DISTINCT sutta_number) as suttas,
                SUM(CASE WHEN has_translation = 1 THEN 1 ELSE 0 END) as with_trans,
                SUM(CASE WHEN has_pali = 1 THEN 1 ELSE 0 END) as with_pali,
                SUM(is_complete) as complete
            FROM source_availability
            WHERE source_id = ?
        """, (source_id,))
        
        row = cur.fetchone()
        total_scraped = row["suttas"] if row else 0
        with_translation = row["with_trans"] if row else 0
        with_pali = row["with_pali"] if row else 0
        is_complete = row["complete"] if row else 0
        
        # Get missing suttas
        cur.execute("""
            SELECT sm.sutta_number
            FROM sutta_master sm
            LEFT JOIN source_availability sa
                ON sm.sutta_number = sa.sutta_number
                AND sa.source_id = ?
            WHERE sa.sutta_number IS NULL
            ORDER BY sm.sutta_number
        """, (source_id,))
        
        missing = [row[0] for row in cur.fetchall() if row[0]]
        
        return SourceCoverage(
            source_id=source_id,
            total_expected=total_expected,
            total_scraped=total_scraped,
            with_translation=with_translation,
            with_pali=with_pali,
            is_complete=is_complete,
            missing_suttas=missing,
        )
    
    def analyze_all_sources(self) -> dict[str, SourceCoverage]:
        """Analyze coverage for all sources."""
        results = {}
        
        for source in self.SOURCES:
            try:
                results[source] = self.analyze_source(source)
            except Exception as e:
                logger.debug(f"Error analyzing {source}: {e}")
                results[source] = SourceCoverage(
                    source_id=source,
                    total_expected=len(self._master_suttas),
                    total_scraped=0,
                )
        
        return results
    
    def analyze_by_nikaya(self) -> dict[str, NikayaCoverage]:
        """Analyze coverage breakdown by nikaya."""
        conn = self.connect()
        cur = conn.cursor()
        
        results = {}
        
        for nikaya in self.NIKAYAS:
            expected = self.NIKAYA_EXPECTED.get(nikaya, 0)
            
            cur.execute("""
                SELECT COUNT(DISTINCT sutta_number)
                FROM source_availability
                WHERE sutta_number LIKE ?
            """, (f"{nikaya}%",))
            
            scraped = cur.fetchone()[0] or 0
            
            results[nikaya] = NikayaCoverage(
                nikaya=nikaya,
                expected=expected,
                scraped=scraped,
            )
        
        return results
    
    def analyze_multi_source(self) -> dict[int, int]:
        """Analyze how many sources each sutta has."""
        conn = self.connect()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT source_count, COUNT(*) as count
            FROM (
                SELECT sutta_number, COUNT(DISTINCT source_id) as source_count
                FROM source_availability
                GROUP BY sutta_number
            )
            GROUP BY source_count
        """)
        
        results = {}
        for row in cur.fetchall():
            results[row[0]] = row[1]
        
        return results
    
    def detect_duplicates(self) -> dict[str, list[str]]:
        """Detect potential duplicate suttas."""
        conn = self.connect()
        cur = conn.cursor()
        
        duplicates = {}
        
        for source in self.SOURCES:
            for nikaya in self.NIKAYAS:
                table = f"{source}_{nikaya}"
                try:
                    cur.execute(f"""
                        SELECT sutta_number, COUNT(*) as cnt
                        FROM {table}
                        GROUP BY sutta_number
                        HAVING cnt > 1
                    """)
                    
                    for row in cur.fetchall():
                        if row[1] > 1:
                            if source not in duplicates:
                                duplicates[source] = []
                            duplicates[source].append(row[0])
                except sqlite3.OperationalError:
                    continue
        
        return duplicates
    
    def detect_malformed_ids(self) -> list[str]:
        """Detect malformed sutta IDs."""
        conn = self.connect()
        cur = conn.cursor()
        
        malformed = []
        pattern = re.compile(r"^(dn|mn|sn|an|kn|dhp|iti|snp|thag|thig|ud|kp)\d+(\.\d+)?$", re.IGNORECASE)
        
        cur.execute("SELECT sutta_number FROM source_availability")
        
        for row in cur.fetchall():
            sutta = row[0]
            if sutta and not pattern.match(sutta.lower()):
                malformed.append(sutta)
        
        return malformed
    
    def get_summary(self) -> dict:
        """Get complete coverage summary."""
        if not self._master_suttas:
            self.load_master_list()
        
        sources = self.analyze_all_sources()
        by_nikaya = self.analyze_by_nikaya()
        multi = self.analyze_multi_source()
        
        return {
            "total_master_suttas": len(self._master_suttas),
            "sources": {
                s.source_id: {
                    "total_expected": s.total_expected,
                    "total_scraped": s.total_scraped,
                    "coverage_percent": s.coverage_percent,
                    "missing_count": len(s.missing_suttas),
                }
                for s in sources.values()
            },
            "by_nikaya": {
                n.nikaya: {
                    "expected": n.expected,
                    "scraped": n.scraped,
                    "coverage_percent": n.coverage_percent,
                }
                for n in by_nikaya.values()
            },
            "multi_source": multi,
        }
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()