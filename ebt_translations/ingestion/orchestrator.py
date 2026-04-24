"""Ingestion orchestrator for EBT pipeline.

Pipeline: load → normalize → deduplicate → validate → insert → log
"""

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from ebt_translations.ingestion.loader import SourceLoader, LoadedSutta
from ebt_translations.ingestion.normalizer import SuttaNormalizer, NormalizedSutta
from ebt_translations.ingestion.deduplicator import Deduplicator
from ebt_translations.ingestion.validator import Validator
from ebt_translations.ingestion.inserter import Inserter
from ebt_translations.ingestion.tracker import IngestionTracker

logger = logging.getLogger(__name__)


class IngestionOrchestrator:
    """Orchestrate the full ingestion pipeline."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        
        self.loader = SourceLoader(db_path=Path(db_path))
        self.normalizer = SuttaNormalizer()
        self.deduplicator = Deduplicator(db_path=db_path)
        self.validator = Validator(db_path=db_path)
        self.inserter = Inserter(db_path=db_path)
        self.tracker = IngestionTracker(db_path=db_path)
        
        self._sources = ["sc", "tbw", "dt", "tpk", "pau"]
    
    def run(
        self,
        source: str,
        dry_run: bool = False,
    ) -> dict:
        """Run ingestion pipeline for a source.
        
        Args:
            source: Source identifier
            dry_run: If True, don't actually insert
            
        Returns:
            Summary dict
        """
        logger.info(f"Starting ingestion for source: {source}")
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            self.deduplicator.connect(self.db_path)
            self.validator.connect(self.db_path)
            self.inserter.connect(self.db_path)
            self.tracker.connect(self.db_path)
            
            self.tracker.setup_log_table()
            self.validator.load_master_suttas()
            self.deduplicator.load_existing(source)
            
            result = self._process_source(source, conn, dry_run)
            
            return result
            
        finally:
            conn.close()
            self.deduplicator.close()
            self.validator.close()
            self.inserter.close()
            self.tracker.close()
    
    def _process_source(
        self,
        source: str,
        conn: sqlite3.Connection,
        dry_run: bool,
    ) -> dict:
        """Process a single source."""
        
        processed = 0
        inserted = 0
        duplicates = 0
        failed = 0
        malformed = 0
        
        cur = conn.cursor()
        
        for nikaya in ["dn", "mn", "sn", "an"]:
            table = f"sc_{nikaya}"
            
            try:
                cur.execute(f"SELECT sutta_number, translation_text FROM {table}")
            except sqlite3.OperationalError:
                continue
            
            for sutta_number, text in cur.fetchall():
                if not sutta_number or not text:
                    continue
                
                processed += 1
                
                normalized = self.normalizer.normalize(sutta_number)
                
                if not normalized:
                    malformed += 1
                    self.tracker.log_malformed(sutta_number, source, "normalization_failed")
                    continue
                
                is_dup = self.deduplicator.check_db_duplicate(
                    normalized.normalized_id, source
                )
                
                if is_dup:
                    duplicates += 1
                    self.tracker.log_duplicate(sutta_number, source)
                    continue
                
                validation = self.validator.validate(
                    normalized.normalized_id, text
                )
                
                if not validation.is_valid:
                    failed += 1
                    self.tracker.log_skipped(
                        sutta_number, source, validation.reason
                    )
                    continue
                
                if not dry_run:
                    success = self.inserter.insert(
                        source_id=source,
                        sutta_number=normalized.normalized_id,
                        text=text,
                    )
                    
                    if success:
                        inserted += 1
                        self.tracker.log_inserted(
                            sutta_number=sutta_number,
                            source_id=source,
                            normalized_id=normalized.normalized_id,
                            char_count=len(text),
                        )
                    else:
                        failed += 1
                        self.tracker.log_failed(
                            sutta_number, source, "insert_failed"
                        )
                else:
                    inserted += 1
        
        stats = self.tracker.get_summary()
        stats["source"] = source
        stats["processed"] = processed
        
        logger.info(f"Ingestion complete for {source}: {inserted} inserted")
        
        return stats
    
    def compute_coverage(self) -> dict:
        """Compute coverage metrics after ingestion."""
        
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        try:
            cur.execute("SELECT COUNT(*) FROM sutta_master")
            total_expected = cur.fetchone()[0] or 0
        except sqlite3.OperationalError:
            total_expected = 0
        
        coverage_by_source = {}
        
        for source in self._sources:
            try:
                cur.execute("""
                    SELECT COUNT(DISTINCT sutta_number) 
                    FROM source_availability 
                    WHERE source_id = ?
                """, (source,))
                count = cur.fetchone()[0] or 0
                coverage_by_source[source] = {
                    "count": count,
                    "pct": round(count / max(1, total_expected) * 100, 1),
                }
            except sqlite3.OperationalError:
                coverage_by_source[source] = {"count": 0, "pct": 0}
        
        conn.close()
        
        return {
            "total_expected": total_expected,
            "coverage_by_source": coverage_by_source,
        }
    
    def generate_reports(
        self,
        output_dir: Path,
        coverage_before: dict,
        coverage_after: dict,
    ):
        """Generate report files."""
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        summary = {
            "timestamp": datetime.now().isoformat(),
            "coverage_before": coverage_before,
            "coverage_after": coverage_after,
        }
        
        with open(output_dir / "ingestion_summary.json", "w") as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Reports generated: {output_dir}")
    
    def close(self):
        """Clean up resources."""
        pass