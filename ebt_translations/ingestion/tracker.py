"""Ingestion tracking system for EBT ingestion pipeline.

Creates ingestion_log table to track:
- inserted
- duplicate
- skipped
- failed
"""

import logging
import sqlite3
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class IngestionTracker:
    """Track ingestion operations for audit trail."""
    
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.stats = {
            "inserted": 0,
            "duplicates": 0,
            "skipped": 0,
            "failed": 0,
            "malformed": 0,
        }
    
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
    
    def setup_log_table(self):
        """Create ingestion_log table if not exists."""
        if not self.conn:
            return
        
        cur = self.conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sutta_number TEXT NOT NULL,
                source_id TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT,
                normalized_id TEXT,
                char_count INTEGER,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_log_sutta 
            ON ingestion_log(sutta_number, source_id)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_log_status 
            ON ingestion_log(status)
        """)
        
        self.conn.commit()
        logger.info("Ingestion log table initialized")
    
    def log(
        self,
        sutta_number: str,
        source_id: str,
        status: str,
        reason: Optional[str] = None,
        normalized_id: Optional[str] = None,
        char_count: Optional[int] = None,
    ):
        """Log an ingestion operation.
        
        Args:
            sutta_number: Original sutta number
            source_id: Source identifier
            status: inserted | duplicate | skipped | failed | malformed
            reason: Optional reason for status
            normalized_id: Normalized sutta number
            char_count: Character count of text
        """
        if not self.conn:
            return
        
        cur = self.conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO ingestion_log
                (sutta_number, source_id, status, reason, normalized_id, char_count)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                sutta_number.lower() if sutta_number else "",
                source_id,
                status,
                reason,
                normalized_id.lower() if normalized_id else None,
                char_count,
            ))
            self.conn.commit()
            
            # Update stats
            if status in self.stats:
                self.stats[status] += 1
            
        except sqlite3.OperationalError as e:
            logger.error(f"Log insert error: {e}")
    
    def log_inserted(
        self,
        sutta_number: str,
        source_id: str,
        normalized_id: Optional[str] = None,
        char_count: Optional[int] = None,
    ):
        """Log successful insertion."""
        self.log(
            sutta_number=sutta_number,
            source_id=source_id,
            status="inserted",
            normalized_id=normalized_id,
            char_count=char_count,
        )
    
    def log_duplicate(
        self,
        sutta_number: str,
        source_id: str,
        reason: str = "duplicate_key",
    ):
        """Log duplicate detection."""
        self.log(
            sutta_number=sutta_number,
            source_id=source_id,
            status="duplicate",
            reason=reason,
        )
    
    def log_skipped(
        self,
        sutta_number: str,
        source_id: str,
        reason: str,
    ):
        """Log skipped record."""
        self.log(
            sutta_number=sutta_number,
            source_id=source_id,
            status="skipped",
            reason=reason,
        )
    
    def log_failed(
        self,
        sutta_number: str,
        source_id: str,
        reason: str,
    ):
        """Log failed insertion."""
        self.log(
            sutta_number=sutta_number,
            source_id=source_id,
            status="failed",
            reason=reason,
        )
    
    def log_malformed(
        self,
        sutta_number: str,
        source_id: str,
        reason: str = "malformed_id",
    ):
        """Log malformed sutta ID."""
        self.log(
            sutta_number=sutta_number,
            source_id=source_id,
            status="malformed",
            reason=reason,
        )
    
    def get_stats(self) -> dict:
        """Get ingestion statistics."""
        return self.stats.copy()
    
    def get_summary(self) -> dict:
        """Get full summary including percentages."""
        total = sum(self.stats.values())
        
        return {
            "total_processed": total,
            "inserted": self.stats["inserted"],
            "duplicates": self.stats["duplicates"],
            "skipped": self.stats["skipped"],
            "failed": self.stats["failed"],
            "malformed": self.stats["malformed"],
            "insertion_rate": round(
                self.stats["inserted"] / max(1, total) * 100, 1
            ) if total > 0 else 0,
        }
    
    def get_recent_logs(
        self,
        limit: int = 100,
        status: Optional[str] = None,
    ) -> list[dict]:
        """Get recent logs."""
        if not self.conn:
            return []
        
        cur = self.conn.cursor()
        
        query = "SELECT * FROM ingestion_log"
        params = []
        
        if status:
            query += " WHERE status = ?"
            params.append(status)
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        cur.execute(query, params)
        
        results = []
        for row in cur.fetchall():
            results.append({
                "id": row[0],
                "sutta_number": row[1],
                "source_id": row[2],
                "status": row[3],
                "reason": row[4],
                "normalized_id": row[5],
                "char_count": row[6],
                "timestamp": row[7],
            })
        
        return results
    
    def get_source_summary(self, source_id: str) -> dict:
        """Get summary for specific source."""
        if not self.conn:
            return {}
        
        cur = self.conn.cursor()
        
        cur.execute("""
            SELECT status, COUNT(*) 
            FROM ingestion_log 
            WHERE source_id = ?
            GROUP BY status
        """, (source_id,))
        
        summary = {"source_id": source_id}
        for status, count in cur.fetchall():
            summary[status] = count
        
        return summary