#!/usr/bin/env python3
"""Auto-loop coverage improvement script."""

import argparse
import logging
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from ebt_translations.coverage.coverage_analyzer import CoverageAnalyzer
from ebt_translations.coverage.coverage_report import CoverageReporter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).parent.parent / "data" / "db" / "EBT_Unified (1).db"
TARGET_COVERAGE = 60.0


class CoverageImprover:
    """Auto-loop coverage improvement."""
    
    SOURCES_TO_IMPROVE = ["dt", "ati", "tbw", "tpk", "pau"]
    
    def __init__(self, db_path: str, target: float = TARGET_COVERAGE):
        self.db_path = db_path
        self.target = target
        self.conn: Optional[sqlite3.Connection] = None
    
    def connect(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
        return self.conn
    
    def close(self):
        if self.conn:
            self.conn.close()
    
    def get_current_coverage(self) -> dict:
        """Get current coverage by source."""
        conn = self.connect()
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM sutta_master")
        total = cur.fetchone()[0] or 0
        
        coverage = {"total": total, "sources": {}}
        
        for source in self.SOURCES_TO_IMPROVE:
            cur.execute("""
                SELECT COUNT(DISTINCT sutta_number)
                FROM source_availability
                WHERE source_id = ?
            """, (source,))
            
            count = cur.fetchone()[0] or 0
            pct = (count / total * 100) if total > 0 else 0
            
            coverage["sources"][source] = {"count": count, "pct": pct}
        
        return coverage
    
    def find_lowest_source(self) -> Optional[str]:
        """Find source with lowest coverage."""
        coverage = self.get_current_coverage()
        
        lowest = None
        lowest_pct = 100.0
        
        for source, data in coverage["sources"].items():
            if data["pct"] < self.target and data["pct"] < lowest_pct:
                lowest = source
                lowest_pct = data["pct"]
        
        return lowest
    
    def run_coverage_check(self) -> dict:
        """Run coverage check and generate reports."""
        logger.info("Running coverage check...")
        
        analyzer = CoverageAnalyzer(self.db_path)
        reporter = CoverageReporter()
        
        summary = analyzer.get_summary()
        
        reporter.report_json(analyzer)
        reporter.report_csv(analyzer)
        reporter.report_missing(analyzer)
        
        analyzer.close()
        
        return summary
    
    def run_scraper(self, source: str, limit: int = 100) -> int:
        """Run scraper for a source."""
        logger.info(f"Running scraper for {source} (limit={limit})...")
        
        # Build command
        cmd = [
            sys.executable,
            str(Path(__file__).parent / "scrape_missing.py"),
            "--source", source,
            "--limit", str(limit),
            "--db", str(self.db_path),
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,
            )
            
            if result.returncode == 0:
                logger.info(f"Scraper completed for {source}")
                # Parse output for inserted count
                for line in result.stdout.split("\n"):
                    if "Inserted:" in line:
                        parts = line.split("Inserted:")[1].strip()
                        return int(parts.split()[0])
            else:
                logger.error(f"Scraper failed: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            logger.error(f"Scraper timed out for {source}")
        except Exception as e:
            logger.error(f"Scraper error: {e}")
        
        return 0
    
    def run(self, max_iterations: int = 10):
        """Run auto-loop improvement."""
        logger.info("="*60)
        logger.info("COVERAGE IMPROVEMENT LOOP")
        logger.info("="*60)
        logger.info(f"Target: {self.target}%")
        
        for i in range(max_iterations):
            logger.info(f"\n--- Iteration {i+1}/{max_iterations} ---")
            
            # Get current coverage
            coverage = self.get_current_coverage()
            
            lowest = self.find_lowest_source()
            
            if lowest is None:
                logger.info("All sources at or above target!")
                break
            
            data = coverage["sources"][lowest]
            logger.info(f"Lowest: {lowest} ({data['pct']:.1f}%)")
            
            # Run coverage check
            summary = self.run_coverage_check()
            
            # Log status
            logger.info("\nCurrent status:")
            for source, sdata in coverage["sources"].items():
                pct = sdata["pct"]
                status = "✓" if pct >= self.target else "✗"
                logger.info(f"  {source}: {sdata['count']} ({pct:.1f}%) {status}")
            
            if data["pct"] >= self.target:
                logger.info(f"\nTarget reached! {lowest} is at {data['pct']:.1f}%")
                break
            
            # Run scraper for lowest
            logger.info(f"\nRunning scraper for {lowest}...")
            inserted = self.run_scraper(lowest, limit=50)
            
            logger.info(f"Inserted {inserted} new suttas")
            
            if inserted == 0:
                logger.warning("No progress this iteration")
                # Could add early exit here
        
        # Final coverage check
        logger.info("\n" + "="*60)
        logger.info("FINAL COVERAGE")
        logger.info("="*60)
        
        final = self.get_current_coverage()
        for source, data in final["sources"].items():
            pct = data["pct"]
            status = "✓" if pct >= self.target else "✗"
            logger.info(f"  {source}: {data['count']} ({pct:.1f}%) {status}")
        
        self.close()


def main():
    parser = argparse.ArgumentParser(description="Auto-loop coverage improvement")
    parser.add_argument("--target", type=float, default=TARGET_COVERAGE, 
                        help="Target coverage percentage")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB,
                        help="Database path")
    parser.add_argument("--max-iterations", type=int, default=10,
                        help="Max iterations")
    
    args = parser.parse_args()
    
    if not args.db.exists():
        logger.error(f"Database not found: {args.db}")
        sys.exit(1)
    
    improver = CoverageImprover(str(args.db), args.target)
    improver.run(args.max_iterations)


if __name__ == "__main__":
    main()