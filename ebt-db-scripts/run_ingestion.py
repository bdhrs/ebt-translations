#!/usr/bin/env python3
"""EBT Ingestion CLI.

Run ingestion pipeline:
    uv run python ebt-db-scripts/run_ingestion.py [--source SOURCE] [--dry-run]

Examples:
    # Run full ingestion for all sources
    uv run python ebt-db-scripts/run_ingestion.py
    
    # Dry run to see what would be inserted
    uv run python ebt-db-scripts/run_ingestion.py --dry-run
    
    # Run for specific source only
    uv run python ebt-db-scripts/run_ingestion.py --source sc
    
    # Generate reports only (no ingestion)
    uv run python ebt-db-scripts/run_ingestion.py --reports-only
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add parent dir to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ebt_translations.ingestion.orchestrator import IngestionOrchestrator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


DEFAULT_DB = Path(__file__).parent.parent / "data" / "db" / "EBT_Unified (1).db"
DEFAULT_REPORTS_DIR = Path(__file__).parent.parent / "data" / "reports"


def main():
    parser = argparse.ArgumentParser(
        description="Run EBT data ingestion pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="Path to unified database",
    )
    parser.add_argument(
        "--source",
        choices=["sc", "tbw", "dt", "tpk", "pau", "ati"],
        help="Process only specific source",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate without inserting",
    )
    parser.add_argument(
        "--reports-only",
        action="store_true",
        help="Generate reports without running ingestion",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_REPORTS_DIR,
        help="Output directory for reports",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    db_path = args.db
    
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        sys.exit(1)
    
    if args.reports_only:
        logger.info("Generating reports only...")
        orchestrator = IngestionOrchestrator(str(db_path))
        coverage_after = orchestrator.compute_coverage()
        
        # Generate reports
        orchestrator.generate_reports(
            args.output_dir,
            coverage_before=coverage_after,
            coverage_after=coverage_after,
        )
        
        _print_summary({
            "processed": 0,
            "inserted": 0,
            "duplicates": 0,
            "failed": 0,
            "malformed": 0,
        }, coverage_after)
        
        return
    
    logger.info(f"Starting ingestion (DB: {db_path})")
    logger.info(f"Dry run: {args.dry_run}")
    
    orchestrator = IngestionOrchestrator(str(db_path))
    
    coverage_before = orchestrator.compute_coverage()
    logger.info(f"Coverage before: {coverage_before}")
    
    sources = [args.source] if args.source else ["sc", "tbw", "dt", "tpk", "pau"]
    
    all_results = {
        "processed": 0,
        "inserted": 0,
        "duplicates": 0,
        "failed": 0,
        "malformed": 0,
    }
    
    for source in sources:
        logger.info(f"\n{'='*50}")
        logger.info(f"Processing source: {source}")
        logger.info(f"{'='*50}")
        
        result = orchestrator.run(source, dry_run=args.dry_run)
        
        for key in all_results:
            if key in result:
                all_results[key] += result.get(key, 0)
    
    coverage_after = orchestrator.compute_coverage()
    
    # Generate reports
    orchestrator.generate_reports(
        args.output_dir,
        coverage_before=coverage_before,
        coverage_after=coverage_after,
    )
    
    _print_summary(all_results, coverage_after)


def _print_summary(results: dict, coverage: dict):
    """Print final summary."""
    
    print("\n" + "=" * 60)
    print("INGESTION SUMMARY")
    print("=" * 60)
    
    print(f"\n{'Processed:':<20} {results.get('processed', 0):,}")
    print(f"{'Inserted:':<20} {results.get('inserted', 0):,}")
    print(f"{'Duplicates:':<20} {results.get('duplicates', 0):,}")
    print(f"{'Failed:':<20} {results.get('failed', 0):,}")
    print(f"{'Malformed:':<20} {results.get('malformed', 0):,}")
    
    print("\n" + "-" * 60)
    print("COVERAGE")
    print("-" * 60)
    
    total_expected = coverage.get("total_expected", 0)
    print(f"\n{'Total expected:':<20} {total_expected:,}")
    
    if "coverage_by_source" in coverage:
        for source, data in coverage["coverage_by_source"].items():
            print(f"  {source:<10} {data.get('count', 0):>6,} ({data.get('pct', 0):>5.1f}%)")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()