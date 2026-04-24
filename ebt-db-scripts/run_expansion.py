#!/usr/bin/env python3
"""EBT Source Expansion Pipeline CLI.

Run expansion:
    python ebt-db-scripts/run_expansion.py

Options:
    --source SOURCE: Process specific source (dt, ati, tbw, pau, tpk)
    --limit N: Limit number of suttas to process
    --dry-run: Don't insert, just show what would be done
    --report: Show gap report only
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ebt_translations.expansion.source_expander import SourceExpander, GapDetector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).parent.parent / "data" / "db" / "EBT_Unified (1).db"


def main():
    parser = argparse.ArgumentParser(description="EBT Source Expansion Pipeline")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Database path")
    parser.add_argument("--source", choices=["dt", "ati", "tbw", "pau", "tpk"], help="Source to expand")
    parser.add_argument("--limit", type=int, default=0, help="Limit suttas to process")
    parser.add_argument("--dry-run", action="store_true", help="Dry run (no insert)")
    parser.add_argument("--report", action="store_true", help="Show gap report only")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    db_path = args.db
    
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        sys.exit(1)
    
    detector = GapDetector(str(db_path))
    
    # Show report
    if args.report:
        show_report(detector)
        return
    
    # Run expansion
    sources = [args.source] if args.source else ["dt", "ati", "tbw", "pau", "tpk"]
    
    logger.info(f"Starting expansion from sources: {sources}")
    logger.info(f"Database: {db_path}")
    
    if args.dry_run:
        logger.info("DRY RUN MODE - no inserts")
        show_gaps(detector, sources, args.limit)
        return
    
    expander = SourceExpander(str(db_path))
    
    try:
        result = expander.run(sources=sources, limit=args.limit)
        _print_summary(result)
    finally:
        expander.close()


def show_report(detector: GapDetector):
    """Show gap report."""
    print("\n" + "=" * 60)
    print("GAP REPORT")
    print("=" * 60)
    
    gaps = detector.get_all_gaps()
    
    total_gaps = sum(len(g) for g in gaps.values())
    
    print(f"\n{'Total missing:':<20} {total_gaps:,}")
    
    print("\n" + "-" * 60)
    print("BY SOURCE")
    print("-" * 60)
    
    for source, source_gaps in gaps.items():
        print(f"  {source:<10} {len(source_gaps):>6,} suttas")
    
    print("\n" + "=" * 60)


def show_gaps(detector: GapDetector, sources: list[str], limit: int):
    """Show gaps for sources."""
    for source in sources:
        gaps = detector.get_gaps(source)
        
        if limit:
            gaps = gaps[:limit]
        
        print(f"\n{source.upper()} - First {len(gaps)} gaps:")
        
        for i, sutta in enumerate(gaps[:20]):
            print(f"  {i+1}. {sutta}")
        
        if len(gaps) > 20:
            print(f"  ... and {len(gaps) - 20} more")


def _print_summary(result: dict):
    """Print summary."""
    print("\n" + "=" * 60)
    print("EXPANSION SUMMARY")
    print("=" * 60)
    
    print(f"\n{'Processed:':<20} {result.get('processed', 0):,}")
    print(f"{'Inserted:':<20} {result.get('inserted', 0):,}")
    print(f"{'Duplicates:':<20} {result.get('duplicates', 0):,}")
    print(f"{'Failed:':<20} {result.get('failed', 0):,}")
    print(f"{'Skipped:':<20} {result.get('skipped', 0):,}")
    
    print("\n" + "-" * 60)
    print("COVERAGE")
    print("-" * 60)
    
    coverage_before = result.get("coverage_before", {})
    coverage_after = result.get("coverage_after", {})
    
    total = coverage_before.get("total", 0)
    
    print(f"\n{'Total suttas:':<20} {total:,}")
    
    print("\nBefore:")
    for source, data in coverage_before.get("by_source", {}).items():
        print(f"  {source:<10} {data.get('count', 0):>6,} ({data.get('pct', 0):>5.1f}%)")
    
    print("\nAfter:")
    for source, data in coverage_after.get("by_source", {}).items():
        print(f"  {source:<10} {data.get('count', 0):>6,} ({data.get('pct', 0):>5.1f}%)")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()