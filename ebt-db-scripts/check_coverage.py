#!/usr/bin/env python3
"""EBT Coverage Check CLI.

Usage:
    python ebt-db-scripts/check_coverage.py

Options:
    --db PATH          Database path
    --output-dir PATH   Output directory
    --console         Print to console (default: True)
    --json            Generate JSON report (default: True)
    --csv             Generate CSV report (default: True)
    --missing         Generate missing lists (default: True)
    --source SOURCE    Check specific source only
    -v, --verbose     Verbose logging
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ebt_translations.coverage.coverage_analyzer import CoverageAnalyzer
from ebt_translations.coverage.coverage_report import CoverageReporter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Check EBT coverage per source",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--db", type=Path,
        help="Database path",
    )
    parser.add_argument(
        "--output-dir", type=Path,
        help="Output directory for reports",
    )
    parser.add_argument(
        "--no-console",
        action="store_true",
        help="Skip console output",
    )
    parser.add_argument(
        "--json/--no-json",
        dest="json_flag",
        action="store_true",
        default=True,
        help="Generate JSON report",
    )
    parser.add_argument(
        "--csv/--no-csv",
        dest="csv_flag", 
        action="store_true",
        default=True,
        help="Generate CSV report",
    )
    parser.add_argument(
        "--missing/--no-missing",
        dest="missing",
        action="store_true",
        default=True,
        help="Generate missing lists",
    )
    parser.add_argument(
        "--source",
        choices=["sc", "tbw", "dt", "ati", "tpk", "pau", "cst"],
        help="Check specific source only",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose logging",
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    db_path = args.db or Path("data/db/EBT_Unified (1).db")
    
    # Try absolute if relative not found
    if not db_path.exists():
        base = Path(__file__).parent.parent
        db_path = base / "data" / "db" / "EBT_Unified (1).db"
    
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        sys.exit(1)
    
    logger.info(f"Using database: {db_path}")
    
    analyzer = CoverageAnalyzer(str(db_path))
    
    if args.source:
        coverage = analyzer.analyze_source(args.source)
        
        print(f"\n{args.source.upper()} Coverage:")
        print(f"  Total: {coverage.total_scraped:,} / {coverage.total_expected:,}")
        print(f"  Coverage: {coverage.coverage_percent}%")
        print(f"  Missing: {len(coverage.missing_suttas):,}")
        
        if coverage.missing_suttas and args.verbose:
            print(f"\n  First 10 missing:")
            for sutta in coverage.missing_suttas[:10]:
                print(f"    {sutta}")
    else:
        reporter = CoverageReporter(args.output_dir)
        
        if not args.no_console:
            reporter.report_console(analyzer)
        
        try:
            reporter.report_json(analyzer)
            logger.info("JSON report generated")
        except Exception as e:
            logger.warning(f"JSON report failed: {e}")
        
        try:
            reporter.report_csv(analyzer)
            logger.info("CSV report generated")
        except Exception as e:
            logger.warning(f"CSV report failed: {e}")
        
        try:
            paths = reporter.report_missing(analyzer)
            logger.info(f"Generated {len(paths)} missing lists")
        except Exception as e:
            logger.warning(f"Missing report failed: {e}")
    
    analyzer.close()
    logger.info("Done")


if __name__ == "__main__":
    main()