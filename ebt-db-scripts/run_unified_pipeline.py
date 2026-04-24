#!/usr/bin/env python3
"""Unified Pipeline CLI.

Usage:
    python ebt-db-scripts/run_unified_pipeline.py

Options:
    --db PATH          Database path (default: data/db/EBT_Unified.db)
    --output DIR       Output directory (default: data/output)
    --tbw-dir DIR     TBW HTML directory (default: data/bw2_20260118)
    --no-ai          Skip AI-assisted mapping
    -v, --verbose    Verbose logging
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ebt_translations.unified import run_unified_pipeline
from ebt_translations.unified.models import UnifiedConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Run Unified EBT Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--db", type=Path,
        default=Path("data/db/EBT_Unified.db"),
        help="Database path",
    )
    parser.add_argument(
        "--output", type=Path,
        default=Path("data/output"),
        help="Output directory",
    )
    parser.add_argument(
        "--tbw-dir", type=Path,
        default=Path("data/bw2_20260118"),
        help="TBW HTML directory",
    )
    parser.add_argument(
        "--no-ai",
        action="store_true",
        help="Skip AI-assisted mapping",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose logging",
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    db_path = args.db
    if not db_path.exists():
        base = Path(__file__).parent.parent
        db_path = base / "data" / "db" / "EBT_Unified.db"
    
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        sys.exit(1)
    
    logger.info(f"Using database: {db_path}")
    
    config = UnifiedConfig(
        db_path=str(db_path),
        output_dir=str(args.output),
        tbw_html_dir=str(args.tbw_dir),
        use_ai_for_pau=not args.no_ai,
    )
    
    stats = run_unified_pipeline(config)
    
    logger.info("\n" + "=" * 40)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 40)
    logger.info(f"  Total extracted: {stats.get('total_extracted', 0)}")
    logger.info(f"  Total valid: {stats.get('total_valid', 0)}")
    logger.info(f"  Duplicates removed: {stats.get('duplicates_removed', 0)}")
    logger.info(f"  Invalid: {stats.get('invalid', 0)}")


if __name__ == "__main__":
    main()