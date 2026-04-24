"""CLI runner for TPK mapper."""

import sys
import logging
from pathlib import Path

from ebt_translations.paths import UNIFIED_DB_PATH, ensure_data_directories
from ebt_translations.expansion.tpk_mapper import run_tpk_mapper


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    dry_run = "--dry-run" in sys.argv

    ensure_data_directories()
    print(f"TPK Mapper")
    print(f"Database: {UNIFIED_DB_PATH}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print("=" * 60)

    result = run_tpk_mapper(str(UNIFIED_DB_PATH), dry_run=dry_run)

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Total source_tables: {result.get('total_source_tables', 0)}")
    print(f"Auto-mapped: {result.get('auto_mapped', 0)}")
    print(f"TPK-only: {result.get('tpk_only', 0)}")
    print(f"Invalid format: {result.get('invalid', 0)}")
    print(f"Text too short: {result.get('text_too_short', 0)}")
    print(f"Inserted: {result.get('inserted', 0)}")
    print(f"Failed: {result.get('failed', 0)}")
    print()
    print(f"TPK coverage: {result.get('tpk_count', 0)}/{result.get('total_suttas', 0)} ({result.get('tpk_pct', 0)}%)")
    print("=" * 60)


if __name__ == "__main__":
    main()