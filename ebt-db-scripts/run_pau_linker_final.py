"""CLI runner for PAU linker final."""

import sys
import logging
from pathlib import Path

from ebt_translations.paths import UNIFIED_DB_PATH, ensure_data_directories
from ebt_translations.expansion.pau_linker_final import run_pau_linker


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    limit = 0
    dry_run = "--dry-run" in sys.argv

    for arg in sys.argv[1:]:
        if arg.isdigit():
            limit = int(arg)

    ensure_data_directories()
    print(f"PAU Linker Final")
    print(f"Database: {UNIFIED_DB_PATH}")
    print(f"Limit: {limit or 'unlimited'}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print("=" * 60)

    result = run_pau_linker(str(UNIFIED_DB_PATH), limit=limit, dry_run=dry_run)

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Processed: {result.get('processed', 0)}")
    print(f"Inserted: {result.get('inserted', 0)}")
    print(f"Duplicates: {result.get('duplicates', 0)}")
    print(f"Not found: {result.get('not_found', 0)}")
    print(f"Text too short: {result.get('text_short', 0)}")
    print(f"Failed: {result.get('failed', 0)}")
    print()
    cov_before = result.get('coverage_before', {})
    cov_after = result.get('coverage_after', {})
    print(f"Coverage Before: PAU={cov_before.get('pau', 0)}")
    print(f"Coverage After:  PAU={cov_after.get('pau', 0)}")
    print("=" * 60)


if __name__ == "__main__":
    main()