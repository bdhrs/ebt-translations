"""CLI runner for ATI expander."""

import sys
import logging
from pathlib import Path

from ebt_translations.paths import UNIFIED_DB_PATH, ensure_data_directories
from ebt_translations.expansion.ati_expander import run_ati_expander


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    limit = 0
    dry_run = "--dry-run" in sys.argv

    for arg in sys.argv[1:]:
        if arg.isdigit():
            limit = int(arg)

    ensure_data_directories()
    print(f"ATI Expander")
    print(f"Database: {UNIFIED_DB_PATH}")
    print(f"Limit: {limit or 'unlimited'}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print("=" * 60)

    result = run_ati_expander(str(UNIFIED_DB_PATH), limit=limit, dry_run=dry_run)

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Processed: {result.get('processed', 0)}")
    print(f"Inserted: {result.get('inserted', 0)}")
    print(f"Skipped: {result.get('skipped', 0)}")
    print(f"Duplicates: {result.get('duplicates', 0)}")
    print(f"Not found: {result.get('not_found', 0)}")
    print(f"Text too short: {result.get('text_short', 0)}")
    print(f"Failed: {result.get('failed', 0)}")
    print()
    cov_before = result.get('coverage_before', {})
    cov_after = result.get('coverage_after', {})
    print(f"Coverage Before: ATI={cov_before.get('ati', 0)}")
    print(f"Coverage After:  ATI={cov_after.get('ati', 0)}")
    print("=" * 60)


if __name__ == "__main__":
    main()