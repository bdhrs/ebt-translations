from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from ebt_translations.paths import PROJECT_ROOT


PIPELINE_SCRIPTS = [
    Path("ebt-db-scripts") / "download_massive_table.py",
    Path("ebt-db-scripts") / "scrape_dt.py",
    Path("ebt-db-scripts") / "scrape_tipitaka.py",
    Path("ebt-db-scripts") / "scrape_tp.py",
    Path("ebt-db-scripts") / "build_unified_db.py",
    Path("ebt-db-scripts") / "check_unified_schema.py",
    Path("ebt-db-scripts") / "scrape_all_sources.py",
]


def script_label(script_path: Path) -> str:
    return script_path.stem


def run_script(script_path: Path) -> None:
    absolute_path = PROJECT_ROOT / script_path
    print(f"\n=== Running {script_label(script_path)} ===")
    subprocess.run([sys.executable, str(absolute_path)], check=True, cwd=PROJECT_ROOT)


def run_pipeline() -> None:
    for script_path in PIPELINE_SCRIPTS:
        run_script(script_path)
