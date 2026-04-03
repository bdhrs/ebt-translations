from pathlib import Path


def test_default_paths_are_repo_relative():
    from ebt_translations.paths import DATA_DIR, DB_DIR, EXCEL_FILE_PATH, OLD_DB_PATH, PROJECT_ROOT, UNIFIED_DB_PATH

    assert PROJECT_ROOT == Path(__file__).resolve().parents[1]
    assert DATA_DIR == PROJECT_ROOT / "data"
    assert DB_DIR == DATA_DIR / "db"
    assert OLD_DB_PATH == DB_DIR / "EBT_Suttas.db"
    assert UNIFIED_DB_PATH == DB_DIR / "EBT_Unified.db"
    assert EXCEL_FILE_PATH == DATA_DIR / "input" / "Massive Table of Sutta Data.xlsx"


def test_ensure_data_directories_creates_expected_paths():
    from ebt_translations.paths import DB_DIR, INPUT_DIR, ensure_data_directories

    ensure_data_directories()

    assert DB_DIR.is_dir()
    assert INPUT_DIR.is_dir()
