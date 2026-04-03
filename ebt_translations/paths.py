from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DB_DIR = DATA_DIR / "db"
INPUT_DIR = DATA_DIR / "input"

OLD_DB_PATH = DB_DIR / "EBT_Suttas.db"
UNIFIED_DB_PATH = DB_DIR / "EBT_Unified.db"
EXCEL_FILE_PATH = INPUT_DIR / "Massive Table of Sutta Data.xlsx"


def ensure_data_directories() -> None:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
