from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parents[1] / "ebt-db-scripts"
SCRIPT_PATHS = sorted(SCRIPT_DIR.glob("*.py"))


def test_scripts_do_not_contain_windows_machine_paths():
    assert SCRIPT_PATHS

    for script_path in SCRIPT_PATHS:
        content = script_path.read_text(encoding="utf-8")
        assert r"C:\Users" not in content, script_path.name
