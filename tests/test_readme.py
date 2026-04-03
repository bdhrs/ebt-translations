from pathlib import Path


def test_readme_documents_uv_setup_and_data_directories():
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text(encoding="utf-8")

    assert "uv sync" in readme
    assert "uv run" in readme
    assert "data/db" in readme
    assert "data/input" in readme
    assert "paths.py" in readme


def test_pyproject_includes_excel_reader_dependency():
    pyproject = (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text(encoding="utf-8")

    assert "openpyxl" in pyproject
