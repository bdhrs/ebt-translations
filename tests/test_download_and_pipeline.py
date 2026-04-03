from pathlib import Path


def test_massive_table_download_target():
    from ebt_translations.downloads import MASSIVE_TABLE_SHEET_ID, build_google_sheet_export_url
    from ebt_translations.paths import EXCEL_FILE_PATH

    assert MASSIVE_TABLE_SHEET_ID == "1sR8NT204STTwOoDrr9GBjhXVYEn0qqZTxgjoLKMmaaE"
    assert build_google_sheet_export_url(MASSIVE_TABLE_SHEET_ID) == (
        "https://docs.google.com/spreadsheets/d/"
        "1sR8NT204STTwOoDrr9GBjhXVYEn0qqZTxgjoLKMmaaE/export?format=xlsx"
    )
    assert EXCEL_FILE_PATH.name == "Massive Table of Sutta Data.xlsx"


def test_pipeline_runs_scripts_in_fixed_order():
    from ebt_translations.pipeline import PIPELINE_SCRIPTS, script_label

    names = [path.name for path in PIPELINE_SCRIPTS]

    assert names == [
        "download_massive_table.py",
        "scrape_dt.py",
        "scrape_tipitaka.py",
        "scrape_tp.py",
        "build_unified_db.py",
        "check_unified_schema.py",
        "scrape_all_sources.py",
    ]
    assert script_label(PIPELINE_SCRIPTS[0]) == "download_massive_table"
    assert all(path.parent == Path("ebt-db-scripts") for path in PIPELINE_SCRIPTS)
