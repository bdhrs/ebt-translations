from __future__ import annotations

from pathlib import Path

import requests

from ebt_translations.paths import EXCEL_FILE_PATH, ensure_data_directories


MASSIVE_TABLE_SHEET_ID = "1sR8NT204STTwOoDrr9GBjhXVYEn0qqZTxgjoLKMmaaE"


def build_google_sheet_export_url(sheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"


def download_file(url: str, destination: Path, timeout: int = 60) -> Path:
    ensure_data_directories()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_suffix(destination.suffix + ".tmp")

    with requests.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        with temp_path.open("wb") as file_handle:
            for chunk in response.iter_content(chunk_size=1024 * 64):
                if chunk:
                    file_handle.write(chunk)

    temp_path.replace(destination)
    return destination


def download_massive_table(destination: Path = EXCEL_FILE_PATH) -> Path:
    url = build_google_sheet_export_url(MASSIVE_TABLE_SHEET_ID)
    return download_file(url, destination)
