# ebt-translations — Agent Instructions

## Project Overview

A database and toolset for collecting, comparing, and presenting multiple translations of Early Buddhist Texts (EBTs). Covers Vinaya + Sutta Piṭaka: DN, MN, SN, AN, KN 1–9.

## Tech Stack

- **Language:** Python 3.13
- **Package manager:** `uv` (all scripts run via `uv run python ...`)
- **Database:** SQLite
- **Data formats:** Markdown, JSON, Excel (.xlsx)

## Project Structure

```
ebt-db-scripts/     # scraping and database build scripts
src/                # main source package
tests/              # test suite
data/
  db/               # SQLite databases (EBT_Suttas.db, EBT_Unified.db)
  input/            # input files (Massive Table of Sutta Data.xlsx)
docs/               # planning documents
```

## Running Scripts

```bash
uv run python ebt-db-scripts/check_unified_schema.py
uv run python ebt-db-scripts/build_unified_db.py
uv run python ebt-db-scripts/scrape_dt.py
uv run python ebt-db-scripts/scrape_tipitaka.py
uv run python ebt-db-scripts/scrape_tp.py
uv run python ebt-db-scripts/scrape_all_sources.py
```

## Environment Variables

Override default data paths if needed:

- `EBT_PROJECT_ROOT`
- `EBT_OLD_DB`
- `EBT_UNIFIED_DB`
- `EBT_EXCEL_FILE`

## Rules

- Do not run scripts unless the user explicitly asks.
- Do not commit or push to git unless explicitly instructed.
- Do not modify `.env` or `.ini` files.
- Keep code simple — no over-engineering.
- Do not add unnecessary comments.
