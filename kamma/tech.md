---
name: ebt-translations tech stack
type: tech
---

# Tech Stack

- **Language:** Python 3.13 (requires >= 3.11)
- **Package manager:** `uv` — all scripts run via `uv run python ...`
- **Database:** SQLite (`.db` files in `data/db/`)
- **Data formats:** Excel `.xlsx`, JSON, Markdown
- **Test runner:** pytest (via `uv run pytest -v`)
- **Linter:** ruff (via `uv run ruff check`)

## Key packages
- `beautifulsoup4` — HTML scraping
- `openpyxl` — Excel reading
- `pandas` — data manipulation
- `requests` — HTTP downloads

## Project layout
- `ebt_translations/` — importable package (paths, pipeline, downloads)
- `ebt-db-scripts/` — standalone scripts run via pipeline
- `tests/` — pytest suite
- `data/db/` — SQLite databases (gitignored)
- `data/input/` — input files like the Massive Table xlsx (gitignored)
- `docs/` — planning documents
