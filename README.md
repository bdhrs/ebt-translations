# ebt-translations

A database and toolset for collecting, comparing, and presenting multiple translations of Early Buddhist Texts (EBTs).

## Setup

This repo now uses `uv` so Windows, macOS, and Linux users can run the same commands.

1. Install `uv`.
2. Clone the repo.
3. Run `uv sync`.

That creates a local `.venv` and installs the project dependencies.

The day-to-day command pattern is the same on every platform:

```bash
uv sync
uv run pytest -v
uv run python ebt-db-scripts/download_massive_table.py
uv run python ebt-db-scripts/build_unified_db.py
```

## Project Data Layout

By default the scripts read and write project data inside this repo:

- `data/db/EBT_Suttas.db`
- `data/db/EBT_Unified.db`
- `data/input/Massive Table of Sutta Data.xlsx`

The `data/db/` and `data/input/` directories are created automatically when needed.

All project paths are defined in `ebt_translations/paths.py`.
If the repo owner wants different locations, edit that file directly.

## Running Scripts

All platforms use the same command style:

```bash
uv run python ebt-db-scripts/download_massive_table.py
uv run python ebt-db-scripts/check_unified_schema.py
uv run python ebt-db-scripts/build_unified_db.py
uv run python ebt-db-scripts/scrape_dt.py
uv run python ebt-db-scripts/scrape_tipitaka.py
uv run python ebt-db-scripts/scrape_tp.py
uv run python ebt-db-scripts/scrape_all_sources.py
uv run python ebt-db-scripts/run_pipeline.py
```

## Expected Local Inputs

Some scripts depend on local files or previously built databases:

- `build_unified_db.py` expects `data/db/EBT_Suttas.db`
- `download_massive_table.py` fetches `data/input/Massive Table of Sutta Data.xlsx`
- scraper scripts may also expect source-specific tables to already exist in `EBT_Suttas.db`

## Pipeline

To run the repo in its current expected order:

```bash
uv run python ebt-db-scripts/run_pipeline.py
```

That pipeline runs:

1. `download_massive_table.py`
2. `scrape_dt.py`
3. `scrape_tipitaka.py`
4. `scrape_tp.py`
5. `build_unified_db.py`
6. `check_unified_schema.py`
7. `scrape_all_sources.py`

So the setup is portable, but a full successful run still depends on having the required source data locally.

## Tech Stack

- **Language:** Python
- **Database:** SQLite
- **Data formats:** Markdown, JSON, Excel

## Scope

Vinaya + Sutta Piṭaka EBTs: DN, MN, SN, AN, KN 1–9

## Development Stages

- [ ] 0. Planning
- [ ] 1. Data collection — build database, extract from all sources
- [ ] 2. Prompting / processing
- [ ] 3. Front end

## Sources

### 1. DPD Massive Table of Sutta Data

- [Google Spreadsheet](https://docs.google.com/spreadsheets/d/1sR8NT204STTwOoDrr9GBjhXVYEn0qqZTxgjoLKMmaaE/edit?usp=sharing)
- Mapping for each sutta number and name across all different data sources

### 2. CST — Chaṭṭha Saṅgāyana Tipiṭaka (6th Council)

- [Repo](https://github.com/vipassanatech/tipitaka-xml)
- [Roman script](https://github.com/VipassanaTech/tipitaka-xml/tree/main/romn)
- [Devanagari](https://github.com/VipassanaTech/tipitaka-xml/tree/main/deva)
- Format: XML, UTF-16
- Use BeautifulSoup to extract — see [this extraction example](https://github.com/digitalpalidictionary/dpd-db/blob/95830f8502c32e13d71963747ad4600e65e8de3c/scripts/build/cst4_xml_to_txt.py)
- Note: CST is organised per book, not per sutta — suttas must be extracted from within each book

### 3. SuttaCentral

- [Repo](https://github.com/suttacentral/sc-data/)
- [Pāḷi texts](https://github.com/suttacentral/sc-data/tree/main/sc_bilara_data/root/pli/ms)
- [English translations (Sujato)](https://github.com/suttacentral/sc-data/blob/main/sc_bilara_data/translation/en/sujato/sutta/)
- Keys in the Pāḷi text match keys in the English translation (bilara JSON format)

### 4. TBW — The Buddha's Words (Bhikkhu Bodhi)

- [Download ZIP](https://drive.google.com/drive/folders/1HawM4A_Ns37VGpHgH4YFpkkJpjtpNLEw) — offline website
- [Online version](https://find.dhamma.gift/bw/dn/dn1.html)
- Use Bhikkhu Bodhi translations (not Sujato)

### 5. Dhammatalks.org (Bhikkhu Thanissaro)

- [Website](https://www.dhammatalks.org/suttas/)
- No known repo — scrape required
- Partial list of suttas only

### 6. Pa Auk AI Translations

- [Website](https://tipitaka.paauksociety.org/)
- [Repo](https://github.com/digitalpalidictionary/tipitaka-translation-db) — find DB in Releases
- Column: `english_translation` in each table

### 7. ePitaka AI Translation

- [Website](https://epitaka.org/tpk/)
- Repo unknown — find the developer and repo

### 8. Indian Spoken English Translation

### 9... Hindi, Kannada, Telugu, Tamil, Marathi, and other Indian language translations
