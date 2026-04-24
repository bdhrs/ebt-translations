# ebt-translations

A database and toolset for collecting, comparing, and presenting multiple translations of Early Buddhist Texts (EBTs).

## Project Overview

This project aggregates Buddhist suttas from multiple English translation sources into a unified database:

- **SuttaCentral** (SC) – Bhikkhu Sujato translations
- **The Buddha's Words** (TBW) – Bhikkhu Bodhi translations  
- **Dhamma Talks** (DT) – Thanissaro Bhikkhu translations
- **Access to Insight** (ATI) – Various translator translations

### Scope

- Digha Nikaya (DN) – 34 suttas
- Majjhima Nikaya (MN) – 152 suttas
- Samyutta Nikaya (SN) – 2,169 suttas
- Anguttara Nikaya (AN) – 1,743 suttas
- Khuddaka Nikaya (KN) – 2,720 suttas

**Total**: ~6,837 suttas

---

## What Problem It Solves

Different Buddhist translation sources use different:
- Sutta ID formats
- Organization schemes  
- Website structures

This project normalizes all translations and links them to a canonical sutta list (`sutta_master`), enabling:
- Cross-source comparison
- Translation quality analysis
- Multi-source datasets for AI training

---

## Architecture

```
ebt-translations/
├── ebt_translations/      # Core package
├── ebt-db-scripts/        # CLI scripts
├── data/
│   ├── db/               # SQLite (gitignored)
│   ├── input/            # Input files (gitignored)
│   ├── output/           # Generated datasets
│   └── reports/         # Coverage reports
├── docs/                 # Documentation
├── agents/               # AI agent instructions
└── README.md
```

---

## Setup

### UV Workflow

Instructions to pull and run the UV workflow:

```bash
git pull origin main
uv sync
uv run python ebt-db-scripts/run_pipeline.py
optional scripts
uv run pytest -v
```

### Steps Explained

1. **git pull origin main** – Get latest code
2. **uv sync** – Install dependencies
3. **run_pipeline.py** – Run main data pipeline
4. **optional scripts** – Run specific tasks (coverage, etc.)
5. **pytest -v** – Run tests

### Requirements

- Python 3.13+
- uv (package manager)

### Project Data

Local data directories (auto-created, gitignored):
- `data/db/EBT_Suttas.db` – Legacy database
- `data/db/EBT_Unified.db` – Current unified database
- `data/input/Massive Table of Sutta Data.xlsx` – Sutta mappings

---

## Running Scripts

### Main Pipeline
```bash
uv run python ebt-db-scripts/run_pipeline.py
```

### Individual Scripts
```bash
uv run python ebt-db-scripts/build_unified_db.py
uv run python ebt-db-scripts/check_unified_schema.py
uv run python ebt-db-scripts/check_coverage.py
```

### Tests
```bash
uv run pytest -v
```

---

## Data Sources

| Source | Coverage | Status |
|--------|----------|--------|
| SC | 69% | Primary |
| TBW | 62% | Maps to SC |
| DT | 15% | Scraping exhausted |
| ATI | 14% | Offline only |

See `docs/DATA_SOURCES.md` for details.

---

## Key Files

- `ebt_translations/paths.py` – Path configuration
- `ebt_translations/pipeline.py` – Main pipeline
- `ebt_translations/ingestion/` – Data ingestion modules
- `ebt_translations/unified/` – Unified pipeline

---

## Cross-Platform

This project uses `uv`, making commands identical on:
- Windows
- macOS
- Linux

---

## Documentation

- `docs/PROJECT_ANALYSIS.md` – Analysis and issues report
- `docs/ARCHITECTURE.md` – System design
- `docs/DATA_SOURCES.md` – Source documentation
- `agents/AGENTS.md` – Agent instructions

---

## License

- Translations: Various (CC BY-NC-SA 4.0, Public Domain, etc.)
- Code: MIT