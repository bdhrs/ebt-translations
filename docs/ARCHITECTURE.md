# EBT Project Architecture

## System Overview

The EBT (Early Buddhist Texts) project is a data engineering system that aggregates multiple translations of Buddhist suttas from various sources into a unified SQLite database.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     EBT Pipeline                           │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐            │
│  │Scraping  │───▶│Ingestion │───▶│ Quality │───▶│ DB     │
│  └──────────┘    └──────────┘    └──────────┘            │
│       │                │                                  │
│       ▼                ▼                                  │
│  ┌──────────┐    ┌──────────┐                            │
│  │ Offline  │    │Expansion │                           │
│  │ Sources  │    │  Pipeline│                           │
│  └──────────┘    └──────────┘                            │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Ingestion Pipeline (`ebt_translations/ingestion/`)

- **Loader**: Load data from DB, XML, HTML sources
- **Normalizer**: Normalize sutta IDs to canonical format
- **Validator**: Validate against sutta_master
- **Inserter**: Insert into source tables
- **Tracker**: Track operations (inserted/skipped/failed)
- **Deduplicator**: Remove exact duplicates

### 2. Expansion Pipeline (`ebt_translations/expansion/`)

- **Source Expander**: Expand existing sources
- **ATI Expander**: Expand from ATI offline files
- **TPK Mapper**: Map TPK data
- **PAU Linker**: Link PAU translations

### 3. Quality Pipeline (`ebt_translations/quality/`)

- **Cleaner**: Clean HTML/text
- **Filter**: Filter unwanted content
- **Scorer**: Score quality
- **Pack Builder**: Build translation packs
- **Deduplicator**: Remove duplicates

### 4. CLI Scripts (`ebt-db-scripts/`)

- `run_pipeline.py`: Main pipeline runner
- `build_unified_db.py`: Build unified database
- `check_unified_schema.py`: Verify DB schema
- `scrape_dt.py`: Scrape Dhamma Talks
- `scrape_tipitaka.py`: Scrape Tipitaka
- `scrape_all_sources.py`: Scrape all sources

## Database Schema

### Core Tables

```sql
sources          -- Source metadata
nikayas         -- Nikaya definitions
kn_subcollections -- KN sub-collections
sutta_master    -- Master list of all suttas
source_availability -- Links suttas to sources
```

### Source Tables

Each source has per-nikaya tables:

- `sc_dn`, `sc_mn`, `sc_sn`, `sc_an`, `sc_kn`
- `tbw_dn`, `tbw_mn`, `tbw_sn`, `tbw_an`, `tbw_kn`
- `dt_dn`, `dt_mn`, `dt_sn`
- `ati_dn`, `ati_mn`, `ati_sn`, `ati_an`, `ati_kn`
- `pau_dn`, `pau_mn`, `pau_sn`, `pau_an`, `pau_kn`
- `tpk_dn`, `tpk_mn`, `tpk_sn`, `tpk_an`, `tpk_kn`

### Table Schema

```sql
CREATE TABLE source_NIKAYA (
    id INTEGER PRIMARY KEY,
    sutta_number TEXT UNIQUE,
    sutta_title TEXT,
    pali_text TEXT,
    translation_text TEXT,
    source_url TEXT,
    char_count INTEGER,
    is_complete BOOLEAN,
    last_updated TIMESTAMP
);
```

## Data Flow

### Primary Flow

1. **Source Data** → Scrapers / Offline files
2. **Raw Data** → Loader (`ingestion/loader.py`)
3. **Normalization** → Sutta ID canonicalization (`ingestion/normalizer.py`)
4. **Validation** → Check against sutta_master (`ingestion/validator.py`)
5. **Deduplication** → Remove duplicates (`ingestion/deduplicator.py`)
6. **Insertion** → Source tables (`ingestion/inserter.py`)
7. **Tracking** → Update `source_availability`

### Quality Flow

1. **Raw Translation** → Cleaner (`quality/cleaner.py`)
2. **Clean Text** → Filter (`quality/filter.py`)
3. **Filtered Text** → Quality Score (`quality/scorer.py`)
4. **Output** → Translation Packs (`quality/pack_builder.py`)

## File Structure

```
ebt-translations/
├── ebt_translations/        # Core package
│   ├── __init__.py
│   ├── paths.py             # Path configuration
│   ├── pipeline.py         # Pipeline runner
│   ├── ingestion/          # Ingestion modules
│   ├── expansion/         # Expansion modules
│   ├── quality/           # Quality modules
│   ├── coverage/          # Coverage analysis
│   ├── unified/           # Unified pipeline
│   └── utils/             # Utilities
│
├── ebt-db-scripts/         # CLI scripts
│   ├── run_pipeline.py
│   ├── build_unified_db.py
│   ├── scrape_dt.py
│   └── ...
│
├── data/
│   ├── db/                # SQLite databases
│   ├── input/             # Input files
│   ├── processed/         # Output data
│   └── reports/          # Coverage reports
│
├── docs/
│   ├── PROJECT_ANALYSIS.md
│   ├── ARCHITECTURE.md
│   └── DATA_SOURCES.md
│
├── agents/
│   ├── AGENTS.md
│   ├── CLAUDE.md
│   └── GEMINI.md
│
├── tests/                 # Test suite
├── README.md
├── .gitignore
├── pyproject.toml
└── uv.lock
```

## Configuration

- **Database path**: `ebt_translations/paths.py`
- **UV config**: `pyproject.toml`
- **Project root**: Resolved via `paths.py`

## Dependencies

- Python 3.13+
- uv (package manager)
- sqlite3 (built-in)
- pandas (Excel processing)
- beautifulsoup4 (HTML parsing)
- requests (HTTP)

## Notes

- All paths use `pathlib.Path`
- No environment variable configuration
- Database files are gitignored
- Cross-platform (Windows/macOS/Linux)