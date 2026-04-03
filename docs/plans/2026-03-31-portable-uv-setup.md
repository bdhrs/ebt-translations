# Portable UV Setup Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the existing database scripts runnable on Linux and Windows with a `uv`-managed environment and repo-relative default paths.

**Architecture:** Introduce a small Python package that centralizes path resolution for databases and input files, then refactor the existing scripts to consume that shared configuration instead of hard-coded machine-specific paths. Add a minimal test suite around path behavior and document the setup and smoke-run workflow in the README.

**Tech Stack:** Python, uv, pytest, pandas, requests, beautifulsoup4, SQLite

---

### Task 1: Project Packaging

**Files:**
- Create: `pyproject.toml`
- Create: `src/ebt_translations/__init__.py`
- Create: `src/ebt_translations/config.py`

**Step 1: Write the failing test**

Create tests that import `ebt_translations.config` and assert repo-relative defaults are produced.

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_config.py -v`
Expected: FAIL because the package and config module do not exist yet.

**Step 3: Write minimal implementation**

Add the package layout, `pyproject.toml`, and a config module that resolves:
- project root
- `data/db/EBT_Suttas.db`
- `data/db/EBT_Unified.db`
- `data/input/Massive Table of Sutta Data.xlsx`

Allow overrides via environment variables.

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_config.py -v`
Expected: PASS

### Task 2: Script Portability Refactor

**Files:**
- Modify: `ebt-db-scripts/build_unified_db.py`
- Modify: `ebt-db-scripts/check_unified_schema.py`
- Modify: `ebt-db-scripts/scrape_all_sources.py`
- Modify: `ebt-db-scripts/scrape_dt.py`
- Modify: `ebt-db-scripts/scrape_tipitaka.py`
- Modify: `ebt-db-scripts/scrape_tp.py`

**Step 1: Write the failing test**

Add tests proving the scripts no longer contain hard-coded `C:\Users\...` paths and that the config directories are created locally.

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_scripts_portability.py -v`
Expected: FAIL because the scripts still contain Windows-only paths.

**Step 3: Write minimal implementation**

Refactor each script to import config helpers, use repo-relative defaults, and create parent directories where needed.

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_scripts_portability.py -v`
Expected: PASS

### Task 3: Documentation And Smoke Verification

**Files:**
- Modify: `README.md`
- Create: `data/db/.gitkeep`
- Create: `data/input/.gitkeep`

**Step 1: Write the failing test**

Add a small documentation test or assertion that the README includes `uv` setup and local data-path instructions.

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_readme.py -v`
Expected: FAIL because the current README does not document the new workflow.

**Step 3: Write minimal implementation**

Document setup, installation, environment overrides, and smoke-run commands. Add local data directories for expected defaults.

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_readme.py -v`
Expected: PASS

### Task 4: Fresh Verification

**Files:**
- Verify only

**Step 1: Run full targeted verification**

Run: `uv run pytest -v`
Expected: all tests pass

**Step 2: Run smoke commands**

Run:
- `uv run python ebt-db-scripts/check_unified_schema.py`
- `uv run python ebt-db-scripts/build_unified_db.py`

Expected:
- schema checker should fail clearly if the local database is absent, or succeed if present
- database builder should start with local paths and fail only on missing local inputs/tables, not on path or import issues
