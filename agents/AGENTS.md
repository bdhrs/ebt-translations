# EBT Project – Agent Instructions

## Project Overview

A database and toolset for collecting, comparing, and presenting multiple translations of Early Buddhist Texts (EBTs). Covers Vinaya + Sutta Piṭaka: DN, MN, SN, AN, KN.

## Tech Stack

- **Language:** Python 3.13+
- **Package manager:** `uv` (all scripts run via `uv run python ...`)
- **Database:** SQLite
- **Data formats:** Markdown, JSON, Excel (.xlsx)

## Project Structure

```
ebt-translations/
├── ebt_translations/       # Core package
│   ├── ingestion/       # Data ingestion modules
│   ├── expansion/      # Expansion modules
│   ├── quality/       # Quality pipeline
│   ├── coverage/      # Coverage analysis
│   └── unified/       # Unified pipeline
│
├── ebt-db-scripts/     # CLI scripts
│   ├── run_pipeline.py
│   ├── build_unified_db.py
│   └── ...
│
├── data/
│   ├── db/            # SQLite databases (gitignored)
│   ├── input/         # Input files (gitignored)
│   ├── output/        # Generated outputs
│   └── reports/      # Analysis reports
│
├── docs/
│   ├── PROJECT_ANALYSIS.md
│   ├── ARCHITECTURE.md
│   └── DATA_SOURCES.md
│
├── agents/
├── tests/
├── README.md
└── .gitignore
```

---

## DO

### Before Coding
- Always read all relevant files first
- Always understand the current state of the pipeline
- Review existing code patterns in the project

### Planning
- Always plan before coding
- Show the plan before execution
- Refine plan with feedback before proceeding

### Implementation
- Follow deterministic logic first
- Use AI only when deterministic is not possible
- Validate before inserting data

### Code Quality
- Maintain clean repo structure
- Use pathlib.Path for all file paths
- Keep code simple — no over-engineering

### Documentation
- Update this file after mistakes or learnings
- Document new workflows in docs/

---

## DO NOT

### Data Integrity
- Do not modify sutta_master (canonical source)
- Do not insert duplicate data
- Do not overwrite valid existing data

### Assumptions
- Do not assume mappings are correct
- Do not assume sources are available
- Do not assume scraping is possible

### Over-engineering
- Do not add unnecessary comments
- Do not create complex abstractions for simple tasks
- Do not introduce environment variables

### Git
- Do not auto-commit
- Do not commit generated data files
- Do not commit database files

---

## Prompt Strategy

### Be Explicit
- Define exact scope
- Specify input and output formats
- Provide examples

### Provide Context
- Reference existing files
- Explain what was tried before
- Include error messages if applicable

### Examples for Outputs
```markdown
# Good prompt
Extract translations from sc_dn table in data/db/EBT_Unified.db.
Output: list of {"sutta_number", "translation_text"}
```

---

## Planning Rules

### Show Plan First
1. List files to read
2. Explain approach
3. Show pseudocode
4. Ask for confirmation

### Refine with Feedback
- Adjust based on user input
- Acknowledge limitations
- Highlight risks

---

## Execution Rules

### Smallest Working First
- Start with minimal viable solution
- Test manually after each step
- Iterate based on results

### Error Handling
- Catch and log errors
- Do not crash silently
- Provide meaningful error messages

---

## Git Rules

### Before Commit
- Review changes with `git diff`
- Ensure no database files included
- Verify .gitignore is correct

### Commit Messages
- Commit per logical unit
- Use clear, concise messages
- Example: "Add unified pipeline module"

---

## Failure Handling

### If Something Breaks
1. Revert broken changes
2. Update this agents file
3. Improve the plan
4. Start again

### Common Issues
- **Data availability**: Not all sources have complete data
- **Scraping blocked**: Many sites block scraping
- **ID mapping**: PAU uses non-standard IDs
- **Offline only**: Some sources require local files

---

## Quick Reference

### Run Pipeline
```bash
uv sync
uv run python ebt-db-scripts/run_pipeline.py
```

### Run Tests
```bash
uv run pytest -v
```

### Check Coverage
```bash
uv run python ebt-db-scripts/check_coverage.py
```

### Database Path
- `data/db/EBT_Unified.db`

---

## Key Files to Know

- `ebt_translations/paths.py` – Path configuration
- `ebt_translations/pipeline.py` – Main pipeline
- `ebt_translations/ingestion/` – Ingestion modules
- `ebt_db_scripts/run_pipeline.py` – Pipeline runner