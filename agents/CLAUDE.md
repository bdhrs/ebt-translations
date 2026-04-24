# Claude (Anthropic) – Usage Notes

## How AI Agents Were Used

### Planning-First Workflow

1. **Ask Questions First**
   - Clarified project goals
   - Understood data constraints
   - Identified real limitations

2. **Analysis Before Building**
   - Ran SQL queries to understand data
   - Analyzed source coverage
   - Identified root causes

3. **Build with Verification**
   - Created unified pipeline module
   - Ran and verified output
   - Fixed bugs iteratively

---

## Model Selection

### Primary Model
- **Claude (Sonnet/Haiku)** - Used for:
  - Planning and analysis
  - Code generation
  - Document creation

### Fallback
- **Qwen/Ollama** - Used for:
  - PAU ID mapping (when available)
  - Complex normalization

---

## Prompt Engineering Approach

### For Analysis Tasks
```
You are a Senior Software Engineer.
Analyze the EBT project database.

Do:
- Run SQL queries to get facts
- Explain what data shows
- Identify root causes

Do NOT:
- modify database
- run scripts
- assume data availability
```

### For Build Tasks
```
You are a Senior Software Engineer.
Create a pipeline module for EBT.

Requirements:
- Use existing patterns
- Handle all edge cases
- Output to JSON Lines

Files to read first:
- ebt_translations/paths.py
- ebt_translations/ingestion/loader.py
```

---

## Key Learnings

### 1. Planning Mode is Valuable
- Before making changes, analyze the current state
- ASK QUESTIONS when unclear
- Identify what cannot be changed

### 2. Data Availability is Fundamental
- The system CAN extract and link data
- The limitation is SOURCE DATA AVAILABILITY
- External sources limit what can be scraped

### 3. PAU ID Mapping is Broken
- PAU uses `s1`, `s10` format
- Standard is `sn1.1`, `sn1.10`
- Without AI, 20k records unmappable

### 4. Some Sources are Exhausted
- DT (Thanissaro) site blocks scraping
- ATI web scraping not viable
- Only offline files available

### 5. KN Coverage is Weak
- KN (Khuddaka Nikaya) at 42%
- Subcollections incomplete
- Opportunity for improvement

---

## Workflow Used

### Initial Analysis
```python
# SQL queries to understand data
SELECT COUNT(*) FROM sutta_master
SELECT COUNT(DISTINCT sutta_number) FROM sc_sn WHERE translation_text IS NOT NULL
```

### Pipeline Creation
```python
# Created unified module
ebt_translations/unified/
├── __init__.py
├── models.py
├── extract_tbw.py
├── extract_db.py
├── extract_pau.py
├── orchestrator.py
```

### Verification
```python
# Output verification
uv run python ebt-db-scripts/run_unified_pipeline.py
# Check data/output/ebt_unified.jsonl
```

---

## What Worked Well

1. **Read-First Approach**
   - Always read relevant files before coding
   - Understand existing patterns

2. **Deterministic First**
   - Try simple logic before AI
   - Use AI only when needed

3. **Verification Loop**
   - Run and check output
   - Fix bugs incrementally

---

## What Didn't Work

1. **Over-ambitious PAU Mapping**
   - Tried to map 20k PAU records with AI
   - Too slow (timed out)
   - Needs separate project

2. **HTML Extraction**
   - TBW HTML parsing very slow
   - DB already has the data
   - No need to re-extract

---

## Files Created by AI

- `docs/ARCHITECTURE.md` - System design
- `docs/DATA_SOURCES.md` - Source documentation
- `agents/AGENTS.md` - Project instructions
- `PROJECT_ANALYSIS.md` - Analysis report

---

## Notes for Future Work

1. **Focus on KN**
   - Dhammapada, Itivuttaka, Sutta Nipata
   - Subcollections need data

2. **PAU Separate Project**
   - Batch AI mapping required
   - Time-intensive but possible

3. **Alternative Sources**
   - New translation sources needed
   - Current sources maxed out