# Gemini (Google) – Usage Notes

## How AI Agents Were Used

### Same Approach as Claude

The workflow was essentially identical to how Claude was used:

1. **Analysis Mode** - Run queries, understand data
2. **Plan Mode** - Create structured plan
3. **Build Mode** - Execute and verify
4. **Documentation** - Create reports

---

## Model Selection

### Primary Model
- **Gemini Pro/Flash** - Used for:
  - Complex analysis
  - Code generation
  - Multi-step reasoning

### Characteristics
- Strong reasoning for SQL queries
- Good at understanding database schema
- Effective for structured output

---

## Prompt Engineering

### For Database Analysis
```
Analyze the EBT SQLite database:
- Count total suttas
- Coverage per source
- Multi-source distribution

Output in markdown table format.
```

### For Pipeline Building
```
Create a Python module that:
1. Extracts from SQLite tables
2. Normalizes sutta IDs
3. Outputs to JSON Lines

Use existing patterns from:
- ebt_translations/ingestion/loader.py
```

---

## Key Differences from Claude

### 1. SQL Understanding
- Gemini tends to generate more complex SQL
- Sometimes needs SQL validation

### 2. Python Generation
- Good at following existing patterns
- Sometimes generates type hints that cause LSP errors

### 3. Planning
- Similar planning-first approach
- Often creates more detailed plans

---

## Shared Learnings

1. **Data Availability is Key**
   - Cannot exceed what sources provide
   - External limits apply

2. **Deterministic First**
   - Try simple logic before AI
   - Regex works for most ID normalization

3. **Verification Essential**
   - Always check output
   - Small iterations work better

---

## Files Created By Both Models

- `docs/PROJECT_ANALYSIS.md` - Analysis report
- `docs/ARCHITECTURE.md` - System design
- `docs/DATA_SOURCES.md` - Source documentation
- `agents/AGENTS.md` - Project instructions
- `agents/CLAUDE.md` - Claude usage notes
- `agents/GEMINI.md` - Gemini usage notes
- `ebt_translations/unified/` - Unified pipeline

---

## Conclusion

Both Claude and Gemini can effectively:
- Analyze database state
- Create pipeline modules
- Generate documentation

Key is understanding the CONSTRAINTS:
- Data availability (not system)
- Scraping limitations
- ID mapping issues