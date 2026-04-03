---
name: kamma workflow
type: workflow
---

# Kamma Workflow

## Per-task flow
1. Implement the task
2. Run tests if applicable (`uv run pytest -v`)
3. Mark task complete in plan.md
4. Commit at phase boundaries

## Phase completion
- Run `uv run pytest -v` and confirm all tests pass
- Commit with a descriptive message

## Commit style
- Use conventional commits: `chore:`, `feat:`, `fix:`, `docs:`
- Keep messages short and descriptive

## Rules
- Do not run scripts unless the user explicitly asks
- Do not commit or push unless explicitly instructed
- Use `pathlib.Path` for all filesystem paths in Python
