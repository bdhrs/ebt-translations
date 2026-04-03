# Plan: Repo Cleanup and Shareable Commit

## Phase 1 — Fix .gitignore
- [ ] Add `data/input/` to .gitignore
- [ ] Verify .gitignore covers __pycache__, .venv, .pytest_cache, data/db/*.db, data/input/

## Phase 2 — Preserve data directory structure
- [ ] Add `data/db/.gitkeep`
- [ ] Add `data/input/.gitkeep`
- [ ] Phase check: git status shows .gitkeep files as new

## Phase 3 — Commit everything
- [ ] Stage all appropriate files
- [ ] Create commit
- [ ] Phase check: git status is clean

## Phase 4 — Produce handover instructions
- [ ] Write instructions for original creator
