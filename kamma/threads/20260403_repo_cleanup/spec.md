# Spec: Repo Cleanup and Shareable Commit

## Overview
Get the repo into a clean, portable state ready for the original creator to pull and use.

## What it should do
- Fix .gitignore so large data files (Excel, .db) are excluded
- Add .gitkeep files so the data/ directory structure is preserved in git
- Commit all appropriate tracked files (code, tests, pyproject.toml, uv.lock, docs/, kamma/)
- Produce a ready-to-share commit on main

## Constraints
- Do not commit data/db/*.db or data/input/*.xlsx (large/downloadable files)
- Do not commit __pycache__ or .venv
- Do not run any scripts

## How we'll know it's done
- `git status` shows a clean working tree
- `data/db/` and `data/input/` exist in git with .gitkeep but no data files
- All source code, tests, pyproject.toml, uv.lock, docs/, and kamma/ are committed

## What's not included
- Pushing to GitHub (user will do that)
- Running the pipeline or tests in CI
