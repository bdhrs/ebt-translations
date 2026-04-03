---
name: ebt-translations project
type: project
---

# ebt-translations

**Goal:** Build a database and toolset for collecting, comparing, and presenting multiple translations of Early Buddhist Texts (EBTs).

**Audience:** Developers and researchers working with Pāḷi canon translations. The repo should be portable and runnable by any collaborator on any OS.

**Scope:** Vinaya + Sutta Piṭaka — DN, MN, SN, AN, KN 1–9.

**Success criteria:**
- Any collaborator can clone, run `uv sync`, and execute the full pipeline with `uv run python ebt-db-scripts/run_pipeline.py`
- All data paths are relative to the repo root (no hardcoded machine paths)
- Source data files that are large or downloadable are not tracked in git
