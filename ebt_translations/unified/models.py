"""Unified EBT Pipeline - Data models."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SuttaTranslation:
    """A single translation of a sutta."""
    source_id: str
    sutta_number: str
    text: str
    is_primary: bool = True
    is_certain: bool = True


@dataclass
class SuttaEntry:
    """All translations for a single sutta."""
    sutta_number: str
    nikaya: str
    translations: list[SuttaTranslation] = field(default_factory=list)


@dataclass
class OperationResult:
    """Result of an operation."""
    operation: str
    source: str
    inserted: int = 0
    skipped: int = 0
    failed: int = 0
    message: str = ""


@dataclass
class UnifiedConfig:
    """Configuration for unified pipeline."""
    db_path: str = "data/db/EBT_Unified.db"
    output_dir: str = "data/output"
    reports_dir: str = "data/reports"
    tbw_html_dir: str = "data/bw2_20260118"
    min_text_length: int = 50
    min_word_count: int = 10
    use_ai_for_pau: bool = True
    ollama_url: str = "http://localhost:11434"