"""EBT Translations Ingestion Module.

Production-grade data ingestion system for merging multiple sources
into the unified SQLite database with deduplication and validation.
"""

from ebt_translations.ingestion.loader import SourceLoader
from ebt_translations.ingestion.normalizer import SuttaNormalizer
from ebt_translations.ingestion.deduplicator import Deduplicator
from ebt_translations.ingestion.validator import Validator
from ebt_translations.ingestion.inserter import Inserter
from ebt_translations.ingestion.tracker import IngestionTracker
from ebt_translations.ingestion.orchestrator import IngestionOrchestrator

__all__ = [
    "SourceLoader",
    "SuttaNormalizer",
    "Deduplicator",
    "Validator",
    "Inserter",
    "IngestionTracker",
    "IngestionOrchestrator",
]