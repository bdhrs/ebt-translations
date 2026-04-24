"""AI pipeline package."""

from .tipitaka_mapper import (
    parse_source_table,
    group_segments,
    validate_sutta,
    run_mapper,
    MappingResult,
)

__all__ = [
    "parse_source_table",
    "group_segments", 
    "validate_sutta",
    "run_mapper",
    "MappingResult",
]