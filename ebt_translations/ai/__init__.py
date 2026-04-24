"""AI-based sutta extraction pipeline for Buddhist text processing."""

__version__ = "0.1.0"

from .chunker import TextChunker
from .regex_detector import RegexDetector
from .ai_detector import AIDetector
from .aggregator import Aggregator
from .validator import Validator
from .pipeline.tipitaka_mapper import run_mapper

__all__ = [
    "TextChunker",
    "RegexDetector", 
    "AIDetector",
    "Aggregator",
    "Validator",
    "run_mapper",
]