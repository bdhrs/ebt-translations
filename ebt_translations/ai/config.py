"""Configuration for AI pipeline."""

import os

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
MODEL = os.environ.get("OLLAMA_MODEL", "llama3:8b")

MAX_CHUNK_WORDS = 500
CHUNK_OVERLAP = 50

DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "data", "db", "EBT_Unified (1).db"
)

AI_TIMEOUT = 30
AI_RETRY_COUNT = 2