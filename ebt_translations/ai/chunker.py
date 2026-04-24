"""Text chunking for AI pipeline."""

import re
from typing import List
from dataclasses import dataclass

from . import config


@dataclass
class TextChunk:
    """A chunk of text with metadata."""
    text: str
    source_table: str
    segment_id: int
    chunk_index: int
    word_count: int


class TextChunker:
    """Split text into chunks for processing."""
    
    def __init__(self, max_words: int = None, overlap: int = None):
        self.max_words = max_words or config.MAX_CHUNK_WORDS
        self.overlap = overlap or config.CHUNK_OVERLAP
    
    def chunkify(self, text: str, source_table: str, segment_id: int) -> List[TextChunk]:
        """Split text into word-limited chunks."""
        if not text or not text.strip():
            return []
        
        paragraphs = self._split_paragraphs(text)
        chunks = []
        current_chunk = []
        current_words = 0
        chunk_index = 0
        
        for para in paragraphs:
            para_words = self._count_words(para)
            
            if current_words + para_words > self.max_words and current_chunk:
                chunks.append(self._create_chunk(
                    current_chunk, source_table, segment_id, chunk_index
                ))
                overlap_text = self._get_overlap_text(current_chunk)
                current_chunk = [overlap_text] if overlap_text else []
                current_words = self._count_words(" ".join(current_chunk))
                chunk_index += 1
            
            current_chunk.append(para)
            current_words += para_words
        
        if current_chunk:
            chunks.append(self._create_chunk(
                current_chunk, source_table, segment_id, chunk_index
            ))
        
        return chunks
    
    def _split_paragraphs(self, text: str) -> List[str]:
        """Split text into paragraphs."""
        paras = re.split(r'\n\s*\n', text)
        return [p.strip() for p in paras if p.strip()]
    
    def _count_words(self, text: str) -> int:
        """Count words in text."""
        return len(text.split())
    
    def _create_chunk(self, parts: List[str], source_table: str, 
                     segment_id: int, chunk_index: int) -> TextChunk:
        """Create a TextChunk from parts."""
        text = " ".join(parts)
        return TextChunk(
            text=text,
            source_table=source_table,
            segment_id=segment_id,
            chunk_index=chunk_index,
            word_count=self._count_words(text)
        )
    
    def _get_overlap_text(self, parts: List[str]) -> str:
        """Extract overlap text from previous chunk."""
        text = " ".join(parts)
        words = text.split()
        if len(words) <= self.overlap:
            return ""
        return " ".join(words[-self.overlap:])