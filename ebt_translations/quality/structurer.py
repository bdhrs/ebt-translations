"""Text structurer for EBT quality pipeline.

Convert all texts into:
{
  title,
  paragraphs[]
}

Ensure:
- consistent paragraph splitting
- no giant unstructured blocks
"""

import logging
import re
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class StructuredText:
    """Structured text result."""
    title: str
    paragraphs: list[str]
    original_text: str
    
    def __post_init__(self):
        if self.paragraphs is None:
            self.paragraphs = []


class TextStructurer:
    """Structure translation text into paragraphs."""
    
    def __init__(self):
        self._stats = {
            "titles_extracted": 0,
            "paragraphs_split": 0,
        }
    
    def structure(self, text: str, sutta_id: Optional[str] = None) -> StructuredText:
        """Structure text into title + paragraphs.
        
        Args:
            text: Raw translation text
            sutta_id: Optional sutta ID for title extraction
            
        Returns:
            StructuredText
        """
        if not text:
            return StructuredText(title="", paragraphs=[], original_text="")
        
        original = text
        
        # Step 1: Clean up text first
        text = text.strip()
        
        # Step 2: Extract title
        title, text = self._extract_title(text, sutta_id)
        
        # Step 3: Split into paragraphs
        paragraphs = self._split_paragraphs(text)
        
        # Step 4: Clean paragraphs
        paragraphs = self._clean_paragraphs(paragraphs)
        
        if title:
            self._stats["titles_extracted"] += 1
        
        if len(paragraphs) > 1:
            self._stats["paragraphs_split"] += 1
        
        return StructuredText(
            title=title,
            paragraphs=paragraphs,
            original_text=original,
        )
    
    def _extract_title(self, text: str, sutta_id: Optional[str]) -> tuple[str, str]:
        """Extract title from text."""
        title = ""
        
        # Use sutta_id if provided
        if sutta_id:
            title = sutta_id.upper()
        
        # Try to find title from first line
        lines = text.split("\n")
        if lines:
            first = lines[0].strip()
            
            # If first line is short and looks like a title
            if len(first) < 100 and not first.endswith((".", ",", ";")):
                title = first
                text = "\n".join(lines[1:])
        
        return title, text
    
    def _split_paragraphs(self, text: str) -> list[str]:
        """Split text into paragraphs."""
        # Split by double newlines
        paragraphs = re.split(r"\n{2,}", text)
        
        # If no double newlines, try single
        if len(paragraphs) == 1:
            paragraphs = re.split(r"\n", text)
        
        # Filter empty
        paragraphs = [p.strip() for p in paragraphs if p.strip()]
        
        return paragraphs
    
    def _clean_paragraphs(self, paragraphs: list[str]) -> list[str]:
        """Clean paragraphs."""
        cleaned = []
        
        for para in paragraphs:
            # Remove leading/trailing whitespace
            para = para.strip()
            
            # Remove very short paragraphs that are likely noise
            if len(para) < 20:
                continue
            
            # Capitalize first letter if not already
            if para and para[0].islower():
                para = para[0].upper() + para[1:]
            
            cleaned.append(para)
        
        return cleaned
    
    def to_dict(self, structured: StructuredText) -> dict:
        """Convert to dictionary format."""
        return {
            "title": structured.title,
            "paragraphs": structured.paragraphs,
            "paragraph_count": len(structured.paragraphs),
            "char_count": sum(len(p) for p in structured.paragraphs),
        }
    
    def structure_batch(self, texts: list[dict]) -> list[dict]:
        """Structure multiple texts.
        
        Args:
            texts: [{"sutta_number", "source_id", "text"}, ...]
            
        Returns:
            Structured texts
        """
        results = []
        
        for trans in texts:
            text = trans.get("text", "")
            sutta_id = trans.get("sutta_number")
            
            structured = self.structure(text, sutta_id)
            
            results.append({
                "sutta_number": trans.get("sutta_number"),
                "source_id": trans.get("source_id"),
                "title": structural.title,
                "paragraphs": structured.paragraphs,
                "paragraph_count": len(structured.paragraphs),
            })
        
        return results
    
    def get_stats(self) -> dict:
        """Get structuring statistics."""
        return self._stats.copy()


def structure_text(text: str) -> StructuredText:
    """Quick structuring function."""
    structurer = TextStructurer()
    return structurer.structure(text)