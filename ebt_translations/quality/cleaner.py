"""Text cleaning engine for EBT quality pipeline.

Tasks:
- remove HTML tags
- remove navigation/footer text
- normalize whitespace
- fix broken paragraphs
- standardize punctuation

Qwen usage via Ollama ONLY for:
- repairing messy paragraphs
- improving readability without changing meaning
"""

import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class CleanedText:
    """Cleaned text result."""
    original: str
    cleaned: str
    was_modified: bool
    issues_fixed: list[str] = None
    
    def __post_init__(self):
        if self.issues_fixed is None:
            self.issues_fixed = []


class TextCleaner:
    """Clean and normalize translation text."""
    
    # Navigation/footer patterns to remove
    NAV_PATTERNS = [
        r"<nav[^>]*>.*?</nav>",
        r"<footer[^>]*>.*?</footer>",
        r"<header[^>]*>.*?</header>",
        r"<aside[^>]*>.*?</aside>",
        r'<div class="[^"]*(nav|footer|header|sidebar|menu)[^"]*"[^>]*>.*?</div>',
        r"<script[^>]*>.*?</script>",
        r"<style[^>]*>.*?</style>",
    ]
    
    # Noise patterns
    NOISE_PATTERNS = [
        r"\[/?[^\]]*\]",  # [...], [1], etc.
        r"\{[^{}]*\}",     # {...}
    ]
    
    # Empty/wrapper tags to remove
    EMPTY_TAGS = [
        r"<p>\s*</p>",
        r"<p>\s*<br\s*/>\s*</p>",
        r"<div>\s*</div>",
        r"<span>\s*</span>",
    ]
    
    def __init__(self, ollama_url: Optional[str] = None):
        self.ollama_url = ollama_url or "http://localhost:11434"
        self._ollama_available = None
        self.stats = {
            "html_tags_removed": 0,
            "whitespace_normalized": 0,
            "paragraphs_fixed": 0,
            "ai_refined": 0,
        }
    
    def clean(self, text: str, use_ai: bool = False) -> CleanedText:
        """Clean text.
        
        Args:
            text: Raw text input
            use_ai: Use AI for refinement
            
        Returns:
            CleanedText
        """
        if not text:
            return CleanedText(original=text, cleaned="", was_modified=False)
        
        original = text
        issues = []
        
        # Step 1: Remove HTML tags
        text = self._remove_html_tags(text)
        if text != original:
            self.stats["html_tags_removed"] += 1
            issues.append("html_removed")
        
        # Step 2: Remove navigation/footer
        text = self._remove_navigation(text)
        
        # Step 3: Normalize whitespace
        text = self._normalize_whitespace(text)
        if "\n\n\n" in text or "  " in text:
            self.stats["whitespace_normalized"] += 1
            issues.append("whitespace_normalized")
        
        # Step 4: Fix broken paragraphs
        text = self._fix_paragraphs(text)
        
        # Step 5: Standardize punctuation
        text = self._standardize_punctuation(text)
        
        # Step 6: Fix double spaces, etc.
        text = self._fix_double_spacing(text)
        
        # Step 7: AI refinement (optional)
        if use_ai and self._check_ollama():
            text, refined = self._ai_refine(text)
            if refined:
                self.stats["ai_refined"] += 1
                issues.append("ai_refined")
        
        cleaned = text.strip()
        was_modified = cleaned != original.strip()
        
        return CleanedText(
            original=original,
            cleaned=cleaned,
            was_modified=was_modified,
            issues_fixed=issues,
        )
    
    def _remove_html_tags(self, text: str) -> str:
        """Remove HTML tags."""
        import re
        
        # Remove script and style first (they may contain tags)
        text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove comments
        text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
        
        # Remove all HTML tags
        text = re.sub(r"<[^>]+>", "", text)
        
        # Decode HTML entities
        text = self._decode_entities(text)
        
        return text
    
    def _decode_entities(self, text: str) -> str:
        """Decode HTML entities."""
        import html
        try:
            return html.unescape(text)
        except Exception:
            return text
    
    def _remove_navigation(self, text: str) -> str:
        """Remove navigation and footer elements."""
        import re
        
        for pattern in self.NAV_PATTERNS:
            text = re.sub(pattern, "", text, flags=re.DOTALL | re.IGNORECASE)
        
        return text
    
    def _normalize_whitespace(self, text: str) -> str:
        """Normalize whitespace."""
        import re
        
        # Replace multiple newlines with double newline
        text = re.sub(r"\n{3,}", "\n\n", text)
        
        # Replace multiple spaces with single space
        text = re.sub(r" {2,}", " ", text)
        
        # Replace tabs with space
        text = text.replace("\t", " ")
        
        # Remove leading/trailing whitespace from each line
        lines = [line.strip() for line in text.split("\n")]
        text = "\n".join(lines)
        
        # Remove trailing whitespace
        text = text.rstrip()
        
        return text
    
    def _fix_paragraphs(self, text: str) -> str:
        """Fix broken/missing paragraph breaks."""
        import re
        
        # Fix missing space after period
        text = re.sub(r"\.([A-Z])", r". \1", text)
        
        # Fix missing space after question mark
        text = re.sub(r"\?([A-Z])", r"? \1", text)
        
        # Fix missing space after exclamation
        text = re.sub(r"!([A-Z])", r"! \1", text)
        
        return text
    
    def _standardize_punctuation(self, text: str) -> str:
        """Standardize punctuation marks."""
        # Normalize quotes
        text = text.replace(""", '"').replace(""", '"')
        text = text.replace("'", "'").replace("'", "'")
        
        # Normalize dashes
        text = text.replace("—", "—").replace("--", "—")
        
        # Normalize ellipsis
        text = text.replace("...", "…")
        
        # Remove extra periods after ellipsis
        text = text.replace("… .", "…")
        
        return text
    
    def _fix_double_spacing(self, text: str) -> str:
        """Fix double spacing issues."""
        import re
        
        # Remove double spaces
        text = re.sub(r" {2,}", " ", text)
        
        return text
    
    def _ai_refine(self, text: str) -> tuple[str, bool]:
        """Use Qwen via Ollama for refinement.
        
        Returns (refined_text, was_refined).
        """
        if not self._check_ollama():
            return text, False
        
        try:
            import requests
            
            prompt = f"""Clean and improve the readability of this translation text.
Keep the exact meaning. Fix any broken sentences. Fix paragraph breaks if needed.
Do NOT add or change any content - only fix formatting issues.

Text:
{text[:2000]}

Output ONLY the cleaned text, nothing else."""
            
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": "qwen",
                    "prompt": prompt,
                    "stream": False,
                    "options": {"num_predict": 500},
                },
                timeout=60,
            )
            
            if response.status_code == 200:
                result = response.json().get("response", "").strip()
                if result and len(result) > len(text) * 0.5:
                    return result, True
            
        except Exception as e:
            logger.debug(f"AI refine failed: {e}")
        
        return text, False
    
    def _check_ollama(self) -> bool:
        """Check if Ollama is available."""
        if self._ollama_available is not None:
            return self._ollama_available
        
        try:
            import requests
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=5)
            self._ollama_available = response.status_code == 200
        except Exception:
            self._ollama_available = False
        
        return self._ollama_available
    
    def clean_batch(self, texts: list[str], use_ai: bool = False) -> list[CleanedText]:
        """Clean multiple texts."""
        return [self.clean(text, use_ai) for text in texts]
    
    def get_stats(self) -> dict:
        """Get cleaning statistics."""
        return self.stats.copy()


def clean_text_simple(text: str) -> str:
    """Simple cleaning function for quick use."""
    cleaner = TextCleaner()
    result = cleaner.clean(text, use_ai=False)
    return result.cleaned