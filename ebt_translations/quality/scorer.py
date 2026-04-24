"""Quality scorer for EBT quality pipeline.

Score each translation:
+ length (not too short)
+ paragraph structure  
+ punctuation quality
- noise / HTML remnants
- broken sentences

Output:
{
  sutta,
  source,
  score
}
"""

import logging
import re
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class QualityScore:
    """Quality score result."""
    total_score: float
    length_score: float
    structure_score: float
    punctuation_score: float
    noise_score: float
    issues: list[str] = None
    
    def __post_init__(self):
        if self.issues is None:
            self.issues = []


class QualityScorer:
    """Score translation quality."""
    
    MIN_LENGTH = 200
    MAX_LENGTH = 50000
    OPTIMAL_MIN_LENGTH = 1000
    OPTIMAL_MAX_LENGTH = 20000
    
    def __init__(self):
        self._stats_scored = 0
        self._stats_issues = {}
    
    def score(self, text: str) -> QualityScore:
        """Score a translation.
        
        Args:
            text: Translation text
            
        Returns:
            QualityScore
        """
        if not text:
            return QualityScore(
                total_score=0,
                length_score=0,
                structure_score=0,
                punctuation_score=0,
                noise_score=0,
                issues=["empty_text"],
            )
        
        self._stats_scored += 1
        
        issues = []
        
        # Length score (0-25)
        length_score, length_issues = self._score_length(text)
        issues.extend(length_issues)
        
        # Structure score (0-25)
        structure_score, struct_issues = self._score_structure(text)
        issues.extend(struct_issues)
        
        # Punctuation score (0-25)
        punct_score, punct_issues = self._score_punctuation(text)
        issues.extend(punct_issues)
        
        # Noise score (0-25)
        noise_score, noise_issues = self._score_noise(text)
        issues.extend(noise_issues)
        
        # Total
        total = length_score + structure_score + punct_score + noise_score
        
        return QualityScore(
            total_score=total,
            length_score=length_score,
            structure_score=structure_score,
            punctuation_score=punct_score,
            noise_score=noise_score,
            issues=issues,
        )
    
    def _score_length(self, text: str) -> tuple[float, list[str]]:
        """Score based on text length."""
        length = len(text)
        issues = []
        
        if length < self.MIN_LENGTH:
            score = max(0, 25 * length / self.MIN_LENGTH)
            issues.append("too_short")
        elif length > self.MAX_LENGTH:
            score = max(0, 25 - (length - self.MAX_LENGTH) / 1000)
            issues.append("too_long")
        elif length < self.OPTIMAL_MIN_LENGTH:
            score = 25 * (length - self.MIN_LENGTH) / (self.OPTIMAL_MIN_LENGTH - self.MIN_LENGTH)
        elif length > self.OPTIMAL_MAX_LENGTH:
            score = 25 * (1 - (length - self.OPTIMAL_MAX_LENGTH) / 20000)
        else:
            score = 25
        
        return score, issues
    
    def _score_structure(self, text: str) -> tuple[float, list[str]]:
        """Score based on paragraph structure."""
        issues = []
        
        # Split by paragraphs
        paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
        
        if not paragraphs:
            return 0, ["no_paragraphs"]
        
        # Check for proper paragraph breaks
        if "\n\n" not in text and len(text) > 500:
            issues.append("no_paragraph_breaks")
        
        # Check for excessively long paragraphs
        long_paras = sum(1 for p in paragraphs if len(p) > 1000)
        
        if long_paras > len(paragraphs) * 0.5:
            issues.append("many_long_paragraphs")
        
        # Score
        score = 25
        
        if issues:
            score = max(0, 25 - len(issues) * 5)
        
        return score, issues
    
    def _score_punctuation(self, text: str) -> tuple[float, list[str]]:
        """Score based on punctuation quality."""
        issues = []
        
        # Count punctuation
        periods = text.count(".")
        commas = text.count(",")
        questions = text.count("?")
        exclamations = text.count("!")
        
        # Check for broken sentences (missing punctuation)
        sentences = re.split(r"[.!?]", text)
        broken = sum(1 for s in sentences if len(s.strip()) > 200)
        
        if broken > len(sentences) * 0.3:
            issues.append("broken_sentences")
        
        # Check for excessive punctuation
        if periods < 3 and len(text) > 500:
            issues.append("missing_periods")
        
        # Score
        score = 25
        
        if issues:
            score = max(0, 25 - len(issues) * 5)
        
        return score, issues
    
    def _score_noise(self, text: str) -> tuple[float, list[str]]:
        """Score based on noise/HTML remnants."""
        issues = []
        
        # Check for HTML remnants
        if "<" in text or ">" in text:
            issues.append("html_remnants")
        
        # Check for markdown
        if "[" in text and "]" in text:
            issues.append("markdown_links")
        
        # Check for broken brackets
        open_brackets = text.count("[")
        close_brackets = text.count("]")
        if abs(open_brackets - close_brackets) > 2:
            issues.append("broken_brackets")
        
        # Check for URLs
        urls = re.findall(r"https?://\S+", text)
        if len(urls) > 3:
            issues.append("many_urls")
        
        # Check for email addresses
        emails = re.findall(r"\S+@\S+\.\S+", text)
        if emails:
            issues.append("email_addresses")
        
        # Score starts at 25 and loses points
        score = 25 - len(issues) * 5
        
        return max(0, score), issues
    
    def score_batch(self, translations: list[dict]) -> list[dict]:
        """Score multiple translations.
        
        Args:
            translations: [{"sutta_number", "source_id", "text"}, ...]
            
        Returns:
            List with scores added
        """
        results = []
        
        for trans in translations:
            text = trans.get("text", "")
            
            score_result = self.score(text)
            
            results.append({
                "sutta_number": trans.get("sutta_number"),
                "source_id": trans.get("source_id"),
                "text": text,
                "score": score_result.total_score,
                "length_score": score_result.length_score,
                "structure_score": score_result.structure_score,
                "punctuation_score": score_result.punctuation_score,
                "noise_score": score_result.noise_score,
                "issues": score_result.issues,
            })
        
        return results
    
    def get_top_translations(
        self,
        translations: list[dict],
        top_n: int = 5,
    ) -> list[dict]:
        """Get top N translations by score."""
        scored = self.score_batch(translations)
        scored.sort(key=lambda x: x.get("score", 0), reverse=True)
        return scored[:top_n]
    
    def get_stats(self) -> dict:
        """Get scoring statistics."""
        return {
            "scored": self._stats_scored,
            "issues": self._stats_issues.copy(),
        }


def quick_score(text: str) -> float:
    """Quick scoring function."""
    scorer = QualityScorer()
    result = scorer.score(text)
    return result.total_score