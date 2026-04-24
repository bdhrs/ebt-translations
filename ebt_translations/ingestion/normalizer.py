"""Sutta ID normalization for EBT ingestion pipeline.

Normalizes sutta IDs to canonical format:
- sn01 -> sn1
- sn1_1 -> sn1.1
- an3-45 -> an3.45
"""

import logging
import re
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class NormalizedSutta:
    """Container for normalized sutta data."""
    original_id: str
    normalized_id: str
    nikaya: str
    sutta_number: int
    vagga: Optional[int] = None
    requires_ai: bool = False


class SuttaNormalizer:
    """Normalize sutta IDs to canonical format."""
    
    # Nikaya prefixes
    NIKAYAS = {
        "dn": "Digha Nikaya",
        "mn": "Majjhima Nikaya",
        "sn": "Samyutta Nikaya",
        "an": "Anguttara Nikaya",
        "kn": "Khuddaka Nikaya",
    }
    
    # Regex patterns for various ID formats
    PATTERNS = {
        # DN: dn1, dn01, dn001
        "dn": re.compile(r"^dn0*(\d+)$", re.IGNORECASE),
        # MN: mn1, mn01, mn001
        "mn": re.compile(r"^mn0*(\d+)$", re.IGNORECASE),
        # SN: sn1.1, sn1_1, sn01.1, sn1-1
        "sn": re.compile(r"^sn0*(\d+)[._-](\d+)$", re.IGNORECASE),
        # AN: an1.1, an1_1, an01.1, an1-1
        "an": re.compile(r"^an0*(\d+)[._-](\d+)$", re.IGNORECASE),
    }
    
    def __init__(self, ollama_url: Optional[str] = None):
        self.ollama_url = ollama_url or "http://localhost:11434"
        self._ollama_client = None
    
    def normalize(self, raw_id: str) -> Optional[NormalizedSutta]:
        """Normalize a raw sutta ID.
        
        Args:
            raw_id: Raw sutta ID from source
            
        Returns:
            NormalizedSutta or None if invalid
        """
        if not raw_id:
            return None
        
        # Clean the input
        clean_id = raw_id.strip().lower().replace(" ", "")
        
        # Try deterministic normalization first
        result = self._normalize_deterministic(clean_id)
        if result:
            return result
        
        # Try with AI fallback
        return self._normalize_with_ai(raw_id)
    
    def _normalize_deterministic(self, clean_id: str) -> Optional[NormalizedSutta]:
        """Deterministic normalization using regex patterns."""
        
        for nikaya, pattern in self.PATTERNS.items():
            match = pattern.match(clean_id)
            if match:
                groups = match.groups()
                
                if nikaya in ["dn", "mn"]:
                    return NormalizedSutta(
                        original_id=clean_id,
                        normalized_id=f"{nikaya}{int(groups[0])}",
                        nikaya=nikaya,
                        sutta_number=int(groups[0]),
                    )
                
                elif nikaya in ["sn", "an"]:
                    return NormalizedSutta(
                        original_id=clean_id,
                        normalized_id=f"{nikaya}{groups[0]}.{groups[1]}",
                        nikaya=nikaya,
                        sutta_number=int(groups[0]),
                        vagga=int(groups[1]),
                    )
        
        # Check for Khuddaka Nikaya sub-collections
        return self._normalize_kn(clean_id)
    
    def _normalize_kn(self, clean_id: str) -> Optional[NormalizedSutta]:
        """Normalize Khuddaka Nikaya suttas."""
        
        # KN sub-collection patterns
        kn_patterns = [
            ("dhp", re.compile(r"^dhp(\d+)$", re.IGNORECASE)),
            ("iti", re.compile(r"^iti(\d+)$", re.IGNORECASE)),
            ("snp", re.compile(r"^snp(\d+)$", re.IGNORECASE)),
            ("thag", re.compile(r"^thag(\d+)$", re.IGNORECASE)),
            ("thig", re.compile(r"^thig(\d+)$", re.IGNORECASE)),
            ("ud", re.compile(r"^ud(\d+)$", re.IGNORECASE)),
            ("kp", re.compile(r"^kp(\d+)$", re.IGNORECASE)),
        ]
        
        for sub, pattern in kn_patterns:
            match = pattern.match(clean_id)
            if match:
                return NormalizedSutta(
                    original_id=clean_id,
                    normalized_id=f"{sub}{int(match.group(1))}",
                    nikaya="kn",
                    sutta_number=int(match.group(1)),
                )
        
        return None
    
    def _normalize_with_ai(self, raw_id: str) -> Optional[NormalizedSutta]:
        """Use Qwen via Ollama for complex normalization."""
        
        # Check if Ollama is available
        if not self._check_ollama():
            logger.warning(f"Cannot normalize: {raw_id} (Ollama not available)")
            return None
        
        try:
            import requests
            
            prompt = f"""Normalize this sutta ID to canonical format (e.g., dn1, sn1.1, an3.45).
ID: {raw_id}
Output ONLY the normalized ID, nothing else."""
            
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": "qwen",
                    "prompt": prompt,
                    "stream": False,
                    "options": {"num_predict": 20},
                },
                timeout=30,
            )
            
            if response.status_code == 200:
                result = response.json().get("response", "").strip()
                
                # Parse result
                result_id = result.split()[0] if result else ""
                
                # Try to normalize the result
                return self._normalize_deterministic(result_id)
            
        except Exception as e:
            logger.error(f"AI normalization failed: {e}")
        
        return None
    
    def _check_ollama(self) -> bool:
        """Check if Ollama is available."""
        try:
            import requests
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=5)
            return response.status_code == 200
        except Exception:
            return False
    
    def batch_normalize(self, raw_ids: list[str]) -> dict[str, Optional[NormalizedSutta]]:
        """Normalize multiple IDs."""
        
        results = {}
        for raw_id in raw_ids:
            results[raw_id] = self.normalize(raw_id)
        
        return results
    
    def is_malformed(self, sutta_id: str) -> bool:
        """Check if sutta ID is malformed."""
        
        normalized = self.normalize(sutta_id)
        return normalized is None
    
    def validate_format(self, sutta_id: str) -> bool:
        """Validate sutta ID format (without normalization)."""
        
        clean = sutta_id.strip().lower()
        
        # Check against all patterns
        for pattern in self.PATTERNS.values():
            if pattern.match(clean):
                return True
        
        # Check KN patterns
        for _, pattern in [
            ("dhp", re.compile(r"^dhp\d+$", re.IGNORECASE)),
            ("iti", re.compile(r"^iti\d+$", re.IGNORECASE)),
            ("snp", re.compile(r"^snp\d+$", re.IGNORECASE)),
            ("thag", re.compile(r"^thag\d+$", re.IGNORECASE)),
            ("thig", re.compile(r"^thig\d+$", re.IGNORECASE)),
            ("ud", re.compile(r"^ud\d+$", re.IGNORECASE)),
            ("kp", re.compile(r"^kp\d+$", re.IGNORECASE)),
        ]:
            if pattern.match(clean):
                return True
        
        return False