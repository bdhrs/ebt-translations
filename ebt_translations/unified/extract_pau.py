"""PAU AI-assisted mapper - Map PAU translations to canonical IDs using AI."""

import logging
import re
import sqlite3
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class PAUMapper:
    """Map PAU translations to canonical sutta IDs using AI assistance."""

    NIKAYAS = ["dn", "mn", "sn", "an", "kn"]
    
    PATTERNS = {
        "dn": re.compile(r"^dn(\d+)$", re.IGNORECASE),
        "mn": re.compile(r"^mn(\d+)$", re.IGNORECASE),
        "sn": re.compile(r"^sn(\d+)[._-](\d+)$", re.IGNORECASE),
        "an": re.compile(r"^an(\d+)[._-](\d+)$", re.IGNORECASE),
    }

    def __init__(self, db_path: Path, ollama_url: str = "http://localhost:11434"):
        self.db_path = db_path
        self.ollama_url = ollama_url
        self._ollama_available = None
        self._mappings = {"certain": [], "uncertain": []}
        self._stats = {"total": 0, "certain": 0, "uncertain": 0, "failed": 0}

    def map_all(self) -> list[dict]:
        """Map all PAU translations to canonical IDs."""
        self._mappings = {"certain": [], "uncertain": []}
        
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()
        
        for nikaya in self.NIKAYAS:
            table_name = f"pau_{nikaya}"
            try:
                cur.execute(f"SELECT * FROM {table_name}")
                columns = [desc[0] for desc in cur.description]
            except sqlite3.OperationalError:
                continue
            
            for row in cur.fetchall():
                row_data = dict(zip(columns, row))
                result = self._process_row(row_data, nikaya)
                if result:
                    self._mappings["certain" if result["is_certain"] else "uncertain"].append(result)
                    self._stats["total"] += 1
                    if result["is_certain"]:
                        self._stats["certain"] += 1
                    else:
                        self._stats["uncertain"] += 1
        
        conn.close()
        return self._mappings["certain"] + self._mappings["uncertain"]

    def _process_row(self, row_data: dict, nikaya: str) -> Optional[dict]:
        """Process a single row from PAU table."""
        sutta_id = row_data.get("sutta_id") or row_data.get("sutta_number")
        if not sutta_id:
            return None
        
        sutta_id = str(sutta_id).strip()
        text = row_data.get("translation_text") or row_data.get("translation_markdown")
        if not text or len(text) < 50:
            return None

        normalized = self._normalize_deterministic(sutta_id)
        
        if not normalized and self._check_ollama():
            normalized = self._map_with_ai(sutta_id, nikaya)
        
        if normalized:
            return {
                "source_id": "pau",
                "sutta_number": normalized,
                "text": text,
                "nikaya": nikaya,
                "is_certain": normalized is not None,
            }
        
        return None

    def _normalize_deterministic(self, sutta_id: str) -> Optional[str]:
        """Try to normalize using deterministic rules."""
        clean = sutta_id.strip().lower()
        
        for nikaya, pattern in self.PATTERNS.items():
            match = pattern.match(clean)
            if match:
                groups = match.groups()
                if nikaya in ["dn", "mn"]:
                    return f"{nikaya}{int(groups[0])}"
                else:
                    return f"{nikaya}{groups[0]}.{groups[1]}"
        
        return None

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

    def _map_with_ai(self, sutta_id: str, nikaya: str) -> Optional[str]:
        """Use AI to map uncertain ID."""
        if not self._check_ollama():
            return None
        
        try:
            import requests
            
            prompt = f"""Map this Pa Auk sutta ID to canonical EBT format.
PAU ID: {sutta_id}
Nikaya: {nikaya}
Convert to format: dn#, mn#, sn#.#, an#.# (e.g., dn1, sn2.15, an1.3)
Output ONLY the normalized ID, nothing else."""
            
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": "qwen",
                    "prompt": prompt,
                    "stream": False,
                    "options": {"num_predict": 30},
                },
                timeout=30,
            )
            
            if response.status_code == 200:
                result = response.json().get("response", "").strip()
                return self._normalize_deterministic(result.split()[0])
        
        except Exception as e:
            logger.error(f"AI mapping failed: {e}")
        
        return None

    def get_stats(self) -> dict:
        """Get mapping statistics."""
        return self._stats.copy()


def map_pau(db_path: Path, ollama_url: str = "http://localhost:11434") -> list[dict]:
    """Quick function to map PAU translations."""
    mapper = PAUMapper(db_path, ollama_url)
    return mapper.map_all()