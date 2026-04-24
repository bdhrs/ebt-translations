"""AI-based sutta detection using Ollama."""

import json
import re
from typing import Optional, List

from .ollama_client import OllamaClient
from .chunker import TextChunk


SYSTEM_PROMPT = """You are a Buddhist text analysis expert. Your task is to identify which Tipitaka sutta a given text passage comes from.

Available suttas include:
- Dhp (Dhammapada): dhp1-dhp423
- SN (Samutta Nikaya): sn1.1-sn56.30
- AN (Anguttara Nikaya): an1.1-an11.190
- MN (Majjhima Nikaya): mn1-mn152
- DN (Digha Nikaya): dn1-dn34
- Ud (Udana): ud1-ud88
- Iti (Itivuttaka): iti1-iti112
- Jv (Jataka): jv1-jv550
- Snp (Sutta Nipata): snp1-snp1274
- Thag (Theragatha): thag1-thag1277
- Thag (Therigatha): thag1-thag523

Output ONLY valid JSON like:
{"sutta_id": "sn12.5", "reason": "text mentions X"}
If uncertain, respond: {"sutta_id": null, "reason": "no clear match"}
"""

PROMPT_TEMPLATE = """Analyze this text and identify the sutta it comes from:

{chunk_text}

Output JSON:"""


class AIDetector:
    """Use Ollama to detect sutta IDs."""
    
    def __init__(self, client: OllamaClient = None):
        self.client = client or OllamaClient()
        self.system_prompt = SYSTEM_PROMPT
    
    def detect(self, chunk: TextChunk) -> Optional[str]:
        """Detect sutta ID from a text chunk."""
        prompt = f"{SYSTEM_PROMPT}\n\n{PROMPT_TEMPLATE.format(chunk_text=chunk.text)}"
        
        try:
            response = self.client.call(prompt)
            if not response:
                return None
            
            result = self._parse_response(response)
            return result.get("sutta_id")
        except Exception:
            return None
    
    def detect_batch(self, chunks: List[TextChunk]) -> List[Optional[str]]:
        """Detect sutta IDs for multiple chunks."""
        results = []
        for chunk in chunks:
            result = self.detect(chunk)
            results.append(result)
        return results
    
    def _parse_response(self, response: str) -> dict:
        """Parse JSON response from model."""
        response = response.strip()
        
        match = re.search(r'\{[^{}]*\}', response, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
        
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            return {"sutta_id": None, "reason": "parse failed"}


def detect_sutta(text: str) -> Optional[str]:
    """Convenience function to detect sutta from text."""
    from .chunker import TextChunk
    client = OllamaClient()
    detector = AIDetector(client)
    chunk = TextChunk(text=text, source_table="", segment_id=0, chunk_index=0, word_count=len(text.split()))
    return detector.detect(chunk)