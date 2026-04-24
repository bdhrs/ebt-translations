"""Ollama client for LLM-based sutta detection."""

import json
import time
from typing import Optional
import requests

from . import config


class OllamaClient:
    """Client for interacting with Ollama API."""
    
    def __init__(self, url: str = None, model: str = None, timeout: int = None):
        self.url = url or config.OLLAMA_URL
        self.model = model or config.MODEL
        self.timeout = timeout or config.AI_TIMEOUT
    
    def call(self, prompt: str, retry_count: int = None) -> Optional[str]:
        """Call Ollama with a prompt and return the response."""
        retry = retry_count or config.AI_RETRY_COUNT
        
        for attempt in range(retry):
            try:
                response = requests.post(
                    f"{self.url}/api/generate",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "stream": False,
                        "format": "json"
                    },
                    timeout=self.timeout
                )
                response.raise_for_status()
                data = response.json()
                return data.get("response", "").strip()
            except requests.exceptions.Timeout:
                if attempt < retry - 1:
                    time.sleep(1)
                    continue
                return None
            except requests.exceptions.RequestException as e:
                if attempt < retry - 1:
                    time.sleep(1)
                    continue
                return None
            except json.JSONDecodeError:
                return None
        
        return None
    
    def is_available(self) -> bool:
        """Check if Ollama is available."""
        try:
            response = requests.get(f"{self.url}/api/tags", timeout=5)
            return response.status_code == 200
        except:
            return False


def call_llama(prompt: str) -> Optional[str]:
    """Convenience function to call Ollama."""
    client = OllamaClient()
    return client.call(prompt)
