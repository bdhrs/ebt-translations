"""TBW HTML Extractor - Extract translations from offline TBW HTML files."""

import logging
import re
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class TBWExtractor:
    """Extract translations from The Buddha's Words offline HTML."""

    NIKAYA_DIRS = {
        "dn": "dn",
        "mn": "mn", 
        "sn": "sn",
        "an": "an",
    }

    def __init__(self, html_dir: Path):
        self.html_dir = html_dir
        self._extracted = []
        self._stats = {"total": 0, "failed": 0}

    def extract(self) -> list[dict]:
        """Extract all TBW translations from HTML files."""
        self._extracted = []

        for nikaya, dir_name in self.NIKAYA_DIRS.items():
            nikaya_dir = self.html_dir / dir_name
            if not nikaya_dir.exists():
                logger.warning(f"Directory not found: {nikaya_dir}")
                continue

            logger.info(f"Extracting {nikaya.upper()} from {nikaya_dir}")
            count = self._extract_nikaya(nikaya, nikaya_dir)
            logger.info(f"  Extracted {count} suttas")

        self._stats["total"] = len(self._extracted)
        return self._extracted

    def _extract_nikaya(self, nikaya: str, nikaya_dir: Path) -> int:
        """Extract suttas from a nikaya directory."""
        count = 0
        html_files = sorted(nikaya_dir.glob("*.html"))
        
        for html_file in html_files:
            if html_file.name in ("index.html", "home.html", "an.html", "dn.html"):
                continue

            try:
                result = self._parse_html_file(nikaya, html_file)
                if result:
                    self._extracted.append(result)
                    count += 1
            except Exception as e:
                logger.error(f"Failed to parse {html_file}: {e}")
                self._stats["failed"] += 1

        return count

    def _parse_html_file(self, nikaya: str, html_file: Path) -> Optional[dict]:
        """Parse a single HTML file and extract translation."""
        with open(html_file, encoding="utf-8", errors="ignore") as f:
            soup = BeautifulSoup(f.read(), "html.parser")

        sutta_number = self._extract_sutta_number(nikaya, html_file.stem)
        if not sutta_number:
            return None

        text = self._extract_translation_text(soup)
        if not text or len(text.strip()) < 50:
            return None

        return {
            "source_id": "tbw",
            "sutta_number": sutta_number,
            "text": text.strip(),
            "nikaya": nikaya,
            "source_file": str(html_file),
        }

    def _extract_sutta_number(self, nikaya: str, filename: str) -> Optional[str]:
        """Extract sutta number from filename.
        
        Examples:
            dn1 -> dn1
            dn01 -> dn1
            an1.1 -> an1.1
            an1_1 -> an1.1
        """
        name = filename.strip().lower()
        
        if nikaya == "dn":
            match = re.match(r"^dn0*(\d+)$", name)
            if match:
                return f"dn{match.group(1)}"
        
        elif nikaya == "mn":
            match = re.match(r"^mn0*(\d+)$", name)
            if match:
                return f"mn{match.group(1)}"
        
        elif nikaya == "sn":
            match = re.match(r"^sn0*(\d+)[._-]?(\d+)$", name)
            if match:
                return f"sn{match.group(1)}.{match.group(2)}"
        
        elif nikaya == "an":
            match = re.match(r"^an0*(\d+)[._-]?(\d+)$", name)
            if match:
                return f"an{match.group(1)}.{match.group(2)}"

        return None

    def _extract_translation_text(self, soup: BeautifulSoup) -> str:
        """Extract main translation text from HTML."""
        text_parts = []

        for elem in soup.find_all(["p", "div"]):
            classes = elem.get("class", [])
            elem_id = elem.get("id", "")
            
            if "title" in classes or "title" in elem_id:
                continue
            if "nav" in classes or "footer" in classes:
                continue
            if "note" in classes or "footnote" in classes:
                continue

            text = elem.get_text(strip=True)
            if len(text) > 100:
                text_parts.append(text)

        return "\n\n".join(text_parts)

    def get_stats(self) -> dict:
        """Get extraction statistics."""
        return self._stats.copy()


def extract_tbw(html_dir: Path) -> list[dict]:
    """Quick function to extract TBW from HTML."""
    extractor = TBWExtractor(html_dir)
    return extractor.extract()