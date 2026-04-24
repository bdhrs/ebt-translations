"""ATI Expander for EBT.

Expands ATI (Access to Insight) source coverage by scraping missing suttas.

Process:
1. Gap detection - find missing suttas from sutta_master
2. URL builder - construct ATI URLs
3. Fetch - scrape translation from web
4. Extract - remove HTML, extract main text
5. Validate - check sutta_master and text length
6. Dedup - skip existing entries
7. Insert - add to ati_* tables
8. Update source_availability
9. Track operations
"""

import logging
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class AtiExpander:
    """Expand ATI source coverage via web scraping."""

    SOURCE_ID = "ati"
    MIN_TEXT_LENGTH = 200
    REQUEST_TIMEOUT = 30
    RATE_LIMIT_DELAY = 0.5

    ATI_BASE_URL = "https://www.accesstoinsight.org/tipitaka"

    NIKAYA_MAP = {
        "dn": {"folder": "dn", "format": "dn{}.html"},
        "mn": {"folder": "mn", "format": "mn{}.html"},
        "sn": {"folder": "sn", "format": "sn/sn{}/{}.html"},
        "an": {"folder": "an", "format": "an/an{}/{}.html"},
    }

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.conn: Optional[sqlite3.Connection] = None
        self.stats = {
            "processed": 0,
            "inserted": 0,
            "skipped": 0,
            "failed": 0,
            "duplicates": 0,
            "not_found": 0,
            "text_short": 0,
        }
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        self._session.allow_redirects = True
        self._last_request_time = 0

    def connect(self):
        if self.conn is None:
            self.conn = sqlite3.connect(str(self.db_path))
            self.conn.row_factory = sqlite3.Row
        return self.conn

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def run(self, limit: int = 0, dry_run: bool = False) -> dict:
        """Run ATI expansion."""
        conn = self.connect()
        cur = conn.cursor()

        coverage_before = self._get_coverage(cur)

        gaps = self._find_gaps(cur)
        if limit:
            gaps = gaps[:limit]

        logger.info(f"Processing {len(gaps)} gaps for ATI")

        for sutta_number in gaps:
            self._rate_limit()
            self.stats["processed"] += 1

            success = self._process_ati(sutta_number, cur, dry_run)

            if success:
                self.stats["inserted"] += 1
            elif success is None:
                self.stats["not_found"] += 1
            else:
                self.stats["failed"] += 1

        coverage_after = self._get_coverage(cur)

        result = {
            **self.stats,
            "coverage_before": coverage_before,
            "coverage_after": coverage_after,
        }

        return result

    def _find_gaps(self, cur: sqlite3.Cursor) -> list[str]:
        """Find suttas missing from ATI."""
        cur.execute("""
            SELECT sm.sutta_number, sm.nikaya
            FROM sutta_master sm
            LEFT JOIN source_availability sa
                ON sm.sutta_number = sa.sutta_number
                AND sa.source_id = ?
                AND sa.has_translation = 1
            WHERE sa.sutta_number IS NULL
            ORDER BY sm.nikaya, sm.sutta_number
        """, (self.SOURCE_ID,))

        gaps = []
        for row in cur.fetchall():
            sutta = row[0]
            nikaya = row[1]
            if nikaya in ["dn", "mn", "sn", "an"]:
                gaps.append(sutta)

        return gaps

    def _rate_limit(self):
        """Apply rate limiting."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self.RATE_LIMIT_DELAY:
            time.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = time.time()

    def _normalize_sutta_number(self, sutta_number: str) -> str:
        """Normalize sutta number."""
        s = sutta_number.strip().lower()
        
        for nikaya in ["dn", "mn", "sn", "an"]:
            if s.startswith(nikaya):
                return s
        
        return s

    def _build_url(self, sutta_number: str) -> Optional[str]:
        """Build ATI URL."""
        s = self._normalize_sutta_number(sutta_number)

        nikaya = None
        if s.startswith("dn"):
            nikaya = "dn"
            num = s[2:]
        elif s.startswith("mn"):
            nikaya = "mn"
            num = s[2:]
        elif s.startswith("sn"):
            nikaya = "sn"
            num = s[2:]
        elif s.startswith("an"):
            nikaya = "an"
            num = s[2:]
        
        if not nikaya or not num:
            return None

        if nikaya not in self.NIKAYA_MAP:
            return None

        config = self.NIKAYA_MAP[nikaya]

        if nikaya == "sn" or nikaya == "an":
            num = num.replace(".", "_")
            return f"{self.ATI_BASE_URL}/{config['folder']}/{config['folder']}{num}.html"
        elif nikaya == "dn" or nikaya == "mn":
            return f"{self.ATI_BASE_URL}/{config['folder']}/{config['format'].format(num)}"

        return None

    def _process_ati(self, sutta_number: str, cur: sqlite3.Cursor, dry_run: bool) -> Optional[bool]:
        """Process a single ATI sutta."""
        url = self._build_url(sutta_number)
        if not url:
            self.stats["failed"] += 1
            return False

        text = self._fetch(url)
        if not text:
            return None

        if len(text) < self.MIN_TEXT_LENGTH:
            self.stats["text_short"] += 1
            return False

        return self._insert(sutta_number, text, url, cur, dry_run)

    def _fetch(self, url: str) -> Optional[str]:
        """Fetch translation from ATI."""
        try:
            response = self._session.get(url, timeout=self.REQUEST_TIMEOUT)
            if response.status_code != 200:
                return None

            soup = BeautifulSoup(response.text, "html.parser")

            text = ""
            for elem in soup.find_all(["div"], id=["content", "mainContent"]):
                text = elem.get_text(strip=True)
                if text and len(text) > 100:
                    break

            if not text:
                for elem in soup.find_all(["div"], class_=["content", "main"]):
                    txt = elem.get_text(strip=True)
                    if txt and len(txt) > 100:
                        text = txt
                        break

            if not text:
                paragraphs = []
                for elem in soup.find_all(["p"]):
                    txt = elem.get_text(strip=True)
                    if len(txt) > 200:
                        paragraphs.append(txt)
                if paragraphs:
                    text = " ".join(paragraphs)

            if text:
                return self._clean_text(text)

            return None

        except Exception as e:
            logger.debug(f"Fetch error for {url}: {e}")
            return None

    def _clean_text(self, text: str) -> str:
        """Clean extracted text."""
        text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<nav[^>]*>.*?</nav>", "", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<footer[^>]*>.*?</footer>", "", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        return text.strip()

    def _insert(self, sutta_number: str, text: str, url: str, cur: sqlite3.Cursor, dry_run: bool) -> bool:
        """Insert translation into database."""
        nikaya = self._get_nikaya(sutta_number)
        table_name = f"{self.SOURCE_ID}_{nikaya}"

        cur.execute(f"""
            SELECT 1 FROM {table_name}
            WHERE sutta_number = ? AND translation_text IS NOT NULL AND LENGTH(translation_text) > ?
        """, (sutta_number, self.MIN_TEXT_LENGTH))

        if cur.fetchone():
            self.stats["duplicates"] += 1
            return True

        if dry_run:
            return True

        try:
            cur.execute(f"""
                INSERT OR REPLACE INTO {table_name}
                (sutta_number, translation_text, source_url, char_count, is_complete, last_updated)
                VALUES (?, ?, ?, ?, 1, ?)
            """, (sutta_number, text, url, len(text), datetime.now().isoformat()))

            cur.execute("""
                INSERT OR IGNORE INTO source_availability
                (sutta_number, source_id, has_translation, is_complete)
                VALUES (?, ?, 1, 1)
            """, (sutta_number, self.SOURCE_ID))

            self.conn.commit()
            return True

        except Exception as e:
            logger.debug(f"Insert error: {e}")
            self.stats["failed"] += 1
            return False

    def _get_nikaya(self, sutta_number: str) -> str:
        """Get nikaya code."""
        s = sutta_number.lower()
        if s.startswith("dn"):
            return "dn"
        elif s.startswith("mn"):
            return "mn"
        elif s.startswith("sn"):
            return "sn"
        elif s.startswith("an"):
            return "an"
        return "kn"

    def _get_coverage(self, cur: sqlite3.Cursor) -> dict:
        """Get ATI coverage."""
        cur.execute("""
            SELECT COUNT(DISTINCT sutta_number) FROM source_availability
            WHERE source_id = ? AND has_translation = 1
        """, (self.SOURCE_ID,))
        return {self.SOURCE_ID: cur.fetchone()[0] or 0}


def run_ati_expander(db_path: str, limit: int = 0, dry_run: bool = False) -> dict:
    """Run ATI expander."""
    expander = AtiExpander(db_path)
    result = expander.run(limit=limit, dry_run=dry_run)
    expander.close()
    return result