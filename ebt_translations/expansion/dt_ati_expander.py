"""DT/ATI Expander for EBT.

Expands DT and ATI sources by scraping missing suttas from web.

Process:
1. Gap detection - find missing suttas per source
2. Build URLs - construct DT and ATI URLs
3. Fetch - scrape translation from web
4. Extract - remove HTML, extract translation
5. Validate - check sutta_master exists
6. Insert - add to source tables
7. Track - log results
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


class DtAtiExpander:
    """Expand DT and ATI sources via web scraping."""

    SOURCE_IDS = ["dt", "ati"]
    MIN_TEXT_LENGTH = 200
    REQUEST_TIMEOUT = 30
    RATE_LIMIT_DELAY = 0.5  # 500ms between requests

    DT_BASE_URL = "https://www.dhammatalks.org/suttas"
    ATI_BASE_URL = "https://www.accesstoinsight.org/tipitaka"

    NIKAYA_URL_MAP = {
        "dn": {"folder": "DN", "prefix": "DN"},
        "mn": {"folder": "MN", "prefix": "MN"},
        "sn": {"folder": "SN", "prefix": "SN"},
        "an": {"folder": "AN", "prefix": "AN"},
    }

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.conn: Optional[sqlite3.Connection] = None
        self.stats = {
            "processed": 0,
            "inserted": 0,
            "duplicates": 0,
            "failed": 0,
            "not_found": 0,
            "text_too_short": 0,
        }
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
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

    def run(self, sources: list[str] = None, limit: int = 0, dry_run: bool = False) -> dict:
        """Run expansion for DT and/or ATI."""
        if sources is None:
            sources = self.SOURCE_IDS

        conn = self.connect()
        cur = conn.cursor()

        coverage_before = self._get_coverage(cur)

        for source_id in sources:
            gaps = self._find_gaps(source_id, cur)
            if limit:
                gaps = gaps[:limit]

            logger.info(f"Processing {len(gaps)} gaps for {source_id}")

            for sutta_number in gaps:
                self._rate_limit()
                self.stats["processed"] += 1

                if source_id == "dt":
                    success = self._process_dt(sutta_number, cur, dry_run)
                else:
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

    def _discover_dt_urls(self, nikaya: str) -> dict[str, str]:
        """Discover available DT suttas from index pages."""
        urls = {}
        folder = self.NIKAYA_URL_MAP.get(nikaya, {}).get("folder", nikaya.upper())
        
        try:
            index_url = f"{self.DT_BASE_URL}/{folder}/"
            response = self._session.get(index_url, timeout=self.REQUEST_TIMEOUT)
            if response.status_code != 200:
                return urls

            soup = BeautifulSoup(response.text, "html.parser")
            prefix = self.NIKAYA_URL_MAP.get(nikaya, {}).get("prefix", nikaya.upper())

            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if not href or not href.endswith(".html"):
                    continue
                if f"/suttas/{folder}/" in href:
                    filename = href.split("/")[-1].replace(".html", "")
                    if filename.startswith(prefix):
                        num_str = filename[len(prefix):]
                        num = num_str.replace("_", ".")
                        if any(c.isdigit() for c in num):
                            full_url = f"{self.DT_BASE_URL}/{href.lstrip('/')}"
                            urls[f"{nikaya}{num}"] = full_url

        except Exception as e:
            logger.debug(f"DT discovery error for {nikaya}: {e}")

        return urls

    def _find_gaps(self, source_id: str, cur: sqlite3.Cursor) -> list[str]:
        """Find suttas missing from a source."""
        if source_id == "dt":
            return self._find_dt_gaps(cur)
        elif source_id == "ati":
            return self._find_ati_gaps(cur)
        
        cur.execute("""
            SELECT sm.sutta_number
            FROM sutta_master sm
            LEFT JOIN source_availability sa
                ON sm.sutta_number = sa.sutta_number
                AND sa.source_id = ?
                AND sa.has_translation = 1
            WHERE sa.sutta_number IS NULL
            ORDER BY sm.sutta_number
            LIMIT 200
        """, (source_id,))
        
        return [row[0] for row in cur.fetchall()]

    def _find_dt_gaps(self, cur: sqlite3.Cursor) -> list[str]:
        """Find DT gaps using index page discovery."""
        all_urls = {}
        for nikaya in ["mn", "sn"]:
            all_urls.update(self._discover_dt_urls(nikaya))

        if not all_urls:
            return []

        cur.execute("""
            SELECT sutta_number FROM dt_mn WHERE translation_text IS NOT NULL
            UNION SELECT sutta_number FROM dt_sn WHERE translation_text IS NOT NULL
        """)
        existing = {row[0] for row in cur.fetchall()}

        gaps = [s for s in all_urls if s not in existing]
        return gaps[:500]

    def _find_ati_gaps(self, cur: sqlite3.Cursor) -> list[str]:
        """Find ATI gaps."""
        urls = {}
        
        try:
            for nikaya in ["dn", "mn", "sn", "an"]:
                url = f"{self.ATI_BASE_URL}/{nikaya}/"
                resp = self._session.get(url, timeout=10)
                if resp.status_code != 200:
                    continue
                    
                soup = BeautifulSoup(resp.text, "html.parser")
                
                for a in soup.find_all("a", href=True):
                    href = a.get("href", "")
                    if f"/tipitaka/{nikaya}" in href and ".html" in href:
                        num = href.split("/")[-1].replace(".html", "")
                        if nikaya in ["dn", "mn"]:
                            num = num.replace(nikaya.upper(), "")
                        if num.isdigit():
                            urls[f"{nikaya}{num}"] = a.get("href", "")
                            
        except Exception as e:
            logger.debug(f"ATI discovery error: {e}")
        
        if not urls:
            return []
            
        cur.execute("""
            SELECT sutta_number FROM ati_mn WHERE translation_text IS NOT NULL
            UNION SELECT sutta_number FROM ati_sn WHERE translation_text IS NOT NULL
            UNION SELECT sutta_number FROM ati_an WHERE translation_text IS NOT NULL
            UNION SELECT sutta_number FROM ati_dn WHERE translation_text IS NOT NULL
        """)
        existing = {row[0] for row in cur.fetchall()}
        
        gaps = [s for s in urls if s not in existing]
        return gaps[:200]

    def _rate_limit(self):
        """Apply rate limiting."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self.RATE_LIMIT_DELAY:
            time.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = time.time()

    def _normalize_sutta_number(self, sutta_number: str) -> str:
        """Normalize sutta number for URL building."""
        s = sutta_number.strip().lower()

        for nikaya in ["dn", "mn", "sn", "an"]:
            if s.startswith(nikaya):
                num = s[len(nikaya):]
                num = re.sub(r'^0+', '', num) or "0"
                return f"{nikaya}{num}"

        return s

    def _build_dt_url(self, sutta_number: str) -> Optional[str]:
        """Build Dhamma Talks URL."""
        s = self._normalize_sutta_number(sutta_number)

        for nikaya_folder, config in self.NIKAYA_URL_MAP.items():
            if s.startswith(nikaya_folder):
                num = s[len(nikaya_folder):]

                if nikaya_folder == "mn":
                    url_num = num.replace(".", "")
                elif nikaya_folder == "sn":
                    url_num = num.replace(".", "_")
                elif nikaya_folder == "an":
                    url_num = num
                else:
                    url_num = num.replace(".", "")

                return f"{self.DT_BASE_URL}/{config['folder']}/{config['prefix']}{url_num}.html"

        return None

    def _build_ati_url(self, sutta_number: str) -> Optional[str]:
        """Build Access to Insight URL."""
        s = self._normalize_sutta_number(sutta_number)

        for nikaya in ["dn", "mn", "sn", "an"]:
            if s.startswith(nikaya):
                num = s[len(nikaya):]
                return f"{self.ATI_BASE_URL}/{nikaya}{num}.html"

        return None

    def _process_dt(self, sutta_number: str, cur: sqlite3.Cursor, dry_run: bool) -> Optional[bool]:
        """Process a single DT sutta."""
        url = self._build_dt_url(sutta_number)
        if not url:
            return False

        try:
            head = self._session.head(url, timeout=5, allow_redirects=True)
            if head.status_code != 200:
                self.stats["not_found"] += 1
                return None
        except Exception:
            self.stats["not_found"] += 1
            return None

        text = self._fetch_dt(url)
        if not text:
            self.stats["not_found"] += 1
            return None

        if len(text) < self.MIN_TEXT_LENGTH:
            self.stats["text_too_short"] += 1
            return False

        return self._insert_translation("dt", sutta_number, text, url, cur, dry_run)

    def _process_ati(self, sutta_number: str, cur: sqlite3.Cursor, dry_run: bool) -> Optional[bool]:
        """Process a single ATI sutta."""
        url = self._build_ati_url(sutta_number)
        if not url:
            return False

        text = self._fetch_ati(url)
        if not text:
            return None

        if len(text) < self.MIN_TEXT_LENGTH:
            self.stats["text_too_short"] += 1
            return False

        return self._insert_translation("ati", sutta_number, text, url, cur, dry_run)

    def _fetch_dt(self, url: str) -> Optional[str]:
        """Fetch translation from Dhamma Talks."""
        try:
            response = self._session.get(url, timeout=self.REQUEST_TIMEOUT)
            if response.status_code != 200:
                return None

            soup = BeautifulSoup(response.text, "html.parser")

            for elem in soup.find_all(["div"], class_=[]):
                if elem.get("id") in ["sutta", "content", "main"]:
                    text = elem.get_text(strip=True)
                    if text and len(text) > 100:
                        return self._clean_text(text)

            paragraphs = []
            for elem in soup.find_all(["p"]):
                text = elem.get_text(strip=True)
                if len(text) > 200:
                    paragraphs.append(text)

            if paragraphs:
                return self._clean_text(" ".join(paragraphs))

            return None

        except Exception as e:
            logger.debug(f"DT fetch error: {e}")
            return None

    def _fetch_ati(self, url: str) -> Optional[str]:
        """Fetch translation from Access to Insight."""
        try:
            response = self._session.get(url, timeout=self.REQUEST_TIMEOUT)
            if response.status_code != 200:
                return None

            soup = BeautifulSoup(response.text, "html.parser")

            text = ""
            for elem in soup.find_all(["div"], id=["content", "main"]):
                text = elem.get_text(strip=True)
                break

            if text:
                return self._clean_text(text)

            return None

        except Exception as e:
            logger.debug(f"ATI fetch error: {e}")
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

    def _insert_translation(
        self,
        source_id: str,
        sutta_number: str,
        text: str,
        url: str,
        cur: sqlite3.Cursor,
        dry_run: bool,
    ) -> bool:
        """Insert translation into database."""
        nikaya = self._get_nikaya(sutta_number)
        table_name = f"{source_id}_{nikaya}"

        try:
            cur.execute(f"""
                SELECT 1 FROM {table_name}
                WHERE sutta_number = ? AND translation_text IS NOT NULL AND LENGTH(translation_text) > ?
            """, (sutta_number, self.MIN_TEXT_LENGTH))

            if cur.fetchone():
                self.stats["duplicates"] += 1
                return True

            if dry_run:
                return True

            cur.execute(f"""
                INSERT OR REPLACE INTO {table_name}
                (sutta_number, translation_text, source_url, char_count, is_complete, last_updated)
                VALUES (?, ?, ?, ?, 1, ?)
            """, (sutta_number, text, url, len(text), datetime.now().isoformat()))

            cur.execute("""
                INSERT OR REPLACE INTO source_availability
                (sutta_number, source_id, has_translation, is_complete)
                VALUES (?, ?, 1, 1)
            """, (sutta_number, source_id))

            self.conn.commit()
            return True

        except sqlite3.OperationalError as e:
            logger.debug(f"Insert error: {e}")
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
        """Get current coverage stats."""
        coverage = {}
        for source_id in self.SOURCE_IDS:
            cur.execute("""
                SELECT COUNT(DISTINCT sutta_number)
                FROM source_availability
                WHERE source_id = ? AND has_translation = 1
            """, (source_id,))
            coverage[source_id] = cur.fetchone()[0] or 0
        return coverage


def run_dt_ati_expander(
    db_path: str,
    sources: list[str] = None,
    limit: int = 0,
    dry_run: bool = False,
) -> dict:
    """Run DT/ATI expansion."""
    expander = DtAtiExpander(db_path)
    result = expander.run(sources=sources, limit=limit, dry_run=dry_run)
    expander.close()
    return result