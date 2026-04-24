#!/usr/bin/env python3
"""Scrape Missing Suttas - Target only missing suttas for each source."""

import argparse
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

from ebt_translations.utils.normalizer import normalize_sutta_id, get_nikaya

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).parent.parent / "data" / "db" / "EBT_Unified (1).db"
REPORTS_DIR = Path(__file__).parent.parent / "data" / "reports" / "missing_by_source"


class MissingSuttaScraper:
    """Scrape only missing suttas for a source."""
    
    # Source configuration
    SOURCES = {
        "dt": {
            "base_url": "https://www.dhammatalks.org/suttas",
            "nikaya_folders": {"dn": "DN", "mn": "MN", "sn": "SN", "an": "AN"},
            "id_prefix": {"dn": "DN", "mn": "MN", "sn": "SN", "an": "AN"},
        },
        "ati": {
            "base_url": "https://www.accesstoinsight.org/tipitaka",
            "nikaya_folders": {"dn": "dn.html", "mn": "mn.html", "sn": "sn.html", "an": "an.html"},
            "id_prefix": {"dn": "DN", "mn": "MN", "sn": "SN", "an": "AN"},
        },
    }
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.stats = {"attempted": 0, "inserted": 0, "failed": 0, "skipped": 0}
    
    def connect(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
        return self.conn
    
    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def load_missing_suttas(self, source: str) -> list[str]:
        """Load missing suttas from report file."""
        missing_file = REPORTS_DIR / f"missing_suttas_{source}.txt"
        
        if not missing_file.exists():
            logger.warning(f"Missing file not found: {missing_file}")
            return []
        
        suttas = []
        with open(missing_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    suttas.append(line)
        
        logger.info(f"Loaded {len(suttas)} missing suttas for {source}")
        return suttas
    
    def scrape_dt(self, sutta_ids: list[str]) -> int:
        """Scrape from Dhammatalks."""
        inserted = 0
        
        # Test URLs first
        test_url = "https://www.dhammatalks.org/suttas/DN/DN01.html"
        try:
            response = requests.get(test_url, timeout=10)
            logger.info(f"Test URL status: {response.status_code}")
        except Exception as e:
            logger.warning(f"Network test failed: {e}")
        
        for sutta_id in sutta_ids:
            self.stats["attempted"] += 1
            
            # Skip if already exists
            if self._exists_in_db(sutta_id, "dt"):
                self.stats["skipped"] += 1
                continue
            
            # Try to fetch
            try:
                normalized = normalize_sutta_id(sutta_id)
                nikaya = get_nikaya(normalized)
                
                # Extract number parts
                num_match = re.match(r'^[a-z]+(\d+)\.?(\d*)', normalized)
                if not num_match:
                    continue
                
                main_num = int(num_match.group(1))
                sub_num = num_match.group(2)
                
                # Build folder and prefix
                folder = {"dn": "DN", "mn": "MN", "sn": "SN", "an": "AN"}.get(nikaya, "DN")
                
                # DHAMMATALKS URL patterns:
                # DN: DN01.html (zero-padded), MN: MN1.html (no padding), SN/AN: underscore with no zero-padding
                if nikaya == "an" or nikaya == "sn":
                    if sub_num:
                        url = f"https://www.dhammatalks.org/suttas/{folder}/{folder}{main_num}_{sub_num}.html"
                    else:
                        url = f"https://www.dhammatalks.org/suttas/{folder}/{folder}{main_num}.html"
                elif nikaya == "dn":
                    url = f"https://www.dhammatalks.org/suttas/{folder}/{folder}{str(main_num).zfill(2)}.html"
                else:  # mn
                    url = f"https://www.dhammatalks.org/suttas/{folder}/{folder}{main_num}.html"
                
                response = requests.get(url, timeout=30, headers={
                    "User-Agent": "Mozilla/5.0 (compatible; EBT-Bot/1.0)"
                })
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")
                    text = ""
                    
                    # Try multiple selectors - main tag first
                    main = soup.find("main")
                    if main:
                        text = main.get_text(separator="\n\n", strip=True)
                    
                    # If no main or too short, try article
                    if not text or len(text) < 200:
                        article = soup.find("article")
                        if article:
                            text = article.get_text(separator="\n\n", strip=True)
                    
                    # Fallback: get all paragraph text
                    if not text or len(text) < 200:
                        paras = []
                        for p in soup.find_all("p"):
                            t = p.get_text(strip=True)
                            if len(t) > 100:
                                paras.append(t)
                        text = "\n\n".join(paras[:20])
                    
                    if text and len(text) > 200:
                        if self._insert_sutta(normalized, "dt", text, nikaya):
                            inserted += 1
                            self.stats["inserted"] += 1
                            logger.debug(f"Inserted: {normalized} ({len(text)} chars)")
                    else:
                        self.stats["failed"] += 1
                        logger.debug(f"No text for {normalized}: got {len(text) if text else 0} chars")
                else:
                    self.stats["failed"] += 1
                    logger.debug(f"HTTP {response.status_code}: {url}")
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                self.stats["failed"] += 1
                logger.debug(f"Error {sutta_id}: {e}")
            
            # Progress every 10
            if self.stats["attempted"] % 10 == 0:
                pct = (self.stats["inserted"] / self.stats["attempted"]) * 100 if self.stats["attempted"] > 0 else 0
                logger.info(f"Progress: {self.stats['attempted']}/{len(sutta_ids)} - {pct:.1f}% inserted")
        
        return inserted
    
    def scrape_ati(self, sutta_ids: list[str]) -> int:
        """Scrape from Access to Insight."""
        inserted = 0
        
        for sutta_id in sutta_ids:
            self.stats["attempted"] += 1
            
            if self._exists_in_db(sutta_id, "ati"):
                self.stats["skipped"] += 1
                continue
            
            try:
                nikaya = get_nikaya(sutta_id)
                num = re.sub(r'^[a-z]+', '', sutta_id)
                
                # ATI URL pattern: /tipitaka/dn/dn1.html
                url = f"https://www.accesstoinsight.org/tipitaka/{nikaya}/{nikaya}{num}.html"
                
                response = requests.get(url, timeout=30)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")
                    text = ""
                    
                    for div in soup.find_all("div", id=["content", "main"]):
                        text = div.get_text(strip=True)
                        if len(text) > 200:
                            break
                    
                    if text and len(text) > 200:
                        if self._insert_sutta(sutta_id, "ati", text, nikaya):
                            inserted += 1
                            self.stats["inserted"] += 1
                
                time.sleep(0.3)
                
            except Exception as e:
                self.stats["failed"] += 1
                logger.debug(f"Failed {sutta_id}: {e}")
            
            if self.stats["attempted"] % 10 == 0:
                pct = (self.stats["inserted"] / self.stats["attempted"]) * 100 if self.stats["attempted"] > 0 else 0
                logger.info(f"Progress: {self.stats['attempted']}/{len(sutta_ids)} - {pct:.1f}%")
        
        return inserted
    
    def _exists_in_db(self, sutta_id: str, source: str) -> bool:
        """Check if sutta exists in database."""
        conn = self.connect()
        cur = conn.cursor()
        
        normalized = normalize_sutta_id(sutta_id)
        
        cur.execute("""
            SELECT 1 FROM source_availability
            WHERE sutta_number = ? AND source_id = ?
        """, (normalized, source))
        
        return cur.fetchone() is not None
    
    def _insert_sutta(self, sutta_id: str, source: str, text: str, nikaya: str) -> bool:
        """Insert sutta into database."""
        conn = self.connect()
        cur = conn.cursor()
        
        normalized = normalize_sutta_id(sutta_id)
        table = f"{source}_{nikaya}"
        
        try:
            cur.execute(f"""
                INSERT OR REPLACE INTO {table}
                (sutta_number, translation_text, char_count, is_complete, last_updated)
                VALUES (?, ?, ?, 1, ?)
            """, (normalized, text, len(text), datetime.now()))
            
            cur.execute("""
                INSERT OR REPLACE INTO source_availability
                (sutta_number, source_id, has_translation, is_complete)
                VALUES (?, ?, 1, 1)
            """, (normalized, source))
            
            conn.commit()
            return True
            
        except sqlite3.OperationalError as e:
            logger.debug(f"Insert error: {e}")
            return False
    
    def get_coverage_before(self, source: str) -> int:
        """Get current coverage count."""
        conn = self.connect()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT COUNT(DISTINCT sutta_number)
            FROM source_availability
            WHERE source_id = ?
        """, (source,))
        
        return cur.fetchone()[0] or 0


def main():
    parser = argparse.ArgumentParser(description="Scrape missing suttas")
    parser.add_argument("--source", choices=["dt", "ati", "tbw"], help="Source to scrape")
    parser.add_argument("--limit", type=int, default=100, help="Limit suttas to scrape")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Database path")
    
    args = parser.parse_args()
    
    db_path = args.db
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        return
    
    scraper = MissingSuttaScraper(str(db_path))
    
    sources = [args.source] if args.source else ["dt", "ati"]
    
    for source in sources:
        logger.info(f"\n{'='*50}")
        logger.info(f"Processing source: {source}")
        logger.info(f"{'='*50}")
        
        # Skip AN for DT (DT uses book-based structure, not 1:1)
        if source == "dt":
            logger.info("Note: AN on DT uses book-based URLs, skipping AN for now")
        
        # Get coverage before
        before = scraper.get_coverage_before(source)
        logger.info(f"Coverage before: {before}")
        
        # Load missing
        missing = scraper.load_missing_suttas(source)
        
        if not missing:
            logger.info(f"No missing suttas for {source}")
            continue
        
        # Limit
        if args.limit:
            missing = missing[:args.limit]
        
        # Scrape
        if source == "dt":
            scraper.scrape_dt(missing)
        elif source == "ati":
            scraper.scrape_ati(missing)
        
        # Coverage after
        after = scraper.get_coverage_before(source)
        logger.info(f"Coverage after: {after}")
        logger.info(f"New suttas: {after - before}")
    
    scraper.close()
    
    # Summary
    logger.info("\n" + "="*50)
    logger.info("SUMMARY")
    logger.info("="*50)
    logger.info(f"Attempted: {scraper.stats['attempted']}")
    logger.info(f"Inserted: {scraper.stats['inserted']}")
    logger.info(f"Failed: {scraper.stats['failed']}")
    logger.info(f"Skipped: {scraper.stats['skipped']}")


if __name__ == "__main__":
    main()