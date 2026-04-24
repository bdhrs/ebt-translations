"""TPK Mapper for EBT.

Converts tpk_segments into sutta-level data linked to sutta_master.

Process:
1. Load data from tpk_segments
2. Group by source_table
3. Parse source_table to get sutta number
4. Validate against sutta_master
5. Insert into tpk_* tables
6. Update source_availability
"""

import logging
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class TpkMapResult:
    source_table: str
    sutta_number: Optional[str]
    status: str
    reason: str = ""


class TpkMapper:
    SOURCE_ID = "tpk"
    MIN_TEXT_LENGTH = 200

    NIKAYA_MAP = {
        'd': 'dn',
        'm': 'mn', 
        's': 'sn',
        'e': 'an',
        'k': 'kn',
    }

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.conn: Optional[sqlite3.Connection] = None
        self.stats = {
            "total_source_tables": 0,
            "auto_mapped": 0,
            "tpk_only": 0,
            "invalid": 0,
            "text_too_short": 0,
            "inserted": 0,
            "failed": 0,
        }

    def connect(self):
        if self.conn is None:
            self.conn = sqlite3.connect(str(self.db_path))
            self.conn.row_factory = sqlite3.Row
        return self.conn

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def run(self, dry_run: bool = False) -> dict:
        logger.info("Starting TPK mapping")
        conn = self.connect()
        cur = conn.cursor()
        self._setup_log_table()
        grouped = self._load_and_group(cur)
        print(f"Loaded {len(grouped)} unique source_tables")
        for source_table, data in grouped.items():
            self._process_source_table(source_table, data, cur, dry_run)
        coverage = self._get_coverage()
        print(f"TPK coverage: {coverage}")
        conn.close()
        return {**self.stats, **coverage}

    def _load_and_group(self, cur: sqlite3.Cursor) -> dict:
        cur.execute("""
            SELECT source_table, english_translation, segment_text
            FROM tpk_segments
            WHERE source_table IS NOT NULL
            AND source_table != ''
        """)
        rows = cur.fetchall()
        grouped = defaultdict(lambda: {"text": "", "pali": "", "count": 0})
        for row in rows:
            source_table = row["source_table"]
            english = row["english_translation"] or ""
            pali = row["segment_text"] or ""
            grouped[source_table]["text"] += " " + english if english else ""
            grouped[source_table]["pali"] += " " + pali if pali else ""
            grouped[source_table]["count"] += 1
        self.stats["total_source_tables"] = len(grouped)
        return dict(grouped)

    def parse_source_table(self, source_table: str) -> Optional[str]:
        if not source_table or len(source_table) < 4:
            return None
        tbl = source_table.strip().lower()
        nikaya_code = tbl[0]
        nikaya = self.NIKAYA_MAP.get(nikaya_code)
        if not nikaya:
            return None
        try:
            group_num = int(tbl[1:3])
            sutta_num = int(tbl[3:5])
            sutta_number = f"{nikaya}{group_num}.{sutta_num}"
            return sutta_number
        except (ValueError, IndexError):
            return None

    def _process_source_table(self, source_table: str, data: dict, cur: sqlite3.Cursor, dry_run: bool):
        text = data["text"].strip()
        pali = data["pali"].strip()
        if len(text) < self.MIN_TEXT_LENGTH:
            self.stats["text_too_short"] += 1
            self._log_operation(source_table, None, "text_too_short", dry_run)
            return
        sutta_number = self.parse_source_table(source_table)
        if not sutta_number:
            self.stats["invalid"] += 1
            self._log_operation(source_table, None, "invalid_format", dry_run)
            return
        exists_in_master = self._exists_in_master(sutta_number, cur)
        status = "auto" if exists_in_master else "tpk_only"
        self._log_operation(source_table, sutta_number, status, dry_run)
        if exists_in_master:
            self.stats["auto_mapped"] += 1
        else:
            self.stats["tpk_only"] += 1
        if not dry_run:
            success = self._insert_tpk_data(sutta_number, text, pali, cur)
            if success:
                self.stats["inserted"] += 1
                if exists_in_master:
                    self._update_availability(sutta_number, bool(pali), cur)
            else:
                self.stats["failed"] += 1

    def _exists_in_master(self, sutta_number: str, cur: sqlite3.Cursor) -> bool:
        cur.execute("SELECT 1 FROM sutta_master WHERE sutta_number = ?", (sutta_number,))
        return cur.fetchone() is not None

    def _insert_tpk_data(self, sutta_number: str, text: str, pali: str, cur: sqlite3.Cursor) -> bool:
        nikaya = self._get_nikaya(sutta_number)
        table_name = f"tpk_{nikaya}"
        try:
            cur.execute(f"""INSERT OR REPLACE INTO {table_name} (sutta_number, translation_text, pali_text, char_count, is_complete, last_updated) VALUES (?, ?, ?, ?, 1, ?)""", (sutta_number, text, pali, len(text), datetime.now()))
            self.conn.commit()
            return True
        except sqlite3.OperationalError as e:
            logger.debug(f"Insert error: {e}")
            return False

    def _update_availability(self, sutta_number: str, has_pali: bool, cur: sqlite3.Cursor):
        try:
            cur.execute("""INSERT OR REPLACE INTO source_availability (sutta_number, source_id, has_pali, has_translation, is_complete, coverage_type) VALUES (?, ?, ?, 1, 1, 'partial')""", (sutta_number, self.SOURCE_ID, 1 if has_pali else 0))
            self.conn.commit()
        except sqlite3.IntegrityError:
            pass

    def _get_nikaya(self, sutta_number: str) -> str:
        s = sutta_number.lower()
        if s.startswith("dn"): return "dn"
        elif s.startswith("mn"): return "mn"
        elif s.startswith("sn"): return "sn"
        elif s.startswith("an"): return "an"
        return "kn"

    def _setup_log_table(self):
        cur = self.conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS ingestion_log (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT DEFAULT CURRENT_TIMESTAMP, source_id TEXT, sutta_number TEXT, normalized_id TEXT, status TEXT, reason TEXT, text_hash TEXT, char_count INTEGER)""")
        self.conn.commit()

    def _log_operation(self, source_table: str, sutta_number: Optional[str], status: str, dry_run: bool):
        cur = self.conn.cursor()
        try:
            cur.execute("""INSERT INTO ingestion_log (source_id, sutta_number, status, reason, timestamp) VALUES (?, ?, ?, ?, ?)""", (self.SOURCE_ID, source_table, status, sutta_number or "", datetime.now().isoformat()))
            if not dry_run:
                self.conn.commit()
        except Exception as e:
            logger.debug(f"Log error: {e}")

    def _get_coverage(self) -> dict:
        if not self.conn: return {"tpk_count": 0, "tpk_pct": 0}
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sutta_master")
        total = cur.fetchone()[0] or 0
        cur.execute("SELECT COUNT(DISTINCT sutta_number) FROM source_availability WHERE source_id = ?", (self.SOURCE_ID,))
        tpk_count = cur.fetchone()[0] or 0
        return {"total_suttas": total, "tpk_count": tpk_count, "tpk_pct": round(tpk_count / max(1, total) * 100, 1)}


def run_tpk_mapper(db_path: str, dry_run: bool = False) -> dict:
    mapper = TpkMapper(db_path)
    result = mapper.run(dry_run=dry_run)
    mapper.close()
    return result