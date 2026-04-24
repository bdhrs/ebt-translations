"""Source data loader for EBT ingestion pipeline.

Loads data from:
- SQLite DBs
- XML files  
- HTML files
- offline folders
"""

import logging
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class LoadedSutta:
    """Container for loaded sutta data."""
    source_id: str
    raw_id: str
    text: str
    language: str = "en"
    metadata: Optional[dict] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class SourceLoader:
    """Load sutta data from various sources."""

    SOURCE_PATTERNS = {
        "sc": "suttacentral",
        "tbw": "buddhas_words",
        "dt": "dhamma_talks",
        "ati": "accesstoinsight",
        "tpk": "tipitaka_pali",
        "pau": "pa_auk",
    }

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self, db_path: Optional[Path] = None) -> sqlite3.Connection:
        """Connect to database."""
        if db_path:
            self.db_path = db_path
        if not self.db_path or not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {db_path}")
        
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
        return self._conn

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def load_from_sqlite(
        self,
        source: str,
        table_name: Optional[str] = None,
    ) -> list[LoadedSutta]:
        """Load suttas from SQLite table.
        
        Args:
            source: Source identifier (sc, tbw, dt, etc.)
            table_name: Optional explicit table name
            
        Returns:
            List of LoadedSutta objects
        """
        conn = self.connect()
        cur = conn.cursor()
        
        if table_name is None:
            table_name = f"{source}_dn"  # Default to DN table
        
        results: list[LoadedSutta] = []
        
        try:
            # Try to detect sutta_id column
            for col_name in ["sutta_id", "sutta_number", "id"]:
                try:
                    cur.execute(f"SELECT {col_name} FROM {table_name} LIMIT 1")
                    break
                except sqlite3.OperationalError:
                    continue
            else:
                logger.warning(f"Table {table_name} not found")
                return results

            # Load all columns
            cur.execute(f"SELECT * FROM {table_name}")
            columns = [desc[0] for desc in cur.description]
            
            for row in cur.fetchall():
                row_data = dict(zip(columns, row))
                
                # Find sutta ID
                sutta_id = None
                for col in ["sutta_id", "sutta_number", "id"]:
                    if col in row_data and row_data[col]:
                        sutta_id = str(row_data[col]).strip()
                        break
                
                if not sutta_id:
                    continue
                
                # Extract text
                text = ""
                for col in ["translation_text", "translation_markdown", "text", "content"]:
                    if col in row_data and row_data[col]:
                        text = str(row_data[col])
                        break
                
                if not text:
                    continue
                
                # Determine language
                lang = "en"
                if "language" in row_data:
                    lang = str(row_data.get("language", "en"))
                elif source in ["tpk", "cst"]:
                    lang = "pli"
                
                results.append(LoadedSutta(
                    source_id=source,
                    raw_id=sutta_id,
                    text=text,
                    language=lang,
                    metadata=row_data,
                ))
                
        except sqlite3.OperationalError as e:
            logger.error(f"Error loading from {table_name}: {e}")
        
        logger.info(f"Loaded {len(results)} suttas from {source} ({table_name})")
        return results

    def load_from_xml(
        self,
        source: str,
        xml_dir: Path,
    ) -> list[LoadedSutta]:
        """Load suttas from XML files."""
        results: list[LoadedSutta] = []
        
        if not xml_dir.exists():
            logger.warning(f"XML directory not found: {xml_dir}")
            return results
        
        import xml.etree.ElementTree as ET
        
        for xml_file in xml_dir.glob("**/*.xml"):
            try:
                tree = ET.parse(xml_file)
                root = tree.getroot()
                
                for elem in root.findall(".//sutta"):
                    raw_id = elem.get("id", "")
                    if not raw_id:
                        continue
                    
                    text = ""
                    for child in elem:
                        if child.tag in ["translation", "text", "content"]:
                            text = child.text or ""
                            break
                    
                    if text:
                        results.append(LoadedSutta(
                            source_id=source,
                            raw_id=raw_id,
                            text=text,
                            metadata={"source_file": str(xml_file)},
                        ))
                        
            except ET.ParseError as e:
                logger.error(f"Error parsing {xml_file}: {e}")
        
        logger.info(f"Loaded {len(results)} suttas from XML")
        return results

    def load_from_html_folder(
        self,
        source: str,
        html_dir: Path,
    ) -> list[LoadedSutta]:
        """Load suttas from HTML files (e.g., BW2 offline)."""
        results: list[LoadedSutta] = []
        
        if not html_dir.exists():
            logger.warning(f"HTML directory not found: {html_dir}")
            return results
        
        import re
        from bs4 import BeautifulSoup
        
        for html_file in html_dir.glob("**/*.html"):
            try:
                with open(html_file, encoding="utf-8", errors="ignore") as f:
                    soup = BeautifulSoup(f.read(), "html.parser")
                
                # Extract sutta ID from filename
                filename = html_file.stem
                raw_id = self._extract_id_from_filename(source, filename)
                
                if not raw_id:
                    continue
                
                # Extract text content
                text = ""
                for elem in soup.find_all(["div", "p"]):
                    if elem.get("class"):
                        text = elem.get_text(strip=True)
                        if len(text) > 100:
                            break
                
                if text:
                    results.append(LoadedSutta(
                        source_id=source,
                        raw_id=raw_id,
                        text=text,
                        metadata={"source_file": str(html_file)},
                    ))
                    
            except Exception as e:
                logger.error(f"Error parsing {html_file}: {e}")
        
        logger.info(f"Loaded {len(results)} suttas from HTML")
        return results

    def _extract_id_from_filename(self, source: str, filename: str) -> Optional[str]:
        """Extract sutta ID from HTML filename.
        
        Examples:
            - an1.1.html -> an1.1
            - dn01.html -> dn1
            - sn22_85.html -> sn22.85
        """
        # Remove extension
        name = Path(filename).stem
        
        # Match patterns like dn1, dn01, an1.1, an1_1, sn22.85, sn22_85
        patterns = {
            "dn": r"^dn(\d+)$",
            "mn": r"^mn(\d+)$",
            "sn": r"^sn(\d+)[._](\d+)$",
            "an": r"^an(\d+)[._](\d+)$",
        }
        
        for prefix, pattern in patterns.items():
            match = re.match(pattern, name, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) == 1:
                    return f"{prefix}{int(groups[0])}"
                else:
                    return f"{prefix}{groups[0]}.{groups[1]}"
        
        return None

    def discover_sources(self, data_dir: Path) -> dict[str, list[Path]]:
        """Discover available data sources in data directory."""
        sources: dict[str, list[Path]] = {}
        
        # Check for SQLite databases
        for db_file in data_dir.glob("**/*.db"):
            if db_file.name.startswith("."):
                continue
            
            name = db_file.stem.lower()
            
            # Map to source ID
            source_id = None
            for src, pattern in self.SOURCE_PATTERNS.items():
                if pattern in name:
                    source_id = src
                    break
            
            if source_id:
                if source_id not in sources:
                    sources[source_id] = []
                sources[source_id].append(db_file)
        
        # Check for directories
        for subdir in data_dir.iterdir():
            if not subdir.is_dir():
                continue
            
            name = subdir.name.lower()
            
            for src, pattern in self.SOURCE_PATTERNS.items():
                if pattern in name:
                    if src not in sources:
                        sources[src] = []
                    sources[src].append(subdir)
                    break
        
        logger.info(f"Discovered sources: {list(sources.keys())}")
        return sources