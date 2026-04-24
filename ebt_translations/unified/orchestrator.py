"""Unified Pipeline Orchestrator - Coordinates all extraction and processing."""

import hashlib
import json
import logging
import re
import sqlite3
from pathlib import Path
from typing import Optional

from ebt_translations.unified.models import UnifiedConfig, OperationResult
from ebt_translations.unified.extract_tbw import extract_tbw
from ebt_translations.unified.extract_db import extract_all_from_db
from ebt_translations.unified.extract_pau import map_pau

logger = logging.getLogger(__name__)


class UnifiedOrchestrator:
    """Orchestrate the unified EBT pipeline."""

    def __init__(self, config: Optional[UnifiedConfig] = None):
        self.config = config or UnifiedConfig()
        self._translations = []
        self._sutta_entries = {}
        self._operations = []
        self._stats = {
            "total_extracted": 0,
            "total_valid": 0,
            "duplicates_removed": 0,
            "invalid": 0,
        }

    def run(self) -> dict:
        """Run the full unified pipeline."""
        logger.info("=" * 60)
        logger.info("UNIFIED PIPELINE STARTING")
        logger.info("=" * 60)

        self._extract_all_sources()
        self._normalize_sutta_ids()
        self._clean_text()
        self._deduplicate()
        self._link_to_suttas()
        self._build_output()
        self._generate_report()

        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 60)
        
        return self._stats

    def _extract_all_sources(self):
        """Extract from all sources."""
        logger.info("\n[1/6] Extracting from all sources...")
        
        db_path = Path(self.config.db_path)
        tbw_dir = Path(self.config.tbw_html_dir)
        
        sources_extracted = {
            "sc": 0,
            "tbw": 0,
            "dt": 0,
            "ati": 0,
            "pau": 0,
        }
        
        logger.info("  Extracting from DB...")
        all_db_data = extract_all_from_db(db_path)
        
        for d in all_db_data:
            src = d.get("source_id", "")
            if src in sources_extracted:
                sources_extracted[src] += 1
        
        self._translations.extend(all_db_data)
        
        if tbw_dir.exists():
            logger.info("  Extracting TBW from HTML...")
            tbw_data = extract_tbw(tbw_dir)
            self._translations.extend(tbw_data)
            sources_extracted["tbw"] = len(tbw_data)
        
        logger.info("  Extracting PAU with AI mapping...")
        pau_data = map_pau(db_path, self.config.ollama_url)
        self._translations.extend(pau_data)
        sources_extracted["pau"] = len(pau_data)
        
        self._operations.append(OperationResult(
            operation="extract",
            source="all",
            inserted=sum(sources_extracted.values()),
            message=str(sources_extracted),
        ))
        
        self._stats["total_extracted"] = len(self._translations)
        logger.info(f"  Total extracted: {len(self._translations)}")

    def _normalize_sutta_ids(self):
        """Normalize all sutta IDs to canonical format."""
        logger.info("\n[2/6] Normalizing sutta IDs...")
        
        normalized = []
        invalid_count = 0
        
        for trans in self._translations:
            sutta_id = trans.get("sutta_number", "")
            if not sutta_id:
                invalid_count += 1
                continue
            
            new_id = self._normalize_id(sutta_id)
            if new_id:
                trans["sutta_number"] = new_id
                normalized.append(trans)
            else:
                invalid_count += 1
        
        self._translations = normalized
        self._operations.append(OperationResult(
            operation="normalize",
            source="all",
            inserted=len(normalized),
            skipped=invalid_count,
        ))
        
        logger.info(f"  Normalized: {len(normalized)}, Invalid: {invalid_count}")

    def _normalize_id(self, sutta_id: str) -> Optional[str]:
        """Normalize a single sutta ID."""
        clean = sutta_id.strip().lower()
        
        patterns = [
            (re.compile(r"^dn0*(\d+)$", re.IGNORECASE), "dn"),
            (re.compile(r"^mn0*(\d+)$", re.IGNORECASE), "mn"),
            (re.compile(r"^sn0*(\d+)[._-](\d+)$", re.IGNORECASE), "sn"),
            (re.compile(r"^an0*(\d+)[._-](\d+)$", re.IGNORECASE), "an"),
        ]
        
        for pattern, nikaya in patterns:
            match = pattern.match(clean)
            if match:
                groups = match.groups()
                num = int(groups[0])
                vagga = int(groups[1]) if len(groups) > 1 else 1
                
                if nikaya == "an" and vagga < 1:
                    continue
                    
                if nikaya in ["dn", "mn"]:
                    return f"{nikaya}{num}"
                else:
                    return f"{nikaya}{groups[0]}.{groups[1]}"
        
        return None

    def _clean_text(self):
        """Clean translation text."""
        logger.info("\n[3/6] Cleaning text...")
        
        cleaned = []
        
        for trans in self._translations:
            text = trans.get("text", "")
            if not text or len(text.strip()) < self.config.min_text_length:
                self._stats["invalid"] += 1
                continue
            
            clean_text = self._clean_translation_text(text)
            if clean_text and len(clean_text) >= self.config.min_text_length:
                trans["text"] = clean_text
                cleaned.append(trans)
            else:
                self._stats["invalid"] += 1
        
        self._translations = cleaned
        self._stats["total_valid"] = len(cleaned)
        
        self._operations.append(OperationResult(
            operation="clean",
            source="all",
            inserted=len(cleaned),
            skipped=self._stats["invalid"],
        ))
        
        logger.info(f"  Valid: {len(cleaned)}, Invalid: {self._stats['invalid']}")

    def _clean_translation_text(self, text: str) -> str:
        """Clean translation text."""
        text = re.sub(r"<[^>]+>", "", text)
        text = " ".join(text.split())
        return text.strip()

    def _deduplicate(self):
        """Remove duplicate translations."""
        logger.info("\n[4/6] Deduplicating...")
        
        seen = {}
        unique = []
        
        for trans in self._translations:
            key = f"{trans['sutta_number']}:{trans['source_id']}"
            text_hash = hashlib.md5(trans["text"].lower().encode()).hexdigest()
            
            if text_hash in seen:
                self._stats["duplicates_removed"] += 1
                continue
            
            seen[text_hash] = key
            unique.append(trans)
        
        self._translations = unique
        self._stats["total_valid"] = len(unique)
        
        self._operations.append(OperationResult(
            operation="deduplicate",
            source="all",
            inserted=len(unique),
            skipped=self._stats["duplicates_removed"],
        ))
        
        logger.info(f"  Unique: {len(unique)}, Duplicates removed: {self._stats['duplicates_removed']}")

    def _link_to_suttas(self):
        """Link translations to suttas."""
        logger.info("\n[5/6] Linking to suttas...")
        
        self._sutta_entries = {}
        
        for trans in self._translations:
            sutta_id = trans["sutta_number"]
            
            if sutta_id not in self._sutta_entries:
                nikaya = self._extract_nikaya(sutta_id)
                self._sutta_entries[sutta_id] = {
                    "sutta_number": sutta_id,
                    "nikaya": nikaya,
                    "translations": [],
                }
            
            self._sutta_entries[sutta_id]["translations"].append({
                "source": trans["source_id"],
                "text": trans["text"],
            })
        
        self._operations.append(OperationResult(
            operation="link",
            source="all",
            inserted=len(self._sutta_entries),
        ))
        
        logger.info(f"  Linked suttas: {len(self._sutta_entries)}")

    def _extract_nikaya(self, sutta_id: str) -> str:
        """Extract nikaya from sutta ID."""
        if sutta_id.startswith("dn"):
            return "dn"
        elif sutta_id.startswith("mn"):
            return "mn"
        elif sutta_id.startswith("sn"):
            return "sn"
        elif sutta_id.startswith("an"):
            return "an"
        elif sutta_id.startswith(("dhp", "iti", "snp", "thag", "thig", "ud", "kp")):
            return "kn"
        return "unknown"

    def _build_output(self):
        """Build final JSONL output."""
        logger.info("\n[6/6] Building output...")
        
        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / "ebt_unified.jsonl"
        
        with open(output_file, "w", encoding="utf-8") as f:
            for sutta_id, entry in sorted(self._sutta_entries.items()):
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        
        logger.info(f"  Output: {output_file}")
        logger.info(f"  Total suttas: {len(self._sutta_entries)}")

    def _generate_report(self):
        """Generate coverage report."""
        logger.info("\nGenerating coverage report...")
        
        reports_dir = Path(self.config.reports_dir)
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        source_counts = {}
        for entry in self._sutta_entries.values():
            for trans in entry["translations"]:
                src = trans["source"]
                source_counts[src] = source_counts.get(src, 0) + 1
        
        coverage = {
            "total_suttas": len(self._sutta_entries),
            "sources": source_counts,
            "stats": self._stats,
            "operations": [
                {
                    "operation": op.operation,
                    "source": op.source,
                    "inserted": op.inserted,
                    "skipped": op.skipped,
                    "failed": op.failed,
                }
                for op in self._operations
            ],
        }
        
        report_file = reports_dir / "unified_coverage.json"
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(coverage, f, indent=2)
        
        logger.info(f"  Report: {report_file}")


def run_unified_pipeline(config: Optional[UnifiedConfig] = None) -> dict:
    """Quick function to run the unified pipeline."""
    orchestrator = UnifiedOrchestrator(config)
    return orchestrator.run()