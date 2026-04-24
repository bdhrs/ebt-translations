"""Coverage Reporter - Generate reports for EBT coverage tracking."""

import csv
import json
import logging
import sys
import io
from datetime import datetime
from pathlib import Path
from typing import Optional

from ebt_translations.coverage.coverage_analyzer import CoverageAnalyzer

logger = logging.getLogger(__name__)


class CoverageReporter:
    """Generate coverage reports in various formats."""
    
    SOURCE_NAMES = {
        "sc": "SuttaCentral",
        "tbw": "Buddha Words",
        "dt": "Dhamma Talks",
        "ati": "Access to Insight",
        "tpk": "Tipitaka Pali",
        "pau": "Pa Auk AI",
        "cst": "Chaṭṭha Sangayana",
        "epi": "ePitaka",
    }
    
    def __init__(self, output_dir: Optional[Path] = None):
        self.output_dir = output_dir or self._find_output_dir()
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def _find_output_dir(self) -> Path:
        """Find or create output directory."""
        # Navigate from ebt_translations/coverage/ to repo root and then to data/reports
        repo_root = Path(__file__).parent.parent.parent  # Goes from coverage -> ebt_translations -> repo root
        reports_dir = repo_root / "data" / "reports"
        
        # Create if needed
        if not reports_dir.exists():
            try:
                reports_dir.mkdir(parents=True, exist_ok=True)
            except Exception:
                return Path("data/reports")
        
        return reports_dir
    
    def report_console(self, analyzer: CoverageAnalyzer) -> None:
        """Print coverage to console."""
        try:
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        except Exception:
            pass
        
        summary = analyzer.get_summary()
        
        print("\n" + "=" * 60)
        print("COVERAGE REPORT")
        print("=" * 60)
        
        print(f"\nTotal master suttas: {summary['total_master_suttas']:,}")
        
        print("\n" + "-" * 60)
        print("BY SOURCE")
        print("-" * 60)
        
        sources = summary.get("sources", {})
        for source_id, data in sorted(sources.items(), key=lambda x: x[1].get("total_scraped", 0), reverse=True):
            name = self.SOURCE_NAMES.get(source_id, source_id)
            scraped = data.get("total_scraped", 0)
            expected = data.get("total_expected", 0)
            pct = data.get("coverage_percent", 0)
            missing = data.get("missing_count", 0)
            print(f"\n{name} ({source_id}):")
            print(f"  Scraped: {scraped:,} / {expected:,} ({pct}%)")
            if missing > 0:
                print(f"  Missing: {missing:,}")
        
        print("\n" + "-" * 60)
        print("BY NIKAYA")
        print("-" * 60)
        
        by_nikaya = summary.get("by_nikaya", {})
        for nikaya, data in by_nikaya.items():
            name = self._nikaya_name(nikaya)
            print(f"  {name}: {data.get('scraped', 0):,} / {data.get('expected', 0):,} ({data.get('coverage_percent', 0)}%)")
        
        print("\n" + "-" * 60)
        print("MULTI-SOURCE COVERAGE")
        print("-" * 60)
        
        multi = summary.get("multi_source", {})
        total_multi = sum(multi.values())
        for count, num in sorted(multi.items()):
            pct = round(num / max(1, total_multi) * 100, 1)
            print(f"  {count} sources: {num:,} suttas ({pct}%)")
        
        print("\n" + "=" * 60)
    
    def report_json(self, analyzer: CoverageAnalyzer) -> Path:
        """Generate JSON report."""
        summary = analyzer.get_summary()
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "total_master_suttas": summary["total_master_suttas"],
            "sources": {},
            "by_nikaya": summary.get("by_nikaya", {}),
            "multi_source": summary.get("multi_source", {}),
        }
        
        for source_id, data in summary.get("sources", {}).items():
            report["sources"][source_id] = {
                "name": self.SOURCE_NAMES.get(source_id, source_id),
                "total_expected": data.get("total_expected", 0),
                "total_scraped": data.get("total_scraped", 0),
                "coverage_percent": data.get("coverage_percent", 0),
                "missing_count": data.get("missing_count", 0),
                "missing_suttas": [],
            }
        
        output_path = self.output_dir / "coverage.json"
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"JSON report: {output_path}")
        return output_path
    
    def report_csv(self, analyzer: CoverageAnalyzer) -> Path:
        """Generate CSV report."""
        summary = analyzer.get_summary()
        
        output_path = self.output_dir / "coverage.csv"
        
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["source", "total_expected", "total_scraped", "coverage_percent", "missing_count"])
            
            for source_id, data in summary.get("sources", {}).items():
                writer.writerow([
                    source_id,
                    data.get("total_expected", 0),
                    data.get("total_scraped", 0),
                    data.get("coverage_percent", 0),
                    data.get("missing_count", 0),
                ])
        
        logger.info(f"CSV report: {output_path}")
        return output_path
    
    def report_missing(self, analyzer: CoverageAnalyzer) -> list[Path]:
        """Generate missing sutta files per source."""
        output_paths = []
        
        missing_dir = self.output_dir / "missing_by_source"
        missing_dir.mkdir(parents=True, exist_ok=True)
        
        for source_id in CoverageAnalyzer.SOURCES:
            coverage = analyzer.analyze_source(source_id)
            
            if coverage.missing_suttas:
                output_file = missing_dir / f"missing_suttas_{source_id}.txt"
                
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(f"# Missing suttas from {source_id}\n")
                    f.write(f"# Generated: {datetime.now().isoformat()}\n")
                    f.write(f"# Total missing: {len(coverage.missing_suttas)}\n\n")
                    
                    for sutta in sorted(coverage.missing_suttas):
                        f.write(f"{sutta}\n")
                
                output_paths.append(output_file)
                logger.info(f"Missing list: {output_file}")
        
        return output_paths
    
    def generate_all(self, analyzer: CoverageAnalyzer) -> dict:
        """Generate all reports."""
        results = {
            "console": True,
            "json": str(self.report_json(analyzer)),
            "csv": str(self.report_csv(analyzer)),
            "missing_files": [],
        }
        
        missing_paths = self.report_missing(analyzer)
        results["missing_files"] = [str(p) for p in missing_paths]
        
        return results
    
    def _nikaya_name(self, code: str) -> str:
        """Get nikaya full name."""
        names = {
            "dn": "Digha Nikaya",
            "mn": "Majjhima Nikaya",
            "sn": "Samyutta Nikaya",
            "an": "Anguttara Nikaya",
            "kn": "Khuddaka Nikaya",
        }
        return names.get(code, code.upper())


def generate_report(
    db_path: Optional[str] = None,
    output_dir: Optional[Path] = None,
    console: bool = True,
    json_out: bool = True,
    csv_out: bool = True,
    missing_out: bool = True,
) -> dict:
    """Quick function to generate all reports."""
    analyzer = CoverageAnalyzer(db_path)
    reporter = CoverageReporter(output_dir)
    
    results = {}
    
    if console:
        reporter.report_console(analyzer)
        results["console"] = True
    
    if json_out:
        results["json"] = str(reporter.report_json(analyzer))
    
    if csv_out:
        results["csv"] = str(reporter.report_csv(analyzer))
    
    if missing_out:
        paths = reporter.report_missing(analyzer)
        results["missing_files"] = [str(p) for p in paths]
    
    analyzer.close()
    return results