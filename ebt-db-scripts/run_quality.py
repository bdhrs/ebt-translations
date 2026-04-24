#!/usr/bin/env python3
"""EBT Quality Pipeline CLI.

Run quality pipeline:
    python ebt-db-scripts/run_quality.py

Options:
    --dry-run: Don't write to DB
    --limit: Process only N suttas
    --source: Process specific source only
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ebt_translations.quality.cleaner import TextCleaner
from ebt_translations.quality.deduplicator_quality import QualityDeduplicator
from ebt_translations.quality.scorer import QualityScorer
from ebt_translations.quality.structurer import TextStructurer
from ebt_translations.quality.filter import QualityFilter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).parent.parent / "data" / "db" / "EBT_Unified (1).db"
DEFAULT_OUTPUT = Path(__file__).parent.parent / "data" / "processed"


def main():
    parser = argparse.ArgumentParser(description="Run EBT quality pipeline")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Database path")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT, help="Output directory")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--limit", type=int, default=0, help="Limit suttas to process")
    parser.add_argument("--source", choices=["sc", "tbw", "dt", "tpk", "pau"], help="Source to process")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    db_path = args.db
    
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        sys.exit(1)
    
    logger.info(f"Starting quality pipeline")
    logger.info(f"Database: {db_path}")
    
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    
    cleaner = TextCleaner()
    dedup = QualityDeduplicator()
    scorer = QualityScorer()
    structurer = TextStructurer()
    quality_filter = QualityFilter()
    
    sources = [args.source] if args.source else ["sc", "tbw", "dt", "tpk"]
    
    all_translations = []
    stats = {
        "total_processed": len(all_translations),
        "cleaned": 0,
        "duplicates_removed": 0,
        "scored": 0,
        "filtered": 0,
    }
    
    # Load translations
    logger.info("Loading translations...")
    
    for source in sources:
        for nikaya in ["dn", "mn", "sn", "an"]:
            table = f"{source}_{nikaya}"
            
            try:
                cur.execute(f"""
                    SELECT sutta_number, translation_text 
                    FROM {table}
                    WHERE translation_text IS NOT NULL
                """)
            except sqlite3.OperationalError:
                continue
            
            for sutta_number, text in cur.fetchall():
                if not sutta_number or not text:
                    continue
                
                all_translations.append({
                    "sutta_number": sutta_number,
                    "source_id": source,
                    "text": text,
                    "nikaya": nikaya,
                })
                
                if args.limit and len(all_translations) >= args.limit:
                    break
        
        if args.limit and len(all_translations) >= args.limit:
            break
    
    logger.info(f"Loaded {len(all_translations)} translations")
    
    # Clean
    logger.info("Cleaning texts...")
    cleaned = []
    
    for trans in all_translations:
        result = cleaner.clean(trans["text"], use_ai=False)
        trans["cleaned_text"] = result.cleaned
        trans["was_modified"] = result.was_modified
        
        if result.was_modified:
            stats["cleaned"] += 1
        
        cleaned.append(trans)
    
    logger.info(f"Cleaned {stats['cleaned']} texts")
    
    # Deduplicate
    logger.info("Removing duplicates...")
    
    deduped = []
    for trans in cleaned:
        result = dedup.check(
            trans["sutta_number"],
            trans["source_id"],
            trans["cleaned_text"],
        )
        
        if not result.is_duplicate:
            deduped.append(trans)
        else:
            stats["duplicates_removed"] += 1
    
    logger.info(f"Removed {stats['duplicates_removed']} duplicates")
    logger.info(f"Unique translations: {len(deduped)}")
    
    # Score
    logger.info("Scoring translations...")
    
    scored = scorer.score_batch(deduped)
    stats["scored"] = len(scored)
    
    # Filter
    logger.info("Filtering...")
    
    filtered = quality_filter.filter_batch(scored)
    stats["filtered"] = len(filtered)
    
    # Structure
    logger.info("Structuring...")
    
    structured = []
    for trans in filtered:
        text = trans.get("cleaned_text", trans.get("text", ""))
        s = structurer.structure(text, trans["sutta_number"])
        trans["structured"] = structurer.to_dict(s)
        structured.append(trans)
    
    # Group by sutta
    logger.info("Grouping by sutta...")
    
    packs = {}
    for trans in structured:
        sutta = trans["sutta_number"]
        
        if sutta not in packs:
            packs[sutta] = {
                "sutta_number": sutta,
                "sources": set(),
                "translations": [],
            }
        
        packs[sutta]["sources"].add(trans["source_id"])
        packs[sutta]["translations"].append({
            "source_id": trans["source_id"],
            "text": trans.get("cleaned_text", trans.get("text", "")),
            "score": trans.get("score", 0),
            "was_cleaned": trans.get("was_modified", False),
        })
    
    # Stats
    total_suttas = len(packs)
    total_sources = sum(len(p["sources"]) for p in packs.values())
    avg_sources = total_sources / max(1, total_suttas)
    
    # Output files
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Write sutta_translation_packs.json
    packs_output = {}
    for sutta, data in packs.items():
        packs_output[sutta] = {
            "sources": list(data["sources"]),
            "translation_count": len(data["translations"]),
            "translations": data["translations"],
        }
    
    with open(output_dir / "sutta_translation_packs.json", "w") as f:
        json.dump(packs_output, f, indent=2)
    
    # Write top_translations.json
    top = []
    for sutta, data in packs.items():
        sorted_trans = sorted(
            data["translations"], 
            key=lambda x: x.get("score", 0), 
            reverse=True
        )
        top.append({
            "sutta_number": sutta,
            "source_count": len(data["sources"]),
            "top_translation": sorted_trans[0] if sorted_trans else None,
        })
    
    with open(output_dir / "top_translations.json", "w") as f:
        json.dump(top, f, indent=2)
    
    # Write quality_report.json
    quality_report = {
        "timestamp": datetime.now().isoformat(),
        "stats": {
            "total_suttas": total_suttas,
            "avg_sources_per_sutta": round(avg_sources, 2),
            "total_translations": len(all_translations),
            "unique_translations": len(deduped),
            "filtered_translations": stats["filtered"],
            "cleaned_texts": stats["cleaned"],
            "duplicates_removed": stats["duplicates_removed"],
        },
        "sources": sources,
        "by_source": {},
    }
    
    # By source breakdown
    for source in sources:
        source_trans = [t for t in structured if t["source_id"] == source]
        quality_report["by_source"][source] = {
            "count": len(source_trans),
            "avg_score": sum(t.get("score", 0) for t in source_trans) / max(1, len(source_trans)),
        }
    
    with open(output_dir / "quality_report.json", "w") as f:
        json.dump(quality_report, f, indent=2)
    
    conn.close()
    
    _print_summary(stats, quality_report, output_dir)


def _print_summary(stats: dict, report: dict, output_dir: Path):
    """Print summary."""
    
    print("\n" + "=" * 60)
    print("QUALITY PIPELINE SUMMARY")
    print("=" * 60)
    
    print(f"\n{'Total processed:':<25} {stats.get('total_processed', stats.get('scored', 0)):,}")
    print(f"{'Cleaned:':<25} {stats.get('cleaned', 0):,}")
    print(f"{'Duplicates removed:':<25} {stats.get('duplicates_removed', 0):,}")
    print(f"{'Filtered:':<25} {stats.get('filtered', 0):,}")
    
    print("\n" + "-" * 60)
    print("COVERAGE")
    print("-" * 60)
    
    stats_report = report.get("stats", {})
    print(f"\n{'Total suttas:':<25} {stats_report.get('total_suttas', 0):,}")
    print(f"{'Avg sources/sutta:':<25} {stats_report.get('avg_sources_per_sutta', 0):.2f}")
    print(f"{'Unique translations:':<25} {stats_report.get('unique_translations', 0):,}")
    
    print("\n" + "-" * 60)
    print("BY SOURCE")
    print("-" * 60)
    
    by_source = report.get("by_source", {})
    for source, data in by_source.items():
        print(f"  {source:<10} {data.get('count', 0):>6,} translations (avg score: {data.get('avg_score', 0):.1f})")
    
    print(f"\nOutput: {output_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()