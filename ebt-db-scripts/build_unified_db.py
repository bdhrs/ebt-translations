import sqlite3
import sys
from datetime import datetime

import pandas as pd

from ebt_translations.paths import EXCEL_FILE_PATH, OLD_DB_PATH, UNIFIED_DB_PATH, ensure_data_directories


def prnt(msg):
    sys.stdout.buffer.write((str(msg) + "\n").encode("utf-8"))


SOURCES_DATA = [
    ("sc", "sc", "SuttaCentral", "Various", "en", "human", 1, 1, "https://suttacentral.net", "CC BY-NC-SA 4.0", 0),
    ("tbw", "tbw", "The Buddha's Words", "Bhikkhu Bodhi", "en", "human", 1, 1, "https://www.budsas.org", "Public Domain", 0),
    ("dt", "dt", "Dhamma Talks", "Thanissaro Bhikkhu", "en", "human", 1, 1, "https://www.dhammatalks.org/suttas", "CC BY-NC-ND 4.0", 0),
    ("cst", "cst", "Chaṭṭha Saṅgāyana Tipiṭaka", "6th Council", "pli", "human", 1, 0, "https://tipitaka.org", "Public Domain", 0),
    ("tpk", "tpk", "Tipitaka Pali", "Pa Auk Society", "pli", "human", 1, 0, "https://tipitaka.paauksociety.org", "Restricted", 0),
    ("pau", "pau", "Pa Auk AI Translation", "AI + Human", "en", "ai", 1, 1, "https://tipitaka.paauksociety.org", "Various", 0),
    ("epi", "epi", "ePitaka", "AI Translation", "en", "ai", 1, 1, "https://epitaka.org", "Various", 0),
]

NIKAYAS_DATA = [
    ("dn", "Digha Nikaya", "long", 34, 3, "Long discourses (Suttas 1-34)"),
    ("mn", "Majjhima Nikaya", "middle", 152, 15, "Middle-length discourses (Suttas 1-152)"),
    ("sn", "Samyutta Nikaya", "linked", 2309, 56, "Linked/grouped discourses by topic"),
    ("an", "Anguttara Nikaya", "numerical", 2300, 11, "Numerical discourses (1-11 factors)"),
    ("kn", "Khuddaka Nikaya", "minor", 18, 18, "Minor collections (15+ texts)"),
]

KN_SUBS = [
    ("dhp", "Dhammapada", 423, "kn"),
    ("iti", "Itivuttaka", 112, "kn"),
    ("snp", "Sutta Nipata", 77, "kn"),
    ("thag", "Theragatha", 264, "kn"),
    ("thig", "Therigatha", 73, "kn"),
    ("ud", "Udana", 88, "kn"),
    ("kp", "Khuddakapatha", 9, "kn"),
    ("ps", "Patisambhida", 357, "kn"),
    ("ap", "Apadana", 547, "kn"),
    ("bv", "Buddhavamsa", 24, "kn"),
    ("cp", "Cariyapitaka", 35, "kn"),
]

SOURCES_LIST = ["sc", "tbw", "dt", "cst", "tpk", "pau"]
NIKAYAS_LIST = ["dn", "mn", "sn", "an", "kn"]


def create_base_schema(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sources (
            source_id TEXT PRIMARY KEY,
            source_code TEXT,
            source_name TEXT,
            translator TEXT,
            language TEXT,
            translation_type TEXT,
            has_pali INTEGER,
            has_translation INTEGER,
            url TEXT,
            license TEXT,
            completeness_pct INTEGER DEFAULT 0
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nikayas (
            nikaya_code TEXT PRIMARY KEY,
            nikaya_name TEXT,
            division_type TEXT,
            expected_suttas INTEGER,
            vagga_count INTEGER,
            description TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS kn_subcollections (
            sub_code TEXT PRIMARY KEY,
            sub_name TEXT,
            expected_count INTEGER,
            parent_nikaya TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sutta_master (
            sutta_number TEXT PRIMARY KEY,
            nikaya TEXT,
            sub_collection TEXT,
            has_pali TEXT,
            has_english TEXT,
            has_sc_pali INTEGER DEFAULT 0,
            has_sc_english INTEGER DEFAULT 0,
            last_updated TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS source_availability (
            sutta_number TEXT NOT NULL,
            source_id TEXT NOT NULL,
            has_pali INTEGER DEFAULT 0,
            has_translation INTEGER DEFAULT 0,
            is_complete INTEGER DEFAULT 0,
            PRIMARY KEY (sutta_number, source_id)
        )
        """
    )


def create_source_table(conn, cur, source_code, nikaya_code):
    table_name = f"{source_code}_{nikaya_code}"
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sutta_number TEXT NOT NULL UNIQUE,
            sutta_title TEXT,
            pali_text TEXT,
            translation_text TEXT,
            source_url TEXT,
            char_count INTEGER,
            is_complete BOOLEAN DEFAULT 1,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (sutta_number) REFERENCES sutta_master(sutta_number)
        )
        """
    )
    conn.commit()


def load_excel_suttas(excel_file):
    all_suttas = set()
    try:
        df_excel = pd.read_excel(excel_file)
        prnt(f"  Excel loaded: {len(df_excel)} rows")
    except Exception as exc:
        prnt(f"  Excel import error: {exc}")
        return all_suttas

    if "sc_code" in df_excel.columns:
        for code in df_excel["sc_code"].dropna().unique():
            if isinstance(code, str) and code:
                all_suttas.add(code.strip().lower())

    if "cst_code" in df_excel.columns:
        for code in df_excel["cst_code"].dropna().unique():
            if isinstance(code, str) and code:
                normalized = code.strip().lower().split(".")[0]
                if len(normalized) >= 2:
                    all_suttas.add(normalized)

    if "dpd_code" in df_excel.columns:
        for code in df_excel["dpd_code"].dropna().unique():
            if isinstance(code, str) and code:
                all_suttas.add(code.strip().lower().replace("_", ""))

    prnt(f"  Found {len(all_suttas)} unique sutta numbers from Excel")
    return all_suttas


def read_existing_sutta_ids(old_cur, all_suttas):
    sutta_count = 0

    for nikaya in ["dn", "mn", "sn", "an"]:
        table = f"sc_{nikaya}"
        try:
            old_cur.execute(f"SELECT sutta_id FROM {table}")
            for (uid,) in old_cur.fetchall():
                if uid:
                    normalized = uid.strip().lower()
                    all_suttas.add(normalized)
                    sutta_count += 1
        except Exception:
            pass

    try:
        old_cur.execute("SELECT sutta_id FROM sc_kn")
        for (uid,) in old_cur.fetchall():
            if uid:
                all_suttas.add(uid.strip().lower())
                sutta_count += 1
    except Exception:
        pass

    for nikaya in ["dn", "mn", "sn", "an"]:
        table = f"tb_{nikaya}"
        try:
            old_cur.execute(f"SELECT sutta_id FROM {table}")
            for (uid,) in old_cur.fetchall():
                if uid:
                    all_suttas.add(uid.strip().lower())
                    sutta_count += 1
        except Exception:
            pass

    for nikaya in ["dn", "mn", "sn"]:
        table = f"dt_{nikaya}"
        try:
            old_cur.execute(f"SELECT sutta_id FROM {table}")
            for (uid,) in old_cur.fetchall():
                if uid:
                    all_suttas.add(uid.strip().lower())
                    sutta_count += 1
        except Exception:
            pass

    return sutta_count


def categorize_kn_suttas(all_suttas):
    kn_suttas = {sub: [] for sub, _, _, _ in KN_SUBS}
    main_nikaya_suttas = {
        "dn": sorted([s for s in all_suttas if s.startswith("dn")]),
        "mn": sorted([s for s in all_suttas if s.startswith("mn")]),
        "sn": sorted([s for s in all_suttas if s.startswith("sn")]),
        "an": sorted([s for s in all_suttas if s.startswith("an")]),
    }

    for sutta in all_suttas:
        if sutta.startswith("dhp"):
            kn_suttas["dhp"].append(sutta)
        elif sutta.startswith("iti"):
            kn_suttas["iti"].append(sutta)
        elif sutta.startswith("snp"):
            kn_suttas["snp"].append(sutta)
        elif sutta.startswith("thag"):
            kn_suttas["thag"].append(sutta)
        elif sutta.startswith("thig"):
            kn_suttas["thig"].append(sutta)
        elif sutta.startswith("ud"):
            kn_suttas["ud"].append(sutta)
        elif sutta.startswith("kp"):
            kn_suttas["kp"].append(sutta)
        elif sutta.startswith(("kn", "pli", "vin", "dn", "mn", "sn", "an")):
            continue
        else:
            kn_suttas["dhp"].append(sutta)

    return main_nikaya_suttas, kn_suttas


def insert_suttas(cur, suttas, nikaya_code, sub_collection=None):
    inserted = 0
    for sutta in suttas:
        try:
            cur.execute(
                """
                INSERT OR IGNORE INTO sutta_master (sutta_number, nikaya, sub_collection, last_updated)
                VALUES (?, ?, ?, ?)
                """,
                (sutta, nikaya_code, sub_collection, datetime.now()),
            )
            inserted += 1
        except Exception:
            pass
    return inserted


def import_source_data(old_cur, new_cur, src_old, src_new, nikaya):
    table_old = f"{src_old}_{nikaya}"
    table_new = f"{src_new}_{nikaya}"

    try:
        old_cur.execute(f"SELECT sutta_id, pali_text, translation_markdown FROM {table_old}")
    except Exception:
        return 0

    imported = 0
    for sutta_id, pali, trans in old_cur.fetchall():
        if not sutta_id:
            continue
        sutta_num = sutta_id.strip().lower()
        pali_len = len(pali) if pali else 0
        trans_len = len(trans) if trans else 0

        try:
            new_cur.execute(
                f"""
                INSERT OR REPLACE INTO {table_new}
                (sutta_number, pali_text, translation_text, char_count, is_complete)
                VALUES (?, ?, ?, ?, ?)
                """,
                (sutta_num, pali, trans, pali_len + trans_len, 1),
            )
            new_cur.execute(
                """
                UPDATE sutta_master SET
                    has_pali = CASE
                        WHEN has_pali IS NULL OR has_pali = '' THEN ?
                        WHEN instr(has_pali, ?) > 0 THEN has_pali
                        ELSE has_pali || ',' || ?
                    END,
                    has_english = CASE
                        WHEN ? = 0 THEN has_english
                        WHEN has_english IS NULL OR has_english = '' THEN ?
                        WHEN instr(has_english, ?) > 0 THEN has_english
                        ELSE has_english || ',' || ?
                    END
                WHERE sutta_number = ?
                """,
                (src_new, src_new, src_new, 1 if trans else 0, src_new, src_new, src_new, sutta_num),
            )
            new_cur.execute(
                """
                INSERT OR REPLACE INTO source_availability
                (sutta_number, source_id, has_pali, has_translation, is_complete)
                VALUES (?, ?, ?, ?, 1)
                """,
                (sutta_num, src_new, 1 if pali else 0, 1 if trans else 0),
            )
            imported += 1
        except Exception:
            pass

    return imported


def import_sc_kn(old_cur, new_cur):
    try:
        old_cur.execute("SELECT sutta_id, sub_collection, pali_text, translation_markdown FROM sc_kn")
    except Exception:
        return 0

    imported = 0
    for sutta_id, sub, pali, trans in old_cur.fetchall():
        if not sutta_id:
            continue
        sutta_num = sutta_id.strip().lower()
        try:
            new_cur.execute(
                """
                INSERT OR REPLACE INTO sc_kn
                (sutta_number, sutta_title, pali_text, translation_text, is_complete)
                VALUES (?, ?, ?, ?, ?)
                """,
                (sutta_num, sub, pali, trans, 1),
            )
            new_cur.execute(
                "UPDATE sutta_master SET sub_collection = ? WHERE sutta_number = ?",
                (sub, sutta_num),
            )
            imported += 1
        except Exception:
            pass
    return imported


def update_source_completeness(new_cur):
    for src in ["sc", "tbw", "dt"]:
        try:
            new_cur.execute(f"SELECT COUNT(*) FROM {src}_dn")
            dn_count = new_cur.fetchone()[0]
        except Exception:
            dn_count = 0

        new_cur.execute("SELECT expected_suttas FROM nikayas WHERE nikaya_code = ?", ("dn",))
        row = new_cur.fetchone()
        expected = row[0] if row and row[0] else 34
        pct = min(100, int(dn_count * 100 / expected))
        new_cur.execute("UPDATE sources SET completeness_pct = ? WHERE source_id = ?", (pct, src))


def main():
    ensure_data_directories()
    old_conn = sqlite3.connect(OLD_DB_PATH)
    new_conn = sqlite3.connect(UNIFIED_DB_PATH)
    old_cur = old_conn.cursor()
    new_cur = new_conn.cursor()

    prnt("=" * 70)
    prnt("BUILDING EBT UNIFIED DATABASE")
    prnt("=" * 70)
    prnt(f"Old DB: {OLD_DB_PATH}")
    prnt(f"Unified DB: {UNIFIED_DB_PATH}")
    prnt(f"Excel file: {EXCEL_FILE_PATH}")

    prnt("\n[1/6] Populating metadata...")
    create_base_schema(new_cur)
    new_cur.executemany(
        """
        INSERT OR REPLACE INTO sources
        (source_id, source_code, source_name, translator, language, translation_type, has_pali, has_translation, url, license, completeness_pct)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        SOURCES_DATA,
    )
    new_cur.executemany(
        """
        INSERT OR REPLACE INTO nikayas
        (nikaya_code, nikaya_name, division_type, expected_suttas, vagga_count, description)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        NIKAYAS_DATA,
    )
    new_cur.executemany(
        """
        INSERT OR REPLACE INTO kn_subcollections
        (sub_code, sub_name, expected_count, parent_nikaya)
        VALUES (?, ?, ?, ?)
        """,
        KN_SUBS,
    )
    new_conn.commit()
    prnt("  Metadata populated")

    prnt("\n[2/6] Importing sutta numbers from Excel...")
    all_suttas = load_excel_suttas(EXCEL_FILE_PATH)

    prnt("\n[3/6] Creating sutta master from existing data...")
    sutta_count = read_existing_sutta_ids(old_cur, all_suttas)
    prnt(f"  Collected {len(all_suttas)} unique sutta numbers")
    prnt(f"  Processed {sutta_count} records from old DB")

    main_nikaya_suttas, kn_suttas = categorize_kn_suttas(all_suttas)
    prnt("  Inserting sutta numbers...")
    for nikaya_code in ["dn", "mn", "sn", "an"]:
        insert_suttas(new_cur, main_nikaya_suttas[nikaya_code], nikaya_code)
    for sub, suttas in kn_suttas.items():
        if suttas:
            insert_suttas(new_cur, suttas, "kn", sub)
    new_conn.commit()

    new_cur.execute("SELECT COUNT(*) FROM sutta_master")
    total = new_cur.fetchone()[0]
    prnt(f"  sutta_master populated: {total} suttas")

    prnt("\n[4/6] Creating source tables...")
    tables_created = 0
    for src in SOURCES_LIST:
        for nik in NIKAYAS_LIST:
            create_source_table(new_conn, new_cur, src, nik)
            tables_created += 1
    prnt(f"  Created {tables_created} source tables")

    prnt("\n[5/6] Importing data from EBT_Suttas.db...")
    prnt("  Importing SuttaCentral (sc)...")
    for nikaya in ["dn", "mn", "sn", "an"]:
        count = import_source_data(old_cur, new_cur, "sc", "sc", nikaya)
        prnt(f"    sc_{nikaya}: {count} imported")

    sc_kn_count = import_sc_kn(old_cur, new_cur)
    prnt(f"    sc_kn: {sc_kn_count} imported")

    prnt("  Importing The Buddha's Words (tbw)...")
    for nikaya in ["dn", "mn", "sn", "an"]:
        count = import_source_data(old_cur, new_cur, "tb", "tbw", nikaya)
        prnt(f"    tbw_{nikaya}: {count} imported")

    prnt("  Importing Dhamma Talks (dt)...")
    for nikaya in ["dn", "mn", "sn"]:
        count = import_source_data(old_cur, new_cur, "dt", "dt", nikaya)
        prnt(f"    dt_{nikaya}: {count} imported")
    new_conn.commit()

    prnt("\n[6/6] Finalizing database...")
    update_source_completeness(new_cur)
    new_conn.commit()

    prnt("\n" + "=" * 70)
    prnt("DATABASE BUILD COMPLETE")
    prnt("=" * 70)

    new_cur.execute("SELECT COUNT(*) FROM sutta_master")
    total = new_cur.fetchone()[0]
    prnt(f"\nSutta Master: {total} suttas")

    prnt("\nNikaya counts:")
    for nikaya, name in [("dn", "Digha"), ("mn", "Majjhima"), ("sn", "Samyutta"), ("an", "Anguttara"), ("kn", "Khuddaka")]:
        new_cur.execute("SELECT COUNT(*) FROM sutta_master WHERE nikaya = ?", (nikaya,))
        count = new_cur.fetchone()[0]
        prnt(f"  {nikaya} ({name}): {count}")

    prnt("\nSource table counts:")
    for src in ["sc", "tbw", "dt"]:
        total_src = 0
        for nik in ["dn", "mn", "sn", "an"]:
            try:
                new_cur.execute(f"SELECT COUNT(*) FROM {src}_{nik}")
                total_src += new_cur.fetchone()[0]
            except Exception:
                pass
        prnt(f"  {src.upper()}: {total_src} suttas")

    prnt(f"\nDatabase: {UNIFIED_DB_PATH}")

    old_conn.close()
    new_conn.close()
    prnt("\nDone!")


if __name__ == "__main__":
    main()
