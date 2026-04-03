import sqlite3

import requests
from bs4 import BeautifulSoup

from ebt_translations.paths import UNIFIED_DB_PATH, ensure_data_directories


def scrape_dt_suttas(nikaya):
    url = f"https://www.dhammatalks.org/suttas/{nikaya}/"

    try:
        resp = requests.get(url, timeout=30)
        soup = BeautifulSoup(resp.text, "html.parser")
        links = soup.find_all("a", href=True)
    except Exception:
        return set()

    files = set()
    for link in links:
        href = link.get("href", "")
        if f"/suttas/{nikaya}/" in href and href.endswith(".html"):
            files.add(href.split("/")[-1])
    return files


def convert_dt_filename(filename, nikaya):
    name = filename.replace(".html", "")
    parts = name.split("_")
    if len(parts) == 2:
        book = parts[0].replace(nikaya.upper(), "")
        sutta = parts[1]
        return f"{nikaya.lower()}{book}.{sutta}"
    return None


def main():
    ensure_data_directories()
    conn = sqlite3.connect(UNIFIED_DB_PATH)
    cur = conn.cursor()

    print("=" * 100)
    print("COMPLETING MISSING DATA - ALL METHODS")
    print("=" * 100)

    print("\n" + "=" * 100)
    print("METHOD 1: COMPLETE DHAMMATALKS SN/AN SCRAPING")
    print("=" * 100)

    for nik in ["sn", "an"]:
        dt_table = f"dt_{nik}"
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {dt_table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sutta_number TEXT UNIQUE,
                translation_text TEXT,
                source_url TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        files = sorted(scrape_dt_suttas(nik.upper()))
        print(f"\n{nik.upper()}: Found {len(files)} suttas")

        processed = 0
        for filename in files:
            sutta_number = convert_dt_filename(filename, nik.upper())
            if not sutta_number:
                continue

            url = f"https://www.dhammatalks.org/suttas/{nik.upper()}/{filename}"
            try:
                resp = requests.get(url, timeout=10)
                soup = BeautifulSoup(resp.text, "html.parser")
                content = soup.find("div", {"id": "content"})
                if content:
                    text = content.get_text(separator=" ", strip=True)
                    if len(text) > 100:
                        cur.execute(
                            f"""
                            INSERT OR IGNORE INTO {dt_table} (sutta_number, translation_text, source_url)
                            VALUES (?, ?, ?)
                            """,
                            (sutta_number, text, url),
                        )
            except Exception:
                pass

            processed += 1
            if processed % 20 == 0:
                conn.commit()
                print(f"  {nik.upper()}: Processed {processed}/{len(files)}...")

        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {dt_table} WHERE translation_text IS NOT NULL AND length(translation_text) > 100")
        count = cur.fetchone()[0]
        print(f"  {nik.upper()}: {count} translations now")

    print("\n" + "=" * 100)
    print("METHOD 2: TRY ACCESS TO INSIGHT FOR DN/MN")
    print("=" * 100)
    for url in [
        "https://www.accesstoinsight.org/tipitaka/kn/index.html",
        "https://www.accesstoinsight.org/tipitaka/dn/index.html",
        "https://www.accesstoinsight.org/tipitaka/mn/index.html",
    ]:
        print(f"\nChecking: {url}")
        try:
            resp = requests.get(url, timeout=30)
            print(f"  Status: {resp.status_code}")
        except Exception as exc:
            print(f"  Error: {str(exc)[:50]}")

    print("\n" + "=" * 100)
    print("METHOD 3: CHECK DIGITAL PALI READER")
    print("=" * 100)
    try:
        resp = requests.get("https://www.digitalpalireader.online/", timeout=10)
        print(f"DPR: {resp.status_code}")
    except Exception as exc:
        print(f"DPR: {str(exc)[:50]}")

    print("\n" + "=" * 100)
    print("METHOD 4: SUTTACENTRAL API")
    print("=" * 100)
    try:
        resp = requests.get("https://suttacentral.net/api/", timeout=10)
        print(f"SC API: {resp.status_code}")
    except Exception as exc:
        print(f"SC API: {str(exc)[:50]}")

    conn.close()
    print("\n" + "=" * 100)
    print("SCRAPING COMPLETE")
    print("=" * 100)


if __name__ == "__main__":
    main()
