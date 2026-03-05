# ebt-translations

A database and toolset for collecting, comparing, and presenting multiple translations of Early Buddhist Texts (EBTs).

## Tech Stack

- **Language:** Python, SQLAlchemy
- **Database:** SQLite
- **Data formats:** Markdown, JSON

## Scope

Vinaya + Sutta Piṭaka EBTs: DN, MN, SN, AN, KN 1–9

## Development Stages

- [ ] 0. Planning
- [ ] 1. Data collection — build database, extract from all sources
- [ ] 2. Prompting / processing
- [ ] 3. Front end

## Sources

### 1. DPD Massive Table of Sutta Data

- [Google Spreadsheet](https://docs.google.com/spreadsheets/d/1sR8NT204STTwOoDrr9GBjhXVYEn0qqZTxgjoLKMmaaE/edit?usp=sharing)
- Mapping for each sutta number and name across all different data sources

### 2. CST — Chaṭṭha Saṅgāyana Tipiṭaka (6th Council)

- [Repo](https://github.com/vipassanatech/tipitaka-xml)
- [Roman script](https://github.com/VipassanaTech/tipitaka-xml/tree/main/romn)
- [Devanagari](https://github.com/VipassanaTech/tipitaka-xml/tree/main/deva)
- Format: XML, UTF-16
- Use BeautifulSoup to extract — see [this extraction example](https://github.com/digitalpalidictionary/dpd-db/blob/95830f8502c32e13d71963747ad4600e65e8de3c/scripts/build/cst4_xml_to_txt.py)
- Note: CST is organised per book, not per sutta — suttas must be extracted from within each book

### 3. SuttaCentral

- [Repo](https://github.com/suttacentral/sc-data/)
- [Pāḷi texts](https://github.com/suttacentral/sc-data/tree/main/sc_bilara_data/root/pli/ms)
- [English translations (Sujato)](https://github.com/suttacentral/sc-data/blob/main/sc_bilara_data/translation/en/sujato/sutta/)
- Keys in the Pāḷi text match keys in the English translation (bilara JSON format)

### 4. TBW — The Buddha's Words (Bhikkhu Bodhi)

- [Download ZIP](https://drive.google.com/drive/folders/1HawM4A_Ns37VGpHgH4YFpkkJpjtpNLEw) — offline website
- [Online version](https://find.dhamma.gift/bw/dn/dn1.html)
- Use Bhikkhu Bodhi translations (not Sujato)

### 5. Dhammatalks.org (Bhikkhu Thanissaro)

- [Website](https://www.dhammatalks.org/suttas/)
- No known repo — scrape required
- Partial list of suttas only

### 6. Pa Auk AI Translations

- [Website](https://tipitaka.paauksociety.org/)
- [Repo](https://github.com/digitalpalidictionary/tipitaka-translation-db) — find DB in Releases
- Column: `english_translation` in each table

### 7. ePitaka AI Translation

- [Website](https://epitaka.org/tpk/)
- Repo unknown — find the developer and repo

### 8. Indian Spoken English Translation

### 9... Hindi, Kannada, Telugu, Tamil, Marathi, and other Indian language translations
