# EBT Data Sources

## Overview

This document describes each data source used in the EBT project, including availability, coverage, and usage notes.

---

## Primary Sources

### 1. SuttaCentral (SC)

| Attribute | Value |
|-----------|-------|
| Full name | SuttaCentral |
| Translator | Bhikkhu Sujato |
| Language | English |
| Format | JSON (bilara) |
| URL | suttacentral.net |
| License | CC BY-NC-SA 4.0 |
| Coverage | 4,726 suttas (69%) |

**Description**: Primary Pali and English translation source. SuttaCentral provides the most complete coverage but is limited by what they have published.

**Data format**: Bilara JSON with `pli/ms` (Pali) and `en/sujato/sutta` (English)

**Notes**:
- Most reliable source
- Complete translations available
- Limited by external data availability

---

### 2. The Buddha's Words (TBW)

| Attribute | Value |
|-----------|-------|
| Full name | The Buddha's Words |
| Translator | Bhikkhu Bodhi |
| Language | English |
| Format | HTML (offline) |
| URL | budsas.org |
| License | Public Domain |
| Coverage | 4,227 suttas (62%) |

**Description**: Bhikkhu Bodhi's translation of the Nikayas. Derived from SuttaCentral mapping.

**Data availability**: Offline HTML in `data/bw2_20260118/`

**Notes**:
- Maps to SC sutta IDs
- Strong coverage but dependent on SC
- Good quality translations

---

### 3. Dhamma Talks (DT)

| Attribute | Value |
|-----------|-------|
| Full name | Dhamma Talks |
| Translator | Thanissaro Bhikkhu |
| Language | English |
| Format | HTML |
| URL | dhammatalks.org |
| License | CC BY-NC-ND 4.0 |
| Coverage | 1,002 suttas (15%) |

**Description**: Thanissaro Bhikkhu's translations from dhammatalks.org

**Notes**:
- Scraper exhausted - site blocks further access
- Only partial coverage available
- High quality translations but limited availability

---

### 4. Access to Insight (ATI)

| Attribute | Value |
|-----------|-------|
| Full name | Access to Insight |
| Translator | Various |
| Language | English |
| Format | HTML (offline) |
| URL | accesstoinsight.org |
| License | Various |
| Coverage | 966 suttas (14%) |

**Description**: Buddhist translations from accesstoinsight.org

**Notes**:
- Web scraping not viable
- Only offline files available
- Limited to source material available locally

---

## Secondary Sources

### 5. Pa Auk (PAU)

| Attribute | Value |
|-----------|-------|
| Full name | Pa Auk Translation |
| Translator | Pa Auk Society |
| Language | English |
| Format | Database |
| URL | tipitaka.paauksociety.org |
| License | Restricted |
| Coverage | 13,901 (ID mapping broken) |

**Description**: AI-assisted + human translations from Pa Auk Society

**Critical Issue**: Uses non-standard ID format:
- PAU: `s1`, `s10`, `s100` 
- Expected: `sn1.1`, `sn1.10`, `sn1.100`

**Notes**:
- Cannot automatically map to canonical IDs
- Would require AI/OLLAMA for mapping
- ~20k records unusable without mapping

---

### 6. Tipitaka Pali (TPK)

| Attribute | Value |
|-----------|-------|
| Full name | Tipitaka Pali |
| Translator | 6th Council |
| Language | Pali |
| Format | XML |
| URL | tipitaka.org |
| License | Public Domain |
| Coverage | 37 suttas |

**Description**: Pali text only, no English translations

**Notes**:
- Useful for Pali text
- Not a translation source
- Limited value for English dataset

---

## Data Availability Summary

| Source | Status | Coverage | Limitation |
|--------|--------|----------|-----------|
| SC | Active | 69% | Data availability |
| TBW | Linked | 62% | Maps to SC |
| DT | Exhausted | 15% | Site blocks scraping |
| ATI | Offline | 14% | No web access |
| PAU | Broken | N/A | ID mapping |
| TPK | Pali only | 0.5% | No English |

---

## What Can Be Used

### For Translation Dataset

1. **SC** - Use fully
2. **TBW** - Use if independent mapping available
3. **DT** - Use what's available
4. **ATI** - Extract from offline files

### Not Usable Without Significant Work

1. **PAU** - Needs AI mapping (time-intensive)
2. **TPK** - No English available

---

## Data Files

Expected local data files (gitignored):

```
data/
├── db/
│   ├── EBT_Suttas.db        # Legacy
│   └── EBT_Unified.db      # Current
├── input/
│   └── Massive Table of Sutta Data.xlsx
├── bw2_20260118/           # TBW offline
├── tipitaka-xml-main/      # CST XML
└── pau_db/             # PAU database
```

---

## Recommendations

1. **For maximum coverage**: Use SC + TBW + DT
2. **For multi-source**: Focus on DN, MN, SN, AN (KN is weak)
3. **For PAU**: Consider batch AI mapping as a separate project
4. **For TPK**: Use only for Pali text, not translations