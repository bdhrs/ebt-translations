
# data format
- markdown
- json

# language
- python
- sqlalchemy

# database
- sqlite

# useful abbreviations
- dn Dīgha Nikāya
- mn Majjhima Nikāya
- sn
- an
- kn

# range
- vinaya
- sutta EBTs, dn, mn, sn, an, kn1-9


# sources
1. DPD Massive Table of Sutta Data 
https://docs.google.com/spreadsheets/d/1sR8NT204STTwOoDrr9GBjhXVYEn0qqZTxgjoLKMmaaE/edit?usp=sharing. 
Mapping for each sutta number and name in each different data source   

2. CST Chaṭṭha Saṅgāyana Tipiṭaka (6th council)
- Repo https://github.com/vipassanatech/tipitaka-xml
- Roman script https://github.com/VipassanaTech/tipitaka-xml/tree/main/romn
- Deva https://github.com/VipassanaTech/tipitaka-xml/tree/main/deva
- XML UTF16
- Use Beautiful Soup to extract. 
- See https://github.com/digitalpalidictionary/dpd-db/blob/95830f8502c32e13d71963747ad4600e65e8de3c/scripts/build/cst4_xml_to_txt.py for a typical extraction method
- CST is not per sutta, it's per book, so suttas need to get extracted out of a book

3. Sutta Central
- Repo is https://github.com/suttacentral/sc-data/
- Pāḷi texts https://github.com/suttacentral/sc-data/tree/main/sc_bilara_data/root/pli/ms
- English Translations https://github.com/suttacentral/sc-data/blob/main/sc_bilara_data/translation/en/sujato/sutta/
- key in the pāḷi text matches the key in the English translation

4. TBW The Buddha's words
- ZIP https://drive.google.com/drive/folders/1HawM4A_Ns37VGpHgH4YFpkkJpjtpNLEw
- offline website
- Online version https://find.dhamma.gift/bw/dn/dn1.html
- Use Bhikkhu Bodhi not Sujāto

5. Dhammatalk.org 
- Website https://www.dhammatalks.org/suttas/
- No known repo, scrape it!
- Partial list of suttas

6. Pa Auk AI Translations
- Website https://tipitaka.paauksociety.org/
- Repo with sqlite sb https://github.com/digitalpalidictionary/tipitaka-translation-db look in releases!
- Column is `english_translation` in each table

7. ePitaka AI translation
- Website https://epitaka.org/tpk/
- Repo ??
- find the coder and the repo

8. Indian Spoken ENglish Translation
9. 10. Hindi, Kannada, Telugu, Tamil, Marathi Translations, etc. etc. 

# Stages
0. Planning
1. Data collection
    - Building the database
    - extracting from all the sources
2. Prompting
3. Front End    

# github collaborators
ksk_235
bdhrs

