# Course data

The labs use the **IQVIA National Medical and Treatment Audit (NMTA)** extract that your instructor shares in Canvas.
It is licensed data: it stays on your own computer, in `data/iqvia/`, and is never stored in this repository.

The IQVIA file is **aggregated**. Each of its 632,690 rows counts the patient visits for one combination of month
(February 2020 to January 2026), payer, age range, gender, drug, and diagnosis, broken out by specialty, region,
state, a YES/NO category, and care setting. It contains no patient-level records and no patient identifiers.

| Folder | Created by | Contents | Used in |
|---|---|---|---|
| `iqvia/` | you, from Canvas | the IQVIA CSV file | Labs 3 and up |
| `raw/iqvia_sample20.jsonl` | `prepare_iqvia.py` | 20 IQVIA rows from January 2026, for the streaming demo | Lab 3 |
| `raw/visits/` | `build_patient_visits.py` | patient visits with **synthetic** patient identities | Lab 4 |
| `curated/` | Lab 4 ETL | clean, masked visits in Parquet format, one folder per year | Lab 4 |
| `dlq/` | Lab 4 ETL | the dead-letter zone: rejected visit IDs and reasons, no patient details | Lab 4 |

Everything except `iqvia/README.md` is created on your computer and is not stored in the repository.

## Synthetic patient identities (Lab 4 only)

Lab 4 practices protecting patient identifiers (PHI), which the aggregated IQVIA file doesn't have. So
`build_patient_visits.py` splits every IQVIA row into single visits (2,892,144 in all) and gives each visit a
**synthetic** patient: an ID, a name, a Social Security number, and a street address. These identities are made up by
the script and do not describe any real person. Social Security numbers use area 666, which is never assigned.

The script also plants a fixed set of data-quality problems for Lab 4: 500 duplicate visits, 300 missing
`icd10_code`, 200 missing `patient_id`, 50 unreadable months, and 200 months written as `YYYY-MM-DD` instead of
`Mon YYYY`. Every run creates exactly the same records.

From Lab 5 on, the labs work with the IQVIA data at the aggregate level, without any patient identifiers.
