# Course data

All data in this course is **synthetic**: made up, shaped like the IQVIA NMTA dataset described in Module 2. No row
describes a real person, provider, or claim. Never copy real patient or IQVIA data into this folder.

Create the data from the repository folder (Lab 3 shows when):

```bash
docker compose exec spark-master python3 data/generate_claims.py
```

It always creates the same 1,025 records, including planted data-quality problems for Lab 4:
25 duplicate claims, 20 missing `icd10_code`, 10 missing `patient_id`, 5 impossible months, and 15 months written
as `YYYY-MM-DD` instead of `MM/DD/YYYY`.

| Folder | Created by | Contents |
|---|---|---|
| `raw/` | `generate_claims.py` | `claims.csv`, `claims.jsonl`, `claims_sample20.jsonl` |
| `curated/` | Lab 4 ETL | clean, masked claims in Parquet format |
| `dlq/` | Lab 4 ETL | the dead-letter zone: rejected claim IDs and reasons, no patient details |

These folders are created on your computer and are not stored in the repository.
