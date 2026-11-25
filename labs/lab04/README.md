# Lab 4: Run your first ETL pipeline with Kafka and Spark

In this lab you run a version of Module 4's Sprint 0 pipeline on the Kafka and Spark infrastructure from Lab 3, using
patient visits built from the course's IQVIA data:

| Step | What happens | Where |
|---|---|---|
| **Extract** | Publish one month of patient visits to a Kafka topic and read it with Spark; then read all 2.9 million visits from files | Kafka, files |
| **Transform** | Check required fields, turn the month into a real date, remove duplicate visits | Spark |
| **Mask** | Hash `patient_id` and `patient_name` with SHA-256 plus a salt; redact the Social Security number and address | Spark |
| **Load** | Write clean records to Parquet, one folder per year; send rejected records, with the reason only, to a dead-letter zone | Your `data` folder |
| **Check** | Confirm input = loaded + dead-letter, that no plaintext patient identifiers remain, and that the visits still add up to the IQVIA file | Spark |

**Time:** about 75 minutes.
**Before you start:** finish Lab 3. Kafka and Spark should start without errors, and the IQVIA file from Canvas
should be in your `data\iqvia` folder.

> **The patient identities in this lab are synthetic.** The IQVIA data is aggregated and has no patient-level
> records, so a script in this repository turns its visit counts into individual visits and gives each one a made-up
> patient: an ID, a name, a Social Security number, and a street address. These identities do not represent any
> real person. They exist only to practice handling HIPAA-protected information (PHI). Later labs work with the
> IQVIA data at the aggregate level and do not use patient-level data.

---

## Part A. Watch the preparation videos (about 45 minutes)

1. **Create Spark DataFrame from CSV JSON Parquet | PySpark Tutorial for Beginners** (coder2j)
   <https://www.youtube.com/watch?v=dKr1o0tShNM>
2. **Build Your First PySpark ETL Pipeline Step by Step** (Ankit Bansal)
   <https://www.youtube.com/watch?v=vA5-s7jjofI>

> These are third-party videos and may use different versions than this course. When they differ, follow these
> lab instructions.

---

## Part B. Get the latest files and build the patient visits

**B1. Update your copy of the repository** the same way as in Lab 3, Part B (`git pull`, or download the ZIP
again), then move into the repository folder and check the files:

```powershell
cd $HOME\Documents\csc894\healthriskai-labs      # macOS / Linux: cd ~/Documents/csc894/healthriskai-labs
ls labs -Name                                    # macOS / Linux: ls labs
ls data\iqvia -Name                              # macOS / Linux: ls data/iqvia
```

You should see `lab01` to `lab04`, then `README.md` and the IQVIA `.csv` file. If you downloaded the ZIP again,
copy the IQVIA file back into `data\iqvia` first.

> **Run every command in this lab from this folder** (the one with `compose.yaml`).

**B2. Start Kafka and Spark:**

```powershell
docker compose up -d
docker compose ps
```

Wait until **kafka** and **spark-master** both show `(healthy)` (run `docker compose ps` again to refresh).

**B3. Build the patient visits with synthetic patient identities.** This takes a few minutes:

```powershell
docker compose exec spark-master python3 data/build_patient_visits.py
```

At the end you should see:

```text
Synthetic patient visits built from the IQVIA extract.
  IQVIA visits                 2,892,144
  Planted duplicates        +        500
  Records written              2,892,644  -> data/raw/visits/visits_YYYY.csv.gz
  Dec 2025 records for Kafka    27,939  -> data/raw/visits/visits_2025-12.jsonl
  Synthetic patients             857,336  (IDs, names, SSNs, and addresses are made up)
  Planted problems: 500 duplicates, 300 missing icd10_code, 200 missing patient_id,
                    50 unreadable months, 200 months in YYYY-MM-DD format
```

The script splits every IQVIA row into single visits, so the 632,690 rows become 2,892,144 visits. It then adds a
few data-quality problems on purpose, the same ones for every student, so the pipeline has something to catch. It
always creates exactly the same records.

Open `data\raw\visits` in File Explorer: there is one compressed CSV file per year, plus a JSON file with the
December 2025 visits for Kafka.

---

## Part C. Extract: publish one month of visits to Kafka

**C1. Create a topic for this lab with one partition**, so the visits stay in the order they were sent:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic patient_visits_lab4 --partitions 1
```

You should see **Created topic patient_visits_lab4.** (plus the same harmless topic-name `WARNING` as in Lab 3).

**C2. Publish the December 2025 visits:**

```powershell
docker compose exec kafka sh -c "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic patient_visits_lab4 < /opt/healthriskai/data/raw/visits/visits_2025-12.jsonl"
```

It takes a few seconds and finishes without printing anything.

**C3. Check how many messages the topic holds:**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic patient_visits_lab4
```

You should see **`patient_visits_lab4:0:27939`**: partition 0 holds 27,939 messages. Run C2 **only once**. If you
run it twice, the topic holds 55,878 messages (see Part F to find out what happens then).

---

## Part D. Transform, mask, and load with Spark

**D1. Run the pipeline on the month in Kafka:**

```powershell
docker compose exec spark-master bash bin/submit.sh --kafka labs/lab04/etl_claims.py
```

This takes about a minute. Spark prints some download and `WARN` lines first (for example, `Unable to load
native-hadoop library`); they're harmless. At the end you should see:

```text
================ Lab 4 ETL summary ================
Source: Kafka topic patient_visits_lab4
  Input records                   27,939
  Loaded to curated (Parquet)     27,933   -> data/curated/visits
  Sent to dead-letter zone             6   -> data/dlq/visits
      duplicate_visit_id               2
      missing_icd10_code               1
      missing_patient_id               2
      invalid_month                    1
---------------------------------------------------
Check 1  input = loaded + dead-letter (27,939 = 27,933 + 6): PASS
Check 2  no plaintext patient identifiers in curated output: PASS
===================================================
```

**D2. Scale up: run the same pipeline on all 2.9 million visits.** Real pipelines stream new records through Kafka
and load history in bulk from files. This run reads the yearly files from Part B:

```powershell
docker compose exec spark-master bash bin/submit.sh labs/lab04/etl_claims.py --source file
```

This takes about 2 to 5 minutes, depending on your computer. At the end you should see:

```text
================ Lab 4 ETL summary ================
Source: 7 files in data/raw/visits
  Input records                2,892,644
  Loaded to curated (Parquet)  2,891,594   -> data/curated/visits
  Sent to dead-letter zone         1,050   -> data/dlq/visits
      duplicate_visit_id             500
      missing_icd10_code             300
      missing_patient_id             200
      invalid_month                   50
---------------------------------------------------
Check 1  input = loaded + dead-letter (2,892,644 = 2,891,594 + 1,050): PASS
Check 2  no plaintext patient identifiers in curated output: PASS
Check 3  visits match the IQVIA file (2,891,594 + 550 = 2,892,144): PASS
===================================================
```

**Take a screenshot of this summary:** it's your submission. Every number must match.

What happened to the planted problems:

- **Duplicates:** 500 visits arrived twice. The pipeline kept the first copy and sent the repeat to the dead-letter
  zone, so no visit is counted twice.
- **Missing fields:** 500 visits had no `icd10_code` or no `patient_id`. They can't be used, so they were set aside.
- **Bad dates:** 50 visits had an unreadable month (`UNKNOWN`) and were rejected. Another 200 used `YYYY-MM-DD`
  instead of the IQVIA format (`Dec 2025`). Those were *fixed*, not rejected: every loaded visit now has a real
  `service_month` date.
- **Reconciliation:** Check 1 confirms no record was lost (2,891,594 + 1,050 = 2,892,644). Check 3 goes back to the
  original IQVIA file: every one of its 2,892,144 visits was either loaded or rejected for a field problem (550).

---

## Part E. Look at the results

**E1. Inspect the output with Spark:**

```powershell
docker compose exec spark-master bash bin/submit.sh labs/lab04/inspect_output.py
```

You should see:

1. **Five curated visits.** `patient_id` and `patient_name` are now 64-character hashes, and `ssn` and `address`
   are `REDACTED`. The same patient always gets the same hash, so a patient's visits can still be linked without
   revealing who they are.
2. **The curated schema.** `service_month` is now a `date`, and `service_year` is a number; in Lab 3 every column
   was text.
3. **Curated visits by year,** from 2020 to 2026. Spark saved each year in its own folder, so a later query for one
   year only reads that folder.
4. **Five dead-letter records** with only `visit_id`, `reason`, and `source` (the file the record came from), and
   the count by reason: 500, 300, 200, and 50.

**E2. Find the files on your computer.** Open `data\curated\visits` in File Explorer. You should see one folder per
year (`service_year=2020` to `service_year=2026`), each with a `.snappy.parquet` file (Parquet is a compressed,
column-based format, which is why you can't read it in Notepad), and an empty `_SUCCESS` file that Spark writes when
a job finishes. Small `.crc` files next to them are Spark's checksums.

Then open the dead-letter file in Notepad:

```powershell
notepad (Get-ChildItem data\dlq\visits\*.json).FullName     # macOS: open -e data/dlq/visits/*.json
```

It has 1,050 lines. Each line looks like `{"visit_id":"V0059057","reason":"duplicate_visit_id","source":"visits_2020.csv.gz"}`:
enough to find and fix the record, but no patient details.

---

## Part F (optional). What if the same visits arrive twice?

Real feeds sometimes resend data. Try it with the Kafka month:

1. Publish the visits again (repeat Part C, step C2).
2. Run the Kafka pipeline again (Part D, step D1).

You should now see **Input records 55,878**, **Loaded 27,933**, and **dead-letter 27,945**: 27,937 duplicates,
2 missing `icd10_code`, 4 missing `patient_id`, and 2 invalid months. The field checks run before the duplicate
check, so the resent copies of bad records are rejected for the same reason again, and every other resent visit is
caught as a duplicate. Both checks still pass, and the curated data didn't change.

To start over with a clean topic, delete it and repeat Part C:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic patient_visits_lab4
```

Then run D2 again so your `data\curated` folder holds all the visits.

---

## Part G. Stop the infrastructure

```powershell
docker compose down
```

Your `data\curated` and `data\dlq` folders stay on your computer.

---

## What to submit

1. **One screenshot** of the ETL summary from **Part D2** (the run on all visits), showing the input, loaded, and
   dead-letter counts and all three checks passing.
2. **A discussion post** in the Lab 4 discussion: post your screenshot and describe one issue you ran into while
   completing Lab 4 and how you resolved it. If everything worked the first time, describe one data-quality problem
   the pipeline caught and why it matters for a healthcare dataset.

---

## How the masking works

The pipeline uses the same approach as Module 4's example: `sha2(concat(patient_id, salt), 256)`. The salt is a
secret value added before hashing, so someone with a list of patient IDs can't simply hash them and match the
results. In this lab the salt has a default value so everyone gets the same output. In a real system it would be
kept secret, outside the code.

Hashing keeps a pseudonymous key, so analysts can still link one patient's visits. The Social Security number and
the street address are not needed for analysis at all, so the pipeline removes them (`REDACTED`). The state stays,
because state-level geography is allowed in de-identified data.

Next: Lab 5. From Lab 5 on, the labs use the IQVIA data at the aggregate level, without patient identifiers.
