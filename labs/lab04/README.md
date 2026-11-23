# Lab 4: Run your first ETL pipeline with Kafka and Spark

In this lab you run a small version of Module 4's Sprint 0 pipeline on the Kafka and Spark infrastructure from
Lab 3:

| Step | What happens | Where |
|---|---|---|
| **Extract** | Publish 1,025 synthetic claims to a Kafka topic, then read the whole topic with Spark | Kafka |
| **Transform** | Check required fields, turn the month into a real date, remove duplicate claims | Spark |
| **Mask** | Hash `patient_id` and `patient_name` with SHA-256 plus a salt; redact the address | Spark |
| **Load** | Write clean records to Parquet files; send rejected records, with the reason only, to a dead-letter zone | Your `data` folder |
| **Check** | Confirm input = loaded + dead-letter, and that no plaintext patient identifiers remain | Spark |

You'll load these validated records into MongoDB in a later lab.

**Time:** about 60 minutes.
**Before you start:** finish Lab 3. Kafka and Spark should start without errors.

> **Synthetic data only.** The claims are made-up records shaped like the IQVIA NMTA dataset. The labs never use
> real IQVIA or patient data.

---

## Part A. Watch the preparation videos (about 45 minutes)

1. **Create Spark DataFrame from CSV JSON Parquet | PySpark Tutorial for Beginners** (coder2j)
   <https://www.youtube.com/watch?v=dKr1o0tShNM>
2. **Build Your First PySpark ETL Pipeline Step by Step** (Ankit Bansal)
   <https://www.youtube.com/watch?v=vA5-s7jjofI>

> These are third-party videos and may use different versions than this course. When they differ, follow these
> lab instructions.

---

## Part B. Get the latest files and start the infrastructure

**B1. Update your copy of the repository** the same way as in Lab 3, Part B (`git pull`, or download the ZIP
again), then move into the repository folder:

```powershell
cd $HOME\Documents\csc894\healthriskai-labs      # macOS / Linux: cd ~/Documents/csc894/healthriskai-labs
ls labs -Name                                    # macOS / Linux: ls labs
```

You should see `lab01`, `lab02`, `lab03`, and `lab04`.

> **Run every command in this lab from this folder** (the one with `compose.yaml`).

**B2. Start Kafka and Spark:**

```powershell
docker compose up -d
docker compose ps
```

Wait until **kafka** and **spark-master** both show `(healthy)` (run `docker compose ps` again to refresh).

**B3. Create the synthetic claims dataset** (safe to run again; it always creates the same data):

```powershell
docker compose exec spark-master python3 data/generate_claims.py
```

You should see **1,025 records**.

---

## Part C. Extract: publish the claims to Kafka

**C1. Create a topic for this lab with one partition**, so the claims stay in the order they were sent:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic iqvia_claims_lab4 --partitions 1
```

You should see **Created topic iqvia_claims_lab4.** (plus the same harmless topic-name `WARNING` as in Lab 3).

**C2. Publish all 1,025 claims** from the data file:

```powershell
docker compose exec kafka sh -c "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic iqvia_claims_lab4 < /opt/healthriskai/data/raw/claims.jsonl"
```

It finishes without printing anything.

**C3. Check how many messages the topic holds:**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic iqvia_claims_lab4
```

You should see **`iqvia_claims_lab4:0:1025`**: partition 0 holds 1,025 messages. Run C2 **only once**. If you run
it twice, the topic holds 2,050 messages (see Part F to find out what happens then).

---

## Part D. Transform, mask, and load with Spark

**D1. Run the ETL pipeline:**

```powershell
docker compose exec spark-master bash bin/submit.sh --kafka labs/lab04/etl_claims.py
```

This takes about a minute. Spark prints some download and `WARN` lines first (for example, `Unable to load
native-hadoop library`); they're harmless. At the end you should see this summary:

```text
================ Lab 4 ETL summary ================
Source: Kafka topic iqvia_claims_lab4
  Input records                 1,025
  Loaded to curated (Parquet)     965   -> data/curated/claims
  Sent to dead-letter zone         60   -> data/dlq/claims
      duplicate_claim_id           25
      missing_icd10_code           20
      missing_patient_id           10
      invalid_month                 5
---------------------------------------------------
Check 1  input = loaded + dead-letter (1,025 = 965 + 60): PASS
Check 2  no plaintext patient identifiers in curated output: PASS
===================================================
```

**Take a screenshot of this summary** for your submission.

What happened to the planted problems:

- **Duplicates:** 25 claims arrived twice. The pipeline kept the first copy and sent the repeat to the dead-letter
  zone, so no medical event is counted twice.
- **Missing fields:** 30 claims had no `icd10_code` or no `patient_id`. They can't be used for risk modeling, so
  they were set aside.
- **Bad dates:** 5 claims had an impossible month (`13/45/2020`) and were rejected. Another 15 used `YYYY-MM-DD`
  instead of `MM/DD/YYYY`. Those were *fixed*, not rejected: every loaded claim now has a real `service_month`
  date.
- **Reconciliation:** 965 + 60 = 1,025, so no record was lost.

---

## Part E. Look at the results

**E1. Inspect the output with Spark:**

```powershell
docker compose exec spark-master bash bin/submit.sh labs/lab04/inspect_output.py
```

You should see:

1. **Five curated claims.** `patient_id` and `patient_name` are now 64-character hashes, and `address` is
   `REDACTED`. The same patient always gets the same hash, so the patient's claims can still be linked without
   revealing who they are. **Take a screenshot of this table** for your submission.
2. **The curated schema.** `service_month` is now a `date` and `patient_visits` an `integer`; in Lab 3 every column
   was text.
3. **Five dead-letter records** with only `claim_id`, `reason`, and `source_offset` (the record's position in
   Kafka), and the count by reason: 25, 20, 10, and 5.

**E2. Find the files on your computer.** Open `data\curated\claims` in File Explorer. You should see one
`.snappy.parquet` file (Parquet is a compressed, column-based format, which is why you can't read it in Notepad)
and an empty `_SUCCESS` file that Spark writes when a job finishes. Small `.crc` files next to them are Spark's
checksums.

Then open the dead-letter file in Notepad:

```powershell
notepad (Get-ChildItem data\dlq\claims\*.json).FullName     # macOS: open -e data/dlq/claims/*.json
```

It has 60 lines, the duplicates first. Each line looks like
`{"claim_id":"C000062","reason":"missing_icd10_code","source_offset":61}`: enough to find and fix the record, but
no patient details.

---

## Part F (optional). What if the same claims arrive twice?

Real feeds sometimes resend data. Try it:

1. Publish the claims again (repeat Part C, step C2).
2. Run the pipeline again (Part D, step D1).

You should now see **Input records 2,050**, **Loaded 965**, and **dead-letter 1,085**: 1,015 duplicates, 40
missing `icd10_code`, 20 missing `patient_id`, and 10 invalid months. The field checks run before the duplicate
check, so the resent copies of bad records are rejected for the same reason again, and the other 1,015 resent claims
are caught as duplicates. Both checks still pass. The duplicate check caught every resent claim, so the curated data didn't
change.

To start over with a clean topic, delete it and repeat Part C:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic iqvia_claims_lab4
```

---

## Part G. Stop the infrastructure

```powershell
docker compose down
```

Your `data\curated` and `data\dlq` folders stay on your computer.

---

## What to submit

1. A screenshot of the ETL summary from **Part D1**, showing the input, loaded, and dead-letter counts and that
   they reconcile.
2. A screenshot of the curated claims table from **Part E1**, showing hashed patient IDs and no plaintext
   identifiers.
3. A short paragraph (3–5 sentences) describing one data-quality problem the pipeline caught and how the pipeline
   handled it.

---

## How the masking works

The pipeline uses the same approach as Module 4's example: `sha2(concat(patient_id, salt), 256)`. The salt is a
secret value added before hashing, so someone with a list of patient IDs can't simply hash them and match the
results. In this lab the salt has a default value so everyone gets the same output. In a real system it would be
kept secret, outside the code.

Next: Lab 5.
