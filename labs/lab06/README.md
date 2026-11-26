# Lab 6: Execute Sprint 1: clean, check, and enrich the IQVIA data end to end

In this lab you carry out the Sprint 1 plan you wrote in Lab 5, following Module 6's five-day cycle on a
laptop-sized scale. You run the provided Spark jobs on the course infrastructure (Kafka, Spark, and HDFS), with
compliance built in from the first step:

| Day | Sprint work | Job |
|---|---|---|
| 1 | **Cleaning and compliance:** validate and normalize the raw data; reject bad rows; confirm no patient identifiers | `day1_clean.py` |
| 2 | **Enrichment:** add derived fields; build monthly features for forecasting; load the curated zone | `day2_enrich.py` |
| 3 | **Integration:** run the same cleaning on records streaming in from Kafka | `day3_stream.py` |
| 4 | **Efficiency testing:** time the cleaning on 50,000 and 500,000 rows | `day4_efficiency.py` |
| 5 | **Integrity and edge-case testing:** reconcile every step; inject malformed records | `day5_integrity.py` |

Every job writes an audit entry to a **lineage log**, which you will ship to Elasticsearch with Filebeat in the
monitoring lab.

**Time:** about 2 to 2.5 hours, including about 15 minutes of Spark jobs.
**Before you start:** finish Lab 5 and have your Sprint 1 plan at hand. The IQVIA file from Canvas should be in your
`data\iqvia` folder.

> **Aggregate data.** Lab 6 uses the IQVIA file as it is: visit counts with no patient-level records and no patient
> identifiers. "Compliance from the first step" therefore means *proving* that no identifiers are present, keeping
> IQVIA's own privacy restrictions intact, and knowing which counts are too small to publish. The IQVIA file is
> licensed for this course only: never commit, upload, or share it.

---

## Part A. Watch the preparation video (about 30 minutes)

1. **Build Your First PySpark ETL Pipeline Step by Step** (Ankit Bansal)
   <https://www.youtube.com/watch?v=vA5-s7jjofI>

> This is a third-party video and may use different versions than this course. When they differ, follow these lab
> instructions.

---

## Part B. Get the latest files, start the infrastructure, and load the raw zone

**B1. Update your copy of the repository** the same way as in Lab 3, Part B (`git pull`, or download the ZIP
again; if you download it, copy the IQVIA file back into `data\iqvia`), then check the files:

```powershell
cd $HOME\Documents\csc894\healthriskai-labs      # macOS / Linux: cd ~/Documents/csc894/healthriskai-labs
ls labs\lab06 -Name                              # macOS / Linux: ls labs/lab06
```

You should see `day1_clean.py` to `day5_integrity.py`, `nmta.py` (the Sprint 1 rules every job shares), and
`README.md`.

> **Run every command in this lab from this folder** (the one with `compose.yaml`).

**B2. Turn on HDFS for this window, then start the infrastructure** (as in Lab 5):

```powershell
$env:COMPOSE_PROFILES = "hdfs"                   # macOS / Linux: export COMPOSE_PROFILES=hdfs
docker compose up -d
docker compose ps
```

If you open a new PowerShell window during this lab, run the first line there too before any `docker compose`
command.

Wait until **kafka**, **spark-master**, **namenode**, and **datanode** all say `(healthy)`.

**B3. Create the data-lake zones and load the raw file again.** `docker compose down` at the end of Lab 5 removed
the data in HDFS, so repeat Lab 5, Part C:

```powershell
docker compose exec namenode hdfs dfsadmin -safemode wait
docker compose exec namenode hdfs dfs -mkdir -p /datalake/raw/iqvia /datalake/staging /datalake/curated /datalake/dead-letter /datalake/profiles
docker compose exec namenode bash -c "hdfs dfs -put -f /opt/healthriskai/data/iqvia/*.csv /datalake/raw/iqvia/"
docker compose exec namenode hdfs dfs -ls /datalake/raw/iqvia
```

The last command lists the IQVIA file.

---

## Part C. Day 1: cleaning and compliance

**C1. Run the cleaning job:**

```powershell
docker compose exec spark-master bash bin/submit.sh labs/lab06/day1_clean.py
```

This takes about 3 to 5 minutes. It applies these rules to every raw row (they live in `labs\lab06\nmta.py`):

| Rule | What happens |
|---|---|
| A required field (month, payer, drug class, diagnosis, product) is blank | Rejected: `missing_required_field` |
| The month can't be read as `Mon YYYY` or `YYYY-MM-DD` | Rejected: `invalid_month` |
| A visit count isn't a whole number of 0 or more (blank means 0) | Rejected: `invalid_visit_count` |
| The row counts no visits at all | Rejected: `no_visits` |
| The state, region, or setting totals don't match the specialty total | Rejected: `inconsistent_totals` |
| The ICD-10 code doesn't match the pattern (and isn't privacy-restricted) | Rejected: `invalid_icd10_code` |
| The same row arrives twice | The copy is rejected: `duplicate_row` |
| IQVIA hid the diagnosis (`-1 PRIVACY RESTRICTED`) | **Kept**, labeled `RESTRICTED`: the visits are real |
| Payer, gender, or age is `UNKNOWN MEDICAL` or `UNSPECIFIED` | **Kept**, labeled `UNKNOWN` (a sparse value, flagged) |

Clean rows go to the **staging** zone with a real `service_month` date, numeric visit counts, the ICD-10 code split
from its description, and the visits by care setting. Rejected rows go to the **dead-letter** zone with the reason.
Then the job runs the compliance check. At the end you should see:

```text
===================== Lab 6 Day 1: cleaning =====================
Source: hdfs://namenode:8020/datalake/raw/iqvia
  Raw rows                                               632,690
  Clean rows -> staging zone                             632,690
    patient visits in clean rows                       2,892,144
  Rejected rows -> dead-letter zone                            0
  Privacy-restricted diagnosis (kept as RESTRICTED)           60
  Unknown payer, gender, or age (kept as UNKNOWN)         10,243
------------------------ Compliance check ------------------------
  Fields named like a patient identifier                 0  PASS
  Values that look like an SSN, email, or phone          0  PASS
  Rows with fewer than 11 visits (small counts)          583,747
-------------------------------------------------------------------
Check  raw rows = clean + rejected (632,690 = 632,690 + 0): PASS
===================================================================
```

**Take a screenshot of this summary** for your sprint review.

Zero rejected rows is a real finding, not a missing test: this IQVIA extract passes every rule. You'll prove on Day 5
that the rules do reject bad data. The small counts matter for compliance: results built from very small groups can
point to individual patients, so in a real project you would publish them only in aggregate.

---

## Part D. Day 2: enrichment

**D1. Run the enrichment job:**

```powershell
docker compose exec spark-master bash bin/submit.sh labs/lab06/day2_enrich.py
```

This takes about a minute. It writes two datasets to the **curated** zone:

- `curated/nmta_visits`: every clean row with derived fields: `service_year`, `service_quarter`, `payer_group`
  (COMMERCIAL, MEDICARE, MEDICAID, or UNKNOWN), `is_chronic`, `telehealth_share`, `hospital_share`, and
  `small_count`. It is **partitioned by service month**: one HDFS folder per month, so a query for one month reads
  only that folder.
- `curated/monthly_product_features`: one row for each of the 60 products in each of the 72 months, with the visits
  and the features a forecasting model needs: last month's visits (`visits_lag_1`), the same month last year
  (`visits_lag_12`), the average of the previous three months, the year-over-year change, and the chronic and
  Medicare shares. A month with no visits for a product gets 0, so "last month" always means last month.

You should see the last six months of ELIQUIS features, then:

```text
================ Lab 6 Day 2: enrichment ================
  Clean rows read from staging                632,690   (2,892,144 visits)
  Curated visit rows (nmta_visits)            632,690   in 72 service-month folders
  Monthly product features                      4,320   (60 products x 72 months)
---------------------------------------------------------
Check 1  curated visits = staging visits (2,892,144 = 2,892,144): PASS
Check 2  feature visits = staging visits (2,892,144 = 2,892,144): PASS
=========================================================
```

---

## Part E. Day 3: integration with Kafka streaming

Now the same cleaning runs on records that arrive through Kafka, as new data would in production.

**E1. Prepare 100 raw IQVIA rows from January 2026:**

```powershell
docker compose exec spark-master bash bin/submit.sh labs/lab06/day3_stream.py --prepare
```

You should see **Wrote 100 raw IQVIA rows from January 2026 to data/raw/nmta_stream100.jsonl**.

**E2. Create a topic and publish the 100 rows:**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic nmta_stream --partitions 1
docker compose exec kafka sh -c "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic nmta_stream < /opt/healthriskai/data/raw/nmta_stream100.jsonl"
```

**E3. Run the streaming job:**

```powershell
docker compose exec spark-master bash bin/submit.sh --kafka labs/lab06/day3_stream.py
```

Spark Structured Streaming reads the topic in micro-batches of up to 40 records, cleans each one with the Day 1
rules, and lands the results in the HDFS staging zone. Unlike Lab 3, this job stops by itself once it has read
everything in the topic. You should see three micro-batches (40, 40, and 20 records), then:

```text
=============== Lab 6 Day 3: streaming integration ===============
  Records read from Kafka topic nmta_stream          100
  Clean -> datalake/staging/nmta_stream              100   (394 visits)
  Rejected -> dead-letter zone                         0
--------------------------------------------------------------------
Check 1  every record in the sample arrived (100 of 100): PASS
Check 2  streaming output = batch output for the same records: PASS
====================================================================
```

Check 2 is the point of integration testing: the streaming path and the batch path give exactly the same result.

---

## Part F. Day 4: efficiency testing

**F1. Run the efficiency test.** While it runs, open <http://localhost:4040> to watch the jobs and stages in the
Spark web interface:

```powershell
docker compose exec spark-master bash bin/submit.sh labs/lab06/day4_efficiency.py
```

It first saves samples of 50,000 and 500,000 raw rows (not timed), then times the Day 1 cleaning on each. You should
see a table like this; **your times will differ**, because they depend on your computer:

```text
=============== Lab 6 Day 4: efficiency ===============
    Raw rows  Clean rows   Seconds  Rows per second
      50,000      50,000      30.9            1,618
     500,000     500,000      89.7            5,571
=======================================================
```

Record your results for your sprint review. Ten times the rows took only about three times as long: Spark has a
fixed cost to plan and start each job, and larger inputs keep both CPU cores busy. In the Spark web interface, look
at the **Stages** tab for the longest stage, and think about one change that would make it faster.

---

## Part G. Day 5: integrity and edge-case testing

**G1. Run the integrity tests:**

```powershell
docker compose exec spark-master bash bin/submit.sh labs/lab06/day5_integrity.py
```

Part 1 reconciles the counts at every step of the pipeline. Part 2 copies some real rows, injects ten malformed or
unusual records, runs the cleaning, and checks that each bad record is rejected for the right reason, the fixable
ones are kept, and the pipeline finishes without stopping. You should see:

```text
================== Lab 6 Day 5: integrity summary ==================
Part 1. Counts reconcile at every step
  Raw rows = staging + dead-letter                       632,690 = 632,690 + 0  PASS
  Raw visits = staging + dead-letter visits          2,892,144 = 2,892,144 + 0  PASS
  Curated rows = staging rows                                632,690 = 632,690  PASS
  Curated visits = staging visits                        2,892,144 = 2,892,144  PASS
  Feature visits = curated visits                        2,892,144 = 2,892,144  PASS
  Stream records = 100 (clean + dead-letter)                     100 = 100 + 0  PASS
  No patient identifiers in the curated zone                0 fields, 0 values  PASS
Part 2. Injected edge cases (the pipeline finished without stopping)
  Unreadable month (Jann 2026)                                -> invalid_month  PASS
  Missing product                                    -> missing_required_field  PASS
  Negative visit count (-2)                             -> invalid_visit_count  PASS
  Visit count that isn't a number (five)                -> invalid_visit_count  PASS
  Every visit count blank                                         -> no_visits  PASS
  State counts don't add up                             -> inconsistent_totals  PASS
  Malformed ICD-10 code (XYZ)                            -> invalid_icd10_code  PASS
  Exact duplicate of a real row                               -> duplicate_row  PASS
  Month written 2026-01-01 (fixable)                                   -> kept  PASS
  Privacy-restricted diagnosis (keep)                                  -> kept  PASS
  Real rows kept alongside the bad ones                                 5 of 5  PASS
--------------------------------------------------------------------
Result: ALL CHECKS PASS
=====================================================================
```

**Take a screenshot of this summary:** it's part of your submission. Every line must say PASS.

---

## Part H. Look at the lineage log and the data lake

**H1. The lineage log.** Every job appended one line to `data\lineage\lineage.jsonl` on your computer:

```powershell
Get-Content data\lineage\lineage.jsonl          # macOS / Linux: cat data/lineage/lineage.jsonl
```

Each line records the step, its inputs and outputs, the record counts, and how long it took. This audit trail is
what the monitoring lab ships to Elasticsearch.

**H2. The data lake.** On <http://localhost:9870>, choose **Utilities** > **Browse the file system** and open
`datalake`. Look at `staging/nmta_clean`, `curated/nmta_visits` (one `service_month=...` folder per month), and
`curated/monthly_product_features`.

---

## Part I. Stop the infrastructure

```powershell
docker compose down
```

The data in HDFS is removed with the containers; your lineage log stays in `data\lineage`.

---

## What to submit

Submit one **sprint review document** (Word or PDF) with:

1. **A screenshot of the Day 5 integrity summary** (Part G), showing every check passing.
2. **A screenshot of the Day 1 cleaning summary** (Part C), showing the compliance check.
3. **Your efficiency results** for 50,000 and 500,000 rows (seconds and rows per second, from Part F), with one
   change you would make to improve them.
4. **A short sprint review and retrospective** (one page): what was delivered against your Lab 5 acceptance
   criteria, what went well, and what you would move earlier in the next sprint.

---

## Command summary

| Command | What it does |
|---|---|
| `docker compose exec spark-master bash bin/submit.sh labs/lab06/day1_clean.py` | Day 1: clean, check compliance |
| `docker compose exec spark-master bash bin/submit.sh labs/lab06/day2_enrich.py` | Day 2: enrich, load curated |
| `docker compose exec spark-master bash bin/submit.sh labs/lab06/day3_stream.py --prepare` | Day 3: write 100 rows for Kafka |
| `docker compose exec spark-master bash bin/submit.sh --kafka labs/lab06/day3_stream.py` | Day 3: stream from Kafka to HDFS |
| `docker compose exec spark-master bash bin/submit.sh labs/lab06/day4_efficiency.py` | Day 4: efficiency test |
| `docker compose exec spark-master bash bin/submit.sh labs/lab06/day5_integrity.py` | Day 5: integrity and edge cases |
