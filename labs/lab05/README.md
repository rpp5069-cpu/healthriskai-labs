# Lab 5: Set up the HDFS data lake and run the Initial Discovery Phase

In this lab you put Module 5's first adaptation, the **Initial Discovery Phase**, into practice. You add Apache
Hadoop HDFS to the course infrastructure, create the data-lake zones, load the IQVIA file into the raw landing zone,
and run a Spark profiling job that surfaces the data problems your Sprint 1 plan must handle. Then you use those
findings to write your own Sprint 1 plan.

**Time:** about 90 minutes, including a one-time download of about 1 GB.
**Before you start:** finish Lab 4. Kafka and Spark should start without errors, and the IQVIA file from Canvas
should be in your `data\iqvia` folder. From this lab on, Docker runs five services; 16 GB of RAM is recommended.

> **Aggregate data from here on.** From Lab 5 on, the labs use the IQVIA file as it is: visit counts by month, payer,
> age range, gender, drug, and diagnosis, with no patient-level records and no patient identifiers. The synthetic
> patient identities from Lab 4 are not used again. The IQVIA file is licensed for this course only: keep it on your
> own computer, and never commit, upload, or share it.

---

## Part A. Watch the preparation video (about 30 minutes)

1. **Hadoop Distributed File System | HDFS commands - Hands on** (Blismos Academy)
   <https://www.youtube.com/watch?v=N_pVioKoG6k>

> This is a third-party video and may use different versions than this course. When they differ, follow these lab
> instructions.

---

## Part B. Get the latest files and start the infrastructure

**B1. Update your copy of the repository** the same way as in Lab 3, Part B (`git pull`, or download the ZIP
again), then move into the repository folder and check the files:

```powershell
cd $HOME\Documents\csc894\healthriskai-labs      # macOS / Linux: cd ~/Documents/csc894/healthriskai-labs
ls labs\lab05 -Name                              # macOS / Linux: ls labs/lab05
ls data\iqvia -Name                              # macOS / Linux: ls data/iqvia
```

You should see `profile_raw.py`, `README.md`, and `sprint1_plan_template.md`, then the folder's `README.md` and
the IQVIA `.csv` file. If you downloaded the ZIP again, copy the IQVIA file back into `data\iqvia` first.

> **Run every command in this lab from this folder** (the one with `compose.yaml`).

**B2. Turn on HDFS for this window, then start the infrastructure.** HDFS is part of the course setup from this lab
on, in a Compose *profile* named `hdfs`:

```powershell
$env:COMPOSE_PROFILES = "hdfs"                   # macOS / Linux: export COMPOSE_PROFILES=hdfs
docker compose up -d
```

The first line lasts until you close this PowerShell window. **If you open a new window, run it again before any
`docker compose` command**, or Docker won't see the HDFS services.

The first time, Docker downloads the Hadoop image (about 1 GB), which takes a few minutes. You should then see five
containers: `healthriskai-kafka-1`, `healthriskai-spark-master-1`, `healthriskai-spark-worker-1`,
`healthriskai-namenode-1`, and `healthriskai-datanode-1`.

**B3. Check that everything is running:**

```powershell
docker compose ps
```

Wait until **kafka**, **spark-master**, **namenode**, and **datanode** all say `(healthy)` (up to a minute; run
the command again to refresh). **spark-worker** just says `Up`.

HDFS has two parts. The **NameNode** keeps the directory tree: which files exist and where their blocks are. The
**DataNode** stores the blocks themselves. A real data lake has many DataNodes and keeps three copies of every
block; ours has one DataNode and one copy, which works the same way on a smaller scale.

**B4. Look at HDFS.** Open <http://localhost:9870>. You should see the **Overview** page of the NameNode. Under
**Summary**, **Live Nodes** should be **1**.

---

## Part C. Create the data-lake zones and load the raw data

A data lake is organized into **zones**, one for each stage of the pipeline:

| Zone | What it holds | Used in |
|---|---|---|
| `raw` | files exactly as they arrived, never changed | Labs 5 and 6 |
| `staging` | cleaned data, not yet enriched | Lab 6 |
| `curated` | cleaned and enriched data, ready for analysis and modeling | Lab 6 and later |
| `dead-letter` | records rejected by the pipeline, with the reason | Lab 6 |
| `profiles` | data profiles from the discovery phase | Lab 5 |

**C1. Wait until HDFS is ready for writes:**

```powershell
docker compose exec namenode hdfs dfsadmin -safemode wait
```

You should see **Safe mode is OFF**. (HDFS starts in a read-only "safe mode" until its DataNodes have reported in.)

**C2. Create the zones:**

```powershell
docker compose exec namenode hdfs dfs -mkdir -p /datalake/raw/iqvia /datalake/staging /datalake/curated /datalake/dead-letter /datalake/profiles
```

It finishes without printing anything.

**C3. Upload the IQVIA file to the raw zone.** This takes about 10 to 30 seconds:

```powershell
docker compose exec namenode bash -c "hdfs dfs -put -f /opt/healthriskai/data/iqvia/*.csv /datalake/raw/iqvia/"
```

**C4. Check the result:**

```powershell
docker compose exec namenode hdfs dfs -ls -R /datalake
docker compose exec namenode hdfs dfs -du -s -h /datalake/raw/iqvia
```

The first command lists the five zones and the IQVIA file in `/datalake/raw/iqvia`. The second shows its size:
about **244.7 M**.

**C5. Browse the data lake.** On <http://localhost:9870>, choose **Utilities** > **Browse the file system**, then
open `datalake`. You should see the five zones. Open `raw` > `iqvia` to see the file, its size, and its block size
(128 MB). HDFS splits large files into blocks, so the IQVIA file is stored as two blocks.

---

## Part D. Run the discovery profiling job

**D1. Profile the raw data with Spark:**

```powershell
docker compose exec spark-master bash bin/submit.sh labs/lab05/profile_raw.py
```

This takes about 1 to 2 minutes. Spark reads the file straight from HDFS (`hdfs://namenode:8020`). You should see
seven sections:

1. **Schema:** 136 columns, all read as text: 13 describe each row and 123 count patient visits, in five
   breakdowns (60 specialties, 5 regions, 52 states, a YES/NO category, and 4 care settings).
2. **Size:** 632,690 rows and 2,892,144 patient visits.
3. **Blank (null) rates:** no describing value is blank, but 93.1% of the visit cells are blank. A blank cell
   means 0 visits, which your cleaning job must turn into a number.
4. **Value distributions:** visits by payer, gender, age range, acute or chronic, brand or generic, pharma type,
   and drug class. Almost all of this extract is antithrombotics (blood thinners).
5. **Date formats:** one format in use, `Mon YYYY` (for example `Jan 2026`), over 72 months.
6. **ICD-10 codes outside the expected pattern:** 60 rows, all labeled `PRIVACY RESTRICTED`. IQVIA hid the
   diagnosis in these rows to protect privacy. Codes such as `C7A` look unusual but are valid ICD-10-CM codes.
7. **Duplicates and consistency:** no duplicate rows, the breakdowns all agree, 10,243 rows have an unknown or
   unspecified payer, gender, or age, and 583,747 rows count fewer than 11 visits.

It ends with this summary:

```text
=================== Lab 5 discovery summary ===================
Source: hdfs://namenode:8020/datalake/raw/iqvia
  Rows                                                 632,690
  Columns (13 describing, 123 visit counts)                136
  Patient visits                                     2,892,144
  Months (Feb 2020 to Jan 2026)                             72
  Date formats in use                                        1
  Blank visit cells                                      93.1%
  Duplicate rows                                             0
  Rows whose breakdown totals disagree                       0
  Rows with an ICD-10 code outside the pattern              60
  Rows with unknown payer, gender, or age               10,243
  Rows with fewer than 11 visits                       583,747
Profile saved to hdfs://namenode:8020/datalake/profiles/iqvia_nmta
================================================================
```

**Take a screenshot of this summary:** it's part of your submission. Every number must match.

**D2. Look at the saved profile in HDFS:**

```powershell
docker compose exec namenode bash -c "hdfs dfs -cat /datalake/profiles/iqvia_nmta/part-*.json | head -5"
```

Each line is one measurement, for example `{"section":"size","item":"rows","value":"632690"}`. The profile also
holds the blank rate of every one of the 136 columns and every value distribution, so you can come back to it while
you plan.

---

## Part E. Write your Sprint 1 plan

The discovery phase exists so the sprint plan rests on facts, not guesses. Copy
`labs\lab05\sprint1_plan_template.md` (open it in any text editor or in Word) and fill it in:

1. **Sprint goal:** one sentence about what Sprint 1 delivers.
2. **Scope and assumptions:** what is in and out of the sprint, and what you assume about the data.
3. **Three acceptance criteria:** outcome-based and measurable, each tied to a number from your profile. For
   example: *"All 2,892,144 visits in the raw file are accounted for after cleaning (curated plus dead-letter)."*
4. **The six tasks** from Module 5 (data cleaning, compliance checks, schema mapping for MongoDB, integration
   testing, efficiency profiling, and documentation), each with story points.
5. **A 40% buffer** for data debt: if your team can complete 30 story points in a sprint, commit to 18 and keep 12
   for problems the data reveals.

Think about which findings need work in Sprint 1: the blank visit cells, the 60 privacy-restricted codes, the
10,243 rows with unknown values, the YES/NO columns whose meaning isn't documented, and the 583,747 small counts.
In Lab 6 you carry out this plan.

---

## Part F. Stop the infrastructure

```powershell
docker compose down
```

Removing the containers also removes the data in HDFS (and the Kafka topics). That's expected: Lab 6 starts by
loading the raw file again, which takes less than a minute.

---

## What to submit

1. **One screenshot** of the Lab 5 discovery summary from **Part D1**, with all eleven numbers visible.
2. **Your Sprint 1 plan** (one to two pages), with each acceptance criterion tied to a number from your profile.

---

## Command summary

| Command | What it does |
|---|---|
| `docker compose exec namenode hdfs dfsadmin -safemode wait` | Wait until HDFS accepts writes |
| `docker compose exec namenode hdfs dfs -mkdir -p PATH` | Create a folder (and its parents) in HDFS |
| `docker compose exec namenode hdfs dfs -put -f LOCAL HDFS_PATH` | Upload a file to HDFS, replacing it if present |
| `docker compose exec namenode hdfs dfs -ls -R PATH` | List a folder in HDFS, including subfolders |
| `docker compose exec namenode hdfs dfs -du -s -h PATH` | Show the size of a folder in HDFS |
| `docker compose exec namenode hdfs dfs -cat FILE` | Print a file stored in HDFS |
| `docker compose exec spark-master bash bin/submit.sh labs/lab05/profile_raw.py` | Run the discovery profile |

Next: Lab 6.
