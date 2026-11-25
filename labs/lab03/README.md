# Lab 3: Set up Kafka and Spark in Docker containers

In this lab you start the course infrastructure, a Kafka broker and a small Spark cluster, with one command.
Then you use it the way Module 3 describes: **streaming ingestion** through Kafka, **batch ingestion** with
Spark, and the two together (**hybrid ingestion**) with Spark Structured Streaming reading from Kafka. The data is
the course's IQVIA National Medical and Treatment Audit (NMTA) extract.

**Time:** about 90 minutes, including a one-time download of about 1.5 GB.
**Before you start:** finish Lab 2. Your environment check should show no FAIL lines.

Each step shows the **command** to type and **what you should see**. Type commands one at a time and wait for each
to finish. If something doesn't match, check [Troubleshooting](../../docs/troubleshooting.md).

> **Licensed data.** The IQVIA file from Canvas is licensed for this course only. Keep it in the `data\iqvia`
> folder on your own computer, and never commit, upload, or share it. The file is aggregated: it has visit counts,
> not patient records.

---

## Part A. Watch the preparation videos (about 40 minutes)

1. **Kafka PySpark Structured Streaming Demo** (Saif Shaikh)
   <https://www.youtube.com/watch?v=uZz6CSLEO_c>
2. **Spark Structured Streaming Introduction** (Anirvan Decodes)
   <https://www.youtube.com/watch?v=vnUOILQA3jo>

> These are third-party videos and may use different versions than this course. When they differ, follow these
> lab instructions.

---

## Part B. Get the latest course files

The repository has new files for this lab, so update your copy first.

- **If you used Git (Option 2 in Lab 2):** open PowerShell (Terminal on macOS), then run:

  ```powershell
  cd $HOME\Documents\csc894\healthriskai-labs      # macOS / Linux: cd ~/Documents/csc894/healthriskai-labs
  git pull
  ```

- **If you downloaded the ZIP:** download it again and replace your old copy:
  1. Delete (or rename) your old `healthriskai-labs` folder in `Documents\csc894`.
  2. Download the ZIP from the repository page (**Code** > **Download ZIP**), extract it into `Documents\csc894`,
     and rename `healthriskai-labs-main` to `healthriskai-labs`, as in Lab 2, Part D.

Move into the repository folder and check the files:

```powershell
cd $HOME\Documents\csc894\healthriskai-labs      # macOS / Linux: cd ~/Documents/csc894/healthriskai-labs
ls -Name                                         # macOS / Linux: ls
```

You should see `bin`, `data`, `docs`, `labs`, `scripts`, `compose.yaml`, and `README.md`.

> **Run every command in this lab from this folder** (the one with `compose.yaml`).

**Add the IQVIA file.** Download the IQVIA NMTA file from Canvas (unzip it if it comes as a `.zip`), and move the
`.csv` file into the `data\iqvia` folder of the repository. Check that it's there:

```powershell
ls data\iqvia -Name                               # macOS / Linux: ls data/iqvia
```

You should see `README.md` and the IQVIA `.csv` file. Keep only one `.csv` file in this folder.

---

## Part C. Start the course infrastructure

**C1. Make sure Docker Desktop is running** (bottom-left says **Engine running**) and that nothing from Lab 2 is
still running:

```powershell
docker ps
```

You should see an empty list (just the column headings). If `lab02-web` is listed, run `docker rm -f lab02-web`.

**C2. Start Kafka and Spark:**

```powershell
docker compose up -d
```

The first time, Docker downloads the Kafka and Spark images (about 1.5 GB), which takes a few minutes. You should
then see the network, a volume, and three containers created: `healthriskai-kafka-1`,
`healthriskai-spark-master-1`, and `healthriskai-spark-worker-1`.

**C3. Check that everything is running:**

```powershell
docker compose ps
```

You should see three rows. Wait until **kafka** and **spark-master** both say `(healthy)` in the STATUS column
(up to a minute; run the command again to refresh). **spark-worker** just says `Up`.

**C4. Look at the Spark cluster.** Open <http://localhost:8081> in your browser. You should see the **Spark
Master** page with **Alive Workers: 1**, **Cores in use: 2 Total**, and **Memory in use: 2.0 GiB Total**. One
worker with two CPU cores is a tiny cluster, but it works exactly like a large one.

---

## Part D. Streaming ingestion with Kafka

Kafka stores messages in **topics**. Programs called **producers** write messages to a topic, and **consumers**
read them. Every command below runs Kafka's own tools inside the `kafka` container.

**D1. Create a topic for IQVIA records, split into 3 partitions:**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic iqvia_nmta --partitions 3
```

You should see **Created topic iqvia_nmta.** Kafka may also print a `WARNING` about periods (`.`) and
underscores (`_`) in topic names. That's harmless; you can ignore it.

**D2. List the topics and look at the new one:**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic iqvia_nmta
```

The first command shows `iqvia_nmta`. The second shows `PartitionCount: 3` and one line for each partition
(0, 1, and 2).

**D3. Be a producer: send three messages by hand.**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic iqvia_nmta
```

You should see a `>` prompt. At the prompt, type **only the message text** below (not a command), and press
**Enter** after each line:

```text
hello kafka
first test message
claims pipeline lab 3
```

Then press **Ctrl + C** to stop the producer and get back to PowerShell. If Docker then prints a line like
`What's next: Debug this Compose error with Gordon`, ignore it: that's Docker's message for any command stopped with
Ctrl + C.

> **Typed something else at the `>` prompt?** Everything you type there becomes a message, including a pasted
> command. To start the topic over, delete it, then repeat D1 to D3:
>
> ```powershell
> docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic iqvia_nmta
> ```

**D4. Be a consumer: read the messages back.** First make sure the producer has stopped and you're back at the
`PS C:\...>` prompt. A command typed at the `>` prompt becomes another Kafka message.

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic iqvia_nmta --from-beginning --max-messages 3
```

You should see your three messages, then **Processed a total of 3 messages**. Kafka may first print a note about
its consumer group protocol; that's harmless. They may come back in a different
order: Kafka keeps order *within* each partition, and your three messages may have landed in different
partitions.

---

## Part E. Batch ingestion with Spark

**E1. Check the IQVIA file and prepare a small sample for Part F:**

```powershell
docker compose exec spark-master python3 data/prepare_iqvia.py
```

You should see **IQVIA file found**, **632,690 rows covering 72 months, 136 columns**, and **20 rows from Jan 2026**.
The sample is in `data\raw\iqvia_sample20.jsonl`. If you see **Put the IQVIA CSV file from Canvas in the
data/iqvia folder**, go back to Part B and add the file.

**E2. Load and profile the whole file with Spark:**

```powershell
docker compose exec spark-master bash bin/submit.sh labs/lab03/batch_profile.py
```

This takes about a minute. `bin/submit.sh` is a short helper that sends a Python script to the Spark cluster.
You should see five sections:

1. **Columns:** 136 columns, of which 13 describe each row (month, payer, age range, gender, drug, diagnosis) and
   123 count patient visits by specialty, region, state, a YES/NO category, and care setting. The header uses the
   name `OTHER_Patient Visits` three times, so Spark renames the copies by their position (for example,
   `OTHER_Patient Visits53`). You'll handle details like this in Lab 4.
2. **Size:** `Total rows: 632,690`, `Months: 72 (Feb 2020 to Jan 2026)`, and `Total visits: 2,892,144`.
3. **Visits by method of payment:** a table led by `COMMERCIAL`, then `MEDICARE`.
4. **Top 5 products by visits:** led by `ELIQUIS`. Almost all of this extract is antithrombotics (blood thinners).
5. **A first look at data quality:** `Rows whose specialty, state, and setting totals disagree: 0` (every
   breakdown counts each visit once) and `Rows with an unknown or unspecified payer, gender, or age: 10,243`.

**E3. See the job on the cluster.** Refresh <http://localhost:8081>. Under **Completed Applications**, you should
see `lab03-batch-profile` with state `FINISHED`.

---

## Part F. Hybrid ingestion: Spark reads Kafka as records arrive

Now Spark reads the Kafka topic continuously with **Structured Streaming**. You need **two** PowerShell (or
Terminal) windows for this part.

**F1. Window 1: start the streaming job.** In your current window, run:

```powershell
docker compose exec spark-master bash bin/submit.sh --kafka labs/lab03/stream_claims.py
```

The first time, Spark downloads its Kafka connector, so you'll see lines such as `:: resolving dependencies ::`
for about a minute. Then you should see **Streaming job started. Waiting for IQVIA records on Kafka topic
'iqvia_nmta'...**, followed by an empty table labeled `Batch: 0`. Leave this window running.

A `WARN` line saying `spark.sql.adaptive.enabled is not supported in streaming` is expected; Spark turns that
setting off for streaming jobs.

**F2. Window 2: publish 20 IQVIA rows.** Open a **second** PowerShell window, move into the repository folder,
and publish the sample file to Kafka:

```powershell
cd $HOME\Documents\csc894\healthriskai-labs
docker compose exec kafka sh -c "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic iqvia_nmta < /opt/healthriskai/data/raw/iqvia_sample20.jsonl"
```

The command finishes without printing anything. That's expected.

**F3. Window 1: watch the records arrive.** Within about 5 seconds, window 1 should print a new batch (for example
`Batch: 1`) with a table of **20 IQVIA rows from January 2026**: `month`, `method_of_payment`,
`patient_age_range`, `patient_gender`, `acute_chronic`, `product`, `icd10_code`, and `total_visits`, plus the Kafka
`partition` and `offset` each one came from. Spark picked them up as they arrived; you never restarted the job.
**Take a screenshot of this window** showing the whole table: it's your submission.

All 20 rows will probably show the **same partition** (offsets 0 to 19). That's normal: the console producer sends
the file as one batch, and Kafka puts a batch of messages without keys into a single partition. Your partition
number may differ from a classmate's. A `WARN ... Current batch is falling behind` line after the first real batch
is also harmless; Spark is just warming up.

While the job is still running, open <http://localhost:4040> and click **Structured Streaming** to see the job's
progress.

**F4. Window 2: read the topic with the console consumer.**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic iqvia_nmta --from-beginning --max-messages 23
```

You should see your three hand-typed messages and the 20 IQVIA rows (as JSON lines), then **Processed a total of 23
messages**. They may come back in a different order, because they're in different partitions.

**F5. Window 1: stop the streaming job.** Press **Ctrl + C**. Spark may print a few lines about the query
stopping. That's normal. You can close window 2.

---

## Part G. Stop the infrastructure

```powershell
docker compose down
```

You should see the three containers and the network removed. Topics are deleted when the containers are removed,
so Lab 4 starts with a clean Kafka. Your files in `data\iqvia` and `data\raw` stay on your computer, and Docker
keeps the Spark download cache, so the next start is faster.

---

## What to submit

1. **One screenshot** of window 1 from **Part F3**, showing the `Batch: 1` table with all 20 IQVIA rows from
   January 2026, including the `partition` and `offset` columns. This shows that the records you sent to Kafka were
   received and processed by Spark.
2. **A discussion post** in the Lab 3 discussion: post your screenshot and describe one issue you ran into while
   completing Lab 3 and how you resolved it. If everything worked the first time, describe the step that was hardest
   to understand, or a warning message you saw, and what you learned from it.

---

## Command summary

| Command | What it does |
|---|---|
| `docker compose up -d` / `docker compose down` | Start / stop the course infrastructure |
| `docker compose ps` | Check the containers' status |
| `docker compose logs kafka` | Show Kafka's log (useful when something fails) |
| `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list` | List topics |
| `... kafka-topics.sh ... --create --topic NAME --partitions N` | Create a topic |
| `... kafka-console-producer.sh --bootstrap-server kafka:9092 --topic NAME` | Type messages into a topic |
| `... kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic NAME --from-beginning` | Read a topic |
| `docker compose exec spark-master python3 data/prepare_iqvia.py` | Check the IQVIA file and create the 20-row sample |
| `docker compose exec spark-master bash bin/submit.sh SCRIPT.py` | Run a PySpark script on the cluster |
| `docker compose exec spark-master bash bin/submit.sh --kafka SCRIPT.py` | Same, with Spark's Kafka connector |

Next: Lab 4.
