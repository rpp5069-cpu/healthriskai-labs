# Lab 3: Set up Kafka and Spark in Docker containers

In this lab you start the course infrastructure, a Kafka broker and a small Spark cluster, with one command.
Then you use it the way Module 3 describes: **streaming ingestion** through Kafka, **batch ingestion** with
Spark, and the two together (**hybrid ingestion**) with Spark Structured Streaming reading from Kafka.

**Time:** about 90 minutes, including a one-time download of about 1.5 GB.
**Before you start:** finish Lab 2. Your environment check should show no FAIL lines.

Each step shows the **command** to type and **what you should see**. Type commands one at a time and wait for each
to finish. If something doesn't match, check [Troubleshooting](../../docs/troubleshooting.md).

> **Synthetic data only.** Everything in this lab is made-up data shaped like the IQVIA NMTA dataset. Never use
> real patient data.

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

**D1. Create a topic for claims events, split into 3 partitions:**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic iqvia_claims --partitions 3
```

You should see **Created topic iqvia_claims.** Kafka may also print a `WARNING` about periods (`.`) and
underscores (`_`) in topic names. That's harmless; you can ignore it.

**D2. List the topics and look at the new one:**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic iqvia_claims
```

The first command shows `iqvia_claims`. The second shows `PartitionCount: 3` and one line for each partition
(0, 1, and 2).

**D3. Be a producer: send three messages by hand.**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic iqvia_claims
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
> docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic iqvia_claims
> ```

**D4. Be a consumer: read the messages back.**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic iqvia_claims --from-beginning --max-messages 3
```

You should see your three messages, then **Processed a total of 3 messages**. Kafka may first print a note about
its consumer group protocol; that's harmless. They may come back in a different
order: Kafka keeps order *within* each partition, and your three messages may have landed in different
partitions.

---

## Part E. Batch ingestion with Spark

**E1. Create the synthetic claims dataset:**

```powershell
docker compose exec spark-master python3 data/generate_claims.py
```

You should see **Synthetic claims generated (no real patient data)** and **1,025 records**. The files are on your
computer too: open the `data\raw` folder in File Explorer to see `claims.csv`, `claims.jsonl`, and
`claims_sample20.jsonl`. Running the generator again creates exactly the same data.

**E2. Load and profile the whole file with Spark:**

```powershell
docker compose exec spark-master bash bin/submit.sh labs/lab03/batch_profile.py
```

This takes about 30 seconds. `bin/submit.sh` is a short helper that sends a Python script to the Spark cluster.
You should see five sections:

1. **Schema:** the 19 columns Spark found, from `claim_id` to `patient_visits`, all read as `string`.
2. **Record count:** `Total records: 1,025`.
3. **Claims by method of payment:** a table led by `COMMERCIAL`, then `MEDICARE`.
4. **Top 5 drug classes:** led by `ANTIDIABETICS`.
5. **A first look at data quality:** `Records missing icd10_code: 20` and `Duplicate claim_id records: 25`. You
   fix these in Lab 4.

**E3. See the job on the cluster.** Refresh <http://localhost:8081>. Under **Completed Applications**, you should
see `lab03-batch-profile` with state `FINISHED`.

---

## Part F. Hybrid ingestion: Spark reads Kafka as events arrive

Now Spark reads the Kafka topic continuously with **Structured Streaming**. You need **two** PowerShell (or
Terminal) windows for this part.

**F1. Window 1: start the streaming job.** In your current window, run:

```powershell
docker compose exec spark-master bash bin/submit.sh --kafka labs/lab03/stream_claims.py
```

The first time, Spark downloads its Kafka connector, so you'll see lines such as `:: resolving dependencies ::`
for about a minute. Then you should see **Streaming job started. Waiting for claims events on Kafka topic
'iqvia_claims'...**, followed by an empty table labeled `Batch: 0`. Leave this window running.

A `WARN` line saying `spark.sql.adaptive.enabled is not supported in streaming` is expected; Spark turns that
setting off for streaming jobs.

**F2. Window 2: publish 20 claims events.** Open a **second** PowerShell window, move into the repository folder,
and publish the sample file to Kafka:

```powershell
cd $HOME\Documents\csc894\healthriskai-labs
docker compose exec kafka sh -c "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic iqvia_claims < /opt/healthriskai/data/raw/claims_sample20.jsonl"
```

The command finishes without printing anything. That's expected.

**F3. Window 1: watch the events arrive.** Within about 5 seconds, window 1 should print a new batch (for example
`Batch: 1`) with a table of **20 claims**: `claim_id`, `month`, `method_of_payment`, `major_class`,
`icd10_code`, `product`, `state`, plus the Kafka `partition` and `offset` each one came from. Spark picked them up
as they arrived; you never restarted the job.

All 20 claims will probably show the **same partition** (offsets 0 to 19). That's normal: the console producer
sends the file as one batch, and Kafka puts a batch of messages without keys into a single partition. Your partition
number may differ from a classmate's. A `WARN ... Current batch is falling behind` line after the first real batch
is also harmless; Spark is just warming up.

While the job is still running, open <http://localhost:4040> and click **Structured Streaming** to see the job's
progress.

**F4. Window 2: read the claims with the console consumer.**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic iqvia_claims --from-beginning --max-messages 23
```

You should see your three hand-typed messages and the 20 claims events (as JSON lines), then **Processed a total of
23 messages**. **Take a screenshot of this window** for your submission.

**F5. Window 1: stop the streaming job.** Press **Ctrl + C**. Spark may print a few lines about the query
stopping. That's normal. You can close window 2.

---

## Part G. Stop the infrastructure

```powershell
docker compose down
```

You should see the three containers and the network removed. Topics are deleted when the containers are removed,
so Lab 4 starts with a clean Kafka. Your files in `data\raw` stay on your computer, and Docker keeps the Spark
download cache, so the next start is faster.

---

## What to submit

1. A screenshot of the console consumer from **Part F4**, showing the claims events you sent.
2. A screenshot of your Spark job's output from **Part E2**, showing the schema and record count.
3. A short paragraph (3–5 sentences): in Module 3's HealthRiskAI scenario, which data would you ingest through
   Kafka and which through Spark batch jobs, and why?

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
| `docker compose exec spark-master python3 data/generate_claims.py` | Create the synthetic claims data |
| `docker compose exec spark-master bash bin/submit.sh SCRIPT.py` | Run a PySpark script on the cluster |
| `docker compose exec spark-master bash bin/submit.sh --kafka SCRIPT.py` | Same, with Spark's Kafka connector |

Next: Lab 4.
