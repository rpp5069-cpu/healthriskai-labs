"""Lab 6, Day 3: run the same cleaning on records streaming in from Kafka (integration).

Run from the repository folder:
    docker compose exec spark-master bash bin/submit.sh labs/lab06/day3_stream.py --prepare
        writes 100 raw IQVIA rows (January 2026) to data/raw/nmta_stream100.jsonl, ready to publish to Kafka
    docker compose exec spark-master bash bin/submit.sh --kafka labs/lab06/day3_stream.py
        reads everything in the Kafka topic nmta_stream with Spark Structured Streaming, cleans each micro-batch
        with the Day 1 rules, lands the results in the HDFS staging zone, then stops by itself

Each run starts fresh: it removes the previous stream output and checkpoint first.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

import nmta

SAMPLE = "data/raw/nmta_stream100.jsonl"
TOPIC = "nmta_stream"
OUT, DLQ = f"{nmta.STAGING}/nmta_stream", f"{nmta.DEAD_LETTER}/nmta_stream"
CHECKPOINT = f"{nmta.STAGING}/_checkpoints/nmta_stream"

started = time.time()
spark = SparkSession.builder.appName("lab06-day3-stream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

if "--prepare" in sys.argv:
    rows = (nmta.read_raw(spark).filter(F.col("month") == "Jan 2026").orderBy("source_row").limit(100)
            .drop("source_row").collect())
    os.makedirs(os.path.dirname(SAMPLE), exist_ok=True)
    with open(SAMPLE, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r.asDict()) + "\n")
    print(f"\nWrote {len(rows)} raw IQVIA rows from January 2026 to {SAMPLE}\n")
    spark.stop()
    sys.exit(0)

with open(SAMPLE, encoding="utf-8") as f:
    columns = list(json.loads(f.readline()))
schema = StructType([StructField(c, StringType()) for c in columns])
for path in (OUT, DLQ, CHECKPOINT):
    nmta.delete_path(spark, path)

events = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("subscribe", TOPIC)
          .option("startingOffsets", "earliest")
          .option("maxOffsetsPerTrigger", 40)          # small micro-batches, so you can watch them arrive
          .load()
          .select(F.from_json(F.col("value").cast("string"), schema).alias("r"), F.col("offset").alias("source_row"))
          .select("r.*", "source_row"))


def process(batch, batch_id):
    """Apply the Day 1 cleaning rules to one micro-batch and land it in HDFS."""
    if batch.isEmpty():
        return
    clean, rejected = nmta.clean(batch)
    clean.write.mode("append").parquet(OUT)
    bad = rejected.count()
    if bad:
        rejected.write.mode("append").json(DLQ)
    print(f"  Micro-batch {batch_id}: {batch.count()} records from Kafka -> {clean.count()} clean, {bad} rejected",
          flush=True)


print(f"\nReading Kafka topic '{TOPIC}' with Spark Structured Streaming...", flush=True)
query = (events.writeStream.foreachBatch(process)
         .option("checkpointLocation", CHECKPOINT)
         .trigger(availableNow=True)                    # process everything in the topic, then stop
         .start())
query.awaitTermination()

fs_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path
conf = spark.sparkContext._jsc.hadoopConfiguration()


def exists(path):
    p = fs_path(path)
    return p.getFileSystem(conf).exists(p)


streamed = spark.read.parquet(OUT) if exists(OUT) else None
rows_clean = streamed.count() if streamed is not None else 0
rows_bad = spark.read.json(DLQ).count() if exists(DLQ) else 0
rows_in = rows_clean + rows_bad

# The same 100 rows, cleaned as a batch from the file, must give exactly the same result.
batch = spark.read.schema(schema).json(os.path.abspath(SAMPLE)).withColumn("source_row", F.monotonically_increasing_id())
batch_clean, _ = nmta.clean(batch)
key = ["service_month", "method_of_payment", "patient_age_range", "patient_gender", "minor_class", "icd10_code",
       "product", "brand_generic", "total_visits"]
differ = (streamed.select(key).exceptAll(batch_clean.select(key)).count()
          + batch_clean.select(key).exceptAll(streamed.select(key)).count()) if streamed is not None else -1
visits = streamed.agg(F.sum("total_visits")).first()[0] if streamed is not None else 0

ok1 = rows_in == batch.count()
ok2 = differ == 0
print("\n=============== Lab 6 Day 3: streaming integration ===============")
print(f"  {'Records read from Kafka topic ' + TOPIC:<44}{rows_in:>10,}")
print(f"  {'Clean -> datalake/staging/nmta_stream':<44}{rows_clean:>10,}   ({visits:,} visits)")
print(f"  {'Rejected -> dead-letter zone':<44}{rows_bad:>10,}")
print("--------------------------------------------------------------------")
print(f"Check 1  every record in the sample arrived ({rows_in} of {batch.count()}): {'PASS' if ok1 else 'FAIL'}")
print(f"Check 2  streaming output = batch output for the same records: {'PASS' if ok2 else 'FAIL'}")
print("====================================================================\n")

nmta.log_lineage("day3_stream", [f"kafka:{TOPIC}"], [OUT, DLQ], rows_in, rows_clean, rows_bad, started)
spark.stop()
if not (ok1 and ok2):
    raise SystemExit(1)
