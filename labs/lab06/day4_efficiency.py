"""Lab 6, Day 4: efficiency testing. Time the Day 1 cleaning on 50,000 and 500,000 raw rows.

Run from the repository folder:
    docker compose exec spark-master bash bin/submit.sh labs/lab06/day4_efficiency.py

While it runs, open http://localhost:4040 to watch the jobs and stages in the Spark web interface.
Your times depend on your computer; record them for your sprint review.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pyspark.sql import SparkSession

import nmta

SIZES = [50_000, 500_000]

spark = SparkSession.builder.appName("lab06-day4-efficiency").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
raw = nmta.read_raw(spark)

# Set-up (not timed): save each sample of raw rows to the staging zone, so the timing covers only the cleaning.
for n in SIZES:
    raw.limit(n).write.mode("overwrite").parquet(f"{nmta.STAGING}/efficiency/input_{n}")
print("Samples ready. Timing the cleaning now...", flush=True)

results = []
for n in SIZES:
    started = time.time()
    sample = spark.read.parquet(f"{nmta.STAGING}/efficiency/input_{n}")
    clean, rejected = nmta.clean(sample)
    out = f"{nmta.STAGING}/efficiency/clean_{n}"
    clean.write.mode("overwrite").parquet(out)
    seconds = time.time() - started
    rows_out = spark.read.parquet(out).count()
    results.append((n, rows_out, seconds))
    print(f"  {n:,} rows cleaned in {seconds:,.1f} seconds", flush=True)
    nmta.log_lineage("day4_efficiency", [nmta.RAW], [out], n, rows_out, n - rows_out, started)

print("\n=============== Lab 6 Day 4: efficiency ===============")
print(f"  {'Raw rows':>10}  {'Clean rows':>10}  {'Seconds':>8}  {'Rows per second':>15}")
for n, rows_out, seconds in results:
    print(f"  {n:>10,}  {rows_out:>10,}  {seconds:>8.1f}  {n / seconds:>15,.0f}")
print("=======================================================\n")
spark.stop()
