"""Lab 6, Day 1: clean the raw IQVIA data and check compliance.

Run from the repository folder:
    docker compose exec spark-master bash bin/submit.sh labs/lab06/day1_clean.py

Reads the raw zone, applies the Sprint 1 cleaning rules (see nmta.py), writes clean rows to the staging zone and
rejected rows, with the reason, to the dead-letter zone. Then it checks that no patient identifiers are present.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import nmta

started = time.time()
spark = SparkSession.builder.appName("lab06-day1-clean").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = nmta.read_raw(spark).cache()
rows_in = raw.count()
clean, rejected = nmta.clean(raw)

out, dlq = f"{nmta.STAGING}/nmta_clean", f"{nmta.DEAD_LETTER}/nmta"
clean.write.mode("overwrite").parquet(out)
rejected.coalesce(1).write.mode("overwrite").json(dlq)

staged = spark.read.parquet(out)
rows_clean = staged.count()
visits_clean = staged.agg(F.sum("total_visits")).first()[0]
dead = spark.read.json(dlq) if rows_in > rows_clean else None
rows_dead = dead.count() if dead is not None else 0
reasons = {r["reason"]: r["count"] for r in dead.groupBy("reason").count().collect()} if dead is not None else {}
stats = staged.agg(F.sum(F.col("diagnosis_restricted").cast("int")).alias("restricted"),
                   F.sum(F.col("has_unknown_value").cast("int")).alias("unknown")).first()
fields, values, small = nmta.compliance_check(staged)

ok_rows = rows_in == rows_clean + rows_dead
ok_comp = not fields and values == 0


def line(label, value):
    print(f"  {label:<50}{value:>12}")


print("\n===================== Lab 6 Day 1: cleaning =====================")
print(f"Source: {nmta.RAW}")
line("Raw rows", f"{rows_in:,}")
line("Clean rows -> staging zone", f"{rows_clean:,}")
line("  patient visits in clean rows", f"{visits_clean:,}")
line("Rejected rows -> dead-letter zone", f"{rows_dead:,}")
for reason in nmta.REASONS:
    if reasons.get(reason):
        line(f"  {reason}", f"{reasons[reason]:,}")
line("Privacy-restricted diagnosis (kept as RESTRICTED)", f"{stats['restricted']:,}")
line("Unknown payer, gender, or age (kept as UNKNOWN)", f"{stats['unknown']:,}")
print("------------------------ Compliance check ------------------------")
line("Fields named like a patient identifier", f"{len(fields)}  {'PASS' if not fields else 'FAIL'}")
line("Values that look like an SSN, email, or phone", f"{values}  {'PASS' if values == 0 else 'FAIL'}")
line(f"Rows with fewer than {nmta.SMALL_COUNT} visits (small counts)", f"{small:,}")
print("-------------------------------------------------------------------")
print(f"Check  raw rows = clean + rejected ({rows_in:,} = {rows_clean:,} + {rows_dead:,}): "
      f"{'PASS' if ok_rows else 'FAIL'}")
print("===================================================================\n")

nmta.log_lineage("day1_clean", [nmta.RAW], [out, dlq], rows_in, rows_clean, rows_dead, started,
                 compliance="PASS" if ok_comp else "FAIL")
spark.stop()
if not (ok_rows and ok_comp):
    raise SystemExit(1)
