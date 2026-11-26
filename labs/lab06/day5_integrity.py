"""Lab 6, Day 5: integrity and edge-case testing.

Run from the repository folder after Days 1 to 3:
    docker compose exec spark-master bash bin/submit.sh labs/lab06/day5_integrity.py

Part 1 reconciles the counts at every step of the pipeline. Part 2 injects malformed records into a copy of real rows
and confirms the pipeline rejects each one for the right reason, keeps the fixable ones, and finishes normally.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import nmta

started = time.time()
spark = SparkSession.builder.appName("lab06-day5-integrity").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
jvm_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path
conf = spark.sparkContext._jsc.hadoopConfiguration()


def exists(path):
    p = jvm_path(path)
    return p.getFileSystem(conf).exists(p)


def rows_and_visits(df, col="total_visits"):
    r = df.agg(F.count(F.lit(1)), F.sum(col)).first()
    return r[0], r[1] or 0


def dead_letter(path):
    if not exists(path):
        return 0, 0
    files = spark.read.json(path)
    if not files.columns:
        return 0, 0
    return rows_and_visits(files)


# ---------- Part 1: reconcile every step ----------
raw = nmta.read_raw(spark)
spec = nmta.visit_columns(raw, "spec")
raw_rows, raw_visits = rows_and_visits(
    raw.select(sum(F.coalesce(F.expr(f"try_cast(`{c}` AS INT)"), F.lit(0)) for c in spec).alias("v")), "v")
stage_rows, stage_visits = rows_and_visits(spark.read.parquet(f"{nmta.STAGING}/nmta_clean"))
dlq_rows, dlq_visits = dead_letter(f"{nmta.DEAD_LETTER}/nmta")
curated = spark.read.parquet(f"{nmta.CURATED}/nmta_visits")
cur_rows, cur_visits = rows_and_visits(curated)
features = spark.read.parquet(f"{nmta.CURATED}/monthly_product_features")
feat_visits = features.agg(F.sum("visits")).first()[0]
stream_rows, _ = rows_and_visits(spark.read.parquet(f"{nmta.STAGING}/nmta_stream")) \
    if exists(f"{nmta.STAGING}/nmta_stream") else (0, 0)
stream_dlq, _ = dead_letter(f"{nmta.DEAD_LETTER}/nmta_stream")
fields_c, values_c, _ = nmta.compliance_check(curated)
fields_f, values_f, _ = nmta.compliance_check(features)

checks = [
    ("Raw rows = staging + dead-letter", f"{raw_rows:,} = {stage_rows:,} + {dlq_rows:,}",
     raw_rows == stage_rows + dlq_rows),
    ("Raw visits = staging + dead-letter visits", f"{raw_visits:,} = {stage_visits:,} + {dlq_visits:,}",
     raw_visits == stage_visits + dlq_visits),
    ("Curated rows = staging rows", f"{cur_rows:,} = {stage_rows:,}", cur_rows == stage_rows),
    ("Curated visits = staging visits", f"{cur_visits:,} = {stage_visits:,}", cur_visits == stage_visits),
    ("Feature visits = curated visits", f"{feat_visits:,} = {cur_visits:,}", feat_visits == cur_visits),
    ("Stream records = 100 (clean + dead-letter)", f"{stream_rows + stream_dlq} = {stream_rows} + {stream_dlq}",
     stream_rows + stream_dlq == 100),
    ("No patient identifiers in the curated zone", f"{len(fields_c) + len(fields_f)} fields, "
     f"{values_c + values_f} values", not fields_c and not fields_f and values_c + values_f == 0),
]

# ---------- Part 2: inject malformed records ----------
base = (raw.filter(F.col("month") == "Jan 2026").orderBy("source_row").limit(3).collect())
good = base[0].asDict()
state_col = next(c for c in nmta.visit_columns(raw, "state") if good[c])
spec_col = next(c for c in spec if good[c])
cases = [  # (description, changes to a copy of a real row, expected result)
    ("Unreadable month (Jann 2026)", {"month": "Jann 2026"}, "invalid_month"),
    ("Missing product", {"product": ""}, "missing_required_field"),
    ("Negative visit count (-2)", {spec_col: "-2"}, "invalid_visit_count"),
    ("Visit count that isn't a number (five)", {spec_col: "five"}, "invalid_visit_count"),
    ("Every visit count blank", {c: None for c in nmta.visit_columns(raw, "spec") + nmta.visit_columns(raw, "region")
                                + nmta.visit_columns(raw, "state") + nmta.visit_columns(raw, "yesno")
                                + nmta.visit_columns(raw, "setting")}, "no_visits"),
    ("State counts don't add up", {state_col: str(int(good[state_col]) + 1)}, "inconsistent_totals"),
    ("Malformed ICD-10 code (XYZ)", {"icd10_5": "XYZ - NOT A CODE"}, "invalid_icd10_code"),
    ("Exact duplicate of a real row", {}, "duplicate_row"),
    ("Month written 2026-01-01 (fixable)", {"month": "2026-01-01"}, "kept"),
    ("Privacy-restricted diagnosis (keep)", {"icd10_5": "-1XX - PRIVACY RESTRICTED"}, "kept"),
]
rows = [r.asDict() for r in base]
for i, (_, changes, _) in enumerate(cases):
    rows.append({**good, **changes, "source_row": 10**12 + i})
test = spark.createDataFrame(rows, schema=raw.schema)
clean, rejected = nmta.clean(test)
reasons = {r["source_row"]: r["reason"] for r in rejected.select("source_row", "reason").collect()}
kept = clean.count()

edge = []
for i, (label, _, expected) in enumerate(cases):
    actual = reasons.get(10**12 + i, "kept")
    edge.append((label, expected, actual, actual == expected))
expected_kept = len(base) + sum(1 for *_, e in cases if e == "kept")
edge_ok = all(ok for *_, ok in edge) and kept == expected_kept

# ---------- Report ----------
print("\n================== Lab 6 Day 5: integrity summary ==================")
print("Part 1. Counts reconcile at every step")
for label, numbers, ok in checks:
    print(f"  {label:<44}{numbers:>32}  {'PASS' if ok else 'FAIL'}")
print("Part 2. Injected edge cases (the pipeline finished without stopping)")
for label, expected, actual, ok in edge:
    print(f"  {label:<44}{'-> ' + actual:>32}  {'PASS' if ok else 'FAIL'}")
print(f"  {'Real rows kept alongside the bad ones':<44}{f'{kept} of {expected_kept}':>32}  "
      f"{'PASS' if kept == expected_kept else 'FAIL'}")
all_ok = all(ok for *_, ok in checks) and edge_ok
print("--------------------------------------------------------------------")
print(f"Result: {'ALL CHECKS PASS' if all_ok else 'SOME CHECKS FAILED'}")
print("=====================================================================\n")

nmta.log_lineage("day5_integrity", [nmta.RAW, nmta.STAGING, nmta.CURATED], [], raw_rows, cur_rows, dlq_rows,
                 started, checks_passed=all_ok)
spark.stop()
if not all_ok:
    raise SystemExit(1)
