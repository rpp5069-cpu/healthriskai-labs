"""Lab 4: a Sprint 0 ETL pipeline on patient visits built from the IQVIA data. Extract, transform, mask, and load.

Run from the repository folder:
    docker compose exec spark-master bash bin/submit.sh --kafka labs/lab04/etl_claims.py      (one month, from Kafka)
    docker compose exec spark-master bash bin/submit.sh labs/lab04/etl_claims.py --source file (all visits, from files)

Steps
  Extract    read the visit records from the Kafka topic, or all of them from data/raw/visits/
  Transform  check required fields, turn the month into a real date, remove duplicate visits
  Mask       hash patient_id and patient_name with SHA-256 plus a salt; redact the SSN and the street address
  Load       write clean records to Parquet, one folder per year (data/curated/visits), and rejected records,
             with the reason only, to the dead-letter zone (data/dlq/visits)
Checks       input = loaded + dead-letter; no plaintext patient identifiers in the output; and, for the full run,
             the visits still add up to the original IQVIA file

The patient identities in these records are synthetic (made by data/build_patient_visits.py). They exist only to
practice protecting patient identifiers, and later labs do not use them.
"""
import argparse
import glob
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

FIELDS = ["visit_id", "patient_id", "patient_name", "ssn", "address", "month", "method_of_payment",
          "patient_age_range", "patient_gender", "major_class", "minor_class", "pharma_type", "acute_chronic",
          "icd10_3", "icd10_code", "icd10_description", "product", "brand_generic", "specialty",
          "care_setting", "state", "region"]
SCHEMA = StructType([StructField(name, StringType()) for name in FIELDS])
REASONS = ["duplicate_visit_id", "missing_icd10_code", "missing_patient_id", "invalid_month", "malformed_record"]

ap = argparse.ArgumentParser()
ap.add_argument("--source", choices=["kafka", "file"], default="kafka")
ap.add_argument("--topic", default="patient_visits_lab4")
ap.add_argument("--files", default="data/raw/visits/visits_*.csv.gz")
ap.add_argument("--curated", default="data/curated/visits")
ap.add_argument("--dlq", default="data/dlq/visits")
args = ap.parse_args()

# In a real system the salt is a secret kept outside the code (for example, in a secrets manager).
SALT = os.environ.get("HEALTHRISKAI_SALT", "csc894-lab-salt")

spark = SparkSession.builder.appName(f"lab04-etl-{args.source}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def blank(col):
    return F.col(col).isNull() | (F.trim(F.col(col)) == "")


# ---------- Extract ----------
if args.source == "kafka":
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", "kafka:9092")
           .option("subscribe", args.topic)
           .option("startingOffsets", "earliest")
           .option("endingOffsets", "latest")
           .load()
           .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("v"), F.col("offset").alias("source_offset"))
           .select("v.*", "source_offset")
           .withColumn("source", F.concat(F.lit("kafka offset "), F.col("source_offset"))))
    source = f"Kafka topic {args.topic}"
else:
    files = sorted(glob.glob(args.files))
    if not files:
        raise SystemExit(f"No files match {args.files}. Run data/build_patient_visits.py first (Lab 4, Part B).")
    # Every column is read as text, exactly as it arrives; the pipeline decides what is valid.
    raw = (spark.read.option("header", True).schema(SCHEMA).csv(files)
           .withColumn("source_offset", F.monotonically_increasing_id())   # the order records were read in
           .withColumn("source", F.element_at(F.split(F.input_file_name(), "/"), -1)))
    source = f"{len(files)} files in {os.path.dirname(args.files)}"
raw = raw.persist()
input_count = raw.count()
if input_count == 0:
    raise SystemExit(f"No records found in {source}. Publish the visits first (Lab 4, Part C).")

# ---------- Transform: validate ----------
service_month = F.coalesce(F.expr("try_to_timestamp(concat('1 ', month), 'd MMM yyyy')"),
                           F.expr("try_to_timestamp(month, 'yyyy-MM-dd')")).cast("date")
checked = (raw
           .withColumn("service_month", service_month)
           .withColumn("reason",
                       F.when(blank("visit_id"), "malformed_record")
                        .when(blank("patient_id"), "missing_patient_id")
                        .when(blank("icd10_code"), "missing_icd10_code")
                        .when(F.col("service_month").isNull(), "invalid_month")))

# ---------- Transform: remove duplicate visits (keep the first one received) ----------
first = Window.partitionBy("visit_id").orderBy("source_offset")
valid = (checked.filter(F.col("reason").isNull())
         .withColumn("copy_number", F.row_number().over(first))
         .withColumn("reason", F.when(F.col("copy_number") > 1, F.lit("duplicate_visit_id"))))
invalid = checked.filter(F.col("reason").isNotNull()).withColumn("copy_number", F.lit(None).cast("int"))
checked = valid.unionByName(invalid).persist()


# ---------- Mask PHI ----------
def hash_with_salt(col):
    return F.sha2(F.concat(F.col(col), F.lit(SALT)), 256)


clean = (checked.filter(F.col("reason").isNull())
         .withColumn("patient_id", hash_with_salt("patient_id"))
         .withColumn("patient_name", hash_with_salt("patient_name"))
         .withColumn("ssn", F.lit("REDACTED"))
         .withColumn("address", F.lit("REDACTED"))
         .withColumn("service_year", F.year("service_month"))
         .drop("month", "reason", "copy_number", "source_offset", "source"))

# The dead-letter zone keeps only what's needed to fix the record later: never patient details.
dead_letter = checked.filter(F.col("reason").isNotNull()).select("visit_id", "reason", "source")

# ---------- Load ----------
clean.repartition("service_year").write.mode("overwrite").partitionBy("service_year").parquet(args.curated)
dead_letter.coalesce(1).write.mode("overwrite").json(args.dlq)

loaded = spark.read.parquet(args.curated)
loaded_count = loaded.count()
dlq = spark.read.json(args.dlq) if input_count > loaded_count else None
dlq_count = dlq.count() if dlq is not None else 0
reasons = {r["reason"]: r["count"] for r in dlq.groupBy("reason").count().collect()} if dlq is not None else {}
plaintext = loaded.filter(~F.col("patient_id").rlike("^[0-9a-f]{64}$")
                          | ~F.col("patient_name").rlike("^[0-9a-f]{64}$")
                          | (F.col("ssn") != "REDACTED") | (F.col("address") != "REDACTED")).count()

# Reconcile with the original IQVIA file: every IQVIA visit is either loaded or rejected for a field problem.
iqvia_total = None
iqvia_files = glob.glob("data/iqvia/*.csv")
if args.source == "file" and len(iqvia_files) == 1:
    nmta = spark.read.option("header", True).csv(iqvia_files[0])
    spec = nmta.columns[13:nmta.columns.index("MIDWEST_Patient Visits")]
    iqvia_total = nmta.select(sum(F.coalesce(F.col(f"`{c}`").cast("int"), F.lit(0)) for c in spec).alias("v")) \
                      .agg(F.sum("v")).first()[0]

# ---------- Report ----------
w = 9
print("\n================ Lab 4 ETL summary ================")
print(f"Source: {source}")
print(f"  Input records                {input_count:>{w},}")
print(f"  Loaded to curated (Parquet)  {loaded_count:>{w},}   -> {args.curated}")
print(f"  Sent to dead-letter zone     {dlq_count:>{w},}   -> {args.dlq}")
for reason in REASONS:
    if reasons.get(reason):
        print(f"      {reason:<24} {reasons[reason]:>{w},}")
print("---------------------------------------------------")
ok1 = input_count == loaded_count + dlq_count
print(f"Check 1  input = loaded + dead-letter ({input_count:,} = {loaded_count:,} + {dlq_count:,}): "
      f"{'PASS' if ok1 else 'FAIL'}")
ok2 = plaintext == 0
print(f"Check 2  no plaintext patient identifiers in curated output: {'PASS' if ok2 else 'FAIL'}")
ok3 = True
if iqvia_total is not None:
    field_rejects = dlq_count - reasons.get("duplicate_visit_id", 0)
    ok3 = loaded_count + field_rejects == iqvia_total
    print(f"Check 3  visits match the IQVIA file ({loaded_count:,} + {field_rejects:,} = {iqvia_total:,}): "
          f"{'PASS' if ok3 else 'FAIL'}")
print("===================================================\n")
spark.stop()
if not (ok1 and ok2 and ok3):
    raise SystemExit(1)
