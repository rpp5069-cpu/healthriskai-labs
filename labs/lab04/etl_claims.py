"""Lab 4: a small Sprint 0 ETL pipeline. Extract claims from Kafka, then transform, mask, and load them.

Run from the repository folder:
    docker compose exec spark-master bash bin/submit.sh --kafka labs/lab04/etl_claims.py

Steps
  Extract    read every claims event in the Kafka topic (a batch read of the whole topic)
  Transform  check required fields, normalize the month, remove duplicate claims
  Mask       hash patient_id and patient_name with SHA-256 plus a salt; redact the address
  Load       write clean records to Parquet (data/curated/claims) and rejected records,
             with the reason only, to the dead-letter zone (data/dlq/claims)
Checks       input = loaded + dead-letter, and no plaintext patient identifiers in the output

Options: --source file reads data/raw/claims.jsonl instead of Kafka (useful for troubleshooting).
"""
import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

FIELDS = ["claim_id", "patient_id", "patient_name", "address", "month", "method_of_payment",
          "patient_age_range", "patient_gender", "major_class", "minor_class", "acute_chronic",
          "icd10_3", "icd10_code", "product", "brand_generic", "specialty", "region", "state",
          "patient_visits"]
SCHEMA = StructType([StructField(name, StringType()) for name in FIELDS])

ap = argparse.ArgumentParser()
ap.add_argument("--source", choices=["kafka", "file"], default="kafka")
ap.add_argument("--topic", default="iqvia_claims_lab4")
ap.add_argument("--file", default="data/raw/claims.jsonl")
ap.add_argument("--curated", default="data/curated/claims")
ap.add_argument("--dlq", default="data/dlq/claims")
args = ap.parse_args()

# In a real system the salt is a secret kept outside the code (for example, in a secrets manager).
SALT = os.environ.get("HEALTHRISKAI_SALT", "csc894-lab-salt")

spark = SparkSession.builder.appName("lab04-etl-claims").config("spark.sql.shuffle.partitions", "4").getOrCreate()
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
           .select(F.col("value").cast("string").alias("json"), F.col("offset").alias("source_offset")))
else:
    raw = (spark.read.text(args.file)
           .withColumnRenamed("value", "json")
           .withColumn("source_offset", F.monotonically_increasing_id()))
raw = raw.cache()
input_count = raw.count()
if input_count == 0:
    raise SystemExit(f"No records found in {'topic ' + args.topic if args.source == 'kafka' else args.file}. "
                     "Publish the claims first (Lab 4, Part D).")

parsed = raw.select("source_offset", F.from_json("json", SCHEMA).alias("c")).select("source_offset", "c.*")

# ---------- Transform: validate ----------
service_month = F.coalesce(F.expr("try_to_timestamp(month, 'MM/dd/yyyy')"),
                           F.expr("try_to_timestamp(month, 'yyyy-MM-dd')")).cast("date")
checked = (parsed
           .withColumn("service_month", service_month)
           .withColumn("visits", F.expr("try_cast(patient_visits AS INT)"))
           .withColumn("reason",
                       F.when(F.col("claim_id").isNull(), "malformed_record")
                        .when(blank("patient_id"), "missing_patient_id")
                        .when(blank("icd10_code"), "missing_icd10_code")
                        .when(F.col("service_month").isNull(), "invalid_month")
                        .when(F.col("visits").isNull() | (F.col("visits") < 0), "invalid_patient_visits")))

# ---------- Transform: remove duplicate claims (keep the first one received) ----------
first = Window.partitionBy("claim_id").orderBy("source_offset")
valid = (checked.filter(F.col("reason").isNull())
         .withColumn("copy_number", F.row_number().over(first))
         .withColumn("reason", F.when(F.col("copy_number") > 1, F.lit("duplicate_claim_id"))))
invalid = checked.filter(F.col("reason").isNotNull()).withColumn("copy_number", F.lit(None).cast("int"))
checked = valid.unionByName(invalid).cache()

# ---------- Mask PHI ----------
def hash_with_salt(col):
    return F.sha2(F.concat(F.col(col), F.lit(SALT)), 256)


clean = (checked.filter(F.col("reason").isNull())
         .withColumn("patient_id", hash_with_salt("patient_id"))
         .withColumn("patient_name", hash_with_salt("patient_name"))
         .withColumn("address", F.lit("REDACTED"))
         .withColumn("patient_visits", F.col("visits"))
         .drop("month", "visits", "reason", "copy_number", "source_offset"))

# The dead-letter zone keeps only what's needed to fix the record later: never patient details.
dead_letter = (checked.filter(F.col("reason").isNotNull())
               .select("claim_id", "reason", "source_offset"))

# ---------- Load ----------
clean.coalesce(1).write.mode("overwrite").parquet(args.curated)
dead_letter.coalesce(1).write.mode("overwrite").json(args.dlq)

loaded = spark.read.parquet(args.curated)
loaded_count = loaded.count()
dlq = spark.read.json(args.dlq)
dlq_count = dlq.count()
reasons = {r["reason"]: r["count"] for r in dlq.groupBy("reason").count().collect()}
plaintext = loaded.filter(~F.col("patient_id").rlike("^[0-9a-f]{64}$")
                          | ~F.col("patient_name").rlike("^[0-9a-f]{64}$")
                          | (F.col("address") != "REDACTED")).count()

# ---------- Report ----------
source = f"Kafka topic {args.topic}" if args.source == "kafka" else args.file
print("\n================ Lab 4 ETL summary ================")
print(f"Source: {source}")
print(f"  Input records                {input_count:>6,}")
print(f"  Loaded to curated (Parquet)  {loaded_count:>6,}   -> {args.curated}")
print(f"  Sent to dead-letter zone     {dlq_count:>6,}   -> {args.dlq}")
for reason in ["duplicate_claim_id", "missing_icd10_code", "missing_patient_id", "invalid_month",
               "invalid_patient_visits", "malformed_record"]:
    if reasons.get(reason):
        print(f"      {reason:<24} {reasons[reason]:>6,}")
print("---------------------------------------------------")
ok1 = input_count == loaded_count + dlq_count
print(f"Check 1  input = loaded + dead-letter ({input_count:,} = {loaded_count:,} + {dlq_count:,}): "
      f"{'PASS' if ok1 else 'FAIL'}")
ok2 = plaintext == 0
print(f"Check 2  no plaintext patient identifiers in curated output: {'PASS' if ok2 else 'FAIL'}")
print("===================================================\n")
spark.stop()
if not (ok1 and ok2):
    raise SystemExit(1)
