"""Lab 3, batch ingestion: load the whole IQVIA NMTA file with Spark and profile it.

Run from the repository folder:
    docker compose exec spark-master bash bin/submit.sh labs/lab03/batch_profile.py
"""
import glob
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

files = sys.argv[1:] or glob.glob("data/iqvia/*.csv")
if len(files) != 1:
    raise SystemExit("Put the IQVIA CSV file from Canvas in the data/iqvia folder (exactly one .csv file). "
                     f"Found {len(files)}.")
SOURCE = files[0]

spark = SparkSession.builder.appName("lab03-batch-profile").config("spark.sql.shuffle.partitions", "4").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Batch ingestion: read the whole file at once. Every column is read as text; Lab 4 sets proper types.
nmta = spark.read.option("header", True).csv(SOURCE)
cols = nmta.columns
first_visit, first_region = 13, cols.index("MIDWEST_Patient Visits")
first_state, first_yes, first_setting = cols.index("ALABAMA_Patient Visits"), cols.index("YES_Patient Visits"), \
    cols.index("HOSPITAL_Patient Visits")


def total(names):
    return sum(F.coalesce(F.col(f"`{c}`").cast("int"), F.lit(0)) for c in names)


# Every visit is counted once in each breakdown (specialty, region, state, YES/NO, care setting).
nmta = (nmta.withColumn("visits_by_specialty", total(cols[first_visit:first_region]))
            .withColumn("visits_by_state", total(cols[first_state:first_yes]))
            .withColumn("visits_by_setting", total(cols[first_setting:]))
            .cache())

print("\n=== 1. Columns Spark found ===")
print(f"{len(cols)} columns: {first_visit} describe each row, and {len(cols) - first_visit} count patient visits.")
print("Describing columns: " + ", ".join(cols[:first_visit]))
repeated = [c for c in cols if c.startswith("OTHER_Patient Visits")]
print(f"The header repeats 'OTHER_Patient Visits', so Spark renamed the copies: {', '.join(repeated)}\n")

rows = nmta.count()
first_month, last_month = nmta.select(F.min(F.to_date(F.concat(F.lit("1 "), "Month"), "d MMM yyyy")),
                                      F.max(F.to_date(F.concat(F.lit("1 "), "Month"), "d MMM yyyy"))).first()
visits = nmta.agg(F.sum("visits_by_specialty")).first()[0]
print("=== 2. Size ===")
print(f"Total rows:   {rows:,}")
print(f"Months:       {nmta.select('Month').distinct().count()} ({first_month:%b %Y} to {last_month:%b %Y})")
print(f"Total visits: {visits:,}\n")

print("=== 3. Visits by method of payment ===")
(nmta.groupBy(F.col("Method Of Payment").alias("method_of_payment"))
     .agg(F.sum("visits_by_specialty").alias("visits"))
     .orderBy(F.desc("visits")).show(truncate=False))

print("=== 4. Top 5 products by visits ===")
(nmta.groupBy(F.col("Product Sum").alias("product"))
     .agg(F.sum("visits_by_specialty").alias("visits"))
     .orderBy(F.desc("visits")).show(5, truncate=False))

print("=== 5. A first look at data quality ===")
mismatched = nmta.filter((F.col("visits_by_specialty") != F.col("visits_by_state"))
                         | (F.col("visits_by_specialty") != F.col("visits_by_setting"))).count()
unknown = nmta.filter(F.col("Method Of Payment").isin("UNKNOWN MEDICAL", "UNSPECIFIED")
                      | (F.col("Patient Gender") == "UNSPECIFIED")
                      | (F.col("Patient Age Range") == "UNSPECIFIED")).count()
print(f"Rows whose specialty, state, and setting totals disagree: {mismatched}")
print(f"Rows with an unknown or unspecified payer, gender, or age: {unknown:,}")
print("Visit cells are blank when the count is zero; Lab 4 turns them into numbers.\n")

spark.stop()
