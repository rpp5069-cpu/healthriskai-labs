"""Lab 3, batch ingestion: load the synthetic claims file with Spark and profile it.

Run from the repository folder:
    docker compose exec spark-master bash bin/submit.sh labs/lab03/batch_profile.py
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCE = sys.argv[1] if len(sys.argv) > 1 else "data/raw/claims.csv"

spark = SparkSession.builder.appName("lab03-batch-profile").config("spark.sql.shuffle.partitions", "4").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Batch ingestion: read the whole file at once. Every column is read as text; Lab 4 sets proper types.
claims = spark.read.option("header", True).csv(SOURCE)

print("\n=== 1. Schema (the columns Spark found) ===")
claims.printSchema()

total = claims.count()
print("=== 2. Record count ===")
print(f"Total records: {total:,}\n")

print("=== 3. Claims by method of payment ===")
claims.groupBy("method_of_payment").count().orderBy(F.desc("count")).show(truncate=False)

print("=== 4. Top 5 drug classes ===")
claims.groupBy("major_class").count().orderBy(F.desc("count")).show(5, truncate=False)

missing_icd = claims.filter(F.col("icd10_code").isNull() | (F.trim("icd10_code") == "")).count()
dupes = total - claims.select("claim_id").distinct().count()
print("=== 5. A first look at data quality (you fix these in Lab 4) ===")
print(f"Records missing icd10_code: {missing_icd}")
print(f"Duplicate claim_id records: {dupes}\n")

spark.stop()
