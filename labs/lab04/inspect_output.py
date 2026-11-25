"""Lab 4: look inside the curated Parquet output and the dead-letter zone.

Run from the repository folder after etl_claims.py:
    docker compose exec spark-master bash bin/submit.sh labs/lab04/inspect_output.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("lab04-inspect-output").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

curated = spark.read.parquet("data/curated/visits")
print("\n=== Curated visits: 5 records (patient fields are masked) ===")
curated.select("visit_id", "patient_id", "patient_name", "ssn", "address", "service_month", "icd10_code",
               "product").orderBy("visit_id").show(5, truncate=False)

print("=== Curated schema (note the real date type and the service_year folders) ===")
curated.printSchema()

print("=== Curated visits by year (one Parquet folder per year) ===")
curated.groupBy("service_year").count().orderBy("service_year").show(truncate=False)

dlq = spark.read.json("data/dlq/visits")
print("=== Dead-letter zone: 5 records (visit ID and reason only, no patient details) ===")
dlq.select("visit_id", "reason", "source").orderBy("visit_id").show(5, truncate=False)

print("=== Dead-letter records by reason ===")
dlq.groupBy("reason").count().orderBy(F.desc("count")).show(truncate=False)
spark.stop()
