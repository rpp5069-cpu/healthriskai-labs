"""Lab 4: look inside the curated Parquet output and the dead-letter zone.

Run from the repository folder after etl_claims.py:
    docker compose exec spark-master bash bin/submit.sh labs/lab04/inspect_output.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("lab04-inspect-output").config("spark.sql.shuffle.partitions", "4").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

curated = spark.read.parquet("data/curated/claims")
print("\n=== Curated claims: 5 records (patient fields are masked) ===")
curated.select("claim_id", "patient_id", "patient_name", "address", "service_month", "icd10_code", "product") \
       .orderBy("claim_id").show(5, truncate=False)

print("=== Curated schema (note the real date and number types) ===")
curated.printSchema()

dlq = spark.read.json("data/dlq/claims")
print("=== Dead-letter zone: 5 records (claim ID and reason only, no patient details) ===")
dlq.orderBy("source_offset").show(5, truncate=False)

print("=== Dead-letter records by reason ===")
dlq.groupBy("reason").count().orderBy(F.desc("count")).show(truncate=False)
spark.stop()
