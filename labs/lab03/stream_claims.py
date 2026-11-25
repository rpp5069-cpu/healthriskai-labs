"""Lab 3, streaming ingestion: read IQVIA records from Kafka with Spark Structured Streaming as they arrive.

Run from the repository folder (leave it running, then publish records from a second window):
    docker compose exec spark-master bash bin/submit.sh --kafka labs/lab03/stream_claims.py
Stop it with Ctrl + C.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

TOPIC = "iqvia_nmta"
FIELDS = ["month", "method_of_payment", "patient_age_range", "patient_gender", "acute_chronic", "product",
          "icd10_code"]
SCHEMA = StructType([StructField(name, StringType()) for name in FIELDS]
                    + [StructField("total_visits", IntegerType())])

spark = SparkSession.builder.appName("lab03-stream-nmta").config("spark.sql.shuffle.partitions", "4").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

events = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("subscribe", TOPIC)
          .option("startingOffsets", "latest")      # only records that arrive after the job starts
          .load())

# Each Kafka message is one IQVIA row as JSON.
records = (events
           .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("r"), "partition", "offset")
           .select("r.*", "partition", "offset"))

query = (records.writeStream
         .format("console")
         .option("truncate", False)
         .option("numRows", 50)
         .trigger(processingTime="5 seconds")
         .start())

print(f"\nStreaming job started. Waiting for IQVIA records on Kafka topic '{TOPIC}'...")
print("Publish records from a second PowerShell or Terminal window. Press Ctrl + C here to stop.\n", flush=True)
query.awaitTermination()
