"""Lab 3, streaming ingestion: read claims events from Kafka with Spark Structured Streaming as they arrive.

Run from the repository folder (leave it running, then publish events from a second window):
    docker compose exec spark-master bash bin/submit.sh --kafka labs/lab03/stream_claims.py
Stop it with Ctrl + C.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

TOPIC = "iqvia_claims"
FIELDS = ["claim_id", "month", "method_of_payment", "major_class", "icd10_code", "product", "state"]
SCHEMA = StructType([StructField(name, StringType()) for name in FIELDS])

spark = SparkSession.builder.appName("lab03-stream-claims").config("spark.sql.shuffle.partitions", "4").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

events = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("subscribe", TOPIC)
          .option("startingOffsets", "latest")      # only events that arrive after the job starts
          .load())

# Each Kafka message is a JSON claim. Keep only non-identifying fields for display.
claims = (events
          .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("c"), "partition", "offset")
          .select("c.*", "partition", "offset"))

query = (claims.writeStream
         .format("console")
         .option("truncate", False)
         .option("numRows", 50)
         .trigger(processingTime="5 seconds")
         .start())

print(f"\nStreaming job started. Waiting for claims events on Kafka topic '{TOPIC}'...")
print("Publish events from a second PowerShell or Terminal window. Press Ctrl + C here to stop.\n", flush=True)
query.awaitTermination()
