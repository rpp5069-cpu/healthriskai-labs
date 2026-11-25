#!/usr/bin/env bash
# Run a PySpark script on the course Spark cluster.
# Use it through Docker from the repository folder, for example:
#   docker compose exec spark-master bash bin/submit.sh labs/lab03/batch_profile.py
#   docker compose exec spark-master bash bin/submit.sh --kafka labs/lab03/stream_claims.py
# --kafka adds Spark's Kafka connector (downloaded once, the first time you use it).
set -euo pipefail

SPARK_VERSION=4.1.3
EXTRA=()
if [ "${1:-}" = "--kafka" ]; then
  EXTRA+=(--packages "org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VERSION}")
  shift
fi
if [ $# -lt 1 ]; then
  echo "Usage: bash bin/submit.sh [--kafka] path/to/script.py [arguments]" >&2
  exit 2
fi

exec /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/opt/ivy \
  --conf spark.ui.showConsoleProgress=false \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.sql.debug.maxToStringFields=200 \
  --driver-java-options "-Dlog4j2.configurationFile=file:/opt/healthriskai/bin/log4j2.properties" \
  "${EXTRA[@]}" "$@"
