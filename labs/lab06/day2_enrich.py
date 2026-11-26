"""Lab 6, Day 2: enrich the clean data and load it into the curated zone.

Run from the repository folder after Day 1:
    docker compose exec spark-master bash bin/submit.sh labs/lab06/day2_enrich.py

Writes two curated datasets to HDFS:
  curated/nmta_visits             the clean rows with derived fields, partitioned by service month
  curated/monthly_product_features one row per product and month, with the features a forecasting model needs
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

import nmta

started = time.time()
spark = SparkSession.builder.appName("lab06-day2-enrich").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

staged = spark.read.parquet(f"{nmta.STAGING}/nmta_clean")
rows_in = staged.count()
visits_in = staged.agg(F.sum("total_visits")).first()[0]

# ---------- Derived fields on every row ----------
payer_group = (F.when(F.col("method_of_payment").contains("MEDICAID"), "MEDICAID")
                .when(F.col("method_of_payment").isin("COMMERCIAL", "MEDICARE"), F.col("method_of_payment"))
                .otherwise("UNKNOWN"))
visits = (staged
          .withColumn("service_year", F.year("service_month"))
          .withColumn("service_quarter", F.quarter("service_month"))
          .withColumn("payer_group", payer_group)
          .withColumn("is_chronic", F.col("acute_chronic") == "CHRONIC")
          .withColumn("telehealth_share", F.round(F.try_divide("setting_telehealth", "total_visits"), 4))
          .withColumn("hospital_share", F.round(F.try_divide("setting_hospital", "total_visits"), 4))
          .withColumn("small_count", F.col("total_visits") < nmta.SMALL_COUNT))
keep = [c for c in visits.columns if not c.startswith(("spec_", "state_"))]  # breakdowns stay in staging
visits_out = f"{nmta.CURATED}/nmta_visits"
visits.select(*keep).repartition("service_month").write.mode("overwrite").partitionBy("service_month") \
      .parquet(visits_out)

# ---------- Monthly features per product (for the forecasting model) ----------
monthly = (visits.groupBy("product", "service_month")
           .agg(F.sum("total_visits").alias("visits"),
                F.sum(F.when(F.col("is_chronic"), F.col("total_visits")).otherwise(0)).alias("chronic_visits"),
                F.sum(F.when(F.col("payer_group") == "MEDICARE", F.col("total_visits")).otherwise(0))
                 .alias("medicare_visits"),
                F.sum("setting_telehealth").alias("telehealth_visits")))
# Every product gets every month, with 0 visits where it had none, so "last month" always means last month.
# Shares and changes are left empty (null) where they would divide by zero.
grid = (staged.select("product").distinct()
        .crossJoin(staged.select("service_month").distinct()))
by_month = Window.partitionBy("product").orderBy("service_month")
features = (grid.join(monthly, ["product", "service_month"], "left").fillna(0)
            .withColumn("visits_lag_1", F.lag("visits", 1).over(by_month))
            .withColumn("visits_lag_12", F.lag("visits", 12).over(by_month))
            .withColumn("visits_avg_prev_3", F.round(F.avg("visits").over(by_month.rowsBetween(-3, -1)), 1))
            .withColumn("yoy_change_pct", F.round(F.try_divide((F.col("visits") - F.col("visits_lag_12")) * 100.0,
                                                               F.col("visits_lag_12")), 1))
            .withColumn("chronic_share", F.round(F.try_divide("chronic_visits", "visits"), 4))
            .withColumn("medicare_share", F.round(F.try_divide("medicare_visits", "visits"), 4)))
features_out = f"{nmta.CURATED}/monthly_product_features"
features.coalesce(1).write.mode("overwrite").parquet(features_out)

# ---------- Report ----------
curated = spark.read.parquet(visits_out)
feat = spark.read.parquet(features_out)
rows_out = curated.count()
visits_out_total = curated.agg(F.sum("total_visits")).first()[0]
feat_rows = feat.count()
feat_visits = feat.agg(F.sum("visits")).first()[0]
products = feat.select("product").distinct().count()
months = feat.select("service_month").distinct().count()
partitions = curated.select("service_month").distinct().count()

print("\n=== Monthly features for ELIQUIS, last 6 months ===")
(feat.filter(F.col("product") == "ELIQUIS").orderBy(F.desc("service_month")).limit(6)
     .select("service_month", "visits", "visits_lag_1", "visits_lag_12", "visits_avg_prev_3", "yoy_change_pct",
             "chronic_share", "medicare_share").orderBy("service_month").show(truncate=False))

ok1 = rows_out == rows_in and visits_out_total == visits_in
ok2 = feat_visits == visits_in
print("================ Lab 6 Day 2: enrichment ================")
print(f"  Clean rows read from staging             {rows_in:>10,}   ({visits_in:,} visits)")
print(f"  Curated visit rows (nmta_visits)         {rows_out:>10,}   in {partitions} service-month folders")
print(f"  Monthly product features                 {feat_rows:>10,}   ({products} products x {months} months)")
print("---------------------------------------------------------")
print(f"Check 1  curated visits = staging visits ({visits_out_total:,} = {visits_in:,}): {'PASS' if ok1 else 'FAIL'}")
print(f"Check 2  feature visits = staging visits ({feat_visits:,} = {visits_in:,}): {'PASS' if ok2 else 'FAIL'}")
print("=========================================================\n")

nmta.log_lineage("day2_enrich", [f"{nmta.STAGING}/nmta_clean"], [visits_out, features_out], rows_in, rows_out, 0,
                 started, feature_rows=feat_rows)
spark.stop()
if not (ok1 and ok2):
    raise SystemExit(1)
