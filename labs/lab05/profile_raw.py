"""Lab 5, Initial Discovery Phase: profile the raw IQVIA file in the HDFS data lake.

Run from the repository folder after loading the IQVIA file into HDFS (Lab 5, Part C):
    docker compose exec spark-master bash bin/submit.sh labs/lab05/profile_raw.py

It reports the schema, blank (null) rates for every field, value distributions, the date formats in use, ICD-10
codes that don't match the expected pattern, duplicate rows, and whether the visit breakdowns agree. It saves the
full profile to the HDFS profiles zone (datalake/profiles/iqvia_nmta) and ends with a discovery summary.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LAKE = os.environ.get("HEALTHRISKAI_LAKE", "hdfs://namenode:8020/datalake")
RAW = f"{LAKE}/raw/iqvia"
PROFILE = f"{LAKE}/profiles/iqvia_nmta"
SMALL_CELL = 11  # counts below 11 are commonly suppressed before results are published

spark = SparkSession.builder.appName("lab05-profile-raw").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = spark.read.option("header", True).csv(RAW).cache()
cols = raw.columns
desc = cols[:13]
group_start = {"specialty": 13, "region": cols.index("MIDWEST_Patient Visits"),
               "state": cols.index("ALABAMA_Patient Visits"), "yes_no": cols.index("YES_Patient Visits"),
               "setting": cols.index("HOSPITAL_Patient Visits")}
bounds = list(group_start.values()) + [len(cols)]
groups = {g: cols[a:b] for g, a, b in zip(group_start, bounds, bounds[1:])}
visit_cols = cols[13:]
metrics = []  # (section, item, value) rows saved to the profiles zone


def q(c):
    return F.col(f"`{c}`")


def blank(c):
    return q(c).isNull() | (F.trim(q(c)) == "")


def total(names):
    return sum(F.coalesce(q(c).cast("int"), F.lit(0)) for c in names)


print("\n=== 1. Schema ===")
print(f"{len(cols)} columns, all read as text: {len(desc)} describe each row, {len(visit_cols)} count patient visits.")
for g, names in groups.items():
    print(f"  {g:<10} {len(names):>3} visit-count columns")
repeated = [c for c in cols if c.startswith("OTHER_Patient Visits")]
print(f"The header repeats 'OTHER_Patient Visits'; Spark renamed the copies: {', '.join(repeated)}")

rows = raw.count()
with_totals = raw.select(*[q(c) for c in desc],
                         *[total(names).alias(f"visits_by_{g}") for g, names in groups.items()]).cache()
visits = with_totals.agg(F.sum("visits_by_specialty")).first()[0]
print("\n=== 2. Size ===")
print(f"Rows: {rows:,}   Patient visits: {visits:,}")
metrics += [("size", "rows", rows), ("size", "visits", visits), ("size", "columns", len(cols))]

print("\n=== 3. Blank (null) rates ===")
blanks = raw.agg(*[F.sum(F.when(blank(c), 1).otherwise(0)).alias(c) for c in cols]).first().asDict()
for c in cols:
    metrics.append(("blank_rate", c, round(blanks[c] / rows, 6)))
desc_blank = sum(blanks[c] for c in desc)
print(f"Describing columns: {desc_blank:,} blank values ({desc_blank / (rows * len(desc)):.1%})")
for g, names in groups.items():
    b = sum(blanks[c] for c in names)
    print(f"  {g:<10} visit cells blank: {b / (rows * len(names)):.1%}")
all_blank = sum(blanks[c] for c in visit_cols) / (rows * len(visit_cols))
print(f"All visit cells blank: {all_blank:.1%} (a blank cell means 0 visits)")
metrics.append(("blank_rate", "all_visit_cells", round(all_blank, 6)))

print("\n=== 4. Value distributions (rows and visits) ===")
for c in ["Method Of Payment", "Patient Gender", "Patient Age Range", "Acute/Chronic", "Brand/Generic",
          "Pharma Type", "Major Class"]:
    dist = (with_totals.groupBy(q(c).alias("value")).agg(F.count("*").alias("rows"),
                                                         F.sum("visits_by_specialty").alias("visits"))
            .orderBy(F.desc("visits")).collect())
    print(f"{c}: " + ", ".join(f"{r['value']} {r['visits']:,}" for r in dist[:6])
          + (f", +{len(dist) - 6} more" if len(dist) > 6 else ""))
    metrics += [("distribution", f"{c}={r['value']}", r["visits"]) for r in dist]

print("\n=== 5. Date formats in use (column Month) ===")
fmt = (raw.select(F.when(q("Month").rlike(r"^[A-Z][a-z]{2} [0-9]{4}$"), "Mon YYYY")
                  .when(q("Month").rlike(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"), "YYYY-MM-DD")
                  .when(q("Month").rlike(r"^[0-9]{2}/[0-9]{2}/[0-9]{4}$"), "MM/DD/YYYY")
                  .otherwise("other").alias("format"))
       .groupBy("format").count().collect())
for r in fmt:
    print(f"{r['format']}: {r['count']:,} rows")
    metrics.append(("date_format", r["format"], r["count"]))
months = raw.select(F.to_date(F.concat(F.lit("1 "), q("Month")), "d MMM yyyy").alias("m")).agg(
    F.min("m"), F.max("m"), F.countDistinct("m")).first()
print(f"Months: {months[2]} ({months[0]:%b %Y} to {months[1]:%b %Y})")
metrics.append(("date_format", "distinct_months", months[2]))

print("\n=== 6. ICD-10 codes that don't match the expected pattern ===")
code3 = F.split(q("ICD10 3"), " - ").getItem(0)
code5 = F.split(q("ICD10 5"), " - ").getItem(0)
bad = raw.filter(~code3.rlike(r"^[A-Z][0-9][0-9A-Z]$") | ~code5.rlike(r"^[A-Z][0-9][0-9A-Z]{3}$"))
bad_rows = bad.count()
print(f"Rows with an unexpected ICD-10 code: {bad_rows:,}")
for r in bad.groupBy(q("ICD10 5").alias("code")).count().orderBy(F.desc("count")).limit(5).collect():
    print(f"  {r['code']}: {r['count']:,} rows")
metrics.append(("icd10", "rows_not_matching_pattern", bad_rows))

print("\n=== 7. Duplicates and consistency ===")
dupes = rows - raw.select(*[q(c) for c in desc]).distinct().count()
disagree = with_totals.filter((F.col("visits_by_specialty") != F.col("visits_by_state"))
                              | (F.col("visits_by_specialty") != F.col("visits_by_setting"))
                              | (F.col("visits_by_specialty") != F.col("visits_by_region"))).count()
yes_no_disagree = with_totals.filter(F.col("visits_by_specialty") != F.col("visits_by_yes_no")).count()
unknown = raw.filter(q("Method Of Payment").isin("UNKNOWN MEDICAL", "UNSPECIFIED")
                     | (q("Patient Gender") == "UNSPECIFIED") | (q("Patient Age Range") == "UNSPECIFIED")).count()
small = with_totals.filter(F.col("visits_by_specialty") < SMALL_CELL).count()
print(f"Duplicate rows (same values in all 13 describing columns): {dupes:,}")
print(f"Rows whose specialty, region, state, and setting totals disagree: {disagree:,}")
print(f"Rows whose YES/NO totals disagree with the visit total: {yes_no_disagree:,} (meaning of YES/NO not documented)")
print(f"Rows with an unknown or unspecified payer, gender, or age: {unknown:,}")
print(f"Rows with fewer than {SMALL_CELL} visits (small counts): {small:,}")
metrics += [("quality", "duplicate_rows", dupes), ("quality", "breakdown_totals_disagree", disagree),
            ("quality", "unknown_payer_gender_or_age", unknown), ("quality", "rows_under_11_visits", small)]

# Save the profile to the HDFS profiles zone.
(spark.createDataFrame([(s, i, str(v)) for s, i, v in metrics], "section string, item string, value string")
      .coalesce(1).write.mode("overwrite").json(PROFILE))

summary = [
    ("Rows", f"{rows:,}"),
    ("Columns (13 describing, 123 visit counts)", f"{len(cols)}"),
    ("Patient visits", f"{visits:,}"),
    (f"Months ({months[0]:%b %Y} to {months[1]:%b %Y})", f"{months[2]}"),
    ("Date formats in use", f"{len(fmt)}"),
    ("Blank visit cells", f"{all_blank:.1%}"),
    ("Duplicate rows", f"{dupes:,}"),
    ("Rows whose breakdown totals disagree", f"{disagree:,}"),
    ("Rows with an ICD-10 code outside the pattern", f"{bad_rows:,}"),
    ("Rows with unknown payer, gender, or age", f"{unknown:,}"),
    (f"Rows with fewer than {SMALL_CELL} visits", f"{small:,}"),
]
print("\n=================== Lab 5 discovery summary ===================")
print(f"Source: {RAW}")
for label, value in summary:
    print(f"  {label:<46}{value:>14}")
print(f"Profile saved to {PROFILE}")
print("================================================================\n")
spark.stop()
