"""Shared Sprint 1 transformations for the IQVIA NMTA data (Lab 6).

Every Lab 6 job imports this module, so the batch job, the streaming job, and the tests all apply exactly the same
rules. Nothing here needs editing for the lab.
"""
import datetime
import json
import os
import re
import time

from pyspark.sql import Window
from pyspark.sql import functions as F

LAKE = os.environ.get("HEALTHRISKAI_LAKE", "hdfs://namenode:8020/datalake")
RAW = f"{LAKE}/raw/iqvia"
STAGING = f"{LAKE}/staging"
CURATED = f"{LAKE}/curated"
DEAD_LETTER = f"{LAKE}/dead-letter"
LINEAGE_LOG = os.environ.get("HEALTHRISKAI_LINEAGE", "data/lineage/lineage.jsonl")

DESCRIBING = ["month", "method_of_payment", "patient_age_range", "patient_gender", "major_class", "minor_class",
              "pharma_type", "acute_chronic", "icd10_3", "icd10_4", "icd10_5", "product", "brand_generic"]
REQUIRED = ["month", "method_of_payment", "minor_class", "icd10_5", "product"]
REASONS = ["missing_required_field", "invalid_month", "invalid_visit_count", "no_visits", "inconsistent_totals",
           "invalid_icd10_code", "duplicate_row"]
UNKNOWN_VALUES = {"method_of_payment": ["UNKNOWN MEDICAL", "UNSPECIFIED"], "patient_gender": ["UNSPECIFIED"],
                  "patient_age_range": ["UNSPECIFIED"]}
# Names of fields that would hold patient identifiers. None should ever appear from Lab 5 on.
IDENTIFIER_FIELDS = re.compile(r"(patient_id|patient_name|ssn|social_security|address|birth|dob|phone|email|mrn)")
SMALL_COUNT = 11


def snake(text):
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


# ---------- Reading ----------
def rename(raw):
    """Give the 136 IQVIA columns short, unique names. The visit columns get a prefix for their breakdown, which also
    separates the three columns the header calls 'OTHER_Patient Visits'."""
    cols = raw.columns
    starts = [("spec", 13), ("region", cols.index("MIDWEST_Patient Visits")),
              ("state", cols.index("ALABAMA_Patient Visits")), ("yesno", cols.index("YES_Patient Visits")),
              ("setting", cols.index("HOSPITAL_Patient Visits")), (None, len(cols))]
    names = list(DESCRIBING)
    for (prefix, a), (_, b) in zip(starts, starts[1:]):
        names += [f"{prefix}_{snake(re.sub(r'_Patient Visits[0-9]*$', '', c))}" for c in cols[a:b]]
    return raw.toDF(*names)


def read_raw(spark, path=RAW):
    """Read the raw IQVIA file(s) exactly as they arrived (all text), with short column names and a row number."""
    raw = spark.read.option("header", True).csv(path)
    return rename(raw).withColumn("source_row", F.monotonically_increasing_id())


def visit_columns(df, prefix):
    return [c for c in df.columns if c.startswith(prefix + "_")]


# ---------- Cleaning ----------
def clean(df):
    """Validate, normalize, and deduplicate renamed IQVIA rows. Returns (clean, rejected). Expects a source_row.

    Clean rows keep the describing fields, the total visits, and the visits by care setting. The specialty and state
    breakdowns stay in the raw zone."""
    groups = {p: visit_columns(df, p) for p in ["spec", "region", "state", "setting"]}

    def is_blank(c):
        return F.col(c).isNull() | (F.trim(F.col(c)) == "")

    # Each breakdown becomes one array of numbers: blank means 0 visits; anything that isn't a whole number becomes
    # empty (null) and is caught below. One array per breakdown keeps the Spark plan small, even with 123 columns.
    arrays = {f"_{g}": F.expr("transform(array(" + ", ".join(f"`{c}`" for c in cols) + "), x -> CASE WHEN x IS NULL "
                              "OR trim(x) = '' THEN 0 ELSE try_cast(trim(x) AS INT) END)")
              for g, cols in groups.items()}
    bad_count = sum(F.expr(f"size(filter(_{g}, x -> x IS NULL OR x < 0))") for g in groups)

    def total(g):
        return F.expr(f"aggregate(_{g}, 0, (acc, x) -> acc + coalesce(x, 0))")

    service_month = F.coalesce(F.expr("try_to_timestamp(concat('1 ', trim(month)), 'd MMM yyyy')"),
                               F.expr("try_to_timestamp(trim(month), 'yyyy-MM-dd')")).cast("date")
    code = F.upper(F.trim(F.split(F.col("icd10_5"), " - ").getItem(0)))
    restricted = code.startswith("-1")

    checked = (df.withColumns(arrays)
               .withColumn("service_month", service_month)
               .withColumn("bad_counts", bad_count)
               .withColumn("total_visits", total("spec"))
               .withColumn("_by_region", total("region"))
               .withColumn("_by_state", total("state"))
               .withColumn("_by_setting", total("setting"))
               .withColumn("diagnosis_restricted", restricted)
               .withColumn("reason",
                           F.when(F.greatest(*[is_blank(c).cast("int") for c in REQUIRED]) == 1,
                                  "missing_required_field")
                            .when(F.col("service_month").isNull(), "invalid_month")
                            .when(F.col("bad_counts") > 0, "invalid_visit_count")
                            .when(F.col("total_visits") == 0, "no_visits")
                            .when((F.col("_by_state") != F.col("total_visits"))
                                  | (F.col("_by_setting") != F.col("total_visits"))
                                  | (F.col("_by_region") != F.col("total_visits")), "inconsistent_totals")
                            .when(~restricted & ~code.rlike(r"^[A-Z][0-9][0-9A-Z]{3}$"), "invalid_icd10_code")))

    # The same row twice (all 13 describing values equal) is a duplicate: keep the first one received.
    first = Window.partitionBy(*DESCRIBING).orderBy("source_row")
    valid = (checked.filter(F.col("reason").isNull())
             .withColumn("_copy", F.row_number().over(first))
             .withColumn("reason", F.when(F.col("_copy") > 1, F.lit("duplicate_row"))))
    invalid = checked.filter(F.col("reason").isNotNull()).withColumn("_copy", F.lit(None).cast("int"))
    checked = valid.unionByName(invalid)

    def unknown(c):
        return F.when(F.col(c).isin(*UNKNOWN_VALUES[c]), F.lit("UNKNOWN")).otherwise(F.col(c))

    clean_rows = (checked.filter(F.col("reason").isNull())
                  .select(
                      "service_month",
                      unknown("method_of_payment").alias("method_of_payment"),
                      unknown("patient_age_range").alias("patient_age_range"),
                      unknown("patient_gender").alias("patient_gender"),
                      "major_class", "minor_class", "pharma_type", "acute_chronic",
                      F.when(restricted, "RESTRICTED").otherwise(F.split("icd10_3", " - ").getItem(0))
                       .alias("icd10_category"),
                      F.when(restricted, "RESTRICTED").otherwise(code).alias("icd10_code"),
                      F.trim(F.element_at(F.split("icd10_5", " - ", 2), -1)).alias("icd10_description"),
                      "diagnosis_restricted", "product", "brand_generic",
                      F.greatest(*[F.col(c).isin(*v).cast("int") for c, v in UNKNOWN_VALUES.items()])
                       .cast("boolean").alias("has_unknown_value"),
                      F.col("total_visits").cast("int").alias("total_visits"),
                      *[F.element_at("_setting", i + 1).alias(c) for i, c in enumerate(groups["setting"])]))
    rejected = (checked.filter(F.col("reason").isNotNull())
                .select("reason", "source_row", "month", "method_of_payment", "product", "icd10_5",
                        F.when(F.col("bad_counts") == 0, F.col("total_visits")).alias("total_visits")))
    return clean_rows, rejected


# ---------- Compliance ----------
def compliance_check(df):
    """Confirm the data holds no patient identifiers. Returns (identifier_fields, identifier_values, small_rows)."""
    fields = [c for c in df.columns if IDENTIFIER_FIELDS.search(c)]
    text = [c for c, t in df.dtypes if t == "string"]
    pattern = r"([0-9]{3}-[0-9]{2}-[0-9]{4})|(@[A-Za-z0-9.-]+\.[A-Za-z]{2,})|(\([0-9]{3}\) ?[0-9]{3}-[0-9]{4})"
    flags = [F.coalesce(F.col(c), F.lit("")).rlike(pattern).cast("int") for c in text]
    if not flags:
        values = df.limit(0)
    else:
        values = df.filter((F.greatest(*flags) if len(flags) > 1 else flags[0]) == 1)
    small = df.filter(F.col("total_visits") < SMALL_COUNT).count() if "total_visits" in df.columns else None
    return fields, values.count(), small


# ---------- Helpers ----------
def delete_path(spark, path):
    """Delete a folder in HDFS (or on disk) if it exists."""
    jvm = spark.sparkContext._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    p.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).delete(p, True)


def log_lineage(step, inputs, outputs, records_in, records_out, rejected, started, **extra):
    """Append one audit entry to the lineage log (shipped to Elasticsearch in the monitoring lab)."""
    entry = {"timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
             "lab": "lab06", "step": step, "inputs": inputs, "outputs": outputs, "records_in": records_in,
             "records_out": records_out, "records_rejected": rejected,
             "seconds": round(time.time() - started, 1), **extra}
    os.makedirs(os.path.dirname(LINEAGE_LOG), exist_ok=True)
    with open(LINEAGE_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")
