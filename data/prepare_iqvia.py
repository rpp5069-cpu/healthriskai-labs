"""Check the IQVIA NMTA file and prepare a small sample of it for the Lab 3 streaming demo.

The IQVIA file comes from your instructor in Canvas. It is licensed data: keep it in data/iqvia/ on your own
computer and never commit or share it (the repository's .gitignore already keeps it out of Git).

Run inside the Spark container from the repository folder:
    docker compose exec spark-master python3 data/prepare_iqvia.py
Output:
    data/raw/iqvia_sample20.jsonl   20 IQVIA rows from January 2026, one JSON object per line, for Kafka
"""
import argparse
import csv
import glob
import json
import os
import sys

SAMPLE_MONTH = "Jan 2026"
FIELDS = {  # IQVIA column -> field name used in the labs
    "Month": "month", "Method Of Payment": "method_of_payment", "Patient Age Range": "patient_age_range",
    "Patient Gender": "patient_gender", "Acute/Chronic": "acute_chronic", "ICD10 5": "icd10_5",
    "Product Sum": "product",
}


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", help="the IQVIA CSV (default: the one .csv file in data/iqvia/)")
    ap.add_argument("--out", default=os.path.join(here, "raw", "iqvia_sample20.jsonl"))
    args = ap.parse_args()

    files = [args.input] if args.input else sorted(glob.glob(os.path.join(here, "iqvia", "*.csv")))
    if len(files) != 1:
        sys.exit("Put the IQVIA CSV file from Canvas in the data/iqvia folder (exactly one .csv file), "
                 f"then run this again. Found {len(files)} .csv files there.")

    rows, months, sample = 0, set(), []
    with open(files[0], newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        col = {name: i for i, name in reversed(list(enumerate(header)))}
        spec = range(13, col["MIDWEST_Patient Visits"])  # the specialty columns: one count per visit
        for r in reader:
            rows += 1
            months.add(r[0])
            if r[0] == SAMPLE_MONTH and len(sample) < 20:
                rec = {out: r[col[name]] for name, out in FIELDS.items()}
                code, _, _ = rec.pop("icd10_5").partition(" - ")
                rec["icd10_code"] = code
                rec["total_visits"] = sum(int(float(r[i])) for i in spec if r[i])
                sample.append(rec)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for rec in sample:
            f.write(json.dumps(rec) + "\n")
    print(f"IQVIA file found: data/iqvia/{os.path.basename(files[0])}")
    print(f"  {rows:,} rows covering {len(months)} months, {len(header)} columns")
    print(f"  {len(sample)} rows from {SAMPLE_MONTH} -> data/raw/iqvia_sample20.jsonl")


if __name__ == "__main__":
    main()
