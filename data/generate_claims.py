"""Generate a synthetic claims dataset in the IQVIA NMTA format for the CSC 894 labs.

Everything here is made up. No row describes a real person, provider, or claim.
The data contains a fixed number of planted data-quality problems so every student gets the same counts:

    1,000 unique claims
    +  25 exact duplicate claims          -> 1,025 records in total
       20 records missing icd10_code
       10 records missing patient_id
        5 records with an impossible month (13/45/2020)
       15 records with the month in YYYY-MM-DD instead of MM/DD/YYYY (fixable, not an error)

Run inside the Spark container from the repository folder:
    python3 data/generate_claims.py
Outputs (in data/raw/):
    claims.csv              all 1,025 records, for batch ingestion
    claims.jsonl            the same records, one JSON object per line, for publishing to Kafka
    claims_sample20.jsonl   20 clean records for the Lab 3 streaming demo
"""
import argparse
import csv
import json
import os
import random

FIELDS = ["claim_id", "patient_id", "patient_name", "address", "month", "method_of_payment",
          "patient_age_range", "patient_gender", "major_class", "minor_class", "acute_chronic",
          "icd10_3", "icd10_code", "product", "brand_generic", "specialty", "region", "state",
          "patient_visits"]

# (major class, minor class, product, brand/generic, ICD-10 level 3, ICD-10 code, acute/chronic)
THERAPIES = [
    ("ANTIDIABETICS", "BIGUANIDES", "METFORMIN HCL", "GENERIC", "E11 - TYPE 2 DIABETES MELLITUS", "E119", "CHRONIC"),
    ("ANTIDIABETICS", "SGLT2 INHIBITORS", "EMPAGLIFLOZIN", "BRANDED", "E11 - TYPE 2 DIABETES MELLITUS", "E1165", "CHRONIC"),
    ("ANTIDIABETICS", "GLP-1 RECEPTOR AGONISTS", "SEMAGLUTIDE", "BRANDED", "E11 - TYPE 2 DIABETES MELLITUS", "E1122", "CHRONIC"),
    ("ANTITHROMBOTICS", "DIRECT FACTOR XA INHIBS", "APIXABAN", "BRANDED", "I48 - ATRIAL FIBRILLATION AND FLUTTER", "I4891", "CHRONIC"),
    ("ANTITHROMBOTICS", "ANTICOAGULANTS", "HEPARIN LOCK FLUSH", "GENERIC", "I26 - PULMONARY EMBOLISM", "I2699", "ACUTE"),
    ("ANTIHYPERTENSIVES", "ACE INHIBITORS", "LISINOPRIL", "GENERIC", "I10 - ESSENTIAL (PRIMARY) HYPERTENSION", "I10", "CHRONIC"),
    ("ANTIHYPERTENSIVES", "CALCIUM CHANNEL BLOCKERS", "AMLODIPINE BESYLATE", "GENERIC", "I10 - ESSENTIAL (PRIMARY) HYPERTENSION", "I10", "CHRONIC"),
    ("LIPID REGULATORS", "STATINS", "ATORVASTATIN CALCIUM", "GENERIC", "E78 - DISORDERS OF LIPOPROTEIN METABOLISM", "E785", "CHRONIC"),
    ("BRONCHODILATORS", "BETA AGONISTS", "ALBUTEROL SULFATE", "BRANDED GENERIC", "J45 - ASTHMA", "J45909", "CHRONIC"),
    ("ANTIBIOTICS", "PENICILLINS", "AMOXICILLIN", "GENERIC", "J02 - ACUTE PHARYNGITIS", "J029", "ACUTE"),
    ("ANALGESICS", "NSAIDS", "IBUPROFEN", "GENERIC", "M54 - DORSALGIA", "M545", "ACUTE"),
    ("ANTIDEPRESSANTS", "SSRIS", "SERTRALINE HCL", "GENERIC", "F32 - DEPRESSIVE EPISODE", "F329", "CHRONIC"),
]
PAYERS = [("COMMERCIAL", 45), ("MEDICARE", 30), ("FFS MEDICAID", 10), ("MANAGED MEDICAID", 10), ("CASH", 5)]
AGES = [("0 TO 17", 8), ("18 TO 39", 22), ("40 TO 59", 32), ("60 TO 64", 13), ("65+", 25)]
GENDERS = [("FEMALE", 52), ("MALE", 46), ("UNSPECIFIED", 2)]
SPECIALTIES = ["FAMILY MEDICINE", "INTERNAL MEDICINE", "CARDIOLOGY", "ENDOCRINOLOGY", "PULMONARY DISEASE",
               "PSYCHIATRY", "EMERGENCY MEDICINE", "NURSE PRACTITIONER"]
STATES = {
    "NORTHEAST": ["PENNSYLVANIA", "NEW YORK", "NEW JERSEY", "MASSACHUSETTS", "CONNECTICUT"],
    "MIDWEST": ["OHIO", "ILLINOIS", "MICHIGAN", "WISCONSIN", "MINNESOTA"],
    "SOUTH": ["TEXAS", "FLORIDA", "GEORGIA", "NORTH CAROLINA", "VIRGINIA"],
    "WEST": ["CALIFORNIA", "WASHINGTON", "ARIZONA", "COLORADO", "OREGON"],
}
FIRST = ["AVERY", "JORDAN", "TAYLOR", "MORGAN", "CASEY", "RILEY", "QUINN", "SKYLER", "ROWAN", "EMERSON",
         "HARPER", "LOGAN", "PARKER", "REESE", "SAWYER", "DAKOTA"]
LAST = ["SAMPLE", "EXAMPLE", "TESTER", "DEMO", "PLACEHOLDER", "FICTION", "MOCKWELL", "SYNTHEX"]
STREETS = ["MAPLE", "OAK", "CEDAR", "PINE", "ELM", "BIRCH", "WILLOW", "ASPEN"]


def pick(rng, weighted):
    values, weights = zip(*weighted)
    return rng.choices(values, weights=weights, k=1)[0]


def make_claim(rng, n, patients):
    patient = patients[rng.randrange(len(patients))]
    therapy = THERAPIES[rng.randrange(len(THERAPIES))]
    region = rng.choice(list(STATES))
    year, month = rng.choice([2019, 2020, 2021]), rng.randint(1, 12)
    return {
        "claim_id": f"C{n:06d}",
        "patient_id": patient["patient_id"],
        "patient_name": patient["patient_name"],
        "address": patient["address"],
        "month": f"{month:02d}/01/{year}",
        "method_of_payment": pick(rng, PAYERS),
        "patient_age_range": patient["age"],
        "patient_gender": patient["gender"],
        "major_class": therapy[0], "minor_class": therapy[1], "product": therapy[2], "brand_generic": therapy[3],
        "icd10_3": therapy[4], "icd10_code": therapy[5], "acute_chronic": therapy[6],
        "specialty": rng.choice(SPECIALTIES), "region": region, "state": rng.choice(STATES[region]),
        "patient_visits": rng.randint(1, 45),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out-dir", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "raw"))
    ap.add_argument("--seed", type=int, default=894)
    args = ap.parse_args()
    rng = random.Random(args.seed)

    patients = [{
        "patient_id": f"P{100000 + i}",
        "patient_name": f"{rng.choice(FIRST)} {rng.choice(LAST)}",
        "address": f"{rng.randint(10, 9999)} {rng.choice(STREETS)} ST (SYNTHETIC)",
        "age": pick(rng, AGES), "gender": pick(rng, GENDERS),
    } for i in range(400)]
    claims = [make_claim(rng, n, patients) for n in range(1, 1001)]

    # Planted problems, each on its own set of rows so the counts stay exact.
    idx = list(range(len(claims)))
    rng.shuffle(idx)
    missing_icd, missing_pid, bad_month, iso_month, dup_src = (
        idx[0:20], idx[20:30], idx[30:35], idx[35:50], idx[50:75])
    for i in missing_icd:
        claims[i]["icd10_code"] = ""
    for i in missing_pid:
        claims[i]["patient_id"] = ""
    for i in bad_month:
        claims[i]["month"] = "13/45/2020"
    for i in iso_month:
        mm, dd, yyyy = claims[i]["month"].split("/")
        claims[i]["month"] = f"{yyyy}-{mm}-{dd}"

    # Each duplicate is an exact copy placed somewhere after its original, like a resent message.
    records = list(claims)
    for i in sorted(dup_src, reverse=True):
        copy = dict(claims[i])
        pos = records.index(claims[i])
        records.insert(rng.randint(pos + 1, len(records)), copy)

    os.makedirs(args.out_dir, exist_ok=True)
    csv_path = os.path.join(args.out_dir, "claims.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(records)
    jsonl_path = os.path.join(args.out_dir, "claims.jsonl")
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    bad = set(missing_icd) | set(missing_pid) | set(bad_month)
    sample = [claims[i] for i in range(len(claims)) if i not in bad][:20]
    sample_path = os.path.join(args.out_dir, "claims_sample20.jsonl")
    with open(sample_path, "w", encoding="utf-8") as f:
        for r in sample:
            f.write(json.dumps(r) + "\n")

    print("Synthetic claims generated (no real patient data).")
    print(f"  {len(records):,} records  -> data/raw/claims.csv and data/raw/claims.jsonl")
    print(f"  {len(sample)} clean records -> data/raw/claims_sample20.jsonl")
    print("  Planted problems: 25 duplicates, 20 missing icd10_code, 10 missing patient_id,")
    print("                    5 impossible months, 15 months in YYYY-MM-DD format")


if __name__ == "__main__":
    main()
