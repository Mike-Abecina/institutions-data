"""
Aggregate course data at institution level using batched database queries.

Processes all active courses in batches of 20,000 rows, building institution-level
metrics around course offerings, careers, fees, ATAR entry requirements, and more.

Usage:
    python acir_db/aggregations/aggregate_courses.py

Output:
    acir_db/aggregations/output/institution_course_aggregates.csv

Aggregations
------------
Course structure:   total_courses, course_level distribution %, faculty_count
Fees:               avg/min/max domestic_full_fee, free_tafe_count
International:      avg_ielts_requirement, pct_courses_with_ielts
Entry requirements: atar_min/max/median, courses_with_atar, pct_with_alternate_entry
Careers:            career_diversity, top_5_careers, top_anzsco_group,
                    stem_career_pct, health_career_pct, education_career_pct,
                    business_finance_career_pct
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Allow running from repo root or this directory
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from acir_db.get_acir_data import get_data

BATCH_SIZE = 20_000
SQL_DIR    = Path(__file__).parent.parent / "sql"
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_FILE = OUTPUT_DIR / "institution_course_aggregates.csv"

# ANZSCO major group labels (first digit of 6-digit ANZSCO code)
ANZSCO_GROUPS = {
    "1": "Managers",
    "2": "Professionals",
    "3": "Technicians & Trades",
    "4": "Community & Personal Service",
    "5": "Clerical & Administrative",
    "6": "Sales Workers",
    "7": "Machine Operators & Drivers",
    "8": "Labourers",
}

# Career domain definitions using 2-digit ANZSCO sub-major group prefixes.
# ANZSCO structure: digit 1 = major, digits 1-2 = sub-major, digits 1-3 = minor.
# Each entry maps output column → tuple of 2-digit sub-major codes to match.
CAREER_DOMAINS = {
    "stem_career_pct": (
        "23",   # Design, Engineering, Science & Transport Professionals
        "26",   # ICT Professionals
        "31",   # Engineering, ICT & Science Technicians
    ),
    "health_career_pct": (
        "25",   # Health Professionals (doctors, nurses, pharmacists…)
        "41",   # Health & Welfare Support Workers
        "42",   # Carers & Aides
    ),
    "education_career_pct": (
        "24",   # Education Professionals (teachers, lecturers, trainers)
    ),
    "business_finance_career_pct": (
        "13",   # Specialist Managers (finance, HR, marketing managers)
        "22",   # Business, HR & Marketing Professionals
        "55",   # Numerical Clerks (accountants, bookkeepers)
    ),
}

# Careers query: fetches all careers for a given set of course_ids.
# The {placeholders} token is replaced with a comma-separated list of ints.
CAREERS_SQL = """
SELECT
    cc.course_id,
    c.id    AS career_id,
    c.name  AS career_name,
    c.anzsco
FROM course_career cc
    INNER JOIN careers c ON cc.career_id = c.id
WHERE c.deleted_at IS NULL
  AND cc.course_id IN ({placeholders})
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def categorize_level(name):
    """Map course_level_name string → broad category via keyword match."""
    if pd.isna(name):
        return "unknown"
    n = str(name).lower()
    if "undergraduate" in n:
        return "undergraduate"
    if "postgraduate" in n:
        return "postgraduate"
    if any(x in n for x in ["trade", "apprentice", "traineeship", "vocational",
                              "skilled", "semi-skilled", "para-professional",
                              "additional job"]):
        return "vet_vocational"
    if any(x in n for x in ["secondary", "k12", "preparation"]):
        return "secondary"
    return "other"


def anzsco_major_group(code):
    """Map ANZSCO code → major group label (first digit)."""
    if not code or pd.isna(code):
        return None
    return ANZSCO_GROUPS.get(str(code).strip()[0])


def anzsco_matches(code, prefixes):
    """Return True if the ANZSCO code starts with any of the given 2-digit prefixes."""
    if not code or pd.isna(code):
        return False
    s = str(code).strip()
    return any(s.startswith(p) for p in prefixes)


# ── Per-institution aggregation ───────────────────────────────────────────────

def aggregate_institution(org_courses: pd.DataFrame, org_careers: pd.DataFrame) -> dict:
    """Compute all institution-level aggregates from accumulated course + career rows."""
    agg = {
        "organisation_id":   org_courses["organisation_id"].iloc[0],
        "organisation_name": org_courses["organisation_name"].iloc[0],
    }

    # One row per course (drop duplicate course_site rows for the same course)
    courses = org_courses.drop_duplicates(subset=["course_id"]).copy()

    # ── Course counts ─────────────────────────────────────────────────────────
    agg["total_courses"] = len(courses)

    # ── Course level distribution ─────────────────────────────────────────────
    courses["level_cat"] = courses["course_level_name"].apply(categorize_level)
    level_counts = courses["level_cat"].value_counts(normalize=True) * 100
    for key in ["undergraduate", "postgraduate", "vet_vocational", "secondary"]:
        agg[f"pct_{key}"] = round(float(level_counts.get(key, 0.0)), 1)

    # ── Faculty breadth ───────────────────────────────────────────────────────
    agg["faculty_count"] = int(courses["faculty_name"].nunique())

    # ── Fees ──────────────────────────────────────────────────────────────────
    fees = pd.to_numeric(courses["domestic_full_fee"], errors="coerce").dropna()
    agg["avg_domestic_fee"] = round(float(fees.mean()), 0) if not fees.empty else None
    agg["min_domestic_fee"] = float(fees.min()) if not fees.empty else None
    agg["max_domestic_fee"] = float(fees.max()) if not fees.empty else None
    agg["free_tafe_count"]  = int(courses["free_tafe_course"].fillna(0).astype(int).sum())

    # ── International language requirements ───────────────────────────────────
    ielts = pd.to_numeric(courses["overall_ielts_score"], errors="coerce").dropna()
    agg["avg_ielts_requirement"]  = round(float(ielts.mean()), 2) if not ielts.empty else None
    agg["pct_courses_with_ielts"] = round(float(courses["overall_ielts_score"].notna().mean() * 100), 1)

    # ── ATAR entry requirements (from course_site_atar table via SQL join) ─────
    # atar_value is a raw varchar; extract the leading number, validate range.
    atar_raw = courses["atar_value"].dropna().astype(str)
    atar_vals = pd.to_numeric(
        atar_raw.str.extract(r'(\d{2,3}(?:\.\d{1,2})?)')[0],
        errors="coerce",
    ).dropna()
    atar_vals = atar_vals[atar_vals.between(30, 99.95)]

    agg["courses_with_atar"] = int(atar_vals.count())
    agg["atar_min"]    = round(float(atar_vals.min()),    2) if not atar_vals.empty else None
    agg["atar_max"]    = round(float(atar_vals.max()),    2) if not atar_vals.empty else None
    agg["atar_median"] = round(float(atar_vals.median()), 2) if not atar_vals.empty else None

    # ── Alternate entry pathways ──────────────────────────────────────────────
    has_alt = (
        courses["course_alternate_entry_requirements"].notna()
        & (courses["course_alternate_entry_requirements"].astype(str).str.strip() != "")
    )
    agg["pct_with_alternate_entry"] = round(float(has_alt.mean() * 100), 1)

    # ── Careers ───────────────────────────────────────────────────────────────
    if org_careers is not None and not org_careers.empty:
        agg["career_diversity"] = int(org_careers["career_id"].nunique())

        top5 = (
            org_careers.groupby("career_name")["course_id"]
            .count()
            .sort_values(ascending=False)
            .head(5)
        )
        agg["top_5_careers"] = " | ".join(top5.index.tolist())

        org_careers = org_careers.copy()
        org_careers["anzsco_major"] = org_careers["anzsco"].apply(anzsco_major_group)
        group_dist = (
            org_careers["anzsco_major"].dropna()
            .value_counts(normalize=True) * 100
        )
        agg["top_anzsco_group"]     = group_dist.index[0] if not group_dist.empty else None
        agg["top_anzsco_group_pct"] = round(float(group_dist.iloc[0]), 1) if not group_dist.empty else None

        # Domain percentages: % of career links matching each 2-digit ANZSCO sub-major group
        n = len(org_careers)
        for col, prefixes in CAREER_DOMAINS.items():
            mask = org_careers["anzsco"].apply(lambda x, p=prefixes: anzsco_matches(x, p))
            agg[col] = round(float(mask.sum() / n * 100), 1)
    else:
        agg["career_diversity"]     = 0
        agg["top_5_careers"]        = None
        agg["top_anzsco_group"]     = None
        agg["top_anzsco_group_pct"] = None
        for col in CAREER_DOMAINS:
            agg[col] = None

    return agg


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-batches", type=int, default=None,
                        help="Stop after this many batches (omit to process all)")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── Step 1: get exact batch count ─────────────────────────────────────────
    print("Counting active courses...")
    count_sql = (SQL_DIR / "courses_count.sql").read_text()
    count_df  = get_data(count_sql)
    if count_df is None or count_df.empty:
        print("ERROR: Could not connect to database or count query returned nothing.")
        sys.exit(1)

    total_courses = int(count_df["total_courses"].iloc[0])
    n_batches     = (total_courses + BATCH_SIZE - 1) // BATCH_SIZE
    if args.max_batches:
        n_batches = min(n_batches, args.max_batches)
    print(f"  {total_courses:,} active courses  →  processing {n_batches} of "
          f"{(total_courses + BATCH_SIZE - 1) // BATCH_SIZE} batches  ({BATCH_SIZE:,} rows each)")

    # ── Step 2: load batch SQL template ───────────────────────────────────────
    batch_template = (SQL_DIR / "courses_batch.sql").read_text()

    # ── Step 3: batch loop ────────────────────────────────────────────────────
    # Accumulate raw DataFrames per organisation across all batches.
    # We don't aggregate inside the loop so that courses split across batch
    # boundaries (due to multi-site rows) are correctly combined.
    institution_courses: dict[int, list] = {}
    institution_careers: dict[int, list] = {}

    for batch_num in range(n_batches):
        offset = batch_num * BATCH_SIZE
        print(f"\nBatch {batch_num + 1}/{n_batches}  (rows {offset:,}–{offset + BATCH_SIZE:,})...")

        courses_df = get_data(batch_template.format(limit=BATCH_SIZE, offset=offset))
        if courses_df is None or courses_df.empty:
            print("  (empty — skipping)")
            continue

        unique_courses = courses_df["course_id"].nunique()
        print(f"  {len(courses_df):,} rows  |  {unique_courses:,} unique courses  |  "
              f"{courses_df['organisation_id'].nunique():,} organisations")

        # Fetch careers for this batch's course IDs
        course_ids   = courses_df["course_id"].unique().tolist()
        placeholders = ",".join(str(i) for i in course_ids)
        careers_df   = get_data(CAREERS_SQL.format(placeholders=placeholders))
        print(f"  {len(careers_df) if careers_df is not None else 0:,} career links")

        # Accumulate per organisation
        for org_id, grp in courses_df.groupby("organisation_id"):
            institution_courses.setdefault(org_id, []).append(grp)
            if careers_df is not None and not careers_df.empty:
                org_course_ids = grp["course_id"].unique()
                org_careers    = careers_df[careers_df["course_id"].isin(org_course_ids)]
                if not org_careers.empty:
                    institution_careers.setdefault(org_id, []).append(org_careers)

    # ── Step 4: aggregate per institution ─────────────────────────────────────
    print(f"\nAggregating {len(institution_courses):,} institutions...")
    records = []
    for org_id, course_parts in institution_courses.items():
        all_courses = pd.concat(course_parts, ignore_index=True)
        career_parts = institution_careers.get(org_id, [])
        all_careers  = pd.concat(career_parts, ignore_index=True) if career_parts else pd.DataFrame()
        records.append(aggregate_institution(all_courses, all_careers))

    result = pd.DataFrame(records).sort_values("organisation_name").reset_index(drop=True)
    result.to_csv(OUTPUT_FILE, index=False)

    # ── Step 5: coverage/stats summary ────────────────────────────────────────
    numeric_cols = [
        "total_courses", "pct_undergraduate", "pct_postgraduate", "pct_vet_vocational", "pct_secondary",
        "faculty_count", "avg_domestic_fee", "free_tafe_count",
        "avg_ielts_requirement", "pct_courses_with_ielts",
        "atar_median", "courses_with_atar", "pct_with_alternate_entry",
        "career_diversity",
        *CAREER_DOMAINS.keys(),
    ]
    print(f"\n{'Metric':<35} {'Cover':>7}  {'Mean':>10}  {'Min':>8}  {'Max':>8}")
    print("-" * 75)
    for col in numeric_cols:
        if col in result.columns:
            s   = pd.to_numeric(result[col], errors="coerce").dropna()
            cov = result[col].notna().mean() * 100
            print(f"  {col:<33} {cov:>5.1f}%  {s.mean():>10.1f}  {s.min():>8.1f}  {s.max():>8.1f}")

    print(f"\nOutput  : {OUTPUT_FILE}")
    print(f"Rows    : {len(result):,} institutions")
    print(f"Columns : {len(result.columns)}")


if __name__ == "__main__":
    main()
