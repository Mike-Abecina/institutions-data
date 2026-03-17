"""
Merge SA2 geo/census metrics with institution-level course aggregates
into one mega table keyed on organisation_id.

Inputs:
    geo_mapping/output/institutions_meme_metrics.csv   (4,102 rows, 122 cols)
    acir_db/aggregations/output/institution_course_aggregates.csv  (3,361 rows, 27 cols)

Output:
    acir_db/aggregations/output/mega_table.csv
"""

from pathlib import Path
import pandas as pd

GEO_FILE    = Path(__file__).parent.parent.parent / "geo_mapping" / "output" / "institutions_meme_metrics.csv"
COURSE_FILE = Path(__file__).parent / "output" / "institution_course_aggregates.csv"
OUTPUT_FILE = Path(__file__).parent / "output" / "mega_table.csv"


def main():
    geo    = pd.read_csv(GEO_FILE,    dtype={"organisation_id": str, "sa2_code": str})
    course = pd.read_csv(COURSE_FILE, dtype={"organisation_id": str})

    # Drop redundant name col from course side (geo has 'name', course has 'organisation_name')
    course = course.drop(columns=["organisation_name"], errors="ignore")

    result = geo.merge(course, on="organisation_id", how="left")

    result.to_csv(OUTPUT_FILE, index=False)

    matched = result["total_courses"].notna().sum()
    print(f"Institutions          : {len(result):,}")
    print(f"With course data      : {matched:,}  ({matched/len(result)*100:.1f}%)")
    print(f"Without course data   : {len(result) - matched:,}")
    print(f"Total columns         : {len(result.columns)}")
    print(f"Output                : {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
