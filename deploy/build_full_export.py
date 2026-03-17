"""
build_full_export.py
====================
Combines export_table.csv (metrics + pre-baked annotations) with ACIR
institution metadata from the database into a single self-contained CSV.

The result is the only file streamlit_app_v3.py needs — no database
connection required at runtime.

Run locally (with VPN / RDS access) before deploying.

Usage:
    python deploy/build_full_export.py

Inputs:
    acir_db/aggregations/output/export_table.csv   (metrics + _emoji/_band/_signal cols)
    MySQL: acir_db/sql/organisations.sql            (addresses, descriptions, study areas…)

Output:
    deploy/data/full_export.csv
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from dotenv import load_dotenv
from acir_db.get_acir_data import get_data_from_file

load_dotenv()

ROOT         = Path(__file__).parent.parent
EXPORT_TABLE = ROOT / "acir_db" / "aggregations" / "output" / "export_table.csv"
ORGS_SQL     = ROOT / "acir_db" / "sql" / "organisations.sql"
OUT_DIR      = Path(__file__).parent / "data"
OUT_PATH     = OUT_DIR / "full_export.csv"

# Truncate long free-text fields to keep file size reasonable.
# The AI prompt uses at most 500 chars of description anyway.
TEXT_TRUNCATE = {
    "organisation_description": 800,
    "site_study_area":          600,
    "site_transport":           400,
    "site_accommodation":       400,
    "site_comments":            400,
}


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── 1. Load export_table ──────────────────────────────────────────────────
    print("Loading export_table.csv…")
    export = pd.read_csv(EXPORT_TABLE, dtype={"organisation_id": str, "sa2_code": str})
    print(f"  {len(export):,} rows, {len(export.columns)} columns")

    # ── 2. Fetch ACIR metadata ────────────────────────────────────────────────
    print("Fetching ACIR institution metadata from DB…")
    acir_df = get_data_from_file(str(ORGS_SQL))
    if acir_df is None or acir_df.empty:
        print(
            "ERROR: No data returned from DB.\n"
            "  • Check your .env has valid DB credentials.\n"
            "  • Make sure you're on the VPN / have RDS network access."
        )
        sys.exit(1)

    acir_df["organisation_id"] = acir_df["organisation_id"].astype(str)
    print(f"  {len(acir_df):,} rows from organisations.sql")

    # ── 3. Deduplicate ACIR: keep primary site per institution ────────────────
    primary     = acir_df[acir_df["site_primary_site"] == 1]
    non_primary = acir_df[~acir_df["organisation_id"].isin(primary["organisation_id"])]
    acir_dedup  = (
        pd.concat([primary, non_primary])
        .drop_duplicates(subset=["organisation_id"], keep="first")
        .copy()
    )
    print(f"  {len(acir_dedup):,} institutions after deduplication")

    # ── 4. Truncate heavy text fields ─────────────────────────────────────────
    for col, limit in TEXT_TRUNCATE.items():
        if col in acir_dedup.columns:
            acir_dedup[col] = acir_dedup[col].astype(str).str[:limit].where(
                acir_dedup[col].notna(), other=None
            )

    # ── 5. Drop columns that are already in export_table or not needed ────────
    drop_cols = ["organisation_name", "site_primary_site"]
    acir_dedup = acir_dedup.drop(columns=drop_cols, errors="ignore")

    # ── 6. Merge ──────────────────────────────────────────────────────────────
    print("Merging…")
    result = export.merge(
        acir_dedup,
        on="organisation_id",
        how="left",
        suffixes=("", "_acir"),
    )

    # Drop any _acir suffix duplicates that snuck through
    dupe_cols = [c for c in result.columns if c.endswith("_acir")]
    if dupe_cols:
        result = result.drop(columns=dupe_cols)

    # ── 7. Write ──────────────────────────────────────────────────────────────
    result.to_csv(OUT_PATH, index=False)

    acir_new_cols = len(result.columns) - len(export.columns)
    matched       = result["organisation_web_address"].notna().sum() if "organisation_web_address" in result.columns else 0

    print(f"\nOutput  : {OUT_PATH.relative_to(ROOT)}")
    print(f"Rows    : {len(result):,}")
    print(f"Columns : {len(result.columns)}  ({len(export.columns)} from export_table + {acir_new_cols} ACIR cols)")
    print(f"ACIR match: {matched:,} / {len(result):,} institutions have a web address")

    size_mb = OUT_PATH.stat().st_size / 1_048_576
    print(f"File size: {size_mb:.1f} MB")
    if size_mb > 50:
        print("  ⚠️  File is over 50 MB — GitHub will warn. Consider reducing TEXT_TRUNCATE limits.")
    else:
        print("  ✅ File size is within GitHub limits")

    print()
    print("Next steps:")
    print("  1. python deploy/security_check.py")
    print("  2. git add deploy/data/full_export.csv")
    print("  3. git commit -m 'Refresh full_export.csv' && git push")


if __name__ == "__main__":
    main()
