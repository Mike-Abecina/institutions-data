"""
cache_acir_data.py
==================
Run this locally (where you have RDS access / VPN) to pre-bake the ACIR
institutions query into a CSV for production deployment.

The production app reads deploy/data/acir_institutions.csv instead of
hitting MySQL live. Commit the CSV to the repo after running this script.

Usage:
    python deploy/cache_acir_data.py

Output:
    deploy/data/acir_institutions.csv
"""

import os
import sys
from pathlib import Path

# Allow imports from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from acir_db.get_acir_data import get_data_from_file

load_dotenv()

ROOT     = Path(__file__).parent.parent
SQL_PATH = ROOT / "acir_db" / "sql" / "organisations.sql"
OUT_DIR  = Path(__file__).parent / "data"
OUT_PATH = OUT_DIR / "acir_institutions.csv"


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Running query: {SQL_PATH.relative_to(ROOT)}")
    df = get_data_from_file(str(SQL_PATH))

    if df is None or df.empty:
        print(
            "ERROR: No data returned.\n"
            "  • Check your .env has valid DB credentials.\n"
            "  • Make sure you're on the VPN / have RDS network access.\n"
            f"  • DB host expected: {os.getenv('DATABSE_HOST', '<not set>')}"
        )
        sys.exit(1)

    print(f"  {len(df):,} rows, {df.shape[1]} columns")
    df.to_csv(OUT_PATH, index=False)
    print(f"  Written → {OUT_PATH.relative_to(ROOT)}")
    print()
    print("Next steps:")
    print("  1. Run: python deploy/security_check.py")
    print("  2. git add deploy/data/acir_institutions.csv")
    print("  3. git commit -m 'Refresh cached ACIR institutions data'")


if __name__ == "__main__":
    main()
