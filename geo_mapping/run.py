"""
Full geo-enrichment pipeline. Run from the project root:

    .venv/bin/python geo_mapping/run.py

Steps:
  1. download_boundaries.py  — ABS SA2/SA3/LGA shapefiles
  2. map_institutions.py     — spatial join institutions → SA2/SA3/LGA
  3. download_abs_data.py    — Census GCP tables, SEIFA 2021, BOM climate
  4. enrich_with_abs.py      — derive metrics, join to institutions

Skip flags (re-run from a specific step):
  --from-step 3              — skip steps 1 & 2
"""

import subprocess
import sys
from pathlib import Path

SCRIPTS = [
    "download_boundaries.py",
    "map_institutions.py",
    "download_abs_data.py",
    "enrich_with_abs.py",
    "compute_vibe_metrics.py",
    "compute_student_metrics.py",
    "compute_pow_metrics.py",
    "compute_meme_metrics.py",
]

def main():
    start = 0
    if "--from-step" in sys.argv:
        idx = sys.argv.index("--from-step")
        start = int(sys.argv[idx + 1]) - 1

    base = Path(__file__).parent
    for i, script in enumerate(SCRIPTS):
        if i < start:
            print(f"Skipping step {i+1}: {script}")
            continue
        print(f"\n{'='*60}")
        print(f"Step {i+1}: {script}")
        print("="*60)
        result = subprocess.run([sys.executable, str(base / script)], check=False)
        if result.returncode != 0:
            print(f"\nFailed at step {i+1}: {script}. Stopping.")
            sys.exit(result.returncode)

    print("\n All steps complete.")
    print(" Vibe output:    geo_mapping/output/institutions_vibe_metrics.csv")
    print(" Student output: geo_mapping/output/institutions_student_metrics.csv")
    print(" POW output:     geo_mapping/output/institutions_pow_metrics.csv")
    print(" Meme output:    geo_mapping/output/institutions_meme_metrics.csv")


if __name__ == "__main__":
    main()
