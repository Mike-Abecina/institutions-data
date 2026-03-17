"""
Download ABS Census 2021 GCP DataPack (SA2), SEIFA 2021, and BOM climate data.
Safe to re-run — skips files already present.

Downloads into geo_mapping/abs_data/:
  census/   — 2021 GCP SA2 tables (G02, G04, G09, G63)
  seifa/    — SEIFA 2021 SA2 Excel
  bom/      — BOM climate CSVs per city
"""

import zipfile
import urllib.request
from pathlib import Path

ABS_DATA_DIR = Path(__file__).parent / "abs_data"

CENSUS_URL = "https://www.abs.gov.au/census/find-census-data/datapacks/download/2021_GCP_SA2_for_AUS_short-header.zip"
SEIFA_URL = (
    "https://www.abs.gov.au/statistics/people/people-and-communities/"
    "socio-economic-indexes-areas-seifa-australia/2021/"
    "Statistical%20Area%20Level%202%2C%20Indexes%2C%20SEIFA%202021.xlsx"
)

# Only the tables we need from the Census zip
# Vibe metrics: G02, G04, G09, G13A, G15, G23, G34, G36, G37, G44, G54A/B, G62
# Student metrics: G17A, G38, G43, G49A/B, G50A, G52A, G53A, G60A/B
CENSUS_TABLES = {
    "G02", "G04A", "G04B", "G09A", "G13A", "G15", "G23",
    "G34", "G36", "G37", "G44", "G54A", "G54B", "G57A", "G62",
    "G17A", "G38", "G43", "G49A", "G49B", "G50A", "G52A", "G53A", "G60A", "G60B",
}

BOM_CITIES = {
    "Sydney":     "http://www.bom.gov.au/clim_data/cdio/tables/text/IDCJCM0037_066062.csv",
    "Melbourne":  "http://www.bom.gov.au/clim_data/cdio/tables/text/IDCJCM0035_086282.csv",
    "Brisbane":   "http://www.bom.gov.au/clim_data/cdio/tables/text/IDCJCM0036_040842.csv",
    "Perth":      "http://www.bom.gov.au/clim_data/cdio/tables/text/IDCJCM0037_009225.csv",
    "Adelaide":   "http://www.bom.gov.au/clim_data/cdio/tables/text/IDCJCM0037_023034.csv",
    "Canberra":   "http://www.bom.gov.au/clim_data/cdio/tables/text/IDCJCM0037_070282.csv",
    "Darwin":     "http://www.bom.gov.au/clim_data/cdio/tables/text/IDCJCM0038_014015.csv",
    "Hobart":     "http://www.bom.gov.au/clim_data/cdio/tables/text/IDCJCM0033_094029.csv",
    "Townsville": "http://www.bom.gov.au/clim_data/cdio/tables/text/IDCJCM0036_032040.csv",
    "Gold Coast": "http://www.bom.gov.au/clim_data/cdio/tables/text/IDCJCM0036_040764.csv",
}


def download_file(url, dest_path, label):
    if dest_path.exists():
        print(f"  {label}: already exists, skipping")
        return
    print(f"  {label}: downloading...")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as response, open(dest_path, "wb") as f:
        f.write(response.read())
    print(f"  {label}: done ({dest_path.stat().st_size / 1_000_000:.1f} MB)")


def download_census():
    census_dir = ABS_DATA_DIR / "census"
    census_dir.mkdir(parents=True, exist_ok=True)

    # Check if already extracted
    existing = list(census_dir.glob("*.csv"))
    if len(existing) >= len(CENSUS_TABLES):
        print(f"  Census GCP: already extracted ({len(existing)} files), skipping")
        return

    zip_path = ABS_DATA_DIR / "census_gcp_sa2.zip"
    download_file(CENSUS_URL, zip_path, "Census GCP SA2 (41 MB)")

    print("  Census GCP: extracting needed tables...")
    with zipfile.ZipFile(zip_path, "r") as z:
        for member in z.namelist():
            # Match e.g. 2021Census_G02_AUST_SA2.csv, G04A, G09A, G63A
            fname = Path(member).name
            if any(f"_{t}_" in fname for t in CENSUS_TABLES) and fname.endswith(".csv"):
                target = census_dir / fname
                if not target.exists():
                    with z.open(member) as src, open(target, "wb") as dst:
                        dst.write(src.read())
                    print(f"    extracted: {fname}")

    zip_path.unlink()
    print(f"  Census GCP: done -> {census_dir}")


def download_seifa():
    seifa_dir = ABS_DATA_DIR / "seifa"
    seifa_dir.mkdir(parents=True, exist_ok=True)
    dest = seifa_dir / "SEIFA_2021_SA2.xlsx"
    download_file(SEIFA_URL, dest, "SEIFA 2021 SA2")


def download_bom():
    bom_dir = ABS_DATA_DIR / "bom"
    bom_dir.mkdir(parents=True, exist_ok=True)
    for city, url in BOM_CITIES.items():
        dest = bom_dir / f"{city.replace(' ', '_')}.csv"
        if dest.exists():
            print(f"  BOM {city}: already exists, skipping")
            continue
        print(f"  BOM {city}: downloading...")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req) as response, open(dest, "wb") as f:
                f.write(response.read())
            print(f"  BOM {city}: done")
        except Exception as e:
            print(f"  BOM {city}: SKIPPED ({e})")


def main():
    ABS_DATA_DIR.mkdir(exist_ok=True)
    print("=== Downloading ABS Census 2021 GCP ===")
    download_census()
    print("\n=== Downloading SEIFA 2021 ===")
    download_seifa()
    print("\n=== Downloading BOM Climate Data ===")
    download_bom()
    print("\nAll ABS/BOM data ready.")


if __name__ == "__main__":
    main()
