"""
Download ABS ASGS boundary shapefiles (SA2, SA3, LGA) into geo_mapping/boundaries/.
Safe to re-run — skips files already downloaded.
"""

import os
import zipfile
import urllib.request
from pathlib import Path

BOUNDARIES_DIR = Path(__file__).parent / "boundaries"

FILES = {
    "SA2": "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SA2_2021_AUST_SHP_GDA2020.zip",
    "SA3": "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SA3_2021_AUST_SHP_GDA2020.zip",
    "LGA": "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/LGA_2025_AUST_GDA2020.zip",
}


def download_and_unzip(name, url):
    dest_dir = BOUNDARIES_DIR / name
    if dest_dir.exists() and any(dest_dir.glob("*.shp")):
        print(f"  {name}: already downloaded, skipping")
        return

    dest_dir.mkdir(parents=True, exist_ok=True)
    zip_path = BOUNDARIES_DIR / f"{name}.zip"

    print(f"  {name}: downloading...")
    urllib.request.urlretrieve(url, zip_path)
    print(f"  {name}: unzipping...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(dest_dir)
    zip_path.unlink()
    print(f"  {name}: done -> {dest_dir}")


def main():
    print("Downloading ABS boundary files...")
    for name, url in FILES.items():
        download_and_unzip(name, url)
    print("\nAll boundary files ready.")


if __name__ == "__main__":
    main()
