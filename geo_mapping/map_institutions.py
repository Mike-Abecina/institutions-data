"""
Spatial join: map ABS boundaries (SA2, SA3, LGA) to each institution's lat/long.

Pulls all non-school institutions with coordinates from the ACIR DB,
then does a point-in-polygon join against ABS shapefiles.

Output: geo_mapping/output/institutions_with_abs_geography.csv
"""

import os
import sys
from pathlib import Path
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from dotenv import load_dotenv
import mysql.connector

load_dotenv(Path(__file__).parent.parent / ".env")

BOUNDARIES_DIR = Path(__file__).parent / "boundaries"
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_FILE = OUTPUT_DIR / "institutions_with_abs_geography.csv"

# Non-school org types to include
NON_SCHOOL_TYPES = (
    "University/Higher Education Institution",
    "Higher Education Institutions",
    "Dual Sector University",
    "TAFE Institute",
    "Registered Training Organisation",
    "ELICOS/Foundation",
    "Private Provider",
    "Other",
    "Agency",
    "Industry",
    "Philanthropic Organisation",
    "Government - Commonwealth",
    "Government - State",
    "Government - Local",
)


def get_db_conn():
    return mysql.connector.connect(
        host=os.getenv("DATABSE_HOST"),
        port=int(os.getenv("DATABASE_PORT", "3306")),
        user=os.getenv("DATABASE_USER", "admin"),
        password=os.getenv("DATABASE_PASSWORD"),
        database=os.getenv("DATABASE_NAME", "master"),
        connect_timeout=30,
    )


def load_institutions():
    """Pull all non-school institutions with coordinates from DB."""
    print("Loading institutions from DB...")
    placeholders = ", ".join(["%s"] * len(NON_SCHOOL_TYPES))
    sql = f"""
    SELECT
        o.id AS organisation_id,
        o.name,
        ot.name AS org_type,
        st.abbreviation AS state,
        si.suburb,
        si.postcode,
        CAST(si.latitude AS DECIMAL(18,10)) AS latitude,
        CAST(si.longitude AS DECIMAL(18,10)) AS longitude
    FROM organisations o
    JOIN (
        SELECT organisation_id, MIN(id) AS first_site_id
        FROM sites
        WHERE deleted_at IS NULL
          AND latitude IS NOT NULL AND latitude != ''
          AND longitude IS NOT NULL AND longitude != ''
        GROUP BY organisation_id
    ) best ON best.organisation_id = o.id
    JOIN sites si ON si.id = best.first_site_id
    JOIN states st ON si.state_id = st.id
    JOIN organisation_types ot ON o.organisation_type_id = ot.id
    WHERE o.deleted_at IS NULL
      AND ot.name IN ({placeholders})
    ORDER BY st.abbreviation, o.name
    """
    conn = get_db_conn()
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = pd.read_sql(sql, conn, params=NON_SCHOOL_TYPES)
    conn.close()

    df = df.dropna(subset=["latitude", "longitude"])
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])

    print(f"  Loaded {len(df):,} institutions with coordinates")
    return df


def to_geodataframe(df):
    """Convert institutions DataFrame to GeoDataFrame (WGS84)."""
    geometry = [Point(lon, lat) for lon, lat in zip(df["longitude"], df["latitude"])]
    return gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")


def load_boundary(name, code_col, name_col):
    """Load an ABS shapefile and return key columns in WGS84."""
    shp_dir = BOUNDARIES_DIR / name
    shp_files = list(shp_dir.glob("*.shp"))
    if not shp_files:
        print(f"  ERROR: No shapefile found in {shp_dir}. Run download_boundaries.py first.")
        sys.exit(1)

    print(f"  Loading {name} boundary ({shp_files[0].name})...")
    gdf = gpd.read_file(shp_files[0])
    gdf = gdf[[code_col, name_col, "geometry"]].copy()
    gdf = gdf.to_crs("EPSG:4326")
    return gdf


def spatial_join(institutions_gdf, boundary_gdf, code_col, name_col, prefix):
    """Join institutions to a boundary layer, adding prefixed code/name columns."""
    joined = gpd.sjoin(institutions_gdf, boundary_gdf[[code_col, name_col, "geometry"]], how="left", predicate="within")
    joined = joined.rename(columns={
        code_col: f"{prefix}_code",
        name_col: f"{prefix}_name",
    })
    # Drop sjoin artifacts
    joined = joined.drop(columns=["index_right"], errors="ignore")
    return joined


def main():
    # Check boundaries exist
    for name in ["SA2", "SA3", "LGA"]:
        if not list((BOUNDARIES_DIR / name).glob("*.shp")):
            print(f"Missing {name} shapefile. Run download_boundaries.py first.")
            sys.exit(1)

    # Load institutions
    inst_df = load_institutions()
    inst_gdf = to_geodataframe(inst_df)

    # Load boundaries
    print("\nLoading ABS boundary files...")
    sa2 = load_boundary("SA2", "SA2_CODE21", "SA2_NAME21")
    sa3 = load_boundary("SA3", "SA3_CODE21", "SA3_NAME21")
    lga = load_boundary("LGA", "LGA_CODE25", "LGA_NAME25")

    # Spatial joins
    print("\nRunning spatial joins...")
    print("  Joining SA2...")
    result = spatial_join(inst_gdf, sa2, "SA2_CODE21", "SA2_NAME21", "sa2")
    print("  Joining SA3...")
    result = spatial_join(result, sa3, "SA3_CODE21", "SA3_NAME21", "sa3")
    print("  Joining LGA...")
    result = spatial_join(result, lga, "LGA_CODE25", "LGA_NAME25", "lga")

    # Drop geometry, save as CSV
    result = result.drop(columns=["geometry"])
    OUTPUT_DIR.mkdir(exist_ok=True)
    result.to_csv(OUTPUT_FILE, index=False)

    # Summary
    total = len(result)
    sa2_matched = result["sa2_code"].notna().sum()
    lga_matched = result["lga_code"].notna().sum()
    print(f"\nDone.")
    print(f"  Total institutions: {total:,}")
    print(f"  Matched to SA2:     {sa2_matched:,} ({100*sa2_matched/total:.1f}%)")
    print(f"  Matched to LGA:     {lga_matched:,} ({100*lga_matched/total:.1f}%)")
    print(f"\nOutput: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
