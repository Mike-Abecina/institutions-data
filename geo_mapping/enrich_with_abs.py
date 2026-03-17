"""
Join ABS Census, SEIFA, and BOM climate data to institutions via SA2 code.

Input:  geo_mapping/output/institutions_with_abs_geography.csv
Output: geo_mapping/output/institutions_enriched.csv

Derived metrics:
  youth_pct         — % of SA2 population aged 15–24 (student density)
  overseas_born_pct — % born overseas (global mix)
  median_rent_weekly— median weekly rent in SA2 (AUD)
  seifa_irsad_score — raw IRSAD score (higher = more advantaged)
  seifa_irsad_decile— 1–10 decile (national)
  seifa_label       — plain-English vibe label
  sunshine_hours_yr — annual sunshine hours (BOM, city-level)
"""

import sys
import re
import warnings
from pathlib import Path

import pandas as pd

ABS_DATA_DIR = Path(__file__).parent / "abs_data"
OUTPUT_DIR   = Path(__file__).parent / "output"
INPUT_FILE   = OUTPUT_DIR / "institutions_with_abs_geography.csv"
OUTPUT_FILE  = OUTPUT_DIR / "institutions_enriched.csv"

# Map state → nearest BOM city for sunshine lookup
STATE_TO_BOM_CITY = {
    "NSW": "Sydney",
    "VIC": "Melbourne",
    "QLD": "Brisbane",
    "WA":  "Perth",
    "SA":  "Adelaide",
    "ACT": "Canberra",
    "NT":  "Darwin",
    "TAS": "Hobart",
    "ALL STATES": "Sydney",  # fallback
}

SEIFA_LABELS = {
    (9, 10): "Established & Affluent",
    (7,  8): "Up-and-Coming",
    (5,  6): "Grounded & Real",
    (1,  4): "Hustle Suburb",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def find_col(df, *patterns):
    """Return first column matching any of the regex patterns (case-insensitive)."""
    for pat in patterns:
        matches = [c for c in df.columns if re.search(pat, c, re.IGNORECASE)]
        if matches:
            return matches[0]
    return None


def seifa_label(decile):
    if pd.isna(decile):
        return None
    d = int(decile)
    for (lo, hi), label in SEIFA_LABELS.items():
        if lo <= d <= hi:
            return label
    return None


# ── Loaders ──────────────────────────────────────────────────────────────────

def load_census_g02():
    """Median rent and income from G02."""
    files = list((ABS_DATA_DIR / "census").glob("*G02*.csv"))
    if not files:
        print("  WARNING: G02 not found, skipping rent/income")
        return pd.DataFrame()
    df = pd.read_csv(files[0])
    sa2_col = find_col(df, r"SA2_CODE")
    rent_col = find_col(df, r"Median_rent", r"Med_rent")
    if not sa2_col:
        print("  WARNING: G02 SA2 code column not found")
        return pd.DataFrame()
    out = df[[sa2_col]].copy()
    out["sa2_code"] = out[sa2_col].astype(str)
    if rent_col:
        out["median_rent_weekly"] = pd.to_numeric(df[rent_col], errors="coerce")
    else:
        out["median_rent_weekly"] = None
    return out[["sa2_code", "median_rent_weekly"]]


def load_census_g04():
    """Youth % (15–24) from G04."""
    files_a = list((ABS_DATA_DIR / "census").glob("*G04A*.csv"))
    files_b = list((ABS_DATA_DIR / "census").glob("*G04B*.csv"))
    if not files_a:
        print("  WARNING: G04A not found, skipping youth %")
        return pd.DataFrame()

    df = pd.read_csv(files_a[0])
    if files_b:
        df_b = pd.read_csv(files_b[0])
        sa2_col_b = find_col(df_b, r"SA2_CODE")
        if sa2_col_b:
            df_b = df_b.drop(columns=[sa2_col_b])
        df = pd.concat([df, df_b], axis=1)

    sa2_col = find_col(df, r"SA2_CODE")
    if not sa2_col:
        print("  WARNING: G04 SA2 code column not found")
        return pd.DataFrame()

    out = pd.DataFrame()
    out["sa2_code"] = df[sa2_col].astype(str)

    # Sum 15–19 and 20–24 persons columns
    age_cols = [c for c in df.columns if re.search(r"(15_19|20_24).*_P$", c, re.IGNORECASE)]
    tot_col  = find_col(df, r"Tot_P$", r"^Tot_P")

    if age_cols and tot_col:
        youth = df[age_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)
        total = pd.to_numeric(df[tot_col], errors="coerce")
        out["youth_pct"] = (youth / total * 100).round(1)
    else:
        print(f"  WARNING: could not find age 15-24 columns in G04. Available: {[c for c in df.columns if 'yr' in c.lower() or 'age' in c.lower()][:10]}")
        out["youth_pct"] = None

    return out[["sa2_code", "youth_pct"]]


def load_census_g09():
    """Overseas-born % from G09A.

    G09A has male counts by country (M_CountryName_AgeGroup).
    We sum all M_*_Tot to get total males, exclude M_Australia_Tot for overseas males.
    The male ratio approximates the overall overseas-born %.
    """
    files = list((ABS_DATA_DIR / "census").glob("*G09A*.csv"))
    if not files:
        print("  WARNING: G09A not found, skipping overseas born %")
        return pd.DataFrame()

    df = pd.read_csv(files[0])
    sa2_col = find_col(df, r"SA2_CODE")

    if not sa2_col:
        print("  WARNING: G09A SA2 code column not found")
        return pd.DataFrame()

    tot_cols = [c for c in df.columns if c.endswith("_Tot")]
    aus_col  = [c for c in tot_cols if "Australia" in c]

    out = pd.DataFrame()
    out["sa2_code"] = df[sa2_col].astype(str)

    if tot_cols:
        total    = df[tot_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)
        overseas_cols = [c for c in tot_cols if c not in aus_col]
        overseas = df[overseas_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)
        out["overseas_born_pct"] = (overseas / total * 100).round(1)
    else:
        print("  WARNING: no _Tot columns found in G09A")
        out["overseas_born_pct"] = None

    return out[["sa2_code", "overseas_born_pct"]]


def load_census_g63():
    """Public transport commute % from G63A."""
    files = list((ABS_DATA_DIR / "census").glob("*G63A*.csv"))
    if not files:
        print("  WARNING: G63A not found, skipping transit %")
        return pd.DataFrame()

    df = pd.read_csv(files[0])
    sa2_col = find_col(df, r"SA2_CODE")

    # Public transport: train + bus + ferry + tram
    pt_patterns = [r"Train.*_P$", r"Bus.*_P$", r"Ferry.*_P$", r"Tram.*_P$"]
    pt_cols = [c for c in df.columns if any(re.search(p, c, re.IGNORECASE) for p in pt_patterns)]
    tot_col = find_col(df, r"Tot_P$", r"^Tot_P")

    if not sa2_col:
        print("  WARNING: G63A SA2 code column not found")
        return pd.DataFrame()

    out = pd.DataFrame()
    out["sa2_code"] = df[sa2_col].astype(str)

    if pt_cols and tot_col:
        pt_total = df[pt_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)
        total    = pd.to_numeric(df[tot_col], errors="coerce")
        out["transit_pct"] = (pt_total / total * 100).round(1)
    else:
        print(f"  WARNING: transit columns not found in G63A")
        out["transit_pct"] = None

    return out[["sa2_code", "transit_pct"]]


def load_seifa():
    """IRSAD score and decile from SEIFA 2021 SA2 Excel."""
    seifa_file = ABS_DATA_DIR / "seifa" / "SEIFA_2021_SA2.xlsx"
    if not seifa_file.exists():
        print("  WARNING: SEIFA file not found, skipping")
        return pd.DataFrame()

    print("  Loading SEIFA...")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        # Sheet with IRSAD is usually sheet index 4 or named "Table 2"
        xl = pd.ExcelFile(seifa_file)
        print(f"    Sheets: {xl.sheet_names}")

        # Try each sheet to find IRSAD
        irsad_df = None
        for sheet in xl.sheet_names:
            df = xl.parse(sheet, header=None)
            # Look for "IRSAD" in header rows
            flat = df.iloc[:5].astype(str).values.flatten()
            if any("IRSAD" in v or "Advantage and Disadvantage" in v for v in flat):
                irsad_df = df
                print(f"    Using sheet: {sheet}")
                break

        if irsad_df is None:
            print("  WARNING: IRSAD sheet not found in SEIFA Excel")
            return pd.DataFrame()

    # Structure: row 5 = headers, row 6+ = data
    # Cols: 0=SA2 code, 1=SA2 name, 2=IRSD score, 3=IRSD decile,
    #       4=IRSAD score, 5=IRSAD decile, 6=IER score, 7=IER decile,
    #       8=IEO score, 9=IEO decile, 10=population
    df = irsad_df.iloc[6:].copy()
    df = df.reset_index(drop=True)

    out = pd.DataFrame()
    out["sa2_code"]          = df[0].astype(str).str.strip().str.zfill(9)
    out["seifa_irsad_score"] = pd.to_numeric(df[4], errors="coerce")
    out["seifa_irsad_decile"]= pd.to_numeric(df[5], errors="coerce")
    out["seifa_label"]       = out["seifa_irsad_decile"].apply(seifa_label)

    return out.dropna(subset=["sa2_code", "seifa_irsad_score"])


def load_bom_sunshine():
    """Annual sunshine hours per city from BOM CSVs."""
    bom_dir = ABS_DATA_DIR / "bom"
    sunshine = {}

    for csv_file in bom_dir.glob("*.csv"):
        city = csv_file.stem.replace("_", " ")
        try:
            # BOM CSVs have a variable number of header rows; scan for sunshine
            with open(csv_file, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()

            lines = content.splitlines()
            annual_hours = None

            for i, line in enumerate(lines):
                if re.search(r"sunshine|sun.*hours", line, re.IGNORECASE):
                    # Look for annual value — usually in the last numeric column
                    nums = re.findall(r"[\d]+\.?\d*", line)
                    if nums:
                        # Annual value is typically the last number on the row
                        annual_hours = float(nums[-1])
                        # Sanity check: annual sunshine hours ~1000–3600
                        if not (500 < annual_hours < 5000):
                            annual_hours = None
                    if annual_hours:
                        break

            if annual_hours:
                sunshine[city] = annual_hours
                print(f"    {city}: {annual_hours:.0f} hrs/yr")
            else:
                print(f"    {city}: sunshine hours not found in CSV")

        except Exception as e:
            print(f"    {city}: error reading BOM file — {e}")

    return sunshine


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not INPUT_FILE.exists():
        print(f"ERROR: {INPUT_FILE} not found. Run map_institutions.py first.")
        sys.exit(1)

    print("Loading institutions...")
    inst = pd.read_csv(INPUT_FILE, dtype={"sa2_code": str})
    # Pad SA2 codes to 9 digits to match ABS format
    inst["sa2_code"] = inst["sa2_code"].str.strip().str.zfill(9)
    print(f"  {len(inst):,} institutions")

    print("\nLoading Census G02 (rent)...")
    g02 = load_census_g02()

    print("Loading Census G04 (age / youth %)...")
    g04 = load_census_g04()

    print("Loading Census G09 (overseas born %)...")
    g09 = load_census_g09()

    print("Loading SEIFA 2021...")
    seifa = load_seifa()

    print("Loading BOM sunshine data...")
    sunshine_map = load_bom_sunshine()

    # Merge all SA2-level data
    print("\nMerging...")
    result = inst.copy()
    for df, label in [(g02, "G02"), (g04, "G04"), (g09, "G09"), (seifa, "SEIFA")]:
        if len(df) > 0:
            df["sa2_code"] = df["sa2_code"].astype(str).str.strip().str.zfill(9)
            result = result.merge(df, on="sa2_code", how="left")
            print(f"  Merged {label}")

    # Map BOM sunshine by state (city-level approximation)
    # Perth and Adelaide BOM CSVs were unavailable; use known historical averages
    SUNSHINE_FALLBACK = {"Perth": 3200, "Adelaide": 2500}
    sunshine_map = {**SUNSHINE_FALLBACK, **sunshine_map}
    result["bom_city"] = result["state"].map(STATE_TO_BOM_CITY)
    result["sunshine_hours_yr"] = result["bom_city"].map(sunshine_map)
    result = result.drop(columns=["bom_city"])

    # Save
    OUTPUT_DIR.mkdir(exist_ok=True)
    result.to_csv(OUTPUT_FILE, index=False)

    # Summary
    print(f"\nDone. {len(result):,} institutions enriched.")
    metrics = ["median_rent_weekly", "youth_pct", "overseas_born_pct",
               "seifa_irsad_decile", "sunshine_hours_yr"]
    print(f"\n{'Metric':<25} {'Coverage':>10}")
    print("-" * 37)
    for m in metrics:
        if m in result.columns:
            pct = result[m].notna().mean() * 100
            print(f"  {m:<23} {pct:>8.1f}%")
    print(f"\nOutput: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
