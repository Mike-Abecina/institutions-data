"""
Compute 13 student-life vibe metrics per SA2 from ABS Census 2021 GCP tables.

Input:  geo_mapping/output/institutions_enriched.csv  (has sa2_code)
Output: geo_mapping/output/institutions_vibe_metrics.csv

Metrics
-------
1.  car_jail_score          % dwellings with 0 cars  (G34)
2.  car_free_commute_pct    % workers NOT using a car to commute  (G62)
3.  night_economy_pct       % employed in hospitality + arts/recreation  (G54)
4.  student_bubble_pct      % of population attending uni/TAFE aged 15-24  (G15)
5.  renter_republic_pct     % dwellings being rented  (G37)
6.  vertical_city_pct       % dwellings that are flats/apartments  (G36)
7.  wfh_pct                 % workers who worked from home  (G62)
8.  pedal_path_pct          % commuters cycling or walking only  (G62)
9.  global_mix_score        avg of overseas_born_pct + non-English at home %  (G09+G13)
10. housing_stress_ratio    annual rent / annual income × 100  (G02)
11. fresh_energy_pct        % who lived at a different address 1 year ago  (G44)
12. community_glue_pct      % of 15+ year olds who volunteer  (G23)
13. knowledge_worker_pct    % employed in professional/education/health  (G54)
"""

import sys
from pathlib import Path
import pandas as pd

CENSUS_DIR = Path(__file__).parent / "abs_data" / "census"
OUTPUT_DIR = Path(__file__).parent / "output"
INPUT_FILE = OUTPUT_DIR / "institutions_enriched.csv"
OUTPUT_FILE = OUTPUT_DIR / "institutions_vibe_metrics.csv"


# ── Helpers ──────────────────────────────────────────────────────────────────

def read_census(table_code):
    files = list(CENSUS_DIR.glob(f"*{table_code}_AUST_SA2.csv"))
    if not files:
        print(f"  WARNING: {table_code} not found in {CENSUS_DIR}")
        return pd.DataFrame()
    df = pd.read_csv(files[0])
    df["sa2_code"] = df["SA2_CODE_2021"].astype(str).str.zfill(9)
    return df


def safe_pct(numerator, denominator):
    """Return % rounded to 1dp; NaN where denominator is 0."""
    return (numerator / denominator.replace(0, float("nan")) * 100).round(1)


# ── Individual metric loaders ─────────────────────────────────────────────────

def metric_car_jail(g34):
    """% dwellings with zero cars."""
    out = g34[["sa2_code"]].copy()
    out["car_jail_score"] = safe_pct(g34["Num_MVs_per_dweling_0_MVs"], g34["Total_dwelings"])
    return out


def metric_car_free_commute(g62):
    """% commuters who did NOT use a private car (any mode)."""
    # Car modes to exclude
    car_cols = [c for c in g62.columns if any(x in c for x in
                ["Car_as_driver", "Car_as_passenger", "Truck"]) and c.endswith("_P")]
    all_tot = g62["Tot_P"]
    car_total = g62[car_cols].sum(axis=1)
    out = g62[["sa2_code"]].copy()
    out["car_free_commute_pct"] = safe_pct(all_tot - car_total, all_tot)
    return out


def metric_night_economy(g54a, g54b):
    """% employed in Accommodation & Food + Arts & Recreation."""
    g54 = pd.merge(g54a[["sa2_code"]], g54a, on="sa2_code")
    # Persons totals for each industry (P_ prefix in G54B, M_ in G54A — use _Tot cols)
    # G54A has M_ (male), G54B has F_ (female). Sum both for persons totals.
    def tot_col(df, industry_fragment):
        cols = [c for c in df.columns if industry_fragment in c and c.endswith("_Tot")]
        return df[cols].sum(axis=1) if cols else pd.Series(0, index=df.index)

    accom_m   = tot_col(g54a, "Accom_food")
    arts_m    = tot_col(g54a, "Art_recn")
    accom_f   = tot_col(g54b, "Accom_food")
    arts_f    = tot_col(g54b, "Art_recn")

    # Total employed = sum all industry _Tot cols (M + F)
    all_m_tots = [c for c in g54a.columns if c.endswith("_Tot") and c.startswith("M_") and "ID_NS" not in c]
    all_f_tots = [c for c in g54b.columns if c.endswith("_Tot") and c.startswith("F_") and "ID_NS" not in c]
    total_employed = g54a[all_m_tots].sum(axis=1) + g54b[all_f_tots].sum(axis=1)
    night_workers  = accom_m + arts_m + accom_f + arts_f

    out = g54a[["sa2_code"]].copy()
    out["night_economy_pct"] = safe_pct(night_workers, total_employed)
    return out


def metric_knowledge_worker(g54a, g54b):
    """% employed in Professional/Scientific/Technical + Education + Health."""
    def tot_col(df, fragment):
        cols = [c for c in df.columns if fragment in c and c.endswith("_Tot")]
        return df[cols].sum(axis=1) if cols else pd.Series(0, index=df.index)

    know_m = tot_col(g54a, "Pro_scien_tec") + tot_col(g54a, "Educ_trng") + tot_col(g54a, "HlthCare_SocAs")
    know_f = tot_col(g54b, "Pro_scien_tec") + tot_col(g54b, "Educ_trng") + tot_col(g54b, "HlthCare_SocAs")

    all_m_tots = [c for c in g54a.columns if c.endswith("_Tot") and c.startswith("M_") and "ID_NS" not in c]
    all_f_tots = [c for c in g54b.columns if c.endswith("_Tot") and c.startswith("F_") and "ID_NS" not in c]
    total_employed = g54a[all_m_tots].sum(axis=1) + g54b[all_f_tots].sum(axis=1)

    out = g54a[["sa2_code"]].copy()
    out["knowledge_worker_pct"] = safe_pct(know_m + know_f, total_employed)
    return out


def metric_student_bubble(g15):
    """% of population attending uni or TAFE aged 15-24."""
    uni_cols  = [c for c in g15.columns if "Tert_Uni" in c and "15_24" in c and c.endswith("_P")]
    tafe_cols = [c for c in g15.columns if "Tert_Voc" in c and "15_24" in c and c.endswith("_P")]
    students  = g15[uni_cols + tafe_cols].sum(axis=1)
    out = g15[["sa2_code"]].copy()
    out["student_bubble_pct"] = safe_pct(students, g15["Tot_P"])
    return out


def metric_renter_republic(g37):
    """% dwellings being rented (any landlord type)."""
    rent_cols = [c for c in g37.columns if c.startswith("R_") and c.endswith("_Total")
                 and "Tot" not in c.replace("Total", "")]
    total_col = "Total_Total"
    out = g37[["sa2_code"]].copy()
    out["renter_republic_pct"] = safe_pct(g37[rent_cols].sum(axis=1), g37[total_col])
    return out


def metric_vertical_city(g36):
    """% dwellings that are flats or apartments (any storey height)."""
    flat_cols = [c for c in g36.columns if "Flt_apart" in c and c.endswith("_Dwgs")
                 and "Tot" in c]
    total_col = "OPDs_Tot_OPDs_Dwellings"
    out = g36[["sa2_code"]].copy()
    out["vertical_city_pct"] = safe_pct(g36[flat_cols].sum(axis=1), g36[total_col])
    return out


def metric_wfh(g62):
    """% of workers who worked from home."""
    out = g62[["sa2_code"]].copy()
    out["wfh_pct"] = safe_pct(g62["Worked_home_P"], g62["Tot_P"])
    return out


def metric_pedal_path(g62):
    """% commuters who cycled or walked only."""
    active = g62["One_method_Bicycle_P"] + g62["One_method_Walked_only_P"]
    out = g62[["sa2_code"]].copy()
    out["pedal_path_pct"] = safe_pct(active, g62["Tot_P"])
    return out


def metric_global_mix(g13a, existing_overseas_pct):
    """Avg of overseas_born_pct and % speaking non-English at home.

    Non-English at home = sum of individual leaf-level MOL_*_Tot columns
    (excluding group parent totals like MOL_CL_Tot_Tot and MOL_IAL_Tot_Tot,
    and UOLSE sub-totals) divided by (english_only + non_english).

    The original approach of summing all MOL_*_Tot columns double-counted
    because Chinese Languages, Indian/Asian Languages etc. have both individual
    subcategory columns AND a group _Tot_Tot rollup column.
    """
    # Leaf-level individual language columns only — no group totals, no UOLSE sub-totals
    non_eng_cols = [
        c for c in g13a.columns
        if c.startswith("MOL_")
        and c.endswith("_Tot")
        and "_UOLSE_" not in c
        and not c.endswith("_Tot_Tot")
    ]

    out = g13a[["sa2_code"]].copy()

    if non_eng_cols and "MSEO_Tot" in g13a.columns:
        non_english = g13a[non_eng_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)
        eng_only    = pd.to_numeric(g13a["MSEO_Tot"], errors="coerce")
        total       = eng_only + non_english
        non_eng_pct = safe_pct(non_english, total)
    else:
        non_eng_pct = pd.Series(float("nan"), index=g13a.index)

    out["non_english_home_pct"] = non_eng_pct

    out = out.merge(existing_overseas_pct, on="sa2_code", how="left")
    out["global_mix_score"] = ((out["non_english_home_pct"] + out["overseas_born_pct"]) / 2).round(1)
    return out[["sa2_code", "non_english_home_pct", "global_mix_score"]]


def metric_housing_stress(g02):
    """Ratio of annual rent to annual personal income (%)."""
    rent   = pd.to_numeric(g02["Median_rent_weekly"],         errors="coerce") * 52
    income = pd.to_numeric(g02["Median_tot_prsnl_inc_weekly"], errors="coerce") * 52
    out = g02[["sa2_code"]].copy()
    out["housing_stress_ratio"] = safe_pct(rent, income)
    return out


def metric_fresh_energy(g44):
    """% who lived at a different address 1 year ago."""
    moved_col = "Difnt_Usl_add_1_yr_ago_Tot_P"
    tot_col   = "Tot_P"
    out = g44[["sa2_code"]].copy()
    out["fresh_energy_pct"] = safe_pct(pd.to_numeric(g44[moved_col], errors="coerce"),
                                        pd.to_numeric(g44[tot_col],   errors="coerce"))
    return out


def metric_community_glue(g23):
    """% of 15+ year olds who do voluntary work."""
    out = g23[["sa2_code"]].copy()
    out["community_glue_pct"] = safe_pct(
        pd.to_numeric(g23["P_Tot_Volunteer"],    errors="coerce"),
        pd.to_numeric(g23["P_Tot_Tot"],          errors="coerce"),
    )
    return out


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not INPUT_FILE.exists():
        print(f"ERROR: {INPUT_FILE} not found. Run enrich_with_abs.py first.")
        sys.exit(1)

    print("Loading institutions...")
    inst = pd.read_csv(INPUT_FILE, dtype={"sa2_code": str})
    inst["sa2_code"] = inst["sa2_code"].str.zfill(9)
    print(f"  {len(inst):,} institutions")

    print("\nLoading Census tables...")
    g02  = read_census("G02")
    g13a = read_census("G13A")
    g15  = read_census("G15")
    g23  = read_census("G23")
    g34  = read_census("G34")
    g36  = read_census("G36")
    g37  = read_census("G37")
    g44  = read_census("G44")
    g54a = read_census("G54A")
    g54b = read_census("G54B")
    g62  = read_census("G62")

    # Align G54B index to G54A
    if not g54b.empty:
        g54b = g54b.set_index("sa2_code").reindex(g54a.set_index("sa2_code").index).reset_index()
        g54a = g54a.reset_index(drop=True)
        g54b = g54b.reset_index(drop=True)

    print("\nComputing metrics...")
    metrics = {}

    def add(name, df):
        if not df.empty:
            df["sa2_code"] = df["sa2_code"].astype(str).str.zfill(9)
            metrics[name] = df
            print(f"  ✓ {name}")
        else:
            print(f"  ✗ {name} (skipped — missing data)")

    add("car_jail",          metric_car_jail(g34))
    add("car_free_commute",  metric_car_free_commute(g62))
    add("wfh",               metric_wfh(g62))
    add("pedal_path",        metric_pedal_path(g62))
    add("night_economy",     metric_night_economy(g54a, g54b))
    add("knowledge_worker",  metric_knowledge_worker(g54a, g54b))
    add("student_bubble",    metric_student_bubble(g15))
    add("renter_republic",   metric_renter_republic(g37))
    add("vertical_city",     metric_vertical_city(g36))
    add("housing_stress",    metric_housing_stress(g02))
    add("fresh_energy",      metric_fresh_energy(g44))
    add("community_glue",    metric_community_glue(g23))

    # Global mix needs overseas_born_pct already on inst
    if "overseas_born_pct" in inst.columns and not g13a.empty:
        overseas = inst[["sa2_code", "overseas_born_pct"]].drop_duplicates()
        add("global_mix", metric_global_mix(g13a, overseas))

    print("\nMerging into institutions...")
    result = inst.copy()
    for name, df in metrics.items():
        df["sa2_code"] = df["sa2_code"].astype(str).str.zfill(9)
        result = result.merge(df, on="sa2_code", how="left")

    # Normalise: clip raw values to [0, 100] and add _norm columns
    # These are the raw metric columns that should be percentages but can
    # occasionally exceed bounds due to small-population SA2 anomalies.
    metric_cols = [
        "car_jail_score", "car_free_commute_pct", "wfh_pct", "pedal_path_pct",
        "night_economy_pct", "knowledge_worker_pct", "student_bubble_pct",
        "renter_republic_pct", "vertical_city_pct", "housing_stress_ratio",
        "fresh_energy_pct", "community_glue_pct", "global_mix_score",
    ]
    print("\nAdding normalised columns (clipped to 0–100)...")
    for col in metric_cols:
        if col in result.columns:
            result[f"{col}_norm"] = result[col].clip(lower=0, upper=100).round(1)

    OUTPUT_DIR.mkdir(exist_ok=True)
    result.to_csv(OUTPUT_FILE, index=False)

    # Coverage + stats summary (raw)
    print(f"\n{'Metric':<30} {'Cover':>7}  {'Mean':>7}  {'Raw min':>8}  {'Raw max':>8}")
    print("-" * 67)
    for col in metric_cols:
        if col in result.columns:
            s = result[col].dropna()
            cov = result[col].notna().mean() * 100
            print(f"  {col:<28} {cov:>5.1f}%  {s.mean():>7.1f}  {s.min():>8.1f}  {s.max():>8.1f}")

    print(f"\nOutput: {OUTPUT_FILE}")
    print(f"Columns: {len(result.columns)} total ({len(metric_cols)} raw + {len(metric_cols)} norm)  |  Institutions: {len(result):,}")


if __name__ == "__main__":
    main()
