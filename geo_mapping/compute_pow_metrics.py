"""
Liveability metrics derived from ABS Census 2021 Working Population Profile (WPP).

Key insight: who works IN an SA2 (place of work) is a proxy for what infrastructure
physically exists there — regardless of where those workers live.

  More healthcare workers in SA2  →  clinics, hospitals, specialists are actually here
  More food/hospitality workers   →  cafes, restaurants, bars physically exist
  More arts/rec workers           →  venues, theatres, studios are here
  More retail workers             →  shops, markets, everyday convenience
  More education workers          →  schools, unis, libraries nearby
  High job gravity ratio          →  area pulls people in → bustling, not a ghost town

Input:  geo_mapping/output/institutions_student_metrics.csv
Output: geo_mapping/output/institutions_pow_metrics.csv

Metrics
-------
1.  social_scene_score     Food + Arts/Rec workers % of all POW workers. Combined "alive after 5pm" signal.  (W09A+B)
2.  food_scene_pct         Accommodation & Food Services workers % POW. Cafes, restaurants, bars exist here.  (W09A)
3.  entertainment_pct      Arts & Recreation workers % POW. Venues, theatres, studios, events.  (W09B)
4.  healthcare_access_pct  Healthcare & Social Assistance workers % POW. Clinics, hospitals, allied health nearby.  (W09B)
5.  education_hub_pct      Education & Training workers % POW. Schools, unis, tutoring, libraries here.  (W09B)
6.  retail_density_pct     Retail Trade workers % POW. Shops, markets, everyday convenience.  (W09A)
7.  civic_services_pct     Public Admin + Safety workers % POW. Courts, councils, emergency services.  (W09B)
8.  knowledge_hub_pct      Professional/Scientific/Tech + Finance workers % POW. Career networking, high-value employers.  (W09B)
9.  job_gravity_ratio      POW total workers / resident labour force. >1 = area pulls people in, bustling by day.  (W01A+G43)
"""

import sys
import math
from pathlib import Path
import pandas as pd

WPP_DIR    = Path(__file__).parent / "abs_data" / "wpp"
CENSUS_DIR = Path(__file__).parent / "abs_data" / "census"
OUTPUT_DIR = Path(__file__).parent / "output"
INPUT_FILE = OUTPUT_DIR / "institutions_student_metrics.csv"
OUTPUT_FILE = OUTPUT_DIR / "institutions_pow_metrics.csv"


# ── Helpers ───────────────────────────────────────────────────────────────────

def read_wpp(table_code):
    files = list(WPP_DIR.glob(f"*{table_code}_*.csv"))
    if not files:
        print(f"  WARNING: {table_code} not found in {WPP_DIR}")
        return pd.DataFrame()
    df = pd.read_csv(files[0])
    df["sa2_code"] = df["POW_SA2_CODE_2021"].astype(str).str.zfill(9)
    return df


def read_census(table_code):
    files = list(CENSUS_DIR.glob(f"*{table_code}_AUST_SA2.csv"))
    if not files:
        print(f"  WARNING: {table_code} not found in {CENSUS_DIR}")
        return pd.DataFrame()
    df = pd.read_csv(files[0])
    df["sa2_code"] = df["SA2_CODE_2021"].astype(str).str.zfill(9)
    return df


def _col(df, name):
    return pd.to_numeric(df[name], errors="coerce").fillna(0) if name in df.columns else pd.Series(0, index=df.index)


def safe_pct(num, denom):
    return (num / denom.replace(0, float("nan")) * 100).round(1)


def safe_ratio(num, denom):
    return (num / denom.replace(0, float("nan"))).round(3)


def pow_total(w09a, w09b_aligned):
    """Total POW workers from all industries combined."""
    ind_a = ["AgFF_Tot_P", "Min_Tot_P", "Mnf_Tot_P", "EGWWS_Tot_P",
             "Const_Tot_P", "WST_Tot_P", "RetT_Tot_P", "AcFd_Tot_P"]
    ind_b = ["TPW_Tot_P", "IMT_Tot_P", "FinIns_Tot_P", "RHRE_Tot_P",
             "ProSTS_Tot_P", "AdSup_Tot_P", "PubAS_Tot_P", "EdTrn_Tot_P",
             "HC_SA_Tot_P", "ArtsR_Tot_P", "OthSvs_Tot_P"]
    tot_a = w09a[[c for c in ind_a if c in w09a.columns]].apply(pd.to_numeric, errors="coerce").fillna(0).sum(axis=1)
    tot_b = w09b_aligned[[c for c in ind_b if c in w09b_aligned.columns]].apply(pd.to_numeric, errors="coerce").fillna(0).sum(axis=1)
    return (tot_a + tot_b).replace(0, float("nan"))


# ── Metric calculators ────────────────────────────────────────────────────────

def metric_social_scene(w09a, w09b_aligned, total):
    """Food services + Arts & Recreation workers. Combined 'is this place alive after 5pm' signal."""
    food = _col(w09a, "AcFd_Tot_P")
    arts = _col(w09b_aligned, "ArtsR_Tot_P")
    out = w09a[["sa2_code"]].copy()
    out["social_scene_score"] = safe_pct(food + arts, total)
    return out


def metric_food_scene(w09a, total):
    """Accommodation & Food Services workers — cafes, restaurants, bars physically here."""
    out = w09a[["sa2_code"]].copy()
    out["food_scene_pct"] = safe_pct(_col(w09a, "AcFd_Tot_P"), total)
    return out


def metric_entertainment(w09b_aligned, w09a_sa2, total):
    """Arts & Recreation workers — venues, theatres, studios, live music."""
    out = w09a_sa2.copy()
    out["entertainment_pct"] = safe_pct(_col(w09b_aligned, "ArtsR_Tot_P"), total)
    return out


def metric_healthcare_access(w09b_aligned, w09a_sa2, total):
    """Healthcare & Social Assistance workers — clinics, hospitals, allied health here."""
    out = w09a_sa2.copy()
    out["healthcare_access_pct"] = safe_pct(_col(w09b_aligned, "HC_SA_Tot_P"), total)
    return out


def metric_education_hub(w09b_aligned, w09a_sa2, total):
    """Education & Training workers — schools, unis, libraries, tutoring here."""
    out = w09a_sa2.copy()
    out["education_hub_pct"] = safe_pct(_col(w09b_aligned, "EdTrn_Tot_P"), total)
    return out


def metric_retail_density(w09a, total):
    """Retail Trade workers — shops, markets, everyday convenience."""
    out = w09a[["sa2_code"]].copy()
    out["retail_density_pct"] = safe_pct(_col(w09a, "RetT_Tot_P"), total)
    return out


def metric_civic_services(w09b_aligned, w09a_sa2, total):
    """Public Administration & Safety workers — council services, courts, emergency services."""
    out = w09a_sa2.copy()
    out["civic_services_pct"] = safe_pct(_col(w09b_aligned, "PubAS_Tot_P"), total)
    return out


def metric_knowledge_hub(w09b_aligned, w09a_sa2, total):
    """Professional/Scientific/Tech + Finance workers — high-value employers, career networking."""
    prost = _col(w09b_aligned, "ProSTS_Tot_P")
    fin   = _col(w09b_aligned, "FinIns_Tot_P")
    out = w09a_sa2.copy()
    out["knowledge_hub_pct"] = safe_pct(prost + fin, total)
    return out


def metric_job_gravity(w01a, g43):
    """Ratio of workers flowing IN (POW) to resident labour force.
    >1 = more jobs exist here than workers who live here → area is bustling by day."""
    m_tot_cols = [c for c in w01a.columns if c.startswith("M_") and c.endswith("_Tot")]
    f_tot_cols = [c for c in w01a.columns if c.startswith("F_") and c.endswith("_Tot")]
    pow_workers = (
        w01a[m_tot_cols].apply(pd.to_numeric, errors="coerce").fillna(0).sum(axis=1)
        + w01a[f_tot_cols].apply(pd.to_numeric, errors="coerce").fillna(0).sum(axis=1)
    )
    resident_lf = pd.to_numeric(g43["lfs_Tot_LF_P"], errors="coerce")
    g43_lf = g43[["sa2_code", "lfs_Tot_LF_P"]].copy()
    g43_lf["lfs_Tot_LF_P"] = resident_lf

    out = w01a[["sa2_code"]].copy()
    out["_pow_workers"] = pow_workers.values
    out = out.merge(g43_lf, on="sa2_code", how="left")
    out["job_gravity_ratio"] = safe_ratio(
        out["_pow_workers"],
        pd.to_numeric(out["lfs_Tot_LF_P"], errors="coerce").replace(0, float("nan"))
    )
    return out[["sa2_code", "job_gravity_ratio"]]


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not INPUT_FILE.exists():
        print(f"ERROR: {INPUT_FILE} not found. Run compute_student_metrics.py first.")
        sys.exit(1)

    print("Loading institutions...")
    inst = pd.read_csv(INPUT_FILE, dtype={"sa2_code": str})
    inst["sa2_code"] = inst["sa2_code"].str.zfill(9)
    print(f"  {len(inst):,} institutions")

    print("\nLoading WPP tables...")
    w01a = read_wpp("W01A")
    w09a = read_wpp("W09A")
    w09b = read_wpp("W09B")

    print("Loading Census G43 (resident labour force)...")
    g43 = read_census("G43")

    # Align W09B to W09A index for consistent row-wise arithmetic
    w09b_aligned = w09b.set_index("sa2_code").reindex(w09a["sa2_code"].values).reset_index()
    w09a_sa2 = w09a[["sa2_code"]].copy()  # lightweight base for merges
    total = pow_total(w09a, w09b_aligned)

    print("\nComputing metrics...")
    metrics = {}

    def add(name, df):
        if not df.empty and len(df.columns) > 1:
            df["sa2_code"] = df["sa2_code"].astype(str).str.zfill(9)
            metrics[name] = df
            print(f"  ✓ {name}")
        else:
            print(f"  ✗ {name} (skipped)")

    add("social_scene",       metric_social_scene(w09a, w09b_aligned, total))
    add("food_scene",         metric_food_scene(w09a, total))
    add("entertainment",      metric_entertainment(w09b_aligned, w09a_sa2.copy(), total))
    add("healthcare_access",  metric_healthcare_access(w09b_aligned, w09a_sa2.copy(), total))
    add("education_hub",      metric_education_hub(w09b_aligned, w09a_sa2.copy(), total))
    add("retail_density",     metric_retail_density(w09a, total))
    add("civic_services",     metric_civic_services(w09b_aligned, w09a_sa2.copy(), total))
    add("knowledge_hub",      metric_knowledge_hub(w09b_aligned, w09a_sa2.copy(), total))
    if not w01a.empty and not g43.empty:
        add("job_gravity",    metric_job_gravity(w01a, g43))

    print("\nMerging into institutions...")
    result = inst.copy()
    for name, df in metrics.items():
        df["sa2_code"] = df["sa2_code"].astype(str).str.zfill(9)
        result = result.merge(df, on="sa2_code", how="left")

    # Normalise percentage metrics to 0–100
    pct_cols = [
        "social_scene_score", "food_scene_pct", "entertainment_pct",
        "healthcare_access_pct", "education_hub_pct", "retail_density_pct",
        "civic_services_pct", "knowledge_hub_pct",
    ]
    print("\nAdding normalised columns...")
    for col in pct_cols:
        if col in result.columns:
            result[f"{col}_norm"] = result[col].clip(0, 100).round(1)

    # job_gravity: log-scale then map to 0–100 (ratio spans 0.01 to 8000+)
    # Use log2 scale: log2(0.5)=-1, log2(1)=0, log2(2)=1, log2(10)≈3.3
    # Map log2 range [-4, 6] → [0, 100]
    if "job_gravity_ratio" in result.columns:
        log_ratio = result["job_gravity_ratio"].clip(0.001, None).apply(
            lambda x: math.log2(x) if pd.notna(x) and x > 0 else float("nan")
        )
        result["job_gravity_ratio_norm"] = ((log_ratio + 4) / 10 * 100).clip(0, 100).round(1)

    OUTPUT_DIR.mkdir(exist_ok=True)
    result.to_csv(OUTPUT_FILE, index=False)

    all_cols = pct_cols + ["job_gravity_ratio"]
    print(f"\n{'Metric':<30} {'Cover':>7}  {'Mean':>8}  {'Min':>8}  {'Max':>8}")
    print("-" * 68)
    for col in all_cols:
        if col in result.columns:
            s = result[col].dropna()
            cov = result[col].notna().mean() * 100
            print(f"  {col:<28} {cov:>5.1f}%  {s.mean():>8.2f}  {s.min():>8.2f}  {s.max():>8.2f}")

    print(f"\nOutput: {OUTPUT_FILE}")
    print(f"Columns: {len(result.columns)} total  |  Institutions: {len(result):,}")


if __name__ == "__main__":
    main()
