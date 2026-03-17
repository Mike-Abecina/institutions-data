"""
Compute 7 student/academic/job-opportunity metrics per SA2 from ABS Census 2021 GCP tables.

Input:  geo_mapping/output/institutions_vibe_metrics.csv  (has sa2_code + vibe metrics)
Output: geo_mapping/output/institutions_student_metrics.csv

Metrics
-------
1.  qualification_density   % of 15+ pop with bachelor's degree or higher  (G49B)
2.  grad_capture_rate       % of 25–34 year olds with any post-school qual  (G49B)
3.  professional_job_pct    % of employed in manager or professional roles  (G60B)
4.  stem_field_pct          % of qualified workers with STEM field of study  (G50A)
5.  income_growth_signal    % of 15+ earning $1,500+/week  (G17A)
6.  employment_rate         employment-to-population ratio (15+)  (G43)
7.  mortgage_stress_pct     % of mortgaged dwellings paying $3,000+/month  (G38)
"""

import sys
from pathlib import Path
import pandas as pd

CENSUS_DIR = Path(__file__).parent / "abs_data" / "census"
OUTPUT_DIR = Path(__file__).parent / "output"
INPUT_FILE = OUTPUT_DIR / "institutions_vibe_metrics.csv"
OUTPUT_FILE = OUTPUT_DIR / "institutions_student_metrics.csv"


# ── Helpers ───────────────────────────────────────────────────────────────────

def read_census(table_code):
    files = list(CENSUS_DIR.glob(f"*{table_code}_AUST_SA2.csv"))
    if not files:
        print(f"  WARNING: {table_code} not found in {CENSUS_DIR}")
        return pd.DataFrame()
    df = pd.read_csv(files[0])
    df["sa2_code"] = df["SA2_CODE_2021"].astype(str).str.zfill(9)
    return df


def safe_pct(numerator, denominator):
    return (numerator / denominator.replace(0, float("nan")) * 100).round(1)


# ── Individual metric loaders ─────────────────────────────────────────────────

def _col(df, name):
    """Return column as numeric Series, or zeros if column missing."""
    return pd.to_numeric(df[name], errors="coerce").fillna(0) if name in df.columns else pd.Series(0, index=df.index)


def metric_qualification_density(g49b):
    """% of 15+ population with bachelor's degree or higher."""
    bach_plus = (
        _col(g49b, "P_PGrad_Deg_Total")
        + _col(g49b, "P_GradDip_and_GradCert_Total")
        + _col(g49b, "P_BachDeg_Total")
    )
    total = pd.to_numeric(g49b["P_Tot_Total"], errors="coerce")
    out = g49b[["sa2_code"]].copy()
    out["qualification_density"] = safe_pct(bach_plus, total)
    return out


def metric_grad_capture_rate(g49b):
    """% of 25–34 year olds holding any post-school qualification."""
    qual_25_34 = (
        _col(g49b, "P_BachDeg_25_34")
        + _col(g49b, "P_GradDip_and_GradCert_25_34")
        + _col(g49b, "P_AdvDip_and_Dip_25_34")
        + _col(g49b, "P_Cert_III_IV_25_34")
        + _col(g49b, "P_Cert_I_II_25_34")
    )
    total_25_34 = pd.to_numeric(g49b["P_Tot_25_34"], errors="coerce")
    out = g49b[["sa2_code"]].copy()
    out["grad_capture_rate"] = safe_pct(qual_25_34, total_25_34)
    return out


def metric_professional_job_pct(g60b):
    """% of employed workers in manager or professional roles."""
    managers = pd.to_numeric(g60b["P_Tot_Managers"], errors="coerce").fillna(0)
    professionals = pd.to_numeric(g60b["P_Tot_Professionals"], errors="coerce").fillna(0)
    total = pd.to_numeric(g60b["P_Tot_Tot"], errors="coerce")
    out = g60b[["sa2_code"]].copy()
    out["professional_job_pct"] = safe_pct(managers + professionals, total)
    return out


def metric_stem_field_pct(g50a):
    """% of qualified residents whose field of study is STEM."""
    stem_fields = ["NatPhyl_Scn", "InfoTech", "Eng_RelTec", "ArchtBldng"]
    stem = sum(_col(g50a, f"M_{f}_Tot") + _col(g50a, f"F_{f}_Tot") for f in stem_fields)
    # G50A has M_Tot_Tot but no F_Tot_Tot — derive female total from all F_*_Tot cols
    f_tot_cols = [c for c in g50a.columns if c.startswith("F_") and c.endswith("_Tot")]
    f_total = g50a[f_tot_cols].apply(pd.to_numeric, errors="coerce").fillna(0).sum(axis=1)
    total = _col(g50a, "M_Tot_Tot") + f_total
    out = g50a[["sa2_code"]].copy()
    out["stem_field_pct"] = safe_pct(stem, total)
    return out


def metric_income_growth_signal(g17a):
    """% of 15+ population earning $1,500+/week (high earner density)."""
    high_brackets = ["1500_1749", "1750_1999", "2000_2999", "3000_3499", "3500_more"]
    high_earners = sum(_col(g17a, f"M_{b}_Tot") + _col(g17a, f"F_{b}_Tot") for b in high_brackets)
    total = _col(g17a, "M_Tot_Tot") + _col(g17a, "F_Tot_Tot")
    out = g17a[["sa2_code"]].copy()
    out["income_growth_signal"] = safe_pct(high_earners, total)
    return out


def metric_employment_rate(g43):
    """Employment-to-population ratio (%) for 15+ year olds."""
    rate = pd.to_numeric(g43["Percnt_Employment_to_populn_P"], errors="coerce")
    out = g43[["sa2_code"]].copy()
    out["employment_rate"] = rate.round(1)
    return out


def metric_mortgage_stress_pct(g38):
    """% of mortgaged dwellings paying $3,000+/month (affordability pressure)."""
    stressed = _col(g38, "M_3000_3999_Tot") + _col(g38, "M_4000_over_Tot")
    total = pd.to_numeric(g38["Tot_Tot"], errors="coerce")
    out = g38[["sa2_code"]].copy()
    out["mortgage_stress_pct"] = safe_pct(stressed, total)
    return out


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not INPUT_FILE.exists():
        print(f"ERROR: {INPUT_FILE} not found. Run compute_vibe_metrics.py first.")
        sys.exit(1)

    print("Loading institutions...")
    inst = pd.read_csv(INPUT_FILE, dtype={"sa2_code": str})
    inst["sa2_code"] = inst["sa2_code"].str.zfill(9)
    print(f"  {len(inst):,} institutions")

    print("\nLoading Census tables...")
    g17a = read_census("G17A")
    g38  = read_census("G38")
    g43  = read_census("G43")
    g49b = read_census("G49B")
    g50a = read_census("G50A")
    g60b = read_census("G60B")

    print("\nComputing metrics...")
    metrics = {}

    def add(name, df):
        if not df.empty:
            df["sa2_code"] = df["sa2_code"].astype(str).str.zfill(9)
            metrics[name] = df
            print(f"  ✓ {name}")
        else:
            print(f"  ✗ {name} (skipped — missing data)")

    add("qualification_density",  metric_qualification_density(g49b))
    add("grad_capture_rate",      metric_grad_capture_rate(g49b))
    add("professional_job_pct",   metric_professional_job_pct(g60b))
    add("stem_field_pct",         metric_stem_field_pct(g50a))
    add("income_growth_signal",   metric_income_growth_signal(g17a))
    add("employment_rate",        metric_employment_rate(g43))
    add("mortgage_stress_pct",    metric_mortgage_stress_pct(g38))

    print("\nMerging into institutions...")
    result = inst.copy()
    for name, df in metrics.items():
        df["sa2_code"] = df["sa2_code"].astype(str).str.zfill(9)
        result = result.merge(df, on="sa2_code", how="left")

    # Normalise: add _norm columns clipped to 0–100
    metric_cols = [
        "qualification_density", "grad_capture_rate", "professional_job_pct",
        "stem_field_pct", "income_growth_signal", "employment_rate",
        "mortgage_stress_pct",
    ]
    print("\nAdding normalised columns (clipped to 0–100)...")
    for col in metric_cols:
        if col in result.columns:
            result[f"{col}_norm"] = result[col].clip(lower=0, upper=100).round(1)

    OUTPUT_DIR.mkdir(exist_ok=True)
    result.to_csv(OUTPUT_FILE, index=False)

    # Summary
    print(f"\n{'Metric':<30} {'Cover':>7}  {'Mean':>7}  {'Min':>8}  {'Max':>8}")
    print("-" * 67)
    for col in metric_cols:
        if col in result.columns:
            s = result[col].dropna()
            cov = result[col].notna().mean() * 100
            print(f"  {col:<28} {cov:>5.1f}%  {s.mean():>7.1f}  {s.min():>8.1f}  {s.max():>8.1f}")

    print(f"\nOutput: {OUTPUT_FILE}")
    print(f"Columns: {len(result.columns)} total  |  Institutions: {len(result):,}")


if __name__ == "__main__":
    main()
