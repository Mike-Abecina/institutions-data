"""
Meme-style metrics targeting the 18–34 demographic: student life, early career,
housing independence, relationships, and viral/funny signals.

Input:  geo_mapping/output/institutions_pow_metrics.csv
Output: geo_mapping/output/institutions_meme_metrics.csv

Census tables used
------------------
Already in census dir (extracted by download_abs_data.py):
  G04A  — age denominators: 15–19, 20–24, 25–29, 30–34
  G15   — tertiary enrolment FT/PT by age
  G17A  — personal income by sex × age
  G54A  — industry of employment M_ × age
  G54B  — industry of employment F_ × age
  G60B  — occupation × age (persons)

Extracted at runtime from /tmp/gcp_check.zip:
  G05   — marital status by sex × age
  G27A  — family/household type M_/F_ × age (GrpH, NDpChl, Ptn...)
  G27B  — family/household type F_/P_ × age (has all P_ person totals)
  G46A  — labour force status M_/F_ × age (employed FT/PT, unemployed, LF)

Metrics (20 total)
------------------
Group C — raw % metrics (→ _norm clipped 0–100):
  uni_town_index         Tertiary students aged 15–24 as % of all enrolled
  low_income_youth_pct   20–24 year olds earning <$500/wk as % of 20–24 LF
  sharehouse_capital     Group household members aged 15–34 as % of 15–34 pop
  first_job_energy       Employment rate of 20–24 year olds
  side_hustle_generation Part-time workers aged 20–34 as % of all employed 20–34
  promotion_pipeline     Managers + professionals aged 25–34 as % of all 25–34 employed
  startup_dreamer_density Tech/creative/science workers aged 20–34 as % of all employed 20–34
  bank_of_mum_and_dad    25–34 yo with no dependent children as % of 25–34 pop
  peter_pan_index        Compound: NDpChl rate × 30–34 share of 25–34 cohort
  adulting_score         Lone persons + partnered (no kids) aged 25–34 as % of 25–34 pop
  singles_scene          Never-married 20–34 year olds as % of 20–34 pop
  dink_potential         Partnered (no kids) 25–34 as % of 25–34 pop
  just_one_more_degree   Tertiary students aged 25+ as % of all enrolled

Group B — composites of normed cols (stored after Group C norms are computed):
  ramen_economy_score    mean(student_bubble + renter_republic + low_income_youth)
  delayed_adulting_score mean(singles_scene + sharehouse_capital + renter_republic)

Group A — composites of any normed cols (computed last):
  all_nighter_index      mean(student_bubble + night_economy)
  rent_forever_index     0.6×renter_republic + 0.4×student_bubble
  nightlife_index        mean(night_economy + entertainment + food_scene)
  digital_nomad_potential mean(wfh + knowledge_hub + knowledge_worker)
  flat_white_density     mean(food_scene + knowledge_hub + startup_dreamer)
  global_youth_hub       overseas_born_pct × youth_pct / 100, scaled to 0–100
"""

import io
import math
import sys
import zipfile
from pathlib import Path

import pandas as pd

CENSUS_DIR = Path(__file__).parent / "abs_data" / "census"
OUTPUT_DIR = Path(__file__).parent / "output"
INPUT_FILE = OUTPUT_DIR / "institutions_pow_metrics.csv"
OUTPUT_FILE = OUTPUT_DIR / "institutions_meme_metrics.csv"
GCP_ZIP    = Path("/tmp/gcp_check.zip")


# ── Helpers ───────────────────────────────────────────────────────────────────

def read_census(table_code):
    files = list(CENSUS_DIR.glob(f"*{table_code}_AUST_SA2.csv"))
    if not files:
        print(f"  WARNING: {table_code} not found in {CENSUS_DIR}")
        return pd.DataFrame()
    df = pd.read_csv(files[0])
    df["sa2_code"] = df["SA2_CODE_2021"].astype(str).str.zfill(9)
    return df


def read_from_zip(table_code):
    """Extract a GCP table from /tmp/gcp_check.zip at runtime."""
    if not GCP_ZIP.exists():
        print(f"  WARNING: {GCP_ZIP} not found — {table_code} skipped")
        return pd.DataFrame()
    with zipfile.ZipFile(GCP_ZIP) as z:
        matches = [n for n in z.namelist() if f"_{table_code}_" in n and n.endswith(".csv")]
        if not matches:
            print(f"  WARNING: {table_code} not found in {GCP_ZIP}")
            return pd.DataFrame()
        df = pd.read_csv(io.BytesIO(z.read(matches[0])))
    df["sa2_code"] = df["SA2_CODE_2021"].astype(str).str.zfill(9)
    return df


def _col(df, name):
    """Return column as numeric Series, zeros if column missing."""
    return (
        pd.to_numeric(df[name], errors="coerce").fillna(0)
        if name in df.columns
        else pd.Series(0, index=df.index)
    )


def safe_pct(num, denom):
    return (num / denom.replace(0, float("nan")) * 100).round(1)


def safe_ratio(num, denom):
    return (num / denom.replace(0, float("nan"))).round(3)


# ── Group C: raw metric calculators ──────────────────────────────────────────

def metric_uni_town_index(g15):
    """% of all enrolled students who are FT tertiary students aged 15–24.
    High = genuine university town, not just continuing-ed for adults."""
    uni_ft   = _col(g15, "Tert_Uni_oth_h_edu_Ft_15_24_P")
    voc_ft   = _col(g15, "Tert_Voc_edu_Ft_15_24_P")
    total    = pd.to_numeric(g15["Tot_P"], errors="coerce")
    out = g15[["sa2_code"]].copy()
    out["uni_town_index"] = safe_pct(uni_ft + voc_ft, total)
    return out


def metric_just_one_more_degree(g15):
    """% of all enrolled students who are 25+ (FT or PT).
    High = area draws adult learners, postgrads, and serial degree-collectors."""
    uni_ft  = _col(g15, "Tert_Uni_oth_h_edu_Ft_25_ov_P")
    uni_pt  = _col(g15, "Tert_Uni_oth_h_edu_Pt_25_ov_P")
    voc_ft  = _col(g15, "Tert_Voc_edu_Ft_25_ov_P")
    voc_pt  = _col(g15, "Tert_Voc_edu_Pt_25_ov_P")
    total   = pd.to_numeric(g15["Tot_P"], errors="coerce")
    out = g15[["sa2_code"]].copy()
    out["just_one_more_degree"] = safe_pct(uni_ft + uni_pt + voc_ft + voc_pt, total)
    return out


def metric_low_income_youth_pct(g17a):
    """% of 20–24 year olds earning under $500/wk.
    High = ramen budget territory — think share houses and avocado-toast guilt."""
    low_brackets = ["Neg_Nil_income", "1_149", "150_299", "300_399", "400_499"]
    low_m = sum(_col(g17a, f"M_{b}_20_24_yrs") for b in low_brackets)
    low_f = sum(_col(g17a, f"F_{b}_20_24_yrs") for b in low_brackets)
    total = _col(g17a, "M_Tot_20_24_yrs") + _col(g17a, "F_Tot_20_24_yrs")
    out = g17a[["sa2_code"]].copy()
    out["low_income_youth_pct"] = safe_pct(low_m + low_f, total)
    return out


def metric_sharehouse_capital(g27b, g04a):
    """% of 15–34 year olds living in group households.
    High = sharehouse culture, rotating housemates, mystery leftovers."""
    grp = _col(g27b, "P_GrpH_Mem_15_24") + _col(g27b, "P_GrpH_Mem_25_34")
    pop = (
        _col(g04a, "Age_yr_15_19_P") + _col(g04a, "Age_yr_20_24_P")
        + _col(g04a, "Age_yr_25_29_P") + _col(g04a, "Age_yr_30_34_P")
    )
    out = g27b[["sa2_code"]].copy()
    out["sharehouse_capital"] = safe_pct(grp, pop)
    return out


def metric_bank_of_mum_and_dad(g27b, g04a):
    """% of 25–34 year olds with no dependent children.
    High = still unburdened by school fees — or possibly funded by parental generosity."""
    ndp   = _col(g27b, "P_NDpChl_25_34")
    pop   = _col(g04a, "Age_yr_25_29_P") + _col(g04a, "Age_yr_30_34_P")
    out = g27b[["sa2_code"]].copy()
    out["bank_of_mum_and_dad"] = safe_pct(ndp, pop)
    return out


def metric_peter_pan_index(g27b, g04a):
    """Compound: NDpChl rate × fraction of 25–34 cohort who are 30–34.
    High = lots of 30-somethings who still have no kids — the Peter Pan zone."""
    ndp      = _col(g27b, "P_NDpChl_25_34")
    pop25_34 = (_col(g04a, "Age_yr_25_29_P") + _col(g04a, "Age_yr_30_34_P")).replace(0, float("nan"))
    pop30_34 = _col(g04a, "Age_yr_30_34_P")
    # NDpChl rate × share of cohort that's 30-34, scaled to %
    val = (ndp / pop25_34) * (pop30_34 / pop25_34) * 100
    out = g27b[["sa2_code"]].copy()
    out["peter_pan_index"] = val.round(1)
    return out


def metric_adulting_score(g27b, g04a):
    """% of 25–34 year olds living alone or partnered without kids.
    High = independent adults making rent, owning plants, and meal-prepping."""
    lone   = _col(g27b, "P_LonePsn_25_34")
    ptn_rm = _col(g27b, "P_Ptn_in_RM_25_34")
    ptn_df = _col(g27b, "P_Ptn_in_DFM_25_34")
    pop    = _col(g04a, "Age_yr_25_29_P") + _col(g04a, "Age_yr_30_34_P")
    out = g27b[["sa2_code"]].copy()
    out["adulting_score"] = safe_pct(lone + ptn_rm + ptn_df, pop)
    return out


def metric_singles_scene(g05):
    """% of 20–34 year olds who have never been married.
    High = dating-app active zone — lots of first dates and situationships."""
    never_m = _col(g05, "P_20_24_yr_Never_married") + _col(g05, "P_25_34_yr_Never_married")
    total   = _col(g05, "P_20_24_yr_Tot") + _col(g05, "P_25_34_yr_Tot")
    out = g05[["sa2_code"]].copy()
    out["singles_scene"] = safe_pct(never_m, total)
    return out


def metric_dink_potential(g27b, g04a):
    """% of 25–34 year olds who are partnered (registered or de-facto) with no kids.
    High = dual income, no kids energy — brunch every weekend, two car payments."""
    ptn   = _col(g27b, "P_Ptn_in_RM_25_34") + _col(g27b, "P_Ptn_in_DFM_25_34")
    pop   = _col(g04a, "Age_yr_25_29_P") + _col(g04a, "Age_yr_30_34_P")
    out = g27b[["sa2_code"]].copy()
    out["dink_potential"] = safe_pct(ptn, pop)
    return out


def metric_first_job_energy(g46a):
    """Employment rate (%) of 20–24 year olds in the labour force.
    High = young people here are actually finding work — not just vibing."""
    emp_m = _col(g46a, "M_Tot_Emp_20_24")
    emp_f = _col(g46a, "F_Tot_Emp_20_24")
    lf_m  = _col(g46a, "M_Tot_LF_20_24")
    # F_ has no Tot_LF column — compute as Emp + Unemp
    lf_f  = _col(g46a, "F_Tot_Emp_20_24") + _col(g46a, "F_Tot_Unemp_20_24")
    out = g46a[["sa2_code"]].copy()
    out["first_job_energy"] = safe_pct(emp_m + emp_f, lf_m + lf_f)
    return out


def metric_side_hustle_generation(g46a):
    """% of employed 20–34 year olds working part-time.
    High = gig economy hub — freelancers, baristas with a podcast, and UX consultants."""
    pt_m = _col(g46a, "M_Emp_PartT_20_24") + _col(g46a, "M_Emp_PartT_25_34")
    pt_f = _col(g46a, "F_Emp_PartT_20_24") + _col(g46a, "F_Emp_PartT_25_34")
    emp_m = _col(g46a, "M_Tot_Emp_20_24") + _col(g46a, "M_Tot_Emp_25_34")
    emp_f = _col(g46a, "F_Tot_Emp_20_24") + _col(g46a, "F_Tot_Emp_25_34")
    out = g46a[["sa2_code"]].copy()
    out["side_hustle_generation"] = safe_pct(pt_m + pt_f, emp_m + emp_f)
    return out


def metric_promotion_pipeline(g60b):
    """% of 25–34 employed workers in manager or professional roles.
    High = fast-track territory — people here are already running things."""
    mgr  = pd.to_numeric(g60b["P25_34_Managers"],     errors="coerce").fillna(0)
    pro  = pd.to_numeric(g60b["P25_34_Professionals"], errors="coerce").fillna(0)
    tot  = pd.to_numeric(g60b["P25_34_Tot"],           errors="coerce")
    out = g60b[["sa2_code"]].copy()
    out["promotion_pipeline"] = safe_pct(mgr + pro, tot)
    return out


def metric_startup_dreamer_density(g54a, g54b_aligned):
    """% of employed 20–34 year olds in tech, creative, or science industries.
    High = startup ecosystem — people here are building things and disrupting stuff."""
    creative_cols = ["Info_media_teleco", "Art_recn", "Pro_scien_tec"]
    num = sum(
        _col(g54a, f"M_{c}_20_24") + _col(g54a, f"M_{c}_25_34")
        + _col(g54b_aligned, f"F_{c}_20_24") + _col(g54b_aligned, f"F_{c}_25_34")
        for c in creative_cols
    )
    # Denominator: all M_ and F_ industry cols for age groups 20_24 and 25_34
    m_20_24 = [c for c in g54a.columns if c.startswith("M_") and c.endswith("_20_24")]
    m_25_34 = [c for c in g54a.columns if c.startswith("M_") and c.endswith("_25_34")]
    f_20_24 = [c for c in g54b_aligned.columns if c.startswith("F_") and c.endswith("_20_24")]
    f_25_34 = [c for c in g54b_aligned.columns if c.startswith("F_") and c.endswith("_25_34")]
    denom = (
        g54a[m_20_24].apply(pd.to_numeric, errors="coerce").fillna(0).sum(axis=1)
        + g54a[m_25_34].apply(pd.to_numeric, errors="coerce").fillna(0).sum(axis=1)
        + g54b_aligned[f_20_24].apply(pd.to_numeric, errors="coerce").fillna(0).sum(axis=1)
        + g54b_aligned[f_25_34].apply(pd.to_numeric, errors="coerce").fillna(0).sum(axis=1)
    ).replace(0, float("nan"))
    out = g54a[["sa2_code"]].copy()
    out["startup_dreamer_density"] = (num / denom * 100).round(1)
    return out


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not INPUT_FILE.exists():
        print(f"ERROR: {INPUT_FILE} not found. Run compute_pow_metrics.py first.")
        sys.exit(1)

    print("Loading institutions...")
    inst = pd.read_csv(INPUT_FILE, dtype={"sa2_code": str})
    inst["sa2_code"] = inst["sa2_code"].str.zfill(9)
    print(f"  {len(inst):,} institutions")

    # ── Load census tables ────────────────────────────────────────────────────
    print("\nLoading census tables from census dir...")
    g04a = read_census("G04A")
    g15  = read_census("G15")
    g17a = read_census("G17A")
    g54a = read_census("G54A")
    g54b = read_census("G54B")
    g60b = read_census("G60B")

    print("\nExtracting tables from GCP zip...")
    g05  = read_from_zip("G05")
    g27b = read_from_zip("G27B")
    g46a = read_from_zip("G46A")

    # Align G54B to G54A row order for consistent row-wise arithmetic
    if not g54a.empty and not g54b.empty:
        g54b_aligned = g54b.set_index("sa2_code").reindex(g54a["sa2_code"].values).reset_index()
    else:
        g54b_aligned = g54b

    # Align G04A to G27B for row-wise ops on living-arrangement metrics
    if not g27b.empty and not g04a.empty:
        g04a_aligned = g04a.set_index("sa2_code").reindex(g27b["sa2_code"].values).reset_index()
    else:
        g04a_aligned = g04a

    # ── Compute Group C: raw metrics ──────────────────────────────────────────
    print("\nComputing Group C metrics (raw)...")
    metrics = {}

    def add(name, df):
        if not df.empty and len(df.columns) > 1:
            df["sa2_code"] = df["sa2_code"].astype(str).str.zfill(9)
            metrics[name] = df
            print(f"  ✓ {name}")
        else:
            print(f"  ✗ {name} (skipped)")

    if not g15.empty:
        add("uni_town_index",         metric_uni_town_index(g15))
        add("just_one_more_degree",   metric_just_one_more_degree(g15))
    if not g17a.empty:
        add("low_income_youth_pct",   metric_low_income_youth_pct(g17a))
    if not g27b.empty and not g04a.empty:
        add("sharehouse_capital",     metric_sharehouse_capital(g27b, g04a_aligned))
        add("bank_of_mum_and_dad",    metric_bank_of_mum_and_dad(g27b, g04a_aligned))
        add("peter_pan_index",        metric_peter_pan_index(g27b, g04a_aligned))
        add("adulting_score",         metric_adulting_score(g27b, g04a_aligned))
        add("dink_potential",         metric_dink_potential(g27b, g04a_aligned))
    if not g05.empty:
        add("singles_scene",          metric_singles_scene(g05))
    if not g46a.empty:
        add("first_job_energy",       metric_first_job_energy(g46a))
        add("side_hustle_generation", metric_side_hustle_generation(g46a))
    if not g60b.empty:
        add("promotion_pipeline",     metric_promotion_pipeline(g60b))
    if not g54a.empty and not g54b_aligned.empty:
        add("startup_dreamer_density", metric_startup_dreamer_density(g54a, g54b_aligned))

    # ── Merge into institutions ───────────────────────────────────────────────
    print("\nMerging into institutions...")
    result = inst.copy()
    for name, df in metrics.items():
        df["sa2_code"] = df["sa2_code"].astype(str).str.zfill(9)
        result = result.merge(df, on="sa2_code", how="left")

    # ── Normalise Group C pct cols → _norm (clip 0–100) ──────────────────────
    pct_cols = [
        "uni_town_index", "low_income_youth_pct", "sharehouse_capital",
        "first_job_energy", "side_hustle_generation", "promotion_pipeline",
        "startup_dreamer_density", "bank_of_mum_and_dad", "peter_pan_index",
        "adulting_score", "singles_scene", "dink_potential", "just_one_more_degree",
    ]
    print("\nNormalising Group C cols (clip 0–100)...")
    for col in pct_cols:
        if col in result.columns:
            result[f"{col}_norm"] = result[col].clip(0, 100).round(1)

    # ── Group B composites (need _norm from Group C above) ────────────────────
    print("\nComputing Group B composites...")

    def composite(cols, weights=None):
        """Mean (or weighted sum) of available _norm cols. Returns Series 0–100."""
        available = [c for c in cols if c in result.columns]
        if not available:
            return pd.Series(float("nan"), index=result.index)
        if weights:
            w = [weights[c] for c in available]
            total_w = sum(w)
            return sum(result[c] * (weights[c] / total_w) for c in available).round(1)
        return result[available].mean(axis=1).round(1)

    def store_composite(col, series):
        """Store composite as both col and col_norm (both are 0–100 scale)."""
        result[col] = series
        result[f"{col}_norm"] = series

    store_composite(
        "ramen_economy_score",
        composite(["student_bubble_pct_norm", "renter_republic_pct_norm", "low_income_youth_pct_norm"]),
    )
    store_composite(
        "delayed_adulting_score",
        composite(["singles_scene_norm", "sharehouse_capital_norm", "renter_republic_pct_norm"]),
    )

    # ── Group A composites (can use any _norm cols now available) ─────────────
    print("Computing Group A composites...")

    store_composite(
        "all_nighter_index",
        composite(["student_bubble_pct_norm", "night_economy_pct_norm"]),
    )
    store_composite(
        "rent_forever_index",
        composite(
            ["renter_republic_pct_norm", "student_bubble_pct_norm"],
            weights={"renter_republic_pct_norm": 0.6, "student_bubble_pct_norm": 0.4},
        ),
    )
    store_composite(
        "nightlife_index",
        composite(["night_economy_pct_norm", "entertainment_pct_norm", "food_scene_pct_norm"]),
    )
    store_composite(
        "digital_nomad_potential",
        composite(["wfh_pct_norm", "knowledge_hub_pct_norm", "knowledge_worker_pct_norm"]),
    )
    store_composite(
        "flat_white_density",
        composite(["food_scene_pct_norm", "knowledge_hub_pct_norm", "startup_dreamer_density_norm"]),
    )

    # global_youth_hub: overseas_born_pct × youth_pct / 100, scale to 0–100
    if "overseas_born_pct" in result.columns and "youth_pct" in result.columns:
        raw_hub = (
            pd.to_numeric(result["overseas_born_pct"], errors="coerce")
            * pd.to_numeric(result["youth_pct"], errors="coerce")
            / 100
        )
        p99 = raw_hub.quantile(0.99)
        scaled = (raw_hub / p99 * 100).clip(0, 100).round(1)
        result["global_youth_hub"] = scaled
        result["global_youth_hub_norm"] = scaled

    # ── Write output ──────────────────────────────────────────────────────────
    OUTPUT_DIR.mkdir(exist_ok=True)
    result.to_csv(OUTPUT_FILE, index=False)

    # ── Summary ───────────────────────────────────────────────────────────────
    all_metric_cols = pct_cols + [
        "ramen_economy_score", "delayed_adulting_score",
        "all_nighter_index", "rent_forever_index", "nightlife_index",
        "digital_nomad_potential", "flat_white_density", "global_youth_hub",
    ]
    print(f"\n{'Metric':<30} {'Cover':>7}  {'Mean':>8}  {'Min':>8}  {'Max':>8}")
    print("-" * 70)
    for col in all_metric_cols:
        if col in result.columns:
            s = result[col].dropna()
            cov = result[col].notna().mean() * 100
            print(f"  {col:<28} {cov:>5.1f}%  {s.mean():>8.2f}  {s.min():>8.2f}  {s.max():>8.2f}")

    print(f"\nOutput: {OUTPUT_FILE}")
    print(f"Columns: {len(result.columns)} total  |  Institutions: {len(result):,}")


if __name__ == "__main__":
    main()
