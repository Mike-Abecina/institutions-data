"""
Build enriched export table for the product team.

Takes the mega_table and adds 4 annotation columns per metric:
  {col}_band   — percentile band label  (e.g. "Top 20%")
  {col}_emoji  — band emoji             (e.g. "✅")
  {col}_desc   — plain-English description of what the metric measures
  {col}_signal — the relevant ✅ or ❌ implication for this institution's score

Bands are computed against the SA2-deduplicated national distribution
(same method as the Streamlit app).

Input:  acir_db/aggregations/output/mega_table.csv
Output: acir_db/aggregations/output/export_table.csv

Usage:
    python acir_db/aggregations/build_export_table.py
"""

import sys
from pathlib import Path

import pandas as pd

ROOT       = Path(__file__).parent.parent.parent
MEGA_TABLE = Path(__file__).parent / "output" / "mega_table.csv"
OUTPUT     = Path(__file__).parent / "output" / "export_table.csv"

# ── metric registry (col, display_title, description, good_bad) ──────────────
# Copied from streamlit_app_v2.py — single source of truth for labels/descriptions.

METRIC_REGISTRY = [
    ("car_jail_score",        "Car Jail Score",
     "% of dwellings with zero cars.",
     "✅ High: car-free life is realistic — no rego or parking stress.\n❌ Low: owning a car is non-negotiable."),
    ("car_free_commute_pct",  "Car-Free Commute",
     "% of workers not using a private car to commute.",
     "✅ High: real public transport or walkable commutes.\n❌ Low: everyone drives — painful without a car."),
    ("wfh_pct",               "WFH Culture Index",
     "% of workers who worked from home.",
     "✅ High: café culture, co-working, flexible work is the norm.\n❌ Low: suburb empties out 8–5."),
    ("pedal_path_pct",        "Pedal & Path Score",
     "% cycling or walking to work.",
     "✅ High: flat, safe, bikeable streets.\n❌ Low: not built for bikes or feet."),
    ("night_economy_pct",     "Night Shift Neighbours",
     "% of residents in hospitality + arts/rec.",
     "✅ High: young, social, culturally active resident community.\n❌ Low: 9-to-5 workforce dominates."),
    ("knowledge_worker_pct",  "Professional Neighbours",
     "% of residents in professional, education, and health jobs.",
     "✅ High: educated neighbours, good for informal networking.\n❌ Low: fewer professionals in the local mix."),
    ("student_bubble_pct",    "Student Bubble Density",
     "% of 15–24 population attending uni or TAFE.",
     "✅ High: genuine student area — cheap food, campus events.\n❌ Low: students are a minority here."),
    ("renter_republic_pct",   "Renter Republic Score",
     "% of dwellings being rented.",
     "✅ High: renting is the norm — listings plentiful.\n❌ Low: owner-occupier territory, thin rental stock."),
    ("vertical_city_pct",     "Vertical City Score",
     "% of dwellings that are flats or apartments.",
     "✅ High: dense living — more stock, closer to transit.\n❌ Low: house-dominated, everything is spread out."),
    ("housing_stress_ratio",  "Housing Stress Ratio",
     "Annual rent as % of personal income. High is bad.",
     "✅ Low: rent is manageable.\n❌ High: rent swallows income every fortnight."),
    ("fresh_energy_pct",      "Fresh Energy Score",
     "% who moved here in the last 12 months.",
     "✅ High: lots of newcomers, easy to meet people.\n❌ Low: settled community, hard to break into."),
    ("community_glue_pct",    "Community Glue Score",
     "% of 15+ doing voluntary work.",
     "✅ High: strong local fabric — clubs, events, easy to get involved.\n❌ Low: anonymous, transient living."),
    ("global_mix_score",      "Global Mix Score",
     "Avg of overseas-born % and non-English spoken at home %.",
     "✅ High: multicultural food, events, diverse friend groups.\n❌ Low: less cultural diversity."),
    ("social_scene_score",    "Social Scene Score",
     "Food + Entertainment workers combined (place-of-work).",
     "✅ High: real venues and food nearby.\n❌ Low: nothing much happening on nights or weekends."),
    ("food_scene_pct",        "Food & Drink Scene",
     "Food & hospitality workers as a share of all workers commuting into this area.",
     "✅ High: food and hospitality make up a significant share of what this area does.\n❌ Low: thin on food options, or diluted by a large corporate workforce."),
    ("entertainment_pct",     "Entertainment Quarter",
     "Arts & Recreation workers signal venues, theatres, studios.",
     "✅ High: theatres, live music, gyms and galleries physically exist here.\n❌ Low: travel for anything that isn't Netflix."),
    ("healthcare_access_pct", "Healthcare Access",
     "Clinics + hospitals measured by health workers commuting in.",
     "✅ High: GPs and specialists are actually here.\n❌ Low: when you're sick, add travel time."),
    ("education_hub_pct",     "Education Hub",
     "Schools, unis, tutoring — measured by education workers commuting in.",
     "✅ High: real campus infrastructure and libraries nearby.\n❌ Low: commuting to campus from a suburb with nothing."),
    ("retail_density_pct",    "Shops & Markets",
     "Retail workers signal everyday convenience — groceries, pharmacies.",
     "✅ High: groceries and pharmacies exist locally.\n❌ Low: every errand is a car trip."),
    ("civic_services_pct",    "Civic Infrastructure",
     "Public Admin & Safety workers — councils, courts, emergency services.",
     "✅ High: real services exist here.\n❌ Low: bedroom suburb, limited local support."),
    ("knowledge_hub_pct",     "Knowledge Economy Hub",
     "Professional/Scientific/Tech + Finance workers flowing in.",
     "✅ High: internships and grad jobs within reach.\n❌ Low: professionals commute away."),
    ("job_gravity_ratio",     "Job Gravity",
     "More jobs than local workers = area pulls people in.",
     "✅ High: bustling by day, more opportunity for casual shifts.\n❌ Low: bedroom suburb."),
    ("qualification_density", "Qualification Density",
     "% of residents (15+) with a bachelor's degree or higher.",
     "✅ High: educated neighbourhood.\n❌ Low: fewer uni-educated locals."),
    ("grad_capture_rate",     "Grad Capture Rate",
     "% of 25–34 year olds who hold a post-school qualification.",
     "✅ High: graduates stay — sign of real jobs and lifestyle.\n❌ Low: graduates leave."),
    ("professional_job_pct",  "Professional Job Density",
     "% of employed workers in manager or professional roles.",
     "✅ High: career-track jobs exist locally.\n❌ Low: fewer professional roles locally."),
    ("stem_field_pct",        "STEM Field Concentration",
     "% of qualified residents whose field was STEM.",
     "✅ High: tech and science cluster.\n❌ Low: not a STEM cluster."),
    ("income_growth_signal",  "Income Growth Signal",
     "% of 15+ population earning $1,500+/week.",
     "✅ High: strong earning potential once qualified.\n❌ Low: fewer high-income earners."),
    ("employment_rate",       "Employment Rate",
     "Employment-to-population ratio for 15+ year olds.",
     "✅ High: jobs are easy to find.\n❌ Low: tighter local job market."),
    ("mortgage_stress_pct",   "Mortgage Stress",
     "% of mortgaged dwellings paying $3,000+/month. High is bad.",
     "✅ Low: housing is relatively affordable to buy.\n❌ High: enormous deposit required."),
    ("uni_town_index",        "Uni Town Index",
     "FT tertiary students 15–24 as % of all enrolled students.",
     "✅ High: genuine uni town.\n❌ Low: suburb doesn't know or care about O-Week."),
    ("ramen_economy_score",   "Ramen Economy Score",
     "Composite: student density + renters + low-income youth.",
     "✅ High: classic broke-but-happy energy.\n❌ Low: more expensive and professional."),
    ("sharehouse_capital",    "Sharehouse Capital",
     "Group household members 15–34 as % of 15–34 population.",
     "✅ High: share houses are the norm.\n❌ Low: listings sparse, fight for every room."),
    ("all_nighter_index",     "All-Nighter Index",
     "Composite: student density + night economy.",
     "✅ High: study mode and 3am kebab runs both viable.\n❌ Low: lights go out early."),
    ("first_job_energy",      "First Job Energy",
     "Employment rate of 20–24 year olds in the labour force.",
     "✅ High: 20–24 year olds are actually employed here.\n❌ Low: youth unemployment is higher."),
    ("promotion_pipeline",    "Promotion Pipeline",
     "Managers + professionals aged 25–34 as % of all 25–34 employed.",
     "✅ High: steep trajectory achievable.\n❌ Low: career progression requires commuting further."),
    ("startup_dreamer_density","Startup Dreamer Density",
     "Tech, creative and science workers aged 20–34.",
     "✅ High: startup ecosystem, side projects, informal mentoring.\n❌ Low: not a startup hub."),
    ("side_hustle_generation","Side Hustle Generation",
     "Part-time workers aged 20–34 as % of all employed 20–34.",
     "✅ High: casual jobs plentiful.\n❌ Low: fewer casual options."),
    ("bank_of_mum_and_dad",   "Bank of Mum & Dad",
     "25–34 year olds with no dependent children as % of 25–34 population.",
     "✅ High: lots of people in the same life stage.\n❌ Low: area has shifted to family mode."),
    ("peter_pan_index",       "Peter Pan Index",
     "NDpChl rate × 30–34 share of 25–34 cohort.",
     "✅ High: people in their 30s are still kid-free and socially active.\n❌ Low: by 30 here, most have moved into family mode."),
    ("adulting_score",        "Adulting Score",
     "Lone persons + partnered (no kids) 25–34 as % of 25–34 population.",
     "✅ High: independent living is the norm.\n❌ Low: many still live in family households."),
    ("rent_forever_index",    "Rent Forever Index",
     "Composite: renters (60%) + student density (40%).",
     "✅ High: renting is the default lifestyle.\n❌ Low: area assumes you're on a path to ownership."),
    ("singles_scene",         "Singles Scene",
     "Never-married 20–34 year olds as % of all 20–34.",
     "✅ High: dating culture is normal.\n❌ Low: couples suburb."),
    ("dink_potential",        "DINK Potential",
     "Partnered (no kids) 25–34 as % of 25–34 population.",
     "✅ High: dual-income couples' spending power flows into local venues.\n❌ Low: singles-dominated or already into kids territory."),
    ("delayed_adulting_score","Delayed Adulting Score",
     "Composite: singles + sharehouse + renters.",
     "✅ High: classic quarter-life territory.\n❌ Low: area has moved on."),
    ("global_youth_hub",      "Global Youth Hub",
     "Overseas-born × youth density compound score.",
     "✅ High: young, international, multilingual.\n❌ Low: older or locally-born population."),
    ("nightlife_index",       "Nightlife Index",
     "Composite: night economy + entertainment + food scene.",
     "✅ High: actual nightlife — real venues, real bars.\n❌ Low: footpath rolls up after dinner."),
    ("digital_nomad_potential","Digital Nomad Potential",
     "Composite: WFH culture + knowledge hub + knowledge workers.",
     "✅ High: laptop-and-latte territory.\n❌ Low: very 9-to-5."),
    ("just_one_more_degree",  "Just One More Degree",
     "Tertiary students aged 25+ as % of all enrolled.",
     "✅ High: postgrad culture is strong.\n❌ Low: studying is firmly for under-25s here."),
    ("flat_white_density",    "Flat White Density",
     "Composite: food scene + knowledge hub + startup dreamers.",
     "✅ High: good coffee, smart people, startup energy.\n❌ Low: not that kind of suburb."),
]

LOW_BANDS = {"Bottom 5%", "Bottom 10%", "Bottom 20%", "Bottom 30%"}


# ── band logic (mirrors Streamlit app) ───────────────────────────────────────

def percentile_band(val, arr):
    if pd.isna(val) or len(arr) == 0:
        return "", ""
    rank_pct = (arr < val).sum() / len(arr) * 100
    if rank_pct >= 95: return "🔥", "Top 5%"
    if rank_pct >= 90: return "⭐", "Top 10%"
    if rank_pct >= 80: return "✅", "Top 20%"
    if rank_pct >= 70: return "🔼", "Top 30%"
    if rank_pct >= 30: return "➡️", "Middle"
    if rank_pct >= 20: return "🔽", "Bottom 30%"
    if rank_pct >= 10: return "⚠️", "Bottom 20%"
    if rank_pct >= 5:  return "🔴", "Bottom 10%"
    return "💀", "Bottom 5%"


def split_signals(good_bad):
    """Return (high_signal, low_signal) from the good_bad string."""
    high, low = "", ""
    for line in (good_bad or "").split("\n"):
        if line.startswith("✅"):
            high = line
        elif line.startswith("❌"):
            low = line
    return high, low


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading mega_table…")
    df = pd.read_csv(MEGA_TABLE, dtype={"organisation_id": str, "sa2_code": str})
    print(f"  {len(df):,} institutions, {len(df.columns)} columns")

    # Build national percentile arrays from SA2-deduplicated rows
    print("Computing national percentile distributions…")
    sa2_df = df.drop_duplicates(subset=["sa2_code"]).copy()
    pcts   = {
        col: sa2_df[col].dropna().values
        for col in df.columns if col.endswith("_norm")
    }

    # Stamp annotation columns for each registered metric
    print("Annotating metrics…")
    result = df.copy()
    annotated = 0

    for col, title, desc, good_bad in METRIC_REGISTRY:
        norm_col = f"{col}_norm"
        if norm_col not in df.columns:
            continue

        arr = pcts.get(norm_col, [])
        high_signal, low_signal = split_signals(good_bad)

        emojis = []
        bands  = []

        for val in df[norm_col]:
            emoji, band = percentile_band(val, arr)
            emojis.append(emoji)
            bands.append(band)

        result[f"{col}_emoji"]        = emojis
        result[f"{col}_band"]         = bands
        result[f"{col}_signal_high"]  = high_signal   # same for every row — static
        result[f"{col}_signal_low"]   = low_signal    # same for every row — static
        result[f"{col}_desc"]         = desc          # same for every row — static
        annotated += 1

    print(f"  {annotated} metrics annotated (4 cols each)")

    result.to_csv(OUTPUT, index=False)

    print(f"\nOutput  : {OUTPUT}")
    print(f"Rows    : {len(result):,}")
    print(f"Columns : {len(result.columns)}  "
          f"(was {len(df.columns)} + {len(result.columns) - len(df.columns)} new annotation cols)"
          f"\n          (5 per metric: _emoji, _band, _signal_high, _signal_low, _desc)")


if __name__ == "__main__":
    main()
