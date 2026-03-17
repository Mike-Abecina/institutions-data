"""
Institution Explorer v2 — Streamlit UI
=======================================
4-tab redesign using the mega_table which combines SA2/Census metrics
with institution-level course and career aggregates.

Tabs:
  📍 Overview        — hero, quick stats, auto AI description
  🏙️ Neighbourhood   — vibe/lifestyle/POW/meme metrics (Census/SA2)
  🎓 Courses & Entry — course mix, fees, ATAR, IELTS
  💼 Careers         — top careers, domain breakdown (STEM/Health/etc.)

Run:
    .venv/bin/streamlit run streamlit_app_v2.py
"""

import json
import os
import re
import requests
import pandas as pd
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

from acir_db.get_acir_data import get_data_from_file

# ── env ───────────────────────────────────────────────────────────────────────
load_dotenv()
SERPA_API    = os.getenv("SERPA_API")    or st.secrets.get("SERPA_API", "")
OPENAI_KEY   = os.getenv("OPENAI_API_KEY") or st.secrets.get("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPEN_AI_MODEL") or st.secrets.get("OPEN_AI_MODEL", "gpt-4o")

# ── paths ─────────────────────────────────────────────────────────────────────
ROOT          = Path(__file__).parent
DATA_PATH     = ROOT / "acir_db" / "aggregations" / "output" / "mega_table.csv"
ORGS_SQL      = ROOT / "acir_db" / "sql" / "organisations.sql"
ACIR_CSV_PATH = ROOT / "deploy" / "data" / "acir_institutions.csv"

# ── metric registry ───────────────────────────────────────────────────────────
SECTION_LIFESTYLE = "Lifestyle & Liveability"
SECTION_POW       = "What's Actually Here"
SECTION_STUDENT   = "Academic & Job Opportunity"
SECTION_MEME      = "Meme Metrics — Ages 18–34"

METRIC_REGISTRY = [
    # ── Lifestyle & Liveability ───────────────────────────────────────────
    ("car_jail_score",       "Car Jail Score",           "% of dwellings with zero cars.",
     "✅ High: car-free life is realistic — no rego or parking stress.\n❌ Low: owning a car is non-negotiable.",
     SECTION_LIFESTYLE),
    ("car_free_commute_pct", "Car-Free Commute",         "% of workers not using a private car to commute.",
     "✅ High: real public transport or walkable commutes.\n❌ Low: everyone drives — painful without a car.",
     SECTION_LIFESTYLE),
    ("wfh_pct",              "WFH Culture Index",        "% of workers who worked from home.",
     "✅ High: café culture, co-working, flexible work is the norm.\n❌ Low: suburb empties out 8–5.",
     SECTION_LIFESTYLE),
    ("pedal_path_pct",       "Pedal & Path Score",       "% cycling or walking to work.",
     "✅ High: flat, safe, bikeable streets — skip the gym, save on transport.\n❌ Low: not built for bikes or feet.",
     SECTION_LIFESTYLE),
    ("night_economy_pct",    "Night Shift Neighbours",   "% of residents in hospitality + arts/rec.",
     "✅ High: young, social, culturally active resident community.\n❌ Low: 9-to-5 workforce dominates who lives here.",
     SECTION_LIFESTYLE),
    ("knowledge_worker_pct", "Professional Neighbours",  "% of residents in professional, education, and health jobs.",
     "✅ High: educated neighbours, good for informal networking.\n❌ Low: fewer professionals in the local mix.",
     SECTION_LIFESTYLE),
    ("student_bubble_pct",   "Student Bubble Density",   "% of 15–24 population attending uni or TAFE.",
     "✅ High: genuine student area — cheap food, campus events, midnight study culture.\n❌ Low: students are a minority here.",
     SECTION_LIFESTYLE),
    ("renter_republic_pct",  "Renter Republic Score",    "% of dwellings being rented.",
     "✅ High: renting is the norm — landlords expect young tenants, listings plentiful.\n❌ Low: owner-occupier territory, thin rental stock.",
     SECTION_LIFESTYLE),
    ("vertical_city_pct",    "Vertical City Score",      "% of dwellings that are flats or apartments.",
     "✅ High: dense living — more stock, closer to transit and amenities.\n❌ Low: house-dominated, everything is spread out.",
     SECTION_LIFESTYLE),
    ("housing_stress_ratio", "Housing Stress Ratio",     "Annual rent as % of personal income. ⚠️ High is bad.",
     "✅ Low: rent is manageable — financial breathing room.\n❌ High: rent swallows income every fortnight.",
     SECTION_LIFESTYLE),
    ("fresh_energy_pct",     "Fresh Energy Score",       "% who moved here in the last 12 months.",
     "✅ High: lots of newcomers, easy to meet people, social networks forming.\n❌ Low: settled community, hard to break into.",
     SECTION_LIFESTYLE),
    ("community_glue_pct",   "Community Glue Score",     "% of 15+ doing voluntary work.",
     "✅ High: strong local fabric — clubs, events, easy to get involved.\n❌ Low: anonymous, transient living.",
     SECTION_LIFESTYLE),
    ("global_mix_score",     "Global Mix Score",         "Avg of overseas-born % and non-English spoken at home %.",
     "✅ High: multicultural food, events, diverse friend groups.\n❌ Low: less cultural diversity, homogeneous food scene.",
     SECTION_LIFESTYLE),

    # ── What's Actually Here (POW) ────────────────────────────────────────
    ("social_scene_score",   "Social Scene Score",       "Food + Entertainment workers combined.",
     "✅ High: real venues and food nearby — confirmed by workers who staff them.\n❌ Low: nothing much happening on nights or weekends.",
     SECTION_POW),
    ("food_scene_pct",       "Food & Drink Scene",       "Food & hospitality workers as a share of all workers commuting into this area. Note: major employment hubs can score lower here not because they lack restaurants, but because their huge corporate workforce dilutes the ratio.",
     "✅ High: food and hospitality make up a significant share of what this area actually does — real cafés, restaurants and bars with staff to prove it.\n❌ Low: either genuinely thin on food options, or a large corporate/government workforce is diluting the ratio.",
     SECTION_POW),
    ("entertainment_pct",    "Entertainment Quarter",    "Arts & Recreation workers signal venues, theatres, studios.",
     "✅ High: theatres, live music, gyms and galleries physically exist here.\n❌ Low: travel for anything that isn't Netflix.",
     SECTION_POW),
    ("healthcare_access_pct","Healthcare Access",        "Clinics + hospitals measured by health workers commuting in.",
     "✅ High: GPs and specialists are actually here.\n❌ Low: when you're sick, add travel time to the problem.",
     SECTION_POW),
    ("education_hub_pct",    "Education Hub",            "Schools, unis, tutoring — measured by education workers commuting in.",
     "✅ High: real campus infrastructure and libraries nearby.\n❌ Low: commuting to campus from a suburb with nothing in between.",
     SECTION_POW),
    ("retail_density_pct",   "Shops & Markets",          "Retail workers signal everyday convenience — groceries, pharmacies.",
     "✅ High: groceries and pharmacies exist locally — errands on foot.\n❌ Low: every errand is a car trip.",
     SECTION_POW),
    ("civic_services_pct",   "Civic Infrastructure",     "Public Admin & Safety workers — councils, courts, emergency services.",
     "✅ High: real services exist here — the boring stuff works when you need it.\n❌ Low: bedroom suburb, limited local support.",
     SECTION_POW),
    ("knowledge_hub_pct",    "Knowledge Economy Hub",    "Professional/Scientific/Tech + Finance workers flowing in.",
     "✅ High: high-value employers physically here — internships and grad jobs within reach.\n❌ Low: professionals commute away, fewer local career opportunities.",
     SECTION_POW),
    ("job_gravity_ratio",    "Job Gravity",              "More jobs than local workers = area pulls people in.",
     "✅ High: bustling by day, more opportunity for casual shifts or networking.\n❌ Low: bedroom suburb — everyone leaves every morning.",
     SECTION_POW),

    # ── Academic & Job Opportunity ────────────────────────────────────────
    ("qualification_density", "Qualification Density",   "% of residents (15+) with a bachelor's degree or higher.",
     "✅ High: educated neighbourhood — good amenities and serious career culture.\n❌ Low: fewer uni-educated locals, can feel intellectually quieter.",
     SECTION_STUDENT),
    ("grad_capture_rate",    "Grad Capture Rate",        "% of 25–34 year olds who hold a post-school qualification.",
     "✅ High: graduates stay — sign of real jobs and real lifestyle.\n❌ Low: graduates leave — it's a launchpad, not a destination.",
     SECTION_STUDENT),
    ("professional_job_pct", "Professional Job Density", "% of employed workers in manager or professional roles.",
     "✅ High: career-track jobs exist here without commuting to the CBD.\n❌ Low: fewer professional roles locally.",
     SECTION_STUDENT),
    ("stem_field_pct",       "STEM Field Concentration", "% of qualified residents whose field was STEM.",
     "✅ High: tech and science cluster — peers, events, internships more accessible.\n❌ Low: not a STEM cluster, relevant peers are scattered elsewhere.",
     SECTION_STUDENT),
    ("income_growth_signal", "Income Growth Signal",     "% of 15+ population earning $1,500+/week.",
     "✅ High: strong earning potential once qualified and working.\n❌ Low: fewer high-income earners — may reflect casualised job market.",
     SECTION_STUDENT),
    ("employment_rate",      "Employment Rate",          "Employment-to-population ratio for 15+ year olds.",
     "✅ High: jobs are easy to find — casual and grad roles alike.\n❌ Low: tighter local job market, harder without commuting further.",
     SECTION_STUDENT),
    ("mortgage_stress_pct",  "Mortgage Stress",          "% of mortgaged dwellings paying $3,000+/month. ⚠️ High is bad.",
     "✅ Low: housing is relatively affordable to buy here.\n❌ High: enormous deposit required — long-term renting is likely your reality.",
     SECTION_STUDENT),

    # ── Meme Metrics — Ages 18–34 ─────────────────────────────────────────
    ("uni_town_index",          "Uni Town Index",            "FT tertiary students 15–24 as % of all enrolled students.",
     "✅ High: genuine uni town — student discounts real, O-Week is everything.\n❌ Low: suburb doesn't know or care about O-Week.",
     SECTION_MEME),
    ("ramen_economy_score",     "Ramen Economy Score",       "Composite: student density + renters + low-income youth.",
     "✅ High: classic broke-but-happy energy.\n❌ Low: more expensive and professional — your budget will feel out of step.",
     SECTION_MEME),
    ("sharehouse_capital",      "Sharehouse Capital",        "Group household members 15–34 as % of 15–34 population.",
     "✅ High: share houses are the norm, finding housemates is easy.\n❌ Low: share house listings are sparse, fight for every room on Flatmates.",
     SECTION_MEME),
    ("all_nighter_index",       "All-Nighter Index",         "Composite: student density + night economy.",
     "✅ High: study mode and 3am kebab runs both viable.\n❌ Low: lights go out early, nowhere open at 2am.",
     SECTION_MEME),
    ("first_job_energy",        "First Job Energy",          "Employment rate of 20–24 year olds in the labour force.",
     "✅ High: 20–24 year olds are actually employed here.\n❌ Low: youth unemployment is higher — competition for entry-level is real.",
     SECTION_MEME),
    ("promotion_pipeline",      "Promotion Pipeline",        "Managers + professionals aged 25–34 as % of all 25–34 employed.",
     "✅ High: people in their late 20s are already running things — steep trajectory achievable.\n❌ Low: career progression requires commuting further afield.",
     SECTION_MEME),
    ("startup_dreamer_density", "Startup Dreamer Density",   "Tech, creative and science workers aged 20–34 as % of all employed 20–34.",
     "✅ High: startup ecosystem, side projects, informal mentoring are local fabric.\n❌ Low: not a startup or creative hub — your industry peers are mostly elsewhere.",
     SECTION_MEME),
    ("side_hustle_generation",  "Side Hustle Generation",    "Part-time workers aged 20–34 as % of all employed 20–34.",
     "✅ High: casual jobs plentiful, culture supports balancing study and work.\n❌ Low: fewer casual options — full-time or nothing seems to be the expectation.",
     SECTION_MEME),
    ("bank_of_mum_and_dad",     "Bank of Mum & Dad",         "25–34 year olds with no dependent children as % of 25–34 population.",
     "✅ High: lots of people in the same life stage, free to socialise.\n❌ Low: most 25–34 year olds here already have children — area has shifted to family mode.",
     SECTION_MEME),
    ("peter_pan_index",         "Peter Pan Index",           "NDpChl rate × 30–34 share of 25–34 cohort. Growing up is optional.",
     "✅ High: people in their 30s are still kid-free and socially active.\n❌ Low: by 30 here, most have moved into family mode.",
     SECTION_MEME),
    ("adulting_score",          "Adulting Score",            "Lone persons + partnered (no kids) 25–34 as % of 25–34 population.",
     "✅ High: independent living is the norm — living alone or with a partner.\n❌ Low: many still live in family households — reflects high costs or cultural norms.",
     SECTION_MEME),
    ("rent_forever_index",      "Rent Forever Index",        "Composite: renters (60%) + student density (40%).",
     "✅ High: renting is the default lifestyle here — not a temporary phase.\n❌ Low: the area assumes you're on a path to ownership.",
     SECTION_MEME),
    ("singles_scene",           "Singles Scene",             "Never-married 20–34 year olds as % of all 20–34.",
     "✅ High: dating culture is normal, nobody cares you haven't settled down.\n❌ Low: couples suburb — great if partnered, less so if you're not.",
     SECTION_MEME),
    ("dink_potential",          "DINK Potential",            "Partnered (no kids) 25–34 as % of 25–34 population.",
     "✅ High: dual-income couples' spending power flows into local venues and food.\n❌ Low: either singles-dominated or already into kids territory.",
     SECTION_MEME),
    ("delayed_adulting_score",  "Delayed Adulting Score",    "Composite: singles + sharehouse + renters.",
     "✅ High: classic quarter-life territory — everyone figuring it out together.\n❌ Low: area has moved on — settled, owned, partnered.",
     SECTION_MEME),
    ("global_youth_hub",        "Global Youth Hub",          "Overseas-born × youth density compound score.",
     "✅ High: young, international, multilingual — group chats in four languages.\n❌ Low: older or locally-born population, less international energy.",
     SECTION_MEME),
    ("nightlife_index",         "Nightlife Index",           "Composite: night economy + entertainment + food scene.",
     "✅ High: actual nightlife — real venues, real bars, real reason to stay out.\n❌ Low: footpath rolls up after dinner, every night out requires an Uber elsewhere.",
     SECTION_MEME),
    ("digital_nomad_potential", "Digital Nomad Potential",   "Composite: WFH culture + knowledge hub + knowledge workers.",
     "✅ High: laptop-and-latte territory — Slack at 11am, gym at 2pm.\n❌ Low: very 9-to-5, everyone commutes to an office.",
     SECTION_MEME),
    ("just_one_more_degree",    "Just One More Degree",      "Tertiary students aged 25+ as % of all enrolled.",
     "✅ High: postgrad culture is strong — second degree is unremarkable.\n❌ Low: studying is firmly for under-25s here.",
     SECTION_MEME),
    ("flat_white_density",      "Flat White Density",        "Composite: food scene + knowledge hub + startup dreamers.",
     "✅ High: good coffee, smart people, startup energy — all in the same room.\n❌ Low: not that kind of suburb — no one pitching a SaaS idea over a pour-over.",
     SECTION_MEME),
]

SECTIONS = [SECTION_LIFESTYLE, SECTION_POW, SECTION_STUDENT, SECTION_MEME]
SECTION_ICONS = {
    SECTION_LIFESTYLE: "🏡",
    SECTION_POW:       "📍",
    SECTION_STUDENT:   "🎓",
    SECTION_MEME:      "🔥",
}


# ── data loading ───────────────────────────────────────────────────────────────

@st.cache_data
def load_data():
    return pd.read_csv(DATA_PATH, dtype={"organisation_id": str, "sa2_code": str})


@st.cache_data
def load_acir_data():
    is_production = (
        (os.getenv("APP_ENV") or st.secrets.get("APP_ENV", "")).lower() == "production"
        or not os.getenv("DATABSE_HOST")  # preserves existing env var typo
    )
    if is_production:
        if not ACIR_CSV_PATH.exists():
            st.warning(
                "Production mode: deploy/data/acir_institutions.csv not found. "
                "Institution metadata unavailable. Run deploy/cache_acir_data.py locally and commit the CSV."
            )
            return {}
        df = pd.read_csv(ACIR_CSV_PATH, dtype={"organisation_id": str})
    else:
        df = get_data_from_file(str(ORGS_SQL))
        if df is None or df.empty:
            return {}

    primary     = df[df["site_primary_site"] == 1]
    non_primary = df[~df.index.isin(primary.index)]
    deduped = (
        pd.concat([primary, non_primary])
        .drop_duplicates(subset=["organisation_id"], keep="first")
        .set_index("organisation_id")
    )
    return deduped.to_dict(orient="index")


@st.cache_data
def compute_national_percentiles(df):
    sa2_df   = df.drop_duplicates(subset=["sa2_code"]).copy()
    norm_cols = [c for c in df.columns if c.endswith("_norm")]
    return {col: sa2_df[col].dropna().values for col in norm_cols}


# ── helpers ────────────────────────────────────────────────────────────────────

def percentile_band(val, arr):
    if pd.isna(val) or len(arr) == 0:
        return "—", ""
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


def fmt_val(v):
    if pd.isna(v):      return "—"
    if isinstance(v, float) and v == int(v): return str(int(v))
    if isinstance(v, float): return f"{v:.1f}"
    return str(v)


def fmt_currency(v):
    if pd.isna(v): return "—"
    return f"${int(v):,}"


def _str(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def parse_logo_url(logo_image_urls):
    raw = _str(logo_image_urls)
    if not raw:
        return None
    try:
        data = json.loads(raw)
        for key in ("original", "150x140", "100x100", "75x75", "50x50"):
            val = data.get(key, "")
            if val and val.startswith("http"):
                return val
        for key in ("original", "150x140", "100x100"):
            val = data.get(key, "")
            if val:
                return f"https://s3-ap-southeast-2.amazonaws.com/geg-sia-webapp2/{val}"
    except (json.JSONDecodeError, AttributeError):
        if raw.startswith("http"):
            return raw
    return None


def build_street_address(acir):
    parts = []
    street1  = _str(acir.get("site_street1"))
    street2  = _str(acir.get("site_street2"))
    suburb   = _str(acir.get("site_subrub"))   # typo in DB column name
    postcode = _str(acir.get("site_postcode")).replace(".0", "")
    if street1:  parts.append(street1)
    if street2:  parts.append(street2)
    if suburb:   parts.append(suburb.title())
    if postcode: parts.append(postcode)
    return ", ".join(parts) if parts else None


def strip_html(text):
    return re.sub(r"<[^>]+>", " ", text).strip() if text else ""


def fetch_image(institution_name, acir):
    logo_url = parse_logo_url(acir.get("logo_image_urls") if acir else None)
    if logo_url:
        return logo_url, "ACIR"
    if SERPA_API:
        try:
            resp = requests.post(
                "https://google.serper.dev/images",
                headers={"X-API-KEY": SERPA_API, "Content-Type": "application/json"},
                json={"q": institution_name},
                timeout=10,
                allow_redirects=True,
            )
            images = resp.json().get("images", [])
            if images:
                return images[0].get("imageUrl"), images[0].get("source", "Serper")
        except Exception:
            pass
    return None, None


# ── AI description ─────────────────────────────────────────────────────────────

def _metric_signals(metric_highlights):
    good_bad_lookup = {m[0]: m[3] for m in METRIC_REGISTRY}
    low_bands = {"Bottom 5%", "Bottom 10%", "Bottom 20%", "Bottom 30%"}
    signals = []
    for m in metric_highlights:
        gb = good_bad_lookup.get(m["col"], "")
        if not gb:
            continue
        is_low = m["band"] in low_bands
        lines  = gb.split("\n")
        chosen = None
        for line in lines:
            if is_low and line.startswith("❌"):
                chosen = line
            elif not is_low and line.startswith("✅"):
                chosen = line
        if chosen:
            text = re.sub(r"^[✅❌]\s+\*\*(High|Low):\*\*\s*", "", chosen).strip()
            signals.append(f"- {text}")
    return "\n".join(signals)


def generate_ai_description(row, acir, metric_highlights):
    if not OPENAI_KEY:
        return "_No OPENAI_API_KEY configured._"

    client = OpenAI(api_key=OPENAI_KEY)

    street_address = build_street_address(acir) if acir else None
    acir_suburb    = _str(acir.get("site_subrub")).title() if acir else ""
    acir_postcode  = _str(acir.get("site_postcode")).replace(".0", "") if acir else ""
    region_name    = _str(acir.get("region_name")) if acir else ""
    sector         = _str(acir.get("sector_name")) if acir else ""
    acir_desc      = strip_html(_str(acir.get("organisation_description"))) if acir else ""
    study_areas    = _str(acir.get("site_study_area")) if acir else ""
    accommodation  = _str(acir.get("site_accommodation")) if acir else ""
    transport      = _str(acir.get("site_transport")) if acir else ""
    campus_notes   = _str(acir.get("site_comments")) if acir else ""

    rent    = row.get("median_rent_weekly")
    seifa_d = row.get("seifa_irsad_decile")
    seifa_l = _str(row.get("seifa_label"))
    sunshine = row.get("sunshine_hours_yr")

    rent_str = f"${int(rent)}/week" if pd.notna(rent) else "N/A"
    rent_ctx = ""
    if pd.notna(rent):
        rent_ctx = ("— well above the national median" if rent > 500
                    else "— around the national median" if rent > 350
                    else "— below the national median")

    # Course facts for prompt
    total_courses   = row.get("total_courses")
    pct_ug          = row.get("pct_undergraduate")
    pct_pg          = row.get("pct_postgraduate")
    pct_vet         = row.get("pct_vet_vocational")
    atar_min        = row.get("atar_min")
    atar_max        = row.get("atar_max")
    top5_careers    = _str(row.get("top_5_careers")).split(" | ")[0] if _str(row.get("top_5_careers")) else "N/A"

    def _pct(v):
        return f"{int(v)}%" if pd.notna(v) and v > 0 else None

    course_mix_parts = [p for p in [
        f"{_pct(pct_ug)} undergraduate" if _pct(pct_ug) else None,
        f"{_pct(pct_pg)} postgraduate" if _pct(pct_pg) else None,
        f"{_pct(pct_vet)} VET/vocational" if _pct(pct_vet) else None,
    ] if p]
    course_mix = ", ".join(course_mix_parts) if course_mix_parts else "mix not available"

    atar_str = "N/A"
    if pd.notna(atar_min) and pd.notna(atar_max):
        atar_str = f"{int(atar_min)}–{int(atar_max)}"
    elif pd.notna(atar_max):
        atar_str = f"up to {int(atar_max)}"

    signals = _metric_signals(metric_highlights)

    prompt = f"""You are writing a short, engaging profile of an Australian education institution for 18–24 year olds deciding where to study and live.

RULES:
- Only use the facts provided below. Do not invent anything.
- Do NOT use any technical metric names, score names, index names, or percentile terminology.
- Do NOT use jargon. Write like a person, not a data analyst.
- The neighbourhood signals are plain-English observations from Census data — weave them naturally, don't list or cite them.

=== INSTITUTION ===
Name: {row['name']}
Type: {_str(row.get('org_type'))}
Sector: {sector or 'N/A'}

=== LOCATION ===
Street address: {street_address or 'N/A'}
Suburb: {acir_suburb or _str(row.get('suburb', '')).title()}
State: {_str(row.get('state'))}
SA2 area: {_str(row.get('sa2_name'))}
Region: {region_name}

=== CAMPUS ===
Study areas: {study_areas or 'N/A'}
Transport: {transport or 'N/A'}
Accommodation nearby: {accommodation or 'N/A'}
Campus notes: {campus_notes or 'N/A'}

=== ABOUT ===
{acir_desc[:500] if acir_desc else 'N/A'}

=== COURSES & ENTRY ===
Total courses: {int(total_courses) if pd.notna(total_courses) else 'N/A'}
Course mix: {course_mix}
ATAR range: {atar_str}
Top career pathway: {top5_careers}

=== NEIGHBOURHOOD FACTS ===
Median rent: {rent_str} {rent_ctx}
Annual sunshine: {int(sunshine) if pd.notna(sunshine) else 'N/A'} hours
SEIFA decile: {int(seifa_d) if pd.notna(seifa_d) else 'N/A'}/10 ({seifa_l})

=== NEIGHBOURHOOD SIGNALS ===
{signals}

=== WRITE ===
3 short paragraphs:
1. Where it is and what the area actually feels like to live in.
2. Honest trade-offs — what's great, what's not, rent reality, getting around.
3. Who genuinely thrives here.

Tone: second-year student giving real talk to a future student. Warm, direct, occasionally funny. No buzzwords. Max 220 words."""

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=420,
            temperature=0.75,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"_AI description failed: {e}_"


# ── metric rendering ───────────────────────────────────────────────────────────

def render_metric_section(row, pcts, section_name):
    section_metrics = [m for m in METRIC_REGISTRY if m[4] == section_name]
    if not section_metrics:
        return
    icon = SECTION_ICONS.get(section_name, "")
    with st.expander(f"{icon} {section_name}", expanded=(section_name == SECTION_MEME)):
        for col, title, desc, good_bad, _ in section_metrics:
            norm_col = col + "_norm"
            val = row.get(norm_col, float("nan"))
            arr = pcts.get(norm_col, [])
            emoji, band_label = percentile_band(val, arr)
            tooltip = desc
            if good_bad:
                clean = good_bad.replace("❌ ", "\n❌ ")
                tooltip = f"{desc}\n\n{clean}"
            c1, c2, c3 = st.columns([3, 1, 1])
            with c1:
                st.markdown(f"**{title}**  \n<small>{desc}</small>", unsafe_allow_html=True)
            with c2:
                st.markdown(f"<div style='font-size:1.5rem;text-align:center'>{emoji}</div>",
                            unsafe_allow_html=True)
                st.caption(band_label)
            with c3:
                st.metric(label="Score (0–100)", value=fmt_val(val), help=tooltip)
            st.divider()


def _scored_metrics(row, pcts):
    """Return list of all metrics ranked by national percentile."""
    metric_lookup = {m[0]: m for m in METRIC_REGISTRY}
    scored = []
    for norm_col in [c for c in pcts]:
        base_col = norm_col.replace("_norm", "")
        val = row.get(norm_col, float("nan"))
        if pd.isna(val):
            continue
        arr = pcts.get(norm_col, [])
        if not len(arr):
            continue
        rank_pct = (arr < val).sum() / len(arr) * 100
        m = metric_lookup.get(base_col)
        if m:
            scored.append({"col": base_col, "rank_pct": rank_pct, "title": m[1],
                           "band": percentile_band(val, arr)[1], "section": m[4]})
    scored.sort(key=lambda x: x["rank_pct"], reverse=True)
    return scored


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(page_title="Institution Explorer", page_icon="🎓", layout="wide")

    df   = load_data()
    acir = load_acir_data()
    pcts = compute_national_percentiles(df)

    # ── sidebar ────────────────────────────────────────────────────────────
    with st.sidebar:
        st.title("🎓 Institution Explorer")
        st.caption("ABS Census 2021 · ACIR · Vibe Metrics")
        st.markdown("---")
        st.subheader("Find an institution")

        mode = st.radio("Search by", ["Name", "State", "SA2 area"], horizontal=True)

        if mode == "Name":
            search = st.text_input("Institution name", placeholder="e.g. RMIT, Monash, UTS…")
            if not search.strip():
                st.info("Start typing to search.")
                st.stop()
            filtered_df = df[df["name"].str.contains(search.strip(), case=False, na=False)].sort_values("name")

        elif mode == "State":
            states      = sorted(df["state"].dropna().unique())
            state       = st.selectbox("State / Territory", states)
            filtered_df = df[df["state"] == state].sort_values("name")

        else:
            sa2_search = st.text_input("SA2 area name", placeholder="e.g. Pyrmont, Carlton…")
            if not sa2_search.strip():
                st.info("Start typing an SA2 name.")
                st.stop()
            sa2_matches = (
                df[df["sa2_name"].str.contains(sa2_search.strip(), case=False, na=False)]
                ["sa2_name"].dropna().sort_values().unique().tolist()
            )
            if not sa2_matches:
                st.warning("No SA2 areas match.")
                st.stop()
            selected_sa2 = st.selectbox("SA2 area", sa2_matches)
            filtered_df  = df[df["sa2_name"] == selected_sa2].sort_values("name")

        if filtered_df.empty:
            st.warning("No institutions found.")
            st.stop()

        inst_name = st.selectbox("Institution", filtered_df["name"].tolist())
        row = filtered_df[filtered_df["name"] == inst_name].iloc[0]

        # at-a-glance pills
        st.markdown("---")
        org_type = _str(row.get("org_type"))
        state_v  = _str(row.get("state"))
        st.caption(f"{org_type}  ·  {state_v}")
        if pd.notna(row.get("total_courses")):
            st.caption(f"{int(row['total_courses'])} courses in DB")

    org_id   = str(row["organisation_id"])
    acir_row = acir.get(org_id)

    # ── auto AI description ────────────────────────────────────────────────
    scored     = _scored_metrics(row, pcts)
    highlights = scored[:5] + scored[-2:]

    if st.session_state.get("ai_institution") != inst_name:
        st.session_state.pop("ai_description", None)
        st.session_state["ai_institution"] = inst_name
        with st.spinner("Generating AI description…"):
            st.session_state["ai_description"] = generate_ai_description(row, acir_row, highlights)

    # ── tabs ───────────────────────────────────────────────────────────────
    tab_overview, tab_neighbourhood, tab_courses, tab_careers = st.tabs([
        "📍 Overview",
        "🏙️ Neighbourhood",
        "🎓 Courses & Entry",
        "💼 Careers",
    ])

    # ══════════════════════════════════════════════════════════════════════
    # TAB 1 — OVERVIEW
    # ══════════════════════════════════════════════════════════════════════
    with tab_overview:
        col_info, col_img = st.columns([3, 2])

        with col_info:
            st.subheader(row["name"])
            sector  = _str(acir_row.get("sector_name")) if acir_row else ""
            ot      = _str(acir_row.get("organisation_type_name")) if acir_row else _str(row.get("org_type"))
            st.caption(" · ".join(p for p in [ot, sector] if p))

            street = build_street_address(acir_row) if acir_row else None
            if street:
                st.markdown(f"📍 {street}")

            website = _str(acir_row.get("organisation_web_address")) if acir_row else ""
            if website:
                st.markdown(f"🌐 [{website}]({website})")

        with col_img:
            image_url, img_source = fetch_image(row["name"], acir_row)
            if image_url:
                st.image(image_url, use_container_width=True)
                if img_source:
                    st.caption(f"Image: {img_source}")
            else:
                st.info("No image found.")

        # ── quick-stats strip ──────────────────────────────────────────
        st.markdown("---")
        qs1, qs2, qs3, qs4 = st.columns(4)

        total_c = row.get("total_courses")
        qs1.metric("Courses in DB", int(total_c) if pd.notna(total_c) else "—")

        atar_med = row.get("atar_median")
        qs2.metric("ATAR median", fmt_val(atar_med) if pd.notna(atar_med) else "No ATAR")

        avg_fee = row.get("avg_domestic_fee")
        qs3.metric("Avg domestic fee", fmt_currency(avg_fee) if pd.notna(avg_fee) else "—")

        career_div = row.get("career_diversity")
        qs4.metric("Career pathways", int(career_div) if pd.notna(career_div) else "—")

        # ── neighbourhood snapshot ─────────────────────────────────────
        st.markdown("---")
        ns1, ns2, ns3 = st.columns(3)
        rent   = row.get("median_rent_weekly")
        seifa_d = row.get("seifa_irsad_decile")
        seifa_l = _str(row.get("seifa_label"))
        ns1.metric("SA2 area",      _str(row.get("sa2_name")) or "—")
        ns2.metric("SEIFA",         f"{int(seifa_d)}/10 — {seifa_l}" if pd.notna(seifa_d) else "—")
        ns3.metric("Median rent",   f"${int(rent)}/wk" if pd.notna(rent) else "—")

        # ── top 3 strengths ────────────────────────────────────────────
        if scored:
            st.markdown("---")
            st.markdown("**Neighbourhood strengths**")
            sc1, sc2, sc3 = st.columns(3)
            for i, col_ in enumerate([sc1, sc2, sc3]):
                m = scored[i]
                emoji, band_label = percentile_band(
                    row.get(f"{m['col']}_norm", float("nan")),
                    pcts.get(f"{m['col']}_norm", []),
                )
                col_.metric(m["title"], f"{emoji} {band_label}")

        # ── ACIR campus info ───────────────────────────────────────────
        if acir_row:
            st.markdown("---")
            cricos = _str(acir_row.get("organisation_cricos_code"))
            rto    = _str(acir_row.get("organisation_rto_code"))
            if cricos or rto:
                r1, r2 = st.columns(2)
                if cricos: r1.caption(f"CRICOS: {cricos}")
                if rto:    r2.caption(f"RTO: {rto}")

            for label, key in [
                ("📚 Study areas",    "site_study_area"),
                ("🚌 Transport",      "site_transport"),
                ("🏠 Accommodation",  "site_accommodation"),
                ("🏛️ Campus notes",   "site_comments"),
            ]:
                val = _str(acir_row.get(key))
                if val:
                    with st.expander(label):
                        st.write(val)


    # ══════════════════════════════════════════════════════════════════════
    # TAB 2 — NEIGHBOURHOOD
    # ══════════════════════════════════════════════════════════════════════
    with tab_neighbourhood:
        if scored:
            st.markdown("**Top 3 strengths**")
            c1, c2, c3 = st.columns(3)
            for i, col_ in enumerate([c1, c2, c3]):
                m = scored[i]
                emoji, band_label = percentile_band(
                    row.get(f"{m['col']}_norm", float("nan")),
                    pcts.get(f"{m['col']}_norm", []),
                )
                col_.metric(m["title"], f"{emoji} {band_label}")

            st.markdown("**Watch-outs**")
            w1, w2 = st.columns(2)
            for i, col_ in enumerate([w1, w2]):
                m = scored[-(i + 1)]
                emoji, band_label = percentile_band(
                    row.get(f"{m['col']}_norm", float("nan")),
                    pcts.get(f"{m['col']}_norm", []),
                )
                col_.metric(m["title"], f"{emoji} {band_label}")

            # ── AI neighbourhood description ───────────────────────────────
        st.markdown("---")
        st.subheader("About this area")
        st.markdown(st.session_state.get("ai_description", ""))

        st.markdown("---")

        for section in SECTIONS:
            render_metric_section(row, pcts, section)

    # ══════════════════════════════════════════════════════════════════════
    # TAB 3 — COURSES & ENTRY
    # ══════════════════════════════════════════════════════════════════════
    with tab_courses:
        total_c = row.get("total_courses")
        if pd.isna(total_c):
            st.info("No course data available for this institution.")
        else:
            # ── course profile ─────────────────────────────────────────
            st.subheader("Course profile")
            cp_left, cp_right = st.columns([2, 1])

            with cp_left:
                st.markdown("**Course level mix**")
                levels = {
                    "Undergraduate":  row.get("pct_undergraduate", 0) or 0,
                    "Postgraduate":   row.get("pct_postgraduate",  0) or 0,
                    "VET / Vocational": row.get("pct_vet_vocational", 0) or 0,
                    "Secondary":      row.get("pct_secondary",    0) or 0,
                }
                levels = {k: v for k, v in levels.items() if v > 0}
                if levels:
                    for label, pct in levels.items():
                        st.markdown(f"**{label}** — {pct:.0f}%")
                        st.progress(int(pct))
                else:
                    st.caption("Course level breakdown not available.")

            with cp_right:
                st.metric("Total courses",  int(total_c))
                faculty_c = row.get("faculty_count")
                st.metric("Faculties / areas", int(faculty_c) if pd.notna(faculty_c) else "—")
                free_t = row.get("free_tafe_count")
                if pd.notna(free_t) and int(free_t) > 0:
                    st.metric("Free TAFE courses", int(free_t))

            # ── fees ──────────────────────────────────────────────────
            st.markdown("---")
            st.subheader("Fees (domestic)")
            f1, f2, f3 = st.columns(3)
            f1.metric("Min fee",  fmt_currency(row.get("min_domestic_fee")))
            f2.metric("Avg fee",  fmt_currency(row.get("avg_domestic_fee")))
            f3.metric("Max fee",  fmt_currency(row.get("max_domestic_fee")))

            # ── entry requirements ─────────────────────────────────────
            st.markdown("---")
            st.subheader("Entry requirements")
            e1, e2, e3 = st.columns(3)

            atar_min = row.get("atar_min")
            atar_max = row.get("atar_max")
            atar_med = row.get("atar_median")
            if pd.notna(atar_min) or pd.notna(atar_max):
                atar_range = f"{int(atar_min) if pd.notna(atar_min) else '?'}–{int(atar_max) if pd.notna(atar_max) else '?'}"
                e1.metric("ATAR range", atar_range)
                if pd.notna(atar_med):
                    e1.caption(f"Median: {fmt_val(atar_med)}")
            else:
                e1.metric("ATAR", "Not required / N/A")

            avg_ielts = row.get("avg_ielts_requirement")
            e2.metric("Avg IELTS requirement", fmt_val(avg_ielts) if pd.notna(avg_ielts) else "Not required")

            pct_alt = row.get("pct_with_alternate_entry")
            if pd.notna(pct_alt) and pct_alt > 0:
                e3.metric("Alternate entry", f"{pct_alt:.0f}% of courses")
            else:
                e3.metric("Alternate entry", "—")

            pct_ielts = row.get("pct_courses_with_ielts")
            if pd.notna(pct_ielts) and pct_ielts > 0:
                st.caption(f"{pct_ielts:.0f}% of courses have an IELTS requirement set.")

    # ══════════════════════════════════════════════════════════════════════
    # TAB 4 — CAREERS
    # ══════════════════════════════════════════════════════════════════════
    with tab_careers:
        career_div = row.get("career_diversity")
        if pd.isna(career_div) or int(career_div) == 0:
            st.info("No career mapping available for this institution.")
        else:
            st.subheader("Career pathways")

            # ── top careers ────────────────────────────────────────────
            top5_raw = _str(row.get("top_5_careers"))
            if top5_raw:
                st.markdown("**Top careers linked to courses here**")
                for i, career in enumerate(top5_raw.split(" | "), 1):
                    st.markdown(f"{i}. {career.strip()}")

            st.markdown("---")

            # ── domain breakdown ───────────────────────────────────────
            st.subheader("Career domain breakdown")
            d_left, d_right = st.columns([1, 1])

            with d_left:
                domains = [
                    ("STEM",              "stem_career_pct",              "🔬"),
                    ("Health",            "health_career_pct",            "🏥"),
                    ("Education",         "education_career_pct",         "📚"),
                    ("Business & Finance","business_finance_career_pct",  "💼"),
                ]
                for label, col_, icon in domains:
                    val = row.get(col_)
                    display = f"{val:.0f}%" if pd.notna(val) else "—"
                    st.metric(f"{icon} {label}", display)

            with d_right:
                top_group = _str(row.get("top_anzsco_group"))
                top_pct   = row.get("top_anzsco_group_pct")
                if top_group:
                    st.markdown("**Dominant ANZSCO group**")
                    st.markdown(f"### {top_group}")
                    if pd.notna(top_pct):
                        st.caption(f"{top_pct:.0f}% of all career links")
                st.markdown("---")
                st.metric("Total career pathways mapped", int(career_div))


if __name__ == "__main__":
    main()
