"""
Institution Explorer — Streamlit UI
====================================
State → Institution → vibe metrics, metadata, image, AI description.

Run:
    .venv/bin/streamlit run streamlit_app.py
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

# ── env ──────────────────────────────────────────────────────────────────────
load_dotenv()
SERPA_API      = os.getenv("SERPA_API", "")
OPENAI_KEY     = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL   = os.getenv("OPEN_AI_MODEL", "gpt-4o")

# ── paths ─────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).parent
DATA_PATH   = ROOT / "geo_mapping" / "output" / "institutions_meme_metrics.csv"
CAREER_PATH = ROOT / "acir_db" / "organisation_career_coverage.csv"
ORGS_SQL    = ROOT / "acir_db" / "sql" / "organisations.sql"

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
    ("food_scene_pct",       "Food & Drink Scene",       "Food & hospitality workers as a share of all workers commuting into this area. Note: major employment hubs (like Parramatta or the CBD) can score lower here not because they lack restaurants, but because their huge corporate workforce dilutes the ratio — even if thousands of hospitality workers show up daily.",
     "✅ High: food and hospitality make up a significant share of what this area actually does — real cafés, restaurants and bars with staff to prove it.\n❌ Low: either genuinely thin on food options, or a large corporate/government workforce is diluting the ratio. Worth checking on foot before writing it off.",
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


# ── data loading ──────────────────────────────────────────────────────────────

@st.cache_data
def load_data():
    df = pd.read_csv(DATA_PATH, dtype={"sa2_code": str})
    career = pd.read_csv(CAREER_PATH)
    career = career.rename(columns={"id": "organisation_id"})
    career = career[["organisation_id", "top_career", "course_count", "career_count"]].copy()
    career["organisation_id"] = career["organisation_id"].astype(int)
    df = df.merge(career, on="organisation_id", how="left")
    return df


@st.cache_data
def load_acir_data():
    """
    Load organisations.sql — one row per site per org.
    Keep the primary site where available, else first site per org.
    Returns a dict keyed by organisation_id.
    """
    df = get_data_from_file(str(ORGS_SQL))
    if df is None or df.empty:
        return {}

    # Prefer primary site
    primary = df[df["site_primary_site"] == 1]
    non_primary = df[~df.index.isin(primary.index)]

    # Deduplicate: primary first, then first-available
    deduped = (
        pd.concat([primary, non_primary])
        .drop_duplicates(subset=["organisation_id"], keep="first")
        .set_index("organisation_id")
    )
    return deduped.to_dict(orient="index")


@st.cache_data
def compute_national_percentiles(df):
    sa2_df = df.drop_duplicates(subset=["sa2_code"]).copy()
    norm_cols = [c for c in df.columns if c.endswith("_norm")]
    return {col: sa2_df[col].dropna().values for col in norm_cols}


# ── helpers ───────────────────────────────────────────────────────────────────

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
    if pd.isna(v):
        return "—"
    if isinstance(v, float) and v == int(v):
        return str(int(v))
    if isinstance(v, float):
        return f"{v:.1f}"
    return str(v)


def _str(v):
    """Return stripped string or empty string for None/NaN."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def parse_logo_url(logo_image_urls):
    """Extract the best available logo URL from the ACIR JSON field."""
    raw = _str(logo_image_urls)
    if not raw:
        return None
    try:
        data = json.loads(raw)
        # prefer largest: original > 150x140 > 100x100
        for key in ("original", "150x140", "100x100", "75x75", "50x50"):
            val = data.get(key, "")
            if val and val.startswith("http"):
                return val
        # some entries have relative paths — prepend S3 base
        for key in ("original", "150x140", "100x100"):
            val = data.get(key, "")
            if val:
                return f"https://s3-ap-southeast-2.amazonaws.com/geg-sia-webapp2/{val}"
    except (json.JSONDecodeError, AttributeError):
        # Sometimes it's just a plain URL string
        if raw.startswith("http"):
            return raw
    return None


def build_street_address(acir):
    """Build a clean street address string from ACIR site fields."""
    parts = []
    street1 = _str(acir.get("site_street1"))
    street2 = _str(acir.get("site_street2"))
    suburb  = _str(acir.get("site_subrub"))   # note: typo in DB column name
    postcode = _str(acir.get("site_postcode"))
    if postcode.endswith(".0"):
        postcode = postcode[:-2]

    if street1:
        parts.append(street1)
    if street2:
        parts.append(street2)
    if suburb:
        parts.append(suburb.title())
    if postcode:
        parts.append(postcode)
    return ", ".join(parts) if parts else None


def strip_html(text):
    """Remove HTML tags from a string."""
    return re.sub(r"<[^>]+>", " ", text).strip() if text else ""


# ── image fetching ────────────────────────────────────────────────────────────

def fetch_image(institution_name, acir):
    """
    Image priority:
      1. ACIR logo (hosted on S3, always reliable)
      2. Serper image search
    Returns (url, source_label).
    """
    # 1. ACIR logo
    logo_url = parse_logo_url(acir.get("logo_image_urls") if acir else None)
    if logo_url:
        return logo_url, "ACIR"

    # 2. Serper
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


# ── AI description ────────────────────────────────────────────────────────────

def _metric_signals(metric_highlights):
    """
    Translate metric highlights into plain-English signals for the AI prompt.
    Picks the ✅ or ❌ implication based on whether the score is high or low,
    strips the emoji/label prefix, and returns a bullet list — no metric names,
    no scores, no jargon.
    """
    good_bad_lookup = {m[0]: m[3] for m in METRIC_REGISTRY}
    low_bands = {"Bottom 5%", "Bottom 10%", "Bottom 20%", "Bottom 30%"}

    signals = []
    for m in metric_highlights:
        gb = good_bad_lookup.get(m["col"], "")
        if not gb:
            continue
        is_low = m["band"] in low_bands
        # split on newline, pick the matching half
        lines = gb.split("\n")
        chosen = None
        for line in lines:
            if is_low and line.startswith("❌"):
                chosen = line
            elif not is_low and line.startswith("✅"):
                chosen = line
        if chosen:
            # strip leading emoji + "High:" / "Low:" label
            text = re.sub(r"^[✅❌]\s+\*\*(High|Low):\*\*\s*", "", chosen).strip()
            signals.append(f"- {text}")

    return "\n".join(signals)


def generate_ai_description(row, acir, metric_highlights):
    if not OPENAI_KEY:
        return "_No OPENAI_API_KEY configured._"

    client = OpenAI(api_key=OPENAI_KEY)

    # ── location facts from ACIR (authoritative) ──
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

    # ── ABS/census facts ──
    sa2      = _str(row.get("sa2_name"))
    sa3      = _str(row.get("sa3_name"))
    lga      = _str(row.get("lga_name"))
    state    = _str(row.get("state"))
    rent     = row.get("median_rent_weekly")
    seifa_d  = row.get("seifa_irsad_decile")
    seifa_l  = _str(row.get("seifa_label"))
    sunshine = row.get("sunshine_hours_yr")
    top_car  = row.get("top_career")
    courses  = row.get("course_count")

    rent_str = f"${int(rent)}/week" if pd.notna(rent) else "N/A"
    rent_ctx = ""
    if pd.notna(rent):
        rent_ctx = "— well above the national median" if rent > 500 else ("— around the national median" if rent > 350 else "— below the national median")

    signals = _metric_signals(metric_highlights)

    prompt = f"""You are writing a short, engaging profile of an Australian education institution for 18–24 year olds deciding where to study and live.

RULES:
- Only use the facts provided below. Do not invent anything.
- Do NOT use any technical metric names, score names, index names, or percentile terminology.
- Do NOT use jargon. Write like a person, not a data analyst.
- The neighbourhood signals below are plain-English observations drawn from Census data — weave them naturally into the writing without citing them as statistics.

=== INSTITUTION ===
Name: {row['name']}
Type: {_str(row.get('org_type'))}
Sector: {sector or 'N/A'}

=== LOCATION ===
Street address: {street_address or 'N/A'}
Suburb: {acir_suburb or _str(row.get('suburb', '')).title()}
Postcode: {acir_postcode or _str(row.get('postcode', '')).replace('.0', '')}
State: {state}
SA2 statistical area: {sa2}
SA3 region: {sa3}
LGA: {lga}
Broader region: {region_name}

=== CAMPUS ===
Study areas: {study_areas or 'N/A'}
Transport: {transport or 'N/A'}
Accommodation nearby: {accommodation or 'N/A'}
Campus notes: {campus_notes or 'N/A'}

=== ABOUT THE INSTITUTION ===
{acir_desc[:500] if acir_desc else 'N/A'}

=== KEY FACTS ===
Median weekly rent in this area: {rent_str} {rent_ctx}
Annual sunshine hours: {int(sunshine) if pd.notna(sunshine) else 'N/A'}
Top career pathway: {top_car if pd.notna(top_car) else 'N/A'}
Courses offered: {int(courses) if pd.notna(courses) else 'N/A'}
SEIFA socio-economic decile: {int(seifa_d) if pd.notna(seifa_d) else 'N/A'}/10 ({seifa_l})

=== NEIGHBOURHOOD SIGNALS ===
These observations describe what the surrounding area is actually like. Use them to colour the writing — don't list them, don't cite them as facts, just let them inform the tone and content:
{signals}

=== WRITE ===
3 short paragraphs:
1. Where it is and what the area feels like to actually live in.
2. The honest trade-offs — what's great, what's not, what to expect on rent and getting around.
3. Who this place suits — what kind of person genuinely thrives here.

Tone: like a well-travelled second-year student giving real talk to someone about to move there. Warm, direct, occasionally funny. No buzzwords. Max 220 words."""

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


# ── metric rendering ──────────────────────────────────────────────────────────

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

            # Build tooltip: full description + high/low implications
            tooltip = desc
            if good_bad:
                clean = good_bad.replace("✅ ", "✅ ").replace("❌ ", "\n❌ ")
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


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(page_title="Institution Explorer", page_icon="🎓", layout="wide")
    st.title("🎓 Australian Institution Explorer")
    st.caption("ABS Census 2021 · ACIR database · Vibe Metrics · Student Life · Place-of-Work Infrastructure")

    df      = load_data()
    acir    = load_acir_data()
    pcts    = compute_national_percentiles(df)

    # ── sidebar ────────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("Find institution")

        mode = st.radio("Search by", ["Name", "State", "SA2 area"], horizontal=True)

        if mode == "Name":
            search = st.text_input("Institution name", placeholder="e.g. RMIT, Monash, UTS…")
            if not search.strip():
                st.info("Start typing to search all institutions.")
                st.stop()
            filtered_df = df[df["name"].str.contains(search.strip(), case=False, na=False)].sort_values("name")

        elif mode == "State":
            states      = sorted(df["state"].dropna().unique())
            state       = st.selectbox("State / Territory", states)
            filtered_df = df[df["state"] == state].sort_values("name")

        else:  # SA2 area
            sa2_search = st.text_input("SA2 area name", placeholder="e.g. Pyrmont, Carlton, Fortitude Valley…")
            if not sa2_search.strip():
                st.info("Start typing an SA2 name.")
                st.stop()
            # filter to matching SA2s, then show a selectbox of matching SA2 names
            sa2_matches = (
                df[df["sa2_name"].str.contains(sa2_search.strip(), case=False, na=False)]
                ["sa2_name"].dropna().sort_values().unique().tolist()
            )
            if not sa2_matches:
                st.warning("No SA2 areas match that search.")
                st.stop()
            selected_sa2 = st.selectbox("SA2 area", sa2_matches)
            filtered_df  = df[df["sa2_name"] == selected_sa2].sort_values("name")

        if filtered_df.empty:
            st.warning("No institutions found.")
            st.stop()

        inst_name = st.selectbox("Institution", filtered_df["name"].tolist())
        row = filtered_df[filtered_df["name"] == inst_name].iloc[0]
        st.markdown("---")
        run_ai = st.button("✨ Generate AI description", use_container_width=True)

    org_id   = int(row["organisation_id"])
    acir_row = acir.get(org_id)  # None if not in DB

    # ── header ─────────────────────────────────────────────────────────────
    col_info, col_img = st.columns([3, 2])

    with col_info:
        st.subheader(row["name"])
        org_type = _str(acir_row.get("organisation_type_name")) if acir_row else _str(row.get("org_type"))
        sector   = _str(acir_row.get("sector_name")) if acir_row else ""
        subtitle = " · ".join(p for p in [org_type, sector] if p)
        st.caption(subtitle)

        # ── street address ─────────────────────────────────────────────
        street = build_street_address(acir_row) if acir_row else None
        if street:
            st.markdown(f"📍 {street}")

        website = _str(acir_row.get("organisation_web_address")) if acir_row else ""
        if website:
            st.markdown(f"🌐 [{website}]({website})")

        st.markdown("")

        # ── metadata grid ──────────────────────────────────────────────
        def meta_row(label, value):
            if not value or value == "—":
                return
            c1, c2 = st.columns([1, 2])
            c1.markdown(f"**{label}**")
            c2.write(value)

        meta_row("SA2 area",        _str(row.get("sa2_name")) or "—")
        meta_row("SA3 region",      _str(row.get("sa3_name")) or "—")
        meta_row("LGA",             _str(row.get("lga_name")) or "—")

        rent = row.get("median_rent_weekly")
        meta_row("Median rent",     f"${int(rent)}/wk" if pd.notna(rent) else "—")

        seifa_d = row.get("seifa_irsad_decile")
        seifa_l = _str(row.get("seifa_label"))
        meta_row("SEIFA decile",    f"{int(seifa_d)}/10 — {seifa_l}" if pd.notna(seifa_d) else "—")

        meta_row("Sunshine hrs/yr", fmt_val(row.get("sunshine_hours_yr")))

        cricos = _str(acir_row.get("organisation_cricos_code")) if acir_row else ""
        rto    = _str(acir_row.get("organisation_rto_code")) if acir_row else ""
        meta_row("CRICOS code",     cricos or "—")
        meta_row("RTO code",        rto or "—")

        top_car = row.get("top_career")
        meta_row("Top career path", _str(top_car) if pd.notna(top_car) else "—")

        courses = row.get("course_count")
        meta_row("Courses offered", fmt_val(courses))

        meta_row("Youth pop.",      f"{fmt_val(row.get('youth_pct'))}%")
        meta_row("Overseas-born",   f"{fmt_val(row.get('overseas_born_pct'))}%")

        # ── study areas & campus info ──────────────────────────────────
        if acir_row:
            study_areas = _str(acir_row.get("site_study_area"))
            if study_areas:
                with st.expander("📚 Study areas"):
                    st.write(study_areas)

            transport = _str(acir_row.get("site_transport"))
            if transport:
                with st.expander("🚌 Transport"):
                    st.write(transport)

            accommodation = _str(acir_row.get("site_accommodation"))
            if accommodation:
                with st.expander("🏠 Accommodation"):
                    st.write(accommodation)

            campus_notes = _str(acir_row.get("site_comments"))
            if campus_notes:
                with st.expander("🏛️ Campus notes"):
                    st.write(campus_notes)

    with col_img:
        image_url, img_source = fetch_image(row["name"], acir_row)
        if image_url:
            st.image(image_url, use_container_width=True)
            if img_source:
                st.caption(f"Image: {img_source}")
        else:
            st.info("No image found.")

    # ── AI description ─────────────────────────────────────────────────────
    all_norm_cols = [c for c in df.columns if c.endswith("_norm")]
    metric_lookup = {m[0]: m for m in METRIC_REGISTRY}

    scored = []
    for norm_col in all_norm_cols:
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
    highlights = scored[:5] + scored[-2:]

    st.markdown("---")
    if run_ai:
        # clear stale description when institution changes
        st.session_state.pop("ai_description", None)
        st.session_state.pop("ai_institution", None)
        with st.spinner("Generating description..."):
            description = generate_ai_description(row, acir_row, highlights)
        st.session_state["ai_description"] = description
        st.session_state["ai_institution"] = inst_name

    # only show description if it belongs to the currently selected institution
    if st.session_state.get("ai_institution") == inst_name and "ai_description" in st.session_state:
        st.subheader("About this institution")
        st.markdown(st.session_state["ai_description"])
        st.markdown("---")

    # ── metrics ────────────────────────────────────────────────────────────
    st.subheader("Metric breakdown")

    if scored:
        st.markdown("**Top 3 strengths**")
        cols = st.columns(3)
        for i, m in enumerate(scored[:3]):
            with cols[i]:
                emoji, band_label = percentile_band(
                    row.get(f"{m['col']}_norm", float("nan")),
                    pcts.get(f"{m['col']}_norm", []),
                )
                st.metric(m["title"], f"{emoji} {band_label}")

        st.markdown("**Watch-outs**")
        cols = st.columns(2)
        for i, m in enumerate(scored[-2:]):
            with cols[i]:
                emoji, band_label = percentile_band(
                    row.get(f"{m['col']}_norm", float("nan")),
                    pcts.get(f"{m['col']}_norm", []),
                )
                st.metric(m["title"], f"{emoji} {band_label}")

    st.markdown("---")

    for section in SECTIONS:
        render_metric_section(row, pcts, section)


if __name__ == "__main__":
    main()
