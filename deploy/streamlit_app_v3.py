"""
Institution Explorer v3 — Streamlit UI
=======================================
Single-CSV architecture: loads only deploy/data/full_export.csv.
No database connection. No runtime percentile computation.
All metric bands, emojis, signals and descriptions are pre-baked.

To build the CSV:
    python deploy/build_full_export.py

To run:
    .venv/bin/streamlit run streamlit_app_v3.py
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

# ── env ───────────────────────────────────────────────────────────────────────
load_dotenv()
SERPA_API    = os.getenv("SERPA_API")      or st.secrets.get("SERPA_API", "")
OPENAI_KEY   = os.getenv("OPENAI_API_KEY") or st.secrets.get("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPEN_AI_MODEL")  or st.secrets.get("OPEN_AI_MODEL", "gpt-4o")

# ── paths ─────────────────────────────────────────────────────────────────────
ROOT      = Path(__file__).parent          # → deploy/
DATA_PATH = ROOT / "data" / "full_export.csv"

# ── metric registry ───────────────────────────────────────────────────────────
SECTION_LIFESTYLE = "Lifestyle & Liveability"
SECTION_POW       = "What's Actually Here"
SECTION_STUDENT   = "Academic & Job Opportunity"
SECTION_MEME      = "Meme Metrics — Ages 18–34"

METRIC_REGISTRY = [
    # ── Lifestyle & Liveability ───────────────────────────────────────────
    ("car_jail_score",       "Car Jail Score",           SECTION_LIFESTYLE),
    ("car_free_commute_pct", "Car-Free Commute",         SECTION_LIFESTYLE),
    ("wfh_pct",              "WFH Culture Index",        SECTION_LIFESTYLE),
    ("pedal_path_pct",       "Pedal & Path Score",       SECTION_LIFESTYLE),
    ("night_economy_pct",    "Night Shift Neighbours",   SECTION_LIFESTYLE),
    ("knowledge_worker_pct", "Professional Neighbours",  SECTION_LIFESTYLE),
    ("student_bubble_pct",   "Student Bubble Density",   SECTION_LIFESTYLE),
    ("renter_republic_pct",  "Renter Republic Score",    SECTION_LIFESTYLE),
    ("vertical_city_pct",    "Vertical City Score",      SECTION_LIFESTYLE),
    ("housing_stress_ratio", "Housing Stress Ratio",     SECTION_LIFESTYLE),
    ("fresh_energy_pct",     "Fresh Energy Score",       SECTION_LIFESTYLE),
    ("community_glue_pct",   "Community Glue Score",     SECTION_LIFESTYLE),
    ("global_mix_score",     "Global Mix Score",         SECTION_LIFESTYLE),

    # ── What's Actually Here (POW) ────────────────────────────────────────
    ("social_scene_score",   "Social Scene Score",       SECTION_POW),
    ("food_scene_pct",       "Food & Drink Scene",       SECTION_POW),
    ("entertainment_pct",    "Entertainment Quarter",    SECTION_POW),
    ("healthcare_access_pct","Healthcare Access",        SECTION_POW),
    ("education_hub_pct",    "Education Hub",            SECTION_POW),
    ("retail_density_pct",   "Shops & Markets",          SECTION_POW),
    ("civic_services_pct",   "Civic Infrastructure",     SECTION_POW),
    ("knowledge_hub_pct",    "Knowledge Economy Hub",    SECTION_POW),
    ("job_gravity_ratio",    "Job Gravity",              SECTION_POW),

    # ── Academic & Job Opportunity ────────────────────────────────────────
    ("qualification_density", "Qualification Density",   SECTION_STUDENT),
    ("grad_capture_rate",    "Grad Capture Rate",        SECTION_STUDENT),
    ("professional_job_pct", "Professional Job Density", SECTION_STUDENT),
    ("stem_field_pct",       "STEM Field Concentration", SECTION_STUDENT),
    ("income_growth_signal", "Income Growth Signal",     SECTION_STUDENT),
    ("employment_rate",      "Employment Rate",          SECTION_STUDENT),
    ("mortgage_stress_pct",  "Mortgage Stress",          SECTION_STUDENT),

    # ── Meme Metrics — Ages 18–34 ─────────────────────────────────────────
    ("uni_town_index",          "Uni Town Index",            SECTION_MEME),
    ("ramen_economy_score",     "Ramen Economy Score",       SECTION_MEME),
    ("sharehouse_capital",      "Sharehouse Capital",        SECTION_MEME),
    ("all_nighter_index",       "All-Nighter Index",         SECTION_MEME),
    ("first_job_energy",        "First Job Energy",          SECTION_MEME),
    ("promotion_pipeline",      "Promotion Pipeline",        SECTION_MEME),
    ("startup_dreamer_density", "Startup Dreamer Density",   SECTION_MEME),
    ("side_hustle_generation",  "Side Hustle Generation",    SECTION_MEME),
    ("bank_of_mum_and_dad",     "Bank of Mum & Dad",         SECTION_MEME),
    ("peter_pan_index",         "Peter Pan Index",           SECTION_MEME),
    ("adulting_score",          "Adulting Score",            SECTION_MEME),
    ("rent_forever_index",      "Rent Forever Index",        SECTION_MEME),
    ("singles_scene",           "Singles Scene",             SECTION_MEME),
    ("dink_potential",          "DINK Potential",            SECTION_MEME),
    ("delayed_adulting_score",  "Delayed Adulting Score",    SECTION_MEME),
    ("global_youth_hub",        "Global Youth Hub",          SECTION_MEME),
    ("nightlife_index",         "Nightlife Index",           SECTION_MEME),
    ("digital_nomad_potential", "Digital Nomad Potential",   SECTION_MEME),
    ("just_one_more_degree",    "Just One More Degree",      SECTION_MEME),
    ("flat_white_density",      "Flat White Density",        SECTION_MEME),
]

SECTIONS = [SECTION_LIFESTYLE, SECTION_POW, SECTION_STUDENT, SECTION_MEME]
SECTION_ICONS = {
    SECTION_LIFESTYLE: "🏡",
    SECTION_POW:       "📍",
    SECTION_STUDENT:   "🎓",
    SECTION_MEME:      "🔥",
}

TOP_BANDS    = {"Top 5%", "Top 10%", "Top 20%", "Top 30%"}
BOTTOM_BANDS = {"Bottom 5%", "Bottom 10%", "Bottom 20%", "Bottom 30%"}

# Approximate rank_pct midpoints per band label — used for sorting
BAND_RANK = {
    "Top 5%":     97.5,
    "Top 10%":    92.5,
    "Top 20%":    85.0,
    "Top 30%":    75.0,
    "Middle":     50.0,
    "Bottom 30%": 25.0,
    "Bottom 20%": 15.0,
    "Bottom 10%":  7.5,
    "Bottom 5%":   2.5,
    "":            50.0,
}


# ── data loading ───────────────────────────────────────────────────────────────

@st.cache_data
def load_data():
    if not DATA_PATH.exists():
        st.error(
            f"**{DATA_PATH.name} not found.**\n\n"
            "Run `python deploy/build_full_export.py` locally (needs VPN/RDS access), "
            "then commit `deploy/data/full_export.csv` to the repo."
        )
        st.stop()
    return pd.read_csv(DATA_PATH, dtype={"organisation_id": str, "sa2_code": str})


# ── helpers ────────────────────────────────────────────────────────────────────

def _str(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def fmt_val(v):
    if pd.isna(v):      return "—"
    if isinstance(v, float) and v == int(v): return str(int(v))
    if isinstance(v, float): return f"{v:.1f}"
    return str(v)


def fmt_currency(v):
    if pd.isna(v): return "—"
    return f"${int(v):,}"


def strip_html(text):
    return re.sub(r"<[^>]+>", " ", text).strip() if text else ""


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


def build_street_address(row):
    parts = []
    street1  = _str(row.get("site_street1"))
    street2  = _str(row.get("site_street2"))
    suburb   = _str(row.get("site_subrub"))   # typo is in the DB column name
    postcode = _str(row.get("site_postcode")).replace(".0", "")
    if street1:  parts.append(street1)
    if street2:  parts.append(street2)
    if suburb:   parts.append(suburb.title())
    if postcode: parts.append(postcode)
    return ", ".join(parts) if parts else None


def fetch_image(institution_name, row):
    logo_url = parse_logo_url(row.get("logo_image_urls"))
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


# ── scoring / ranking ──────────────────────────────────────────────────────────

def _scored_metrics(row):
    """Return all metrics sorted by band rank (high → low)."""
    scored = []
    for col, title, section in METRIC_REGISTRY:
        band = _str(row.get(f"{col}_band"))
        if not band:
            continue
        emoji    = _str(row.get(f"{col}_emoji"))
        rank_pct = BAND_RANK.get(band, 50.0)
        scored.append({
            "col":      col,
            "title":    title,
            "section":  section,
            "band":     band,
            "emoji":    emoji,
            "rank_pct": rank_pct,
        })
    scored.sort(key=lambda x: x["rank_pct"], reverse=True)
    return scored


# ── AI description ─────────────────────────────────────────────────────────────

def _metric_signals(highlights, row):
    """Build plain-English signal lines for AI prompt from top/bottom highlights."""
    signals = []
    for m in highlights:
        col  = m["col"]
        band = m["band"]
        if band in TOP_BANDS:
            line = _str(row.get(f"{col}_signal_high"))
        else:
            line = _str(row.get(f"{col}_signal_low"))
        if line:
            # Strip the leading emoji + High:/Low: prefix
            text = re.sub(r"^[✅❌]\s*(High|Low):\s*", "", line).strip()
            signals.append(f"- {text}")
    return "\n".join(signals)


def generate_ai_description(row, highlights):
    if not OPENAI_KEY:
        return "_No OPENAI_API_KEY configured._"

    client = OpenAI(api_key=OPENAI_KEY)

    street_address = build_street_address(row)
    sector         = _str(row.get("sector_name"))
    acir_desc      = strip_html(_str(row.get("organisation_description")))
    study_areas    = _str(row.get("site_study_area"))
    accommodation  = _str(row.get("site_accommodation"))
    transport      = _str(row.get("site_transport"))
    campus_notes   = _str(row.get("site_comments"))
    region_name    = _str(row.get("region_name"))
    suburb         = _str(row.get("site_subrub") or row.get("suburb", "")).title()

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

    total_courses = row.get("total_courses")
    pct_ug        = row.get("pct_undergraduate")
    pct_pg        = row.get("pct_postgraduate")
    pct_vet       = row.get("pct_vet_vocational")
    atar_min      = row.get("atar_min")
    atar_max      = row.get("atar_max")
    top_career    = _str(row.get("top_5_careers")).split(" | ")[0] or "N/A"

    def _pct(v):
        return f"{int(v)}%" if pd.notna(v) and v > 0 else None

    course_mix = ", ".join(p for p in [
        f"{_pct(pct_ug)} undergraduate"  if _pct(pct_ug)  else None,
        f"{_pct(pct_pg)} postgraduate"   if _pct(pct_pg)  else None,
        f"{_pct(pct_vet)} VET/vocational" if _pct(pct_vet) else None,
    ] if p) or "mix not available"

    atar_str = "N/A"
    if pd.notna(atar_min) and pd.notna(atar_max):
        atar_str = f"{int(atar_min)}–{int(atar_max)}"
    elif pd.notna(atar_max):
        atar_str = f"up to {int(atar_max)}"

    signals = _metric_signals(highlights, row)

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
Suburb: {suburb}
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
Top career pathway: {top_career}

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

def render_metric_section(row, section_name):
    section_metrics = [(col, title) for col, title, sec in METRIC_REGISTRY if sec == section_name]
    if not section_metrics:
        return
    icon = SECTION_ICONS.get(section_name, "")
    with st.expander(f"{icon} {section_name}", expanded=(section_name == SECTION_MEME)):
        for col, title in section_metrics:
            emoji = _str(row.get(f"{col}_emoji")) or "—"
            band  = _str(row.get(f"{col}_band"))
            desc  = _str(row.get(f"{col}_desc"))
            sig_h = _str(row.get(f"{col}_signal_high"))
            sig_l = _str(row.get(f"{col}_signal_low"))
            val   = row.get(f"{col}_norm", float("nan"))

            tooltip = desc
            if sig_h or sig_l:
                tooltip = f"{desc}\n\n{sig_h}\n{sig_l}".strip()

            c1, c2, c3 = st.columns([3, 1, 1])
            with c1:
                st.markdown(f"**{title}**  \n<small>{desc}</small>", unsafe_allow_html=True)
            with c2:
                st.markdown(
                    f"<div style='font-size:1.5rem;text-align:center'>{emoji}</div>",
                    unsafe_allow_html=True,
                )
                st.caption(band)
            with c3:
                st.metric(label="Score (0–100)", value=fmt_val(val), help=tooltip)
            st.divider()


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(page_title="Institution Explorer", page_icon="🎓", layout="wide")

    df = load_data()

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

        st.markdown("---")
        st.caption(f"{_str(row.get('org_type'))}  ·  {_str(row.get('state'))}")
        if pd.notna(row.get("total_courses")):
            st.caption(f"{int(row['total_courses'])} courses in DB")

    # ── pre-rank metrics + AI description ─────────────────────────────────
    scored     = _scored_metrics(row)
    highlights = scored[:5] + scored[-2:]

    if st.session_state.get("ai_institution") != inst_name:
        st.session_state.pop("ai_description", None)
        st.session_state["ai_institution"] = inst_name
        with st.spinner("Generating AI description…"):
            st.session_state["ai_description"] = generate_ai_description(row, highlights)

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
            sector = _str(row.get("sector_name"))
            ot     = _str(row.get("organisation_type_name")) or _str(row.get("org_type"))
            st.caption(" · ".join(p for p in [ot, sector] if p))

            street = build_street_address(row)
            if street:
                st.markdown(f"📍 {street}")

            website = _str(row.get("organisation_web_address"))
            if website:
                st.markdown(f"🌐 [{website}]({website})")

        with col_img:
            image_url, img_source = fetch_image(row["name"], row)
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
        rent    = row.get("median_rent_weekly")
        seifa_d = row.get("seifa_irsad_decile")
        seifa_l = _str(row.get("seifa_label"))
        ns1.metric("SA2 area",    _str(row.get("sa2_name")) or "—")
        ns2.metric("SEIFA",       f"{int(seifa_d)}/10 — {seifa_l}" if pd.notna(seifa_d) else "—")
        ns3.metric("Median rent", f"${int(rent)}/wk" if pd.notna(rent) else "—")

        # ── top 3 neighbourhood strengths ──────────────────────────────
        if scored:
            st.markdown("---")
            st.markdown("**Neighbourhood strengths**")
            sc1, sc2, sc3 = st.columns(3)
            for col_, m in zip([sc1, sc2, sc3], scored[:3]):
                col_.metric(m["title"], f"{m['emoji']} {m['band']}")

        # ── ACIR campus info ───────────────────────────────────────────
        cricos = _str(row.get("organisation_cricos_code"))
        rto    = _str(row.get("organisation_rto_code"))
        if cricos or rto:
            st.markdown("---")
            r1, r2 = st.columns(2)
            if cricos: r1.caption(f"CRICOS: {cricos}")
            if rto:    r2.caption(f"RTO: {rto}")

        for label, key in [
            ("📚 Study areas",   "site_study_area"),
            ("🚌 Transport",     "site_transport"),
            ("🏠 Accommodation", "site_accommodation"),
            ("🏛️ Campus notes",  "site_comments"),
        ]:
            val = _str(row.get(key))
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
            for col_, m in zip([c1, c2, c3], scored[:3]):
                col_.metric(m["title"], f"{m['emoji']} {m['band']}")

            st.markdown("**Watch-outs**")
            w1, w2 = st.columns(2)
            for col_, m in zip([w1, w2], list(reversed(scored))[:2]):
                col_.metric(m["title"], f"{m['emoji']} {m['band']}")

        st.markdown("---")
        st.subheader("About this area")
        st.markdown(st.session_state.get("ai_description", ""))

        st.markdown("---")
        for section in SECTIONS:
            render_metric_section(row, section)

    # ══════════════════════════════════════════════════════════════════════
    # TAB 3 — COURSES & ENTRY
    # ══════════════════════════════════════════════════════════════════════
    with tab_courses:
        total_c = row.get("total_courses")
        if pd.isna(total_c):
            st.info("No course data available for this institution.")
        else:
            st.subheader("Course profile")
            cp_left, cp_right = st.columns([2, 1])

            with cp_left:
                st.markdown("**Course level mix**")
                levels = {
                    "Undergraduate":    row.get("pct_undergraduate",  0) or 0,
                    "Postgraduate":     row.get("pct_postgraduate",   0) or 0,
                    "VET / Vocational": row.get("pct_vet_vocational", 0) or 0,
                    "Secondary":        row.get("pct_secondary",      0) or 0,
                }
                levels = {k: v for k, v in levels.items() if v > 0}
                if levels:
                    for label, pct in levels.items():
                        st.markdown(f"**{label}** — {pct:.0f}%")
                        st.progress(int(pct))
                else:
                    st.caption("Course level breakdown not available.")

            with cp_right:
                st.metric("Total courses", int(total_c))
                faculty_c = row.get("faculty_count")
                st.metric("Faculties / areas", int(faculty_c) if pd.notna(faculty_c) else "—")
                free_t = row.get("free_tafe_count")
                if pd.notna(free_t) and int(free_t) > 0:
                    st.metric("Free TAFE courses", int(free_t))

            st.markdown("---")
            st.subheader("Fees (domestic)")
            f1, f2, f3 = st.columns(3)
            f1.metric("Min fee", fmt_currency(row.get("min_domestic_fee")))
            f2.metric("Avg fee", fmt_currency(row.get("avg_domestic_fee")))
            f3.metric("Max fee", fmt_currency(row.get("max_domestic_fee")))

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
            e2.metric("Avg IELTS", fmt_val(avg_ielts) if pd.notna(avg_ielts) else "Not required")

            pct_alt = row.get("pct_with_alternate_entry")
            e3.metric(
                "Alternate entry",
                f"{pct_alt:.0f}% of courses" if pd.notna(pct_alt) and pct_alt > 0 else "—",
            )

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

            top5_raw = _str(row.get("top_5_careers"))
            if top5_raw:
                st.markdown("**Top careers linked to courses here**")
                for i, career in enumerate(top5_raw.split(" | "), 1):
                    st.markdown(f"{i}. {career.strip()}")

            st.markdown("---")
            st.subheader("Career domain breakdown")
            d_left, d_right = st.columns([1, 1])

            with d_left:
                for label, col_, icon in [
                    ("STEM",               "stem_career_pct",             "🔬"),
                    ("Health",             "health_career_pct",           "🏥"),
                    ("Education",          "education_career_pct",        "📚"),
                    ("Business & Finance", "business_finance_career_pct", "💼"),
                ]:
                    val = row.get(col_)
                    st.metric(f"{icon} {label}", f"{val:.0f}%" if pd.notna(val) else "—")

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
