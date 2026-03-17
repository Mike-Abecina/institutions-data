"""
VIBE CHECK -- Institution Discovery Platform
Main Streamlit entry point.

Run from the institutions_design_poc/ directory:
    streamlit run scripts/C_ui/app.py
"""

import json
import sys
from pathlib import Path

import streamlit as st

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so relative imports work when
# running via `streamlit run scripts/C_ui/app.py` from institutions_design_poc/
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Page config -- MUST be the first Streamlit command
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="VIBE CHECK",
    page_icon="\U0001F525",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Now safe to import project modules
# ---------------------------------------------------------------------------
from scripts.C_ui.styles.theme import inject_css
from scripts.C_ui.pages.vibe_quiz import render_vibe_quiz
from scripts.C_ui.pages.discover_feed import render_discover_feed
from scripts.C_ui.pages.compare import render_compare

# ---------------------------------------------------------------------------
# Inject custom CSS
# ---------------------------------------------------------------------------
inject_css()

# ---------------------------------------------------------------------------
# FALLBACK DATA -- 3 sample institutions so the UI works without the
# enrichment pipeline.  Follows the InstitutionCard model from config/models.py.
# ---------------------------------------------------------------------------
FALLBACK_DATA: list[dict] = [
    {
        "id": "jcu-townsville",
        "name": "James Cook University",
        "city": "Townsville",
        "state": "QLD",
        "latitude": -19.3297,
        "longitude": 146.7567,
        "institution_type": "University",
        "student_count": 14000,
        "tagline": "Where the reef is your classroom",
        "tagline_reasoning": "JCU is Australia's leading tropical research university with direct reef access.",
        "hero_gradient": "warm",
        "tags": ["Marine Science", "Tropical", "#3 in Australia for Environment"],
        "vibe_tags": ["laid-back", "outdoorsy", "tight-knit", "research-heavy", "tropical vibes"],
        "campus_mood": {"study": 42, "social": 67, "chill": 83},
        "student_quote": "The campus feels like a tropical resort that accidentally has a world-class marine science lab in it",
        "student_quote_author": "@sarah_marine",
        "student_quote_year": "2nd year Biology",
        "walk_score": 72,
        "transit_score": 45,
        "bike_score": 68,
        "nearby_venues": [
            {
                "name": "The Coffee Club",
                "venue_type": "cafe",
                "distance_walk_min": 3,
                "student_review": "Best $4 flat white in QLD",
                "student_reviewer": "@jake_22",
                "rating": 4.5,
                "price_level": "$",
            },
            {
                "name": "Donna Thai",
                "venue_type": "restaurant",
                "distance_walk_min": 5,
                "student_review": "The pad thai saved my exam week",
                "student_reviewer": "@mei",
                "rating": 4.7,
                "price_level": "$$",
            },
            {
                "name": "Townsville Brewery",
                "venue_type": "bar",
                "distance_walk_min": 10,
                "student_review": "Date night sorted",
                "student_reviewer": "@coralboy_",
                "rating": 4.3,
                "price_level": "$$",
            },
            {
                "name": "Cotters Market",
                "venue_type": "restaurant",
                "distance_walk_min": 8,
                "student_review": "Street food heaven every Saturday",
                "student_reviewer": "@reef_girl",
                "rating": 4.6,
                "price_level": "$",
            },
        ],
        "cafes_10min": 23,
        "bars_10min": 8,
        "beaches_15min": 3,
        "day_in_life": [
            {"time": "7:00", "activity": "Sunrise surf", "emoji": "\U0001F3C4", "description": "Sunrise surf at The Strand"},
            {"time": "9:00", "activity": "Marine lab", "emoji": "\U0001F9EA", "description": "Marine lab -- yes, real coral"},
            {"time": "12:00", "activity": "Lunch", "emoji": "\U0001F35C", "description": "$8 poke bowl from the food court"},
            {"time": "14:00", "activity": "Study", "emoji": "\U0001F4DA", "description": "Study spot: library rooftop"},
            {"time": "17:00", "activity": "Dive club", "emoji": "\U0001F93F", "description": "Dive club training at the reef"},
            {"time": "20:00", "activity": "Sunset beers", "emoji": "\U0001F37B", "description": "Sunset beers at Palmer St"},
        ],
        "clubs": [
            "Dive Club", "Reef Research", "Rugby", "First Nations Society",
            "Enviro Action", "Music Scene", "Esports", "Volunteer Corps",
        ],
        "transport": {
            "bus_to_cbd": "12 min",
            "airport": "20 min",
            "other": "Brisbane: 3.5 hr drive or 2 hr flight",
        },
        "safety_support": [
            "24/7 campus security",
            "Free mental health: 10 sessions/year",
            "Peer mentoring program",
        ],
        "cost": {
            "tuition_range_low": 7200,
            "tuition_range_high": 11800,
            "tuition_comparison": "Cheaper than average",
            "rent_range_low": 180,
            "rent_range_high": 280,
            "rent_comparison": "Way cheaper than Sydney",
            "weekly_budget": {"rent": 220, "food": 80, "transport": 25, "fun": 50},
            "total_weekly": 375,
        },
        "scholarships": [
            {"name": "JCU Excellence Scholarship", "amount": "$10K/yr", "eligibility": "ATAR 95+"},
            {"name": "Regional Student Support", "amount": "$5K/yr", "eligibility": "Regional/remote students"},
            {"name": "First in Family Grant", "amount": "$3K", "eligibility": "First in family to attend uni"},
        ],
        "top_courses": ["Marine Biology", "Environmental Science", "Veterinary Science"],
        "vibe_location": "beach",
        "vibe_energy": "discovering",
        "vibe_weekend": "bushwalk",
    },
    {
        "id": "unsw-sydney",
        "name": "UNSW Sydney",
        "city": "Sydney",
        "state": "NSW",
        "latitude": -33.9173,
        "longitude": 151.2313,
        "institution_type": "University",
        "student_count": 63000,
        "tagline": "The city never sleeps. Neither will you.",
        "tagline_reasoning": "UNSW is in the heart of Sydney's eastern suburbs, known for engineering, tech, and nightlife.",
        "hero_gradient": "purple",
        "tags": ["Engineering", "Tech Hub", "Top 50 Global"],
        "vibe_tags": ["ambitious", "diverse", "fast-paced", "tech-savvy", "urban"],
        "campus_mood": {"study": 75, "social": 60, "chill": 35},
        "student_quote": "You'll pull more all-nighters than you planned, but the people you meet at 2am in the library become your best mates",
        "student_quote_author": "@eng_nerd_sam",
        "student_quote_year": "3rd year Software Engineering",
        "walk_score": 88,
        "transit_score": 82,
        "bike_score": 71,
        "nearby_venues": [
            {
                "name": "Coogee Pavilion",
                "venue_type": "restaurant",
                "distance_walk_min": 12,
                "student_review": "Sunday rooftop sessions are elite",
                "student_reviewer": "@sydney_syd",
                "rating": 4.4,
                "price_level": "$$",
            },
            {
                "name": "Kurtosh",
                "venue_type": "cafe",
                "distance_walk_min": 2,
                "student_review": "The chimney cakes got me through exam season",
                "student_reviewer": "@food_coma_uni",
                "rating": 4.6,
                "price_level": "$",
            },
            {
                "name": "Scary Canary",
                "venue_type": "bar",
                "distance_walk_min": 15,
                "student_review": "Cheapest jugs in Surry Hills, say less",
                "student_reviewer": "@nightowl_k",
                "rating": 4.1,
                "price_level": "$",
            },
            {
                "name": "Mamak",
                "venue_type": "restaurant",
                "distance_walk_min": 18,
                "student_review": "Roti canai at midnight -- trust me",
                "student_reviewer": "@late_bites",
                "rating": 4.5,
                "price_level": "$",
            },
        ],
        "cafes_10min": 45,
        "bars_10min": 18,
        "beaches_15min": 2,
        "day_in_life": [
            {"time": "8:00", "activity": "Coffee run", "emoji": "\u2615", "description": "Flat white from Kurtosh before the 9am lecture"},
            {"time": "9:00", "activity": "Lecture", "emoji": "\U0001F4BB", "description": "Data Structures lecture in Ainsworth"},
            {"time": "12:00", "activity": "Lunch", "emoji": "\U0001F35C", "description": "$10 pho from the Asian food court"},
            {"time": "13:00", "activity": "Lab", "emoji": "\U0001F52C", "description": "Robotics lab -- building something that moves"},
            {"time": "16:00", "activity": "Beach", "emoji": "\U0001F3D6\uFE0F", "description": "Quick dip at Coogee Beach"},
            {"time": "19:00", "activity": "Society", "emoji": "\U0001F3A4", "description": "Comedy society open mic night"},
            {"time": "22:00", "activity": "Study", "emoji": "\U0001F4DA", "description": "Library grind with the group chat"},
        ],
        "clubs": [
            "CSESoc (CompSci)", "Enactus", "Debating", "Surf Club",
            "MedRevue", "Robotics", "Women in Engineering", "Pride",
        ],
        "transport": {
            "bus_to_cbd": "25 min",
            "train": "Light rail to Central: 20 min",
            "airport": "30 min drive",
        },
        "safety_support": [
            "24/7 campus security with SafeZone app",
            "Free counselling: 6 sessions/semester",
            "UNSW Student Wellbeing Hub",
            "Nighttime shuttle service",
        ],
        "cost": {
            "tuition_range_low": 9500,
            "tuition_range_high": 16500,
            "tuition_comparison": "Mid-range for Group of Eight",
            "rent_range_low": 280,
            "rent_range_high": 450,
            "rent_comparison": "Sydney prices -- no sugar-coating",
            "weekly_budget": {"rent": 350, "food": 100, "transport": 40, "fun": 80},
            "total_weekly": 570,
        },
        "scholarships": [
            {"name": "UNSW Scientia Scholarship", "amount": "$20K/yr", "eligibility": "ATAR 99+ or equivalent"},
            {"name": "Equity Scholarship", "amount": "$8K/yr", "eligibility": "Financial hardship"},
            {"name": "Co-op Scholarship", "amount": "$19K total", "eligibility": "Industry placement program"},
        ],
        "top_courses": ["Software Engineering", "Commerce", "Mechanical Engineering"],
        "vibe_location": "city",
        "vibe_energy": "building",
        "vibe_weekend": "live_music",
    },
    {
        "id": "unimelb",
        "name": "University of Melbourne",
        "city": "Melbourne",
        "state": "VIC",
        "latitude": -37.7963,
        "longitude": 144.9614,
        "institution_type": "University",
        "student_count": 52000,
        "tagline": "Sandstone meets street art",
        "tagline_reasoning": "UniMelb sits in Parkville but the Melbourne CBD culture is inseparable from the student experience.",
        "hero_gradient": "cool",
        "tags": ["Arts & Culture", "#1 in Australia", "Melbourne Model"],
        "vibe_tags": ["intellectual", "creative", "progressive", "cafe-obsessed", "eclectic"],
        "campus_mood": {"study": 68, "social": 55, "chill": 52},
        "student_quote": "You'll argue about philosophy over a $5 latte in a laneway cafe and somehow that IS the education",
        "student_quote_author": "@arts_kid_mel",
        "student_quote_year": "2nd year Arts",
        "walk_score": 94,
        "transit_score": 90,
        "bike_score": 85,
        "nearby_venues": [
            {
                "name": "Lygon Street Cafes",
                "venue_type": "cafe",
                "distance_walk_min": 5,
                "student_review": "The espresso capital of Australia, fight me",
                "student_reviewer": "@coffee_snob_mel",
                "rating": 4.8,
                "price_level": "$$",
            },
            {
                "name": "Pellegrini's",
                "venue_type": "restaurant",
                "distance_walk_min": 12,
                "student_review": "Spaghetti bolognese that feels like a hug",
                "student_reviewer": "@pasta_kid",
                "rating": 4.7,
                "price_level": "$$",
            },
            {
                "name": "Carlton Club",
                "venue_type": "bar",
                "distance_walk_min": 6,
                "student_review": "Trivia night on Wednesdays -- bring your A-game",
                "student_reviewer": "@quizmaster_j",
                "rating": 4.3,
                "price_level": "$$",
            },
            {
                "name": "Queen Vic Market",
                "venue_type": "restaurant",
                "distance_walk_min": 15,
                "student_review": "Saturday morning dumplings are non-negotiable",
                "student_reviewer": "@market_mornings",
                "rating": 4.9,
                "price_level": "$",
            },
        ],
        "cafes_10min": 67,
        "bars_10min": 24,
        "beaches_15min": None,
        "day_in_life": [
            {"time": "8:30", "activity": "Coffee", "emoji": "\u2615", "description": "Lygon St flat white -- the ritual"},
            {"time": "10:00", "activity": "Lecture", "emoji": "\U0001F3DB\uFE0F", "description": "Philosophy lecture in the Old Arts building"},
            {"time": "12:00", "activity": "Lunch", "emoji": "\U0001F96A", "description": "Banh mi from the Vietnamese place on Swanston"},
            {"time": "13:30", "activity": "Study", "emoji": "\U0001F4DA", "description": "Baillieu Library -- finding your favourite spot"},
            {"time": "16:00", "activity": "Society", "emoji": "\U0001F3A8", "description": "Student theatre rehearsals"},
            {"time": "18:00", "activity": "Explore", "emoji": "\U0001F3D9\uFE0F", "description": "Laneway street art walk in the CBD"},
            {"time": "20:00", "activity": "Dinner", "emoji": "\U0001F35C", "description": "Cheap eats on Swanston St with the crew"},
        ],
        "clubs": [
            "Melbourne Uni Theatre", "Philosophy Society", "Film Club",
            "Queer Collective", "Environment Collective", "Cricket Club",
            "Debating Society", "Volunteer Program",
        ],
        "transport": {
            "tram": "Free tram zone covers campus to CBD",
            "train": "Flinders St Station: 15 min tram",
            "airport": "Tullamarine: 35 min Skybus",
        },
        "safety_support": [
            "24/7 campus security",
            "Free mental health: 8 sessions/year",
            "Student peer support network",
            "After-hours GP clinic on campus",
        ],
        "cost": {
            "tuition_range_low": 9200,
            "tuition_range_high": 14800,
            "tuition_comparison": "Mid-range for Group of Eight",
            "rent_range_low": 250,
            "rent_range_high": 400,
            "rent_comparison": "Melbourne is cheaper than Sydney, but not by much",
            "weekly_budget": {"rent": 310, "food": 90, "transport": 0, "fun": 70},
            "total_weekly": 470,
        },
        "scholarships": [
            {"name": "Melbourne Chancellor's Scholarship", "amount": "$15K/yr", "eligibility": "ATAR 99.90+"},
            {"name": "Access Melbourne", "amount": "Varies", "eligibility": "Disadvantaged backgrounds"},
            {"name": "Melbourne Global Scholars Award", "amount": "$10K", "eligibility": "International exchange component"},
        ],
        "top_courses": ["Arts", "Biomedicine", "Commerce"],
        "vibe_location": "city",
        "vibe_energy": "creating",
        "vibe_weekend": "markets",
    },
]


# ---------------------------------------------------------------------------
# Data loader
# ---------------------------------------------------------------------------
def load_institutions() -> list[dict]:
    """Load institution data from fixtures JSON, falling back to FALLBACK_DATA."""
    fixtures_path = _PROJECT_ROOT / "data" / "fixtures" / "institutions_sample.json"
    if fixtures_path.exists():
        try:
            data = json.loads(fixtures_path.read_text())
            if isinstance(data, list) and len(data) > 0:
                return data
        except (json.JSONDecodeError, KeyError):
            pass
    return FALLBACK_DATA


# ---------------------------------------------------------------------------
# Session state defaults
# ---------------------------------------------------------------------------
def _init_state() -> None:
    defaults = {
        "page": "quiz",           # "quiz" | "discover" | "compare"
        "vibe_answers": {},
        "saved_institutions": set(),
        "current_index": 0,
        "quiz_question": 0,
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


_init_state()

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
institutions = load_institutions()

# ---------------------------------------------------------------------------
# Top navigation
# ---------------------------------------------------------------------------
st.markdown(
    """
    <div style="text-align: center; margin-bottom: 0.25rem;">
        <span style="font-family: var(--font-heading); font-weight: 800; font-size: 1.8rem;
                     background: linear-gradient(135deg, #FF5733, #FFD166);
                     -webkit-background-clip: text; -webkit-text-fill-color: transparent;
                     background-clip: text;">
            VIBE CHECK
        </span>
    </div>
    """,
    unsafe_allow_html=True,
)

nav_cols = st.columns([1, 1, 1, 1, 1])

with nav_cols[1]:
    if st.button("DISCOVER", key="nav_discover", use_container_width=True):
        if st.session_state.get("vibe_answers"):
            st.session_state["page"] = "discover"
        else:
            st.session_state["page"] = "quiz"
        st.rerun()

with nav_cols[2]:
    if st.button("SEARCH", key="nav_search", use_container_width=True):
        # Search redirects to discover for POC
        if st.session_state.get("vibe_answers"):
            st.session_state["page"] = "discover"
        else:
            st.session_state["page"] = "quiz"
        st.rerun()

with nav_cols[3]:
    if st.button("COMPARE", key="nav_compare", use_container_width=True):
        st.session_state["page"] = "compare"
        st.rerun()

st.markdown("<hr style='border-color: #2A2A2A; margin: 0.5rem 0 1.5rem;'>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Route to the active page
# ---------------------------------------------------------------------------
page = st.session_state.get("page", "quiz")

if page == "quiz":
    render_vibe_quiz()

elif page == "discover":
    render_discover_feed(institutions)

elif page == "compare":
    render_compare(institutions)

else:
    # Fallback
    render_vibe_quiz()
