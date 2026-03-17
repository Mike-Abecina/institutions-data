"""
07_build_institution_json.py -- Merge ALL enrichment outputs into a single
institutions_complete.json, validated against the InstitutionCard Pydantic model.

Reads from data/enriched/:
  - places_nearby.json   (01)
  - walk_scores.json     (02)
  - rent_data.json       (03)
  - cost_data.json       (03)
  - taglines.json        (04)
  - day_in_life.json     (05)
  - vibe_tags.json       (06)

Produces:
  - data/enriched/institutions_complete.json   (canonical output)
  - data/fixtures/institutions_sample.json     (same data, for UI dev)

Run:
    python -m scripts.B_enrichment.07_build_institution_json
"""

import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import ENRICHED_DIR, FIXTURES_DIR
from config.models import (
    InstitutionCard,
    NearbyVenue,
    DayInLifeEntry,
    CostBreakdown,
    Scholarship,
)
from scripts.B_enrichment._base_institutions import INSTITUTIONS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _load_json(filename: str) -> Dict:
    """Load a JSON file from the enriched directory, returning {} if missing."""
    path = ENRICHED_DIR / filename
    if not path.exists():
        print(f"  WARNING: {filename} not found -- using empty defaults")
        return {}
    with open(path) as f:
        return json.load(f)


def _hero_gradient(name: str) -> str:
    """Generate a deterministic CSS gradient based on institution name hash.

    Produces visually distinct gradients that look good as card backgrounds.
    """
    h = hashlib.md5(name.encode()).hexdigest()

    # Use different segments of the hash for hue, saturation adjustments
    hue1 = int(h[:3], 16) % 360
    hue2 = (hue1 + 40 + int(h[3:5], 16) % 30) % 360  # 40-70 degree shift
    sat1 = 55 + int(h[5:7], 16) % 30   # 55-85%
    sat2 = 50 + int(h[7:9], 16) % 35   # 50-85%
    light1 = 35 + int(h[9:11], 16) % 20  # 35-55%
    light2 = 25 + int(h[11:13], 16) % 20  # 25-45%
    angle = 120 + int(h[13:15], 16) % 60  # 120-180 degrees

    return (
        f"linear-gradient({angle}deg, "
        f"hsl({hue1}, {sat1}%, {light1}%), "
        f"hsl({hue2}, {sat2}%, {light2}%))"
    )


def _assign_quiz_vibes(inst: Dict) -> Dict[str, str]:
    """Assign quiz-matching vibe categories based on institution characteristics."""
    vibe_map = {
        "jcu-townsville": {
            "vibe_location": "beach",
            "vibe_energy": "discovering",
            "vibe_weekend": "bushwalk",
        },
        "griffith-gold-coast": {
            "vibe_location": "beach",
            "vibe_energy": "creating",
            "vibe_weekend": "bushwalk",
        },
        "uq-brisbane": {
            "vibe_location": "campus",
            "vibe_energy": "discovering",
            "vibe_weekend": "markets",
        },
        "unsw-sydney": {
            "vibe_location": "city",
            "vibe_energy": "building",
            "vibe_weekend": "live_music",
        },
        "unimelb": {
            "vibe_location": "city",
            "vibe_energy": "creating",
            "vibe_weekend": "live_music",
        },
    }
    return vibe_map.get(inst["id"], {
        "vibe_location": "campus",
        "vibe_energy": "discovering",
        "vibe_weekend": "markets",
    })


# ---------------------------------------------------------------------------
# Default / fallback data (for fields not covered by enrichment scripts)
# ---------------------------------------------------------------------------
DEFAULT_CLUBS: Dict[str, List[str]] = {
    "jcu-townsville": [
        "JCU Reef & Rainforest Society",
        "Marine Biology Students Association",
        "JCU Rugby Union Club",
        "Townsville Adventure Club",
        "Indigenous Students Society",
    ],
    "griffith-gold-coast": [
        "Griffith Surf Club",
        "Gold Coast Film Society",
        "Health Sciences Students Association",
        "Griffith Business Society",
        "Griffith Adventure Club",
    ],
    "uq-brisbane": [
        "UQ Union",
        "UQ Debating Society",
        "Engineering Undergrad Society",
        "UQ Rotaract",
        "St Lucia Running Club",
    ],
    "unsw-sydney": [
        "UNSW Engineering Society",
        "Arc @ UNSW",
        "CSESoc (Computer Science & Engineering)",
        "UNSW Debate Society",
        "Queer Students Collective",
    ],
    "unimelb": [
        "Melbourne University Law Students' Society",
        "UMSU (Student Union)",
        "Melbourne Uni Mountaineering Club",
        "Science Students' Society",
        "Melbourne Uni Film Society",
    ],
}

DEFAULT_TRANSPORT: Dict[str, Dict[str, str]] = {
    "jcu-townsville": {
        "bus_to_cbd": "15 min",
        "airport": "20 min drive",
        "nearest_beach": "The Strand -- 10 min drive",
        "parking": "Free on campus",
    },
    "griffith-gold-coast": {
        "tram_to_surfers": "15 min",
        "airport": "25 min drive",
        "nearest_beach": "Burleigh Heads -- 10 min drive",
        "bus_to_campus": "G:link tram stop on campus",
    },
    "uq-brisbane": {
        "bus_to_cbd": "20 min (412/411)",
        "citycat_ferry": "25 min to South Bank",
        "airport": "35 min drive",
        "bike_path": "Bicentennial Bikeway to CBD",
    },
    "unsw-sydney": {
        "bus_to_cbd": "25 min (891/893)",
        "light_rail": "Kingsford stop -- 5 min walk",
        "airport": "20 min drive",
        "nearest_beach": "Coogee -- 15 min bus",
    },
    "unimelb": {
        "tram_to_cbd": "10 min (tram 1, 6, 16)",
        "train": "Melbourne Central -- 15 min walk",
        "airport": "SkyBus from Southern Cross -- 30 min",
        "bike": "Capital City Trail access",
    },
}

DEFAULT_SAFETY: List[str] = [
    "24/7 campus security",
    "SafeZone app for emergencies",
    "Free counselling services",
    "After-hours study space escorts",
]

DEFAULT_SCHOLARSHIPS: Dict[str, List[Dict]] = {
    "jcu-townsville": [
        {"name": "JCU Regional Scholarship", "amount": "$6,000/yr", "eligibility": "Regional QLD students"},
        {"name": "JCU Excellence Scholarship", "amount": "$10,000/yr", "eligibility": "ATAR 95+"},
    ],
    "griffith-gold-coast": [
        {"name": "Griffith Award for Academic Excellence", "amount": "$5,000/yr", "eligibility": "ATAR 91+"},
        {"name": "Griffith Remarkable Scholarship", "amount": "$10,000/yr", "eligibility": "ATAR 99+"},
    ],
    "uq-brisbane": [
        {"name": "UQ Vice-Chancellor's Scholarship", "amount": "$12,000/yr", "eligibility": "ATAR 99.9+"},
        {"name": "UQ Merit Scholarship", "amount": "$6,000/yr", "eligibility": "ATAR 96+"},
    ],
    "unsw-sydney": [
        {"name": "UNSW Scientia Scholarship", "amount": "$20,000/yr", "eligibility": "ATAR 98+ and leadership"},
        {"name": "UNSW Academic Achievement Award", "amount": "$5,000", "eligibility": "ATAR 96+"},
    ],
    "unimelb": [
        {"name": "Melbourne Chancellor's Scholarship", "amount": "$15,000/yr", "eligibility": "ATAR 99.90+"},
        {"name": "Melbourne Access Scholarship", "amount": "$10,000/yr", "eligibility": "Equity criteria"},
    ],
}


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------
def build_institution_card(inst: Dict, enrichment: Dict[str, Dict]) -> InstitutionCard:
    """Assemble all enrichment data into a single InstitutionCard."""
    iid = inst["id"]

    # -- Tagline (from 04)
    taglines = enrichment.get("taglines", {}).get(iid, {})
    tagline = taglines.get("tagline", f"Discover {inst['name']}")
    tagline_reasoning = taglines.get("reasoning", "")

    # -- Venues (from 01)
    raw_venues = enrichment.get("places_nearby", {}).get(iid, [])
    nearby_venues = [NearbyVenue(**v) for v in raw_venues]
    cafes_10min = sum(1 for v in nearby_venues if v.venue_type == "cafe" and v.distance_walk_min <= 10)
    bars_10min = sum(1 for v in nearby_venues if v.venue_type == "bar" and v.distance_walk_min <= 10)

    # -- Walk scores (from 02)
    walk_data = enrichment.get("walk_scores", {}).get(iid, {})
    walk_score = walk_data.get("walk_score", 50)
    transit_score = walk_data.get("transit_score")
    bike_score = walk_data.get("bike_score")

    # -- Cost (from 03)
    cost_data = enrichment.get("cost_data", {}).get(iid, {})
    rent_data = enrichment.get("rent_data", {}).get(iid, {})
    cost = CostBreakdown(
        tuition_range_low=cost_data.get("tuition_range_low", 8000),
        tuition_range_high=cost_data.get("tuition_range_high", 14000),
        tuition_comparison=cost_data.get("tuition_comparison", "Average"),
        rent_range_low=cost_data.get("rent_range_low", rent_data.get("share_house_low", 200)),
        rent_range_high=cost_data.get("rent_range_high", rent_data.get("one_bedroom_high", 500)),
        rent_comparison=cost_data.get("rent_comparison", rent_data.get("rent_comparison", "")),
        weekly_budget=cost_data.get("weekly_budget", {"rent": 250, "food": 80, "transport": 30, "fun": 50}),
        total_weekly=cost_data.get("total_weekly", 410),
    )

    # -- Day in life (from 05)
    day_data = enrichment.get("day_in_life", {}).get(iid, {})
    raw_entries = day_data.get("entries", [])
    day_in_life = [DayInLifeEntry(**e) for e in raw_entries]

    # -- Vibe tags (from 06)
    vibe_data = enrichment.get("vibe_tags", {}).get(iid, {})
    vibe_tags = vibe_data.get("vibe_tags", ["welcoming", "academic", "diverse"])
    campus_mood = vibe_data.get("campus_mood", {"study": 60, "social": 60, "chill": 60})
    student_quote = vibe_data.get("student_quote", f"Love studying at {inst['name']}!")
    student_quote_author = vibe_data.get("student_quote_author", "Student")
    student_quote_year = vibe_data.get("student_quote_year", "")

    # -- Quiz vibes
    quiz_vibes = _assign_quiz_vibes(inst)

    # -- Hero gradient
    hero_gradient = _hero_gradient(inst["name"])

    # -- Tags (combine top courses + key features into display tags)
    tags = inst["top_courses"][:3] + [inst["key_features"][0]] if inst["key_features"] else inst["top_courses"][:4]

    # -- Scholarships
    raw_scholarships = DEFAULT_SCHOLARSHIPS.get(iid, [])
    scholarships = [Scholarship(**s) for s in raw_scholarships]

    # -- Beaches (for coastal institutions)
    beaches_15min = None
    if iid in ("jcu-townsville", "griffith-gold-coast"):
        beaches_15min = 3
    elif iid == "unsw-sydney":
        beaches_15min = 2

    return InstitutionCard(
        id=iid,
        name=inst["name"],
        city=inst["city"],
        state=inst["state"],
        latitude=inst["latitude"],
        longitude=inst["longitude"],
        institution_type=inst["institution_type"],
        student_count=inst.get("student_count"),
        tagline=tagline,
        tagline_reasoning=tagline_reasoning,
        hero_gradient=hero_gradient,
        tags=tags,
        vibe_tags=vibe_tags,
        campus_mood=campus_mood,
        student_quote=student_quote,
        student_quote_author=student_quote_author,
        student_quote_year=student_quote_year,
        walk_score=walk_score,
        transit_score=transit_score,
        bike_score=bike_score,
        nearby_venues=nearby_venues,
        cafes_10min=cafes_10min,
        bars_10min=bars_10min,
        beaches_15min=beaches_15min,
        day_in_life=day_in_life,
        clubs=DEFAULT_CLUBS.get(iid, []),
        transport=DEFAULT_TRANSPORT.get(iid, {}),
        safety_support=DEFAULT_SAFETY,
        cost=cost,
        scholarships=scholarships,
        top_courses=inst["top_courses"],
        **quiz_vibes,
    )


def main():
    """Load all enrichment files, build InstitutionCards, and save."""
    print("Loading enrichment data...\n")

    # Load all enrichment JSON files
    enrichment = {
        "places_nearby": _load_json("places_nearby.json"),
        "walk_scores": _load_json("walk_scores.json"),
        "rent_data": _load_json("rent_data.json"),
        "cost_data": _load_json("cost_data.json"),
        "taglines": _load_json("taglines.json"),
        "day_in_life": _load_json("day_in_life.json"),
        "vibe_tags": _load_json("vibe_tags.json"),
    }

    # Build cards
    cards: List[Dict[str, Any]] = []
    for inst in INSTITUTIONS:
        print(f"Building card for {inst['name']}...")
        try:
            card = build_institution_card(inst, enrichment)
            card_dict = card.model_dump()
            cards.append(card_dict)
            print(f"  Validated OK: tagline=\"{card.tagline}\", "
                  f"venues={len(card.nearby_venues)}, "
                  f"day_entries={len(card.day_in_life)}, "
                  f"vibe_tags={len(card.vibe_tags)}")
        except Exception as e:
            print(f"  VALIDATION ERROR: {e}")
            # Still include with raw data so we can debug
            cards.append({"id": inst["id"], "name": inst["name"], "_error": str(e)})

    # Save to both output locations
    complete_path = ENRICHED_DIR / "institutions_complete.json"
    fixtures_path = FIXTURES_DIR / "institutions_sample.json"

    for path in (complete_path, fixtures_path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(cards, f, indent=2)

    # Print summary
    print(f"\n{'='*60}")
    print("COMPLETENESS SUMMARY")
    print(f"{'='*60}")
    valid = sum(1 for c in cards if "_error" not in c)
    print(f"  Institutions:   {valid}/{len(INSTITUTIONS)} validated")

    for card in cards:
        if "_error" in card:
            print(f"  {card['name']:40s}  ERROR")
            continue
        completeness = []
        if card.get("tagline") and card["tagline"] != f"Discover {card['name']}":
            completeness.append("tagline")
        if card.get("nearby_venues"):
            completeness.append(f"venues({len(card['nearby_venues'])})")
        if card.get("walk_score", 0) > 0:
            completeness.append("walk_score")
        if card.get("day_in_life"):
            completeness.append(f"day_in_life({len(card['day_in_life'])})")
        if card.get("vibe_tags"):
            completeness.append(f"vibe_tags({len(card['vibe_tags'])})")
        if card.get("cost"):
            completeness.append("cost")
        if card.get("scholarships"):
            completeness.append(f"scholarships({len(card['scholarships'])})")
        pct = len(completeness) / 7 * 100
        print(f"  {card['name']:40s}  {pct:.0f}%  [{', '.join(completeness)}]")

    print(f"\nSaved to:")
    print(f"  {complete_path}")
    print(f"  {fixtures_path}")


if __name__ == "__main__":
    main()
