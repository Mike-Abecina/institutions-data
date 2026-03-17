"""
Base institution data for the 5 POC target institutions.

Importable by all B_enrichment scripts:
    from scripts.B_enrichment._base_institutions import INSTITUTIONS, get_by_id
"""

from typing import Dict, List

INSTITUTIONS: List[Dict] = [
    {
        "id": "jcu-townsville",
        "name": "James Cook University",
        "city": "Townsville",
        "state": "QLD",
        "latitude": -19.3299,
        "longitude": 146.7578,
        "institution_type": "University",
        "student_count": 22_000,
        "top_courses": [
            "Marine Biology",
            "Environmental Science",
            "Tropical Medicine",
            "Veterinary Science",
            "Tourism & Hospitality",
        ],
        "key_features": [
            "Only uni in the world located next to two World Heritage sites",
            "Access to Great Barrier Reef for research",
            "Strong Indigenous health programs",
            "Tropical campus with on-site wildlife",
            "Small class sizes and close-knit community",
        ],
    },
    {
        "id": "griffith-gold-coast",
        "name": "Griffith University Gold Coast",
        "city": "Gold Coast",
        "state": "QLD",
        "latitude": -27.9621,
        "longitude": 153.3822,
        "institution_type": "University",
        "student_count": 50_000,
        "top_courses": [
            "Health Sciences",
            "Criminology",
            "Marine Science",
            "Business & Aviation",
            "Film & Screen Media",
        ],
        "key_features": [
            "Campus minutes from Surfers Paradise beaches",
            "State-of-the-art health and sport facilities",
            "Trimester system for accelerated study",
            "Strong links to Gold Coast 2018 Commonwealth Games legacy",
            "Growing tech and startup hub",
        ],
    },
    {
        "id": "uq-brisbane",
        "name": "University of Queensland",
        "city": "Brisbane",
        "state": "QLD",
        "latitude": -27.4975,
        "longitude": 153.0137,
        "institution_type": "University",
        "student_count": 55_000,
        "top_courses": [
            "Engineering",
            "Biomedical Science",
            "Law",
            "Environmental Management",
            "Computer Science",
        ],
        "key_features": [
            "Sandstone Great Court heritage campus",
            "Group of Eight research-intensive university",
            "UQ Lakes and riverside setting in St Lucia",
            "World-leading vaccine research (including COVID)",
            "450+ student clubs and societies",
        ],
    },
    {
        "id": "unsw-sydney",
        "name": "UNSW Sydney",
        "city": "Sydney",
        "state": "NSW",
        "latitude": -33.9173,
        "longitude": 151.2313,
        "institution_type": "University",
        "student_count": 63_000,
        "top_courses": [
            "Engineering",
            "Computer Science & AI",
            "Commerce & Finance",
            "Law",
            "Design & Architecture",
        ],
        "key_features": [
            "Located in Kensington, close to Sydney CBD and beaches",
            "Group of Eight research-intensive university",
            "Australia's largest engineering faculty",
            "Strong industry partnerships and co-op programs",
            "Diverse student body with 150+ nationalities",
        ],
    },
    {
        "id": "unimelb",
        "name": "University of Melbourne",
        "city": "Melbourne",
        "state": "VIC",
        "latitude": -37.7963,
        "longitude": 144.9614,
        "institution_type": "University",
        "student_count": 65_000,
        "top_courses": [
            "Medicine",
            "Arts & Humanities",
            "Law",
            "Science",
            "Commerce",
        ],
        "key_features": [
            "Australia's #1 ranked university (multiple rankings)",
            "Historic Parkville campus in Melbourne's inner north",
            "Melbourne Model breadth-first curriculum",
            "World-class research across all disciplines",
            "Vibrant laneway culture and foodie scene at doorstep",
        ],
    },
]


def get_by_id(institution_id: str) -> Dict:
    """Look up a single institution by its id string."""
    for inst in INSTITUTIONS:
        if inst["id"] == institution_id:
            return inst
    raise KeyError(f"Unknown institution id: {institution_id}")


def get_all_ids() -> List[str]:
    """Return the list of all institution IDs in order."""
    return [inst["id"] for inst in INSTITUTIONS]
