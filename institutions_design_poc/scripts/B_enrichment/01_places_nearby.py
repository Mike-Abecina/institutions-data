"""
01_places_nearby.py -- Hardcoded nearby venues for the 5 POC institutions.

Uses REAL restaurant/cafe/bar names that exist near each campus.
In production this would call the Google Places API; for the POC the data
is hand-curated so we can iterate on UI without API costs.

Run:
    python -m scripts.B_enrichment.01_places_nearby
"""

import json
import sys
from pathlib import Path

# Ensure the project root is on sys.path so imports work when run as a module
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import ENRICHED_DIR
from scripts.B_enrichment._base_institutions import INSTITUTIONS

# ---------------------------------------------------------------------------
# Hardcoded venue data  (swap for Google Places API call in production)
# ---------------------------------------------------------------------------
VENUES = {
    "jcu-townsville": [
        {
            "name": "Jam Corner",
            "venue_type": "cafe",
            "distance_walk_min": 3,
            "student_review": "Best study spot on campus -- strong flat whites and power outlets everywhere.",
            "student_reviewer": "Mia T., 2nd yr Marine Bio",
            "rating": 4.5,
            "price_level": "$",
        },
        {
            "name": "The Coffee Club Douglas",
            "venue_type": "cafe",
            "distance_walk_min": 6,
            "student_review": "Reliable brekkie before an 8am lecture. The big-brekkie wrap is a lifesaver.",
            "student_reviewer": "Jake P., 3rd yr Vet Science",
            "rating": 4.0,
            "price_level": "$$",
        },
        {
            "name": "Donna Thai",
            "venue_type": "restaurant",
            "distance_walk_min": 8,
            "student_review": "Cheapest pad thai in Townsville and massive portions. Student budget approved.",
            "student_reviewer": "Priya S., 1st yr Nursing",
            "rating": 4.3,
            "price_level": "$",
        },
        {
            "name": "Brewery Townsville",
            "venue_type": "bar",
            "distance_walk_min": 15,
            "student_review": "Friday arvo beers with the cohort. Live music some weekends too.",
            "student_reviewer": "Tom R., 4th yr Engineering",
            "rating": 4.4,
            "price_level": "$$",
        },
        {
            "name": "Palmer Street Food Precinct",
            "venue_type": "restaurant",
            "distance_walk_min": 18,
            "student_review": "A whole strip of restaurants -- we always end up here after exams.",
            "student_reviewer": "Chloe W., 2nd yr Psychology",
            "rating": 4.2,
            "price_level": "$$",
        },
    ],
    "griffith-gold-coast": [
        {
            "name": "Barefoot Barista",
            "venue_type": "cafe",
            "distance_walk_min": 5,
            "student_review": "Walk in from the beach, sandy feet welcome. Best iced coffee on the coast.",
            "student_reviewer": "Lily M., 1st yr Health Science",
            "rating": 4.6,
            "price_level": "$",
        },
        {
            "name": "Burleigh Pavilion",
            "venue_type": "bar",
            "distance_walk_min": 12,
            "student_review": "Sunset sessions on the rooftop -- feels like a holiday even during semester.",
            "student_reviewer": "Finn O., 3rd yr Business",
            "rating": 4.5,
            "price_level": "$$",
        },
        {
            "name": "The Collective Palm Beach",
            "venue_type": "cafe",
            "distance_walk_min": 10,
            "student_review": "Acai bowls and good vibes. The outdoor seating is perfect for group study.",
            "student_reviewer": "Hannah K., 2nd yr Criminology",
            "rating": 4.4,
            "price_level": "$$",
        },
        {
            "name": "BSKT Cafe",
            "venue_type": "cafe",
            "distance_walk_min": 14,
            "student_review": "Healthy eats and the smoothies are unreal. A bit pricey but worth it.",
            "student_reviewer": "Sam D., 1st yr Film & Screen",
            "rating": 4.3,
            "price_level": "$$",
        },
        {
            "name": "Gemelli Italian",
            "venue_type": "restaurant",
            "distance_walk_min": 16,
            "student_review": "Proper Italian. We save it for end-of-semester celebrations.",
            "student_reviewer": "Olivia C., 4th yr Marine Science",
            "rating": 4.6,
            "price_level": "$$$",
        },
    ],
    "uq-brisbane": [
        {
            "name": "Merlo Coffee (UQ Lakes)",
            "venue_type": "cafe",
            "distance_walk_min": 2,
            "student_review": "On-campus perfection. Grab a long black and sit by the lakes between lectures.",
            "student_reviewer": "Alex N., 2nd yr Engineering",
            "rating": 4.5,
            "price_level": "$",
        },
        {
            "name": "Cafe Sari",
            "venue_type": "restaurant",
            "distance_walk_min": 5,
            "student_review": "Malaysian laksa that tastes like home. Massive serves for under fifteen bucks.",
            "student_reviewer": "Wei L., 3rd yr Comp Sci",
            "rating": 4.4,
            "price_level": "$",
        },
        {
            "name": "The Plough Inn",
            "venue_type": "bar",
            "distance_walk_min": 8,
            "student_review": "Historic pub right on the river. Trivia nights are legendary.",
            "student_reviewer": "Bella F., 1st yr Law",
            "rating": 4.3,
            "price_level": "$$",
        },
        {
            "name": "Grill'd St Lucia",
            "venue_type": "restaurant",
            "distance_walk_min": 7,
            "student_review": "Reliable burgers when you need comfort food during assignment season.",
            "student_reviewer": "Marcus J., 2nd yr Biomed",
            "rating": 4.1,
            "price_level": "$$",
        },
        {
            "name": "Cheeky Poke Bar",
            "venue_type": "restaurant",
            "distance_walk_min": 4,
            "student_review": "Fresh poke bowls and quick service -- perfect lunch between tutorials.",
            "student_reviewer": "Sophie T., 3rd yr Env Science",
            "rating": 4.2,
            "price_level": "$$",
        },
    ],
    "unsw-sydney": [
        {
            "name": "Taste of Shanghai Kingsford",
            "venue_type": "restaurant",
            "distance_walk_min": 6,
            "student_review": "Best xiao long bao near campus. Always packed at lunch -- worth the wait.",
            "student_reviewer": "David Z., 2nd yr Commerce",
            "rating": 4.5,
            "price_level": "$$",
        },
        {
            "name": "Mamak",
            "venue_type": "restaurant",
            "distance_walk_min": 20,
            "student_review": "Roti canai at midnight after a library session. Sydney institution.",
            "student_reviewer": "Aisha R., 3rd yr Architecture",
            "rating": 4.4,
            "price_level": "$",
        },
        {
            "name": "Three Blue Ducks Rosebery",
            "venue_type": "cafe",
            "distance_walk_min": 18,
            "student_review": "Fancy brunch spot for when parents visit. The corn fritters are elite.",
            "student_reviewer": "Liam B., 1st yr Design",
            "rating": 4.6,
            "price_level": "$$$",
        },
        {
            "name": "The Doncaster",
            "venue_type": "bar",
            "distance_walk_min": 5,
            "student_review": "The local pub. Cheap jugs on Thursdays and pool tables. Classic uni bar.",
            "student_reviewer": "Emma G., 4th yr Eng",
            "rating": 4.0,
            "price_level": "$",
        },
        {
            "name": "Little L Thai",
            "venue_type": "restaurant",
            "distance_walk_min": 7,
            "student_review": "Ten-dollar pad see ew that actually fills you up. Student essential.",
            "student_reviewer": "Chris M., 2nd yr Comp Sci",
            "rating": 4.3,
            "price_level": "$",
        },
    ],
    "unimelb": [
        {
            "name": "Lune Croissanterie",
            "venue_type": "cafe",
            "distance_walk_min": 15,
            "student_review": "The best croissant in Australia, no debate. Treat yourself after exams.",
            "student_reviewer": "Zara P., 3rd yr Arts",
            "rating": 4.8,
            "price_level": "$$$",
        },
        {
            "name": "Heartattack and Vine",
            "venue_type": "bar",
            "distance_walk_min": 8,
            "student_review": "Wine bar with student-friendly prices. Cosy vibes and a great cheese board.",
            "student_reviewer": "Oscar W., 2nd yr Science",
            "rating": 4.4,
            "price_level": "$$",
        },
        {
            "name": "Pellegrini's Espresso Bar",
            "venue_type": "cafe",
            "distance_walk_min": 18,
            "student_review": "Old-school Italian espresso since 1954. A Melbourne rite of passage.",
            "student_reviewer": "Nina V., 1st yr Commerce",
            "rating": 4.5,
            "price_level": "$",
        },
        {
            "name": "The Rose",
            "venue_type": "bar",
            "distance_walk_min": 6,
            "student_review": "Fitzroy pub crawl starts here. Good pub meals and a massive beer garden.",
            "student_reviewer": "Jack H., 4th yr Law",
            "rating": 4.2,
            "price_level": "$$",
        },
        {
            "name": "Grub Food Van",
            "venue_type": "restaurant",
            "distance_walk_min": 10,
            "student_review": "Brunch in a laneway garden. The shakshuka is legendary among students.",
            "student_reviewer": "Tara L., 2nd yr Medicine",
            "rating": 4.4,
            "price_level": "$$",
        },
    ],
}


def main():
    """Write hardcoded venue data to data/enriched/places_nearby.json."""
    # Validate that we have data for every institution
    for inst in INSTITUTIONS:
        iid = inst["id"]
        if iid not in VENUES:
            print(f"WARNING: No venue data for {iid}")

    output_path = ENRICHED_DIR / "places_nearby.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(VENUES, f, indent=2)

    total_venues = sum(len(v) for v in VENUES.values())
    print(f"Saved {total_venues} venues across {len(VENUES)} institutions")
    print(f"  -> {output_path}")


if __name__ == "__main__":
    main()
