"""
02_walk_score.py -- Hardcoded walkability / transit / bike scores for POC.

Structured so swapping in the real Walk Score API is a single function change:
replace `_fetch_scores_hardcoded()` with `_fetch_scores_api()`.

Run:
    python -m scripts.B_enrichment.02_walk_score
"""

import json
import sys
from pathlib import Path
from typing import Dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import ENRICHED_DIR
from scripts.B_enrichment._base_institutions import INSTITUTIONS

# ---------------------------------------------------------------------------
# Hardcoded scores  (replace this dict with an API call in production)
# ---------------------------------------------------------------------------
HARDCODED_SCORES: Dict[str, Dict[str, int]] = {
    # JCU Townsville -- car-dependent regional city
    "jcu-townsville": {
        "walk_score": 45,
        "transit_score": 25,
        "bike_score": 55,
    },
    # Griffith GC -- suburban coastal campus near Southport shops
    "griffith-gold-coast": {
        "walk_score": 58,
        "transit_score": 45,
        "bike_score": 60,
    },
    # UQ Brisbane -- inner suburb, good bike paths
    "uq-brisbane": {
        "walk_score": 72,
        "transit_score": 65,
        "bike_score": 75,
    },
    # UNSW Sydney -- urban, excellent transit
    "unsw-sydney": {
        "walk_score": 80,
        "transit_score": 85,
        "bike_score": 70,
    },
    # Melbourne Uni -- inner city, walker's paradise
    "unimelb": {
        "walk_score": 95,
        "transit_score": 92,
        "bike_score": 90,
    },
}


def _fetch_scores_hardcoded(institution_id: str, lat: float, lng: float) -> Dict[str, int]:
    """Return hardcoded scores. Drop-in replacement target for API version."""
    return HARDCODED_SCORES[institution_id]


# ---------------------------------------------------------------------------
# Production placeholder (uncomment and fill in API key to use)
# ---------------------------------------------------------------------------
# import httpx
#
# WALK_SCORE_API_KEY = os.getenv("WALK_SCORE_API_KEY", "")
#
# def _fetch_scores_api(institution_id: str, lat: float, lng: float) -> Dict[str, int]:
#     """Call the Walk Score API for real data."""
#     url = "https://api.walkscore.com/score"
#     params = {
#         "format": "json",
#         "lat": lat,
#         "lon": lng,
#         "transit": 1,
#         "bike": 1,
#         "wsapikey": WALK_SCORE_API_KEY,
#     }
#     resp = httpx.get(url, params=params, timeout=15)
#     resp.raise_for_status()
#     data = resp.json()
#     return {
#         "walk_score": data.get("walkscore", 0),
#         "transit_score": data.get("transit", {}).get("score", 0),
#         "bike_score": data.get("bike", {}).get("score", 0),
#     }


def main():
    """Build walk-score data for all institutions and save to JSON."""
    results: Dict[str, Dict] = {}

    for inst in INSTITUTIONS:
        iid = inst["id"]
        scores = _fetch_scores_hardcoded(iid, inst["latitude"], inst["longitude"])
        results[iid] = {
            "institution_id": iid,
            "name": inst["name"],
            "latitude": inst["latitude"],
            "longitude": inst["longitude"],
            **scores,
        }
        label = _walk_label(scores["walk_score"])
        print(f"  {inst['name']:40s}  walk={scores['walk_score']:2d}  transit={scores['transit_score']:2d}  bike={scores['bike_score']:2d}  ({label})")

    output_path = ENRICHED_DIR / "walk_scores.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nSaved walk scores for {len(results)} institutions")
    print(f"  -> {output_path}")


def _walk_label(score: int) -> str:
    """Human-readable label for a walk score."""
    if score >= 90:
        return "Walker's Paradise"
    if score >= 70:
        return "Very Walkable"
    if score >= 50:
        return "Somewhat Walkable"
    if score >= 25:
        return "Car-Dependent"
    return "Almost All Errands Require a Car"


if __name__ == "__main__":
    main()
