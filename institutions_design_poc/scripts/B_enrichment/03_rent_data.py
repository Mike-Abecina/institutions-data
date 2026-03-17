"""
03_rent_data.py -- Hardcoded rent ranges, weekly budgets, and tuition data for POC.

Produces TWO output files:
  - data/enriched/rent_data.json   (rent ranges by institution)
  - data/enriched/cost_data.json   (full cost breakdown: rent + budget + tuition)

Run:
    python -m scripts.B_enrichment.03_rent_data
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
# Hardcoded rent ranges (AUD per week)
# ---------------------------------------------------------------------------
RENT_DATA: Dict[str, Dict] = {
    "jcu-townsville": {
        "one_bedroom_low": 200,
        "one_bedroom_high": 280,
        "share_house_low": 140,
        "share_house_high": 180,
        "suburb": "Douglas / Townsville City",
        "rent_comparison": "Cheapest of the five -- regional advantage",
    },
    "griffith-gold-coast": {
        "one_bedroom_low": 320,
        "one_bedroom_high": 420,
        "share_house_low": 200,
        "share_house_high": 260,
        "suburb": "Southport / Robina",
        "rent_comparison": "Mid-range -- cheaper inland, pricier beachside",
    },
    "uq-brisbane": {
        "one_bedroom_low": 350,
        "one_bedroom_high": 450,
        "share_house_low": 220,
        "share_house_high": 280,
        "suburb": "St Lucia / Toowong / Indooroopilly",
        "rent_comparison": "Above average -- inner Brisbane suburb",
    },
    "unsw-sydney": {
        "one_bedroom_low": 450,
        "one_bedroom_high": 600,
        "share_house_low": 280,
        "share_house_high": 350,
        "suburb": "Kensington / Kingsford / Randwick",
        "rent_comparison": "Most expensive -- Sydney eastern suburbs",
    },
    "unimelb": {
        "one_bedroom_low": 400,
        "one_bedroom_high": 550,
        "share_house_low": 250,
        "share_house_high": 320,
        "suburb": "Parkville / Carlton / North Melbourne",
        "rent_comparison": "High -- inner Melbourne, offset by no car needed",
    },
}

# ---------------------------------------------------------------------------
# Weekly student budget breakdown (AUD)
# ---------------------------------------------------------------------------
WEEKLY_BUDGETS: Dict[str, Dict[str, int]] = {
    "jcu-townsville": {
        "rent": 160,
        "food": 70,
        "transport": 30,
        "fun": 40,
        "total": 300,
    },
    "griffith-gold-coast": {
        "rent": 230,
        "food": 80,
        "transport": 35,
        "fun": 50,
        "total": 395,
    },
    "uq-brisbane": {
        "rent": 250,
        "food": 85,
        "transport": 30,
        "fun": 55,
        "total": 420,
    },
    "unsw-sydney": {
        "rent": 310,
        "food": 90,
        "transport": 40,
        "fun": 60,
        "total": 500,
    },
    "unimelb": {
        "rent": 280,
        "food": 85,
        "transport": 25,
        "fun": 55,
        "total": 445,
    },
}

# ---------------------------------------------------------------------------
# Tuition ranges (CSP domestic, AUD per year)
# ---------------------------------------------------------------------------
TUITION_RANGES: Dict[str, Dict[str, int]] = {
    "jcu-townsville": {
        "tuition_low": 7_200,
        "tuition_high": 11_800,
    },
    "griffith-gold-coast": {
        "tuition_low": 7_500,
        "tuition_high": 12_000,
    },
    "uq-brisbane": {
        "tuition_low": 8_000,
        "tuition_high": 14_000,
    },
    "unsw-sydney": {
        "tuition_low": 8_500,
        "tuition_high": 15_000,
    },
    "unimelb": {
        "tuition_low": 8_800,
        "tuition_high": 15_500,
    },
}


TUITION_COMPARISONS: Dict[str, str] = {
    "jcu-townsville": "Lowest of the five -- regional pricing advantage",
    "griffith-gold-coast": "Below average -- competitive Gold Coast rates",
    "uq-brisbane": "Around average for Australian unis",
    "unsw-sydney": "Above average -- premium Go8 research uni",
    "unimelb": "Highest -- premium Go8, offset by Melbourne Model breadth",
}


def _tuition_comparison(inst_id: str) -> str:
    """Institution-specific comparison string for the UI."""
    return TUITION_COMPARISONS[inst_id]


def main():
    """Build rent and cost data files."""
    # ---- rent_data.json ----
    rent_output: Dict[str, Dict] = {}
    for inst in INSTITUTIONS:
        iid = inst["id"]
        rent_output[iid] = {
            "institution_id": iid,
            "name": inst["name"],
            **RENT_DATA[iid],
        }

    rent_path = ENRICHED_DIR / "rent_data.json"
    rent_path.parent.mkdir(parents=True, exist_ok=True)
    with open(rent_path, "w") as f:
        json.dump(rent_output, f, indent=2)
    print(f"Saved rent data -> {rent_path}")

    # ---- cost_data.json ----
    cost_output: Dict[str, Dict] = {}
    for inst in INSTITUTIONS:
        iid = inst["id"]
        rent = RENT_DATA[iid]
        budget = WEEKLY_BUDGETS[iid]
        tuition = TUITION_RANGES[iid]
        cost_output[iid] = {
            "institution_id": iid,
            "name": inst["name"],
            "tuition_range_low": tuition["tuition_low"],
            "tuition_range_high": tuition["tuition_high"],
            "tuition_comparison": _tuition_comparison(iid),
            "rent_range_low": rent["share_house_low"],
            "rent_range_high": rent["one_bedroom_high"],
            "rent_comparison": rent["rent_comparison"],
            "weekly_budget": budget,
            "total_weekly": budget["total"],
        }
        print(f"  {inst['name']:40s}  rent ${rent['share_house_low']}-${rent['one_bedroom_high']}/wk  "
              f"tuition ${tuition['tuition_low']:,}-${tuition['tuition_high']:,}/yr  "
              f"budget ${budget['total']}/wk")

    cost_path = ENRICHED_DIR / "cost_data.json"
    with open(cost_path, "w") as f:
        json.dump(cost_output, f, indent=2)
    print(f"\nSaved cost data -> {cost_path}")


if __name__ == "__main__":
    main()
