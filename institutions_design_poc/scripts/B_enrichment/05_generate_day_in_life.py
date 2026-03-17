"""
05_generate_day_in_life.py -- Use GPT-4o to generate day-in-the-life timelines.

Each institution gets 6-8 timeline entries spanning 7am-10pm, SPECIFIC to the
institution's location and vibe (e.g. "surf at The Strand" for JCU,
"laneway coffee" for Melbourne).

Run:
    python -m scripts.B_enrichment.05_generate_day_in_life
    python -m scripts.B_enrichment.05_generate_day_in_life --sample 2
"""

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import ENRICHED_DIR, get_openai_client, chat, parse_llm_json
from scripts.B_enrichment._base_institutions import INSTITUTIONS

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------
DAY_SYSTEM = """\
You are writing content for a university discovery app aimed at Australian
high-school students (years 10-12). Generate a "Day in the Life" timeline
for a student at the given university.

RULES:
1. Return EXACTLY 7 entries, spanning from 7:00 to 22:00.
2. Each entry has: time (HH:MM, 24h), activity (short title, 3-5 words),
   emoji (single emoji), description (15-25 words, vivid and specific),
   photo_prompt (a Midjourney-style prompt, 10-20 words, for generating
   a matching photo).
3. Be HYPER-SPECIFIC to this institution's location, campus, and city.
   Reference REAL places: specific cafes, beaches, parks, buildings,
   neighbourhoods, transit routes. No generic "go to class" entries.
4. Mix academic, social, food, and leisure activities.
5. Australian English spelling. Authentic student voice -- casual, not cringe.
6. The day should feel aspirational but realistic.

Respond ONLY with JSON:
{
  "entries": [
    {
      "time": "07:00",
      "activity": "...",
      "emoji": "...",
      "description": "...",
      "photo_prompt": "..."
    }
  ]
}
"""


def _build_user_prompt(inst: Dict) -> str:
    """Build the user message for one institution."""
    lines = [
        f"INSTITUTION: {inst['name']}",
        f"CITY: {inst['city']}, {inst['state']}",
        f"CAMPUS LOCATION: lat {inst['latitude']}, lng {inst['longitude']}",
        f"TOP COURSES: {', '.join(inst['top_courses'])}",
        f"KEY FEATURES: {'; '.join(inst['key_features'])}",
        "",
        "Generate a day-in-the-life timeline for a typical student here.",
    ]
    return "\n".join(lines)


def generate_day(inst: Dict, client) -> Dict:
    """Call the LLM to generate a day-in-the-life for one institution."""
    user_msg = _build_user_prompt(inst)
    raw = chat(client, DAY_SYSTEM, user_msg, temperature=0.8)
    try:
        parsed = parse_llm_json(raw)
        entries = parsed.get("entries", parsed.get("timeline", []))
        return {
            "institution_id": inst["id"],
            "name": inst["name"],
            "entries": entries,
        }
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        return {
            "institution_id": inst["id"],
            "name": inst["name"],
            "entries": [],
            "_error": f"{type(e).__name__}: {str(e)[:200]}",
            "_raw": raw[:500],
        }


def main():
    parser = argparse.ArgumentParser(description="Generate day-in-the-life timelines via LLM")
    parser.add_argument("--sample", type=int, default=0,
                        help="Only process N institutions (0 = all)")
    parser.add_argument("--workers", type=int, default=5,
                        help="Max concurrent LLM calls (default 5)")
    parser.add_argument("--output", type=str, default="",
                        help="Output file path (default: data/enriched/day_in_life.json)")
    args = parser.parse_args()

    institutions = INSTITUTIONS[:args.sample] if args.sample > 0 else INSTITUTIONS
    output_path = Path(args.output) if args.output else ENRICHED_DIR / "day_in_life.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Generating day-in-the-life for {len(institutions)} institutions with {args.workers} workers...\n")
    client = get_openai_client()

    results: Dict[str, Dict] = {}
    completed = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        future_to_inst = {
            pool.submit(generate_day, inst, client): inst
            for inst in institutions
        }
        for future in as_completed(future_to_inst):
            inst = future_to_inst[future]
            completed += 1
            try:
                result = future.result()
            except Exception as e:
                print(f"[{completed}/{len(institutions)}] {inst['name']} -- EXCEPTION: {e}")
                errors += 1
                continue

            if result.get("_error"):
                print(f"[{completed}/{len(institutions)}] {inst['name']} -- PARSE ERROR: {result['_error']}")
                errors += 1
            else:
                n = len(result["entries"])
                print(f"[{completed}/{len(institutions)}] {inst['name']} -- {n} entries")
                for entry in result["entries"]:
                    print(f"    {entry['time']}  {entry.get('emoji', '')}  {entry['activity']}")

            results[inst["id"]] = result

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Done. Generated: {completed - errors}, Errors: {errors}")
    print(f"Saved to {output_path}")


if __name__ == "__main__":
    main()
