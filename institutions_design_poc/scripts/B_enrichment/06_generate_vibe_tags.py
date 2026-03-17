"""
06_generate_vibe_tags.py -- Use GPT-4o to generate vibe tags, mood scores,
and student quotes for each institution.

Output per institution:
  - 5 vibe tags (1-2 words, lowercase, hyphenated)
  - campus_mood scores: study %, social %, chill % (0-100)
  - A student quote attributed to a fake but realistic student

Run:
    python -m scripts.B_enrichment.06_generate_vibe_tags
    python -m scripts.B_enrichment.06_generate_vibe_tags --sample 2
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
VIBE_SYSTEM = """\
You are writing content for a university discovery app aimed at Australian
high-school students (years 10-12). Generate vibe data for the given university.

You must produce THREE things:

1. VIBE TAGS -- exactly 5 tags that capture the feel of this uni.
   - 1-2 words each, all lowercase, hyphenated if multi-word
   - Be specific and evocative, not generic
   - Examples: "laid-back", "beach-life", "research-heavy", "laneway-culture",
     "tight-knit", "surf-and-study", "sandstone-vibes", "late-night-labs",
     "reef-adjacent", "startup-energy", "heritage-quad", "foodie-paradise"

2. CAMPUS MOOD -- three scores from 0 to 100:
   - study: How academic/studious is the atmosphere?
   - social: How active is the social scene?
   - chill: How relaxed/laid-back is the vibe?
   These don't need to add up to 100. They're independent axes.

3. STUDENT QUOTE -- a 1-2 sentence quote from a fictional but realistic student.
   Include their first name, year (e.g. "3rd year"), and program.
   The quote should feel authentic -- something you'd read on Reddit or a
   group chat, not a marketing brochure. Capture what makes THIS uni unique.

Respond ONLY with JSON:
{
  "vibe_tags": ["tag-one", "tag-two", "tag-three", "tag-four", "tag-five"],
  "campus_mood": {
    "study": 65,
    "social": 72,
    "chill": 80
  },
  "student_quote": "The quote text here.",
  "student_quote_author": "First Name",
  "student_quote_year": "3rd year Program Name"
}
"""


def _build_user_prompt(inst: Dict) -> str:
    """Build the user message for one institution."""
    lines = [
        f"INSTITUTION: {inst['name']}",
        f"CITY: {inst['city']}, {inst['state']}",
        f"LOCATION: lat {inst['latitude']}, lng {inst['longitude']}",
        f"STUDENTS: {inst.get('student_count', 'unknown')}",
        f"TOP COURSES: {', '.join(inst['top_courses'])}",
        f"KEY FEATURES: {'; '.join(inst['key_features'])}",
        "",
        "Generate vibe tags, mood scores, and a student quote for this institution.",
    ]
    return "\n".join(lines)


def generate_vibe(inst: Dict, client) -> Dict:
    """Call the LLM to generate vibe data for one institution."""
    user_msg = _build_user_prompt(inst)
    raw = chat(client, VIBE_SYSTEM, user_msg, temperature=0.8)
    try:
        parsed = parse_llm_json(raw)
        return {
            "institution_id": inst["id"],
            "name": inst["name"],
            "vibe_tags": parsed["vibe_tags"],
            "campus_mood": parsed["campus_mood"],
            "student_quote": parsed["student_quote"],
            "student_quote_author": parsed["student_quote_author"],
            "student_quote_year": parsed.get("student_quote_year", ""),
        }
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        return {
            "institution_id": inst["id"],
            "name": inst["name"],
            "vibe_tags": [],
            "campus_mood": {},
            "student_quote": "",
            "student_quote_author": "",
            "student_quote_year": "",
            "_error": f"{type(e).__name__}: {str(e)[:200]}",
            "_raw": raw[:500],
        }


def main():
    parser = argparse.ArgumentParser(description="Generate vibe tags & mood scores via LLM")
    parser.add_argument("--sample", type=int, default=0,
                        help="Only process N institutions (0 = all)")
    parser.add_argument("--workers", type=int, default=5,
                        help="Max concurrent LLM calls (default 5)")
    parser.add_argument("--output", type=str, default="",
                        help="Output file path (default: data/enriched/vibe_tags.json)")
    args = parser.parse_args()

    institutions = INSTITUTIONS[:args.sample] if args.sample > 0 else INSTITUTIONS
    output_path = Path(args.output) if args.output else ENRICHED_DIR / "vibe_tags.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Generating vibe tags for {len(institutions)} institutions with {args.workers} workers...\n")
    client = get_openai_client()

    results: Dict[str, Dict] = {}
    completed = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        future_to_inst = {
            pool.submit(generate_vibe, inst, client): inst
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
                tags_str = ", ".join(result["vibe_tags"])
                mood = result["campus_mood"]
                print(f"[{completed}/{len(institutions)}] {inst['name']}")
                print(f"    Tags: {tags_str}")
                print(f"    Mood: study={mood.get('study')}, social={mood.get('social')}, chill={mood.get('chill')}")
                print(f"    Quote: \"{result['student_quote']}\"")
                print(f"           -- {result['student_quote_author']}, {result['student_quote_year']}")

            results[inst["id"]] = result

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Done. Generated: {completed - errors}, Errors: {errors}")
    print(f"Saved to {output_path}")


if __name__ == "__main__":
    main()
