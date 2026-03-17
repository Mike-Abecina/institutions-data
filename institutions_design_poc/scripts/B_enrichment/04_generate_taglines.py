"""
04_generate_taglines.py -- Use GPT-4o to generate poetic taglines for each institution.

Follows the EXACT pattern of the parent project's regenerate_descriptions.py:
  - argparse with --sample, --workers, --output
  - ThreadPoolExecutor for parallel LLM calls
  - parse_llm_json() for robust response parsing

Run:
    python -m scripts.B_enrichment.04_generate_taglines
    python -m scripts.B_enrichment.04_generate_taglines --sample 2
    python -m scripts.B_enrichment.04_generate_taglines --workers 4
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
TAGLINE_SYSTEM = """\
You are a copywriter for a university discovery app aimed at Australian high-school
students (years 10-12). Your job is to write a single punchy, poetic TAGLINE for
a university.

RULES:
1. MAX 8 WORDS. Shorter is better.
2. Be punchy and poetic -- no corporate-speak, no cliches like "world-class" or
   "leading the way". Write something a student would screenshot and share.
3. Be SPECIFIC to the institution's location, vibe, or unique character.
   Generic lines that could apply to any uni are failures.
4. Australian English spelling. No Americanisms.
5. You may be playful, cheeky, or evocative. Think festival poster, not brochure.

EXAMPLES OF GREAT TAGLINES:
- "Where the reef is your classroom" (JCU Townsville)
- "The city never sleeps. Neither will you." (UTS Sydney)
- "Flip-flops to the lecture hall" (Griffith Gold Coast)
- "Sandstone & startups" (University of Queensland)
- "Laneways, libraries, and late-night labs" (Melbourne)

Respond ONLY with JSON:
{
  "tagline": "<your tagline, max 8 words>",
  "reasoning": "<one sentence explaining why this works>"
}
"""


def _build_user_prompt(inst: Dict) -> str:
    """Build the user message for one institution."""
    lines = [
        f"INSTITUTION: {inst['name']}",
        f"CITY: {inst['city']}, {inst['state']}",
        f"LOCATION: {inst['latitude']}, {inst['longitude']}",
        f"TYPE: {inst['institution_type']}",
        f"STUDENTS: {inst.get('student_count', 'unknown')}",
        f"TOP COURSES: {', '.join(inst['top_courses'])}",
        f"KEY FEATURES: {'; '.join(inst['key_features'])}",
        "",
        "Write one tagline for this institution.",
    ]
    return "\n".join(lines)


def generate_tagline(inst: Dict, client) -> Dict:
    """Call the LLM to generate a tagline for one institution."""
    user_msg = _build_user_prompt(inst)
    raw = chat(client, TAGLINE_SYSTEM, user_msg, temperature=0.9)
    try:
        parsed = parse_llm_json(raw)
        return {
            "institution_id": inst["id"],
            "name": inst["name"],
            "tagline": parsed["tagline"],
            "reasoning": parsed.get("reasoning", ""),
        }
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        return {
            "institution_id": inst["id"],
            "name": inst["name"],
            "tagline": "",
            "reasoning": "",
            "_error": f"{type(e).__name__}: {str(e)[:200]}",
            "_raw": raw[:300],
        }


def main():
    parser = argparse.ArgumentParser(description="Generate poetic taglines via LLM")
    parser.add_argument("--sample", type=int, default=0,
                        help="Only process N institutions (0 = all)")
    parser.add_argument("--workers", type=int, default=5,
                        help="Max concurrent LLM calls (default 5)")
    parser.add_argument("--output", type=str, default="",
                        help="Output file path (default: data/enriched/taglines.json)")
    args = parser.parse_args()

    institutions = INSTITUTIONS[:args.sample] if args.sample > 0 else INSTITUTIONS
    output_path = Path(args.output) if args.output else ENRICHED_DIR / "taglines.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Generating taglines for {len(institutions)} institutions with {args.workers} workers...\n")
    client = get_openai_client()

    results: Dict[str, Dict] = {}
    completed = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        future_to_inst = {
            pool.submit(generate_tagline, inst, client): inst
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
                print(f"[{completed}/{len(institutions)}] {inst['name']}")
                print(f"    \"{result['tagline']}\"")
                print(f"    ({result['reasoning']})")

            results[inst["id"]] = result

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Done. Generated: {completed - errors}, Errors: {errors}")
    print(f"Saved to {output_path}")


if __name__ == "__main__":
    main()
