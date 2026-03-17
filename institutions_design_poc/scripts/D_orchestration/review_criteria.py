"""
Review criteria and quality rubrics for PM agent review gates.
Each workstream has specific criteria the PM agent evaluates against.
"""
from typing import List, Dict


# ---------------------------------------------------------------------------
# Workstream A: Audit Review Criteria
# ---------------------------------------------------------------------------
AUDIT_CRITERIA: Dict[str, str] = {
    "schema_completeness": (
        "Did the audit discover ALL tables and columns in the database? "
        "Were DESCRIBE, SHOW FULL COLUMNS, and SHOW CREATE TABLE all run?"
    ),
    "sample_quality": (
        "Are the data samples representative? Were NULLs, edge cases, and "
        "data distributions properly captured for every column?"
    ),
    "api_coverage": (
        "Were both COURSES_API and PROVIDERS_API probed with at least 5 "
        "different query variations each? Was the response schema fully documented?"
    ),
    "gap_analysis_accuracy": (
        "Does every design doc field have a clear status (AVAILABLE_DB, "
        "AVAILABLE_API, NEEDS_ENRICHMENT, NEEDS_GENERATION, NOT_FEASIBLE)? "
        "Are the source mappings correct?"
    ),
    "actionability": (
        "Can a developer use this report to immediately start the enrichment "
        "pipeline? Are next steps clear for each gap?"
    ),
}

# ---------------------------------------------------------------------------
# Workstream B: Content Review Criteria (LLM-generated content)
# ---------------------------------------------------------------------------
CONTENT_CRITERIA: Dict[str, str] = {
    "tagline_quality": (
        "Are taglines punchy, under 8 words, and specific to each institution? "
        "Do they capture the FEELING of being there?"
    ),
    "tagline_no_cliches": (
        "No 'discover your potential', 'unlock your future', 'where dreams "
        "come true', or similar corporate-speak? No exclamation marks?"
    ),
    "vibe_tags_authenticity": (
        "Do vibe tags feel like what a real Gen Z student would actually say? "
        "Are they specific to the institution, not generic?"
    ),
    "day_in_life_specificity": (
        "Are activities specific to the institution's location? E.g., 'surf at "
        "The Strand' for JCU, 'laneway coffee' for Melbourne Uni. NOT generic "
        "'go to class' entries."
    ),
    "day_in_life_feasibility": (
        "Are the described activities actually possible at this institution? "
        "No mentioning beaches for landlocked campuses, etc."
    ),
    "tone_consistency": (
        "Is the tone consistently Gen Z without being try-hard or cringe? "
        "Authentic, not 'fellow kids' energy."
    ),
    "australian_context": (
        "Australian spelling (colour, organisation, programme)? AUD currency? "
        "Correct geography (states, cities, suburbs)? No US references?"
    ),
    "no_misrepresentation": (
        "Does any content make claims that could mislead a student? "
        "Poetic license is fine but outright false claims are not."
    ),
}

# ---------------------------------------------------------------------------
# Workstream B: Data Review Criteria (hardcoded/API data)
# ---------------------------------------------------------------------------
DATA_CRITERIA: Dict[str, str] = {
    "completeness": (
        "Are ALL 5 institutions enriched with places, walk scores, and rent data? "
        "No missing institutions?"
    ),
    "venue_accuracy": (
        "Are the hardcoded venue names real businesses that actually exist near "
        "each campus? Are distances plausible?"
    ),
    "walk_score_plausibility": (
        "Do walk/transit/bike scores match the known character of each location? "
        "E.g., inner-city Melbourne should score much higher than regional Townsville."
    ),
    "cost_accuracy": (
        "Are tuition and rent ranges within plausible bounds for 2025-2026 "
        "Australian rates? Are comparisons between institutions logically correct?"
    ),
}

# ---------------------------------------------------------------------------
# Workstream B: Merge Review Criteria
# ---------------------------------------------------------------------------
MERGE_CRITERIA: Dict[str, str] = {
    "all_fields_populated": (
        "Does every institution have ALL required fields? Check: tagline, "
        "vibe_tags, campus_mood, day_in_life, nearby_venues, cost, scholarships."
    ),
    "pydantic_validation": (
        "Does the merged JSON validate against the InstitutionCard Pydantic model "
        "without errors?"
    ),
    "fixture_file_valid": (
        "Does data/fixtures/institutions_sample.json exist and contain valid data "
        "for all 5 institutions?"
    ),
    "quiz_matching_fields": (
        "Does every institution have vibe_location, vibe_energy, and vibe_weekend "
        "fields for quiz matching?"
    ),
}

# ---------------------------------------------------------------------------
# Workstream C: UI Review Criteria
# ---------------------------------------------------------------------------
UI_CRITERIA: Dict[str, str] = {
    "dark_mode": (
        "Is the UI consistently dark-mode with #0D0D0D background? "
        "No white flashes or unstyled default Streamlit elements?"
    ),
    "palette_correct": (
        "Are coral (#FF5733), mint (#00E5A0), purple (#7C5CFC), golden (#FFD166) "
        "used correctly for accents, buttons, and highlights?"
    ),
    "quiz_flow": (
        "Does the vibe quiz have 3 questions with visual 2x2 option grids? "
        "Does it show progress? Does it transition to the feed on completion?"
    ),
    "card_design": (
        "Do institution cards show: gradient hero, name, location, tagline, "
        "tags as pills, and 4 tabs?"
    ),
    "tab_content": (
        "Do all 4 tabs (VIBE/EATS/LIFE/COST) render with actual data? "
        "No empty tabs or placeholder-only content?"
    ),
    "compare_mode": (
        "Can you save 2+ institutions and compare them side-by-side? "
        "Does the comparison show meaningful differences?"
    ),
    "no_errors": (
        "Does the app run without Python errors or Streamlit warnings? "
        "Can you navigate through all pages without crashes?"
    ),
}


def get_criteria_for_gate(gate: str) -> Dict[str, str]:
    """Return the review criteria for a given review gate."""
    gate_map = {
        "audit": AUDIT_CRITERIA,
        "content": CONTENT_CRITERIA,
        "data": DATA_CRITERIA,
        "merge": MERGE_CRITERIA,
        "ui": UI_CRITERIA,
    }
    if gate not in gate_map:
        raise ValueError(f"Unknown gate '{gate}'. Valid: {list(gate_map.keys())}")
    return gate_map[gate]


def format_criteria_prompt(gate: str) -> str:
    """Format criteria as a numbered list for inclusion in LLM prompts."""
    criteria = get_criteria_for_gate(gate)
    lines = []
    for i, (key, desc) in enumerate(criteria.items(), 1):
        lines.append(f"{i}. **{key}**: {desc}")
    return "\n".join(lines)
