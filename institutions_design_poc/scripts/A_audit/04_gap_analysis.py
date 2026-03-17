"""
04_gap_analysis.py
==================
Map every field required by the Institution Search design document to the
data sources discovered by scripts 01-03 (DB schema + API schemas).

For each design-doc field the script assigns a status:
  AVAILABLE_DB            -- directly mappable to a DB column
  AVAILABLE_API           -- directly mappable to an API response field
  NEEDS_ENRICHMENT        -- data exists but needs transformation / joining
  NEEDS_GENERATION        -- must be generated via LLM, external API, or UGC
  NOT_FEASIBLE_FOR_POC    -- out-of-scope for the proof-of-concept

Optionally, GPT-4o is used to suggest fuzzy mappings for fields that could
not be matched heuristically.

Outputs
-------
data/reports/gap_analysis.xlsx   -- one row per design-doc field
"""
from __future__ import annotations

import argparse
import json
import sys
import textwrap
import time
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Project imports
# ---------------------------------------------------------------------------
try:
    from config.settings import (
        get_db_connection,
        chat,
        DATA_REPORTS,
    )
    _HAS_CHAT = True
except ImportError:
    try:
        from config.settings import get_db_connection, DATA_REPORTS
        _HAS_CHAT = False
    except ImportError:
        print(
            "[ERROR] Could not import config.settings.\n"
            "  Run from the project root:  cd institutions_design_poc && "
            "python -m scripts.A_audit.04_gap_analysis"
        )
        sys.exit(1)

# Try importing get_openai_client + chat for LLM-assisted mapping
try:
    from config.settings import get_openai_client
except ImportError:
    get_openai_client = None


# ---------------------------------------------------------------------------
# Design-document field definitions
# ---------------------------------------------------------------------------
# Organised by the UI tab / section from INSTITUTION_SEARCH_DESIGN.md.
# Each entry is (field_name, description, tab/section).

DESIGN_DOC_FIELDS: list[dict[str, str]] = [
    # -- Card (the institution card in the feed) --
    {"field": "institution_name",         "tab": "card",    "description": "Official institution name"},
    {"field": "campus_location_city",     "tab": "card",    "description": "City where the campus is located"},
    {"field": "campus_location_state",    "tab": "card",    "description": "State / territory abbreviation"},
    {"field": "hero_image_url",           "tab": "card",    "description": "Full-bleed hero photo URL"},
    {"field": "tagline",                  "tab": "card",    "description": "Poetic one-liner capturing the vibe"},
    {"field": "top_fields_of_study",      "tab": "card",    "description": "Signature disciplines / tags"},
    {"field": "ranking_badge",            "tab": "card",    "description": "Notable ranking (e.g. #3 in Aus for Environment)"},
    {"field": "institution_type",         "tab": "card",    "description": "University / TAFE / private college"},
    {"field": "cricos_code",              "tab": "card",    "description": "CRICOS provider code"},

    # -- Vibe tab --
    {"field": "student_quote",            "tab": "vibe",    "description": "Authentic student testimonial"},
    {"field": "student_quote_author",     "tab": "vibe",    "description": "Quote attribution (name, year, course)"},
    {"field": "vibe_tags",                "tab": "vibe",    "description": "Crowdsourced 3-word vibe descriptors"},
    {"field": "campus_mood_study_pct",    "tab": "vibe",    "description": "% students in study mode (mood bar)"},
    {"field": "campus_mood_social_pct",   "tab": "vibe",    "description": "% students in social mode"},
    {"field": "campus_mood_chill_pct",    "tab": "vibe",    "description": "% students in chill mode"},
    {"field": "student_video_url",        "tab": "vibe",    "description": "Embedded day-in-the-life video"},
    {"field": "social_proof_count",       "tab": "vibe",    "description": "Number of students currently browsing"},

    # -- Eats tab --
    {"field": "nearby_cafes",             "tab": "eats",    "description": "List of nearby cafes with student reviews"},
    {"field": "nearby_restaurants",       "tab": "eats",    "description": "List of nearby restaurants"},
    {"field": "nearby_bars",              "tab": "eats",    "description": "List of nearby bars / nightlife"},
    {"field": "walk_score",               "tab": "eats",    "description": "Walk Score (0-100)"},
    {"field": "cafes_within_10min",       "tab": "eats",    "description": "Count of cafes within 10-minute walk"},
    {"field": "bars_within_10min",        "tab": "eats",    "description": "Count of bars within 10-minute walk"},
    {"field": "beaches_within_15min",     "tab": "eats",    "description": "Count of beaches within 15-minute drive"},
    {"field": "food_map_data",            "tab": "eats",    "description": "Interactive map pin data (lat/lon, type, rating)"},

    # -- Life tab --
    {"field": "day_in_the_life_timeline", "tab": "life",    "description": "Structured timeline (time, photo, caption)"},
    {"field": "clubs_and_societies",      "tab": "life",    "description": "List of notable student clubs"},
    {"field": "transport_info",           "tab": "life",    "description": "Bus/train times, airport distance"},
    {"field": "safety_support_info",      "tab": "life",    "description": "Security, mental health sessions, peer mentoring"},
    {"field": "student_count",            "tab": "life",    "description": "Total enrolled students (approx.)"},
    {"field": "campus_latitude",          "tab": "life",    "description": "Campus latitude for maps"},
    {"field": "campus_longitude",         "tab": "life",    "description": "Campus longitude for maps"},

    # -- Cost tab --
    {"field": "tuition_csp_min",          "tab": "cost",    "description": "Minimum CSP domestic tuition ($/year)"},
    {"field": "tuition_csp_max",          "tab": "cost",    "description": "Maximum CSP domestic tuition ($/year)"},
    {"field": "tuition_comparison_label", "tab": "cost",    "description": "Relative cost label (e.g. 'Cheaper than avg')"},
    {"field": "rent_min_weekly",          "tab": "cost",    "description": "Minimum weekly rent near campus"},
    {"field": "rent_max_weekly",          "tab": "cost",    "description": "Maximum weekly rent near campus"},
    {"field": "weekly_budget_breakdown",  "tab": "cost",    "description": "Typical weekly spend breakdown (rent, food, etc.)"},
    {"field": "scholarships",             "tab": "cost",    "description": "List of matching scholarships"},
    {"field": "student_cost_quote",       "tab": "cost",    "description": "Student testimonial about cost of living"},

    # -- Compare view --
    {"field": "compare_vibe_label",       "tab": "compare", "description": "Short vibe label for comparison table"},
    {"field": "compare_rent_weekly",      "tab": "compare", "description": "Typical rent for comparison"},
    {"field": "compare_walk_score",       "tab": "compare", "description": "Walk Score for comparison"},
    {"field": "compare_cafes_10min",      "tab": "compare", "description": "Cafe count for comparison"},
    {"field": "compare_beach_mins",       "tab": "compare", "description": "Minutes to nearest beach"},
    {"field": "compare_top_for",          "tab": "compare", "description": "What the institution is best known for"},
    {"field": "compare_mood_label",       "tab": "compare", "description": "Dominant campus mood (e.g. Chill 83%)"},
    {"field": "compare_student_count",    "tab": "compare", "description": "Student population for comparison"},
]


# ---------------------------------------------------------------------------
# Source loaders
# ---------------------------------------------------------------------------

def load_db_columns(schema_xlsx: Path) -> set[str]:
    """Extract all column names from the schema_audit.xlsx workbook.

    Falls back to querying the database directly if the file is missing.
    """
    columns: set[str] = set()

    if schema_xlsx.exists():
        print(f"  Loading DB schema from {schema_xlsx}")
        xls = pd.ExcelFile(schema_xlsx, engine="openpyxl")
        for sheet in xls.sheet_names:
            if sheet.endswith("_schema"):
                df = pd.read_excel(xls, sheet_name=sheet)
                # SHOW FULL COLUMNS output has a 'Field' column
                if "Field" in df.columns:
                    columns.update(df["Field"].dropna().str.strip().tolist())
        print(f"  Found {len(columns)} unique DB columns from schema report")
    else:
        print(f"  [WARNING] {schema_xlsx} not found, querying DB directly ...")
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            for t in tables:
                cursor.execute(f"DESCRIBE `{t}`")
                for row in cursor.fetchall():
                    columns.add(row[0].strip())
            conn.close()
            print(f"  Found {len(columns)} unique DB columns via live query")
        except Exception as exc:
            print(f"  [ERROR] Could not query DB: {exc}")

    return columns


def load_api_fields(api_probe_json: Path) -> tuple[set[str], set[str]]:
    """Extract field paths from api_probe.json.

    Returns (courses_fields, providers_fields).
    """
    courses: set[str] = set()
    providers: set[str] = set()

    if not api_probe_json.exists():
        print(f"  [WARNING] {api_probe_json} not found -- API fields will be empty.")
        return courses, providers

    print(f"  Loading API schemas from {api_probe_json}")
    blob = json.loads(api_probe_json.read_text())

    for path in blob.get("courses_api", {}).get("schema", {}):
        courses.add(path)
    for path in blob.get("providers_api", {}).get("schema", {}):
        providers.add(path)

    print(f"  Found {len(courses)} Courses API fields, {len(providers)} Providers API fields")
    return courses, providers


# ---------------------------------------------------------------------------
# Heuristic matching
# ---------------------------------------------------------------------------

# Manual mapping overrides: design_field -> (source, source_field)
# Use None for source_field when the design field matches a clear DB column
# or API path.
MANUAL_OVERRIDES: dict[str, tuple[str, str | None]] = {
    # Examples -- these will be applied before heuristic matching
    # "institution_name": ("db", "trading_name"),
    # "cricos_code":      ("db", "cricos_provider_code"),
}


def _normalise(name: str) -> str:
    """Lowercase, strip underscores/dashes/dots for fuzzy comparison."""
    return name.lower().replace("_", "").replace("-", "").replace(".", "")


def heuristic_match(
    design_field: str,
    db_columns: set[str],
    courses_fields: set[str],
    providers_fields: set[str],
) -> tuple[str | None, str | None]:
    """Try to match *design_field* to a source.

    Returns (status, matched_source_field) or (None, None) if no match.
    """
    # 1. Check manual overrides first
    if design_field in MANUAL_OVERRIDES:
        source, src_field = MANUAL_OVERRIDES[design_field]
        if source == "db":
            return "AVAILABLE_DB", src_field or design_field
        return "AVAILABLE_API", src_field or design_field

    norm_design = _normalise(design_field)

    # 2. Exact match in DB columns
    for col in db_columns:
        if _normalise(col) == norm_design:
            return "AVAILABLE_DB", col

    # 3. Substring / partial match in DB columns
    for col in db_columns:
        nc = _normalise(col)
        if norm_design in nc or nc in norm_design:
            return "AVAILABLE_DB", col

    # 4. Exact or partial match in API fields (providers first, then courses)
    for field_set, label in [(providers_fields, "AVAILABLE_API"), (courses_fields, "AVAILABLE_API")]:
        for fp in field_set:
            # Match on the leaf segment of the dotted path
            leaf = fp.rsplit(".", 1)[-1] if "." in fp else fp
            if _normalise(leaf) == norm_design:
                return label, fp
            if norm_design in _normalise(leaf) or _normalise(leaf) in norm_design:
                return label, fp

    return None, None


# ---------------------------------------------------------------------------
# LLM-assisted mapping (optional)
# ---------------------------------------------------------------------------

def llm_suggest_mappings(
    unmapped_fields: list[dict],
    db_columns: list[str],
    api_fields: list[str],
) -> dict[str, dict]:
    """Ask GPT-4o to suggest mappings for unmapped fields.

    Returns a dict keyed by design field name with keys:
      suggested_source, suggested_field, confidence, reasoning
    """
    if not _HAS_CHAT or get_openai_client is None:
        print("  [SKIP] LLM mapping unavailable (chat / get_openai_client not imported)")
        return {}

    print("  Asking GPT-4o to suggest mappings for unmapped fields ...")

    try:
        client = get_openai_client()
    except Exception as exc:
        print(f"  [WARNING] Could not initialise OpenAI client: {exc}")
        return {}

    system_prompt = (
        "You are an expert data engineer. Given a list of design-document fields "
        "that could not be automatically matched to database columns or API fields, "
        "suggest the best mapping or explain why no mapping is possible.\n\n"
        "Respond with a JSON array. Each element must have:\n"
        '  "field": the design field name,\n'
        '  "suggested_source": one of "db", "api_courses", "api_providers", "external_api", "llm_generation", "ugc", "not_feasible",\n'
        '  "suggested_field": the closest source field name (or null),\n'
        '  "confidence": "high" | "medium" | "low",\n'
        '  "reasoning": a brief explanation\n'
    )

    user_prompt = (
        f"## Unmapped design fields\n"
        f"{json.dumps([f['field'] + ' -- ' + f['description'] for f in unmapped_fields], indent=2)}\n\n"
        f"## Available DB columns\n"
        f"{json.dumps(sorted(db_columns)[:200], indent=2)}\n\n"
        f"## Available API fields (dot paths)\n"
        f"{json.dumps(sorted(api_fields)[:200], indent=2)}\n"
    )

    try:
        raw = chat(client, system_prompt, user_prompt, temperature=0.1)
    except Exception as exc:
        print(f"  [WARNING] LLM call failed: {exc}")
        return {}

    # Parse LLM response
    try:
        # Strip markdown fences if present
        text = raw.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3].strip()
        suggestions = json.loads(text)
    except (json.JSONDecodeError, ValueError) as exc:
        print(f"  [WARNING] Could not parse LLM response: {exc}")
        return {}

    result = {}
    if isinstance(suggestions, list):
        for s in suggestions:
            if isinstance(s, dict) and "field" in s:
                result[s["field"]] = s
        print(f"  LLM suggested mappings for {len(result)} fields")
    return result


# ---------------------------------------------------------------------------
# Status assignment
# ---------------------------------------------------------------------------

# Design fields that are inherently user-generated or require live systems
_GENERATION_FIELDS = {
    "student_quote", "student_quote_author", "vibe_tags",
    "campus_mood_study_pct", "campus_mood_social_pct", "campus_mood_chill_pct",
    "student_video_url", "social_proof_count",
    "student_cost_quote", "day_in_the_life_timeline",
}

_EXTERNAL_API_FIELDS = {
    "nearby_cafes", "nearby_restaurants", "nearby_bars",
    "walk_score", "cafes_within_10min", "bars_within_10min",
    "beaches_within_15min", "food_map_data",
}

_LLM_GENERATION_FIELDS = {
    "tagline", "compare_vibe_label", "compare_mood_label",
    "tuition_comparison_label", "hero_image_url",
}

_ENRICHMENT_FIELDS = {
    "weekly_budget_breakdown", "scholarships",
    "transport_info", "safety_support_info",
    "clubs_and_societies",
}


def assign_status(
    field: str,
    heuristic_status: str | None,
    llm_suggestion: dict | None,
) -> tuple[str, str]:
    """Return (status, mapped_source_field_or_note)."""

    # 1. If heuristic matched, use it
    if heuristic_status:
        return heuristic_status, ""

    # 2. Known generation / UGC fields
    if field in _GENERATION_FIELDS:
        return "NEEDS_GENERATION", "Requires student UGC / live system"

    # 3. Known external-API fields
    if field in _EXTERNAL_API_FIELDS:
        return "NEEDS_ENRICHMENT", "Requires Google Places / Walk Score API"

    # 4. Known LLM-generation fields
    if field in _LLM_GENERATION_FIELDS:
        return "NEEDS_GENERATION", "Generated via LLM from source data"

    # 5. Known enrichment fields
    if field in _ENRICHMENT_FIELDS:
        return "NEEDS_ENRICHMENT", "Requires data enrichment / scraping"

    # 6. LLM suggestion
    if llm_suggestion:
        src = llm_suggestion.get("suggested_source", "")
        src_field = llm_suggestion.get("suggested_field", "")
        reasoning = llm_suggestion.get("reasoning", "")
        confidence = llm_suggestion.get("confidence", "low")

        if src in ("db",):
            return "AVAILABLE_DB", f"(LLM {confidence}) {src_field} -- {reasoning}"
        if src in ("api_courses", "api_providers"):
            return "AVAILABLE_API", f"(LLM {confidence}) {src_field} -- {reasoning}"
        if src in ("external_api", "llm_generation", "ugc"):
            return "NEEDS_GENERATION", f"(LLM {confidence}) {reasoning}"
        if src == "not_feasible":
            return "NOT_FEASIBLE_FOR_POC", f"(LLM) {reasoning}"

        return "NEEDS_ENRICHMENT", f"(LLM {confidence}) {reasoning}"

    # 7. Fallback
    return "NOT_FEASIBLE_FOR_POC", "No source identified"


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

def write_report(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(rows)

    # Reorder columns for readability
    col_order = [
        "tab", "field", "description", "status",
        "matched_source", "notes",
    ]
    for c in col_order:
        if c not in df.columns:
            df[c] = ""
    df = df[col_order]

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        # Full gap analysis
        df.to_excel(writer, sheet_name="gap_analysis", index=False)
        print(f"  [+] Sheet 'gap_analysis' ({len(df)} fields)")

        # Summary pivot: status counts per tab
        summary = (
            df.groupby(["tab", "status"])
            .size()
            .unstack(fill_value=0)
            .reset_index()
        )
        summary.to_excel(writer, sheet_name="summary_by_tab", index=False)
        print(f"  [+] Sheet 'summary_by_tab'")

        # Status totals
        totals = df["status"].value_counts().reset_index()
        totals.columns = ["status", "count"]
        totals.to_excel(writer, sheet_name="status_totals", index=False)
        print(f"  [+] Sheet 'status_totals'")

    print(f"  Report saved to {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Map design-doc fields to available data sources and produce a gap report.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples
            --------
              python -m scripts.A_audit.04_gap_analysis
              python -m scripts.A_audit.04_gap_analysis --no-llm
              python -m scripts.A_audit.04_gap_analysis --schema-xlsx data/reports/schema_audit.xlsx
        """),
    )
    parser.add_argument(
        "--schema-xlsx",
        type=str,
        default=None,
        help="Path to schema_audit.xlsx from step 01 (default: data/reports/schema_audit.xlsx).",
    )
    parser.add_argument(
        "--api-probe-json",
        type=str,
        default=None,
        help="Path to api_probe.json from step 03 (default: data/reports/api_probe.json).",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        default=False,
        help="Skip the LLM-assisted mapping step.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Override the output path (default: data/reports/gap_analysis.xlsx).",
    )
    args = parser.parse_args()

    reports_dir = Path(DATA_REPORTS)
    schema_xlsx = Path(args.schema_xlsx) if args.schema_xlsx else reports_dir / "schema_audit.xlsx"
    api_probe_json = Path(args.api_probe_json) if args.api_probe_json else reports_dir / "api_probe.json"
    output_path = Path(args.output) if args.output else reports_dir / "gap_analysis.xlsx"

    # ---- Load sources ---------------------------------------------------
    print("[04_gap_analysis] Loading data sources ...")
    db_columns = load_db_columns(schema_xlsx)
    courses_fields, providers_fields = load_api_fields(api_probe_json)

    all_api_fields = courses_fields | providers_fields

    # ---- Heuristic matching ---------------------------------------------
    print(f"\n[04_gap_analysis] Matching {len(DESIGN_DOC_FIELDS)} design-doc fields ...")
    start = time.time()

    results: list[dict] = []
    unmapped: list[dict] = []

    for entry in DESIGN_DOC_FIELDS:
        field = entry["field"]
        status, matched = heuristic_match(field, db_columns, courses_fields, providers_fields)
        if status:
            results.append({
                "tab": entry["tab"],
                "field": field,
                "description": entry["description"],
                "status": status,
                "matched_source": matched or "",
                "notes": "",
            })
            print(f"  [MATCH]   {field:35s} -> {status:20s} ({matched})")
        else:
            unmapped.append(entry)
            results.append({
                "tab": entry["tab"],
                "field": field,
                "description": entry["description"],
                "status": None,  # will be filled in below
                "matched_source": "",
                "notes": "",
            })

    matched_count = len(DESIGN_DOC_FIELDS) - len(unmapped)
    print(f"\n  Heuristic matching: {matched_count}/{len(DESIGN_DOC_FIELDS)} matched, "
          f"{len(unmapped)} unmapped")

    # ---- LLM-assisted matching (optional) -------------------------------
    llm_suggestions: dict[str, dict] = {}
    if unmapped and not args.no_llm:
        print(f"\n[04_gap_analysis] LLM-assisted mapping for {len(unmapped)} unmapped fields ...")
        llm_suggestions = llm_suggest_mappings(
            unmapped,
            sorted(db_columns),
            sorted(all_api_fields),
        )

    # ---- Final status assignment ----------------------------------------
    print(f"\n[04_gap_analysis] Assigning final statuses ...")
    for row in results:
        if row["status"] is None:
            field = row["field"]
            llm_sug = llm_suggestions.get(field)
            status, notes = assign_status(field, None, llm_sug)
            row["status"] = status
            row["notes"] = notes
            print(f"  [ASSIGN]  {field:35s} -> {status:25s} {notes[:60]}")

    elapsed = time.time() - start

    # ---- Write report ---------------------------------------------------
    print(f"\n[04_gap_analysis] Writing gap analysis report ...")
    write_report(results, output_path)

    # ---- Print summary --------------------------------------------------
    from collections import Counter
    status_counts = Counter(r["status"] for r in results)
    print(f"\n{'='*60}")
    print(f"  GAP ANALYSIS SUMMARY  ({elapsed:.1f}s)")
    print(f"{'='*60}")
    for status in [
        "AVAILABLE_DB", "AVAILABLE_API", "NEEDS_ENRICHMENT",
        "NEEDS_GENERATION", "NOT_FEASIBLE_FOR_POC",
    ]:
        count = status_counts.get(status, 0)
        bar = "#" * count
        print(f"  {status:25s}  {count:3d}  {bar}")
    print(f"  {'TOTAL':25s}  {len(results):3d}")
    print(f"{'='*60}")

    print("\n[04_gap_analysis] Done.")


if __name__ == "__main__":
    main()
