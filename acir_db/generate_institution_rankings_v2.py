"""
Generate Institution Rankings - Based on Available Data

Creates rankings for:
1. Universities by State (star ratings + course offerings)
2. Schools by State (NAPLAN, ATAR, uni pathways, VCE completion)
3. National Top 10 Universities

Uses only metrics with good data coverage based on data quality analysis.
"""

import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error
from typing import Optional
import json
from datetime import datetime
from collections import defaultdict

# Load environment variables
load_dotenv()

# Database connection config
DB_CONFIG = {
    "host": os.getenv("DATABSE_HOST"),
    "port": int(os.getenv("DATABASE_PORT", "3306")),
    "database": os.getenv("DATABASE_NAME"),
    "user": os.getenv("DATABASE_USER", "admin"),
    "password": os.getenv("DATABASE_PASSWORD"),
    "connect_timeout": 30
}


def get_data(sql: str) -> Optional[pd.DataFrame]:
    """Execute SQL query and return DataFrame."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    except Error as e:
        print(f"Database error: {e}")
        return None


def normalize_z_score(series: pd.Series) -> pd.Series:
    """Normalize using Z-score, handling NaN values."""
    series = pd.to_numeric(series, errors='coerce')
    mean = series.mean()
    std = series.std()
    if std == 0 or pd.isna(std):
        return pd.Series([0.5] * len(series), index=series.index)
    normalized = (series - mean) / std
    # Map to 0-1 range (approximately)
    normalized = (normalized + 3) / 6  # Assumes most values within 3 std devs
    normalized = normalized.clip(0, 1)
    return normalized


# =============================================================================
# UNIVERSITIES
# =============================================================================

def get_universities_with_metrics(state_id: Optional[int] = None) -> pd.DataFrame:
    """
    Get universities with available metrics: star ratings, course count, field diversity.

    Note: MBA and HE enrollment tables are empty, so we only use:
    - Star ratings (from organisation_rating)
    - Course count (from courses table)
    - Field diversity (count of unique fields)
    """
    state_filter = f"AND o.state_id = {state_id}" if state_id else ""

    sql = f"""
    SELECT DISTINCT
        o.id,
        o.name,
        COALESCE(s.name, 'Unknown') AS state_name,
        COALESCE(s.abbreviation, 'N/A') AS state_abbr,

        -- Star ratings (average across all rating types, excluding 0 which means "no rating")
        AVG(CASE WHEN r.stars > 0 THEN r.stars END) as avg_stars,

        -- Course metrics
        COUNT(DISTINCT c.id) as course_count,
        COUNT(DISTINCT cfs.field_of_study_id) as field_count

    FROM organisations o
    LEFT JOIN states s ON o.state_id = s.id
    JOIN organisation_types ot ON o.organisation_type_id = ot.id
    LEFT JOIN organisation_rating r ON o.id = r.organisation_id
    LEFT JOIN courses c ON o.id = c.organisation_id AND c.deleted_at IS NULL
    LEFT JOIN course_field_of_study cfs ON c.id = cfs.course_id

    WHERE (ot.name = 'University/Higher Education Institution'
           OR ot.name = 'Higher Education Institutions'
           OR ot.name = 'Dual Sector University')
      AND o.deleted_at IS NULL
      {state_filter}

    GROUP BY o.id, o.name, s.name, s.abbreviation
    HAVING AVG(CASE WHEN r.stars > 0 THEN r.stars END) IS NOT NULL
           OR course_count > 0
    """

    return get_data(sql)


def calculate_university_composite_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate composite score for universities based on available metrics.

    Formula:
    (0.70 × normalized_star_rating) +     # Primary quality metric (70%)
    (0.20 × normalized_course_count) +    # Course variety (20%)
    (0.10 × normalized_field_diversity)   # Field diversity (10%)
    """
    # Normalize each metric
    df['norm_stars'] = normalize_z_score(df['avg_stars'])
    df['norm_courses'] = normalize_z_score(df['course_count'])
    df['norm_fields'] = normalize_z_score(df['field_count'])

    # Calculate composite score
    df['composite_score'] = (
        (0.70 * df['norm_stars'].fillna(0)) +
        (0.20 * df['norm_courses'].fillna(0)) +
        (0.10 * df['norm_fields'].fillna(0))
    )

    return df


# =============================================================================
# SCHOOLS
# =============================================================================

def get_schools_with_metrics(state_id: int) -> pd.DataFrame:
    """
    Get schools with available metrics.

    Victoria: ATAR, VCE completion, NAPLAN, uni pathways
    Other states: NAPLAN, uni pathways
    """
    sql = f"""
    SELECT DISTINCT
        o.id,
        o.name,
        s.name AS state_name,
        s.abbreviation AS state_abbr,

        -- VIC-specific: ATAR and VCE
        vcaa.median_atar,
        vcaa.vce_completions,
        vcaa.number_vce_students,

        -- NAPLAN scores (using Year 9 as standard)
        AVG(CASE WHEN naplan.grade_id = 9 THEN naplan.reading END) as naplan_reading,
        AVG(CASE WHEN naplan.grade_id = 9 THEN naplan.writing END) as naplan_writing,
        AVG(CASE WHEN naplan.grade_id = 9 THEN naplan.spelling END) as naplan_spelling,
        AVG(CASE WHEN naplan.grade_id = 9 THEN naplan.grammar END) as naplan_grammar,
        AVG(CASE WHEN naplan.grade_id = 9 THEN naplan.numeracy END) as naplan_numeracy,

        -- Post-school destinations
        dest.bachelors as pct_to_university,
        dest.tafe_vet as pct_to_tafe,
        dest.employed as pct_employed

    FROM organisations o
    JOIN states s ON o.state_id = s.id
    JOIN organisation_types ot ON o.organisation_type_id = ot.id
    LEFT JOIN organisation_le_vcaa vcaa ON o.id = vcaa.organisation_id
    LEFT JOIN organisation_le_naplan naplan ON o.id = naplan.organisation_id
    LEFT JOIN organisation_le_destinations dest ON o.id = dest.organisation_id

    WHERE ot.is_lower_ed_type = 1
      AND o.deleted_at IS NULL
      AND o.state_id = {state_id}

    GROUP BY o.id, o.name, s.name, s.abbreviation,
             vcaa.median_atar, vcaa.vce_completions, vcaa.number_vce_students,
             dest.bachelors, dest.tafe_vet, dest.employed

    HAVING (vcaa.median_atar IS NOT NULL
            OR naplan_reading IS NOT NULL
            OR dest.bachelors IS NOT NULL)
    """

    df = get_data(sql)

    if df is not None and len(df) > 0:
        # Calculate composite NAPLAN score
        naplan_cols = ['naplan_reading', 'naplan_writing', 'naplan_spelling',
                       'naplan_grammar', 'naplan_numeracy']
        df['naplan_composite'] = df[naplan_cols].mean(axis=1)

        # Calculate VCE completion rate
        df['vce_completion_rate'] = (
            (df['vce_completions'] / df['number_vce_students'] * 100)
            .where(df['number_vce_students'] > 0)
        )

    return df if df is not None else pd.DataFrame()


def calculate_school_composite_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate composite score for schools.

    Victoria Formula (if ATAR available):
    (0.35 × normalized_median_atar) +
    (0.25 × normalized_pct_to_university) +
    (0.20 × normalized_vce_completion_rate) +
    (0.20 × normalized_naplan_composite)

    Other States Formula:
    (0.60 × normalized_naplan_composite) +
    (0.40 × normalized_pct_to_university)
    """
    # Check if this is Victoria (has ATAR data)
    has_atar = df['median_atar'].notna().any()

    if has_atar:
        # Victoria formula
        df['norm_atar'] = normalize_z_score(df['median_atar'])
        df['norm_uni_pathway'] = normalize_z_score(df['pct_to_university'])
        df['norm_vce_completion'] = normalize_z_score(df['vce_completion_rate'])
        df['norm_naplan'] = normalize_z_score(df['naplan_composite'])

        df['composite_score'] = (
            (0.35 * df['norm_atar'].fillna(0)) +
            (0.25 * df['norm_uni_pathway'].fillna(0)) +
            (0.20 * df['norm_vce_completion'].fillna(0)) +
            (0.20 * df['norm_naplan'].fillna(0))
        )
    else:
        # Other states formula
        df['norm_naplan'] = normalize_z_score(df['naplan_composite'])
        df['norm_uni_pathway'] = normalize_z_score(df['pct_to_university'])

        df['composite_score'] = (
            (0.60 * df['norm_naplan'].fillna(0)) +
            (0.40 * df['norm_uni_pathway'].fillna(0))
        )

    return df


# =============================================================================
# RANKING GENERATION
# =============================================================================

def generate_rankings():
    """Generate all feasible institution rankings."""

    rankings = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "source": "ACIR Database",
            "methodology": "INSTITUTION_RANKING_DATA_FINDINGS.md",
            "data_limitations": {
                "universities": "MBA (salary/employment) and HE enrollment data unavailable. Using star ratings only.",
                "rtos": "No quality metrics available. RTOs not ranked.",
                "schools": "Comprehensive data available. VIC has ATAR, all states have NAPLAN and pathways."
            }
        },
        "by_state_universities": {},
        "by_state_schools": {},
        "national_universities": []
    }

    # Get all states
    states_df = get_data("SELECT id, abbreviation, name FROM states WHERE abbreviation != 'ALL STATES' ORDER BY name")

    if states_df is None:
        print("Error: Could not load states")
        return rankings

    print(f"\nProcessing {len(states_df)} states...")

    # Process each state
    for _, state in states_df.iterrows():
        state_id = state['id']
        state_abbr = state['abbreviation']
        state_name = state['name']

        print(f"\n{'='*80}")
        print(f"STATE: {state_name} ({state_abbr})")
        print('='*80)

        # Universities
        unis_df = get_universities_with_metrics(state_id)
        if unis_df is not None and len(unis_df) > 0:
            unis_df = calculate_university_composite_score(unis_df)
            unis_df = unis_df.sort_values('composite_score', ascending=False).head(10)

            print(f"\n🎓 UNIVERSITIES ({len(unis_df)} found)")
            print("-" * 80)

            rankings["by_state_universities"][state_abbr] = []
            for idx, (_, uni) in enumerate(unis_df.iterrows(), 1):

                # Print detailed metrics for top 3
                if idx <= 3:
                    print(f"\n  #{idx}. {uni['name']}")
                    print(f"      Composite Score: {uni['composite_score']:.3f}")
                    print(f"      Metrics:")
                    print(f"        - Star Rating:    {uni['avg_stars']:.2f}/5" if pd.notna(uni['avg_stars']) and uni['avg_stars'] > 0 else "        - Star Rating:    N/A")
                    print(f"        - Courses:        {int(uni['course_count'])}" if pd.notna(uni['course_count']) else "        - Courses:        N/A")
                    print(f"        - Fields:         {int(uni['field_count'])} disciplines" if pd.notna(uni['field_count']) else "        - Fields:         N/A")

                rankings["by_state_universities"][state_abbr].append({
                    "rank": idx,
                    "organisation_id": int(uni['id']),
                    "name": uni['name'],
                    "composite_score": round(float(uni['composite_score']), 3),
                    "metrics": {
                        "avg_stars": round(float(uni['avg_stars']), 2) if pd.notna(uni['avg_stars']) else None,
                        "course_count": int(uni['course_count']) if pd.notna(uni['course_count']) else None,
                        "field_count": int(uni['field_count']) if pd.notna(uni['field_count']) else None
                    }
                })

            if len(unis_df) > 3:
                print(f"\n  ... and {len(unis_df) - 3} more universities")

        # Schools
        schools_df = get_schools_with_metrics(state_id)
        if len(schools_df) > 0:
            schools_df = calculate_school_composite_score(schools_df)
            schools_df = schools_df.sort_values('composite_score', ascending=False).head(20)

            print(f"\n🏫 SCHOOLS ({len(schools_df)} found)")
            print("-" * 80)

            rankings["by_state_schools"][state_abbr] = []
            for idx, (_, school) in enumerate(schools_df.iterrows(), 1):

                # Print detailed metrics for top 3
                if idx <= 3:
                    print(f"\n  #{idx}. {school['name']}")
                    print(f"      Composite Score: {school['composite_score']:.3f}")
                    print(f"      Metrics:")

                    if pd.notna(school.get('median_atar')) and school['median_atar'] > 0:
                        print(f"        - Median ATAR:         {school['median_atar']:.2f}")

                    if pd.notna(school.get('pct_to_university')) and school['pct_to_university'] > 0:
                        print(f"        - Uni Pathway:         {school['pct_to_university']:.1f}%")

                    if pd.notna(school.get('vce_completion_rate')) and school['vce_completion_rate'] > 0:
                        print(f"        - VCE Completion:      {school['vce_completion_rate']:.1f}%")

                    if pd.notna(school.get('naplan_composite')) and school['naplan_composite'] > 0:
                        print(f"        - NAPLAN Composite:    {school['naplan_composite']:.1f}")

                rankings["by_state_schools"][state_abbr].append({
                    "rank": idx,
                    "organisation_id": int(school['id']),
                    "name": school['name'],
                    "composite_score": round(float(school['composite_score']), 3),
                    "metrics": {
                        "median_atar": round(float(school['median_atar']), 2) if pd.notna(school.get('median_atar')) else None,
                        "pct_to_university": round(float(school['pct_to_university']), 1) if pd.notna(school.get('pct_to_university')) else None,
                        "vce_completion_rate": round(float(school['vce_completion_rate']), 1) if pd.notna(school.get('vce_completion_rate')) else None,
                        "naplan_composite": round(float(school['naplan_composite']), 1) if pd.notna(school.get('naplan_composite')) else None
                    }
                })

            if len(schools_df) > 3:
                print(f"\n  ... and {len(schools_df) - 3} more schools")

    # National top 10 universities
    print(f"\n{'='*80}")
    print("NATIONAL TOP 10 UNIVERSITIES")
    print('='*80)

    national_unis = get_universities_with_metrics()
    if national_unis is not None and len(national_unis) > 0:
        national_unis = calculate_university_composite_score(national_unis)
        national_unis = national_unis.sort_values('composite_score', ascending=False).head(10)

        for idx, (_, uni) in enumerate(national_unis.iterrows(), 1):
            print(f"\n  #{idx}. {uni['name']} ({uni['state_abbr'] if pd.notna(uni['state_abbr']) else 'N/A'})")
            print(f"      Composite Score: {uni['composite_score']:.3f}")
            print(f"      Metrics:")
            print(f"        - Star Rating:    {uni['avg_stars']:.2f}/5" if pd.notna(uni['avg_stars']) else "        - Star Rating:    N/A")
            print(f"        - Courses:        {int(uni['course_count'])}" if pd.notna(uni['course_count']) else "        - Courses:        N/A")
            print(f"        - Fields:         {int(uni['field_count'])} disciplines" if pd.notna(uni['field_count']) else "        - Fields:         N/A")

            rankings["national_universities"].append({
                "rank": idx,
                "organisation_id": int(uni['id']),
                "name": uni['name'],
                "state": uni['state_abbr'] if pd.notna(uni['state_abbr']) else None,
                "composite_score": round(float(uni['composite_score']), 3),
                "metrics": {
                    "avg_stars": round(float(uni['avg_stars']), 2) if pd.notna(uni['avg_stars']) else None,
                    "course_count": int(uni['course_count']) if pd.notna(uni['course_count']) else None,
                    "field_count": int(uni['field_count']) if pd.notna(uni['field_count']) else None
                }
            })

    return rankings


def main():
    """Main execution."""
    print("="*80)
    print("GENERATING INSTITUTION RANKINGS")
    print("="*80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nBased on data quality analysis:")
    print("  ✅ Universities: Star ratings + course offerings")
    print("  ✅ Schools: NAPLAN + uni pathways + ATAR (VIC) + VCE completion (VIC)")
    print("  ❌ RTOs: Insufficient data - not ranked")

    rankings = generate_rankings()

    # Save to JSON
    output_file = "acir_db/institution_rankings_v2.json"
    with open(output_file, 'w') as f:
        json.dump(rankings, f, indent=2)

    print(f"\n{'='*80}")
    print("COMPLETE")
    print("="*80)
    print(f"Rankings saved to: {output_file}")

    # Print summary
    print(f"\nSummary:")
    print(f"  States with university rankings: {len(rankings['by_state_universities'])}")
    print(f"  States with school rankings: {len(rankings['by_state_schools'])}")
    print(f"  National top universities: {len(rankings['national_universities'])}")

    # Count total institutions ranked
    total_unis = sum(len(unis) for unis in rankings['by_state_universities'].values())
    total_schools = sum(len(schools) for schools in rankings['by_state_schools'].values())

    print(f"\nTotal institutions ranked:")
    print(f"  Universities: {total_unis}")
    print(f"  Schools: {total_schools}")
    print(f"  TOTAL: {total_unis + total_schools}")


if __name__ == "__main__":
    main()
