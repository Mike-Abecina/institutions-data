"""
Generate Institution Rankings Data

Queries the ACIR database to generate comprehensive institution ranking data
based on the analysis in INSTITUTION_RANKING_ANALYSIS.md.

Outputs JSON with rankings for:
- Top universities per state
- Top RTOs/TAFEs per state
- Top schools per state
- National top 10 universities
- Hidden gems
- Big impact institutions
"""

import os
import json
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error
from typing import Optional, Dict, List
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


def normalize_z_score(values: pd.Series) -> pd.Series:
    """Normalize values using z-score, then scale to 0-1 range."""
    if values.std() == 0:
        return pd.Series([0.5] * len(values), index=values.index)
    z_scores = (values - values.mean()) / values.std()
    # Scale to 0-1 range (z-scores typically fall between -3 and +3)
    normalized = (z_scores + 3) / 6
    return normalized.clip(0, 1)


def get_states() -> pd.DataFrame:
    """Get all states."""
    sql = """
    SELECT id, name, abbreviation
    FROM states
    ORDER BY name
    """
    return get_data(sql)


def get_organisation_types() -> pd.DataFrame:
    """Get all organisation types."""
    sql = """
    SELECT id, name, description
    FROM organisation_types
    WHERE deleted_at IS NULL
    ORDER BY name
    """
    return get_data(sql)


def get_rating_types() -> pd.DataFrame:
    """Get all rating types."""
    sql = """
    SELECT id, name, short_description, long_description
    FROM rating_types
    WHERE deleted_at IS NULL
    ORDER BY name
    """
    return get_data(sql)


def get_universities_with_metrics(state_id: Optional[int] = None) -> pd.DataFrame:
    """
    Get universities with all relevant metrics for ranking.

    Metrics include:
    - Graduate salary and employment (from organisation_mba)
    - Student composition (from organisation_he_enrolment)
    - Quality ratings (from organisation_rating)
    """
    state_filter = f"AND o.state_id = {state_id}" if state_id else ""

    sql = f"""
    SELECT DISTINCT
        o.id,
        o.name,
        o.abbreviation,
        o.state_id,
        s.name AS state_name,
        s.abbreviation AS state_abbr,
        o.year_established,
        o.web_address,
        ot.name AS organisation_type,

        -- Graduate outcomes (MBA data)
        mba.avg_ft_g_sal AS grad_salary_ft,
        mba.avg_pt_g_sal AS grad_salary_pt,
        mba.no_grad_emp_rate AS employment_rate,
        mba.g_sal_rating AS salary_rating,
        mba.corp_rating AS corporate_rating,
        mba.grad_emp_rating AS employment_rating,

        -- Student composition (HE enrolment)
        he.total_student_numbers,
        he.pc_ug_students AS pct_undergraduate,
        he.pc_pg_students AS pct_postgraduate,
        he.pc_international_students AS pct_international,
        he.pc_ft_students AS pct_fulltime,
        he.pc_mature_age_and_other AS pct_mature_age,
        he.pc_vocational_pathways AS pct_vocational_pathways,
        he.year AS he_year

    FROM organisations o
    LEFT JOIN states s ON o.state_id = s.id
    LEFT JOIN organisation_types ot ON o.organisation_type_id = ot.id
    LEFT JOIN organisation_mba mba ON o.id = mba.organisation_id
    LEFT JOIN organisation_he_enrolment he ON o.id = he.organisation_id
    WHERE
        o.deleted_at IS NULL
        AND ot.name LIKE '%University%'
        {state_filter}
        AND (he.year IS NULL OR he.year = (SELECT MAX(year) FROM organisation_he_enrolment))
    ORDER BY o.name
    """

    df = get_data(sql)

    if df is None or len(df) == 0:
        return pd.DataFrame()

    # Get average star ratings
    ratings_sql = """
    SELECT
        organisation_id,
        AVG(stars) as avg_stars,
        AVG(percentage) as avg_percentage
    FROM organisation_rating
    WHERE stars IS NOT NULL
    GROUP BY organisation_id
    """
    ratings_df = get_data(ratings_sql)

    if ratings_df is not None:
        df = df.merge(ratings_df, left_on='id', right_on='organisation_id', how='left')

    return df


def calculate_university_composite_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate composite score for universities.

    Formula:
    (0.35 × normalized_grad_salary) +
    (0.25 × normalized_employment_rate) +
    (0.20 × normalized_international_pct) +
    (0.10 × normalized_total_students) +
    (0.10 × normalized_quality_rating)
    """
    if len(df) == 0:
        return df

    # Fill NaN values with 0 for calculation
    df['grad_salary_ft'] = df['grad_salary_ft'].fillna(0)
    df['employment_rate'] = df['employment_rate'].fillna(0)
    df['pct_international'] = df['pct_international'].fillna(0)
    df['total_student_numbers'] = df['total_student_numbers'].fillna(0)
    df['avg_stars'] = df['avg_stars'].fillna(0)

    # Normalize each metric
    df['norm_salary'] = normalize_z_score(df['grad_salary_ft'])
    df['norm_employment'] = normalize_z_score(df['employment_rate'])
    df['norm_international'] = normalize_z_score(df['pct_international'])
    df['norm_students'] = normalize_z_score(df['total_student_numbers'])
    df['norm_stars'] = normalize_z_score(df['avg_stars'])

    # Calculate composite score
    df['composite_score'] = (
        (0.35 * df['norm_salary']) +
        (0.25 * df['norm_employment']) +
        (0.20 * df['norm_international']) +
        (0.10 * df['norm_students']) +
        (0.10 * df['norm_stars'])
    )

    return df


def get_rtos_with_metrics(state_id: Optional[int] = None) -> pd.DataFrame:
    """
    Get RTOs/TAFEs with metrics.

    Metrics include:
    - Star ratings and percentages
    - Field of study breadth
    - Course count
    """
    state_filter = f"AND o.state_id = {state_id}" if state_id else ""

    sql = f"""
    SELECT DISTINCT
        o.id,
        o.name,
        o.state_id,
        s.name AS state_name,
        s.abbreviation AS state_abbr,
        o.rto_code,
        o.cricos_code,
        o.year_established,
        o.web_address,
        ot.name AS organisation_type
    FROM organisations o
    LEFT JOIN states s ON o.state_id = s.id
    LEFT JOIN organisation_types ot ON o.organisation_type_id = ot.id
    WHERE
        o.deleted_at IS NULL
        AND (o.rto_code IS NOT NULL OR ot.name LIKE '%TAFE%' OR ot.name LIKE '%RTO%')
        {state_filter}
    ORDER BY o.name
    """

    df = get_data(sql)

    if df is None or len(df) == 0:
        return pd.DataFrame()

    # Get average star ratings
    ratings_sql = """
    SELECT
        organisation_id,
        AVG(stars) as avg_stars,
        AVG(percentage) as avg_percentage,
        COUNT(*) as rating_count
    FROM organisation_rating
    WHERE stars IS NOT NULL
    GROUP BY organisation_id
    """
    ratings_df = get_data(ratings_sql)

    if ratings_df is not None:
        df = df.merge(ratings_df, left_on='id', right_on='organisation_id', how='left')

    # Get field of study breadth
    fos_sql = """
    SELECT
        organisation_id,
        COUNT(DISTINCT field_of_study_id) as field_count
    FROM organisation_rating_field_of_study
    GROUP BY organisation_id
    """
    fos_df = get_data(fos_sql)

    if fos_df is not None:
        df = df.merge(fos_df, left_on='id', right_on='organisation_id', how='left')

    # Get course count
    courses_sql = """
    SELECT
        organisation_id,
        COUNT(*) as course_count
    FROM courses
    WHERE deleted_at IS NULL
    GROUP BY organisation_id
    """
    courses_df = get_data(courses_sql)

    if courses_df is not None:
        df = df.merge(courses_df, left_on='id', right_on='organisation_id', how='left')

    return df


def calculate_rto_composite_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate composite score for RTOs.

    Formula:
    (0.40 × avg_star_rating) +
    (0.30 × field_of_study_breadth) +
    (0.20 × completion_rate_proxy) +
    (0.10 × student_volume_normalized)
    """
    if len(df) == 0:
        return df

    # Fill NaN values
    df['avg_stars'] = df['avg_stars'].fillna(0)
    df['field_count'] = df['field_count'].fillna(0)
    df['avg_percentage'] = df['avg_percentage'].fillna(0)
    df['course_count'] = df['course_count'].fillna(0)

    # Normalize each metric
    df['norm_stars'] = normalize_z_score(df['avg_stars'])
    df['norm_fields'] = normalize_z_score(df['field_count'])
    df['norm_completion'] = normalize_z_score(df['avg_percentage'])
    df['norm_courses'] = normalize_z_score(df['course_count'])

    # Calculate composite score
    df['composite_score'] = (
        (0.40 * df['norm_stars']) +
        (0.30 * df['norm_fields']) +
        (0.20 * df['norm_completion']) +
        (0.10 * df['norm_courses'])
    )

    return df


def get_schools_with_metrics(state_id: Optional[int] = None) -> pd.DataFrame:
    """
    Get schools with metrics.

    Metrics include:
    - VCE/ATAR performance
    - Student destinations
    - NAPLAN scores
    """
    state_filter = f"AND o.state_id = {state_id}" if state_id else ""

    sql = f"""
    SELECT DISTINCT
        o.id,
        o.name,
        o.state_id,
        s.name AS state_name,
        s.abbreviation AS state_abbr,
        o.year_established,
        o.web_address,
        ot.name AS organisation_type,

        -- VCE/VCAA performance
        vcaa.median_atar,
        vcaa.median_vce_score,
        vcaa.above_40 AS pct_atar_above_40,
        vcaa.vce_completions AS vce_completion_rate,
        vcaa.number_vce_students,
        vcaa.ib_score_40,
        vcaa.ib_diploma,
        vcaa.year AS vcaa_year,

        -- Student destinations
        dest.bachelors AS pct_to_university,
        dest.tafe_vet AS pct_to_tafe,
        dest.employed AS pct_employed,
        dest.apprentice_trainee AS pct_apprentice,
        dest.looking_for_work AS pct_looking_for_work,
        dest.year AS dest_year

    FROM organisations o
    LEFT JOIN states s ON o.state_id = s.id
    LEFT JOIN organisation_types ot ON o.organisation_type_id = ot.id
    LEFT JOIN organisation_le ole ON o.id = ole.organisation_id
    LEFT JOIN organisation_le_vcaa vcaa ON o.id = vcaa.organisation_id
    LEFT JOIN organisation_le_destinations dest ON o.id = dest.organisation_id AND vcaa.year = dest.year
    WHERE
        o.deleted_at IS NULL
        AND ole.id IS NOT NULL
        {state_filter}
        AND (vcaa.year IS NULL OR vcaa.year = (SELECT MAX(year) FROM organisation_le_vcaa))
    ORDER BY o.name
    """

    df = get_data(sql)

    if df is None or len(df) == 0:
        return pd.DataFrame()

    # Get NAPLAN scores
    naplan_sql = """
    SELECT
        organisation_id,
        AVG((reading + writing + spelling + grammar + numeracy) / 5.0) as naplan_composite
    FROM organisation_le_naplan
    WHERE publish_data = 1
    GROUP BY organisation_id
    """
    naplan_df = get_data(naplan_sql)

    if naplan_df is not None:
        df = df.merge(naplan_df, left_on='id', right_on='organisation_id', how='left')

    return df


def calculate_school_composite_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate composite score for schools.

    Formula:
    (0.40 × normalized_median_atar) +
    (0.25 × normalized_university_pathway_pct) +
    (0.20 × normalized_vce_completion_rate) +
    (0.15 × normalized_naplan_composite)
    """
    if len(df) == 0:
        return df

    # Fill NaN values
    df['median_atar'] = df['median_atar'].fillna(0)
    df['pct_to_university'] = df['pct_to_university'].fillna(0)
    df['vce_completion_rate'] = df['vce_completion_rate'].fillna(0)
    df['naplan_composite'] = df['naplan_composite'].fillna(0)

    # Normalize each metric
    df['norm_atar'] = normalize_z_score(df['median_atar'])
    df['norm_uni_pathway'] = normalize_z_score(df['pct_to_university'])
    df['norm_vce_completion'] = normalize_z_score(df['vce_completion_rate'])
    df['norm_naplan'] = normalize_z_score(df['naplan_composite'])

    # Calculate composite score
    df['composite_score'] = (
        (0.40 * df['norm_atar']) +
        (0.25 * df['norm_uni_pathway']) +
        (0.20 * df['norm_vce_completion']) +
        (0.15 * df['norm_naplan'])
    )

    return df


def generate_rankings() -> Dict:
    """Generate all institution rankings."""

    print("="*80)
    print("GENERATING INSTITUTION RANKINGS")
    print("="*80)

    rankings = {
        "metadata": {
            "generated_at": pd.Timestamp.now().isoformat(),
            "source": "ACIR Database",
            "analysis_doc": "INSTITUTION_RANKING_ANALYSIS.md"
        },
        "by_state_universities": {},
        "by_state_rtos": {},
        "by_state_schools": {},
        "national_universities": [],
        "hidden_gems": [],
        "big_impact": []
    }

    # Get states
    states_df = get_states()
    if states_df is None:
        print("Failed to retrieve states")
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
        if len(unis_df) > 0:
            unis_df = calculate_university_composite_score(unis_df)
            unis_df = unis_df.sort_values('composite_score', ascending=False).head(10)

            print(f"\n🎓 UNIVERSITIES ({len(unis_df)} found)")
            print("-" * 80)

            rankings["by_state_universities"][state_abbr] = []
            for idx, uni in unis_df.iterrows():
                rank = len(rankings["by_state_universities"][state_abbr]) + 1

                # Print detailed metrics for top 3
                if rank <= 3:
                    print(f"\n  #{rank}. {uni['name']}")
                    print(f"      Composite Score: {uni['composite_score']:.3f}")
                    print(f"      Metrics:")
                    print(f"        - Grad Salary (FT):    ${int(uni['grad_salary_ft']):,}" if pd.notna(uni['grad_salary_ft']) and uni['grad_salary_ft'] > 0 else "        - Grad Salary (FT):    N/A")
                    print(f"        - Employment Rate:     {uni['employment_rate']:.1f}%" if pd.notna(uni['employment_rate']) and uni['employment_rate'] > 0 else "        - Employment Rate:     N/A")
                    print(f"        - International %:     {uni['pct_international']:.1f}%" if pd.notna(uni['pct_international']) and uni['pct_international'] > 0 else "        - International %:     N/A")
                    print(f"        - Total Students:      {int(uni['total_student_numbers']):,}" if pd.notna(uni['total_student_numbers']) and uni['total_student_numbers'] > 0 else "        - Total Students:      N/A")
                    print(f"        - Avg Star Rating:     {uni['avg_stars']:.2f}/5" if pd.notna(uni['avg_stars']) and uni['avg_stars'] > 0 else "        - Avg Star Rating:     N/A")

                rankings["by_state_universities"][state_abbr].append({
                    "rank": rank,
                    "organisation_id": int(uni['id']),
                    "name": uni['name'],
                    "composite_score": round(float(uni['composite_score']), 3),
                    "metrics": {
                        "grad_salary_ft": int(uni['grad_salary_ft']) if pd.notna(uni['grad_salary_ft']) else None,
                        "employment_rate": round(float(uni['employment_rate']), 1) if pd.notna(uni['employment_rate']) else None,
                        "pct_international": round(float(uni['pct_international']), 1) if pd.notna(uni['pct_international']) else None,
                        "total_students": int(uni['total_student_numbers']) if pd.notna(uni['total_student_numbers']) else None,
                        "avg_stars": round(float(uni['avg_stars']), 2) if pd.notna(uni['avg_stars']) else None
                    }
                })

            if len(unis_df) > 3:
                print(f"\n  ... and {len(unis_df) - 3} more universities")

        # RTOs/TAFEs
        rtos_df = get_rtos_with_metrics(state_id)
        if len(rtos_df) > 0:
            rtos_df = calculate_rto_composite_score(rtos_df)
            rtos_df = rtos_df.sort_values('composite_score', ascending=False).head(10)

            print(f"\n🔧 RTOs/TAFEs ({len(rtos_df)} found)")
            print("-" * 80)

            rankings["by_state_rtos"][state_abbr] = []
            for idx, rto in rtos_df.iterrows():
                rank = len(rankings["by_state_rtos"][state_abbr]) + 1

                # Print detailed metrics for top 3
                if rank <= 3:
                    print(f"\n  #{rank}. {rto['name']}")
                    print(f"      Composite Score: {rto['composite_score']:.3f}")
                    print(f"      Metrics:")
                    print(f"        - Avg Star Rating:     {rto['avg_stars']:.2f}/5" if pd.notna(rto['avg_stars']) and rto['avg_stars'] > 0 else "        - Avg Star Rating:     N/A")
                    print(f"        - Fields Offered:      {int(rto['field_count'])} disciplines" if pd.notna(rto['field_count']) and rto['field_count'] > 0 else "        - Fields Offered:      N/A")
                    print(f"        - Avg Completion:      {rto['avg_percentage']:.1f}%" if pd.notna(rto['avg_percentage']) and rto['avg_percentage'] > 0 else "        - Avg Completion:      N/A")
                    print(f"        - Total Courses:       {int(rto['course_count'])}" if pd.notna(rto['course_count']) and rto['course_count'] > 0 else "        - Total Courses:       N/A")

                rankings["by_state_rtos"][state_abbr].append({
                    "rank": rank,
                    "organisation_id": int(rto['id']),
                    "name": rto['name'],
                    "composite_score": round(float(rto['composite_score']), 3),
                    "metrics": {
                        "avg_stars": round(float(rto['avg_stars']), 2) if pd.notna(rto['avg_stars']) else None,
                        "field_count": int(rto['field_count']) if pd.notna(rto['field_count']) else None,
                        "avg_percentage": round(float(rto['avg_percentage']), 1) if pd.notna(rto['avg_percentage']) else None,
                        "course_count": int(rto['course_count']) if pd.notna(rto['course_count']) else None
                    }
                })

            if len(rtos_df) > 3:
                print(f"\n  ... and {len(rtos_df) - 3} more RTOs/TAFEs")

        # Schools
        schools_df = get_schools_with_metrics(state_id)
        if len(schools_df) > 0:
            schools_df = calculate_school_composite_score(schools_df)
            schools_df = schools_df.sort_values('composite_score', ascending=False).head(20)

            print(f"\n🏫 SCHOOLS ({len(schools_df)} found)")
            print("-" * 80)

            rankings["by_state_schools"][state_abbr] = []
            for idx, school in schools_df.iterrows():
                rank = len(rankings["by_state_schools"][state_abbr]) + 1

                # Print detailed metrics for top 3
                if rank <= 3:
                    print(f"\n  #{rank}. {school['name']}")
                    print(f"      Composite Score: {school['composite_score']:.3f}")
                    print(f"      Metrics:")
                    print(f"        - Median ATAR:         {school['median_atar']:.2f}" if pd.notna(school['median_atar']) and school['median_atar'] > 0 else "        - Median ATAR:         N/A")
                    print(f"        - Uni Pathway:         {school['pct_to_university']:.1f}%" if pd.notna(school['pct_to_university']) and school['pct_to_university'] > 0 else "        - Uni Pathway:         N/A")
                    print(f"        - VCE Completion:      {school['vce_completion_rate']:.1f}%" if pd.notna(school['vce_completion_rate']) and school['vce_completion_rate'] > 0 else "        - VCE Completion:      N/A")
                    print(f"        - NAPLAN Score:        {school['naplan_composite']:.1f}" if pd.notna(school['naplan_composite']) and school['naplan_composite'] > 0 else "        - NAPLAN Score:        N/A")

                rankings["by_state_schools"][state_abbr].append({
                    "rank": rank,
                    "organisation_id": int(school['id']),
                    "name": school['name'],
                    "composite_score": round(float(school['composite_score']), 3),
                    "metrics": {
                        "median_atar": round(float(school['median_atar']), 2) if pd.notna(school['median_atar']) else None,
                        "pct_to_university": round(float(school['pct_to_university']), 1) if pd.notna(school['pct_to_university']) else None,
                        "vce_completion_rate": round(float(school['vce_completion_rate']), 1) if pd.notna(school['vce_completion_rate']) else None,
                        "naplan_composite": round(float(school['naplan_composite']), 1) if pd.notna(school['naplan_composite']) else None
                    }
                })

            if len(schools_df) > 3:
                print(f"\n  ... and {len(schools_df) - 3} more schools")

    # National top 10 universities
    print(f"\n{'='*80}")
    print("NATIONAL TOP 10 UNIVERSITIES")
    print('='*80)

    national_unis = get_universities_with_metrics()
    if len(national_unis) > 0:
        national_unis = calculate_university_composite_score(national_unis)
        national_unis = national_unis.sort_values('composite_score', ascending=False).head(10)

        for idx, uni in national_unis.iterrows():
            rank = len(rankings["national_universities"]) + 1

            print(f"\n  #{rank}. {uni['name']} ({uni['state_abbr'] if pd.notna(uni['state_abbr']) else 'N/A'})")
            print(f"      Composite Score: {uni['composite_score']:.3f}")
            print(f"      Metrics:")
            print(f"        - Grad Salary (FT):    ${int(uni['grad_salary_ft']):,}" if pd.notna(uni['grad_salary_ft']) and uni['grad_salary_ft'] > 0 else "        - Grad Salary (FT):    N/A")
            print(f"        - Employment Rate:     {uni['employment_rate']:.1f}%" if pd.notna(uni['employment_rate']) and uni['employment_rate'] > 0 else "        - Employment Rate:     N/A")
            print(f"        - International %:     {uni['pct_international']:.1f}%" if pd.notna(uni['pct_international']) and uni['pct_international'] > 0 else "        - International %:     N/A")
            print(f"        - Postgraduate %:      {uni['pct_postgraduate']:.1f}%" if pd.notna(uni['pct_postgraduate']) and uni['pct_postgraduate'] > 0 else "        - Postgraduate %:      N/A")

            rankings["national_universities"].append({
                "rank": rank,
                "organisation_id": int(uni['id']),
                "name": uni['name'],
                "state": uni['state_abbr'] if pd.notna(uni['state_abbr']) else None,
                "composite_score": round(float(uni['composite_score']), 3),
                "metrics": {
                    "grad_salary_ft": int(uni['grad_salary_ft']) if pd.notna(uni['grad_salary_ft']) else None,
                    "employment_rate": round(float(uni['employment_rate']), 1) if pd.notna(uni['employment_rate']) else None,
                    "pct_international": round(float(uni['pct_international']), 1) if pd.notna(uni['pct_international']) else None,
                    "pct_postgraduate": round(float(uni['pct_postgraduate']), 1) if pd.notna(uni['pct_postgraduate']) else None
                }
            })

    return rankings


def main():
    """Main execution."""
    rankings = generate_rankings()

    # Save to JSON
    output_file = "institution_rankings.json"
    with open(output_file, 'w') as f:
        json.dump(rankings, f, indent=2)

    print(f"\n{'='*80}")
    print("COMPLETE")
    print("="*80)
    print(f"Rankings saved to: {output_file}")

    # Print summary
    print(f"\nSummary:")
    print(f"  States with university rankings: {len(rankings['by_state_universities'])}")
    print(f"  States with RTO rankings: {len(rankings['by_state_rtos'])}")
    print(f"  States with school rankings: {len(rankings['by_state_schools'])}")
    print(f"  National top universities: {len(rankings['national_universities'])}")

    # Print sample from national top
    if rankings['national_universities']:
        print(f"\nTop 3 National Universities:")
        for uni in rankings['national_universities'][:3]:
            print(f"  {uni['rank']}. {uni['name']} ({uni['state']}) - Score: {uni['composite_score']}")


if __name__ == "__main__":
    main()
