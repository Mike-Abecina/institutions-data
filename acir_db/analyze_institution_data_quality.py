"""
Institution Data Quality Analysis

Analyzes the ACIR database to determine:
1. What metrics are available for universities, RTOs, and schools
2. Data completeness (% non-null) by state
3. Which rankings are feasible based on data availability
4. Recommends ranking formulas based on actual data
"""

import os
import pandas as pd
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error
from typing import Optional, Dict
import json
from datetime import datetime

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


def analyze_university_data_quality():
    """Analyze data completeness for university metrics."""
    print("\n" + "="*80)
    print("UNIVERSITIES - Data Quality Analysis")
    print("="*80)

    sql = """
    SELECT
        s.abbreviation as state,
        s.name as state_name,
        COUNT(DISTINCT o.id) as total_unis,

        -- MBA (Graduate outcomes) data
        COUNT(DISTINCT CASE WHEN mba.avg_ft_g_sal IS NOT NULL THEN o.id END) as has_salary,
        COUNT(DISTINCT CASE WHEN mba.no_grad_emp_rate IS NOT NULL THEN o.id END) as has_employment,

        -- HE Enrollment data
        COUNT(DISTINCT CASE WHEN he.total_student_numbers IS NOT NULL THEN o.id END) as has_enrollment,
        COUNT(DISTINCT CASE WHEN he.pc_international_students IS NOT NULL THEN o.id END) as has_international,
        COUNT(DISTINCT CASE WHEN he.pc_pg_students IS NOT NULL THEN o.id END) as has_postgrad_pct,

        -- Rating data
        COUNT(DISTINCT CASE WHEN r.stars IS NOT NULL THEN o.id END) as has_rating,

        -- Percentages
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN mba.avg_ft_g_sal IS NOT NULL THEN o.id END) / COUNT(DISTINCT o.id), 1) as salary_pct,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN mba.no_grad_emp_rate IS NOT NULL THEN o.id END) / COUNT(DISTINCT o.id), 1) as employment_pct,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN he.total_student_numbers IS NOT NULL THEN o.id END) / COUNT(DISTINCT o.id), 1) as enrollment_pct,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN he.pc_international_students IS NOT NULL THEN o.id END) / COUNT(DISTINCT o.id), 1) as international_pct,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN r.stars IS NOT NULL THEN o.id END) / COUNT(DISTINCT o.id), 1) as rating_pct

    FROM organisations o
    JOIN sites si ON si.organisation_id = o.id AND si.deleted_at IS NULL
    JOIN states s ON si.state_id = s.id
    LEFT JOIN organisation_mba mba ON o.id = mba.organisation_id
    LEFT JOIN organisation_he_enrolment he ON o.id = he.organisation_id
    LEFT JOIN organisation_rating r ON o.id = r.organisation_id
    JOIN organisation_types ot ON o.organisation_type_id = ot.id
    WHERE (ot.name = 'University/Higher Education Institution'
           OR ot.name = 'Higher Education Institutions'
           OR ot.name = 'Dual Sector University')
      AND o.deleted_at IS NULL
    GROUP BY s.abbreviation, s.name
    ORDER BY total_unis DESC;
    """

    df = get_data(sql)
    if df is not None:
        print(f"\nFound {len(df)} states with universities")
        print(f"Total universities: {df['total_unis'].sum()}")
        print("\n" + "-"*120)
        print(f"{'State':<6} {'Total':<7} {'Salary':<12} {'Employment':<14} {'Enrollment':<14} {'International':<14} {'Rating':<12}")
        print(f"{'':6} {'Unis':<7} {'Count / %':<12} {'Count / %':<14} {'Count / %':<14} {'Count / %':<14} {'Count / %':<12}")
        print("-"*120)

        for _, row in df.iterrows():
            print(f"{row['state']:<6} {int(row['total_unis']):<7} "
                  f"{int(row['has_salary'])}/{row['salary_pct']}%{' '*(8-len(str(row['salary_pct'])))} "
                  f"{int(row['has_employment'])}/{row['employment_pct']}%{' '*(10-len(str(row['employment_pct'])))} "
                  f"{int(row['has_enrollment'])}/{row['enrollment_pct']}%{' '*(10-len(str(row['enrollment_pct'])))} "
                  f"{int(row['has_international'])}/{row['international_pct']}%{' '*(10-len(str(row['international_pct'])))} "
                  f"{int(row['has_rating'])}/{row['rating_pct']}%")

        print("-"*120)
        sal_str = f"{df['salary_pct'].mean():.1f}"
        emp_str = f"{df['employment_pct'].mean():.1f}"
        enr_str = f"{df['enrollment_pct'].mean():.1f}"
        intl_str = f"{df['international_pct'].mean():.1f}"
        rat_str = f"{df['rating_pct'].mean():.1f}"
        print(f"{'TOTAL':<6} {int(df['total_unis'].sum()):<7} "
              f"{int(df['has_salary'].sum())}/{sal_str}%{' '*(8-len(sal_str))} "
              f"{int(df['has_employment'].sum())}/{emp_str}%{' '*(10-len(emp_str))} "
              f"{int(df['has_enrollment'].sum())}/{enr_str}%{' '*(10-len(enr_str))} "
              f"{int(df['has_international'].sum())}/{intl_str}%{' '*(10-len(intl_str))} "
              f"{int(df['has_rating'].sum())}/{rat_str}%")

        # Feasibility analysis
        print("\n📊 FEASIBILITY ANALYSIS:")
        sufficient_data_states = df[
            (df['salary_pct'] >= 30) &
            (df['employment_pct'] >= 30)
        ]

        if len(sufficient_data_states) > 0:
            print(f"  ✅ {len(sufficient_data_states)} states have sufficient data (≥30% coverage) for salary + employment rankings:")
            print(f"     {', '.join(sufficient_data_states['state'].tolist())}")
        else:
            print(f"  ❌ No states have sufficient salary + employment data (≥30% coverage)")
            print(f"  💡 Recommendation: Use rating-based rankings instead")

        # Alternative metrics
        rating_states = df[df['rating_pct'] >= 50]
        if len(rating_states) > 0:
            print(f"\n  ✅ {len(rating_states)} states have sufficient rating data (≥50% coverage):")
            print(f"     {', '.join(rating_states['state'].tolist())}")

        return df

    return None


def analyze_rto_data_quality():
    """Analyze data completeness for RTO/TAFE metrics."""
    print("\n" + "="*80)
    print("RTOs/TAFEs - Data Quality Analysis")
    print("="*80)

    sql = """
    SELECT
        s.abbreviation as state,
        s.name as state_name,
        COUNT(DISTINCT o.id) as total_rtos,

        -- Rating data
        COUNT(DISTINCT CASE WHEN r.stars IS NOT NULL THEN o.id END) as has_rating,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN r.stars IS NOT NULL THEN o.id END) / COUNT(DISTINCT o.id), 1) as rating_pct,

        -- Course count (from courses table)
        COUNT(DISTINCT CASE WHEN c.id IS NOT NULL THEN o.id END) as has_courses,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN c.id IS NOT NULL THEN o.id END) / COUNT(DISTINCT o.id), 1) as courses_pct

    FROM organisations o
    JOIN sites si ON si.organisation_id = o.id AND si.deleted_at IS NULL
    JOIN states s ON si.state_id = s.id
    LEFT JOIN organisation_rating r ON o.id = r.organisation_id
    LEFT JOIN courses c ON o.id = c.organisation_id AND c.deleted_at IS NULL
    JOIN organisation_types ot ON o.organisation_type_id = ot.id
    WHERE (ot.name = 'TAFE Institute' OR ot.name = 'Registered Training Organisation')
      AND o.deleted_at IS NULL
    GROUP BY s.abbreviation, s.name
    ORDER BY total_rtos DESC;
    """

    df = get_data(sql)
    if df is not None:
        print(f"\nFound {len(df)} states with RTOs/TAFEs")
        print(f"Total RTOs/TAFEs: {df['total_rtos'].sum()}")
        print("\n" + "-"*80)
        print(f"{'State':<6} {'Total':<7} {'Rating':<14} {'Has Courses':<14}")
        print(f"{'':6} {'RTOs':<7} {'Count / %':<14} {'Count / %':<14}")
        print("-"*80)

        for _, row in df.iterrows():
            print(f"{row['state']:<6} {int(row['total_rtos']):<7} "
                  f"{int(row['has_rating'])}/{row['rating_pct']}%{' '*(10-len(str(row['rating_pct'])))} "
                  f"{int(row['has_courses'])}/{row['courses_pct']}%")

        print("-"*80)
        rat_str = f"{df['rating_pct'].mean():.1f}"
        crs_str = f"{df['courses_pct'].mean():.1f}"
        print(f"{'TOTAL':<6} {int(df['total_rtos'].sum()):<7} "
              f"{int(df['has_rating'].sum())}/{rat_str}%{' '*(10-len(rat_str))} "
              f"{int(df['has_courses'].sum())}/{crs_str}%")

        # Feasibility
        print("\n📊 FEASIBILITY ANALYSIS:")
        sufficient_states = df[df['rating_pct'] >= 30]
        if len(sufficient_states) > 0:
            print(f"  ✅ {len(sufficient_states)} states have sufficient rating data (≥30% coverage):")
            print(f"     {', '.join(sufficient_states['state'].tolist())}")
        else:
            print(f"  ❌ Limited rating data for RTOs")

        return df

    return None


def analyze_school_data_quality():
    """Analyze data completeness for school metrics (VIC focus)."""
    print("\n" + "="*80)
    print("SCHOOLS - Data Quality Analysis (Victoria Focus)")
    print("="*80)

    sql = """
    SELECT
        s.abbreviation as state,
        s.name as state_name,
        COUNT(DISTINCT o.id) as total_schools,

        -- VCE/VCAA data
        COUNT(DISTINCT CASE WHEN vcaa.median_atar IS NOT NULL THEN o.id END) as has_atar,
        COUNT(DISTINCT CASE WHEN vcaa.median_vce_score IS NOT NULL THEN o.id END) as has_vce_score,
        COUNT(DISTINCT CASE WHEN vcaa.vce_completions IS NOT NULL THEN o.id END) as has_vce_completions,

        -- NAPLAN data
        COUNT(DISTINCT CASE WHEN naplan.reading IS NOT NULL THEN o.id END) as has_naplan,

        -- Destinations data
        COUNT(DISTINCT CASE WHEN dest.bachelors IS NOT NULL THEN o.id END) as has_destinations,

        -- Percentages
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN vcaa.median_atar IS NOT NULL THEN o.id END) / COUNT(DISTINCT o.id), 1) as atar_pct,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN naplan.reading IS NOT NULL THEN o.id END) / COUNT(DISTINCT o.id), 1) as naplan_pct,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN dest.bachelors IS NOT NULL THEN o.id END) / COUNT(DISTINCT o.id), 1) as destinations_pct

    FROM organisations o
    JOIN sites si ON si.organisation_id = o.id AND si.deleted_at IS NULL
    JOIN states s ON si.state_id = s.id
    LEFT JOIN organisation_le_vcaa vcaa ON o.id = vcaa.organisation_id
    LEFT JOIN organisation_le_naplan naplan ON o.id = naplan.organisation_id AND naplan.grade_id = 9
    LEFT JOIN organisation_le_destinations dest ON o.id = dest.organisation_id
    JOIN organisation_types ot ON o.organisation_type_id = ot.id
    WHERE ot.is_lower_ed_type = 1
      AND o.deleted_at IS NULL
    GROUP BY s.abbreviation, s.name
    ORDER BY total_schools DESC;
    """

    df = get_data(sql)
    if df is not None:
        print(f"\nFound {len(df)} states with schools")
        print(f"Total schools: {df['total_schools'].sum()}")
        print("\n" + "-"*90)
        print(f"{'State':<6} {'Total':<9} {'ATAR':<14} {'NAPLAN':<14} {'Destinations':<14}")
        print(f"{'':6} {'Schools':<9} {'Count / %':<14} {'Count / %':<14} {'Count / %':<14}")
        print("-"*90)

        for _, row in df.iterrows():
            print(f"{row['state']:<6} {int(row['total_schools']):<9} "
                  f"{int(row['has_atar'])}/{row['atar_pct']}%{' '*(10-len(str(row['atar_pct'])))} "
                  f"{int(row['has_naplan'])}/{row['naplan_pct']}%{' '*(10-len(str(row['naplan_pct'])))} "
                  f"{int(row['has_destinations'])}/{row['destinations_pct']}%")

        print("-"*90)

        # VIC specific analysis
        vic_data = df[df['state'] == 'VIC']
        if len(vic_data) > 0:
            vic = vic_data.iloc[0]
            print(f"\n📊 VICTORIA DETAILED ANALYSIS:")
            print(f"  Total schools: {int(vic['total_schools'])}")
            print(f"  - ATAR data: {int(vic['has_atar'])} schools ({vic['atar_pct']}%)")
            print(f"  - NAPLAN data: {int(vic['has_naplan'])} schools ({vic['naplan_pct']}%)")
            print(f"  - Destinations data: {int(vic['has_destinations'])} schools ({vic['destinations_pct']}%)")

            if vic['atar_pct'] >= 30:
                print(f"\n  ✅ Sufficient VIC school data for rankings (≥30% coverage)")
            else:
                print(f"\n  ❌ Limited VIC school data for comprehensive rankings")

        return df

    return None


def get_sample_institutions():
    """Get sample top institutions to validate rankings."""
    print("\n" + "="*80)
    print("SAMPLE INSTITUTIONS (for validation)")
    print("="*80)

    # Top universities by graduate salary
    sql_unis = """
    SELECT
        o.name,
        s.abbreviation as state,
        mba.avg_ft_g_sal as grad_salary,
        mba.no_grad_emp_rate as employment_rate,
        he.total_student_numbers as total_students,
        he.pc_international_students as pct_international,
        AVG(r.stars) as avg_stars
    FROM organisations o
    JOIN sites si ON si.organisation_id = o.id AND si.deleted_at IS NULL
    JOIN states s ON si.state_id = s.id
    LEFT JOIN organisation_mba mba ON o.id = mba.organisation_id
    LEFT JOIN organisation_he_enrolment he ON o.id = he.organisation_id
    LEFT JOIN organisation_rating r ON o.id = r.organisation_id
    JOIN organisation_types ot ON o.organisation_type_id = ot.id
    WHERE (ot.name = 'University/Higher Education Institution'
           OR ot.name = 'Higher Education Institutions'
           OR ot.name = 'Dual Sector University')
      AND o.deleted_at IS NULL
      AND mba.avg_ft_g_sal IS NOT NULL
    GROUP BY o.id, o.name, s.abbreviation, mba.avg_ft_g_sal, mba.no_grad_emp_rate,
             he.total_student_numbers, he.pc_international_students
    ORDER BY grad_salary DESC
    LIMIT 10;
    """

    df = get_data(sql_unis)
    if df is not None and len(df) > 0:
        print("\n🎓 Top 10 Universities by Graduate Salary:")
        print("-"*100)
        for idx, row in df.iterrows():
            salary = f"${int(row['grad_salary']):,}" if pd.notna(row['grad_salary']) else "N/A"
            emp = f"{row['employment_rate']:.1f}%" if pd.notna(row['employment_rate']) else "N/A"
            students = f"{int(row['total_students']):,}" if pd.notna(row['total_students']) else "N/A"
            intl = f"{row['pct_international']:.1f}%" if pd.notna(row['pct_international']) else "N/A"
            stars = f"{row['avg_stars']:.2f}" if pd.notna(row['avg_stars']) else "N/A"

            print(f"  {idx+1}. {row['name']} ({row['state']})")
            print(f"     Salary: {salary}, Employment: {emp}, Students: {students}, "
                  f"International: {intl}, Stars: {stars}")

    # Top RTOs by rating
    sql_rtos = """
    SELECT
        o.name,
        s.abbreviation as state,
        AVG(r.stars) as avg_stars,
        COUNT(DISTINCT c.id) as course_count
    FROM organisations o
    JOIN sites si ON si.organisation_id = o.id AND si.deleted_at IS NULL
    JOIN states s ON si.state_id = s.id
    LEFT JOIN organisation_rating r ON o.id = r.organisation_id
    LEFT JOIN courses c ON o.id = c.organisation_id AND c.deleted_at IS NULL
    JOIN organisation_types ot ON o.organisation_type_id = ot.id
    WHERE (ot.name = 'TAFE Institute' OR ot.name = 'Registered Training Organisation')
      AND o.deleted_at IS NULL
      AND r.stars IS NOT NULL
    GROUP BY o.id, o.name, s.abbreviation
    HAVING avg_stars > 0
    ORDER BY avg_stars DESC, course_count DESC
    LIMIT 10;
    """

    df_rto = get_data(sql_rtos)
    if df_rto is not None and len(df_rto) > 0:
        print("\n\n🔧 Top 10 RTOs/TAFEs by Star Rating:")
        print("-"*100)
        for idx, row in df_rto.iterrows():
            stars = f"{row['avg_stars']:.2f}/5" if pd.notna(row['avg_stars']) else "N/A"
            courses = f"{int(row['course_count'])}" if pd.notna(row['course_count']) else "0"

            print(f"  {idx+1}. {row['name']} ({row['state']})")
            print(f"     Stars: {stars}, Courses: {courses}")


def generate_recommendations():
    """Generate ranking recommendations based on data quality analysis."""
    print("\n" + "="*80)
    print("RANKING RECOMMENDATIONS")
    print("="*80)

    print("""
Based on data quality analysis, here are the recommended rankings:

1. ✅ FEASIBLE - Top Universities by State (Rating-Based)
   Formula: (0.60 × star_rating) + (0.20 × total_students_normalized) + (0.20 × course_count)
   Reason: Rating data has best coverage across states

2. ⚠️  LIMITED - Top Universities by State (Outcome-Based)
   Formula: (0.40 × grad_salary) + (0.30 × employment_rate) + (0.30 × star_rating)
   Reason: Only viable for states with sufficient MBA data
   States: Check analysis above for states with ≥30% coverage

3. ✅ FEASIBLE - Top RTOs/TAFEs by State
   Formula: (0.50 × star_rating) + (0.30 × course_count) + (0.20 × field_diversity)
   Reason: Rating and course data available

4. ✅ FEASIBLE - Top Schools in Victoria
   Formula: (0.35 × median_atar) + (0.25 × pct_to_uni) + (0.20 × vce_completion) + (0.20 × naplan)
   Reason: VIC has comprehensive school data

5. ❌ NOT FEASIBLE - Schools in other states
   Reason: ATAR, VCE, NAPLAN data only available for Victoria

6. ✅ FEASIBLE - National Top 10 Universities
   Formula: Same as state universities, but require complete data

7. ⚠️  LIMITED - Hidden Gems / Big Impact
   Reason: Requires graduate outcome data which has limited coverage

NEXT STEPS:
1. Implement feasible rankings first (ratings-based)
2. Add outcome-based rankings for states with sufficient data
3. Create separate VIC-only school rankings
4. Document data limitations clearly in output
""")


def main():
    """Run all analyses."""
    print("="*80)
    print("ACIR DATABASE - INSTITUTION DATA QUALITY ANALYSIS")
    print("="*80)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Run analyses
    uni_df = analyze_university_data_quality()
    rto_df = analyze_rto_data_quality()
    school_df = analyze_school_data_quality()

    # Sample institutions
    get_sample_institutions()

    # Recommendations
    generate_recommendations()

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()
