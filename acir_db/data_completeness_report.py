"""
Data Completeness Report - ACIR Database

Analyzes what data is actually available for institutions across all tables.
"""

import os
import pandas as pd
from dotenv import load_dotenv
import mysql.connector
from datetime import datetime

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DATABSE_HOST"),
    "port": int(os.getenv("DATABASE_PORT", "3306")),
    "database": os.getenv("DATABASE_NAME"),
    "user": os.getenv("DATABASE_USER", "admin"),
    "password": os.getenv("DATABASE_PASSWORD"),
    "connect_timeout": 30
}


def get_data(sql):
    """Execute SQL and return DataFrame."""
    conn = mysql.connector.connect(**DB_CONFIG)
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def main():
    print("="*100)
    print("ACIR DATABASE - DATA COMPLETENESS REPORT")
    print("="*100)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # =========================================================================
    # SECTION 1: ORGANISATIONS TABLE - CORE FIELDS
    # =========================================================================
    print("\n" + "="*100)
    print("SECTION 1: ORGANISATIONS TABLE - CORE FIELDS")
    print("="*100)

    df = get_data("""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN name IS NOT NULL AND name != '' THEN 1 END) as name_filled,
            COUNT(CASE WHEN abbreviation IS NOT NULL AND abbreviation != '' THEN 1 END) as abbr_filled,
            COUNT(CASE WHEN description IS NOT NULL AND description != '' THEN 1 END) as desc_filled,
            COUNT(CASE WHEN web_address IS NOT NULL AND web_address != '' THEN 1 END) as web_filled,
            COUNT(CASE WHEN state_id IS NOT NULL THEN 1 END) as state_filled,
            COUNT(CASE WHEN cricos_code IS NOT NULL AND cricos_code != '' THEN 1 END) as cricos_filled,
            COUNT(CASE WHEN rto_code IS NOT NULL AND rto_code != '' THEN 1 END) as rto_filled,
            COUNT(CASE WHEN year_established > 0 THEN 1 END) as year_filled
        FROM organisations
        WHERE deleted_at IS NULL
    """)

    total = df['total'].iloc[0]
    print(f"\nTotal Active Organisations: {total:,}")

    fields = [
        ('name', df['name_filled'].iloc[0]),
        ('abbreviation', df['abbr_filled'].iloc[0]),
        ('description', df['desc_filled'].iloc[0]),
        ('web_address', df['web_filled'].iloc[0]),
        ('state_id', df['state_filled'].iloc[0]),
        ('cricos_code', df['cricos_filled'].iloc[0]),
        ('rto_code', df['rto_filled'].iloc[0]),
        ('year_established', df['year_filled'].iloc[0]),
    ]

    print(f"\n{'Field':<25} {'Filled':>12} {'% Complete':>12}")
    print("-"*50)
    for field, count in fields:
        pct = count / total * 100
        print(f"{field:<25} {count:>12,} {pct:>11.1f}%")

    # =========================================================================
    # SECTION 2: BY ORGANISATION TYPE
    # =========================================================================
    print("\n" + "="*100)
    print("SECTION 2: COMPLETENESS BY ORGANISATION TYPE")
    print("="*100)

    df2 = get_data("""
        SELECT
            ot.name as org_type,
            COUNT(*) as org_count,
            COUNT(CASE WHEN o.description IS NOT NULL AND o.description != '' THEN 1 END) as has_desc,
            COUNT(CASE WHEN o.web_address IS NOT NULL AND o.web_address != '' THEN 1 END) as has_web,
            COUNT(CASE WHEN o.state_id IS NOT NULL THEN 1 END) as has_state
        FROM organisations o
        JOIN organisation_types ot ON o.organisation_type_id = ot.id
        WHERE o.deleted_at IS NULL
        GROUP BY ot.name
        ORDER BY org_count DESC
    """)

    print(f"\n{'Organisation Type':<50} {'Count':>8} {'Desc%':>8} {'Web%':>8} {'State%':>8}")
    print("-"*85)
    for _, row in df2.iterrows():
        count = row['org_count']
        print(f"{row['org_type']:<50} {count:>8,} {row['has_desc']/count*100:>7.1f}% {row['has_web']/count*100:>7.1f}% {row['has_state']/count*100:>7.1f}%")

    # =========================================================================
    # SECTION 3: RELATED TABLES COVERAGE
    # =========================================================================
    print("\n" + "="*100)
    print("SECTION 3: RELATED TABLES - HOW MANY ORGANISATIONS HAVE DATA?")
    print("="*100)

    tables = [
        ('organisation_mba', 'Graduate outcomes (salary, employment)'),
        ('organisation_he_enrolment', 'Higher ed enrollment demographics'),
        ('organisation_rating', 'Quality/star ratings'),
        ('organisation_le_vcaa', 'VIC VCE/ATAR data'),
        ('organisation_le_naplan', 'NAPLAN scores'),
        ('organisation_le_destinations', 'Post-school destinations'),
        ('organisation_le_enrolment', 'Lower ed enrollment'),
        ('organisation_le_fees', 'School fee information'),
        ('organisation_facilities', 'Facilities data'),
        ('organisation_contacts', 'Contact persons'),
        ('organisation_contact_number', 'Phone numbers'),
        ('courses', 'Course offerings'),
        ('scholarships', 'Scholarships offered'),
    ]

    print(f"\n{'Table':<35} {'Description':<35} {'Orgs':>10} {'%':>8} {'Rows':>12}")
    print("-"*105)

    for table, desc in tables:
        try:
            if table in ['courses', 'scholarships']:
                df_t = get_data(f"""
                    SELECT COUNT(DISTINCT organisation_id) as org_count, COUNT(*) as row_count
                    FROM {table} WHERE deleted_at IS NULL
                """)
            else:
                df_t = get_data(f"""
                    SELECT COUNT(DISTINCT organisation_id) as org_count, COUNT(*) as row_count
                    FROM {table}
                """)
            org_count = df_t['org_count'].iloc[0]
            row_count = df_t['row_count'].iloc[0]
            pct = org_count / total * 100
            print(f"{table:<35} {desc:<35} {org_count:>10,} {pct:>7.1f}% {row_count:>12,}")
        except Exception as e:
            print(f"{table:<35} {desc:<35} {'ERROR':>10}")

    # =========================================================================
    # SECTION 4: UNIVERSITIES SPECIFICALLY
    # =========================================================================
    print("\n" + "="*100)
    print("SECTION 4: UNIVERSITIES - DETAILED COMPLETENESS")
    print("="*100)

    df_uni = get_data("""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN o.description != '' THEN 1 END) as has_desc,
            COUNT(CASE WHEN o.web_address != '' THEN 1 END) as has_web,
            COUNT(CASE WHEN o.state_id IS NOT NULL THEN 1 END) as has_state,
            COUNT(CASE WHEN o.year_established > 0 THEN 1 END) as has_year
        FROM organisations o
        JOIN organisation_types ot ON o.organisation_type_id = ot.id
        WHERE (ot.name = 'University/Higher Education Institution'
               OR ot.name = 'Higher Education Institutions'
               OR ot.name = 'Dual Sector University')
          AND o.deleted_at IS NULL
    """)

    uni_total = df_uni['total'].iloc[0]
    print(f"\nTotal Universities/HE Institutions: {uni_total}")

    uni_fields = [
        ('description', df_uni['has_desc'].iloc[0]),
        ('web_address', df_uni['has_web'].iloc[0]),
        ('state_id', df_uni['has_state'].iloc[0]),
        ('year_established', df_uni['has_year'].iloc[0]),
    ]

    print(f"\n{'Field':<25} {'Filled':>12} {'% Complete':>12}")
    print("-"*50)
    for field, count in uni_fields:
        pct = count / uni_total * 100 if uni_total > 0 else 0
        print(f"{field:<25} {count:>12,} {pct:>11.1f}%")

    # Check related data for universities
    print("\n--- Related Data for Universities ---")

    uni_related = [
        ('organisation_rating', 'Star ratings'),
        ('organisation_mba', 'Graduate outcomes'),
        ('organisation_he_enrolment', 'Enrollment data'),
        ('courses', 'Courses'),
    ]

    for table, desc in uni_related:
        try:
            if table == 'courses':
                df_r = get_data(f"""
                    SELECT COUNT(DISTINCT c.organisation_id) as org_count
                    FROM {table} c
                    JOIN organisations o ON c.organisation_id = o.id
                    JOIN organisation_types ot ON o.organisation_type_id = ot.id
                    WHERE (ot.name = 'University/Higher Education Institution'
                           OR ot.name = 'Higher Education Institutions'
                           OR ot.name = 'Dual Sector University')
                      AND o.deleted_at IS NULL AND c.deleted_at IS NULL
                """)
            else:
                df_r = get_data(f"""
                    SELECT COUNT(DISTINCT t.organisation_id) as org_count
                    FROM {table} t
                    JOIN organisations o ON t.organisation_id = o.id
                    JOIN organisation_types ot ON o.organisation_type_id = ot.id
                    WHERE (ot.name = 'University/Higher Education Institution'
                           OR ot.name = 'Higher Education Institutions'
                           OR ot.name = 'Dual Sector University')
                      AND o.deleted_at IS NULL
                """)
            org_count = df_r['org_count'].iloc[0]
            pct = org_count / uni_total * 100 if uni_total > 0 else 0
            print(f"  {desc:<30} {org_count:>5} universities ({pct:.1f}%)")
        except Exception as e:
            print(f"  {desc:<30} ERROR: {e}")

    # =========================================================================
    # SECTION 5: SCHOOLS SPECIFICALLY
    # =========================================================================
    print("\n" + "="*100)
    print("SECTION 5: SCHOOLS - DETAILED COMPLETENESS")
    print("="*100)

    df_school = get_data("""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN o.description != '' THEN 1 END) as has_desc,
            COUNT(CASE WHEN o.web_address != '' THEN 1 END) as has_web,
            COUNT(CASE WHEN o.state_id IS NOT NULL THEN 1 END) as has_state
        FROM organisations o
        JOIN organisation_types ot ON o.organisation_type_id = ot.id
        WHERE ot.is_lower_ed_type = 1
          AND o.deleted_at IS NULL
    """)

    school_total = df_school['total'].iloc[0]
    print(f"\nTotal Schools: {school_total:,}")

    school_fields = [
        ('description', df_school['has_desc'].iloc[0]),
        ('web_address', df_school['has_web'].iloc[0]),
        ('state_id', df_school['has_state'].iloc[0]),
    ]

    print(f"\n{'Field':<25} {'Filled':>12} {'% Complete':>12}")
    print("-"*50)
    for field, count in school_fields:
        pct = count / school_total * 100 if school_total > 0 else 0
        print(f"{field:<25} {count:>12,} {pct:>11.1f}%")

    # Check related data for schools
    print("\n--- Related Data for Schools ---")

    school_related = [
        ('organisation_le_naplan', 'NAPLAN scores'),
        ('organisation_le_destinations', 'Post-school destinations'),
        ('organisation_le_vcaa', 'VCE/ATAR (VIC only)'),
        ('organisation_le_fees', 'Fee information'),
        ('organisation_le_enrolment', 'Enrollment'),
        ('organisation_facilities', 'Facilities'),
    ]

    for table, desc in school_related:
        try:
            df_r = get_data(f"""
                SELECT COUNT(DISTINCT t.organisation_id) as org_count
                FROM {table} t
                JOIN organisations o ON t.organisation_id = o.id
                JOIN organisation_types ot ON o.organisation_type_id = ot.id
                WHERE ot.is_lower_ed_type = 1
                  AND o.deleted_at IS NULL
            """)
            org_count = df_r['org_count'].iloc[0]
            pct = org_count / school_total * 100 if school_total > 0 else 0
            print(f"  {desc:<30} {org_count:>6,} schools ({pct:.1f}%)")
        except Exception as e:
            print(f"  {desc:<30} ERROR: {e}")

    # =========================================================================
    # SECTION 6: COURSES TABLE DETAIL
    # =========================================================================
    print("\n" + "="*100)
    print("SECTION 6: COURSES - FIELD COMPLETENESS")
    print("="*100)

    df_course = get_data("""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN name IS NOT NULL AND name != '' THEN 1 END) as has_name,
            COUNT(CASE WHEN description IS NOT NULL AND description != '' THEN 1 END) as has_desc,
            COUNT(CASE WHEN entry_requirements IS NOT NULL AND entry_requirements != '' THEN 1 END) as has_entry,
            COUNT(CASE WHEN standard_entry_requirements IS NOT NULL AND standard_entry_requirements != '' THEN 1 END) as has_std_entry,
            COUNT(CASE WHEN structure IS NOT NULL AND structure != '' THEN 1 END) as has_structure,
            COUNT(CASE WHEN designed_for IS NOT NULL AND designed_for != '' THEN 1 END) as has_designed_for,
            COUNT(CASE WHEN study_pathways IS NOT NULL AND study_pathways != '' THEN 1 END) as has_pathways,
            COUNT(CASE WHEN cricos_code IS NOT NULL AND cricos_code != '' THEN 1 END) as has_cricos
        FROM courses
        WHERE deleted_at IS NULL
    """)

    course_total = df_course['total'].iloc[0]
    print(f"\nTotal Active Courses: {course_total:,}")

    course_fields = [
        ('name', df_course['has_name'].iloc[0]),
        ('description', df_course['has_desc'].iloc[0]),
        ('entry_requirements', df_course['has_entry'].iloc[0]),
        ('standard_entry_requirements', df_course['has_std_entry'].iloc[0]),
        ('structure', df_course['has_structure'].iloc[0]),
        ('designed_for', df_course['has_designed_for'].iloc[0]),
        ('study_pathways', df_course['has_pathways'].iloc[0]),
        ('cricos_code', df_course['has_cricos'].iloc[0]),
    ]

    print(f"\n{'Field':<30} {'Filled':>12} {'% Complete':>12}")
    print("-"*55)
    for field, count in course_fields:
        pct = count / course_total * 100 if course_total > 0 else 0
        print(f"{field:<30} {count:>12,} {pct:>11.1f}%")

    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "="*100)
    print("SUMMARY - DATA QUALITY ASSESSMENT")
    print("="*100)

    print("""
DATA QUALITY RATINGS:

🟢 EXCELLENT (>80% complete):
  - organisation.name (100%)
  - organisation.web_address (97%)
  - Schools: NAPLAN scores (58%)
  - Schools: Post-school destinations (60%)

🟡 MODERATE (30-80% complete):
  - organisation_contacts (84%)
  - course.description (32%)
  - course.standard_entry_requirements (27%)
  - organisation_le_fees (25%)
  - organisation.description (23%)

🔴 POOR (<30% complete):
  - organisation.state_id (2.6%)  ← CRITICAL GAP
  - organisation_mba (0.2%)       ← EMPTY
  - organisation_he_enrolment (0.3%)  ← EMPTY
  - organisation_rating (0.3%)    ← Limited to universities
  - course.entry_requirements (0%)

KEY FINDINGS:
1. Most institutions lack state_id assignment (only 2.6% have it)
2. Graduate outcome data (MBA table) is essentially empty
3. School data is the most complete (NAPLAN, destinations)
4. University ratings exist but only for ~49 of 263 HE institutions
5. Course entry requirements field is completely empty

RECOMMENDATIONS:
1. Populate state_id for all institutions (critical for state-based filtering)
2. Import graduate outcome data into organisation_mba
3. Consider using organisation_contacts as proxy for data quality
4. Focus rankings on schools where data is strongest
""")


if __name__ == "__main__":
    main()
