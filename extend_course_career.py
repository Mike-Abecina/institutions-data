"""
Extend course_career.csv with predicted course-career mappings.

Takes the existing course_career.csv and adds new mappings from
course_career_mappings_threshold_0.05.csv (model predictions).

For each predicted mapping:
- course_id: from predictions file
- anzsco: from predictions file
- career_id: looked up from career_course_full.csv using anzsco
- name: looked up from career_course_full.csv using anzsco
- description: looked up from career_course_full.csv using anzsco
- course_career_id: auto-generated sequential ID starting after max existing ID
- probability: from predictions file (stored for reference but not in final output)
"""

import pandas as pd

def load_existing_course_career():
    """Load existing course_career.csv."""
    print("=" * 70)
    print("EXTENDING COURSE_CAREER DATA WITH PREDICTIONS")
    print("=" * 70)

    print("\nLoading existing course_career.csv...")
    df = pd.read_csv('acir-data/query_data/course_career.csv')
    print(f"  Loaded {len(df):,} existing course-career mappings")
    print(f"  Columns: {list(df.columns)}")

    # Get max course_career_id
    max_id = df['course_career_id'].max()
    print(f"  Max course_career_id: {max_id:,}")

    return df, max_id

def load_anzsco_to_career_mapping():
    """Load ANZSCO to career details mapping from career_course_full.csv."""
    print("\nLoading ANZSCO to career mapping...")
    df = pd.read_csv('career_course_full.csv')

    # Drop rows where anzsco is NaN
    df = df[df['anzsco'].notna()].copy()

    # Create mapping from anzsco to career details
    # Use drop_duplicates to get unique anzsco entries
    df_unique = df[['career_id', 'name', 'anzsco', 'description']].drop_duplicates(subset=['anzsco'])

    print(f"  Found {len(df_unique):,} unique ANZSCO codes")

    # Create dictionary mapping
    anzsco_to_career = {}
    for _, row in df_unique.iterrows():
        anzsco = str(row['anzsco']).strip()
        anzsco_to_career[anzsco] = {
            'career_id': int(row['career_id']) if pd.notna(row['career_id']) else None,
            'name': row['name'] if pd.notna(row['name']) else None,
            'description': row['description'] if pd.notna(row['description']) else None
        }

    return anzsco_to_career

def load_predictions():
    """Load predicted course-career mappings."""
    print("\nLoading predictions...")
    df = pd.read_csv('course_career_mappings_threshold_0.05.csv')
    print(f"  Loaded {len(df):,} predicted course-career pairs")
    print(f"  Unique courses: {df['course_id'].nunique():,}")
    print(f"  Unique ANZSCO codes: {df['anzsco'].nunique():,}")

    return df

def check_for_duplicates(existing_df, predictions_df):
    """Check if any predictions already exist in the existing data."""
    print("\nChecking for duplicate course-career pairs...")

    # Create sets of (course_id, anzsco) tuples for comparison
    existing_pairs = set(zip(existing_df['course_id'], existing_df['anzsco'].astype(str)))
    predicted_pairs = set(zip(predictions_df['course_id'], predictions_df['anzsco'].astype(str)))

    # Find overlaps
    overlaps = existing_pairs.intersection(predicted_pairs)

    if overlaps:
        print(f"  Found {len(overlaps):,} duplicate pairs (will be skipped)")
        print(f"  Example duplicates: {list(overlaps)[:5]}")
    else:
        print(f"  No duplicates found - all predictions are new")

    return overlaps

def create_extended_mappings(existing_df, predictions_df, anzsco_to_career, max_id, overlaps):
    """Create new course_career records from predictions."""
    print("\nCreating new course_career records...")

    new_records = []
    next_id = max_id + 1
    skipped_no_career = 0
    skipped_duplicate = 0

    for idx, row in predictions_df.iterrows():
        course_id = int(row['course_id'])
        anzsco = str(row['anzsco']).strip()
        probability = float(row['probability'])

        # Skip duplicates
        if (course_id, anzsco) in overlaps:
            skipped_duplicate += 1
            continue

        # Lookup career details
        career_details = anzsco_to_career.get(anzsco)

        if not career_details or career_details['career_id'] is None:
            skipped_no_career += 1
            continue

        # Create new record matching the format of course_career.csv
        new_record = {
            'course_career_id': next_id,
            'course_id': course_id,
            'career_id': career_details['career_id'],
            'name': career_details['name'],
            'anzsco': anzsco,
            'description': career_details['description']
        }

        new_records.append(new_record)
        next_id += 1

        if (idx + 1) % 10000 == 0:
            print(f"  Processed {idx + 1:,}/{len(predictions_df):,} predictions...")

    print(f"\n  Created {len(new_records):,} new records")
    print(f"  Skipped {skipped_duplicate:,} duplicates")
    print(f"  Skipped {skipped_no_career:,} predictions with no career mapping")

    return pd.DataFrame(new_records)

def combine_and_save(existing_df, new_df, output_path):
    """Combine existing and new records, then save."""
    print("\nCombining existing and new records...")

    # Ensure column order matches
    columns = ['course_career_id', 'course_id', 'career_id', 'name', 'anzsco', 'description']
    existing_df = existing_df[columns]
    new_df = new_df[columns]

    # Combine
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)

    print(f"  Existing records: {len(existing_df):,}")
    print(f"  New records: {len(new_df):,}")
    print(f"  Total records: {len(combined_df):,}")

    # Statistics
    print(f"\nStatistics:")
    print(f"  Unique courses: {combined_df['course_id'].nunique():,}")
    print(f"  Unique careers: {combined_df['career_id'].nunique():,}")
    print(f"  Unique ANZSCO codes: {combined_df['anzsco'].nunique():,}")

    # Courses per career
    courses_per_career = combined_df.groupby('career_id').size()
    print(f"\n  Courses per career:")
    print(f"    Min: {courses_per_career.min()}")
    print(f"    Max: {courses_per_career.max()}")
    print(f"    Mean: {courses_per_career.mean():.1f}")

    # Careers per course
    careers_per_course = combined_df.groupby('course_id').size()
    print(f"\n  Careers per course:")
    print(f"    Min: {careers_per_course.min()}")
    print(f"    Max: {careers_per_course.max()}")
    print(f"    Mean: {careers_per_course.mean():.1f}")

    # Save
    print(f"\nSaving to: {output_path}")
    combined_df.to_csv(output_path, index=False)
    print(f"✓ Saved {len(combined_df):,} records")

    # Show sample of new records
    print("\nSample of new records (first 10):")
    print(new_df.head(10).to_string(index=False))

    return combined_df

def main():
    """Main function."""
    # Load existing data
    existing_df, max_id = load_existing_course_career()

    # Load ANZSCO to career mapping
    anzsco_to_career = load_anzsco_to_career_mapping()

    # Load predictions
    predictions_df = load_predictions()

    # Check for duplicates
    overlaps = check_for_duplicates(existing_df, predictions_df)

    # Create new records
    new_df = create_extended_mappings(existing_df, predictions_df, anzsco_to_career, max_id, overlaps)

    # Combine and save
    output_path = 'acir-data/query_data/extended_course_career.csv'
    combined_df = combine_and_save(existing_df, new_df, output_path)

    print("\n" + "=" * 70)
    print("✓ COMPLETE")
    print("=" * 70)
    print(f"\nExtended course_career data saved to: {output_path}")
    print(f"Added {len(new_df):,} new predicted course-career mappings")
    print(f"Total mappings: {len(combined_df):,}")

if __name__ == '__main__':
    main()
