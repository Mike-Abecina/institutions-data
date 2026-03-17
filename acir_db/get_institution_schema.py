"""
Extract ACIR Database Schema with Inferred Descriptions

Reads all table/column information from INFORMATION_SCHEMA and outputs
JSON with inferred descriptions for each column to help identify useful
fields for institution ranking and analysis.
"""

import os
import json
import pandas as pd
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


def infer_column_description(column_name: str, data_type: str, table_name: str) -> str:
    """
    Infer a human-readable description based on column name, type, and table context.

    Args:
        column_name: Name of the column
        data_type: MySQL data type (e.g., 'int', 'varchar', 'text')
        table_name: Name of the table the column belongs to

    Returns:
        Inferred description string
    """
    col_lower = column_name.lower()

    # ID fields
    if col_lower.endswith('_id') or col_lower == 'id':
        entity = col_lower.replace('_id', '').replace('_', ' ').title()
        return f"Unique identifier for {entity}"

    # Name fields
    if 'name' in col_lower:
        entity = col_lower.replace('_name', '').replace('name', '').replace('_', ' ').strip().title()
        if entity:
            return f"Name of the {entity}"
        return "Name"

    # Institution-specific fields
    if 'institution' in col_lower or 'organisation' in col_lower or 'university' in col_lower:
        if 'type' in col_lower:
            return "Type/category of institution (e.g., University, TAFE, Private College)"
        if 'state' in col_lower or 'location' in col_lower:
            return "Geographic location of institution"
        if 'rank' in col_lower:
            return "Institution ranking or rating metric"
        if 'impact' in col_lower or 'score' in col_lower:
            return "Impact or quality score metric"
        if 'enrol' in col_lower or 'student' in col_lower:
            return "Student enrollment or population metric"

    # Course fields
    if 'course' in col_lower:
        if 'description' in col_lower:
            return "Detailed description of the course"
        if 'level' in col_lower:
            return "Academic level (e.g., Bachelor, Postgraduate, Diploma)"
        if 'duration' in col_lower:
            return "Length of time to complete the course"
        if 'fee' in col_lower or 'cost' in col_lower:
            return "Course fees or tuition costs"

    # Career/job fields
    if 'career' in col_lower or 'job' in col_lower or 'anzsco' in col_lower:
        if 'code' in col_lower or 'anzsco' in col_lower:
            return "ANZSCO occupation classification code"
        if 'title' in col_lower:
            return "Job or career title"
        if 'description' in col_lower:
            return "Description of the career or occupation"

    # Common metadata fields
    if col_lower in ['created_at', 'created', 'date_created']:
        return "Timestamp when record was created"
    if col_lower in ['updated_at', 'updated', 'modified', 'date_modified']:
        return "Timestamp when record was last updated"
    if col_lower in ['deleted_at', 'deleted']:
        return "Timestamp when record was deleted (soft delete)"
    if col_lower == 'active' or col_lower == 'is_active':
        return "Whether the record is currently active"
    if col_lower == 'status':
        return "Current status of the record"

    # Contact/address fields
    if 'email' in col_lower:
        return "Email address"
    if 'phone' in col_lower or 'mobile' in col_lower:
        return "Phone or mobile number"
    if 'address' in col_lower:
        return "Physical address"
    if 'website' in col_lower or 'url' in col_lower:
        return "Website URL"
    if 'postcode' in col_lower or 'zip' in col_lower:
        return "Postal/ZIP code"
    if 'state' in col_lower and 'address' in table_name.lower():
        return "State or province"
    if 'country' in col_lower:
        return "Country"

    # Description/text fields
    if 'description' in col_lower or 'desc' in col_lower:
        return "Descriptive text"
    if 'notes' in col_lower or 'comment' in col_lower:
        return "Additional notes or comments"

    # Numeric metrics
    if data_type in ['int', 'bigint', 'decimal', 'float', 'double']:
        if 'count' in col_lower:
            return "Count or total number"
        if 'amount' in col_lower or 'value' in col_lower:
            return "Numeric amount or value"
        if 'percent' in col_lower or 'rate' in col_lower:
            return "Percentage or rate value"
        if 'score' in col_lower or 'rating' in col_lower:
            return "Score or rating value"
        return "Numeric value"

    # Boolean fields
    if data_type in ['tinyint', 'boolean', 'bit']:
        if col_lower.startswith('is_') or col_lower.startswith('has_'):
            feature = col_lower.replace('is_', '').replace('has_', '').replace('_', ' ')
            return f"Whether {feature}"
        return "Boolean flag (true/false)"

    # Date/time fields
    if 'date' in data_type or 'time' in data_type:
        if 'start' in col_lower:
            return "Start date/time"
        if 'end' in col_lower or 'finish' in col_lower:
            return "End date/time"
        return "Date/time value"

    # Text fields
    if data_type in ['text', 'longtext', 'mediumtext']:
        return "Long text content"
    if data_type.startswith('varchar'):
        return "Short text field"

    # Default fallback
    return f"Column of type {data_type}"


def get_schema_with_descriptions() -> Optional[Dict[str, List[Dict]]]:
    """
    Extract all table schemas from the database with inferred descriptions.

    Returns:
        Dictionary mapping table names to lists of column info dicts:
        {
            "table_name": [
                {"column_name": "id", "description": "Unique identifier..."},
                ...
            ]
        }
    """
    sql_file_path = "acir_db/sql/table_schema.sql"

    try:
        # Read SQL query
        with open(sql_file_path, 'r') as f:
            sql = f.read()

        # Execute query
        conn = mysql.connector.connect(**DB_CONFIG)
        df = pd.read_sql(sql, conn)
        conn.close()

        print(f"Retrieved schema for {len(df)} columns across {df['TABLE_NAME'].nunique()} tables")

        # Group by table name
        schema_dict = defaultdict(list)

        for _, row in df.iterrows():
            table_name = row['TABLE_NAME']
            column_name = row['COLUMN_NAME']
            data_type = row['DATA_TYPE']

            description = infer_column_description(column_name, data_type, table_name)

            schema_dict[table_name].append({
                "column_name": column_name,
                "description": description,
                "data_type": data_type  # Include data type for reference
            })

        return dict(schema_dict)

    except FileNotFoundError:
        print(f"File not found: {sql_file_path}")
        return None
    except Error as e:
        print(f"Database error: {e}")
        return None


if __name__ == "__main__":
    print("="*80)
    print("EXTRACTING ACIR DATABASE SCHEMA")
    print("="*80)

    schema = get_schema_with_descriptions()

    if schema:
        # Output JSON
        output_file = "acir_db/institution_schema.json"
        with open(output_file, 'w') as f:
            json.dump(schema, f, indent=2)

        print(f"\nSchema saved to: {output_file}")
        print(f"Total tables: {len(schema)}")

        # Print summary
        print("\nTable Summary:")
        print("-" * 80)
        for table_name, columns in sorted(schema.items()):
            print(f"  {table_name}: {len(columns)} columns")

        # Print sample (first table)
        if schema:
            first_table = list(schema.keys())[0]
            print(f"\nSample - {first_table}:")
            print("-" * 80)
            for col in schema[first_table][:5]:  # First 5 columns
                print(f"  • {col['column_name']}: {col['description']}")
            if len(schema[first_table]) > 5:
                print(f"  ... and {len(schema[first_table]) - 5} more columns")
    else:
        print("Failed to extract schema")
