"""
ACIR Database Connector

Simple functions to get data from the ACIR MySQL database.
"""

import os
import pandas as pd
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error
from typing import Optional

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
    """
    Get data from database by passing SQL query directly.

    Args:
        sql: SQL query string

    Returns:
        pandas DataFrame with results, or None on error

    Example:
        df = get_data("SELECT * FROM careers LIMIT 10")
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    except Error as e:
        print(f"Database error: {e}")
        return None


def get_data_from_file(sql_file_path: str) -> Optional[pd.DataFrame]:
    """
    Get data from database by passing path to a .sql file.

    Args:
        sql_file_path: Path to .sql file containing the query

    Returns:
        pandas DataFrame with results, or None on error

    Example:
        df = get_data_from_file("acir_db/sql/course_careers.sql")
    """
    try:
        with open(sql_file_path, 'r') as f:
            sql = f.read()
        return get_data(sql)
    except FileNotFoundError:
        print(f"File not found: {sql_file_path}")
        return None


if __name__ == "__main__":
    # Example usage
    # print("Testing get_data()...")
    # df = get_data("SELECT COUNT(*) as total FROM careers")
    # if df is not None:
    #     print(f"Total careers: {df['total'].iloc[0]}")

    print("\nTesting get_data_from_file()...")
    df = get_data_from_file("acir_db/sql/course_careers.sql")
    if df is not None:
        print(f"Got {len(df)} rows")
        print(df.shape)

    courses = get_data("SELECT id as course_id, description as course_description FROM courses")
    courses_courses_careers = courses.merge(df, how='left', left_on = 'course_id', 
                                            right_on = 'course_id')


    dg = get_data_from_file("acir_db/sql/course_site_course.sql")
    courses_courses_careers = dg.merge(df, how = 'left',
                                       left_on = 'course_id', 
                                       right_on = 'course_id')
    

    courses_courses_careers_w_course_d = courses_courses_careers.merge(courses, how = 'left', 
                                                                       left_on = 'course_id', 
                                                                       right_on = 'course_id')
    



    courses_courses_careers_w_course_d.to_csv("acir-data/query_data/courses_joined_all.csv")