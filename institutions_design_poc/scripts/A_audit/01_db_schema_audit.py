"""
01_db_schema_audit.py
=====================
Connect to the read-only MariaDB instance on AWS RDS and produce a full
schema audit of every table (or a chosen subset).

Outputs
-------
data/reports/schema_audit.xlsx   -- multi-sheet workbook:
    tables_list          : every table with row count
    <table>_schema       : one sheet per table with DESCRIBE + SHOW FULL COLUMNS
    create_statements    : raw CREATE TABLE DDL for every table
"""
from __future__ import annotations

import argparse
import sys
import textwrap
import time
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Project imports -- settings.py is expected to supply the DB connection
# helper and standard path constants.
# ---------------------------------------------------------------------------
try:
    from config.settings import get_db_connection, DATA_RAW, DATA_REPORTS
except ImportError:
    # Friendlier message when running outside the package context.
    print(
        "[ERROR] Could not import config.settings.\n"
        "  Make sure you run this script from the project root:\n"
        "    cd institutions_design_poc && python -m scripts.A_audit.01_db_schema_audit"
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _query(cursor, sql: str) -> list[dict]:
    """Execute *sql* and return every row as a dict."""
    cursor.execute(sql)
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _safe_sheet_name(name: str, max_len: int = 31) -> str:
    """Excel sheet names must be <= 31 chars and free of special chars."""
    clean = name.replace("/", "_").replace("\\", "_").replace(":", "_")
    return clean[:max_len]


# ---------------------------------------------------------------------------
# Core audit logic
# ---------------------------------------------------------------------------

def discover_tables(cursor) -> list[str]:
    """Return a sorted list of all table names in the current database."""
    rows = _query(cursor, "SHOW TABLES")
    # SHOW TABLES returns a single-column result whose header varies.
    key = list(rows[0].keys())[0] if rows else "Tables"
    return sorted(row[key] for row in rows)


def audit_table(cursor, table: str) -> dict:
    """Gather schema metadata and row count for *table*."""
    describe = _query(cursor, f"DESCRIBE `{table}`")
    full_cols = _query(cursor, f"SHOW FULL COLUMNS FROM `{table}`")

    cursor.execute(f"SELECT COUNT(*) AS cnt FROM `{table}`")
    row_count = cursor.fetchone()[0]

    cursor.execute(f"SHOW CREATE TABLE `{table}`")
    create_stmt = cursor.fetchone()[1]

    return {
        "describe": describe,
        "full_columns": full_cols,
        "row_count": row_count,
        "create_statement": create_stmt,
    }


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

def write_report(
    tables: list[str],
    audits: dict[str, dict],
    output_path: Path,
) -> None:
    """Write the multi-sheet Excel workbook."""

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # -- Sheet 1: tables_list -----------------------------------------
        summary_rows = []
        for t in tables:
            info = audits.get(t)
            summary_rows.append({
                "table_name": t,
                "row_count": info["row_count"] if info else "ERROR",
                "column_count": len(info["describe"]) if info else "ERROR",
            })
        pd.DataFrame(summary_rows).to_excel(
            writer, sheet_name="tables_list", index=False,
        )
        print(f"  [+] Sheet 'tables_list' written ({len(summary_rows)} tables)")

        # -- Per-table sheets ---------------------------------------------
        for t in tables:
            info = audits.get(t)
            if info is None:
                continue

            # DESCRIBE sheet
            sheet = _safe_sheet_name(f"{t}_schema")
            pd.DataFrame(info["full_columns"]).to_excel(
                writer, sheet_name=sheet, index=False,
            )
            print(f"  [+] Sheet '{sheet}' written ({len(info['full_columns'])} columns)")

        # -- CREATE TABLE statements sheet --------------------------------
        create_rows = []
        for t in tables:
            info = audits.get(t)
            if info is None:
                continue
            create_rows.append({
                "table_name": t,
                "create_statement": info["create_statement"],
            })
        pd.DataFrame(create_rows).to_excel(
            writer, sheet_name="create_statements", index=False,
        )
        print(f"  [+] Sheet 'create_statements' written ({len(create_rows)} tables)")

    print(f"\n  Report saved to {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit the schema of every table in the target MariaDB database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples
            --------
              python -m scripts.A_audit.01_db_schema_audit
              python -m scripts.A_audit.01_db_schema_audit --tables organisations users
        """),
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=None,
        help="Audit only these tables (default: all tables in the database).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Override the output file path (default: data/reports/schema_audit.xlsx).",
    )
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else Path(DATA_REPORTS) / "schema_audit.xlsx"

    # ---- Connect --------------------------------------------------------
    print("[01_db_schema_audit] Connecting to database ...")
    try:
        conn = get_db_connection()
    except Exception as exc:
        print(
            f"\n[ERROR] Could not connect to the database.\n"
            f"  {type(exc).__name__}: {exc}\n\n"
            f"  Troubleshooting tips:\n"
            f"    1. Check that DATABASE_HOST, DATABASE_PORT, DATABASE_USER,\n"
            f"       DATABASE_PASSWORD, and DATABASE_NAME are set correctly in .env\n"
            f"    2. Ensure you are on a network / VPN that can reach the RDS instance.\n"
            f"    3. Verify the security group allows inbound traffic from your IP.\n"
        )
        sys.exit(1)

    try:
        cursor = conn.cursor()

        # ---- Discover tables --------------------------------------------
        print("[01_db_schema_audit] Discovering tables ...")
        all_tables = discover_tables(cursor)
        print(f"  Found {len(all_tables)} tables: {', '.join(all_tables[:10])}"
              f"{'...' if len(all_tables) > 10 else ''}")

        if args.tables:
            missing = set(args.tables) - set(all_tables)
            if missing:
                print(f"  [WARNING] Requested tables not found in DB: {missing}")
            tables = [t for t in args.tables if t in all_tables]
        else:
            tables = all_tables

        if not tables:
            print("[ERROR] No tables to audit. Exiting.")
            sys.exit(1)

        # ---- Audit each table -------------------------------------------
        audits: dict[str, dict] = {}
        total = len(tables)
        start = time.time()

        for idx, table in enumerate(tables, 1):
            print(f"[01_db_schema_audit] ({idx}/{total}) Auditing table '{table}' ...", end=" ")
            try:
                audits[table] = audit_table(cursor, table)
                info = audits[table]
                print(f"OK  ({info['row_count']:,} rows, {len(info['describe'])} cols)")
            except Exception as exc:
                print(f"FAILED  ({type(exc).__name__}: {exc})")
                audits[table] = None

        elapsed = time.time() - start
        print(f"\n[01_db_schema_audit] Audit complete in {elapsed:.1f}s")

        # ---- Write report -----------------------------------------------
        print(f"[01_db_schema_audit] Writing report to {output_path} ...")
        write_report(tables, audits, output_path)

    finally:
        conn.close()
        print("[01_db_schema_audit] Database connection closed.")

    print("[01_db_schema_audit] Done.")


if __name__ == "__main__":
    main()
