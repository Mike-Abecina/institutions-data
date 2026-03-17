"""
02_db_data_sample.py
====================
Profile the data inside a single table (default: ``organisations``).

For every column the script calculates:
  - total row count, non-null count, distinct count, null percentage
  - top-10 value distribution
  - 20 random sample values  (configurable via --sample-size)
  - average character length for text/varchar columns

It also exports the first 100 rows as a full sample.

Outputs
-------
data/reports/data_sample.xlsx         -- multi-sheet workbook
data/raw/<table>_sample.csv           -- first 100 rows as CSV
"""
from __future__ import annotations

import argparse
import sys
import textwrap
import time
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Project imports
# ---------------------------------------------------------------------------
try:
    from config.settings import get_db_connection, DATA_RAW, DATA_REPORTS
except ImportError:
    print(
        "[ERROR] Could not import config.settings.\n"
        "  Run from the project root:  cd institutions_design_poc && "
        "python -m scripts.A_audit.02_db_data_sample"
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _query(cursor, sql: str, params=None) -> list[dict]:
    cursor.execute(sql, params or ())
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _scalar(cursor, sql: str, params=None):
    cursor.execute(sql, params or ())
    return cursor.fetchone()[0]


def _safe_sheet(name: str, max_len: int = 31) -> str:
    clean = name.replace("/", "_").replace("\\", "_").replace(":", "_")
    return clean[:max_len]


# ---------------------------------------------------------------------------
# Column-level profiling
# ---------------------------------------------------------------------------

def _is_text_type(col_type: str) -> bool:
    """Heuristic check for text-like column types."""
    t = col_type.lower()
    return any(kw in t for kw in ("char", "text", "blob", "json", "enum", "set"))


def profile_column(
    cursor,
    table: str,
    col_name: str,
    col_type: str,
    total_rows: int,
    sample_size: int,
) -> dict:
    """Return a profiling dict for a single column."""
    esc = f"`{col_name}`"
    tbl = f"`{table}`"

    # Counts
    non_null = _scalar(cursor, f"SELECT COUNT({esc}) FROM {tbl}")
    distinct = _scalar(cursor, f"SELECT COUNT(DISTINCT {esc}) FROM {tbl}")
    null_count = total_rows - non_null
    null_pct = (null_count / total_rows * 100) if total_rows else 0.0

    # Top-10 distribution
    top10_rows = _query(
        cursor,
        f"SELECT {esc} AS val, COUNT(*) AS cnt "
        f"FROM {tbl} WHERE {esc} IS NOT NULL "
        f"GROUP BY {esc} ORDER BY cnt DESC LIMIT 10",
    )
    top10 = {str(r["val"]): r["cnt"] for r in top10_rows}

    # Random sample
    sample_rows = _query(
        cursor,
        f"SELECT {esc} AS val FROM {tbl} "
        f"WHERE {esc} IS NOT NULL ORDER BY RAND() LIMIT %s",
        (sample_size,),
    )
    samples = [str(r["val"]) for r in sample_rows]

    # Average length (text columns only)
    avg_len = None
    if _is_text_type(col_type) and non_null > 0:
        avg_len = _scalar(
            cursor,
            f"SELECT ROUND(AVG(CHAR_LENGTH({esc})), 1) FROM {tbl} WHERE {esc} IS NOT NULL",
        )

    # Min / max for numeric or date columns (best-effort)
    min_val = max_val = None
    if non_null > 0:
        try:
            min_val = _scalar(cursor, f"SELECT MIN({esc}) FROM {tbl}")
            max_val = _scalar(cursor, f"SELECT MAX({esc}) FROM {tbl}")
        except Exception:
            pass  # some column types don't support MIN/MAX

    return {
        "column": col_name,
        "type": col_type,
        "total_rows": total_rows,
        "non_null": non_null,
        "null_count": null_count,
        "null_pct": round(null_pct, 2),
        "distinct": distinct,
        "avg_length": avg_len,
        "min": str(min_val) if min_val is not None else None,
        "max": str(max_val) if max_val is not None else None,
        "top_10_values": top10,
        "sample_values": samples,
    }


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

def write_report(
    table: str,
    profiles: list[dict],
    full_sample_df: pd.DataFrame,
    xlsx_path: Path,
    csv_path: Path,
) -> None:
    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        # -- Summary sheet ------------------------------------------------
        summary_rows = []
        for p in profiles:
            summary_rows.append({
                "column": p["column"],
                "type": p["type"],
                "total_rows": p["total_rows"],
                "non_null": p["non_null"],
                "null_count": p["null_count"],
                "null_pct": p["null_pct"],
                "distinct": p["distinct"],
                "avg_length": p["avg_length"],
                "min": p["min"],
                "max": p["max"],
            })
        pd.DataFrame(summary_rows).to_excel(
            writer, sheet_name="column_summary", index=False,
        )
        print(f"  [+] Sheet 'column_summary' written ({len(summary_rows)} columns)")

        # -- Top-10 distribution sheet ------------------------------------
        dist_rows = []
        for p in profiles:
            for val, cnt in p["top_10_values"].items():
                dist_rows.append({
                    "column": p["column"],
                    "value": val,
                    "count": cnt,
                })
        pd.DataFrame(dist_rows).to_excel(
            writer, sheet_name="top10_distribution", index=False,
        )
        print(f"  [+] Sheet 'top10_distribution' written ({len(dist_rows)} rows)")

        # -- Samples sheet ------------------------------------------------
        sample_rows = []
        for p in profiles:
            for i, val in enumerate(p["sample_values"], 1):
                sample_rows.append({
                    "column": p["column"],
                    "sample_number": i,
                    "value": val,
                })
        pd.DataFrame(sample_rows).to_excel(
            writer, sheet_name="random_samples", index=False,
        )
        print(f"  [+] Sheet 'random_samples' written ({len(sample_rows)} rows)")

        # -- Full sample sheet (first 100 rows) ---------------------------
        sheet_name = _safe_sheet(f"{table}_first100")
        full_sample_df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"  [+] Sheet '{sheet_name}' written ({len(full_sample_df)} rows)")

    print(f"  Report saved to {xlsx_path}")

    # CSV
    full_sample_df.to_csv(csv_path, index=False)
    print(f"  CSV sample saved to {csv_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Profile every column in a database table.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples
            --------
              python -m scripts.A_audit.02_db_data_sample
              python -m scripts.A_audit.02_db_data_sample --table organisations --sample-size 50
        """),
    )
    parser.add_argument(
        "--table",
        type=str,
        default="organisations",
        help="Table to profile (default: organisations).",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=20,
        help="Number of random sample values per column (default: 20).",
    )
    parser.add_argument(
        "--output-xlsx",
        type=str,
        default=None,
        help="Override XLSX output path.",
    )
    parser.add_argument(
        "--output-csv",
        type=str,
        default=None,
        help="Override CSV output path.",
    )
    args = parser.parse_args()

    table = args.table
    sample_size = args.sample_size

    xlsx_path = (
        Path(args.output_xlsx)
        if args.output_xlsx
        else Path(DATA_REPORTS) / "data_sample.xlsx"
    )
    csv_path = (
        Path(args.output_csv)
        if args.output_csv
        else Path(DATA_RAW) / f"{table}_sample.csv"
    )

    # ---- Connect --------------------------------------------------------
    print(f"[02_db_data_sample] Connecting to database ...")
    try:
        conn = get_db_connection()
    except Exception as exc:
        print(
            f"\n[ERROR] Could not connect to the database.\n"
            f"  {type(exc).__name__}: {exc}\n\n"
            f"  Check .env settings and network access.\n"
        )
        sys.exit(1)

    try:
        cursor = conn.cursor()

        # ---- Verify table exists ----------------------------------------
        cursor.execute("SHOW TABLES")
        all_tables = [row[0] for row in cursor.fetchall()]
        if table not in all_tables:
            print(
                f"[ERROR] Table '{table}' not found in database.\n"
                f"  Available tables: {', '.join(sorted(all_tables))}"
            )
            sys.exit(1)

        # ---- Get column metadata ----------------------------------------
        print(f"[02_db_data_sample] Profiling table '{table}' ...")
        col_meta = []
        cursor.execute(f"DESCRIBE `{table}`")
        for row in cursor.fetchall():
            col_meta.append({"name": row[0], "type": row[1]})

        total_rows = _scalar(cursor, f"SELECT COUNT(*) FROM `{table}`")
        print(f"  Table has {total_rows:,} rows and {len(col_meta)} columns")

        # ---- Profile each column ----------------------------------------
        profiles: list[dict] = []
        start = time.time()

        for idx, cm in enumerate(col_meta, 1):
            col_name, col_type = cm["name"], cm["type"]
            print(
                f"  ({idx}/{len(col_meta)}) Profiling '{col_name}' ({col_type}) ...",
                end=" ",
            )
            try:
                p = profile_column(cursor, table, col_name, col_type, total_rows, sample_size)
                profiles.append(p)
                print(
                    f"OK  (distinct={p['distinct']}, null={p['null_pct']}%"
                    f"{f', avg_len={p[\"avg_length\"]}' if p['avg_length'] is not None else ''})"
                )
            except Exception as exc:
                print(f"FAILED  ({type(exc).__name__}: {exc})")

        elapsed = time.time() - start
        print(f"\n[02_db_data_sample] Column profiling complete in {elapsed:.1f}s")

        # ---- Full sample (first 100 rows) -------------------------------
        print(f"[02_db_data_sample] Fetching first 100 rows ...")
        rows = _query(cursor, f"SELECT * FROM `{table}` LIMIT 100")
        full_sample_df = pd.DataFrame(rows) if rows else pd.DataFrame()
        print(f"  Fetched {len(full_sample_df)} rows")

        # ---- Write output -----------------------------------------------
        print(f"[02_db_data_sample] Writing reports ...")
        write_report(table, profiles, full_sample_df, xlsx_path, csv_path)

    finally:
        conn.close()
        print("[02_db_data_sample] Database connection closed.")

    print("[02_db_data_sample] Done.")


if __name__ == "__main__":
    main()
