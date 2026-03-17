"""
03_api_probe.py
===============
Probe the Courses and Providers REST APIs to discover their response schemas,
field types, nesting depth, and sample payloads.

For each API a set of representative search queries is fired.  The script
recursively analyses the JSON responses and produces a unified schema map.

Outputs
-------
data/reports/api_probe.json     -- machine-readable full results
data/reports/api_probe.xlsx     -- human-readable workbook with sheets:
    courses_schema      : field inventory for the Courses API
    providers_schema    : field inventory for the Providers API
    courses_samples     : representative payloads
    providers_samples   : representative payloads
    request_log         : status / timing for every request
"""
from __future__ import annotations

import argparse
import json
import sys
import textwrap
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

# ---------------------------------------------------------------------------
# Project imports
# ---------------------------------------------------------------------------
try:
    from config.settings import COURSES_API_BASE, PROVIDERS_API_BASE, DATA_REPORTS
except ImportError:
    print(
        "[ERROR] Could not import config.settings.\n"
        "  Run from the project root:  cd institutions_design_poc && "
        "python -m scripts.A_audit.03_api_probe"
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Default search queries
# ---------------------------------------------------------------------------

COURSES_QUERIES = [
    "engineering",
    "nursing",
    "business",
    "marine science",
    "computer science",
]

PROVIDERS_QUERIES = [
    "university",
    "TAFE",
    "james cook",
    "griffith",
    "monash",
    "unsw",
    "melbourne",
]


# ---------------------------------------------------------------------------
# Recursive schema analyser
# ---------------------------------------------------------------------------

def _python_type_label(value: Any) -> str:
    """Return a human-friendly type label."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def analyse_schema(
    data: Any,
    prefix: str = "",
    depth: int = 0,
    accumulator: dict[str, dict] | None = None,
) -> dict[str, dict]:
    """Walk *data* recursively and build a flat field inventory.

    Each key in the returned dict is a dot-separated JSON path.  The value is
    a dict with ``types`` (set of observed type labels), ``depths`` (set of
    int), ``count`` (number of times the path was observed), and
    ``sample_values`` (up to 5 examples).
    """
    if accumulator is None:
        accumulator = defaultdict(lambda: {
            "types": set(),
            "depths": set(),
            "count": 0,
            "sample_values": [],
        })

    if isinstance(data, dict):
        for key, val in data.items():
            path = f"{prefix}.{key}" if prefix else key
            entry = accumulator[path]
            entry["types"].add(_python_type_label(val))
            entry["depths"].add(depth)
            entry["count"] += 1
            if len(entry["sample_values"]) < 5:
                sample = val
                # Truncate long strings for readability
                if isinstance(val, str) and len(val) > 200:
                    sample = val[:200] + "..."
                elif isinstance(val, (list, dict)):
                    sample = _python_type_label(val)
                entry["sample_values"].append(sample)
            # Recurse into nested structures
            if isinstance(val, dict):
                analyse_schema(val, prefix=path, depth=depth + 1, accumulator=accumulator)
            elif isinstance(val, list):
                analyse_schema(val, prefix=path, depth=depth + 1, accumulator=accumulator)

    elif isinstance(data, list):
        path = prefix or "(root)"
        for item in data:
            if isinstance(item, dict):
                analyse_schema(item, prefix=prefix, depth=depth, accumulator=accumulator)
            elif isinstance(item, list):
                analyse_schema(item, prefix=prefix, depth=depth + 1, accumulator=accumulator)
            else:
                entry = accumulator[f"{path}[]"]
                entry["types"].add(_python_type_label(item))
                entry["depths"].add(depth)
                entry["count"] += 1
                if len(entry["sample_values"]) < 5:
                    entry["sample_values"].append(item)

    return accumulator


def _schema_to_rows(schema: dict[str, dict]) -> list[dict]:
    """Convert the schema accumulator to a list of flat dicts for a DataFrame."""
    rows = []
    for path, info in sorted(schema.items()):
        rows.append({
            "field_path": path,
            "observed_types": ", ".join(sorted(info["types"])),
            "min_depth": min(info["depths"]),
            "max_depth": max(info["depths"]),
            "observation_count": info["count"],
            "sample_values": json.dumps(info["sample_values"][:5], default=str, ensure_ascii=False),
        })
    return rows


# ---------------------------------------------------------------------------
# API caller
# ---------------------------------------------------------------------------

def probe_api(
    base_url: str,
    queries: list[str],
    api_label: str,
    timeout: float = 30.0,
) -> tuple[dict[str, dict], list[dict], list[dict]]:
    """Fire each query at *base_url* and return (schema, samples, log)."""

    schema_acc = defaultdict(lambda: {
        "types": set(),
        "depths": set(),
        "count": 0,
        "sample_values": [],
    })
    samples: list[dict] = []
    request_log: list[dict] = []

    for q in queries:
        print(f"    Querying {api_label} with '{q}' ...", end=" ")
        t0 = time.time()

        try:
            with httpx.Client(timeout=timeout) as client:
                resp = client.get(base_url, params={"query": q})
            elapsed_ms = round((time.time() - t0) * 1000)

            log_entry = {
                "api": api_label,
                "query": q,
                "status_code": resp.status_code,
                "elapsed_ms": elapsed_ms,
                "error": None,
                "result_count": None,
            }

            if resp.status_code != 200:
                log_entry["error"] = f"HTTP {resp.status_code}: {resp.text[:300]}"
                request_log.append(log_entry)
                print(f"HTTP {resp.status_code} ({elapsed_ms}ms)")
                continue

            payload = resp.json()

            # Determine result count heuristically
            if isinstance(payload, list):
                log_entry["result_count"] = len(payload)
            elif isinstance(payload, dict):
                # Try common wrapper keys
                for key in ("results", "data", "items", "records", "courses", "providers"):
                    if key in payload and isinstance(payload[key], list):
                        log_entry["result_count"] = len(payload[key])
                        break
                if log_entry["result_count"] is None:
                    log_entry["result_count"] = 1  # single-object response

            request_log.append(log_entry)
            print(
                f"OK  ({log_entry['result_count']} results, {elapsed_ms}ms)"
            )

            # Accumulate schema
            analyse_schema(payload, accumulator=schema_acc)

            # Store a trimmed sample (first result or first item)
            sample_payload = payload
            if isinstance(payload, list) and payload:
                sample_payload = payload[0]
            elif isinstance(payload, dict):
                for key in ("results", "data", "items", "records", "courses", "providers"):
                    if key in payload and isinstance(payload[key], list) and payload[key]:
                        sample_payload = payload[key][0]
                        break

            samples.append({
                "query": q,
                "sample": sample_payload,
            })

        except httpx.TimeoutException:
            elapsed_ms = round((time.time() - t0) * 1000)
            request_log.append({
                "api": api_label,
                "query": q,
                "status_code": None,
                "elapsed_ms": elapsed_ms,
                "error": "TIMEOUT",
                "result_count": None,
            })
            print(f"TIMEOUT ({elapsed_ms}ms)")

        except httpx.ConnectError as exc:
            elapsed_ms = round((time.time() - t0) * 1000)
            request_log.append({
                "api": api_label,
                "query": q,
                "status_code": None,
                "elapsed_ms": elapsed_ms,
                "error": f"CONNECTION_ERROR: {exc}",
                "result_count": None,
            })
            print(f"CONNECTION ERROR ({elapsed_ms}ms)")

        except Exception as exc:
            elapsed_ms = round((time.time() - t0) * 1000)
            request_log.append({
                "api": api_label,
                "query": q,
                "status_code": None,
                "elapsed_ms": elapsed_ms,
                "error": f"{type(exc).__name__}: {exc}",
                "result_count": None,
            })
            print(f"ERROR: {type(exc).__name__}: {exc} ({elapsed_ms}ms)")

    return dict(schema_acc), samples, request_log


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def _serialisable_schema(schema: dict[str, dict]) -> dict[str, dict]:
    """Convert sets to lists so the schema is JSON-serialisable."""
    out = {}
    for path, info in schema.items():
        out[path] = {
            "types": sorted(info["types"]),
            "min_depth": min(info["depths"]),
            "max_depth": max(info["depths"]),
            "count": info["count"],
            "sample_values": [str(v) for v in info["sample_values"][:5]],
        }
    return out


def write_json(
    courses_schema: dict,
    providers_schema: dict,
    courses_samples: list[dict],
    providers_samples: list[dict],
    request_log: list[dict],
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "courses_api": {
            "base_url": COURSES_API_BASE,
            "schema": _serialisable_schema(courses_schema),
            "samples": courses_samples,
        },
        "providers_api": {
            "base_url": PROVIDERS_API_BASE,
            "schema": _serialisable_schema(providers_schema),
            "samples": providers_samples,
        },
        "request_log": request_log,
    }
    path.write_text(json.dumps(blob, indent=2, default=str, ensure_ascii=False))
    print(f"  JSON report saved to {path}")


def write_xlsx(
    courses_schema: dict,
    providers_schema: dict,
    courses_samples: list[dict],
    providers_samples: list[dict],
    request_log: list[dict],
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        # Schema sheets
        c_rows = _schema_to_rows(courses_schema)
        pd.DataFrame(c_rows).to_excel(writer, sheet_name="courses_schema", index=False)
        print(f"  [+] Sheet 'courses_schema' ({len(c_rows)} fields)")

        p_rows = _schema_to_rows(providers_schema)
        pd.DataFrame(p_rows).to_excel(writer, sheet_name="providers_schema", index=False)
        print(f"  [+] Sheet 'providers_schema' ({len(p_rows)} fields)")

        # Sample sheets
        c_sample_rows = []
        for s in courses_samples:
            c_sample_rows.append({
                "query": s["query"],
                "sample_json": json.dumps(s["sample"], indent=2, default=str, ensure_ascii=False)[:32000],
            })
        pd.DataFrame(c_sample_rows).to_excel(writer, sheet_name="courses_samples", index=False)
        print(f"  [+] Sheet 'courses_samples' ({len(c_sample_rows)} queries)")

        p_sample_rows = []
        for s in providers_samples:
            p_sample_rows.append({
                "query": s["query"],
                "sample_json": json.dumps(s["sample"], indent=2, default=str, ensure_ascii=False)[:32000],
            })
        pd.DataFrame(p_sample_rows).to_excel(writer, sheet_name="providers_samples", index=False)
        print(f"  [+] Sheet 'providers_samples' ({len(p_sample_rows)} queries)")

        # Request log
        pd.DataFrame(request_log).to_excel(writer, sheet_name="request_log", index=False)
        print(f"  [+] Sheet 'request_log' ({len(request_log)} requests)")

    print(f"  XLSX report saved to {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Probe the Courses and Providers REST APIs and analyse response schemas.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples
            --------
              python -m scripts.A_audit.03_api_probe
              python -m scripts.A_audit.03_api_probe --timeout 60
        """),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP request timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Override the output directory (default: data/reports/).",
    )
    args = parser.parse_args()

    out_dir = Path(args.output_dir) if args.output_dir else Path(DATA_REPORTS)
    json_path = out_dir / "api_probe.json"
    xlsx_path = out_dir / "api_probe.xlsx"

    all_logs: list[dict] = []

    # ---- Courses API ----------------------------------------------------
    print(f"[03_api_probe] Probing Courses API at {COURSES_API_BASE}")
    print(f"  Queries: {COURSES_QUERIES}")
    courses_schema, courses_samples, courses_log = probe_api(
        COURSES_API_BASE,
        COURSES_QUERIES,
        api_label="courses",
        timeout=args.timeout,
    )
    all_logs.extend(courses_log)
    successes = sum(1 for l in courses_log if l["status_code"] == 200)
    print(f"  Courses API: {successes}/{len(courses_log)} successful, "
          f"{len(courses_schema)} unique field paths discovered\n")

    # ---- Providers API --------------------------------------------------
    print(f"[03_api_probe] Probing Providers API at {PROVIDERS_API_BASE}")
    print(f"  Queries: {PROVIDERS_QUERIES}")
    providers_schema, providers_samples, providers_log = probe_api(
        PROVIDERS_API_BASE,
        PROVIDERS_QUERIES,
        api_label="providers",
        timeout=args.timeout,
    )
    all_logs.extend(providers_log)
    successes = sum(1 for l in providers_log if l["status_code"] == 200)
    print(f"  Providers API: {successes}/{len(providers_log)} successful, "
          f"{len(providers_schema)} unique field paths discovered\n")

    # ---- Write reports --------------------------------------------------
    print("[03_api_probe] Writing reports ...")

    write_json(
        courses_schema, providers_schema,
        courses_samples, providers_samples,
        all_logs, json_path,
    )
    write_xlsx(
        courses_schema, providers_schema,
        courses_samples, providers_samples,
        all_logs, xlsx_path,
    )

    # ---- Summary --------------------------------------------------------
    total_fields = len(courses_schema) + len(providers_schema)
    total_requests = len(all_logs)
    total_ok = sum(1 for l in all_logs if l["status_code"] == 200)
    print(
        f"\n[03_api_probe] Done. "
        f"{total_requests} requests, {total_ok} successful, "
        f"{total_fields} total unique field paths."
    )


if __name__ == "__main__":
    main()
