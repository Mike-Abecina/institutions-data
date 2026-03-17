"""
Orchestrator: Runs the full POC pipeline with PM review gates.

Flow:
  1. Run A_audit scripts (if DB available) or skip to enrichment
  2. PM Review A
  3. Run B_enrichment Priority 1 (LLM generation: taglines, vibes, day-in-life)
  4. PM Review B-content
  5. Run B_enrichment Priority 2 (hardcoded: places, walk scores, rent)
  6. PM Review B-data
  7. Run B.07 merge
  8. PM Review B-final
  9. Verify C_ui loads
  10. PM Review C
  11. Final sign-off
"""
import json
import subprocess
import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import ENRICHED_DIR, FIXTURES_DIR, REPORTS_DIR
from scripts.D_orchestration.pm_agent import review_json_file, review_artifact
from scripts.D_orchestration.review_criteria import get_criteria_for_gate


def run_script(script_path: str, args: list = None, cwd: str = None) -> bool:
    """Run a Python script and return True if it succeeded."""
    cmd = [sys.executable, "-m", script_path] + (args or [])
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    cwd = cwd or project_root

    print(f"\n{'='*60}")
    print(f"  Running: {script_path}")
    print(f"{'='*60}")

    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.stdout:
            print(result.stdout[-2000:])  # Last 2000 chars
        if result.returncode != 0:
            print(f"  FAILED (exit code {result.returncode})")
            if result.stderr:
                print(f"  STDERR: {result.stderr[-1000:]}")
            return False
        print(f"  SUCCESS")
        return True
    except subprocess.TimeoutExpired:
        print(f"  TIMEOUT after 300s")
        return False
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def run_audit_pipeline(skip_db: bool = False) -> bool:
    """Run workstream A: Database audit scripts."""
    print("\n" + "="*60)
    print("  WORKSTREAM A: DATABASE AUDIT")
    print("="*60)

    if skip_db:
        print("  Skipping DB audit (--skip-db flag). Using fixture data.")
        return True

    scripts = [
        "scripts.A_audit.01_db_schema_audit",
        "scripts.A_audit.02_db_data_sample",
        "scripts.A_audit.03_api_probe",
        "scripts.A_audit.04_gap_analysis",
    ]

    for script in scripts:
        if not run_script(script):
            print(f"  Audit script {script} failed. Continuing with available data...")
            # Don't block on audit failure — enrichment can use hardcoded data
            return False

    return True


def run_enrichment_llm() -> bool:
    """Run workstream B Priority 1: LLM generation scripts."""
    print("\n" + "="*60)
    print("  WORKSTREAM B (Priority 1): LLM GENERATION")
    print("="*60)

    scripts = [
        "scripts.B_enrichment.04_generate_taglines",
        "scripts.B_enrichment.05_generate_day_in_life",
        "scripts.B_enrichment.06_generate_vibe_tags",
    ]

    all_ok = True
    for script in scripts:
        if not run_script(script):
            all_ok = False

    return all_ok


def run_enrichment_data() -> bool:
    """Run workstream B Priority 2: Hardcoded data scripts."""
    print("\n" + "="*60)
    print("  WORKSTREAM B (Priority 2): HARDCODED DATA")
    print("="*60)

    scripts = [
        "scripts.B_enrichment.01_places_nearby",
        "scripts.B_enrichment.02_walk_score",
        "scripts.B_enrichment.03_rent_data",
    ]

    all_ok = True
    for script in scripts:
        if not run_script(script):
            all_ok = False

    return all_ok


def run_merge() -> bool:
    """Run workstream B merge step."""
    print("\n" + "="*60)
    print("  WORKSTREAM B: MERGE")
    print("="*60)

    return run_script("scripts.B_enrichment.07_build_institution_json")


def pm_review(gate: str, artifact_path: str) -> bool:
    """Run a PM review gate. Returns True if passed."""
    print(f"\n{'='*60}")
    print(f"  PM REVIEW GATE: {gate.upper()}")
    print(f"  Artifact: {artifact_path}")
    print(f"{'='*60}")

    result = review_json_file(gate, artifact_path)

    status = "PASSED" if result.passed else "FAILED"
    print(f"\n  Result: {status} (score: {result.score:.2f})")

    if result.blocking_issues:
        print("  Blocking issues:")
        for issue in result.blocking_issues:
            print(f"    ! {issue}")

    if result.suggestions:
        print("  Suggestions:")
        for sug in result.suggestions[:3]:
            print(f"    ~ {sug}")

    # Save review result
    review_path = REPORTS_DIR / f"review_{gate}_{datetime.now().strftime('%H%M%S')}.json"
    review_path.parent.mkdir(parents=True, exist_ok=True)
    review_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")

    return result.passed


def verify_ui_loads() -> bool:
    """Verify the Streamlit app can import and load data without errors."""
    print("\n" + "="*60)
    print("  WORKSTREAM C: UI VERIFICATION")
    print("="*60)

    # Test that the app module can be imported
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    cmd = [
        sys.executable, "-c",
        "import sys; sys.path.insert(0, '.'); "
        "from scripts.C_ui.app import load_institutions; "
        "data = load_institutions(); "
        "print(f'Loaded {len(data)} institutions'); "
        "assert len(data) >= 3, 'Need at least 3 institutions'; "
        "print('UI data load: OK')"
    ]

    try:
        result = subprocess.run(
            cmd,
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.stdout:
            print(f"  {result.stdout.strip()}")
        if result.returncode != 0:
            print(f"  UI verification FAILED")
            if result.stderr:
                print(f"  {result.stderr[-500:]}")
            return False
        print("  UI verification PASSED")
        return True
    except Exception as e:
        print(f"  UI verification ERROR: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Orchestrate the full POC pipeline")
    parser.add_argument("--skip-db", action="store_true",
                        help="Skip database audit (use fixture data)")
    parser.add_argument("--skip-llm", action="store_true",
                        help="Skip LLM generation (use existing enriched data)")
    parser.add_argument("--skip-review", action="store_true",
                        help="Skip PM review gates (just run scripts)")
    parser.add_argument("--stage", choices=["audit", "enrich", "merge", "ui", "all"],
                        default="all", help="Run only a specific stage")
    args = parser.parse_args()

    start_time = datetime.now()
    results = {}

    print("\n" + "#"*60)
    print("  VIBE CHECK POC - ORCHESTRATOR")
    print(f"  Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("#"*60)

    # --- STAGE 1: AUDIT ---
    if args.stage in ("audit", "all"):
        results["audit"] = run_audit_pipeline(skip_db=args.skip_db)

        if not args.skip_review and results.get("audit"):
            gap_file = str(REPORTS_DIR / "gap_analysis.xlsx")
            if Path(gap_file).exists():
                results["review_audit"] = pm_review("audit", gap_file)

    # --- STAGE 2: ENRICHMENT (LLM) ---
    if args.stage in ("enrich", "all"):
        if not args.skip_llm:
            results["enrich_llm"] = run_enrichment_llm()

            if not args.skip_review:
                # Review content quality
                content_files = list(ENRICHED_DIR.glob("*.json"))
                if content_files:
                    combined = {}
                    for f in content_files:
                        try:
                            combined[f.stem] = json.loads(f.read_text())
                        except Exception:
                            pass
                    if combined:
                        results["review_content"] = pm_review(
                            "content",
                            str(ENRICHED_DIR / "taglines.json")
                        )

        # ENRICHMENT (DATA)
        results["enrich_data"] = run_enrichment_data()

        if not args.skip_review:
            places_file = str(ENRICHED_DIR / "places_nearby.json")
            if Path(places_file).exists():
                results["review_data"] = pm_review("data", places_file)

    # --- STAGE 3: MERGE ---
    if args.stage in ("merge", "all"):
        results["merge"] = run_merge()

        if not args.skip_review:
            fixture_file = str(FIXTURES_DIR / "institutions_sample.json")
            if Path(fixture_file).exists():
                results["review_merge"] = pm_review("merge", fixture_file)

    # --- STAGE 4: UI VERIFICATION ---
    if args.stage in ("ui", "all"):
        results["ui_verify"] = verify_ui_loads()

    # --- SUMMARY ---
    elapsed = datetime.now() - start_time
    print("\n" + "#"*60)
    print("  PIPELINE SUMMARY")
    print(f"  Elapsed: {elapsed}")
    print("#"*60)

    for step, passed in results.items():
        status = "PASS" if passed else "FAIL"
        icon = "+" if passed else "!"
        print(f"  [{icon}] {step}: {status}")

    all_passed = all(results.values())
    print(f"\n  {'ALL GATES PASSED' if all_passed else 'SOME GATES FAILED'}")
    print(f"\n  To run the UI: streamlit run scripts/C_ui/app.py")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
