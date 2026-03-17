"""
Security pre-flight check — run before deploying.

Scans the repo for patterns that look like leaked API keys, passwords,
or other secrets. Fails loudly if anything suspicious is found.

Usage:
    python deploy/security_check.py
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent

# ── Patterns that look like secrets ───────────────────────────────────────────
# Only match LITERAL values (quoted strings), not variable references like api_key=MY_VAR
PATTERNS = [
    (r"sk-proj-[A-Za-z0-9_\-]{20,}",
     "OpenAI API key (sk-proj-...)"),
    (r"sk-[A-Za-z0-9]{48}",
     "OpenAI API key (sk-...)"),
    (r"\b[0-9a-f]{40}\b",
     "Hex key 40-char (Serper / SHA-like)"),
    (r"rds\.amazonaws\.com",
     "RDS hostname"),
    (r"AKIA[0-9A-Z]{16}",
     "AWS Access Key ID"),
    # Only flag quoted literal values (≥16 chars of key-like characters)
    # This avoids false positives on  api_key=MY_VAR  or  password=os.getenv(...)
    (r"""(?i)(api[_-]?key|password|secret|token)\s*=\s*["'][A-Za-z0-9+/=_.\-;,@!]{16,}["']""",
     "Hardcoded key/password literal"),
]

# ── Files that must NEVER be tracked by git ───────────────────────────────────
FORBIDDEN_TRACKED = [
    ".env",
    ".streamlit/secrets.toml",
]

# ── File extensions to scan ───────────────────────────────────────────────────
SCAN_EXTENSIONS = {
    ".py", ".toml", ".txt", ".md", ".sql",
    ".json", ".yaml", ".yml",
}
SKIP_DIRS = {".venv", ".git", "__pycache__", ".mypy_cache", "node_modules"}


def scan_files():
    hits = []
    for path in ROOT.rglob("*"):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if not path.is_file():
            continue
        if path.suffix.lower() not in SCAN_EXTENSIONS:
            continue
        # Skip .env itself — we EXPECT keys there, just don't want it committed
        if path.name == ".env":
            continue
        # Skip template/doc files where placeholder text is intentional
        if path.name in ("secrets.toml.example", "README.md") and path.parent.name in ("deploy", ".streamlit"):
            continue
        try:
            text = path.read_text(errors="ignore")
        except Exception:
            continue
        for pattern, label in PATTERNS:
            for m in re.finditer(pattern, text):
                hits.append((
                    str(path.relative_to(ROOT)),
                    m.start(),
                    label,
                    m.group()[:60],
                ))
    return hits


def check_git_tracked():
    import subprocess
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=ROOT, capture_output=True, text=True,
    )
    tracked = set(result.stdout.strip().splitlines())
    return [f for f in FORBIDDEN_TRACKED if f in tracked]


def check_acir_csv():
    acir_csv = ROOT / "deploy" / "data" / "acir_institutions.csv"
    if not acir_csv.exists():
        return None, []  # not yet generated
    text = acir_csv.read_text(errors="ignore")
    hits = []
    # Only check hard patterns (skip generic key= pattern — CSV has legitimate ones)
    for pattern, label in PATTERNS[:6]:
        for m in re.finditer(pattern, text):
            hits.append((label, m.group()[:60]))
    return acir_csv, hits


def main():
    print("=" * 62)
    print("  Security pre-flight check")
    print("=" * 62)
    failed = False

    # 1. Forbidden files in git index
    print("\n[1] Checking git index for secrets files...")
    problems = check_git_tracked()
    if problems:
        for p in problems:
            print(f"  ❌ TRACKED IN GIT: {p}")
        failed = True
    else:
        print("  ✅ .env and secrets.toml are not git-tracked")

    # 2. Scan all committed-eligible source files
    print("\n[2] Scanning source files for secret patterns...")
    hits = scan_files()
    if hits:
        for filepath, pos, label, snippet in hits:
            print(f"  ❌ {filepath}:{pos}  [{label}]")
            print(f"       → '{snippet}'")
        failed = True
    else:
        print("  ✅ No secret patterns found in source files")

    # 3. Check ACIR CSV if it exists
    acir_csv, csv_hits = check_acir_csv()
    if acir_csv is None:
        print("\n[3] deploy/data/acir_institutions.csv not yet generated — skipping CSV check")
        print("    (run deploy/cache_acir_data.py first, then re-run this check)")
    else:
        print(f"\n[3] Checking {acir_csv.relative_to(ROOT)} for embedded secrets...")
        if csv_hits:
            for label, snippet in csv_hits:
                print(f"  ❌ [{label}] → '{snippet}'")
            failed = True
        else:
            print("  ✅ ACIR CSV looks clean")

    print("\n" + "=" * 62)
    if failed:
        print("❌  FAILED — fix issues above before pushing to GitHub.")
        sys.exit(1)
    else:
        print("✅  All checks passed. Safe to commit and push.")


if __name__ == "__main__":
    main()
