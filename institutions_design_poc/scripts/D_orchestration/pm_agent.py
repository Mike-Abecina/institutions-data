"""
PM Agent: Reviews each workstream's output against quality rubrics.
Returns structured ReviewResult with pass/fail, feedback, and rework instructions.
Uses GPT-4o as the reviewer with domain expertise in Gen Z design, Australian
education, and data quality.
"""
import json
import sys
import argparse
from pathlib import Path
from typing import Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import get_openai_client, chat, parse_llm_json
from config.models import ReviewResult
from scripts.D_orchestration.review_criteria import format_criteria_prompt, get_criteria_for_gate


PM_SYSTEM_PROMPT = """\
You are a senior Product Manager and design quality reviewer for an Australian
higher education discovery platform targeting Gen Z (15-25 year olds).

Your expertise spans:
1. GEN Z DESIGN: You know what resonates with 15-25 year olds. TikTok aesthetics,
   authentic tone, no corporate-speak. You can spot "try-hard" copy instantly.
2. DATA QUALITY: You insist on accurate, complete, well-structured data.
   Missing fields, implausible values, and schema mismatches don't slip past you.
3. AUSTRALIAN EDUCATION: You know the Australian higher ed landscape — Go8 vs
   regional, CSP fees, ATAR, QTAC/UAC, ANZSCO codes. You catch US-centric content.
4. CONTENT TONE: You have an ear for authentic vs cringe. The tagline "Where the
   reef is your classroom" passes. "Unlock your oceanic potential!" does not.

You are reviewing artifacts from a POC build. Be constructive but firm. Your role
is to catch issues BEFORE they reach the user.

RESPONSE FORMAT (JSON only):
{
    "domain": "<the review gate name>",
    "passed": true/false,
    "score": 0.0-1.0,
    "feedback": ["Specific positive observations"],
    "blocking_issues": ["Must-fix items that prevent sign-off"],
    "suggestions": ["Nice-to-have improvements for next iteration"]
}

SCORING GUIDE:
- 0.9-1.0: Excellent. Ship it.
- 0.7-0.89: Good with minor issues. Pass with suggestions.
- 0.5-0.69: Needs work. Fail with specific rework instructions.
- Below 0.5: Significant rework needed. Multiple blocking issues.

Pass threshold: score >= 0.7 AND no blocking_issues.
"""


def review_artifact(
    gate: str,
    artifact_content: str,
    client=None,
    max_content_chars: int = 15000,
) -> ReviewResult:
    """
    Review an artifact against the criteria for a given gate.

    Args:
        gate: Review gate name ("audit", "content", "data", "merge", "ui")
        artifact_content: String representation of the artifact to review
        client: OpenAI client (created if not provided)
        max_content_chars: Max chars of artifact to include in prompt

    Returns:
        ReviewResult with pass/fail, score, feedback, and issues
    """
    if client is None:
        client = get_openai_client()

    criteria_text = format_criteria_prompt(gate)

    # Truncate artifact if too long
    if len(artifact_content) > max_content_chars:
        artifact_content = artifact_content[:max_content_chars] + "\n\n[... TRUNCATED ...]"

    user_prompt = f"""\
## Review Gate: {gate.upper()}

## Criteria to evaluate against:
{criteria_text}

## Artifact to review:
```
{artifact_content}
```

Review this artifact against EVERY criterion listed above. Be specific in your
feedback — reference exact items, field names, or content that passes or fails.
"""

    raw = chat(client, PM_SYSTEM_PROMPT, user_prompt, temperature=0.3)
    result_dict = parse_llm_json(raw)
    return ReviewResult(**result_dict)


def review_json_file(gate: str, file_path: str, client=None) -> ReviewResult:
    """Review a JSON file artifact."""
    path = Path(file_path)
    if not path.exists():
        return ReviewResult(
            domain=gate,
            passed=False,
            score=0.0,
            feedback=[],
            blocking_issues=[f"File not found: {file_path}"],
            suggestions=[],
        )

    content = path.read_text(encoding="utf-8")

    # For JSON, pretty-print for readability
    try:
        data = json.loads(content)
        content = json.dumps(data, indent=2, ensure_ascii=False)
    except json.JSONDecodeError:
        pass

    return review_artifact(gate, content, client)


def review_with_rework(
    gate: str,
    artifact_content: str,
    max_iterations: int = 2,
    rework_callback=None,
    client=None,
) -> ReviewResult:
    """
    Review and optionally rework an artifact up to max_iterations times.

    Args:
        gate: Review gate name
        artifact_content: Initial artifact content
        max_iterations: Max rework attempts (default 2)
        rework_callback: Function(feedback: List[str]) -> str that produces
                        a new artifact based on feedback. If None, just returns
                        the review result without rework.
        client: OpenAI client

    Returns:
        Final ReviewResult after all iterations
    """
    if client is None:
        client = get_openai_client()

    current_content = artifact_content

    for iteration in range(max_iterations + 1):
        print(f"  [PM Review] Gate={gate}, iteration={iteration + 1}/{max_iterations + 1}")

        result = review_artifact(gate, current_content, client)

        print(f"  [PM Review] Score: {result.score:.2f}, Passed: {result.passed}")
        if result.feedback:
            for fb in result.feedback[:3]:
                print(f"    + {fb}")
        if result.blocking_issues:
            for issue in result.blocking_issues:
                print(f"    ! BLOCKING: {issue}")

        # Pass condition: score >= 0.7 and no blocking issues
        if result.score >= 0.7 and not result.blocking_issues:
            print(f"  [PM Review] APPROVED (score={result.score:.2f})")
            result.passed = True
            return result

        # If we have a rework callback and haven't exhausted iterations
        if rework_callback and iteration < max_iterations:
            all_feedback = result.blocking_issues + result.feedback + result.suggestions
            print(f"  [PM Review] Requesting rework with {len(all_feedback)} items...")
            current_content = rework_callback(all_feedback)
        else:
            if iteration >= max_iterations:
                print(f"  [PM Review] Max iterations reached. Final score: {result.score:.2f}")
            result.passed = result.score >= 0.7 and not result.blocking_issues
            return result

    return result


def main():
    parser = argparse.ArgumentParser(description="PM Agent: Review artifacts against quality rubrics")
    parser.add_argument("gate", choices=["audit", "content", "data", "merge", "ui"],
                        help="Review gate to evaluate against")
    parser.add_argument("file", help="Path to the artifact file to review")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()

    print(f"[PM Agent] Reviewing {args.file} against '{args.gate}' criteria...")

    result = review_json_file(args.gate, args.file)

    print(f"\n{'='*60}")
    print(f"REVIEW RESULT: {'PASSED' if result.passed else 'FAILED'}")
    print(f"Score: {result.score:.2f}/1.0")
    print(f"{'='*60}")

    if result.feedback:
        print("\nFeedback:")
        for fb in result.feedback:
            print(f"  + {fb}")

    if result.blocking_issues:
        print("\nBlocking Issues:")
        for issue in result.blocking_issues:
            print(f"  ! {issue}")

    if result.suggestions:
        print("\nSuggestions:")
        for sug in result.suggestions:
            print(f"  ~ {sug}")

    # Save review result
    output_path = Path("data/reports") / f"review_{args.gate}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    print(f"\nReview saved to {output_path}")


if __name__ == "__main__":
    main()
