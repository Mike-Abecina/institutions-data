"""
Shared configuration for the institutions_design_poc project.

Loads environment variables from the parent project's .env file and exposes
database connection params, API endpoints, LLM helpers, and file path constants.
"""

import json
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from openai import AzureOpenAI, OpenAI, RateLimitError, APITimeoutError, APIConnectionError

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
# Root of the institutions_design_poc package
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load .env from the parent au-data-check directory
DOTENV_PATH = PROJECT_ROOT.parent / ".env"
load_dotenv(DOTENV_PATH)

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
ENRICHED_DIR = DATA_DIR / "enriched"
REPORTS_DIR = DATA_DIR / "reports"
FIXTURES_DIR = DATA_DIR / "fixtures"

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------
DATABASE_HOST = os.getenv("DATABASE_HOST", "localhost")
DATABASE_PORT = int(os.getenv("DATABASE_PORT", "3306").strip().strip('"'))
DATABASE_NAME = os.getenv("DATABASE_NAME", "master").strip().strip('"')
DATABASE_USER = os.getenv("DATABASE_USER", "admin").strip().strip('"')
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD", "").strip().strip('"')

# ---------------------------------------------------------------------------
# API endpoints (base URLs only -- query params stripped)
# ---------------------------------------------------------------------------
def _strip_query(url: str) -> str:
    """Return the URL without any query string."""
    return url.split("?")[0].strip()

COURSES_API = _strip_query(os.getenv("COURSES_API", ""))
PROVIDERS_API = _strip_query(os.getenv("PROVIDERS_API", ""))

# ---------------------------------------------------------------------------
# LLM / OpenAI configuration
# ---------------------------------------------------------------------------
AZURE_API_BASE = os.getenv("AZURE_API_BASE", "")
AZURE_API_KEY = os.getenv("AZURE_API_KEY", "")
AZURE_API_VERSION = os.getenv("AZURE_API_VERSION", "")
OPEN_AI_MODEL = os.getenv("OPEN_AI_MODEL", "gpt-4o")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# Set to "azure" or "openai" to force a provider.  Leave empty to auto-detect.
PROVIDER = os.getenv("LLM_PROVIDER", "").lower()

MAX_RETRIES = 5
INITIAL_BACKOFF = 2  # seconds


def get_openai_client():
    """Return an OpenAI-compatible client (Azure or direct)."""
    if PROVIDER == "openai" or (not PROVIDER and OPENAI_API_KEY):
        print("[config] Using direct OpenAI API")
        return OpenAI(api_key=OPENAI_API_KEY)
    print("[config] Using Azure OpenAI API")
    return AzureOpenAI(
        azure_endpoint=AZURE_API_BASE,
        api_key=AZURE_API_KEY,
        api_version=AZURE_API_VERSION,
    )


def _backoff_wait(attempt: int, error) -> float:
    """Calculate wait time: use Retry-After header if available, else exponential backoff."""
    retry_after = getattr(getattr(error, "response", None), "headers", {}).get("Retry-After")
    if retry_after:
        try:
            return float(retry_after)
        except ValueError:
            pass
    return INITIAL_BACKOFF * (2 ** (attempt - 1))


def chat(client, system: str, user: str, temperature: float = 0.2) -> str:
    """Single-turn chat with exponential backoff on rate-limit / transient errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = client.chat.completions.create(
                model=OPEN_AI_MODEL,
                temperature=temperature,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            )
            return resp.choices[0].message.content.strip()
        except RateLimitError as e:
            wait = _backoff_wait(attempt, e)
            print(f"  [rate-limited] attempt {attempt}/{MAX_RETRIES}, waiting {wait:.1f}s ...")
            time.sleep(wait)
        except (APITimeoutError, APIConnectionError) as e:
            wait = _backoff_wait(attempt, e)
            print(f"  [transient error] {type(e).__name__}, attempt {attempt}/{MAX_RETRIES}, waiting {wait:.1f}s ...")
            time.sleep(wait)
    raise RuntimeError(f"LLM call failed after {MAX_RETRIES} retries")


def parse_llm_json(raw: str) -> dict:
    """Parse JSON from an LLM response, stripping markdown fences if present."""
    text = raw.strip()
    if text.startswith("```"):
        # Remove opening fence (```json or ```)
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        # Remove closing fence
        if text.endswith("```"):
            text = text[:-3].strip()
    return json.loads(text)
