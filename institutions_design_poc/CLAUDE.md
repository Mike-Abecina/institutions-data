# Institutions Design POC — Vibe Check

TikTok-inspired institution discovery platform POC for 10-25 year olds. Dark-mode Streamlit app with vibe quiz, card feed, and compare mode.

## Quick Start

```bash
cd institutions_design_poc
# Uses parent project's venv at ../venv/
streamlit run scripts/C_ui/app.py
```

## Project Structure

```
institutions_design_poc/
├── config/
│   ├── settings.py          # DB, API, LLM config (loads parent .env)
│   └── models.py            # Pydantic models: InstitutionCard, ReviewResult, etc.
├── data/
│   ├── raw/                 # DB exports, API responses
│   ├── enriched/            # Post-enrichment JSON files
│   ├── reports/             # Audit reports, PM review results
│   └── fixtures/            # institutions_sample.json (UI data source)
├── scripts/
│   ├── A_audit/             # 01-04: DB schema, data sample, API probe, gap analysis
│   ├── B_enrichment/        # 01-07: places, walk scores, rent, taglines, day-in-life, vibes, merge
│   ├── C_ui/                # Streamlit app, pages, components, styles
│   └── D_orchestration/     # PM agent, review criteria, orchestrator
└── tests/
```

## Architecture

### Four Workstreams

- **A_audit**: MariaDB schema discovery + COURSES_API/PROVIDERS_API probing → gap analysis
- **B_enrichment**: Hardcoded data (01-03) + GPT-4o generation (04-06) → merge (07) → `data/fixtures/institutions_sample.json`
- **C_ui**: Streamlit dark-mode app. Quiz → card feed → compare. Each card has 4 tabs: VIBE/EATS/LIFE/COST
- **D_orchestration**: GPT-4o PM agent reviews outputs against quality rubrics. Orchestrator runs full pipeline with review gates

### 5 POC Institutions

JCU (Townsville), Griffith (Gold Coast), UQ (Brisbane), UNSW (Sydney), University of Melbourne. Mix of regional/metro, Go8/non-Go8.

### Data Flow

1. `B_enrichment/01-03` produce hardcoded JSON → `data/enriched/`
2. `B_enrichment/04-06` call GPT-4o → `data/enriched/`
3. `B_enrichment/07_build_institution_json.py` merges all → `data/fixtures/institutions_sample.json`
4. `C_ui/app.py` reads fixture JSON, falls back to inline FALLBACK_DATA

### Key Patterns

- All scripts use `python -m` execution from the parent `au-data-check/` directory
- LLM calls: `config.settings.chat(client, system, user, temperature)` with exponential backoff
- LLM JSON parsing: `config.settings.parse_llm_json(raw)` strips markdown fences
- All data validated through Pydantic models in `config/models.py`
- `_base_institutions.py` is the single source of truth for the 5 POC institutions

## Running Scripts

All scripts run from the parent `au-data-check/` directory using module syntax:

```bash
# Enrichment (hardcoded)
python -m institutions_design_poc.scripts.B_enrichment.01_places_nearby
python -m institutions_design_poc.scripts.B_enrichment.02_walk_score
python -m institutions_design_poc.scripts.B_enrichment.03_rent_data

# Enrichment (LLM — requires OPENAI_API_KEY or Azure config in .env)
python -m institutions_design_poc.scripts.B_enrichment.04_generate_taglines
python -m institutions_design_poc.scripts.B_enrichment.05_generate_day_in_life
python -m institutions_design_poc.scripts.B_enrichment.06_generate_vibe_tags

# Merge all enriched data
python -m institutions_design_poc.scripts.B_enrichment.07_build_institution_json

# PM review
python -m institutions_design_poc.scripts.D_orchestration.pm_agent content data/enriched/taglines.json
python -m institutions_design_poc.scripts.D_orchestration.pm_agent data data/enriched/places_nearby.json
python -m institutions_design_poc.scripts.D_orchestration.pm_agent merge data/fixtures/institutions_sample.json

# Full orchestration
python -m institutions_design_poc.scripts.D_orchestration.orchestrator --skip-db --stage all
```

## Design System

- **Background**: #0D0D0D (midnight)
- **Accents**: coral #FF5733, mint #00E5A0, purple #7C5CFC, golden #FFD166
- **Text**: #F5F5F5 (cloud white)
- **Fonts**: Cabinet Grotesk (headings), General Sans (body) — Fontshare CDN
- **Cards**: 12-16px rounded corners, gradient heroes, tag pills

## Key Models (config/models.py)

- `InstitutionCard` — 30+ fields covering all 4 tabs + quiz matching + compare
- `NearbyVenue` — name, type, distance, student review, rating, price level
- `DayInLifeEntry` — time, activity, emoji, description, photo prompt
- `CostBreakdown` — tuition range, rent range, weekly budget, comparisons
- `ReviewResult` — domain, passed, score, feedback, blocking issues, suggestions

## Environment Variables (from parent .env)

- `OPENAI_API_KEY` or `AZURE_API_BASE`/`AZURE_API_KEY`/`AZURE_API_VERSION` — LLM access
- `OPEN_AI_MODEL` — defaults to "gpt-4o"
- `DATABASE_HOST`/`DATABASE_PORT`/`DATABASE_NAME`/`DATABASE_USER`/`DATABASE_PASSWORD` — MariaDB (audit only)
- `COURSES_API`/`PROVIDERS_API` — REST API endpoints (audit only)

## Dependencies

On top of parent project: `pymysql`, `streamlit`, `httpx` (see `requirements.txt`).

## Conventions

- Hardcoded data scripts (01-03) are drop-in replaceable with real API calls — each has a commented-out API function
- LLM scripts follow the parent project's `argparse` + `ThreadPoolExecutor` + `tqdm` pattern
- PM agent scores: >= 0.7 with no blocking issues = PASS
- Review gates: audit, content, data, merge, ui
- All enrichment output goes to `data/enriched/`, final merged output to `data/fixtures/`
