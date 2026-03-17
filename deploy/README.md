# Deployment Guide — Institution Explorer

Hosted on **Streamlit Community Cloud** (free, always-on, deploys from GitHub).

Estimated cost: **$0–$3/month** (OpenAI API only — ~$0.01 per institution page load).

---

## How it works

The app normally connects to a private AWS RDS at startup. Since that RDS isn't
reachable from the public internet, we pre-bake the query result to a CSV locally
and commit it. In production (`APP_ENV=production`) the app loads that CSV instead
of hitting MySQL.

```
Local dev  →  MySQL RDS  →  ACIR metadata (live)
Production →  deploy/data/acir_institutions.csv  (snapshot, committed to repo)
```

---

## Pre-flight steps (run once locally, then whenever ACIR data changes)

### Step 0 — Security check
Always run this before pushing to GitHub:

```bash
python deploy/security_check.py
```

Must show `✅ All checks passed`. Fix any issues before proceeding.

### Step 1 — Cache ACIR data from the database
Make sure you're on the VPN / have RDS network access, then:

```bash
python deploy/cache_acir_data.py
```

Verify the output:
```bash
python -c "import pandas as pd; df=pd.read_csv('deploy/data/acir_institutions.csv'); print(df.shape)"
```

### Step 2 — Commit the cached data
```bash
git add deploy/data/acir_institutions.csv
git commit -m "Refresh cached ACIR institutions data"
```

---

## One-time setup (first deployment only)

### Step 3 — Push the repo to GitHub
If not already on GitHub:

```bash
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
git push -u origin main
```

### Step 4 — Deploy on Streamlit Community Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io) and sign in with GitHub.
2. Click **"New app"** and configure:
   - **Repository:** your GitHub repo
   - **Branch:** `main`
   - **Main file path:** `streamlit_app_v2.py`
3. Click **"Advanced settings"** and paste your secrets:

   ```toml
   OPENAI_API_KEY = "sk-proj-..."
   OPEN_AI_MODEL = "gpt-4o"
   SERPA_API = "your-serper-key"
   APP_ENV = "production"
   ```

   > These are encrypted by Streamlit Cloud and never exposed in logs or the repo.

4. Click **"Deploy"**. The app will be live at `https://your-slug.streamlit.app`.

---

## Ongoing maintenance

| Task | What to do |
|------|-----------|
| ACIR data changed | Run `cache_acir_data.py` → commit CSV → push |
| App code changed | Push to `main` — Streamlit Cloud auto-redeploys |
| Rotate API keys | Update in Streamlit Cloud **Secrets** UI (no redeploy needed) |
| Add a new metric | Update `streamlit_app_v2.py` → push |

---

## Secrets reference

| Secret | Where to get it | Required? |
|--------|----------------|-----------|
| `OPENAI_API_KEY` | [platform.openai.com/api-keys](https://platform.openai.com/api-keys) | Yes (AI descriptions) |
| `OPEN_AI_MODEL` | Set to `gpt-4o` | No (default: gpt-4o) |
| `SERPA_API` | [serper.dev](https://serper.dev) — free 2,500 searches/month | No (images still work without it) |
| `APP_ENV` | Literal value `production` | Yes (disables MySQL connection) |

---

## Troubleshooting

**App loads but institution metadata is blank (no addresses, no expandable sections)**
→ `deploy/data/acir_institutions.csv` is not committed or the path is wrong.
Run `cache_acir_data.py` and commit the CSV.

**Build fails on Streamlit Cloud**
→ Check `requirements.txt` at repo root. Remove `geopandas`, `shapely`, `pyproj`, `fiona`
if they crept back in — they require compiled C libraries and slow the build significantly.

**AI descriptions not generating**
→ Check `OPENAI_API_KEY` in the Streamlit Cloud Secrets UI. If it's expired or revoked,
generate a new key at platform.openai.com and update the secret.

**Images not loading for institutions**
→ `SERPA_API` may be missing or over the 2,500/month free limit.
The app will still work — images just won't appear for institutions without a known logo URL.
