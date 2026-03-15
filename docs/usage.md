# Corporate Accountability Monitor — Usage Guide

This guide explains how to run the CAM pipeline, score entities, and interpret alerts. The intended audience is analysts, researchers, and engineers who operate the system day-to-day.

---

## Table of Contents

1. [Quick Start](#1-quick-start)
2. [Pipeline CLI Reference](#2-pipeline-cli-reference)
3. [Running with Docker](#3-running-with-docker)
4. [Scheduled Automation (GitHub Actions)](#4-scheduled-automation-github-actions)
5. [Interpreting Alert Levels](#5-interpreting-alert-levels)
6. [Flagging PE-Owned Entities](#6-flagging-pe-owned-entities)
7. [Industry Benchmarking (PE vs. Non-PE)](#7-industry-benchmarking-pe-vs-non-pe)
8. [Generating Alerts for a Single Entity](#8-generating-alerts-for-a-single-entity)
9. [Environment Setup](#9-environment-setup)
10. [Database Requirements](#10-database-requirements)
11. [Running Tests](#11-running-tests)

---

## 1. Quick Start

```bash
# Start all infrastructure (Postgres, Redis, MinIO)
docker-compose up -d

# Apply database migrations
DATABASE_URL=postgresql://cam:cam@localhost:5432/cam alembic upgrade head

# Ingest all regulatory sources (last 30 days by default)
DATABASE_URL=postgresql://cam:cam@localhost:5432/cam \
  EDGAR_USER_AGENT=you@example.com \
  python -m cam.entrypoint ingest --source all

# Score all entities for today
DATABASE_URL=postgresql://cam:cam@localhost:5432/cam \
  EDGAR_USER_AGENT=you@example.com \
  python -m cam.entrypoint score --date today

# Export the static dashboard + weekly digest
DATABASE_URL=postgresql://cam:cam@localhost:5432/cam \
  EDGAR_USER_AGENT=you@example.com \
  python -m cam.entrypoint export --output-dir ./site --digest

# Open the dashboard (no server needed)
open ./site/index.html
```

All three commands exit `0` on success and non-zero on failure, making them safe to chain in CI or cron.

---

## 2. Pipeline CLI Reference

The `cam.entrypoint` module provides three independently-runnable subcommands:

### `ingest` — Fetch regulatory data

```bash
python -m cam.entrypoint ingest [--source SOURCE...] [--since YYYY-MM-DD]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--source` | `all` | One or more of: `osha epa cfpb warn edgar all` |
| `--since` | 30 days ago | Only ingest records on or after this date |

Each source is attempted independently — a failure in one source does not stop the others. Exit code is `1` if any source failed (so the scheduler notices), but successfully-ingested data is committed and available for scoring.

```bash
# Ingest everything from a specific date
python -m cam.entrypoint ingest --source all --since 2025-01-01

# Ingest a single source
python -m cam.entrypoint ingest --source cfpb --since 2025-06-01

# Ingest multiple specific sources
python -m cam.entrypoint ingest --source osha epa --since 2025-03-01
```

### `score` — Compute composite risk scores

```bash
python -m cam.entrypoint score [--date YYYY-MM-DD]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--date` | today | Score date (reads signals written by `ingest`) |

Scores all entities that have at least one signal. Writes to `alert_scores` and fires alerts for entities that cross a threshold for the first time.

```bash
# Score for today
python -m cam.entrypoint score --date today

# Score for a specific past date (backfill)
python -m cam.entrypoint score --date 2025-06-15
```

### `export` — Generate the static dashboard

```bash
python -m cam.entrypoint export --output-dir PATH [--digest] [--digest-since YYYY-MM-DD]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--output-dir` | *(required)* | Destination directory (created if absent) |
| `--digest` | off | Also write `digest.txt` (plaintext weekly email body) |
| `--digest-since` | 7 days ago | Digest covers alerts on or after this date |

```bash
# Export dashboard only
python -m cam.entrypoint export --output-dir ./site

# Export dashboard + weekly digest
python -m cam.entrypoint export --output-dir ./site --digest

# Digest covering the last 30 days
python -m cam.entrypoint export --output-dir ./site --digest --digest-since 2025-06-01
```

The output directory is a self-contained static site. Open `index.html` directly in a browser (`file://` URIs are fully supported) or host on GitHub Pages, S3, or Nginx.

---

## 3. Running with Docker

### Pull the pre-built image from GHCR

```bash
docker pull ghcr.io/rogermyung/cam-project:latest
```

### Run each step

```bash
# Ingest all sources
docker run --rm \
  -e DATABASE_URL=postgresql://... \
  -e EDGAR_USER_AGENT=you@example.com \
  ghcr.io/rogermyung/cam-project:latest \
  ingest --source all --since 2025-01-01

# Score entities
docker run --rm \
  -e DATABASE_URL=postgresql://... \
  -e EDGAR_USER_AGENT=you@example.com \
  ghcr.io/rogermyung/cam-project:latest \
  score --date today

# Export dashboard (mount a local dir to retrieve output)
docker run --rm \
  -e DATABASE_URL=postgresql://... \
  -e EDGAR_USER_AGENT=you@example.com \
  -v "$(pwd)/site:/out" \
  ghcr.io/rogermyung/cam-project:latest \
  export --output-dir /out --digest
```

### Local dev with docker-compose

```bash
# Start all infrastructure
docker-compose up -d

# Build and run the pipeline image locally
docker-compose build cam
docker-compose run --rm cam ingest --source warn
docker-compose run --rm cam score --date today
docker-compose run --rm cam export --output-dir /out --digest
```

### Celery worker (background task queue)

Override the entrypoint to start the Celery worker instead:

```bash
docker run --rm \
  -e DATABASE_URL=postgresql://... \
  -e EDGAR_USER_AGENT=you@example.com \
  -e REDIS_URL=redis://... \
  --entrypoint celery \
  ghcr.io/rogermyung/cam-project:latest \
  -A cam.tasks:celery_app worker --loglevel=info
```

---

## 4. Scheduled Automation (GitHub Actions)

The `.github/workflows/pipeline.yml` workflow runs the full pipeline daily at 06:00 UTC and deploys the static dashboard to GitHub Pages.

### Prerequisites

1. Set these repository secrets (Settings → Secrets and variables → Actions):
   - `DATABASE_URL` — publicly reachable PostgreSQL (Supabase, Neon, Railway)
   - `EDGAR_USER_AGENT` — your contact email (SEC EDGAR requirement)

2. Enable GitHub Pages (Settings → Pages → Source: "GitHub Actions")

### Manual runs

Trigger `workflow_dispatch` from the Actions tab with optional overrides:

| Input | Description |
|-------|-------------|
| `since` | Ingest since date (`YYYY-MM-DD`). Default: 30 days ago |
| `step` | Run only one step (`ingest`, `score`, or `export`). Default: all |
| `score_date` | Score date. Default: `today` |
| `deploy_pages` | Deploy export output to GitHub Pages. Default: `true` |

### Recommended schedule

The pipeline is designed to run as three separate jobs:

```
06:00 UTC   ingest  (all regulatory sources)
↓           score   (reads signals written by ingest)
↓           export  (reads alert_scores, deploys to GitHub Pages)
```

Each step is a separate GHA job so they can fail and be re-run independently.

---

## 5. Interpreting Alert Levels

| Level    | Threshold | Meaning                                     | Recommended Action                    |
|----------|-----------|---------------------------------------------|---------------------------------------|
| `watch`  | ≥ 0.40    | Worth monitoring; no immediate action       | Flag for weekly review                |
| `elevated` | ≥ 0.65  | Elevated risk; analyst review warranted     | Assign analyst; cross-check filings   |
| `critical` | ≥ 0.80  | Significant risk; regulatory action likely  | Escalate; consider regulatory referral|
| *(none)* | < 0.40    | Within normal operating range               | No action                             |

Alerts only fire when the level **increases**. An entity that stays at `watch` for three weeks in a row generates exactly **one** alert — on the day it first crossed the 0.40 threshold.

### Score Composition

The composite score is a weighted sum of six signal components:

| Component                | Weight | Source Module |
|--------------------------|--------|---------------|
| Cross-agency aggregate   | 35%    | M6            |
| Risk language (10-K NLP) | 20%    | M7            |
| Earnings call divergence | 15%    | M8            |
| Proxy escalation signals | 15%    | M9            |
| Merger vertical risk     | 10%    | M10           |
| PE ownership flag        | 5%     | M12           |

Missing components default to **0.0** — the scorer degrades gracefully if a module has not yet produced signals for an entity.

---

## 6. Flagging PE-Owned Entities

CAM tracks private equity ownership through the `Signal` table. Use `flag_pe_entity_for_monitoring` to mark an entity as PE-owned:

```python
from uuid import UUID
from cam.db.session import get_session
from cam.analysis.pe_correlator import flag_pe_entity_for_monitoring

entity_id = UUID("your-entity-uuid-here")

with get_session() as db:
    flag_pe_entity_for_monitoring(
        entity_id,
        db=db,
        evidence="Listed in PE Stakeholder Project database, confirmed 2024-Q1",
    )
    db.commit()
```

The call is **idempotent** — if the entity is already flagged, no duplicate signal is created.

Once flagged, the entity's `pe_warn_flag` component (5% weight) activates in the next daily scoring run.

---

## 7. Industry Benchmarking (PE vs. Non-PE)

The M12 PE/Bankruptcy Correlator can generate a citable comparison table across all NAICS sectors.

```python
from cam.db.session import get_session
from cam.analysis.pe_correlator import summarize_all_industries

with get_session() as db:
    # WARN Act filing rate comparison
    rows = summarize_all_industries(event_type="warn", lookback_years=5, db=db)

    for row in rows[:5]:  # top 5 sectors by rate ratio
        print(
            f"NAICS {row['industry']}: PE rate = {row['pe_rate']:.3f}, "
            f"non-PE rate = {row['non_pe_rate']:.3f}, "
            f"ratio = {row['rate_ratio']:.2f}x, "
            f"p = {row['p_value']:.4f}" if row['p_value'] else "p = N/A"
        )
```

`event_type` accepts `"warn"` (layoff notices) or `"bankruptcy"` (PACER filings).

Only sectors with **more than 10 PE-owned entities** are included, per the statistical sampling requirement.

---

## 8. Generating Alerts for a Single Entity

```python
from datetime import date
from uuid import UUID
from cam.db.session import get_session
from cam.alerts.scorer import compute_entity_score, generate_alert, get_prior_score

entity_id = UUID("your-entity-uuid-here")
today = date.today()

with get_session() as db:
    score = compute_entity_score(entity_id, today, db=db)
    prior = get_prior_score(entity_id, before_date=today, db=db)
    alert = generate_alert(entity_id, score, prior, db=db)
    db.commit()

    if alert:
        print(alert)
    else:
        print(f"No threshold crossed. Current level: {score.alert_level}")
```

---

## 9. Environment Setup

Copy `.env.example` to `.env` and fill in the required values:

```bash
cp .env.example .env
```

| Variable                    | Required | Default                        | Description                                         |
|-----------------------------|----------|--------------------------------|-----------------------------------------------------|
| `DATABASE_URL`              | ✅       | —                              | PostgreSQL connection string                        |
| `EDGAR_USER_AGENT`          | ✅       | —                              | Your email address (SEC EDGAR requires it)          |
| `INGEST_DEFAULT_SINCE_DAYS` | ✗        | `30`                           | Default look-back window when `--since` is omitted  |
| `REDIS_URL`                 | ✗        | `redis://localhost:6379/0`     | Celery broker                                       |
| `S3_BUCKET`                 | ✗        | `cam-documents`                | Raw document storage bucket                         |
| `API_AUTH_TOKEN`            | ✗        | —                              | Required only for the REST API layer (M14)          |
| `ALERT_THRESHOLD_WATCH`     | ✗        | `0.40`                         | Minimum score for `watch` alert level               |
| `ALERT_THRESHOLD_ELEVATED`  | ✗        | `0.65`                         | Minimum score for `elevated` alert level            |
| `ALERT_THRESHOLD_CRITICAL`  | ✗        | `0.80`                         | Minimum score for `critical` alert level            |

All variables can be set in `.env` or as real environment variables. Environment variables take precedence over `.env`.

---

## 10. Database Requirements

CAM uses PostgreSQL for all structured data. Redis is required only for the Celery task queue.

### Minimum Specifications

| Entity Count | vCPUs | RAM   | Storage | Notes |
|-------------|-------|-------|---------|-------|
| ≤ 500       | 1     | 512 MB | 2 GB   | Free tier sufficient for most cloud providers |
| ≤ 5,000     | 1     | 1 GB  | 10 GB   | Suitable for statewide or sector analysis |
| ≤ 50,000    | 2     | 4 GB  | 50 GB   | Full national corpus (all PE-owned employers) |
| > 50,000    | 4+    | 8 GB+ | 100 GB+ | High-frequency re-scoring or multi-year history |

Storage is dominated by the `events` and `signals` tables. Each year of full OSHA + EPA + CFPB data generates roughly 2–5 GB of rows. EDGAR full-text storage (S3) is additional and is bounded by your S3 quota.

### PostgreSQL Version

**PostgreSQL 15 or later** is required. CAM uses:
- `JSONB` columns with GIN indexes (for signal metadata)
- `ROW_NUMBER() OVER (PARTITION BY ...)` window functions (for bounded score history)
- `INSERT ... ON CONFLICT DO NOTHING` (for idempotent ingestion)

PostgreSQL 14 is untested; versions prior to 12 are incompatible.

### Cloud Options for GitHub Actions

GitHub Actions runners cannot reach `localhost`, so the database must be publicly accessible. Recommended free/low-cost options:

| Provider | Free Tier | Connection Limit | Notes |
|----------|-----------|-----------------|-------|
| [Supabase](https://supabase.com) | 500 MB, 1 vCPU | 60 direct connections | Best free option; enable connection pooler for GHA |
| [Neon](https://neon.tech) | 0.5 GB, auto-suspend | 100 connections | Scales to zero; fast cold start (~1 s) |
| [Railway](https://railway.app) | $5/month | unlimited | Simplest setup; no cold starts |
| [Render](https://render.com) | 1 GB (90-day trial) | 25 connections | Good for staging environments |

For production with > 5,000 entities, use a dedicated instance (RDS, Cloud SQL) with at least 2 vCPUs and a connection pooler (PgBouncer or the provider's built-in pooler).

### Connection Pool Settings

CAM uses SQLAlchemy's default pool settings. For GHA or serverless environments, add to your `DATABASE_URL`:

```
postgresql://user:pass@host/db?sslmode=require&connect_timeout=10
```

For high-concurrency deployments, set `pool_size` and `max_overflow` in `cam/db/session.py` or use a dedicated connection pooler upstream.

---

## 11. Running Tests

```bash
# All unit tests (no DB needed)
PYTHONPATH=. .venv/bin/python -m pytest tests/unit/ -v --no-cov

# Single module
PYTHONPATH=. .venv/bin/python -m pytest tests/unit/test_scorer.py -v --no-cov

# With coverage
PYTHONPATH=. .venv/bin/python -m pytest tests/unit/test_scorer.py \
    --cov=cam.alerts.scorer --cov-report=term-missing

# Postgres-gated tests (requires DATABASE_URL)
PYTHONPATH=. .venv/bin/python -m pytest tests/ -m "not requires_db"
```
