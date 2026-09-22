# Corporate Accountability Monitor — Usage Guide

This guide explains how to run the CAM pipeline, score entities, and interpret alerts. The intended audience is analysts, researchers, and engineers who operate the system day-to-day.

---

## Table of Contents

1. [Quick Start](#1-quick-start)
2. [Pipeline CLI Reference](#2-pipeline-cli-reference)
3. [Running with Docker](#3-running-with-docker)
4. [Scheduled Automation (GitHub Actions)](#4-scheduled-automation-github-actions)
5. [Interpreting Alert Levels](#5-interpreting-alert-levels)
6. [Entity Resolution and Jev Alignment](#6-entity-resolution-and-jev-alignment)
7. [Screening Judgments (Proxy Topics, Merger Factors)](#7-screening-judgments-proxy-topics-merger-factors)
8. [Flagging PE-Owned Entities](#8-flagging-pe-owned-entities)
9. [Industry Benchmarking (PE vs. Non-PE)](#9-industry-benchmarking-pe-vs-non-pe)
10. [Generating Alerts for a Single Entity](#10-generating-alerts-for-a-single-entity)
11. [Environment Setup](#11-environment-setup)
12. [Database Requirements](#12-database-requirements)
13. [Running Tests](#13-running-tests)

---

## 1. Quick Start

```bash
# Start all infrastructure (Postgres, Redis, MinIO)
docker-compose up -d

# Create the S3/MinIO bucket EDGAR writes raw filings into (one-time).
# Without it, EDGAR ingestion fails every filing with "NoSuchBucket" and
# commits zero events. The bucket name must match S3_BUCKET (default below).
docker exec cam-project-minio-1 sh -c \
  'mc alias set local http://localhost:9000 minioadmin minioadmin && \
   mc mb --ignore-existing local/cam-documents'

# Apply database migrations
DATABASE_URL=postgresql://cam:cam@localhost:5432/cam alembic upgrade head

# Seed the entities table from SEC EDGAR (one-time on a fresh DB; idempotent)
DATABASE_URL=postgresql://cam:cam@localhost:5432/cam \
  EDGAR_USER_AGENT=you@example.com \
  python -m cam.entrypoint seed

# Ingest all regulatory sources (last 30 days by default)
DATABASE_URL=postgresql://cam:cam@localhost:5432/cam \
  EDGAR_USER_AGENT=you@example.com \
  python -m cam.entrypoint ingest --source all

# Analyze: write Signal rows from ingested Events (REQUIRED before score —
# the scorer reads Signals, so skipping this yields an empty dashboard)
DATABASE_URL=postgresql://cam:cam@localhost:5432/cam \
  EDGAR_USER_AGENT=you@example.com \
  python -m cam.entrypoint analyze --date today

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

## 6. Entity Resolution and Jev Alignment

Every event an ingestion module writes has to be attached to a canonical
entity. The resolver (`cam/entity/resolver.py`) works through five steps and
stops at the first hit:

| Step | Mechanism | Outcome |
|------|-----------|---------|
| 1 | Exact `(raw_name, source)` alias hit | `method='exact'`, confidence 1.0 |
| 2 | Normalised exact match over all aliases **and** every `Entity.canonical_name` | `method='exact'`, confidence 1.0 |
| 3 | rapidfuzz `token_sort_ratio` over the same pool | `method='fuzzy'` above `ENTITY_FUZZY_THRESHOLD`; review queue above `ENTITY_REVIEW_THRESHOLD` |
| 4 | External lookup (Jev alignment, opt-in) | `method='api'` on a merge verdict; review queue on a review verdict |
| 5 | Unresolved | event keeps `entity_id=NULL` |

Steps 1–3 cost nothing and run always. Step 4 runs only when
`ENTITY_JEV_ENABLED=true`.

### Why step 4 exists

Steps 1–3 compare strings. They cannot link `SAFEWAY STORES 4680` to
`Albertsons Companies, Inc.` — the names share no tokens, so no
`token_sort_ratio` threshold will ever connect them. This is the main reason
WARN, OSHA, EPA and CFPB events resolve to nothing while EDGAR events resolve
fine: EDGAR filers *are* the SEC-seeded entities, and regulatory filers
usually are not.

`cam/entity/jev_align.py` keeps rapidfuzz as a cheap candidate generator and
asks [TypeSafe's Jev](https://docs.typesafe.ai/) to adjudicate the shortlist.
Per candidate it asks a three-level score — different company / related or
unclear / the same company — plus two corroborating yes-no questions, all in
one batched request. The rounded level *is* the decision:

| Level | Verdict | Effect |
|-------|---------|--------|
| 2 | merge | event links to the entity; an alias is written so the next hit is free |
| 1 | review | entity review queue, for a human to accept or reject |
| 0 | reject | event stays unlinked |

A wholly owned subsidiary, brand, division or single facility counts as **the
same company**, so a violation at `Tyson Fresh Meats` is attributed to
`Tyson Foods, Inc.`

### Enabling it

```bash
ENTITY_JEV_ENABLED=true
TYPESAFE_API_KEY=...            # from https://console.typesafe.ai/
ENTITY_JEV_CANDIDATE_LIMIT=5    # candidates per raw name; 3 questions each
```

No code changes are needed — `bulk_resolve()` picks the lookup up from
settings, so every ingestion module gets it at once. With the flag off, or the
service unreachable, resolution behaves exactly as it did before: a transient
TypeSafe error is logged and the name falls through to step 5 rather than
failing the ingest.

A **rejected credential is the exception and fails the run.** A bad
`TYPESAFE_API_KEY` rejects every name in the batch, so degrading quietly would
report it as "Jev found no matches" — indistinguishable from a healthy
pipeline that resolved nothing. Given how often this project's real bug has
been a silently empty run, a 401 is made loud on purpose.

Work the review queue with the existing CLI:

```bash
PYTHONPATH=. .venv/bin/python -m cam.entity.cli list
```

### Checking its judgment

Typed output guarantees the shape of the answer, not its correctness. The
model's accuracy is measured against a hand-labelled gold set
(`tests/fixtures/entity/alignment_gold.json`), which is excluded from the
default test run:

```bash
export TYPESAFE_API_KEY="$YOUR_REAL_KEY"   # not a placeholder; a bad key 401s
PYTHONPATH=. .venv/bin/python -m pytest tests/smoke/test_jev_align.py -m live -v -s
```

That prints a per-case table and enforces two floors: 80% verdict accuracy,
and **zero** merges onto the wrong entity. Extend the gold set rather than
loosening the floors — a wrong merge attributes one company's violations to
another, which is the one error this system cannot absorb.

The gate is "is the variable non-empty", not "is the key valid", so an
invalid key fails the run rather than skipping it. That is deliberate: a
silently skipped quality check is worth nothing.

---

## 7. Screening Judgments (Proxy Topics, Merger Factors)

Two analysis modules classify free text with substring tables, and both have a
Jev-backed alternative behind the same seam.

| Module | Judgment | Default | With Jev |
|--------|----------|---------|----------|
| M9 `proxy_parser` | Which of 7 topics a shareholder proposal is about | first matching keyword list wins | one `Choice` per proposal, batched per filing |
| M10 `merger_screener` | Which of 5 vertical risk factors a deal exhibits | substring match per factor | one `Noul` per factor, one request |

### Why

Substring matching has three failure modes the gold set
(`tests/fixtures/analysis/screening_gold.json`) reproduces, with measured
keyword accuracy of **60%** on topics and **62%** on factors:

- **Negation.** "The acquirer does not operate a marketplace and has no
  insurance business" triggers both `platform_plus_seller` and
  `payer_plus_provider`, scoring 3.0 of 9.0 on a deal that explicitly denies
  the theory.
- **Bare-word coincidence.** A deal funded by "a life insurance policy held on
  its founder" scores 1.5 of 9.0 on `payer_plus_provider`.
- **Order dependence.** `classify_proposal_topic` returns the first keyword
  list that matches, so a warehouse-safety proposal mentioning "audit
  findings" is filed under `supply_chain`, not `worker_welfare`. The comment
  above `_TOPIC_KEYWORDS` exists to manage exactly this.

### What stays in code

Only the judgments move. Vote percentages, dollar amounts, the management
recommendation, `flag_escalating_minority`, the factor weights, the score
normalisation, the precedent citations, the review-focus text — all unchanged.

The **HHI > 2500 test is arithmetic and stays in code**, and the model is told
not to estimate it. An explicit numeric HHI above the threshold triggers
`high_hhi_either_market` regardless of what the model made of the prose.

### Enabling it

```bash
ANALYSIS_JEV_ENABLED=true
TYPESAFE_API_KEY=...            # shared with entity alignment
MERGER_FACTOR_THRESHOLD=0.5     # 0.5 keeps weighting identical to the boolean detector
```

Both modules degrade on a transient TypeSafe error by **falling back to their
keyword implementation** — a degraded Jev costs accuracy, not availability. A
rejected credential is re-raised, for the same reason as in §6: it rejects
every subsequent call, and a quiet fallback would hide the misconfiguration.

`MergerRiskScore.factor_confidence` carries the raw per-factor probabilities.
`score` is still computed from the thresholded factor set, so enabling Jev
changes which factors fire but not how they are weighted. The probabilities
are stored so a probability-weighted composite can be evaluated later against
recorded judgments, without re-running inference.

### Checking its judgment

The live check measures Jev **against the keyword baseline on the same cases
in the same run**, and fails if Jev does not beat it — an absolute accuracy
figure would not tell you whether the swap was worth paying for:

```bash
export TYPESAFE_API_KEY="$YOUR_REAL_KEY"
PYTHONPATH=. .venv/bin/python -m pytest tests/smoke/test_jev_screening.py -m live -v -s --no-cov
```

The keyword baseline itself is measured offline, with no key and no network:

```bash
PYTHONPATH=. .venv/bin/python -m pytest tests/unit/test_jev_screening.py -k Baseline -v --no-cov
```

---

## 8. Flagging PE-Owned Entities

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

## 9. Industry Benchmarking (PE vs. Non-PE)

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
            f"p = {row['p_value']:.4f}"
            if row["p_value"]
            else "p = N/A"
        )
```

`event_type` accepts `"warn"` (layoff notices) or `"bankruptcy"` (PACER filings).

Only sectors with **more than 10 PE-owned entities** are included, per the statistical sampling requirement.

---

## 10. Generating Alerts for a Single Entity

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

## 11. Environment Setup

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

## 12. Database Requirements

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

## 13. Running Tests

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
