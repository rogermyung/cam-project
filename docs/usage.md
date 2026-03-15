# Corporate Accountability Monitor — Usage Guide

This guide explains how to run the CAM pipeline, score entities, export the dashboard, and interpret alerts. The intended audience is analysts, researchers, and engineers who operate the system day-to-day.

---

## Table of Contents

1. [Quick Start](#1-quick-start)
2. [Using Docker](#2-using-docker)
3. [Ingesting Regulatory Data](#3-ingesting-regulatory-data)
4. [Flagging PE-Owned Entities](#4-flagging-pe-owned-entities)
5. [Running Daily Alert Scoring](#5-running-daily-alert-scoring)
6. [Exporting the Static Site Dashboard](#6-exporting-the-static-site-dashboard)
7. [Generating the Weekly Email Digest](#7-generating-the-weekly-email-digest)
8. [Interpreting Alert Levels](#8-interpreting-alert-levels)
9. [Generating Alerts for a Single Entity](#9-generating-alerts-for-a-single-entity)
10. [Industry Benchmarking (PE vs. Non-PE)](#10-industry-benchmarking-pe-vs-non-pe)
11. [Environment Setup](#11-environment-setup)
12. [Running Tests](#12-running-tests)

---

## 1. Quick Start

```bash
# Start all infrastructure (Postgres, Redis, MinIO)
docker-compose up -d

# Apply database migrations
DATABASE_URL=postgresql://cam:cam@localhost:5432/cam alembic upgrade head

# Ingest WARN Act notices (all 50 states)
PYTHONPATH=. python -c "
from cam.db.session import get_session
from cam.ingestion.warn import ingest_all_states
with get_session() as db:
    summary = ingest_all_states(db=db)
    print(f'Ingested: {summary}')
"

# Run daily scoring
PYTHONPATH=. python -c "
from datetime import date
from cam.db.session import get_session
from cam.alerts.scorer import run_daily_scoring
with get_session() as db:
    scores = run_daily_scoring(score_date=date.today(), db=db)
    print(f'Scored {len(scores)} entities')
"

# Export the static site dashboard
PYTHONPATH=. python -c "
from cam.db.session import get_session
from cam.output import export_static_site
with get_session() as db:
    result = export_static_site('/tmp/cam-site', db=db)
    print(result)
"
# Open the dashboard: open /tmp/cam-site/index.html
```

---

## 2. Using Docker

The `docker-compose.yml` starts all required infrastructure services and the Celery worker and beat scheduler.

### Start all services

```bash
# Build the CAM image and start Postgres, Redis, MinIO, worker, and beat
docker-compose up -d

# Check that all services are healthy
docker-compose ps
```

### Run one-off pipeline commands inside Docker

```bash
# Apply migrations via the worker container
docker-compose run --rm worker alembic upgrade head

# Ingest WARN Act data
docker-compose run --rm worker python -c "
from cam.db.session import get_session
from cam.ingestion.warn import ingest_all_states
with get_session() as db:
    print(ingest_all_states(db=db))
"

# Run daily scoring
docker-compose run --rm worker python -c "
from datetime import date
from cam.db.session import get_session
from cam.alerts.scorer import run_daily_scoring
with get_session() as db:
    scores = run_daily_scoring(score_date=date.today(), db=db)
    print(f'Scored {len(scores)} entities')
"

# Export the dashboard to a host-mounted path
docker-compose run --rm -v /tmp/cam-site:/out worker python -c "
from cam.db.session import get_session
from cam.output import export_static_site
with get_session() as db:
    print(export_static_site('/out', db=db))
"
```

### Worker and Beat

The `worker` service runs the Celery worker; `beat` runs the Celery beat scheduler for recurring tasks. Environment variables `DATABASE_URL`, `REDIS_URL`, and `S3_ENDPOINT` are automatically set to the Docker service URLs — no `.env` editing is needed for the default Docker setup.

```bash
# Tail worker logs
docker-compose logs -f worker

# Restart a single service
docker-compose restart worker
```

### Build a fresh image after code changes

```bash
docker-compose build
docker-compose up -d
```

---

## 3. Ingesting Regulatory Data

Each ingestion module reads from a government data source and writes to the `events` table. All functions are **idempotent** — running them twice produces the same database state.

### WARN Act Notices (Layoffs)

```python
from cam.db.session import get_session
from cam.ingestion.warn import ingest_state, ingest_all_states

with get_session() as db:
    # Ingest one state
    result = ingest_state("CA", db=db)
    print(f"CA: {result}")

    # Ingest all states (~5 min)
    summary = ingest_all_states(db=db)
```

### OSHA Violations

```python
from cam.db.session import get_session
from cam.ingestion.osha import ingest_osha_violations

with get_session() as db:
    count = ingest_osha_violations(since_date="2020-01-01", db=db)
    print(f"Ingested {count} OSHA violation records")
```

### EPA Enforcement Actions

```python
from cam.db.session import get_session
from cam.ingestion.epa import ingest_epa_violations

with get_session() as db:
    count = ingest_epa_violations(since_date="2020-01-01", db=db)
```

### CFPB Consumer Complaints

```python
from cam.db.session import get_session
from cam.ingestion.cfpb import ingest_cfpb_complaints

with get_session() as db:
    count = ingest_cfpb_complaints(since_date="2020-01-01", db=db)
```

### SEC EDGAR Filings (10-K, Proxy)

```python
from cam.db.session import get_session
from cam.ingestion.edgar import ingest_company_filings

with get_session() as db:
    ingest_company_filings(cik="0000070858", db=db)  # CVS Health
```

---

## 4. Flagging PE-Owned Entities

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

## 5. Running Daily Alert Scoring

The `run_daily_scoring` function scores **all entities** that have at least one `Signal` record. It writes composite scores to the `alert_scores` table and commits once at the end.

```python
from datetime import date
from cam.db.session import get_session
from cam.alerts.scorer import run_daily_scoring, generate_alert, get_prior_score

with get_session() as db:
    today = date.today()
    scores = run_daily_scoring(score_date=today, db=db)

    print(f"Scored {len(scores)} entities")

    # Generate alerts for entities that crossed a threshold
    for score in scores:
        prior = get_prior_score(score.entity_id, before_date=today, db=db)
        alert = generate_alert(score.entity_id, score, prior, db=db)
        if alert:
            print(f"ALERT [{alert.alert_level.upper()}] {alert.canonical_name}")
            print(f"  Score: {alert.score:.3f}  (was: {alert.prior_score})")
            print(f"  Action: {alert.suggested_action}")
            print(f"  Agencies: {', '.join(alert.relevant_regulatory_body)}")
```

This is also exposed as a **Celery task** — see `cam/tasks.py`.

---

## 6. Exporting the Static Site Dashboard

`export_static_site` reads the current `alert_scores`, `entities`, and `signals` tables and writes a self-contained directory of files. The output can be hosted anywhere (S3, GitHub Pages, Netlify) or opened directly from the filesystem — no web server is required.

```python
from cam.db.session import get_session
from cam.output import export_static_site

with get_session() as db:
    result = export_static_site("/path/to/output", db=db)

# result = {"entities": 312, "alerts": 47, "files_written": 643}
print(result)
```

**Parameters:**

| Parameter    | Type              | Description                                            |
|--------------|-------------------|--------------------------------------------------------|
| `output_dir` | `str` or `Path`   | Destination directory. Created (with parents) if absent. |
| `db`         | `Session`         | SQLAlchemy session — read-only, no commit is issued.   |

**Return value:**

| Key             | Description                                         |
|-----------------|-----------------------------------------------------|
| `entities`      | Number of entities exported                         |
| `alerts`        | Number of entities with a non-None alert level      |
| `files_written` | Total files written (JSON + JS companions + HTML)   |

**Viewing the dashboard:**

```bash
# Open directly from the filesystem (works in all modern browsers)
open /path/to/output/index.html

# Or serve it with Python's built-in HTTP server
python -m http.server 8000 --directory /path/to/output
# Then open http://localhost:8000
```

**Re-running is safe:** Export is fully idempotent. Files are written atomically (temp → rename) and stale entity files from previously-exported entities that are no longer in the database are automatically removed.

For a full description of what each file contains and the technical design of the export, see [Output Layer (M14) in architecture.md](architecture.md#7-output-layer-m14).

---

## 7. Generating the Weekly Email Digest

`export_digest` produces a plaintext email body summarising new high-priority alerts and top sectors. The caller is responsible for SMTP delivery.

```python
from datetime import date, timedelta
from cam.db.session import get_session
from cam.output import export_digest

# Summarise the last 7 days
since_date = date.today() - timedelta(days=7)

with get_session() as db:
    body = export_digest(since_date, db=db)

print(body)
# Send via your preferred email library, e.g. smtplib or SendGrid
```

**Parameters:**

| Parameter    | Type    | Description                                                     |
|--------------|---------|-----------------------------------------------------------------|
| `since_date` | `date`  | Alerts with `score_date >= since_date` are included.           |
| `db`         | `Session` | SQLAlchemy session — read-only, no commit is issued.          |

**What the digest includes:**

- All critical and elevated alerts since `since_date`, each with up to two evidence snippets (≤ 120 chars each)
- Top 5 NAICS sectors by average composite score (only sectors with ≥ 3 entities)
- Period header and footer for email formatting

**Typical cron integration:**

```python
# Run every Monday at 08:00 to cover the prior week
import smtplib
from email.message import EmailMessage
from datetime import date, timedelta
from cam.db.session import get_session
from cam.output import export_digest

since_date = date.today() - timedelta(days=7)
with get_session() as db:
    body = export_digest(since_date, db=db)

msg = EmailMessage()
msg["Subject"] = f"CAM Weekly Digest — {date.today()}"
msg["From"] = "cam@example.com"
msg["To"] = "analysts@example.com"
msg.set_content(body)

with smtplib.SMTP("localhost") as s:
    s.send_message(msg)
```

---

## 8. Interpreting Alert Levels

| Level      | Threshold | Meaning                                     | Recommended Action                    |
|------------|-----------|---------------------------------------------|---------------------------------------|
| `watch`    | ≥ 0.40    | Worth monitoring; no immediate action       | Flag for weekly review                |
| `elevated` | ≥ 0.65    | Elevated risk; analyst review warranted     | Assign analyst; cross-check filings   |
| `critical` | ≥ 0.80    | Significant risk; regulatory action likely  | Escalate; consider regulatory referral|
| *(none)*   | < 0.40    | Within normal operating range               | No action                             |

Alerts only fire when the level **increases**. An entity that stays at `watch` for three weeks in a row generates exactly **one** alert — on the day it first crossed the 0.40 threshold.

For the full score composition (component weights, signal types, and module sources), see [Score Composition in architecture.md](architecture.md#7-output-layer-m14).

---

## 9. Generating Alerts for a Single Entity

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

## 10. Industry Benchmarking (PE vs. Non-PE)

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

## 11. Environment Setup

Copy `.env.example` to `.env` and fill in the required values:

```bash
cp .env.example .env
```

| Variable           | Required | Description                                              |
|--------------------|----------|----------------------------------------------------------|
| `DATABASE_URL`     | ✅       | PostgreSQL connection string                             |
| `EDGAR_USER_AGENT` | ✅       | Your email address (SEC EDGAR requires it)               |
| `REDIS_URL`        | ✗        | Default: `redis://localhost:6379/0`                      |
| `S3_BUCKET`        | ✗        | Default: `cam-documents`                                 |
| `API_AUTH_TOKEN`   | ✗        | Required only for the REST API layer (M14)               |

When running via `docker-compose`, `DATABASE_URL`, `REDIS_URL`, and `S3_ENDPOINT` are automatically set to the container network addresses — you only need `.env` for `EDGAR_USER_AGENT` and any optional overrides.

---

## 12. Running Tests

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
