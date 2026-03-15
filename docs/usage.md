# Corporate Accountability Monitor — Usage Guide

This guide explains how to run the CAM pipeline, score entities, and interpret alerts. The intended audience is analysts, researchers, and engineers who operate the system day-to-day.

---

## Table of Contents

1. [Quick Start](#1-quick-start)
2. [Ingesting Regulatory Data](#2-ingesting-regulatory-data)
3. [Flagging PE-Owned Entities](#3-flagging-pe-owned-entities)
4. [Running Daily Alert Scoring](#4-running-daily-alert-scoring)
5. [Interpreting Alert Levels](#5-interpreting-alert-levels)
6. [Generating Alerts for a Single Entity](#6-generating-alerts-for-a-single-entity)
7. [Industry Benchmarking (PE vs. Non-PE)](#7-industry-benchmarking-pe-vs-non-pe)
8. [Environment Setup](#8-environment-setup)
9. [Running Tests](#9-running-tests)

---

## 1. Quick Start

```bash
# Start all infrastructure (Postgres, Redis, MinIO)
docker-compose up -d

# Apply database migrations
DATABASE_URL=postgresql://cam:cam@localhost:5432/cam alembic upgrade head

# Ingest all WARN Act notices (all 50 states)
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
```

---

## 2. Ingesting Regulatory Data

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

## 3. Flagging PE-Owned Entities

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

## 4. Running Daily Alert Scoring

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

## 6. Generating Alerts for a Single Entity

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

## 8. Environment Setup

Copy `.env.example` to `.env` and fill in the required values:

```bash
cp .env.example .env
```

| Variable          | Required | Description                                         |
|-------------------|----------|-----------------------------------------------------|
| `DATABASE_URL`    | ✅       | PostgreSQL connection string                        |
| `EDGAR_USER_AGENT`| ✅       | Your email address (SEC EDGAR requires it)          |
| `REDIS_URL`       | ✗        | Default: `redis://localhost:6379/0`                 |
| `S3_BUCKET`       | ✗        | Default: `cam-documents`                            |
| `API_AUTH_TOKEN`  | ✗        | Required only for the REST API layer (M14)          |

---

## 9. Running Tests

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
