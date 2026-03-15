# Corporate Accountability Monitor — Technical Architecture

This document describes the system architecture, data flows, module responsibilities, and key design decisions in the Corporate Accountability Monitor (CAM).

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Module Dependency Graph](#2-module-dependency-graph)
3. [Data Layer](#3-data-layer)
4. [Ingestion Pipeline (M2–M5, M11)](#4-ingestion-pipeline-m2m5-m11)
5. [Analysis Pipeline (M6–M10, M12)](#5-analysis-pipeline-m6m10-m12)
6. [Alert Scoring Engine (M13)](#6-alert-scoring-engine-m13)
7. [Output Layer (M14)](#7-output-layer-m14)
8. [Entity Resolution (M1)](#8-entity-resolution-m1)
9. [Task Queue Architecture](#9-task-queue-architecture)
10. [Configuration](#10-configuration)
11. [Key Design Decisions](#11-key-design-decisions)
12. [Database Schema](#12-database-schema)

---

## 1. System Overview

CAM is a Python data pipeline that:

1. **Ingests** regulatory filings, violations, and court records from US government sources
2. **Resolves** raw company names to canonical entity identifiers
3. **Analyzes** 10-K filings, earnings calls, proxy statements, and cross-agency violation patterns
4. **Scores** entities daily using a weighted composite of six signal components
5. **Alerts** analysts when an entity's risk score crosses a severity threshold

```
┌──────────────────────────────────────────────────────────────┐
│  Government Sources                                          │
│  EDGAR  OSHA  EPA  CFPB  WARN Act  PACER  HSR               │
└──────────────────────────────────────────────────────────────┘
                         │  (HTTP / FTP)
                         ▼
┌──────────────────────────────────────────────────────────────┐
│  Ingestion Layer (cam/ingestion/)                            │
│  M2 EDGAR │ M3 OSHA │ M4 EPA │ M5 CFPB │ M10 HSR │ M11 WARN │
└──────────────────────────────────────────────────────────────┘
                         │  → events table
                         ▼
┌──────────────────────────────────────────────────────────────┐
│  Entity Resolution (cam/entity/)   M1                        │
│  Fuzzy matching → canonical entity IDs                       │
└──────────────────────────────────────────────────────────────┘
                         │  → entities, entity_aliases tables
                         ▼
┌──────────────────────────────────────────────────────────────┐
│  Analysis Layer (cam/analysis/)                              │
│  M6 Cross-agency │ M7 10-K NLP │ M8 Earnings │ M9 Proxy     │
│  M10 Merger      │ M12 PE Correlator                         │
└──────────────────────────────────────────────────────────────┘
                         │  → signals table
                         ▼
┌──────────────────────────────────────────────────────────────┐
│  Alert Scoring Engine (cam/alerts/)   M13                    │
│  Weighted composite → alert_scores table → Alert records     │
└──────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────┐
│  Output Layer (cam/output/)   M14                            │
│  FastAPI REST  │  Jinja2 Dashboard  │  Email Digest          │
└──────────────────────────────────────────────────────────────┘
```

---

## 2. Module Dependency Graph

```
M0 (Scaffolding)
│
├── M1 (Entity Resolution)
│   └── used by: all ingestion modules
│
├── M2 (EDGAR Ingestion)
├── M3 (OSHA Ingestion)    ─┐
├── M4 (EPA Ingestion)      ├── M6 (Cross-Agency Aggregation)
├── M5 (CFPB Ingestion)    ─┘         │
│                                      │
├── M7 (10-K Risk Language NLP)        │
├── M8 (Earnings Call NLP)             │
├── M9 (Proxy Statement Parser)        │
├── M10 (HSR Merger Screener)          │
├── M11 (WARN Act Ingestion)           │
│                                      │
└── M12 (PE/Bankruptcy Correlator) ────┤
                                       │
                                  M13 (Alert Scoring)
                                       │
                                  M14 (Output Layer)
```

---

## 3. Data Layer

### Database: PostgreSQL

All structured data lives in PostgreSQL, accessed via **SQLAlchemy 2.x ORM**. Schema migrations are managed by **Alembic**.

Primary tables:

| Table            | Purpose                                             |
|------------------|-----------------------------------------------------|
| `entities`       | Canonical company records (one row per company)     |
| `entity_aliases` | Raw name → entity_id mappings with confidence score |
| `events`         | All regulatory events from all ingestion sources    |
| `signals`        | NLP and analytical scores per entity                |
| `alert_scores`   | Daily composite scores; unique per entity per date  |

### Object Store: S3 / MinIO

Raw documents (SEC filings, PACER dockets) are stored in S3-compatible object storage before parsing. The `raw_url` column on the `events` table links back to the stored object.

### Task Queue: Celery + Redis

Long-running ingestion and scoring tasks are dispatched via Celery workers. Redis serves as both the broker and result backend.

---

## 4. Ingestion Pipeline (M2–M5, M11)

All ingestion modules follow the same pattern:

```python
def ingest_<source>(since_date, *, db: Session) -> int:
    records = _fetch_from_api(since_date)          # HTTP fetch with tenacity retry
    for record in records:
        entity = bulk_resolve(record.name, db=db, commit=False)  # M1
        event = Event(entity_id=entity.id, ...)
        db.add(event)
    db.commit()   # single commit at the end
    return len(records)
```

Key conventions:
- **HTTP**: `httpx` client with `tenacity` retry (3 attempts, exponential backoff)
- **Idempotency**: Each event has an `idempotency_key` derived from source fields. Duplicate keys are silently skipped via `INSERT ... ON CONFLICT DO NOTHING` (PostgreSQL) or a pre-insert existence check (SQLite tests).
- **Transaction ownership**: The ingestion function owns the single `db.commit()`. Helpers use `db.flush()` only.
- **NAICS codes**: Events carry the raw NAICS code from the source; entity-level NAICS is set during entity resolution.

### WARN Act (M11) specifics

WARN Act data is scraped state-by-state from DOL's WARN Act database. Each state URL is fetched independently, allowing parallel or sequential ingestion. Idempotency keys for no-date records use SHA-256 of the raw row content to prevent collisions.

---

## 5. Analysis Pipeline (M6–M10, M12)

Analysis modules transform ingested data into **Signal** records written to the `signals` table.

### Signal Schema

```python
Signal(
    entity_id = UUID,       # which entity
    source    = str,        # module identifier ('edgar_10k', 'earnings_call', ...)
    signal_type = str,      # semantic type (see below)
    score       = float,    # 0.0–1.0 normalized severity
    evidence    = str,      # human-readable explanation
    signal_date = date,     # effective date of the signal
)
```

### Signal Types by Module

| Module | signal_type                  | Meaning                                               |
|--------|------------------------------|-------------------------------------------------------|
| M6     | `cross_agency_composite`     | Weighted multi-agency violation score (0–1)           |
| M7     | `risk_language_expansion`    | NLP score for new risk language in 10-K filings       |
| M8     | `earnings_divergence`        | Gap between management tone and financial results     |
| M9     | `proxy_escalation`           | Escalating pay ratios or minority shareholder action  |
| M10    | `merger_vertical_risk`       | Antitrust risk score for pending mergers              |
| M12    | `pe_owned`                   | Binary flag (1.0) indicating PE ownership             |

### Cross-Agency Aggregation (M6)

M6 queries the `events` table for OSHA, EPA, and CFPB events, computes per-agency rates relative to industry benchmarks, and combines them using configurable weights (set in `cam/config.py`):

```
weight_osha_rate     = 0.25
weight_epa_rate      = 0.20
weight_cfpb_spike    = 0.20
weight_agency_overlap = 0.35  (non-linear bonus for concurrent multi-agency signals)
```

### 10-K Risk Language NLP (M7)

Uses a sentence-transformer encoder and zero-shot BART-MNLI classifier to detect new or expanded risk language in SEC 10-K filings relative to prior-year filings. The `risk_language_expansion` score represents the proportion of risk sentences that are semantically novel.

### PE/Bankruptcy Correlator (M12)

Compares WARN Act and bankruptcy filing rates between PE-owned and non-PE-owned companies within the same 2-digit NAICS sector. Uses Fisher's exact test (one-sided, `alternative="greater"`) for statistical significance. p-values are only reported for sectors with **more than 10 PE-owned entities**.

---

## 6. Alert Scoring Engine (M13)

### Score Computation

`compute_entity_score(entity_id, score_date, *, db)` reads the most recent Signal for each of six component types and computes a weighted sum:

```
composite = Σ (weight[component] × score[component])
composite = clamp(composite, 0.0, 1.0)
```

Signal lookup prioritizes signals with explicit `signal_date` (most recent first), then falls back to `created_at` order.

Component signal map:

```python
_COMPONENT_SIGNAL_MAP = {
    "cross_agency_composite":  "cross_agency_composite",  # M6
    "risk_language_expansion": "risk_language_expansion", # M7
    "earnings_divergence":     "earnings_divergence",     # M8
    "proxy_escalation":        "proxy_escalation",        # M9
    "merger_vertical_risk":    "merger_vertical_risk",    # M10
    "pe_warn_flag":            "pe_owned",                # M12
}
```

Missing signals contribute **0.0**, so the system scores entities even when only a subset of upstream modules have produced output.

### Alert Level Assignment

```
score ≥ 0.80 → critical
score ≥ 0.65 → elevated
score ≥ 0.40 → watch
score <  0.40 → None (no alert)
```

### Alert Generation

`generate_alert(entity_id, score, prior_score, *, db)` fires only when the alert level **increases**:

```python
_LEVEL_ORDER = {None: 0, "watch": 1, "elevated": 2, "critical": 3}
fires = _LEVEL_ORDER[new_level] > _LEVEL_ORDER[prior_level]
```

This ensures zero duplicate alerts per entity per threshold within any time period.

### Upsert Semantics

`compute_entity_score` uses a **query-then-update** pattern to avoid violating the `UNIQUE(entity_id, score_date)` constraint on `alert_scores`. If a row already exists for the same entity/date, it is updated in place rather than a new row being inserted.

### Daily Scoring Loop

`run_daily_scoring(score_date, *, db)` queries all entity IDs with at least one Signal, calls `compute_entity_score` for each, and issues a single `db.commit()` at the end. Individual entity failures are caught and logged; the rest of the batch continues.

---

## 7. Output Layer (M14)

*(Planned — not yet implemented)*

### REST API (FastAPI)

```
GET  /entities                          # All entities with current scores
GET  /entities/{id}                     # Detail: scores + signals + evidence
GET  /entities/{id}/timeline            # Score history
GET  /alerts?level=elevated&since=date  # Recent alerts
GET  /alerts/{id}                       # Alert detail
GET  /reports/pe-comparison/{naics}     # PE vs non-PE for a sector
GET  /reports/industry-benchmarks       # Violation rates by industry
```

Authentication: Bearer token via `Authorization: Bearer <API_AUTH_TOKEN>`.

### Dashboard

Server-rendered HTML using Jinja2 templates (no JavaScript framework). Priority views: alert feed, entity detail, industry view, merger watch.

### Weekly Digest

Plaintext email digest (SMTP via `cam/config.py`) summarizing new critical/elevated alerts, high-scoring mergers, and sectors with rising aggregate scores.

---

## 8. Entity Resolution (M1)

The entity resolver (`cam/entity/resolver.py`) maps raw company name strings to canonical `Entity` records using:

1. **Exact match** on the `entity_aliases` table
2. **Fuzzy match** (token-sort ratio, configurable threshold in `config.py`)
3. **Review queue** — ambiguous matches above a lower threshold are flagged in the `signals` table with `signal_type="entity_review_queue"` for human review

Key thresholds (from `cam/config.py`):
- `entity_fuzzy_threshold = 0.85` — auto-accept above this
- `entity_review_threshold = 0.65` — queue for review above this, below fuzzy threshold

---

## 9. Task Queue Architecture

Celery tasks in `cam/tasks.py` wrap the ingestion and scoring functions for scheduled execution:

```
cam.tasks
├── ingest_osha_task     → cam.ingestion.osha.ingest_osha_violations
├── ingest_epa_task      → cam.ingestion.epa.ingest_epa_violations
├── ingest_cfpb_task     → cam.ingestion.cfpb.ingest_cfpb_complaints
├── ingest_warn_task     → cam.ingestion.warn.ingest_all_states
└── daily_scoring_task   → cam.alerts.scorer.run_daily_scoring
```

Workers are started with `celery -A cam.tasks worker`. Scheduled runs are configured via Celery Beat (not yet wired up in M13).

---

## 10. Configuration

All configuration is loaded from environment variables via **Pydantic Settings** (`cam/config.py`). No secrets are hardcoded.

```python
from cam.config import get_settings
settings = get_settings()
settings.alert_threshold_watch    # 0.40
settings.alert_threshold_elevated # 0.65
settings.alert_threshold_critical # 0.80
```

Alert thresholds and aggregation weights can be overridden via environment variables (e.g. `ALERT_THRESHOLD_WATCH=0.35`).

---

## 11. Key Design Decisions

### Transaction Ownership Convention
Every function that writes to the database follows the same rule: **helpers call `db.flush()`; the top-level function calls `db.commit()`**. This prevents partial writes and gives callers full control over transaction boundaries.

### SQLite for Tests
All unit tests use an in-memory SQLite database. This avoids the need for a live PostgreSQL instance. JSONB columns use `sa.JSON().with_variant(JSONB(), "postgresql")` for SQLite compatibility.

### Graceful Degradation in Scoring
`compute_entity_score` never raises for missing signals — it defaults to 0.0. This means the system can score entities with partial upstream coverage and produce useful (if conservative) scores during onboarding.

### Idempotent Ingestion
Every ingestion module uses idempotency keys derived from source fields. Running the same ingestion twice is safe and produces no duplicates. This makes backfills, retries, and scheduled re-runs safe.

### Threshold-Based Alert Deduplication
Alert generation is level-based, not score-based. An entity at 0.42 (watch) on Monday that scores 0.48 (still watch) on Tuesday does not generate a second alert. Only level *increases* produce alerts, guaranteeing at most one alert per entity per threshold crossing.

### scipy for Statistical Tests
Fisher's exact test (M12) requires `scipy`. It is listed separately in `requirements-dev.txt` and must be installed explicitly: `uv pip install "scipy>=1.13,<2.0"`.

---

## 12. Database Schema

```
entities
├── id               UUID (PK)
├── canonical_name   TEXT NOT NULL
├── ticker           VARCHAR(20)
├── lei              VARCHAR(20)
├── ein              VARCHAR(10)
├── naics_code       VARCHAR(10)
├── created_at       TIMESTAMPTZ
└── updated_at       TIMESTAMPTZ

entity_aliases
├── id               UUID (PK)
├── entity_id        UUID → entities.id
├── raw_name         TEXT NOT NULL
├── source           VARCHAR(50)
├── confidence       FLOAT
└── UNIQUE(raw_name, source)

events
├── id               UUID (PK)
├── entity_id        UUID → entities.id  (nullable)
├── source           VARCHAR(50)          e.g. 'osha', 'epa', 'warn'
├── event_type       VARCHAR(50)          e.g. 'violation', 'warn_notice'
├── event_date       DATE
├── penalty_usd      NUMERIC(18,2)
├── description      TEXT
├── raw_url          TEXT
├── raw_json         JSONB
└── ingested_at      TIMESTAMPTZ

signals
├── id               UUID (PK)
├── entity_id        UUID → entities.id  (nullable)
├── source           VARCHAR(50)
├── signal_type      VARCHAR(100)         indexed
├── signal_date      DATE
├── score            FLOAT                0.0–1.0
├── evidence         TEXT
├── document_url     TEXT
└── created_at       TIMESTAMPTZ

alert_scores
├── id               UUID (PK)
├── entity_id        UUID → entities.id
├── score_date       DATE
├── composite_score  FLOAT
├── component_scores JSONB
├── alert_level      VARCHAR(20)          'watch'|'elevated'|'critical'|NULL
├── created_at       TIMESTAMPTZ
└── UNIQUE(entity_id, score_date)
```
