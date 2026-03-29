# Corporate Accountability Monitor — Project Plan

## Purpose

This document is the authoritative implementation plan for the Corporate Accountability Monitor (CAM) system. It is structured for use by Claude Code or any AI coding agent. Each module is defined with clear inputs, outputs, acceptance criteria, and test requirements.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA INGESTION LAYER                     │
│  EDGAR API │ OSHA API │ EPA API │ CFPB API │ PACER │ State  │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                   ENTITY RESOLUTION LAYER                    │
│         Normalize company names → canonical entity IDs       │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                     ANALYSIS LAYER                           │
│  Violation Aggregation │ NLP Signals │ Merger Screening      │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                      ALERT LAYER                             │
│         Threshold scoring → structured alert output          │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                      OUTPUT LAYER                            │
│        Dashboard │ API │ Digest emails │ Export formats       │
└─────────────────────────────────────────────────────────────┘
```

**Language:** Python 3.11+
**Storage:** PostgreSQL (structured data) + S3-compatible object store (raw documents)
**Queue:** Redis-backed task queue (Celery)
**Testing:** pytest, with fixtures for all external API calls (no live API calls in tests)
**Config:** All credentials and thresholds in environment variables; never hardcoded

---

## Module Index

| ID | Module | Depends On | Status |
|----|--------|------------|--------|
| M0 | Project Scaffolding | — | ✅ Complete (#16) |
| M1 | Entity Resolution | M0 | ✅ Complete (#17) |
| M2 | EDGAR Ingestion | M0, M1 | ✅ Complete (#18) |
| M3 | OSHA Ingestion | M0, M1 | ✅ Complete (#19) |
| M4 | EPA Ingestion | M0, M1 | ✅ Complete (#20) |
| M5 | CFPB Ingestion | M0, M1 | ✅ Complete (#22) |
| M6 | Cross-Agency Aggregation | M2–M5 | ✅ Complete (#23) |
| M7 | 10-K Risk Language NLP | M2 | ✅ Complete (#24) |
| M8 | Earnings Call NLP | M2 | ✅ Complete (#25) |
| M9 | Proxy Statement Parser | M2 | ✅ Complete (#26) |
| M10 | HSR Merger Screener | M0, M1 | ✅ Complete (#27) |
| M11 | WARN Act Ingestion | M0, M1 | ✅ Complete (#28) |
| M12 | PE/Bankruptcy Correlator | M11, M0 | ✅ Complete (#29) |
| M13 | Alert Scoring Engine | M6–M12 | ✅ Complete (#30) |
| M14 | Output Layer | M13 | 🔄 In Progress |
| M15 | Pipeline Resilience & DLQ | M2–M5, M11 | TODO |
| M16 | Entity Resolution Review Workflow | M1, M15 | TODO |

---

## M14 — Output Layer

### Goal
Expose the scored data through a React dashboard, a FastAPI JSON endpoint, and a weekly digest email. The dashboard is built in React/TypeScript, Vite-built into `site/`, and served via GitHub Pages.

### Status
In progress. React + shadcn/ui dashboard is scaffolded (PR #47). The remaining work is connecting it to live data from the database and ensuring the pipeline populates all fields the frontend expects.

### Key Deliverables
- `GET /api/entities` — paginated entity list with latest `alert_level` and `composite_score`
- `GET /api/entities/{id}` — full entity detail: events, signals, scores, trends
- `GET /api/dashboard-summary` — aggregate counts (total entities, alerts by level, top risers)
- Weekly HTML digest rendered via Jinja2 template, emailed via SMTP
- `site/data/*.json` written by `cam/output/export.py` for GitHub Pages static hosting

### Acceptance Criteria
- [ ] Dashboard shows data for all seeded entities with non-null composite scores
- [ ] Dashboard correctly reflects current `alert_level` from `alert_scores` table
- [ ] `GET /api/entities` returns < 200 ms for ≤ 500 entities
- [ ] Digest email renders without errors; weekly Celery beat task fires correctly
- [ ] `npm run build` produces `site/` with no TypeScript errors

---

## M15 — Pipeline Resilience & Dead Letter Queue

### Goal

The ingestion pipeline currently fails silently or stops entirely when external APIs return errors, entity resolution fails, or individual records are malformed. This module makes every ingestion source resilient to intermittent failures, enables partial ingestion (pick up where you left off), and provides a queryable dead letter queue (DLQ) so operators can examine and replay failed records.

### Problem Statement

Current failure modes observed in production:
- API timeouts crash the entire batch, losing all progress for that run
- Entity resolution failures (`entity_id = NULL`) silently produce orphaned events that are invisible in the dashboard
- No way to distinguish "permanently bad record" from "transient network glitch" in logs
- Re-running ingestion after a partial failure repeats all successful work unnecessarily (slow)
- Manual review queue (stored in `signals` as `signal_type='entity_review_queue'`) has no associated workflow to process it

### New Database Tables

```sql
-- Dead letter queue: one row per failed record across all sources
CREATE TABLE ingest_failures (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source      TEXT NOT NULL,          -- 'osha', 'edgar', 'epa_tri', etc.
    run_id      UUID NOT NULL,          -- ties failures to a specific pipeline run
    raw_key     TEXT,                   -- idempotency key from the source record (e.g. activity_nr)
    raw_json    JSONB NOT NULL,         -- full original record as received
    error_type  TEXT NOT NULL,          -- 'entity_resolution', 'validation', 'db_write', 'api_error'
    error_msg   TEXT NOT NULL,
    traceback   TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_retry  TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,            -- set when manually replayed or dismissed
    created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_ingest_failures_source ON ingest_failures(source);
CREATE INDEX idx_ingest_failures_run_id ON ingest_failures(run_id);
CREATE INDEX idx_ingest_failures_error_type ON ingest_failures(error_type);
CREATE INDEX idx_ingest_failures_resolved_at ON ingest_failures(resolved_at)
    WHERE resolved_at IS NULL;          -- fast query for open failures

-- Checkpoint: track progress of long-running ingestion runs
CREATE TABLE ingest_checkpoints (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source      TEXT NOT NULL,
    run_id      UUID NOT NULL,
    checkpoint  JSONB NOT NULL,         -- source-specific cursor (page offset, date, CIK, etc.)
    records_ok  INTEGER NOT NULL DEFAULT 0,
    records_err INTEGER NOT NULL DEFAULT 0,
    started_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now(),
    completed_at TIMESTAMPTZ,
    UNIQUE(source, run_id)
);
```

### Key Functions

```python
# cam/ingestion/dlq.py

def record_failure(
    db: Session,
    source: str,
    run_id: UUID,
    raw_record: dict,
    error_type: str,            # 'entity_resolution' | 'validation' | 'db_write' | 'api_error'
    exc: Exception,
    raw_key: str | None = None,
) -> IngestFailure:
    """
    Write a failed record to the DLQ.  Never raises — if the DLQ write itself fails,
    logs to stderr and returns None so the caller can keep processing other records.
    """

def open_failures(
    db: Session,
    source: str | None = None,
    error_type: str | None = None,
    limit: int = 100,
) -> list[IngestFailure]:
    """Return unresolved DLQ entries, optionally filtered by source or error type."""

def mark_resolved(db: Session, failure_ids: list[UUID], note: str = "") -> int:
    """Mark DLQ entries as resolved (manually processed or dismissed). Returns count."""

def replay_failures(
    db: Session,
    failure_ids: list[UUID],
    ingest_fn: Callable[[dict, Session], bool],
) -> ReplayResult:
    """
    Attempt to re-process DLQ entries through the original ingestion function.
    Updates retry_count and last_retry regardless of outcome.
    Marks resolved only on success.
    """


# cam/ingestion/checkpoint.py

def save_checkpoint(
    db: Session,
    source: str,
    run_id: UUID,
    cursor: dict,               # source-specific: {"page": 5} or {"cik": "0001234"} etc.
    records_ok: int,
    records_err: int,
) -> None:
    """Upsert checkpoint. Called periodically during long ingestion runs."""

def load_checkpoint(
    db: Session,
    source: str,
    run_id: UUID | None = None, # None = load latest incomplete run
) -> dict | None:
    """
    Return the cursor dict from the latest incomplete checkpoint for this source,
    or None if no checkpoint exists (start from beginning).
    """

def complete_checkpoint(db: Session, source: str, run_id: UUID) -> None:
    """Mark a run's checkpoint as completed."""
```

### Resilience Patterns to Apply to All Ingestion Modules

These patterns must be applied consistently across M2 (EDGAR), M3 (OSHA), M4 (EPA), M5 (CFPB), and M11 (WARN):

#### 1. Per-Record DLQ on Entity Resolution Failure

Currently, if `bulk_resolve` cannot resolve a company name, the code logs a warning and either drops the record or inserts with `entity_id=NULL`. Both outcomes are silent failures.

**Required behavior:**
```python
# Before (current — silent drop)
result = resolve(raw_name, source=source)
if result.entity_id is None:
    logger.warning(f"Unresolved: {raw_name}")
    stats.errors += 1
    continue

# After (DLQ — record preserved, operator can replay)
result = resolve(raw_name, source=source)
if result.entity_id is None and not result.needs_review:
    record_failure(
        db, source=source, run_id=run_id, raw_record=record,
        error_type="entity_resolution",
        exc=EntityResolutionError(f"No match: conf={result.confidence:.2f}"),
        raw_key=record.get("idempotency_key"),
    )
    stats.errors += 1
    continue
```

Records that `needs_review=True` go to the entity review queue (existing `signals` table), not the DLQ — they are expected and require a different workflow (see M16).

#### 2. Partial Ingestion Checkpointing

For sources that paginate (CFPB, EDGAR index scan) or iterate over a large list (OSHA CSV rows, WARN states), write a checkpoint every N records so a restart can resume mid-run.

```python
# In each ingestion loop:
CHECKPOINT_EVERY = 500  # records

for i, record in enumerate(records):
    try:
        process(record)
        stats.ok += 1
    except Exception as exc:
        record_failure(db, ..., exc=exc)
        stats.err += 1

    if i % CHECKPOINT_EVERY == 0:
        save_checkpoint(db, source=source, run_id=run_id,
                        cursor={"offset": i}, records_ok=stats.ok, records_err=stats.err)

complete_checkpoint(db, source=source, run_id=run_id)
```

On startup, each ingestion function calls `load_checkpoint()`. If a cursor is returned, it skips ahead to that offset instead of starting from row 0.

#### 3. Per-Entity Transaction Isolation

A single bad record must not roll back progress for the entire batch. Each entity's records must be wrapped in a savepoint:

```python
for entity_id, records in grouped_by_entity.items():
    try:
        with db.begin_nested():   # SAVEPOINT
            for r in records:
                db.add(Event(...))
            db.flush()
        stats.ok += len(records)
    except Exception as exc:
        # Savepoint rolls back only this entity's writes
        for r in records:
            record_failure(db, ..., raw_record=r, exc=exc)
        stats.err += len(records)
```

#### 4. Circuit Breaker Per External API

Prevent a down API from consuming the full retry budget on every record. Each ingestion source gets a lightweight circuit breaker:

```python
# cam/ingestion/circuit_breaker.py

class CircuitBreaker:
    """
    Three states: CLOSED (normal), OPEN (failing fast), HALF_OPEN (testing).
    Opens after `failure_threshold` consecutive errors.
    Resets to HALF_OPEN after `recovery_timeout` seconds.
    """
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: int = 300): ...

    def call(self, fn: Callable, *args, **kwargs):
        """
        Execute fn. If breaker is OPEN, raises CircuitOpenError immediately.
        Tracks success/failure to manage state transitions.
        """
```

A global registry maps source names to their breaker instance. All `httpx` calls inside ingestion modules go through their breaker. When a breaker opens, the run writes a DLQ entry with `error_type='api_error'` and stops requesting that source until the recovery window passes.

#### 5. Structured Failure Logging

In addition to the DLQ table, every failure must emit a structured log line with consistent fields so log aggregators (Datadog, CloudWatch, etc.) can build dashboards without DB access:

```python
logger.error(
    "ingest_failure",
    extra={
        "source": source,
        "run_id": str(run_id),
        "error_type": error_type,
        "raw_key": raw_key,
        "entity_name": raw_record.get("company_name"),
        "error": str(exc),
    }
)
```

Use Python's standard `logging` with a `json_formatter` (add `python-json-logger` to requirements). JSON log lines flow to stdout; the deployment environment routes them.

### Updated `IngestResult`

Extend the existing `IngestResult` dataclass to include DLQ context:

```python
@dataclass
class IngestResult:
    total:       int = 0
    ingested:    int = 0
    skipped:     int = 0      # already in DB (idempotency)
    errors:      int = 0      # failed + sent to DLQ
    dlq_ids:     list[UUID] = field(default_factory=list)   # NEW
    run_id:      UUID         = field(default_factory=uuid4) # NEW
    checkpoint:  dict | None  = None                         # NEW — last saved cursor
```

### CLI Commands

```bash
# Show all open DLQ failures
PYTHONPATH=. .venv/bin/python -m cam.ingestion.dlq list

# Filter by source and error type
PYTHONPATH=. .venv/bin/python -m cam.ingestion.dlq list --source osha --error-type entity_resolution

# Export open failures to CSV for offline triage
PYTHONPATH=. .venv/bin/python -m cam.ingestion.dlq export --output failures.csv

# Replay specific DLQ entries
PYTHONPATH=. .venv/bin/python -m cam.ingestion.dlq replay --ids <uuid1> <uuid2>

# Replay all open entity_resolution failures for OSHA
PYTHONPATH=. .venv/bin/python -m cam.ingestion.dlq replay --source osha --error-type entity_resolution

# Dismiss entries that cannot be resolved (marks resolved_at, adds note)
PYTHONPATH=. .venv/bin/python -m cam.ingestion.dlq dismiss --ids <uuid1> --note "test data, not a real company"
```

### Alembic Migration

Create a new migration file in `cam/db/migrations/versions/` that adds `ingest_failures` and `ingest_checkpoints` tables. The migration must be reversible (`downgrade` drops both tables).

### Test Requirements

- **DLQ write is safe**: `record_failure()` must not raise even if the DB session is in a bad state (use a fresh connection or handle gracefully)
- **Checkpoint round-trip**: `save_checkpoint` → `load_checkpoint` returns the same cursor dict
- **Partial resume**: Given a 1000-record mock dataset and a checkpoint at offset 500, the ingestion function processes only records 500–999
- **Circuit breaker state machine**: Test all three transitions (CLOSED→OPEN, OPEN→HALF_OPEN→CLOSED, HALF_OPEN→OPEN on second failure)
- **Per-entity isolation**: Inject a DB error for entity #3 of 5; verify entities #1, #2, #4, #5 are committed and entity #3 has a DLQ entry
- **Structured log fields**: Assert all required fields appear in log output for each `record_failure()` call
- **Replay success**: A DLQ entry replayed with a fixed ingest function is marked resolved; retry_count increments
- **Replay failure**: A DLQ entry replayed against a still-broken function increments retry_count, is NOT marked resolved

### Acceptance Criteria

- [ ] All five ingestion sources (EDGAR, OSHA, EPA, CFPB, WARN) write to DLQ on per-record failure instead of silently dropping
- [ ] `ingest_failures` table is populated after any run that encounters errors
- [ ] Restarting an ingestion after a mid-run crash resumes from the last checkpoint (no duplicate inserts, ≤ CHECKPOINT_EVERY records re-processed)
- [ ] A circuit breaker open event produces a DLQ entry with `error_type='api_error'` and halts further calls to that source
- [ ] `cam.ingestion.dlq list` returns open failures in < 1 second for up to 10,000 DLQ entries
- [ ] 100% test coverage for `dlq.py`, `checkpoint.py`, and `circuit_breaker.py`

---

## M16 — Entity Resolution Review Workflow

### Goal

When entity resolution returns `needs_review=True` (confidence between `entity_review_threshold` and `entity_fuzzy_threshold`), the record is currently stored in the `signals` table as `signal_type='entity_review_queue'` and never acted on. This module builds the complete workflow: a CLI and API for reviewing queued matches, bulk approve/reject operations, CSV import/export for offline review, and an audit trail of manual decisions. It also handles the case where the DLQ has `entity_resolution` failures that were too low-confidence even for the review queue.

### Review Queue States

```
┌─────────────────────────────────────────────────────────────────┐
│                     ENTITY RESOLUTION OUTCOME                    │
├──────────────────┬───────────────────┬──────────────────────────┤
│ confidence ≥     │ Exact or fuzzy     │ Auto-accepted, alias     │
│ fuzzy_threshold  │ auto-accept        │ written to entity_aliases│
├──────────────────┼───────────────────┼──────────────────────────┤
│ review_threshold │ Review queue       │ Written to               │
│ ≤ conf <         │ (needs_review=True)│ signals table, status    │
│ fuzzy_threshold  │                   │ 'pending'                │
├──────────────────┼───────────────────┼──────────────────────────┤
│ conf <           │ Hard fail          │ Written to               │
│ review_threshold │                   │ ingest_failures DLQ,     │
│                  │                   │ error_type=              │
│                  │                   │ 'entity_resolution'      │
└──────────────────┴───────────────────┴──────────────────────────┘
```

### Schema Addition

Add a `status` column and `reviewed_by` column to the existing review queue entries in `signals`. Since these are rows in `signals` (not a separate table), add a partial index for fast queue queries:

```sql
-- Add columns to signals (migration required)
ALTER TABLE signals ADD COLUMN review_status TEXT DEFAULT 'pending'
    CHECK (review_status IN ('pending', 'approved', 'rejected', 'deferred'));
ALTER TABLE signals ADD COLUMN reviewed_by TEXT;    -- username or 'api'
ALTER TABLE signals ADD COLUMN reviewed_at TIMESTAMPTZ;
ALTER TABLE signals ADD COLUMN review_note TEXT;

-- Index for the operator-facing queue page
CREATE INDEX idx_signals_review_queue
    ON signals(created_at DESC)
    WHERE signal_type = 'entity_review_queue' AND review_status = 'pending';
```

### Key Functions

```python
# cam/entity/review.py

def list_pending(
    db: Session,
    source: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[ReviewQueueItem]:
    """
    Return pending review queue items, newest first.
    ReviewQueueItem includes: signal_id, raw_name, candidate_entity_id,
    candidate_name, confidence, source, evidence snippet, created_at.
    """

def approve(
    db: Session,
    signal_id: UUID,
    entity_id: UUID,            # may differ from candidate (operator picks correct entity)
    reviewed_by: str,
    note: str = "",
) -> None:
    """
    Approve a match:
    1. Write alias to entity_aliases (raw_name → entity_id)
    2. Back-fill entity_id on any events that were inserted with entity_id=NULL for this raw_name
    3. Replay any DLQ entries with matching raw_key
    4. Mark signal review_status='approved'
    """

def reject(
    db: Session,
    signal_id: UUID,
    reviewed_by: str,
    note: str = "",
) -> None:
    """
    Reject a proposed match. Mark signal review_status='rejected'.
    The raw record remains unresolved. DLQ entries with matching raw_key
    are marked resolved with error='rejected_by_operator'.
    """

def defer(db: Session, signal_id: UUID, reviewed_by: str, note: str = "") -> None:
    """Push item to the back of the queue (sets review_status='deferred')."""

def export_pending_csv(db: Session, path: Path, source: str | None = None) -> int:
    """
    Export pending queue to CSV for offline review.
    Columns: signal_id, raw_name, candidate_entity_id, candidate_name,
             confidence, source, created_at.
    Returns row count.
    """

def import_decisions_csv(db: Session, path: Path, reviewed_by: str) -> ImportResult:
    """
    Import a CSV where each row has: signal_id, decision (approve/reject/defer),
    entity_id (required if approve), note.
    Calls approve() or reject() for each row; returns counts.
    """
```

### Back-fill on Approval

A critical step: when an operator approves a match, there may be events already in the DB with `entity_id=NULL` for records that failed resolution under this raw name. `approve()` must back-fill them:

```python
# Back-fill events
db.execute(
    update(Event)
    .where(Event.raw_json["company_name"].astext == raw_name)
    .where(Event.entity_id.is_(None))
    .where(Event.source == source)
    .values(entity_id=entity_id)
)
```

This ensures approval is not just forward-looking but also repairs historical data.

### CLI Commands

```bash
# Show review queue
PYTHONPATH=. .venv/bin/python -m cam.entity.cli review list
PYTHONPATH=. .venv/bin/python -m cam.entity.cli review list --source osha --limit 20

# Interactive approve (shows candidate, prompts for confirmation)
PYTHONPATH=. .venv/bin/python -m cam.entity.cli review approve <signal_id>

# Approve with a different entity than the candidate
PYTHONPATH=. .venv/bin/python -m cam.entity.cli review approve <signal_id> --entity-id <uuid>

# Reject
PYTHONPATH=. .venv/bin/python -m cam.entity.cli review reject <signal_id> --note "subsidiary not in our entity list"

# Bulk operations via CSV
PYTHONPATH=. .venv/bin/python -m cam.entity.cli review export --output queue.csv
PYTHONPATH=. .venv/bin/python -m cam.entity.cli review import --input reviewed_queue.csv --reviewed-by alice
```

### API Endpoints (extend M14 FastAPI)

```
GET  /api/review-queue              # list pending items (paginated)
POST /api/review-queue/{id}/approve # approve with optional entity_id override
POST /api/review-queue/{id}/reject  # reject with note
POST /api/review-queue/{id}/defer
GET  /api/review-queue/export       # download CSV of pending items
POST /api/review-queue/import       # upload reviewed CSV
```

### Test Requirements

- **approve() back-fills**: Seed 3 events with `entity_id=NULL` and `raw_json.company_name = "ACME Inc"`. Call `approve()` for "ACME Inc" → entity X. Assert all 3 events now have `entity_id=X`
- **DLQ replay on approve**: Seed a DLQ entry with `raw_key = "acme_inc_osha"`. Approve the matching review queue item. Assert DLQ entry is marked resolved
- **reject() does not back-fill**: Rejected items leave existing NULL events untouched
- **CSV round-trip**: `export_pending_csv` → edit decision column → `import_decisions_csv` → assert queue is empty and decisions applied
- **Pagination**: With 200 pending items, `list_pending(limit=50, offset=150)` returns exactly 50 items
- **Concurrent approve safety**: Two concurrent approve calls for the same signal_id; assert alias is written exactly once (no duplicate `entity_aliases` row)

### Acceptance Criteria

- [ ] `cam.entity.cli review list` shows all pending review items with candidate name, confidence, and source
- [ ] Approving a match immediately writes the alias and back-fills NULL entity_id events in the same transaction
- [ ] Rejecting a match marks it resolved in DLQ and does not pollute entity_aliases
- [ ] CSV import processes 1,000 rows in < 30 seconds
- [ ] All review actions are recorded with `reviewed_by` and `reviewed_at` for audit purposes
- [ ] 90%+ test coverage for `cam/entity/review.py`

---

## Implementation Notes

### Priority Order

Work in this sequence:

1. **M15 first** — The DLQ and checkpoint infrastructure must exist before M16 can reference it. Also unblocks investigation of the current `bugfix/pipeline-empty-dashboard` issue (empty dashboard is almost certainly caused by entity_id=NULL events that never got resolved).

2. **M16 second** — Once DLQ entries are accumulating, operators need the review workflow to process them.

3. **M14 remaining work** — The dashboard empty-state issue may resolve once M15 back-fills NULL entity_ids. Finish the Output Layer only after M15 is merged so dashboard data is reliable.

### Conventions to Maintain

All existing conventions from completed modules continue to apply:

- **Transaction ownership**: `bulk_resolve(commit=False)`; ingestion function issues single `db.commit()`
- **Date guards**: `d is not None and d >= since_date` — never `d is None or d >= ...`
- **SQLAlchemy NULL filter**: Always `.isnot(None)` before date comparison
- **JSONB compat**: `sa.JSON().with_variant(JSONB(), "postgresql")` for SQLite in tests
- **No live HTTP calls in tests** — mock with `responses` or `httpx` mock
- **Ruff before every commit**: `ruff check --fix` then `ruff format`

### Root Cause of Empty Dashboard (Current Bug)

Based on git history and code review, the most likely causes of the empty dashboard are:

1. **entity_id = NULL on events**: Records ingested before entity resolution is fully seeded have no `entity_id`, so they join to nothing in the dashboard query
2. **Signal table not populated**: The `analyze` step that writes to `signals` was only recently added (commit 61ed206); historical ingestion runs may not have triggered it
3. **alert_scores not computed**: The M13 scorer requires signals to exist; without signals, no scores are written, and the dashboard shows nothing

The fix sequence: run M15 back-fill logic → re-run analysis → re-run M13 scorer → verify dashboard shows data.
