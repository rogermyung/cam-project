# Corporate Accountability Monitor — Agent Instructions

## Project Overview

This is the Corporate Accountability Monitor (CAM): a Python system that ingests regulatory data from multiple US agencies, runs NLP analysis on SEC filings, and produces composite risk scores per company. See `README.md` for the full description and `PLAN.md` for the authoritative implementation specification.

## Language & Stack

- **Python 3.11+** (3.13 in use locally)
- **PostgreSQL** — structured data (SQLAlchemy 2.x ORM, Alembic migrations)
- **Redis** — Celery task broker
- **S3-compatible** (MinIO locally) — raw document storage
- **Celery** — background task queue
- **FastAPI + Jinja2** — output API and dashboard
- **pytest** — all tests; run with `PYTHONPATH=. .venv/bin/python -m pytest`
- **uv** — package manager (`uv pip install`, `uv venv`)

## Repository Layout

```
cam/
├── ingestion/      # One subpackage per data source (edgar, osha, epa, cfpb, warn)
├── entity/         # Entity resolution (M1)
├── analysis/       # NLP and aggregation (M6-M10, M12)
├── alerts/         # Scoring and alert logic (M13)
├── output/         # Dashboard, API, digest (M14)
├── db/
│   ├── models.py   # SQLAlchemy models
│   └── migrations/ # Alembic migrations (versions/)
├── config.py       # Pydantic Settings — all config from env vars
└── tasks.py        # Celery task definitions
tests/
├── conftest.py     # Shared fixtures; requires_db skip marker for postgres-gated tests
├── fixtures/       # Canned API responses (edgar/, osha/, cfpb/, epa/)
└── unit/           # Unit tests per module
```

## Implementing Modules

**All module specifications are in `PLAN.md`.** When implementing a module:

1. **Read `PLAN.md`** — find the module section (e.g. `## M3 — OSHA Ingestion`). Follow its Goal, Key Functions, Schema Mapping, Test Requirements, and Acceptance Criteria exactly.
2. **Check dependencies** — use GitHub issues as the source of truth (CLOSED = complete). Run `gh issue list --limit 20 --state all`, cross-reference PLAN.md's Module Index, and sync this table: CLOSED+TODO/In-Progress → ✅ Complete (with PR); starting now → 🔄 In Progress. Stop if any dependency is OPEN.
3. **Create a branch** named `module/m<N>-<short-name>` (e.g. `module/m3-osha-ingestion`).
4. **Mark in-progress** — update this table to `🔄 In Progress` for the module being started, then comment on the GitHub issue (`gh issue comment <N>`) to mark it in-progress.
5. **Implement** the module per the PLAN.md spec. Key conventions:
   - All config from `cam/config.py` (Pydantic Settings, env vars)
   - All external HTTP calls use `httpx` with `tenacity` retry; mock with `responses` or `httpx` mock in tests
   - No live API calls in tests — all external calls must be interceptable
   - SQLite in-memory for unit tests (avoids needing a live DB); use `requires_db` marker from `tests/conftest.py` for Postgres-specific tests
   - JSONB columns use `sa.JSON().with_variant(JSONB(), "postgresql")` for SQLite compat
   - Idempotency required for all ingestion functions (running twice produces same DB state)
6. **Write tests** meeting the Test Requirements in PLAN.md. Minimum 80% coverage; 100% for M13 scorer/alert logic.
7. **Run tests locally**: `PYTHONPATH=. .venv/bin/python -m pytest tests/unit/test_<module>.py -v --no-cov`
8. **Run ruff before every commit** (both commands required):
   ```bash
   .venv/bin/ruff check <files> --fix   # lint and auto-fix
   .venv/bin/ruff format <files>         # format
   ```
9. **Commit** with message `M<N>: <Title> — <brief summary>\n\nCloses #<issue>\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>`
10. **Push** and **create a PR** with `gh pr create --base main` including a summary and test plan.
11. **Update the GitHub issue** with a completion comment listing delivered items.

## Module Status

| Module | Issue | Status | PR |
|--------|-------|--------|-----|
| M0 — Project Scaffolding | #1 | ✅ Complete | #16 |
| M1 — Entity Resolution | #2 | ✅ Complete | #17 |
| M2 — EDGAR Ingestion | #3 | ✅ Complete | #18 |
| M3 — OSHA Ingestion | #4 | ✅ Complete | #19 |
| M4 — EPA Ingestion | #5 | ✅ Complete | #20 |
| M5 — CFPB Ingestion | #6 | ✅ Complete | #22 |
| M6 — Cross-Agency Aggregation | #7 | ✅ Complete | #23 |
| M7 — 10-K Risk Language NLP | #8 | ✅ Complete | #24 |
| M8 — Earnings Call NLP | #9 | ✅ Complete | #25 |
| M9 — Proxy Statement Parser | #10 | ✅ Complete | #26 |
| M10 — HSR Merger Screener | #11 | 🔄 In Progress | — |
| M11 — WARN Act Ingestion | #12 | 🔄 In Progress | — |
| M12 — PE/Bankruptcy Correlator | #13 | ⬜ TODO | — |
| M13 — Alert Scoring Engine | #14 | ⬜ TODO | — |
| M14 — Output Layer | #15 | ⬜ TODO | — |

## Skills (Slash Commands)

Three project-specific skills live in `.claude/commands/`:

- `/start-issue <N>` — Read M\<N\> spec from PLAN.md, check deps, create branch, comment on issue
- `/create-pr` — Run ruff, run tests, commit staged changes, push, open PR
- `/qodo-review` — Fetch and display all review comments from qodo-merge-pro[bot] on the current PR (inline + issue-level; includes action-required bugs and general feedback)

## Ingestion Gotchas

Patterns confirmed across M2–M4 that must be followed in all ingestion modules:

- **Transaction ownership**: call `bulk_resolve(..., commit=False)`; the ingestion function issues the single `db.commit()` at the end. This keeps the whole ingest atomic and lets the caller control visibility — `bulk_resolve(commit=True)` does a single post-batch commit which is fine for interactive use but wrong for bulk ingestion where the caller must own the boundary.
- **Facility name cleaning**: strip `" - LOCATION"` suffix (`r"\s+-\s+[A-Z0-9 ]+$"`) before passing names to `bulk_resolve`. Both OSHA and EPA facilities use this pattern.
- **Date guard**: `_parse_date()` returns `None` for unparseable values. Since-date filters must use `d is not None and d >= since_date` — never `d is None or d >= since_date` (the latter silently admits unparseable rows).

## Testing Conventions

- **No live HTTP calls in tests** — mock everything with `responses` or `httpx` mock
- **Fixture files** go in `tests/fixtures/<source>/` (e.g. `tests/fixtures/osha/violations_sample.csv`)
- **SQLite** for unit tests (no DB required); use `@requires_db` for Postgres-only tests
- **Performance tests** required per module — see PLAN.md acceptance criteria for limits
- Run all tests: `PYTHONPATH=. .venv/bin/python -m pytest --no-cov`

## Environment

Copy `.env.example` to `.env` for local dev. Required vars: `DATABASE_URL`, `EDGAR_USER_AGENT`. See `.env.example` for all options.

## Common Commands

```bash
# Create venv
uv venv --python 3.13 .venv

# Install deps
uv pip install -r requirements-dev.txt

# Run tests (no DB needed for most)
PYTHONPATH=. .venv/bin/python -m pytest tests/unit/ -v --no-cov

# Lint and format (run both before every commit)
.venv/bin/ruff check <files> --fix
.venv/bin/ruff format <files>

# Run migrations (requires postgres)
DATABASE_URL=postgresql://cam:cam@localhost:5432/cam alembic upgrade head

# Start all services
docker-compose up

# Entity review queue CLI
PYTHONPATH=. .venv/bin/python -m cam.entity.cli list
```
