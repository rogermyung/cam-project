# Runbook — End-to-End Local Verify

This runbook is the authoritative, executed sequence for standing up CAM on a
fresh machine and proving data flows all the way from ingestion to a non-empty
dashboard. It was validated end to end on 2026-07-10 (Phase 4). Every command
below was run and its effect confirmed against a real local stack.

The one-line takeaway: **unit tests passing says nothing about live data flow.**
This runbook exists to catch the gap between "757 green tests" and "the
dashboard is empty," which is exactly how this project stalled the first time.

## Prerequisites

- Docker Desktop running.
- The project venv built with deps installed:
  `uv venv --python 3.13 .venv && uv pip install -r requirements-dev.txt`
- `openpyxl` present in the venv (needed for WARN CA XLSX ingestion):
  `uv pip install openpyxl --python .venv/bin/python`

## The sequence

Export the local connection settings once so every step shares them:

```bash
export DATABASE_URL=postgresql://cam:cam@localhost:5432/cam
export EDGAR_USER_AGENT=you@example.com
export S3_ENDPOINT=http://localhost:9000 S3_ACCESS_KEY=minioadmin \
       S3_SECRET_KEY=minioadmin S3_BUCKET=cam-documents
```

1. **Infrastructure.** `docker-compose up -d` and wait for `postgres` and
   `redis` to report `healthy` (`docker compose ps`).

2. **MinIO bucket (one-time).** EDGAR stores each raw filing in S3 *before* it
   writes the Event row, so the bucket must exist first:

   ```bash
   docker exec cam-project-minio-1 sh -c \
     'mc alias set local http://localhost:9000 minioadmin minioadmin && \
      mc mb --ignore-existing local/cam-documents'
   ```

   Skipping this makes every EDGAR filing fail with `NoSuchBucket` and commits
   zero events — the ingest still "succeeds," so the failure is silent until
   you notice the dashboard is empty.

3. **Migrations.** `PYTHONPATH=. .venv/bin/python -m alembic upgrade head`.

4. **Seed entities.** `python -m cam.entrypoint seed` — pulls ~9,300 SEC filers
   from `company_tickers.json` into the `entities` table. On a fresh DB the
   resolver creates no entities on its own, so without this step nothing
   resolves and nothing ingests.

5. **Ingest.** `python -m cam.entrypoint ingest --source all --since 2025-01-01`.
   See "Source reality" below — EDGAR is the source that reliably produces
   committed events against a SEC-seeded entity table.

6. **Analyze.** `python -m cam.entrypoint analyze --date today` — writes the M6
   cross-agency `Signal` rows from the ingested Events. **Required before
   score**: the scorer reads Signals, so skipping analyze produces an empty
   dashboard even when events exist.

7. **Score.** `python -m cam.entrypoint score --date today`.

8. **Export.** `python -m cam.entrypoint export --output-dir ./site --digest`.

9. **Verify non-empty output.** The exporter writes to the *top level* of the
   output dir (`site/entities.json`, `site/alerts.json`, `site/meta.json`,
   `site/entities/<uuid>.json`) — **not** `site/data/` (that path holds stale
   frontend demo fixtures; ignore it). Confirm the run is fresh and populated:

   ```bash
   cat site/meta.json          # exported_at should be now; entity_count > 0
   python3 -c "import json; d=json.load(open('site/entities.json')); \
     print(sum(1 for e in d if e.get('composite_score') is not None), 'scored')"
   ```

## Validated result (2026-07-10)

Seeding a fresh DB, then ingesting five large filers (AAPL, MSFT, JPM, WMT, KO)
produced **11 EDGAR 10-K events → 4 cross-agency signals → 4 scored entities**,
and the export wrote 9,304 entity records with a current `exported_at`
timestamp. Composite scores were `0.0` and no alerts fired — expected, because
EDGAR 10-K data alone (no OSHA/EPA/CFPB violations, and the NLP risk-language
modules are not wired into the `analyze` command) yields a zero cross-agency
composite. The pipeline mechanics are proven; the scores are honestly low.

## Source reality (why "0 records" is common and not always a bug)

The seed populates entities exclusively from SEC `company_tickers.json` —
i.e. **publicly traded filers**. The scorer and dashboard therefore light up
for SEC filers. But the other ingestion sources mostly concern companies that
are *not* SEC filers, which surfaced two distinct failure modes during the
verify:

- **Upstream contract drift (fetch fails):** OSHA bulk CSVs for 2024–2026 were
  404 (not yet published); WARN CA served the old CSV URL as 404 (moved to
  XLSX); WARN MI/FL/IL/OH pages had moved. These are real breakage the weekly
  smoke suite (`tests/smoke`, Phase 3) is designed to catch.
- **Entity-resolution mismatch (fetch + parse succeed, 0 ingested):** WARN NY
  parsed 69 notices and TX parsed 3, but **0 ingested** — the layoff filers
  (small/private employers) don't match any SEC-seeded entity, so they land in
  the review queue instead of becoming Events. CFPB has the same problem at
  scale (millions of complaints against non-filer financial entities), which
  also makes a full `--source all` ingest extremely slow.

The practical consequence: **run ingest per-source, not `--source all`**, when
verifying locally. `--source all` uses a single transaction that commits only
at the very end, so a slow source (CFPB) blocks all others from committing and
can appear hung for many minutes with zero visible progress.

Closing the entity-resolution gap for non-SEC employers (so WARN/OSHA/EPA/CFPB
data actually lands) is follow-on work beyond this runbook — it is the deeper
root cause under "the dashboard is empty," one layer below the seeding and
smoke-test fixes already shipped.
