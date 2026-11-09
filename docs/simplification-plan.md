# CAM simplification plan

Status: Stage 0 and Stage 1 implemented on branch `simplify/warn-tracker` (2026-11-09). Stages 2 to 4 not started.

Deviations from the plan as written, found during Stage 1:

- **TX, NY and PA do not parse live.** Their pages return HTTP 200 but contain no table the parser recognises, so only CA and MI produce data. The earlier "reachable" smoke result only checked the status code. All three stay configured and are reported as failed until their parsers are fixed.
- **Row schema** is `key, state, employer, notice_date, employees, city, county, notice_type, source_url, first_seen`. The sources carry no separate effective date, and the dedup key now includes the city so two sites of one employer filed on the same day are kept apart.
- **A sharp drop in a state's row count marks it `degraded`** in `status.json` instead of failing the run, because sources that publish a rolling window shrink legitimately.
- **`cam/ingestion/cfpb.py` was removed with the rest** since it cannot import without the database layer. Stage 3 should port it from the `archive/full-pipeline` tag.
- **`python -m cam warn` is run with `PYTHONPATH=.`** from a venv built with `uv pip install -r requirements-dev.txt`; there is no `uv sync` project metadata.

## Why the full version never ran

Three things compounded, and the simple versions are designed to avoid each one.

| Cause | Evidence | How the simple version avoids it |
|---|---|---|
| Cross-source entity resolution | OSHA/EPA/CFPB/WARN employers are mostly not SEC filers, so events got `entity_id=NULL`. Fuzzy matching tops out at 83% recall@5. | Each version uses one dataset with its own natural key. No resolver, no review queue. |
| Infrastructure before data | Postgres, Alembic, Redis, Celery and MinIO all had to be up before one row flowed. A missing MinIO bucket silently produced 0 filings. | Flat files in `data/`, committed to git. One command, no Docker, no env vars. |
| Mocked-only tests | ~790 unit tests passed while 5 of 8 WARN states, OSHA and the old CFPB API were broken live. "0 records" counted as success. | Every run hits live sources, fails loudly on 0 records, and publishes per-source status. |

A fourth, quieter one: only M6 is wired into `analyze`, so 65% of the composite score is always 0.0. The simple versions drop the composite score entirely.

## Shape of the result

Two independent single-dataset trackers, built one after the other, sharing only a small file-store helper and the static site.

```
fetch (live HTTP) -> parse (pure functions) -> merge into data/<source>/*.csv -> build site/data/*.json -> static site
```

- **Storage**: CSV files in `data/`, committed by the scheduled workflow. Git history is the audit trail and the backup. Idempotency is "merge by key, then rewrite the file".
- **Runtime**: one process, run from cron in GitHub Actions. No queue, no broker, no API server.
- **Output**: the existing Vite/React site on GitHub Pages, cut down to the pages the data supports.

## Stage 0 — Freeze the full version

1. Tag `main` as `archive/full-pipeline` and tag `analysis/jev-screening` as `archive/jev-screening` (its two commits are not on `main`).
2. Create branch `simplify/warn-tracker` from `main`.
3. Move `PLAN.md` to `docs/archive/PLAN-full-pipeline.md`. Rewrite `README.md` and `CLAUDE.md` for the new scope (the module table, DB conventions and ingestion gotchas no longer apply).

Done when: the tags exist and the old spec no longer reads as current.

## Stage 1 — WARN layoff tracker (one dataset)

**Why this one first**: the data is small, so a full run takes seconds; the parsers are already pure `bytes -> list[WarnRecord]`; and CA and MI were verified against live sources in July. Only the bottom half of `cam/ingestion/warn/__init__.py` touches the database.

**Scope**: CA, TX, NY, PA, MI. FL, IL and OH were broken at the last live check (2026-07-10) and stay out until someone fixes them one at a time.

**Keep and move to `cam/warn/`**
- `_fetch`, `_parse_date`, `_parse_employees`, `_clean_name`, `_parse_by_format`, `_parse_csv`, `_parse_html`, `_idempotency_key`
- `_xlsx.py` (CA), `_mi.py` (MI), `state_urls.py` trimmed to five states
- Their unit tests and fixtures; `tests/smoke/test_warn.py`

**Write new (roughly 200 lines)**
- `cam/store.py`: `load(path)`, `merge(existing, new, key)`, `save(path)`; sorted, stable output so git diffs stay small
- `cam/warn/run.py`: fetch each state independently, merge, record per-state status; a failing state keeps its previous rows
- `cam/warn/build.py`: write `site/data/notices.json`, `summary.json` (by state, by month, largest notices, repeat employers within a state), `meta.json` (per-state last success, row counts)
- `python -m cam warn` as the single entry point

**Row schema for `data/warn/notices.csv`**
`key, state, employer, employer_clean, notice_date, effective_date, employees, city, notice_type, source_url, first_seen`

**Guardrails**
- Exit non-zero if every state fails or total new + existing rows is 0
- Exit non-zero if a state's row count drops by more than half (parser drift)
- One end-to-end test: fixtures in, files out, assert non-empty

**Delete**
`cam/db/`, `alembic.ini`, `docker-compose.yml`, `Dockerfile`, `cam/tasks.py`, `cam/entity/`, `cam/jev.py`, `cam/analysis/`, `cam/alerts/`, `cam/output/`, `cam/ingestion/{dlq,checkpoint,circuit_breaker,base,osha,epa,edgar}.py`, `_parse_pdf` (only IL used it), and the matching tests. `cam/ingestion/cfpb.py` stays untouched until Stage 3.

Dependencies drop to roughly: `httpx`, `tenacity`, `openpyxl`, the HTML parser, `pytest`, `ruff`.

Done when: on a fresh clone, `uv sync` then `python -m cam warn` writes a non-empty `data/warn/notices.csv` in under a minute with no services running, and a second run produces no diff.

## Stage 2 — Publish it

1. Replace the workflows with one: weekly cron -> `python -m cam warn` -> commit `data/` -> `npm run build` -> deploy Pages.
2. Cut the frontend to two pages: a sortable/filterable notices table, and a state overview with a monthly trend chart. Remove Alerts, Entity and Industries pages and the composite-score types.
3. Show per-state freshness from `meta.json` on the page so a silently broken scraper is visible.

Done when: the public site shows real notices and updates itself for two consecutive weeks without intervention.

## Stage 3 — CFPB complaint monitor (second dataset, still standalone)

**Why it fits**: one federal bulk file, one stable URL, and the `Company` column is already normalized by CFPB, so it is its own key.

**Approach**: stream the bulk zip to a temp file (existing `_stream_to_tempfile`), read it in a single pass, and keep only aggregates. Raw complaints are never stored.

**Keep**: `_stream_to_tempfile`, `_parse_bulk_zip_from_path`, `_csv_row_to_complaint`, `_parse_date`, `_clean_company_name`, and the spike logic from `detect_complaint_spike` rewritten to work on in-memory counts.

**Output**: `data/cfpb/monthly.csv` with `company, month, product, complaints`, limited to companies above a minimum volume; `site/data/cfpb_*.json`; one frontend page listing companies with a trend line and a spike flag (latest month vs trailing 12-month baseline).

**Caveat to measure first**: the zip is over 1 GB. Time one full download-and-aggregate run locally before wiring it into the workflow; if it is too slow for a weekly job, run it monthly.

Done when: `python -m cam cfpb` produces a non-empty `monthly.csv` and the same command runs green in the scheduled workflow.

## Stage 4 — Optional light cross-link (only after both run for a month)

Exact match on normalized name between WARN employers, CFPB companies and SEC's `company_tickers.json`. Shown as a link or ticker badge, nothing more. No fuzzy matching, no scores, no review queue. If the exact-match hit rate is too low to be useful, stop here; that is the entity-resolution problem that sank the full version.

## Explicitly out of scope

- Composite risk score and alert levels
- Entity resolution, including the Jev alignment and screening work (preserved under the archive tags)
- EDGAR ingestion and the 10-K / earnings / proxy / merger NLP modules
- OSHA (bulk URL broken) and EPA (only useful with resolution)
- Any database, queue, object store or API server

A later EDGAR-only version (10-K risk language diff, keyed by CIK, filings cached as local files) is a reasonable third tracker, but it is a separate project decision, not part of this plan.

## Open decisions

1. **Which dataset first**: WARN (recommended; fastest feedback, but five scrapers to keep alive) or CFPB (one stable source, but a large download per run).
2. **Where**: in place on a branch with archive tags (recommended) or a fresh repository.
3. **Frontend**: trim the existing React site (recommended) or replace it with a single static HTML page.
