# CAM — WARN layoff tracker

A small, database-free tracker of US state WARN Act layoff and closure notices.
It fetches each state's public notice list, merges new notices into a CSV that
is committed to this repository, and builds JSON for a static site.

```
fetch (live HTTP) -> parse -> merge into data/warn/notices.csv -> build site/data/*.json
```

This replaced a much larger multi-agency pipeline (Postgres, Celery, entity
resolution, composite risk scores) that never ran successfully. That version is
preserved under the git tags `archive/full-pipeline` and `archive/jev-screening`,
and its specification is in `docs/archive/`. The reasoning and remaining stages
are in `docs/simplification-plan.md`.

## Quick start

```bash
uv run -m cam warn
```

No services, no environment variables. A full run takes a few seconds.

Options: `--states CA,MI` limits the run, `--data-dir` and `--site-dir` change
the output locations, `--no-build` skips the site JSON.

## Outputs

| File | Contents |
|---|---|
| `data/warn/notices.csv` | Source of truth, one row per notice. Append-only: existing rows are never rewritten, so `first_seen` is stable. Committed to git. |
| `data/warn/status.json` | Per-state result of the last run: ok, fetched, added, error, last success, degraded. Committed to git. |
| `site/data/notices.json` | All notices, newest first. Build output, not committed. |
| `site/data/summary.json` | Totals by state and by month, the 20 largest notices, and employers with repeat notices in a state. |
| `site/data/meta.json` | Totals, generation time, and the per-state status. |

Columns of `notices.csv`: `key, state, employer, notice_date, employees, city,
county, notice_type, source_url, first_seen`.

## State coverage

| State | Source | Notices (2026-11-09) | Notes |
|---|---|---|---|
| CA | Agency XLSX workbook | 457 | Rolling window, roughly the last five months |
| MI | Agency JSON search API | 41 | `notice_date` is the layoff date |
| NY | Tableau dashboard CSV export | 225 | Current year; one row per affected site |
| PA | Agency accordion page | 319 | Back to 2022. `notice_date` is the layoff effective date; about 5% of notices have no parseable date |
| TX | Texas Open Data Portal CSV | 2,347 | Back to 2019. The agency page is behind a bot challenge, so this uses the portal copy, which lagged by about four months on this date |

A state that fails keeps its previous rows and is reported as `FAILED`; it does
not stop the others. The command exits non-zero only when every state fails or
there is no data at all. `notice_date` holds whatever date the state publishes,
which for some states is the layoff effective date and can be in the future.

## Tests

```bash
uv run pytest                              # unit tests, no network
uv run pytest tests/smoke -m live --no-cov # fetch and parse every live source
```

## Website

`frontend/` is a small React + TypeScript site with two pages: an overview
(totals, notices per month, per-source status, largest notices, repeat
employers) and a filterable, sortable table of every notice.

```bash
cd frontend
npm ci
npm run dev                       # http://localhost:5173/cam-project/ with sample data
node --test "tests/*.test.ts"     # logic tests
npm run build                     # type-check and build into ../site
```

`npm run dev` serves the sample JSON in `frontend/public/data/`, which includes
one failed and one degraded source on purpose so those states are visible. To
see the real dataset locally, run `npm run build`, then `uv run -m cam warn`
from the repository root, then `npm run preview` in `frontend/`.

## Publishing

`.github/workflows/publish.yml` runs every Monday at 08:00 UTC, on manual
dispatch, and on pushes to `main` that touch `cam/` or `frontend/`. It fetches
all states, commits any change under `data/warn/`, builds the site with the
fresh data and deploys it to GitHub Pages at
https://rogermyung.github.io/cam-project/. If every source fails, the failure
is still committed to `status.json`, the run ends red, and the published site
is left as it was.
