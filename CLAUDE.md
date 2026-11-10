# CAM — WARN layoff tracker and CFPB complaint monitor — Agent Instructions

## Project Overview

Two database-free, independent trackers: US state WARN Act layoff notices and
CFPB consumer complaints by company. See `README.md` for usage and
`docs/simplification-plan.md` for the staged plan (Stages 1 to 3 done; Stage 4
optional cross-links).

The earlier multi-agency pipeline is archived under the git tags
`archive/full-pipeline` and `archive/jev-screening`; its spec is in
`docs/archive/`. Do not rebuild from it: no database, no queue, no entity
resolution, no composite score. To reuse old code, read it with
`git show archive/full-pipeline:<path>`.

## Stack

- **Python 3.11+** (3.13 locally), dependencies in `pyproject.toml`, managed by uv (httpx, tenacity, beautifulsoup4, openpyxl)
- **Storage**: CSV and JSON files under `data/`, committed to git
- **pytest** and **ruff**
- **React + TypeScript** static site in `frontend/` (Vite, Tailwind, Recharts), deployed to GitHub Pages by `.github/workflows/publish.yml`

## Layout

```
cam/
├── __main__.py        # CLI: python -m cam warn | cfpb
├── store.py           # load / merge / save for deterministic CSV files (stdlib only)
├── cfpb/
│   ├── __init__.py    # stream_to_tempfile, aggregate (one pass over the bulk zip), to_rows, window_months
│   ├── runner.py      # run(): download, aggregate, replace monthly.csv, write status.json
│   └── build.py       # detect_spike, build(): monthly.csv -> site/data/cfpb_{companies,meta}.json
└── warn/
    ├── __init__.py    # WarnRecord, fetch, parse, record_key, record_to_row, FIELDNAMES
    ├── state_urls.py  # STATE_CONFIGS: one StateConfig per state
    ├── _xlsx.py       # California workbook parser
    ├── _mi.py         # Michigan JSON API parser
    ├── _pa.py         # Pennsylvania accordion-page parser
    ├── runner.py      # run(): fetch each state, merge, write notices.csv + status.json
    └── build.py       # build(): notices.csv -> site/data/{notices,summary,meta}.json
data/warn/             # notices.csv (source of truth), status.json
data/cfpb/             # monthly.csv (company, month, product, complaints), status.json
tests/unit/            # test_tracker_*.py, test_cfpb_*.py, test_store.py — no network
tests/smoke/           # live fetch-and-parse per state; CFPB bulk URL check (-m live)
tests/fixtures/warn/   # captured source files
frontend/
├── src/types.ts       # data contract: mirrors the JSON from cam/warn/build.py and cam/cfpb/build.py
├── src/lib/notices.ts # pure logic (filter, sort, paginate, month range, freshness); no runtime imports
├── src/lib/cfpb.ts    # pure logic (filter, sort, change format, sparkline points); no runtime imports
├── src/pages/         # OverviewPage, NoticesPage, CfpbPage
├── tests/             # node --test "tests/*.test.ts"
└── public/data/       # sample JSON for npm run dev
```

## Conventions

- **No database and no services.** State lives in files; a run must work on a fresh clone with one command.
- **Deterministic files.** `store.save` sorts rows and writes atomically; running twice on the same input must leave `notices.csv` byte-identical.
- **Existing rows win on merge** in the WARN tracker, so `first_seen` never changes.
- **CFPB is replace, not merge.** The bulk file is the full history, so `monthly.csv` is rebuilt from it each run; a failed run must leave the previous file untouched. Raw complaints are never stored.
- **The two trackers stay independent.** No shared keys, no cross-dataset matching (that is Stage 4, exact match only); one failing must not block the other's data or deploy.
- **Per-state isolation.** One state failing must never stop the others or delete its previous rows.
- **Zero records is a failure**, not a success. A page that returns 200 but parses to nothing is drift.
- **Unit tests make no network calls.** Pass a fake `fetcher` to `run()` or monkeypatch `cam.warn.runner.fetch`.
- **Live sources drift.** After changing a parser, run the smoke test for that state; a passing unit test against a fixture says nothing about the live page.
- **Site data contract.** If `cam/warn/build.py` or `cam/cfpb/build.py` output changes, update `frontend/src/types.ts` and regenerate `frontend/public/data/` in the same change.
- **Frontend checks**: from `frontend/`, `node --test "tests/*.test.ts"`, `npm run build`, and `npx eslint src --max-warnings 0` must all pass.
- **Workflows**: pin third-party actions to a commit SHA and give each job only the permissions it needs.
- **Adding or fixing a state**: capture the real page into `tests/fixtures/warn/`, write the test against it, then edit `state_urls.py` or add a parser module dispatched from `parse()`.

## Commands

```bash

uv run -m cam warn                      # fetch, merge, build
uv run -m cam warn --states CA --no-build
uv run -m cam cfpb                      # download (~365 MB), aggregate, build; about 3 min
uv run -m cam cfpb --zip complaints.csv.zip --no-build   # reuse a downloaded copy

uv run pytest --no-cov               # unit tests
uv run pytest tests/smoke -m live --no-cov

uv run ruff check . --fix && uv run ruff format .       # before every commit

cd frontend && npm run dev
```

## Skills (Slash Commands)

- `/create-pr` — run ruff and tests, commit staged changes, push, open a PR
- `/qodo-review` — show review comments from qodo-merge-pro[bot] on the current PR

`/start-issue` belongs to the archived module workflow and no longer applies.
