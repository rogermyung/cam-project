# CAM — WARN layoff tracker — Agent Instructions

## Project Overview

A database-free tracker of US state WARN Act layoff notices. See `README.md` for
usage and `docs/simplification-plan.md` for the staged plan (Stage 1 done;
Stage 2 publish, Stage 3 CFPB tracker, Stage 4 optional cross-links).

The earlier multi-agency pipeline is archived under the git tags
`archive/full-pipeline` and `archive/jev-screening`; its spec is in
`docs/archive/`. Do not rebuild from it: no database, no queue, no entity
resolution, no composite score. To reuse old code (for example the CFPB bulk
parser for Stage 3), read it with `git show archive/full-pipeline:<path>`.

## Stack

- **Python 3.11+** (3.13 locally), dependencies in `pyproject.toml`, managed by uv (httpx, tenacity, beautifulsoup4, openpyxl)
- **Storage**: CSV and JSON files under `data/`, committed to git
- **pytest** and **ruff**
- **React + TypeScript** frontend in `frontend/` (not yet adapted to the new data)

## Layout

```
cam/
├── __main__.py        # CLI: python -m cam warn
├── store.py           # load / merge / save for deterministic CSV files (stdlib only)
└── warn/
    ├── __init__.py    # WarnRecord, fetch, parse, record_key, record_to_row, FIELDNAMES
    ├── state_urls.py  # STATE_CONFIGS: one StateConfig per state
    ├── _xlsx.py       # California workbook parser
    ├── _mi.py         # Michigan JSON API parser
    ├── _pa.py         # Pennsylvania accordion-page parser
    ├── runner.py      # run(): fetch each state, merge, write notices.csv + status.json
    └── build.py       # build(): notices.csv -> site/data/{notices,summary,meta}.json
data/warn/             # notices.csv (source of truth), status.json
tests/unit/            # test_tracker_*.py, test_store.py — no network
tests/smoke/           # live fetch-and-parse per state (-m live)
tests/fixtures/warn/   # captured source files
```

## Conventions

- **No database and no services.** State lives in files; a run must work on a fresh clone with one command.
- **Deterministic files.** `store.save` sorts rows and writes atomically; running twice on the same input must leave `notices.csv` byte-identical.
- **Existing rows win on merge**, so `first_seen` never changes.
- **Per-state isolation.** One state failing must never stop the others or delete its previous rows.
- **Zero records is a failure**, not a success. A page that returns 200 but parses to nothing is drift.
- **Unit tests make no network calls.** Pass a fake `fetcher` to `run()` or monkeypatch `cam.warn.runner.fetch`.
- **Live sources drift.** After changing a parser, run the smoke test for that state; a passing unit test against a fixture says nothing about the live page.
- **Adding or fixing a state**: capture the real page into `tests/fixtures/warn/`, write the test against it, then edit `state_urls.py` or add a parser module dispatched from `parse()`.

## Commands

```bash

uv run -m cam warn                      # fetch, merge, build
uv run -m cam warn --states CA --no-build

uv run pytest --no-cov               # unit tests
uv run pytest tests/smoke -m live --no-cov

uv run ruff check . --fix && uv run ruff format .       # before every commit

cd frontend && npm run dev
```

## Skills (Slash Commands)

- `/create-pr` — run ruff and tests, commit staged changes, push, open a PR
- `/qodo-review` — show review comments from qodo-merge-pro[bot] on the current PR

`/start-issue` belongs to the archived module workflow and no longer applies.
