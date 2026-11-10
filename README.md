# CAM — WARN layoff tracker and CFPB complaint monitor

Two small, database-free, independent trackers that share a file store and a
static site:

- **WARN**: US state WARN Act layoff and closure notices. Fetches each state's
  public notice list and merges new notices into a CSV.
- **CFPB**: consumer complaints by company. Aggregates the CFPB bulk complaint
  file into monthly counts and flags companies whose latest month spiked.

```
warn: fetch (live HTTP) -> parse -> merge into data/warn/notices.csv -> build site/data/*.json
cfpb: download bulk zip -> aggregate in one pass -> data/cfpb/monthly.csv -> build site/data/cfpb_*.json
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

```bash
uv run -m cam cfpb
```

Downloads the CFPB bulk file (about 365 MB) to a temp file, reads it once and
deletes it. Measured on 2026-11-10: about 3 minutes end to end (187 s), of which 58 s is
aggregating 19 million rows; 60 MB peak memory. The built-in downloader is the
slow part: curl fetches the same file in 15 s. Options: `--zip PATH` aggregates
an already-downloaded copy instead, `--min-complaints N` changes the volume
threshold (default 1,000), plus `--data-dir`, `--site-dir` and `--no-build` as
above.

## Outputs

| File | Contents |
|---|---|
| `data/warn/notices.csv` | Source of truth, one row per notice. Append-only: existing rows are never rewritten, so `first_seen` is stable. Committed to git. |
| `data/warn/status.json` | Per-state result of the last run: ok, fetched, added, error, last success, degraded. Committed to git. |
| `site/data/notices.json` | All notices, newest first. Build output, not committed. |
| `site/data/summary.json` | Totals by state and by month, the 20 largest notices, and employers with repeat notices in a state. |
| `site/data/meta.json` | Totals, generation time, and the per-state status. |

| `data/cfpb/monthly.csv` | `company, month, product, complaints` for the 24 full months before the current one, limited to companies with at least 1,000 complaints in that window. Rewritten in full on every successful run (the bulk file is the whole history). Committed to git. |
| `data/cfpb/status.json` | Result of the last CFPB run: ok, error, last success, rows read, companies, window, degraded. Committed to git. |
| `site/data/cfpb_companies.json` | One entry per company: window total, latest month, 12-month baseline, ratio, spike flag, top product, zero-filled monthly series, product breakdown. |
| `site/data/cfpb_meta.json` | Window, totals, number of spikes, and the run status. |

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

## CFPB complaint monitor

Source: the [CFPB Consumer Complaint Database](https://www.consumerfinance.gov/data-research/consumer-complaints/)
bulk export, `https://files.consumerfinance.gov/ccdb/complaints.csv.zip`. Raw
complaints are never stored, only counts.

- **Key**: the `Company` column as CFPB publishes it, with whitespace
  collapsed. No suffix stripping and no matching against other datasets.
- **Window**: the 24 full calendar months before the current month, by date
  received. The current month is excluded because it is incomplete.
- **Spike**: the latest month is more than 50% above the mean of the 12 months
  before it. Complaints are published with a delay, so the latest month can
  still grow, which makes the flag conservative.
- **Failure rules**: a failed download, an unreadable zip or zero rows after
  the volume filter leaves `monthly.csv` untouched, records the error in
  `status.json` and exits non-zero. A company count that falls by more than
  half is written but marked `degraded`.

On 2026-11-10 the window held 12.1 million complaints across 181 companies;
the three credit bureaus account for about 89% of them.

## Tests

```bash
uv run pytest                              # unit tests, no network
uv run pytest tests/smoke -m live --no-cov # fetch and parse every live source
```

## Website

`frontend/` is a small React + TypeScript site with three pages: an overview
(totals, notices per month, per-source status, largest notices, repeat
employers), a filterable, sortable table of every notice, and a CFPB page
listing each company with a trend line, its change against the 12-month
average and a spike flag.

```bash
cd frontend
npm ci
npm run dev                       # http://localhost:5173/cam-project/ with sample data
node --test "tests/*.test.ts"     # logic tests
npm run build                     # type-check and build into ../site
```

`npm run dev` serves the sample JSON in `frontend/public/data/`. The WARN sample
includes one failed and one degraded source on purpose so those states are
visible; the CFPB sample is the real build from 2026-11-10. To
see the real dataset locally, run `npm run build`, then `uv run -m cam warn`
from the repository root, then `npm run preview` in `frontend/`.

## Publishing

`.github/workflows/publish.yml` runs every Monday at 08:00 UTC, on manual
dispatch, and on pushes to `main` that touch `cam/` or `frontend/`. It fetches
all WARN states and the CFPB bulk file, commits any change under `data/warn/`
and `data/cfpb/`, builds the site with the fresh data and deploys it to GitHub
Pages at https://rogermyung.github.io/cam-project/. If every WARN source fails,
the failure is still committed to `status.json`, the run ends red, and the
published site is left as it was. A CFPB failure is committed to its own
`status.json`, raises a workflow warning and shows a notice on the CFPB page,
but does not block the deploy.
