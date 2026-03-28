"""
M11 — WARN Act Ingestion

Ingests mass-layoff and plant-closure notices filed under the Worker Adjustment
and Retraining Notification (WARN) Act from state labour department websites.

Supports three source formats:
- "csv"  — direct CSV download (CA, FL)
- "html" — HTML table on a state webpage (TX, NY, OH, PA, MI)
- "pdf"  — PDF document requiring pdfplumber extraction (IL and some others)

All ingestion functions are idempotent: running the same state twice produces
the same database state.  The idempotency key is (state_code, company, date).

PE ownership is stored as a Signal record with signal_type="pe_owned"; the
``get_pe_owned_entities`` function queries that table.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import uuid as _uuid_mod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any
from uuid import UUID

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from cam.db.models import Event, Signal
from cam.entity.resolver import bulk_resolve
from cam.ingestion.circuit_breaker import get_breaker
from cam.ingestion.dlq import ERROR_DB_WRITE, ERROR_ENTITY_RESOLUTION, record_failure

from .state_urls import STATE_CONFIGS, StateConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class WarnRecord:
    """A single WARN notice row after normalisation."""

    state_code: str
    company: str
    notice_date: date | None
    employees_affected: int | None
    city: str
    county: str
    layoff_type: str  # "Layoff" | "Closure" | other state-specific values
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class IngestResult:
    """Summary of a single-state ingestion run."""

    state_code: str = ""
    total: int = 0
    ingested: int = 0
    skipped: int = 0
    errors: int = 0
    error_details: list[str] = field(default_factory=list)
    dlq_ids: list[UUID] = field(default_factory=list)
    run_id: UUID = field(default_factory=_uuid_mod.uuid4)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _is_retriable(exc: BaseException) -> bool:
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429:
        return True
    return False


def _fetch(url: str, *, client: httpx.Client | None = None) -> bytes:
    """GET *url* with retry; returns raw response bytes.

    The HTTP timeout is loaded from ``cam.config`` Settings (``warn_http_timeout``,
    default 60 s) when making live requests.  Injected clients (used in tests)
    are called with a fixed 60 s timeout since the mock ignores the value.
    """
    breaker = get_breaker("warn")

    @retry(
        retry=retry_if_exception(_is_retriable),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _request() -> bytes:
        if client is not None:
            resp = client.get(url, timeout=60, follow_redirects=True)
        else:
            from cam.config import get_settings

            timeout = get_settings().warn_http_timeout
            resp = httpx.get(url, timeout=timeout, follow_redirects=True)
        resp.raise_for_status()
        return resp.content

    return breaker.call(_request)


# ---------------------------------------------------------------------------
# Date / numeric parsers
# ---------------------------------------------------------------------------


def _parse_date(value: str | None, fmt: str = "%m/%d/%Y") -> date | None:
    """Parse a date string with the given format; return None on failure."""
    raw = (value or "").strip()
    if not raw:
        return None
    for f in (fmt, "%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(raw, f).date()
        except ValueError:
            continue
    logger.warning("WARN: could not parse date %r", value)
    return None


def _parse_employees(value: str | None) -> int | None:
    """Parse employee-count strings like '350' or '1,200'."""
    raw = (value or "").strip().replace(",", "")
    try:
        n = int(raw)
        return n if n > 0 else None
    except ValueError:
        return None


def _clean_name(raw: str | None) -> str:
    """Strip trailing location suffixes like '- OAKLAND' from establishment names."""
    import re

    cleaned = (raw or "").strip()
    cleaned = re.sub(r"\s+-\s+[A-Z0-9 ]+$", "", cleaned)
    return cleaned.strip()


# ---------------------------------------------------------------------------
# Format parsers
# ---------------------------------------------------------------------------


def _parse_csv(content: bytes, cfg: StateConfig) -> list[WarnRecord]:
    """Parse a CSV-format WARN file according to *cfg* column mappings."""
    import csv

    cols = cfg.columns
    records: list[WarnRecord] = []
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        raw_company = row.get(cols.get("company", ""), "")
        records.append(
            WarnRecord(
                state_code=cfg.state_code,
                company=_clean_name(raw_company),
                notice_date=_parse_date(row.get(cols.get("date", ""), ""), cfg.date_fmt),
                employees_affected=_parse_employees(row.get(cols.get("employees", ""), "")),
                city=(row.get(cols.get("city", ""), "") or "").strip(),
                county=(row.get(cols.get("county", ""), "") or "").strip(),
                layoff_type=(row.get(cols.get("layoff_type", ""), "") or "").strip(),
                raw=dict(row),
            )
        )
    return records


def _parse_html(content: bytes, cfg: StateConfig) -> list[WarnRecord]:
    """Parse an HTML table WARN page according to *cfg* column mappings.

    Locates the first ``<table>`` tag (or the one with id matching
    *cfg.html_table_id* if set) and extracts rows.  Falls back gracefully
    if BeautifulSoup is unavailable.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:  # pragma: no cover
        logger.error("beautifulsoup4 not installed; cannot parse HTML WARN pages")
        return []

    cols = cfg.columns
    soup = BeautifulSoup(content.decode("utf-8", errors="replace"), "html.parser")

    # Find the right table
    table = None
    if cfg.html_table_id:
        table = soup.find("table", {"id": cfg.html_table_id})
    if table is None:
        table = soup.find("table")
    if table is None:
        logger.warning("WARN[%s]: no <table> found in HTML page", cfg.state_code)
        return []

    # Extract headers from the first <tr> with <th> cells
    headers: list[str] = []
    for tr in table.find_all("tr"):
        ths = tr.find_all("th")
        if ths:
            headers = [th.get_text(strip=True) for th in ths]
            break

    if not headers:
        logger.warning("WARN[%s]: no <th> headers found in table", cfg.state_code)
        return []

    records: list[WarnRecord] = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds or len(tds) != len(headers):
            continue
        row = {headers[i]: tds[i].get_text(strip=True) for i in range(len(headers))}
        raw_company = row.get(cols.get("company", ""), "")
        records.append(
            WarnRecord(
                state_code=cfg.state_code,
                company=_clean_name(raw_company),
                notice_date=_parse_date(row.get(cols.get("date", ""), ""), cfg.date_fmt),
                employees_affected=_parse_employees(row.get(cols.get("employees", ""), "")),
                city=(row.get(cols.get("city", ""), "") or "").strip(),
                county=(row.get(cols.get("county", ""), "") or "").strip(),
                layoff_type=(row.get(cols.get("layoff_type", ""), "") or "").strip(),
                raw=row,
            )
        )
    return records


def _parse_pdf(content: bytes, cfg: StateConfig) -> list[WarnRecord]:
    """Extract WARN records from a PDF document using pdfplumber.

    Each page is scanned for tables; the first table with a recognisable
    header row is used.  Returns an empty list if no usable table is found
    or if pdfplumber raises an exception.
    """
    try:
        import pdfplumber
    except ImportError:  # pragma: no cover
        logger.error("pdfplumber not installed; cannot parse PDF WARN pages")
        return []

    cols = cfg.columns
    records: list[WarnRecord] = []

    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            headers: list[str] = []
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    if not table:
                        continue
                    # First row is the header
                    candidate_headers = [str(c or "").strip() for c in table[0]]
                    if not headers and any(candidate_headers):
                        headers = candidate_headers
                        data_rows = table[1:]
                    else:
                        data_rows = table  # continuation page — no header row

                    for row_cells in data_rows:
                        if len(row_cells) != len(headers):
                            continue
                        row = {
                            headers[i]: str(row_cells[i] or "").strip() for i in range(len(headers))
                        }
                        raw_company = row.get(cols.get("company", ""), "")
                        if not raw_company:
                            continue
                        records.append(
                            WarnRecord(
                                state_code=cfg.state_code,
                                company=_clean_name(raw_company),
                                notice_date=_parse_date(
                                    row.get(cols.get("date", ""), ""), cfg.date_fmt
                                ),
                                employees_affected=_parse_employees(
                                    row.get(cols.get("employees", ""), "")
                                ),
                                city=(row.get(cols.get("city", ""), "") or "").strip(),
                                county=(row.get(cols.get("county", ""), "") or "").strip(),
                                layoff_type=(
                                    row.get(cols.get("layoff_type", ""), "") or ""
                                ).strip(),
                                raw=row,
                            )
                        )
    except Exception as exc:
        logger.error("WARN[%s]: PDF extraction failed: %s", cfg.state_code, exc)

    return records


# ---------------------------------------------------------------------------
# Idempotency helpers
# ---------------------------------------------------------------------------


def _idempotency_key(
    state_code: str,
    company: str,
    notice_date: date | None,
    raw: dict[str, Any] | None = None,
) -> str:
    """Build a stable dedup key for a WARN record.

    When *notice_date* is None (unparseable or absent), a SHA-256 hash of the
    raw source fields is used instead of the constant ``"unknown"`` so that two
    distinct notices for the same (state, company) without a parseable date are
    not collapsed into the same key and silently dropped on subsequent runs.
    """
    if notice_date is not None:
        date_str = notice_date.isoformat()
    else:
        raw_bytes = json.dumps(raw or {}, sort_keys=True, default=str).encode()
        date_str = "no-date:" + hashlib.sha256(raw_bytes).hexdigest()[:16]
    return f"{state_code}::{company.lower().strip()}::{date_str}"


def _existing_keys(db: Session, state_code: str) -> set[str]:
    """Return all idempotency keys already persisted for *state_code*."""
    stmt = select(Event.raw_json["_warn_key"].as_string()).where(
        Event.source == "warn",
        Event.raw_json["state_code"].as_string() == state_code,
    )
    rows = db.execute(stmt).scalars().all()
    return {r for r in rows if r}


# ---------------------------------------------------------------------------
# Core ingestion
# ---------------------------------------------------------------------------


def _records_to_events(
    records: list[WarnRecord],
    since_date: date | None,
    existing_keys: set[str],
    db: Session,
    run_id: _uuid_mod.UUID | None = None,
) -> IngestResult:
    """Resolve entities and insert WARN events; returns ingestion counts."""
    result = IngestResult(run_id=run_id or _uuid_mod.uuid4())
    result.total = len(records)

    # Date guard (same pattern as M3/M4: None dates are excluded when filter active)
    if since_date is not None:
        records = [r for r in records if r.notice_date is not None and r.notice_date >= since_date]

    # Idempotency filter
    to_process: list[WarnRecord] = []
    for rec in records:
        key = _idempotency_key(rec.state_code, rec.company, rec.notice_date, rec.raw)
        if key in existing_keys:
            result.skipped += 1
        else:
            to_process.append(rec)

    if not to_process:
        return result

    # Bulk entity resolution (commit=False; caller owns the commit).
    resolve_records = [{"name": r.company} for r in to_process]
    resolved = bulk_resolve(resolve_records, "warn", db=db, commit=False)

    for rec, res in zip(to_process, resolved):
        key = _idempotency_key(rec.state_code, rec.company, rec.notice_date, rec.raw)

        if res.entity_id is None and not res.needs_review:
            failure = record_failure(
                db,
                source="warn",
                run_id=result.run_id,
                raw_record={
                    "company": rec.company,
                    "state_code": rec.state_code,
                    "city": rec.city,
                    **rec.raw,
                },
                error_type=ERROR_ENTITY_RESOLUTION,
                exc=ValueError(
                    f"No entity match for {rec.company!r} (confidence={res.confidence:.2f})"
                ),
                raw_key=key,
            )
            if failure is not None:
                result.dlq_ids.append(failure.id)
            result.errors += 1
            result.error_details.append(f"{rec.company}: entity resolution failed")
        else:
            try:
                raw_json: dict[str, Any] = {
                    "_warn_key": key,
                    "state_code": rec.state_code,
                    "company": rec.company,
                    "city": rec.city,
                    "county": rec.county,
                    "layoff_type": rec.layoff_type,
                    "employees_affected": rec.employees_affected,
                    **rec.raw,
                }
                with db.begin_nested():  # SAVEPOINT
                    event = Event(
                        entity_id=res.entity_id,
                        source="warn",
                        event_type="warn_notice",
                        event_date=rec.notice_date,
                        description=(
                            f"{rec.layoff_type}: {rec.employees_affected or '?'} employees "
                            f"affected at {rec.company}, {rec.city}, {rec.state_code}"
                        ),
                        raw_json=raw_json,
                    )
                    db.add(event)
                result.ingested += 1
            except Exception as exc:
                failure = record_failure(
                    db,
                    source="warn",
                    run_id=result.run_id,
                    raw_record={"company": rec.company, "state_code": rec.state_code, **rec.raw},
                    error_type=ERROR_DB_WRITE,
                    exc=exc,
                    raw_key=key,
                )
                if failure is not None:
                    result.dlq_ids.append(failure.id)
                result.errors += 1
                result.error_details.append(f"{rec.company}: {exc}")

    # Caller (ingest_state / ingest_all_states) owns the commit boundary.
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def ingest_state(
    state_code: str,
    since_date: date | None = None,
    *,
    db: Session,
    client: httpx.Client | None = None,
) -> IngestResult:
    """Ingest WARN filings for a single state.

    Downloads the state's WARN data, parses it according to the configured
    format, resolves company names to entities, and inserts new Event records.
    Idempotent: existing records are skipped.

    Parameters
    ----------
    state_code:  Two-letter state abbreviation (e.g. "CA").  Must exist in
                 ``STATE_CONFIGS``.
    since_date:  If set, only records with notice_date >= since_date are
                 ingested.  Records with unparseable dates are excluded.
    db:          SQLAlchemy session.
    client:      Optional ``httpx.Client`` for connection reuse / mocking.
    """
    result = IngestResult(state_code=state_code)

    cfg = STATE_CONFIGS.get(state_code.upper())
    if cfg is None:
        result.errors += 1
        result.error_details.append(f"Unknown state code: {state_code!r}")
        return result

    # Fetch
    try:
        content = _fetch(cfg.url, client=client)
    except Exception as exc:
        result.errors += 1
        result.error_details.append(f"Fetch failed for {state_code}: {exc}")
        return result

    # Parse
    try:
        if cfg.format == "csv":
            records = _parse_csv(content, cfg)
        elif cfg.format == "html":
            records = _parse_html(content, cfg)
        elif cfg.format == "pdf":
            records = _parse_pdf(content, cfg)
        else:
            result.errors += 1
            result.error_details.append(f"Unsupported format: {cfg.format!r}")
            return result
    except Exception as exc:
        result.errors += 1
        result.error_details.append(f"Parse failed for {state_code}: {exc}")
        return result

    # Persist — single commit at end per CLAUDE.md transaction-ownership rule
    existing = _existing_keys(db, state_code.upper())
    sub = _records_to_events(records, since_date, existing, db)

    result.total = sub.total
    result.ingested = sub.ingested
    result.skipped = sub.skipped
    result.errors += sub.errors
    result.error_details.extend(sub.error_details)
    db.commit()
    return result


def ingest_all_states(
    since_date: date | None = None,
    *,
    db: Session,
    max_workers: int = 4,
    client: httpx.Client | None = None,
) -> list[IngestResult]:
    """Ingest all configured priority states in parallel.

    Uses a thread pool of *max_workers* workers.  Each worker gets its own
    DB session scope handled by the caller — the shared ``db`` session is
    accessed sequentially within each task after network I/O completes.

    Note: network fetching is the bottleneck; DB writes are serialised per
    state to avoid SQLite/Postgres session sharing across threads.

    Parameters
    ----------
    since_date:   Passed through to each ``ingest_state`` call.
    db:           SQLAlchemy session.
    max_workers:  Thread pool size (default 4).
    client:       Optional shared httpx.Client for connection reuse / mocking.
    """
    state_codes = list(STATE_CONFIGS.keys())
    results: list[IngestResult] = []

    # Fetch all states in parallel; persist sequentially to avoid session contention
    fetched: dict[str, tuple[bytes | None, str | None]] = {}

    def _fetch_state(code: str) -> tuple[str, bytes | None, str | None]:
        cfg = STATE_CONFIGS[code]
        try:
            data = _fetch(cfg.url, client=client)
            return code, data, None
        except Exception as exc:
            return code, None, str(exc)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_state, code): code for code in state_codes}
        for future in as_completed(futures):
            code, data, err = future.result()
            fetched[code] = (data, err)

    # Parse and persist sequentially
    for code in state_codes:
        data, err = fetched[code]
        result = IngestResult(state_code=code)

        if err or data is None:
            result.errors += 1
            result.error_details.append(err or "No data returned")
            results.append(result)
            continue

        cfg = STATE_CONFIGS[code]
        try:
            if cfg.format == "csv":
                records = _parse_csv(data, cfg)
            elif cfg.format == "html":
                records = _parse_html(data, cfg)
            elif cfg.format == "pdf":
                records = _parse_pdf(data, cfg)
            else:
                result.errors += 1
                result.error_details.append(f"Unsupported format: {cfg.format!r}")
                results.append(result)
                continue
        except Exception as exc:
            result.errors += 1
            result.error_details.append(f"Parse failed: {exc}")
            results.append(result)
            continue

        existing = _existing_keys(db, code)
        sub = _records_to_events(records, since_date, existing, db)
        result.total = sub.total
        result.ingested = sub.ingested
        result.skipped = sub.skipped
        result.errors += sub.errors
        result.error_details.extend(sub.error_details)
        results.append(result)

    # Single commit for the entire batch — honours the single-commit-at-end rule.
    db.commit()
    return results


def get_pe_owned_entities(db: Session) -> list[UUID]:
    """Return entity IDs that have been flagged as PE-owned.

    PE ownership is stored as a Signal record with signal_type="pe_owned".
    The list requires ongoing manual curation (see PLAN.md M11 spec).

    Returns an empty list if the signals table has no PE ownership records.
    """
    stmt = select(Signal.entity_id).where(Signal.signal_type == "pe_owned")
    rows = db.execute(stmt).scalars().all()
    return [r for r in rows if r is not None]
