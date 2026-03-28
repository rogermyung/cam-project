"""
M3 — OSHA Ingestion

Ingests OSHA inspection records, violation citations, and penalty data from
bulk CSV downloads and the DOL near-real-time API.

Data sources:
  Bulk CSV:  https://www.osha.gov/foia/enforcement-data
  DOL API:   https://data.dol.gov/get/inspections

Resilience (M15):
  - Per-record DLQ on entity resolution failure or DB write error
  - Checkpoint every CHECKPOINT_EVERY records so restarts resume mid-file
  - Per-entity savepoint isolation (one bad record cannot roll back others)
  - Circuit breaker on DOL API calls
  - Structured log fields on every failure
"""

from __future__ import annotations

import csv
import logging
import re
import tempfile
import uuid
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from cam.db.models import Event
from cam.entity.resolver import bulk_resolve
from cam.ingestion.base import IngestResult
from cam.ingestion.checkpoint import complete_checkpoint, load_checkpoint, save_checkpoint
from cam.ingestion.circuit_breaker import get_breaker
from cam.ingestion.dlq import (
    ERROR_DB_WRITE,
    ERROR_ENTITY_RESOLUTION,
    record_failure,
)

logger = logging.getLogger(__name__)

# Base URLs
_OSHA_BULK_BASE = "https://www.osha.gov/foia/enforcement-data"
_DOL_INSPECTIONS_URL = "https://data.dol.gov/get/inspections"

# OSHA uses string dollar amounts; strip these chars before parsing.
_PENALTY_STRIP_RE = re.compile(r"[\$,\s]")

# OSHA establishment names often include a city/location suffix: "CO - DALLAS".
# Strip the " - <SUFFIX>" portion to recover the canonical company name.
_ESTAB_SUFFIX_RE = re.compile(r"\s+-\s+[A-Z0-9 ]+$")

# Write a checkpoint every N records processed.
CHECKPOINT_EVERY = 500


# ---------------------------------------------------------------------------
# Retry helpers
# ---------------------------------------------------------------------------


def _is_retriable_error(exc: BaseException) -> bool:
    """Return True for transient network errors and HTTP 429 rate-limits."""
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429:
        return True
    return False


def _make_retry_decorator():
    return retry(
        retry=retry_if_exception(_is_retriable_error),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        stop=stop_after_attempt(5),
        reraise=True,
    )


def _get(
    url: str, params: dict[str, Any] | None = None, *, client: httpx.Client | None = None
) -> httpx.Response:
    """GET with automatic retry on transient errors and 429 back-off."""
    breaker = get_breaker("osha_api")

    @_make_retry_decorator()
    def _request() -> httpx.Response:
        if client is not None:
            resp = client.get(url, params=params, timeout=60)
        else:
            resp = httpx.get(url, params=params, timeout=60)
        resp.raise_for_status()
        return resp

    return breaker.call(_request)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _clean_estab_name(raw: str | None) -> str:
    """Strip trailing '- CITY' location suffixes from OSHA establishment names.

    Example: "AMAZON.COM SERVICES LLC - BALTIMORE" → "AMAZON.COM SERVICES LLC"
    """
    cleaned = (raw or "").strip()
    cleaned = _ESTAB_SUFFIX_RE.sub("", cleaned)
    return cleaned.strip()


def _parse_date(value: str | None) -> date | None:
    """Parse OSHA date strings in YYYYMMDD, YYYY-MM-DD, or MM/DD/YYYY formats."""
    value = (value or "").strip()
    if not value:
        return None
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            from datetime import datetime

            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    logger.warning("Could not parse OSHA date: %r", value)
    return None


def _parse_penalty(value: str | None) -> Decimal | None:
    """Parse OSHA penalty strings like '12500', '$12,500', or '12500.00'."""
    cleaned = _PENALTY_STRIP_RE.sub("", (value or "").strip())
    if not cleaned:
        return None
    try:
        amount = Decimal(cleaned)
        return amount if amount > 0 else None
    except InvalidOperation:
        logger.warning("Could not parse OSHA penalty: %r", value)
        return None


def _event_type(row: dict[str, str]) -> str:
    """Return 'violation' when a violation_type code is present, else 'inspection'."""
    return "violation" if (row.get("violation_type") or "").strip() else "inspection"


def _description(row: dict[str, str]) -> str | None:
    """Build a human-readable description from violation_type + citation_text."""
    vtype = (row.get("violation_type") or "").strip()
    text = (row.get("citation_text") or "").strip()
    if vtype and text:
        return f"{vtype}: {text}"
    return text or vtype or None


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _get_existing_activity_nrs(db: Session) -> set[str]:
    """Return all activity_nr values already persisted for source='osha'."""
    stmt = select(Event.raw_json["activity_nr"].as_string()).where(Event.source == "osha")
    rows = db.execute(stmt).scalars().all()
    return {r for r in rows if r}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def download_bulk_data(year: int, *, client: httpx.Client | None = None) -> Path:
    """Download full OSHA enforcement CSV for a given year.

    Saves to a temporary file and returns its Path.
    """
    url = f"{_OSHA_BULK_BASE}/osha_{year}.csv"
    logger.info("Downloading OSHA bulk CSV for %d from %s", year, url)
    resp = _get(url, client=client)
    out_path = Path(tempfile.gettempdir()) / f"osha_{year}.csv"
    out_path.write_bytes(resp.content)
    logger.info("Saved OSHA CSV to %s (%d bytes)", out_path, len(resp.content))
    return out_path


def ingest_from_csv(
    csv_path: Path,
    since_date: date | None = None,
    *,
    db: Session,
    run_id: uuid.UUID | None = None,
    resume: bool = True,
) -> IngestResult:
    """Parse an OSHA enforcement CSV, resolve entities, and insert events.

    Idempotent: rows whose activity_nr is already in the events table are
    skipped.  Runs entity resolution in bulk to minimise DB round-trips.

    Parameters
    ----------
    csv_path:   Path to the CSV file (local).
    since_date: If provided, only rows with open_date >= since_date are ingested.
    db:         SQLAlchemy session.
    run_id:     UUID for this run (used for DLQ and checkpoints).  A new UUID
                is generated if not provided.
    resume:     If True, load the latest incomplete checkpoint and skip ahead.
    """
    result = IngestResult(run_id=run_id or uuid.uuid4())

    # --- Read CSV ---
    rows: list[dict[str, str]] = []
    try:
        with csv_path.open(newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rows.append(dict(row))
    except OSError as exc:
        result.errors += 1
        result.error_details.append(f"Could not open CSV: {exc}")
        return result

    result.total = len(rows)

    # --- Filter by since_date ---
    # Rows with an unparseable open_date are excluded when since_date is set.
    if since_date is not None:
        filtered = []
        for row in rows:
            d = _parse_date(row.get("open_date", ""))
            if d is not None and d >= since_date:
                filtered.append(row)
        rows = filtered

    # --- Idempotency: skip already-ingested activity_nrs ---
    existing_nrs = _get_existing_activity_nrs(db)

    to_process = []
    for row in rows:
        nr = row.get("activity_nr", "").strip()
        if nr and nr in existing_nrs:
            result.skipped += 1
        else:
            to_process.append(row)

    if not to_process:
        complete_checkpoint(db, "osha", result.run_id)
        db.commit()
        return result

    # --- Resume from checkpoint ---
    start_offset = 0
    if resume:
        cursor = load_checkpoint(db, source="osha", run_id=result.run_id)
        if cursor is not None:
            start_offset = cursor.get("offset", 0)
            logger.info("Resuming OSHA ingest from offset %d", start_offset)
            result.ingested = cursor.get("records_ok", 0)
            result.errors = cursor.get("records_err", 0)

    to_process = to_process[start_offset:]

    # --- Bulk entity resolution ---
    # commit=False: ingest_from_csv owns the transaction boundary.
    resolve_records = [{"name": _clean_estab_name(r.get("estab_name", ""))} for r in to_process]
    resolved = bulk_resolve(resolve_records, "osha", db, commit=False)

    # --- Insert events with per-entity savepoint isolation ---
    for i, (row, res) in enumerate(zip(to_process, resolved)):
        absolute_offset = start_offset + i
        nr = row.get("activity_nr", "").strip()

        # Entity resolution failure → DLQ
        if res.entity_id is None and not res.needs_review:
            failure = record_failure(
                db,
                source="osha",
                run_id=result.run_id,
                raw_record={**row, "activity_nr": nr},
                error_type=ERROR_ENTITY_RESOLUTION,
                exc=ValueError(
                    f"No entity match for {row.get('estab_name')!r} "
                    f"(confidence={res.confidence:.2f})"
                ),
                raw_key=nr or None,
            )
            if failure is not None:
                result.dlq_ids.append(failure.id)
            result.errors += 1
            result.error_details.append(f"activity_nr={nr}: entity resolution failed")
        else:
            try:
                with db.begin_nested():  # SAVEPOINT — isolates this record's failure
                    event = Event(
                        entity_id=res.entity_id,
                        source="osha",
                        event_type=_event_type(row),
                        event_date=_parse_date(row.get("open_date", "")),
                        penalty_usd=_parse_penalty(row.get("initial_penalty", "")),
                        description=_description(row),
                        raw_json={**row, "activity_nr": nr},
                    )
                    db.add(event)
                result.ingested += 1
            except Exception as exc:  # noqa: BLE001
                failure = record_failure(
                    db,
                    source="osha",
                    run_id=result.run_id,
                    raw_record={**row, "activity_nr": nr},
                    error_type=ERROR_DB_WRITE,
                    exc=exc,
                    raw_key=nr or None,
                )
                if failure is not None:
                    result.dlq_ids.append(failure.id)
                result.errors += 1
                result.error_details.append(f"activity_nr={nr}: {exc}")

        # Checkpoint every CHECKPOINT_EVERY records
        if (absolute_offset + 1) % CHECKPOINT_EVERY == 0:
            save_checkpoint(
                db,
                source="osha",
                run_id=result.run_id,
                cursor={"offset": absolute_offset + 1},
                records_ok=result.ingested,
                records_err=result.errors,
            )

    complete_checkpoint(db, "osha", result.run_id)
    result.checkpoint = {"offset": start_offset + len(to_process)}
    db.commit()
    return result


def fetch_recent_inspections(
    days_back: int = 30,
    *,
    client: httpx.Client | None = None,
) -> list[dict]:
    """Poll the DOL API for OSHA inspections in the last *days_back* days.

    Returns a list of raw inspection dicts as returned by the API.
    Intended for near-real-time updates between bulk CSV refreshes.
    Raises CircuitOpenError if the OSHA API circuit breaker is open.
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)
    params = {
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
    }
    logger.info(
        "Fetching recent OSHA inspections from DOL API (%s to %s)",
        start_date,
        end_date,
    )
    resp = _get(_DOL_INSPECTIONS_URL, params=params, client=client)
    data = resp.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("data", data.get("inspections", []))
    logger.warning("Unexpected DOL API response type %s; returning empty list", type(data).__name__)
    return []
