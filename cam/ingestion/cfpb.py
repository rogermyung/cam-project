"""
M5 — CFPB Ingestion

Ingests CFPB consumer complaint database records via the bulk CSV download
and provides complaint-rate normalisation and spike-detection analytics.

Consumer complaint velocity is a leading indicator of consumer harm before
formal regulatory action.  Raw complaint counts are stored per-event; analytics
functions normalise against total assets from EDGAR financial data (soft
dependency on M2).

Data sources:
  Bulk CSV: https://files.consumerfinance.gov/ccdb/complaints.csv.zip
  (Updated daily; contains all complaints ever filed.)
"""

from __future__ import annotations

import csv
import io
import logging
import tempfile
import uuid
import zipfile
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import UUID

import httpx
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from cam.config import get_settings
from cam.db.models import Event
from cam.entity.resolver import bulk_resolve
from cam.ingestion.base import IngestResult
from cam.ingestion.circuit_breaker import get_breaker
from cam.ingestion.dlq import ERROR_DB_WRITE, ERROR_ENTITY_RESOLUTION, record_failure

logger = logging.getLogger(__name__)

# CSV column name → internal field name
_CSV_COL_MAP: dict[str, str] = {
    "Date received": "date_received",
    "Product": "product",
    "Sub-product": "sub_product",
    "Issue": "issue",
    "Sub-issue": "sub_issue",
    "Consumer complaint narrative": "complaint_what_happened",
    "Company public response": "company_public_response",
    "Company": "company",
    "State": "state",
    "ZIP code": "zip_code",
    "Tags": "tags",
    "Consumer consent provided?": "consumer_consent_provided",
    "Submitted via": "submitted_via",
    "Date sent to company": "date_sent_to_company",
    "Company response to consumer": "company_response",
    "Timely response?": "timely",
    "Consumer disputed?": "consumer_disputed",
    "Complaint ID": "complaint_id",
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ComplaintRate:
    """Normalised complaint rate for an entity over a time window."""

    complaints: int
    period_months: int
    rate_per_billion: float | None  # complaints per $1B total assets
    total_assets_usd: Decimal | None


# ---------------------------------------------------------------------------
# Retry helpers
# ---------------------------------------------------------------------------


def _is_retriable_error(exc: BaseException) -> bool:
    """Return True for transient network errors and HTTP 429/5xx errors."""
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in (
        429,  # rate-limited
        500,  # internal server error (transient)
        502,  # bad gateway
        503,  # service unavailable
        504,  # gateway timeout
    ):
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
    url: str,
    *,
    client: httpx.Client | None = None,
    timeout: int = 300,
) -> httpx.Response:
    breaker = get_breaker("cfpb")

    @_make_retry_decorator()
    def _request() -> httpx.Response:
        if client is not None:
            resp = client.get(url, timeout=timeout)
        else:
            resp = httpx.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp

    return breaker.call(_request)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_date(value: str | None) -> date | None:
    """Parse CFPB complaint date strings.

    Handles multiple formats returned by the bulk CSV and legacy API:
    - "YYYY-MM-DD"                   (plain date, most common in bulk CSV)
    - "MM/DD/YYYY"                   (legacy US format)
    - "YYYY-MM-DDTHH:MM:SS±HH:MM"   (ISO 8601 with timezone offset)
    """
    from datetime import datetime

    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    logger.warning("Could not parse CFPB date: %r", value)
    return None


def _parse_decimal(value: str | int | float | None) -> Decimal | None:
    """Parse a numeric value to Decimal, returning None for empty/invalid."""
    if value is None:
        return None
    cleaned = str(value).strip().replace(",", "")
    if not cleaned:
        return None
    try:
        d = Decimal(cleaned)
        return d if d >= 0 else None
    except InvalidOperation:
        return None


def _clean_company_name(raw: str | None) -> str:
    """Normalise CFPB company names for entity resolution.

    CFPB uses all-caps legal names with suffixes like
    'WELLS FARGO BANK, NATIONAL ASSOCIATION'. Strip common legal
    suffixes and trailing punctuation to improve match rates.
    """
    import re

    name = (raw or "").strip()
    # Remove common legal suffixes
    name = (
        re.sub(
            r",?\s*(NATIONAL ASSOCIATION|N\.A\.|NA|BANK|CORP\.?|INC\.?|LLC\.?|LTD\.?)$",
            "",
            name,
            flags=re.IGNORECASE,
        )
        .strip()
        .rstrip(",")
        .strip()
    )
    return name


# ---------------------------------------------------------------------------
# Bulk CSV helpers
# ---------------------------------------------------------------------------


def _csv_row_to_complaint(row: dict[str, str]) -> dict | None:
    """Convert a CSV DictReader row to an internal complaint dict.

    Returns None if the row is missing a Complaint ID (cannot track idempotency).
    """
    complaint_id = row.get("Complaint ID", "").strip()
    if not complaint_id:
        logger.warning("Dropping CSV row with missing Complaint ID")
        return None
    mapped = {_CSV_COL_MAP.get(k, k): v for k, v in row.items()}
    mapped["complaint_id"] = complaint_id  # ensure key always present
    return mapped


def _parse_bulk_zip(zip_bytes: bytes, since_date: date) -> list[dict]:
    """Extract and parse complaints from a ZIP archive bytes, filtered to >= since_date.

    The CFPB bulk archive contains a single CSV with all complaints ever filed.
    We stream through it row-by-row, keeping only those on or after since_date,
    to avoid loading the full multi-million-row dataset into memory.

    .. note::
        This variant accepts raw bytes and is retained for use in unit tests and
        helpers that already hold the zip in memory (e.g. ``_make_zip_response``
        in tests). For the production download path use
        ``_parse_bulk_zip_from_path`` which never materialises the full archive.
    """
    complaints: list[dict] = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            # Use the first CSV file in the archive regardless of exact filename
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                logger.warning("No CSV found in CFPB bulk zip; contents: %s", zf.namelist())
                return []
            with zf.open(csv_names[0]) as raw_file:
                reader = csv.DictReader(io.TextIOWrapper(raw_file, encoding="utf-8-sig"))
                for row in reader:
                    complaint = _csv_row_to_complaint(row)
                    if complaint is None:
                        continue
                    d = _parse_date(complaint.get("date_received"))
                    if d is not None and d >= since_date:
                        complaints.append(complaint)
    except (zipfile.BadZipFile, KeyError, UnicodeDecodeError) as exc:
        logger.warning("Failed to parse CFPB bulk zip: %s", exc)
    return complaints


def _parse_bulk_zip_from_path(zip_path: Path, since_date: date) -> list[dict]:
    """Extract and parse complaints from a ZIP file on disk, filtered to >= since_date.

    Identical logic to ``_parse_bulk_zip`` but reads from a file path so the
    full archive is never loaded into memory — critical for the real 1 GB+ file.
    """
    complaints: list[dict] = []
    try:
        with zipfile.ZipFile(zip_path) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                logger.warning("No CSV found in CFPB bulk zip; contents: %s", zf.namelist())
                return []
            with zf.open(csv_names[0]) as raw_file:
                reader = csv.DictReader(io.TextIOWrapper(raw_file, encoding="utf-8-sig"))
                for row in reader:
                    complaint = _csv_row_to_complaint(row)
                    if complaint is None:
                        continue
                    d = _parse_date(complaint.get("date_received"))
                    if d is not None and d >= since_date:
                        complaints.append(complaint)
    except (zipfile.BadZipFile, KeyError, UnicodeDecodeError, OSError) as exc:
        logger.warning("Failed to parse CFPB bulk zip at %s: %s", zip_path, exc)
    return complaints


def _stream_to_tempfile(
    url: str,
    *,
    client: httpx.Client | None = None,
    timeout: int = 300,
    chunk_size: int = 1 << 20,  # 1 MiB
) -> Path:
    """Stream a URL response body to a temporary file and return its path.

    Uses ``httpx`` streaming so the response body is never held in memory in
    its entirety — each chunk is written directly to disk.  The caller is
    responsible for deleting the returned file when finished.

    The retry / circuit-breaker semantics from :func:`_get` are preserved: a
    ``@_make_retry_decorator()``-wrapped inner function is passed to the breaker
    so that transient network errors are retried before surfacing.

    Parameters
    ----------
    url:        URL to download.
    client:     Optional ``httpx.Client`` (allows injection in tests).
    timeout:    Total timeout in seconds passed to httpx.
    chunk_size: Bytes per read iteration (default 1 MiB).
    """
    breaker = get_breaker("cfpb")

    # Use a NamedTemporaryFile with delete=False so the file persists after
    # close and can be passed by path to zipfile.ZipFile.
    tmp = tempfile.NamedTemporaryFile(
        prefix="cfpb_bulk_",
        suffix=".zip",
        delete=False,
    )
    tmp_path = Path(tmp.name)

    @_make_retry_decorator()
    def _do_stream() -> None:
        # Truncate if we are retrying after a partial write.
        tmp.seek(0)
        tmp.truncate()

        ctx = (
            client.stream("GET", url, timeout=timeout)
            if client is not None
            else httpx.stream("GET", url, timeout=timeout)
        )
        with ctx as resp:
            resp.raise_for_status()
            for chunk in resp.iter_bytes(chunk_size=chunk_size):
                if chunk:
                    tmp.write(chunk)

    try:
        breaker.call(_do_stream)
    except Exception:
        # Clean up the temp file on error so callers don't have to.
        tmp.close()
        tmp_path.unlink(missing_ok=True)
        raise

    tmp.flush()
    tmp.close()
    return tmp_path


def _fetch_bulk_complaints(since_date: date, *, client: httpx.Client | None = None) -> list[dict]:
    """Download the CFPB bulk complaints ZIP and return complaints >= since_date.

    The ZIP is streamed to a temporary file on disk in chunks so the 1 GB+
    archive is never held fully in memory.  The temp file is deleted before
    this function returns.
    """
    url = get_settings().cfpb_bulk_url
    logger.info("Downloading CFPB bulk complaints ZIP from %s", url)
    tmp_path = _stream_to_tempfile(url, client=client, timeout=300)
    try:
        result = _parse_bulk_zip_from_path(tmp_path, since_date)
    finally:
        tmp_path.unlink(missing_ok=True)
    logger.info("CFPB bulk zip: %d complaints on or after %s", len(result), since_date)
    return result


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _get_existing_complaint_ids(db: Session) -> set[str]:
    """Return all complaint_id values already persisted."""
    stmt = select(Event.raw_json["complaint_id"].as_string()).where(
        Event.source == "cfpb_complaint"
    )
    rows = db.execute(stmt).scalars().all()
    return {r for r in rows if r}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def ingest_complaints(
    since_date: date,
    *,
    db: Session,
    client: httpx.Client | None = None,
    complaints: list[dict] | None = None,
    run_id: uuid.UUID | None = None,
) -> IngestResult:
    """Ingest new complaints from the CFPB bulk CSV download.

    Parameters
    ----------
    since_date:  Only process complaints with date_received >= this date.
    db:          SQLAlchemy session.
    client:      Optional httpx.Client for testing.
    complaints:  Optional pre-fetched list of complaint dicts (for testing).
                 Each dict must have ``complaint_id`` and ``date_received`` keys.
    run_id:      UUID for this run (used for DLQ entries).
    """
    result = IngestResult(run_id=run_id or uuid.uuid4())

    if complaints is None:
        complaints = _fetch_bulk_complaints(since_date, client=client)

    result.total = len(complaints)

    if not complaints:
        return result

    # Idempotency by complaint_id
    existing_ids = _get_existing_complaint_ids(db)

    # Filter: must have a complaint_id, not already in DB, and date_received >= since_date
    to_process = [
        c
        for c in complaints
        if (c.get("complaint_id") or "")  # skip blank IDs — cannot be tracked
        and (c.get("complaint_id") or "") not in existing_ids
        and (d := _parse_date(c.get("date_received"))) is not None
        and d >= since_date
    ]
    result.skipped = result.total - len(to_process)

    if not to_process:
        return result

    resolve_records = [{"name": _clean_company_name(c.get("company"))} for c in to_process]
    resolved = bulk_resolve(resolve_records, "cfpb_complaint", db, commit=False)

    for complaint, res in zip(to_process, resolved):
        complaint_id = complaint.get("complaint_id", "")

        if res.entity_id is None and not res.needs_review:
            failure = record_failure(
                db,
                source="cfpb_complaint",
                run_id=result.run_id,
                raw_record=complaint,
                error_type=ERROR_ENTITY_RESOLUTION,
                exc=ValueError(
                    f"No entity match for {complaint.get('company')!r} "
                    f"(confidence={res.confidence:.2f})"
                ),
                raw_key=complaint_id or None,
            )
            if failure is not None:
                result.dlq_ids.append(failure.id)
            result.errors += 1
            result.error_details.append(f"complaint_id={complaint_id}: entity resolution failed")
        else:
            try:
                event_date = _parse_date(complaint.get("date_received"))
                product = (complaint.get("product") or "").strip()
                issue = (complaint.get("issue") or "").strip()

                with db.begin_nested():  # SAVEPOINT
                    event = Event(
                        entity_id=res.entity_id,
                        source="cfpb_complaint",
                        event_type="complaint",
                        event_date=event_date,
                        penalty_usd=None,
                        description=f"{product}: {issue}" if product else issue or None,
                        raw_json={k: v for k, v in complaint.items()},
                    )
                    db.add(event)
                result.ingested += 1
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to ingest complaint %s: %s", complaint_id, exc)
                failure = record_failure(
                    db,
                    source="cfpb_complaint",
                    run_id=result.run_id,
                    raw_record=complaint,
                    error_type=ERROR_DB_WRITE,
                    exc=exc,
                    raw_key=complaint_id or None,
                )
                if failure is not None:
                    result.dlq_ids.append(failure.id)
                result.errors += 1
                result.error_details.append(f"complaint_id={complaint_id}: {exc}")

    db.commit()
    return result


def compute_complaint_rate(
    entity_id: UUID,
    period_months: int = 12,
    *,
    db: Session,
    period_end: date | None = None,
) -> ComplaintRate | None:
    """Return complaint rate per $1B total assets for the trailing period.

    Returns None if the entity has no financial data available for
    normalisation (requires EDGAR 10-K data from M2).

    Parameters
    ----------
    entity_id:     Entity to compute rate for.
    period_months: Trailing window in months (approximate: uses 30d/month).
    db:            SQLAlchemy session.
    period_end:    End of the analysis window (default: today).
    """
    period_end = period_end or date.today()
    period_start = period_end - timedelta(days=period_months * 30)

    # Count complaints in the window using SQL COUNT — avoids loading full rows
    stmt = select(func.count(Event.id)).where(
        Event.entity_id == entity_id,
        Event.source == "cfpb_complaint",
        Event.event_type == "complaint",
        Event.event_date >= period_start,
        Event.event_date <= period_end,
    )
    complaint_count: int = db.execute(stmt).scalar_one()

    # Get most recent total assets from EDGAR xbrl_facts
    edgar_stmt = select(Event).where(
        Event.entity_id == entity_id,
        Event.source == "sec_edgar",
        Event.event_type == "filing",
    )
    edgar_events = db.execute(edgar_stmt).scalars().all()

    total_assets: Decimal | None = None
    best_period_end = ""
    for ev in edgar_events:
        facts = (ev.raw_json or {}).get("xbrl_facts") or {}
        assets_entry = facts.get("Assets") or {}
        assets_val = _parse_decimal(assets_entry.get("value"))
        # Guard None; accept this entry if no asset found yet, or if it's more recent.
        assets_period = assets_entry.get("period_end") or ""
        if assets_val is not None and (not best_period_end or assets_period > best_period_end):
            total_assets = assets_val
            best_period_end = assets_period

    if total_assets is None or total_assets == 0:
        return None

    assets_billions = float(total_assets) / 1_000_000_000
    rate = complaint_count / assets_billions if assets_billions > 0 else None

    return ComplaintRate(
        complaints=complaint_count,
        period_months=period_months,
        rate_per_billion=rate,
        total_assets_usd=total_assets,
    )


def detect_complaint_spike(
    entity_id: UUID,
    lookback_months: int = 6,
    threshold_pct: float = 50.0,
    *,
    db: Session,
    period_end: date | None = None,
) -> bool:
    """Return True if complaint rate has spiked in the recent half of the lookback.

    Compares the most recent ``lookback_months // 2`` months against the prior
    ``lookback_months // 2`` months.  Returns True if the recent count exceeds
    the prior count by more than ``threshold_pct`` percent.

    A prior count of zero with any recent complaints is always a spike.

    Parameters
    ----------
    entity_id:       Entity to check.
    lookback_months: Total window to analyse (split evenly into two halves).
    threshold_pct:   Percentage increase that constitutes a spike (default 50%).
    db:              SQLAlchemy session.
    period_end:      End of the analysis window (default: today).
    """
    today = period_end or date.today()
    half = max(1, lookback_months // 2)
    recent_start = today - timedelta(days=half * 30)
    prior_start = today - timedelta(days=lookback_months * 30)

    def _count(start: date, end: date) -> int:
        stmt = select(func.count(Event.id)).where(
            Event.entity_id == entity_id,
            Event.source == "cfpb_complaint",
            Event.event_type == "complaint",
            Event.event_date >= start,
            Event.event_date < end,
        )
        return db.execute(stmt).scalar_one()

    recent_count = _count(recent_start, today)
    prior_count = _count(prior_start, recent_start)

    if prior_count == 0:
        return recent_count > 0

    return (recent_count / prior_count) > (1 + threshold_pct / 100)
