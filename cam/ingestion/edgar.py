"""
M2 — EDGAR Ingestion

Fetches SEC EDGAR filings for public companies and stores them in the events
table.  Priority filing types: 10-K (annual), DEF 14A (proxy), 8-K (material
events), S-4/424B (debt offerings).

Rate limit: 10 requests/second per EDGAR fair-access policy.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Any
from uuid import UUID

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from cam.config import get_settings

logger = logging.getLogger(__name__)

# EDGAR base URLs
_EDGAR_SUBMISSIONS_BASE = "https://data.sec.gov/submissions"
_EDGAR_COMPANY_TICKERS = "https://data.sec.gov/files/company_tickers.json"
_EDGAR_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"

# Seconds between requests to stay within EDGAR's 10 req/s limit.
# Tests override this via monkeypatch.
REQUEST_DELAY: float = 0.1


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class FilingMetadata:
    """Lightweight metadata for a single SEC filing."""

    cik: str  # zero-padded 10-digit CIK
    accession_number: str  # e.g. "0000320193-24-000006"
    filing_type: str  # e.g. "10-K"
    filed_date: date
    primary_document: str  # relative filename, e.g. "aapl-20231230.htm"
    entity_id: UUID | None = None


@dataclass
class FilingDocument:
    """Downloaded filing text with its object-store path."""

    metadata: FilingMetadata
    text: str
    object_store_path: str  # e.g. "edgar/0000320193/000032019324000006/full.txt"


@dataclass
class IngestResult:
    """Summary of a bulk ingestion run."""

    total: int = 0
    ingested: int = 0
    skipped: int = 0
    errors: int = 0
    error_details: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _headers() -> dict[str, str]:
    return {"User-Agent": get_settings().edgar_user_agent}


def _make_retry_decorator():
    """Return a tenacity retry decorator for transient EDGAR errors."""
    return retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        stop=stop_after_attempt(5),
        reraise=True,
    )


def _get(
    url: str,
    params: dict[str, Any] | None = None,
    *,
    client: httpx.Client | None = None,
) -> httpx.Response:
    """GET with automatic retry on transient errors and 429 back-off."""

    @_make_retry_decorator()
    def _request() -> httpx.Response:
        if client is not None:
            resp = client.get(url, params=params, headers=_headers(), timeout=30)
        else:
            resp = httpx.get(url, params=params, headers=_headers(), timeout=30)

        if resp.status_code == 429:
            # EDGAR rate-limit: treat as a retriable error by raising so
            # tenacity can apply exponential back-off.
            raise httpx.HTTPStatusError(
                "EDGAR rate limited (429)",
                request=resp.request,
                response=resp,
            )

        resp.raise_for_status()
        return resp

    return _request()


def _accession_no_dashes(accession_number: str) -> str:
    return accession_number.replace("-", "")


def _filing_url(cik: str, accession_number: str, primary_document: str) -> str:
    """Build the SEC Archives URL for a filing's primary document."""
    cik_int = str(int(cik))  # drop leading zeros for the Archives path
    acc_clean = _accession_no_dashes(accession_number)
    return f"{_EDGAR_ARCHIVES_BASE}/{cik_int}/{acc_clean}/{primary_document}"


def _object_store_key(cik: str, accession_number: str) -> str:
    """Canonical S3/MinIO key for a filing document."""
    acc_clean = _accession_no_dashes(accession_number)
    return f"edgar/{cik}/{acc_clean}/full.txt"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_cik_for_ticker(
    ticker: str,
    *,
    client: httpx.Client | None = None,
) -> str | None:
    """Resolve a ticker symbol to a zero-padded 10-digit CIK string.

    Returns None if the ticker is not found.
    """
    try:
        resp = _get(_EDGAR_COMPANY_TICKERS, client=client)
        data: dict[str, dict] = resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("Failed to fetch company tickers: %s", exc)
        return None

    upper = ticker.upper()
    for entry in data.values():
        if str(entry.get("ticker", "")).upper() == upper:
            return str(entry["cik_str"]).zfill(10)

    return None


def fetch_company_filings(
    cik: str,
    filing_types: list[str],
    since_date: date,
    *,
    client: httpx.Client | None = None,
) -> list[FilingMetadata]:
    """Fetch filing metadata for a company without downloading documents.

    Queries the EDGAR submissions API for the given CIK and returns metadata
    for all filings whose type is in *filing_types* and whose filing date is
    on or after *since_date*.  Follows pagination links in `filings.files` to
    retrieve older filings.

    Args:
        cik: Company CIK (will be zero-padded to 10 digits).
        filing_types: List of SEC form types to include, e.g. ["10-K"].
        since_date: Only return filings on or after this date.
        client: Optional httpx.Client for test injection.

    Returns:
        List of FilingMetadata, newest first.
    """
    padded = cik.zfill(10)
    url = f"{_EDGAR_SUBMISSIONS_BASE}/CIK{padded}.json"

    try:
        resp = _get(url, client=client)
        data = resp.json()
    except httpx.HTTPError as exc:
        logger.error("Failed to fetch submissions for CIK %s: %s", cik, exc)
        return []

    results: list[FilingMetadata] = []

    def _parse_recent(recent: dict) -> None:
        forms = recent.get("form", [])
        accessions = recent.get("accessionNumber", [])
        filed_dates = recent.get("filingDate", [])
        primary_docs = recent.get("primaryDocument", [])

        for form, acc, filed_str, doc in zip(forms, accessions, filed_dates, primary_docs):
            if form not in filing_types:
                continue
            try:
                filed = date.fromisoformat(filed_str)
            except (ValueError, TypeError):
                continue
            if filed < since_date:
                continue
            results.append(
                FilingMetadata(
                    cik=padded,
                    accession_number=acc,
                    filing_type=form,
                    filed_date=filed,
                    primary_document=doc,
                )
            )

    # Parse the "recent" block
    _parse_recent(data.get("filings", {}).get("recent", {}))

    # Follow pagination links for older filings
    for file_ref in data.get("filings", {}).get("files", []):
        old_url = f"{_EDGAR_SUBMISSIONS_BASE}/{file_ref['name']}"
        try:
            time.sleep(REQUEST_DELAY)
            old_resp = _get(old_url, client=client)
            _parse_recent(old_resp.json())
        except httpx.HTTPError as exc:
            logger.warning("Failed to fetch older filings from %s: %s", old_url, exc)

    return results


def download_filing(
    filing_metadata: FilingMetadata,
    *,
    client: httpx.Client | None = None,
    s3_client=None,
) -> FilingDocument:
    """Download a filing's primary document and persist it to the object store.

    Idempotent: if the filing already exists in the object store, the stored
    text is returned without making another HTTP request to EDGAR.

    Args:
        filing_metadata: Metadata describing the filing to download.
        client: Optional httpx.Client for test injection.
        s3_client: Optional boto3 S3 client for test injection.

    Returns:
        FilingDocument with the full text and object-store path.
    """
    settings = get_settings()
    if s3_client is None:
        import boto3

        s3_client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
        )

    key = _object_store_key(filing_metadata.cik, filing_metadata.accession_number)

    # --- Idempotency check ---
    if _object_exists(s3_client, settings.s3_bucket, key):
        logger.debug(
            "Filing %s already in object store, returning cached copy",
            filing_metadata.accession_number,
        )
        obj = s3_client.get_object(Bucket=settings.s3_bucket, Key=key)
        text = obj["Body"].read().decode("utf-8", errors="replace")
        return FilingDocument(metadata=filing_metadata, text=text, object_store_path=key)

    # --- Download ---
    url = _filing_url(
        filing_metadata.cik,
        filing_metadata.accession_number,
        filing_metadata.primary_document,
    )
    try:
        resp = _get(url, client=client)
        text = resp.text
    except httpx.HTTPError as exc:
        logger.error(
            "Failed to download filing %s: %s", filing_metadata.accession_number, exc
        )
        raise

    # --- Store ---
    s3_client.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType="text/plain",
    )
    logger.info(
        "Stored filing %s → %s", filing_metadata.accession_number, key
    )

    return FilingDocument(metadata=filing_metadata, text=text, object_store_path=key)


def ingest_all_10k(
    since_date: date,
    entity_ids: list[UUID] | None = None,
    *,
    db,
    client: httpx.Client | None = None,
    s3_client=None,
    filing_types: list[str] | None = None,
) -> IngestResult:
    """Ingest 10-K (and optionally other) filings for all tracked entities.

    Designed to run as an overnight Celery task.  Iterates over all Entity
    rows with a non-null ticker, resolves each ticker to a CIK, fetches
    metadata for matching filings, downloads each one, and writes an Event
    row.  Already-ingested filings are skipped (idempotent).

    Args:
        since_date: Only process filings filed on or after this date.
        entity_ids: Limit to these entity UUIDs.  None = all entities.
        db: SQLAlchemy Session.
        client: Optional httpx.Client for test injection.
        s3_client: Optional S3 client for test injection.
        filing_types: Form types to ingest.  Defaults to ["10-K"].

    Returns:
        IngestResult with counts of ingested / skipped / errored filings.
    """
    from sqlalchemy import select

    from cam.db.models import Entity

    if filing_types is None:
        filing_types = ["10-K"]

    result = IngestResult()

    stmt = select(Entity).where(Entity.ticker.isnot(None))
    if entity_ids:
        stmt = stmt.where(Entity.id.in_(entity_ids))

    entities = db.execute(stmt).scalars().all()

    for entity in entities:
        if not entity.ticker:
            continue

        # Resolve ticker → CIK
        time.sleep(REQUEST_DELAY)
        cik = get_cik_for_ticker(entity.ticker, client=client)
        if not cik:
            logger.warning(
                "Could not resolve CIK for ticker %s (entity %s)",
                entity.ticker,
                entity.id,
            )
            result.errors += 1
            result.error_details.append(f"No CIK for ticker {entity.ticker}")
            continue

        # Fetch filing metadata
        try:
            time.sleep(REQUEST_DELAY)
            filings = fetch_company_filings(cik, filing_types, since_date, client=client)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to fetch filings for CIK %s: %s", cik, exc)
            result.errors += 1
            result.error_details.append(f"Fetch error for CIK {cik}: {exc}")
            continue

        result.total += len(filings)

        for filing in filings:
            filing.entity_id = entity.id

            try:
                # Idempotency: skip if already recorded in the DB
                if _filing_in_db(db, filing.accession_number):
                    result.skipped += 1
                    continue

                time.sleep(REQUEST_DELAY)
                doc = download_filing(filing, client=client, s3_client=s3_client)
                _upsert_filing_event(db, filing, doc.object_store_path, entity.id)
                result.ingested += 1

            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Failed to ingest filing %s: %s", filing.accession_number, exc
                )
                result.errors += 1
                result.error_details.append(
                    f"Filing {filing.accession_number}: {exc}"
                )

    return result


# ---------------------------------------------------------------------------
# Internal helpers (not part of the public interface)
# ---------------------------------------------------------------------------


def _object_exists(s3_client, bucket: str, key: str) -> bool:
    """Return True if *key* exists in *bucket*."""
    from botocore.exceptions import ClientError

    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def _filing_in_db(db, accession_number: str) -> bool:
    """Return True if an Event for this accession number already exists."""
    from sqlalchemy import select

    from cam.db.models import Event

    acc_clean = _accession_no_dashes(accession_number)
    stmt = select(Event.id).where(
        Event.source == "sec_edgar",
        Event.raw_url.contains(acc_clean),
    )
    return db.execute(stmt).first() is not None


def _upsert_filing_event(
    db,
    filing: FilingMetadata,
    object_store_path: str,
    entity_id: UUID,
) -> None:
    """Insert an Event row for the filing (idempotent via raw_url uniqueness)."""
    from cam.db.models import Event

    raw_url = _filing_url(filing.cik, filing.accession_number, filing.primary_document)

    event = Event(
        entity_id=entity_id,
        source="sec_edgar",
        event_type="filing",
        event_date=filing.filed_date,
        penalty_usd=None,
        description=f"{filing.filing_type} ({filing.accession_number})",
        raw_url=raw_url,
        raw_json={
            "cik": filing.cik,
            "accession_number": filing.accession_number,
            "filing_type": filing.filing_type,
            "primary_document": filing.primary_document,
            "object_store_path": object_store_path,
        },
    )
    db.add(event)
    db.commit()
