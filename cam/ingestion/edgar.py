"""
M2 — EDGAR Ingestion

Fetches SEC EDGAR filings for public companies and stores them in the events
table.  Priority filing types: 10-K (annual), DEF 14A (proxy), 8-K (material
events), S-4/424B (debt offerings).

Rate limit: 10 requests/second per EDGAR fair-access policy.
"""

from __future__ import annotations

import io
import logging
import time
import uuid
import zipfile
from dataclasses import dataclass
from datetime import date
from html.parser import HTMLParser
from typing import Any
from uuid import UUID

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from cam.config import get_settings
from cam.ingestion.base import IngestResult
from cam.ingestion.circuit_breaker import get_breaker
from cam.ingestion.dlq import ERROR_DB_WRITE, record_failure

logger = logging.getLogger(__name__)

# EDGAR base URLs
_EDGAR_SUBMISSIONS_BASE = "https://data.sec.gov/submissions"
_EDGAR_COMPANY_TICKERS = "https://www.sec.gov/files/company_tickers.json"
_EDGAR_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"
_EDGAR_XBRL_BASE = "https://data.sec.gov/api/xbrl/companyfacts"
# _EDGAR_FULL_INDEX_BASE is now configured via cam.config Settings
# (edgar_full_index_base) to allow environment-specific overrides.

# Key US-GAAP concepts to extract from the companyfacts endpoint.
_XBRL_KEY_CONCEPTS = ("Revenues", "Assets", "NetIncomeLoss", "StockholdersEquity")

# Seconds between requests to stay within EDGAR's 10 req/s limit.
# Tests override this via monkeypatch on cam.ingestion.edgar.REQUEST_DELAY.
REQUEST_DELAY: float = 0.1

# Maximum number of quarterly index files to scan when pre-filtering entities.
# 4 quarters ≈ 1 year; covers the typical daily/weekly ingest window and bounds
# the HTTP call count at O(4) regardless of how far back since_date reaches.
_MAX_INDEX_QUARTERS: int = 4


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
    """Downloaded filing text (plain text) with its object-store path."""

    metadata: FilingMetadata
    text: str
    object_store_path: str  # e.g. "edgar/0000320193/0000320193-24-000006/full.txt"


# ---------------------------------------------------------------------------
# Retry helpers
# ---------------------------------------------------------------------------


def _is_retriable_error(exc: BaseException) -> bool:
    """Return True if *exc* should be retried by tenacity.

    Retriable conditions:
    - Network / timeout errors (transient connectivity issues)
    - HTTP 429 (EDGAR rate limit exceeded)
    """
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429:
        return True
    return False


def _make_retry_decorator():
    """Return a tenacity retry decorator for transient EDGAR errors."""
    return retry(
        retry=retry_if_exception(_is_retriable_error),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        stop=stop_after_attempt(5),
        reraise=True,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _headers() -> dict[str, str]:
    return {"User-Agent": get_settings().edgar_user_agent}


def _get(
    url: str,
    params: dict[str, Any] | None = None,
    *,
    client: httpx.Client | None = None,
) -> httpx.Response:
    """GET with automatic retry on transient errors and 429 back-off."""
    breaker = get_breaker("edgar")

    @_make_retry_decorator()
    def _request() -> httpx.Response:
        if client is not None:
            resp = client.get(url, params=params, headers=_headers(), timeout=30)
        else:
            resp = httpx.get(url, params=params, headers=_headers(), timeout=30)

        if resp.status_code == 429:
            raise httpx.HTTPStatusError(
                "EDGAR rate limited (429)",
                request=resp.request,
                response=resp,
            )

        resp.raise_for_status()
        return resp

    return breaker.call(_request)


def _accession_no_dashes(accession_number: str) -> str:
    return accession_number.replace("-", "")


def _filing_url(cik: str, accession_number: str, primary_document: str) -> str:
    """Build the SEC Archives URL for a filing's primary document."""
    cik_int = str(int(cik))  # drop leading zeros for the Archives path
    acc_clean = _accession_no_dashes(accession_number)
    return f"{_EDGAR_ARCHIVES_BASE}/{cik_int}/{acc_clean}/{primary_document}"


def _object_store_key(cik: str, accession_number: str) -> str:
    """Canonical S3/MinIO key for a filing document.

    Keeps dashes in the accession number to match the PLAN.md convention:
        edgar/{cik}/{accession_number}/full.txt
    """
    return f"edgar/{cik}/{accession_number}/full.txt"


class _HTMLStripper(HTMLParser):
    """Minimal HTML → plain-text converter using the stdlib html.parser."""

    _SKIP_TAGS = frozenset({"script", "style", "head"})

    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []
        self._skip_depth: int = 0

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag.lower() in self._SKIP_TAGS:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in self._SKIP_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)

    def handle_data(self, data: str) -> None:
        if self._skip_depth == 0:
            stripped = data.strip()
            if stripped:
                self._parts.append(stripped)

    def get_text(self) -> str:
        return "\n".join(self._parts)


def _extract_text(content: str) -> str:
    """Return plain text from *content*, stripping HTML tags when present."""
    lowered = content.lstrip()[:500].lower()
    if "<html" in lowered or "<!doctype" in lowered:
        stripper = _HTMLStripper()
        stripper.feed(content)
        return stripper.get_text()
    return content


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


def fetch_xbrl_facts(
    cik: str,
    *,
    client: httpx.Client | None = None,
) -> dict | None:
    """Fetch key XBRL financial facts for a company from the companyfacts API.

    Returns a dict mapping US-GAAP concept names to their most recent annual
    value, or None on failure.  The result is suitable for storage in
    ``events.raw_json["xbrl_facts"]``.

    Args:
        cik: Company CIK (will be zero-padded to 10 digits).
        client: Optional httpx.Client for test injection.
    """
    padded = cik.zfill(10)
    url = f"{_EDGAR_XBRL_BASE}/CIK{padded}.json"

    try:
        resp = _get(url, client=client)
        data = resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("Failed to fetch XBRL facts for CIK %s: %s", cik, exc)
        return None

    us_gaap = data.get("facts", {}).get("us-gaap", {})
    summary: dict[str, dict] = {}

    for concept in _XBRL_KEY_CONCEPTS:
        if concept not in us_gaap:
            continue
        usd_values = us_gaap[concept].get("units", {}).get("USD", [])
        annual = [v for v in usd_values if v.get("form") in ("10-K", "10-K/A")]
        if annual:
            latest = max(annual, key=lambda v: v.get("end", ""))
            summary[concept] = {
                "value": latest.get("val"),
                "period_end": latest.get("end"),
            }

    return summary or None


def download_filing(
    filing_metadata: FilingMetadata,
    *,
    client: httpx.Client | None = None,
    s3_client=None,
) -> FilingDocument:
    """Download a filing's primary document and persist it to the object store.

    HTML content is stripped to plain text before storage.  Idempotent: if the
    filing already exists in the object store, the stored text is returned
    without making another HTTP request to EDGAR.

    Args:
        filing_metadata: Metadata describing the filing to download.
        client: Optional httpx.Client for test injection.
        s3_client: Optional boto3 S3 client for test injection.

    Returns:
        FilingDocument with plain-text content and object-store path.
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
        text = _extract_text(resp.text)
    except httpx.HTTPError as exc:
        logger.error("Failed to download filing %s: %s", filing_metadata.accession_number, exc)
        raise

    # --- Store ---
    s3_client.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType="text/plain",
    )
    logger.info("Stored filing %s → %s", filing_metadata.accession_number, key)

    return FilingDocument(metadata=filing_metadata, text=text, object_store_path=key)


def _quarters_for_since(since_date: date) -> list[tuple[int, int]]:
    """Return ``(year, quarter)`` pairs from *since_date* through today.

    Capped at ``Settings.edgar_max_index_quarters`` quarters (default 4) so
    the index scan always costs a bounded number of HTTP calls, even when
    *since_date* is years in the past.

    When *since_date* falls before the scan window a warning is logged so
    that operators know the backfill may be incomplete.  Increase
    ``EDGAR_MAX_INDEX_QUARTERS`` (env var) to widen the scan window.

    Examples
    --------
    With today = 2026-03-15 and edgar_max_index_quarters = 4::

        _quarters_for_since(date(2026, 1, 1))  → [(2026, 1)]
        _quarters_for_since(date(2025, 10, 1)) → [(2025, 4), (2026, 1)]
        _quarters_for_since(date(2020, 1, 1))  → [(2025, 2), (2025, 3), (2025, 4), (2026, 1)]
    """
    max_quarters = get_settings().edgar_max_index_quarters
    today = date.today()

    def _qi(d: date) -> int:
        """Comparable integer: year*4 + zero-based quarter (0-3)."""
        return d.year * 4 + (d.month - 1) // 3

    end_qi = _qi(today)
    since_qi = _qi(since_date)
    start_qi = max(since_qi, end_qi - max_quarters + 1)

    if since_qi < start_qi:
        logger.warning(
            "_quarters_for_since: since_date %s is older than the %d-quarter scan "
            "window; only the most-recent %d quarters will be checked. "
            "Set EDGAR_MAX_INDEX_QUARTERS to a larger value for complete backfills.",
            since_date,
            max_quarters,
            max_quarters,
        )

    quarters: list[tuple[int, int]] = []
    for qi in range(start_qi, end_qi + 1):
        year, q0 = divmod(qi, 4)
        quarters.append((year, q0 + 1))
    return quarters


def fetch_filings_from_index(
    since_date: date,
    filing_types: list[str],
    *,
    client: httpx.Client | None = None,
) -> set[str]:
    """Download quarterly master.zip index files; return zero-padded CIKs with new filings.

    Returns a set of 10-digit zero-padded CIK strings for companies that have
    at least one filing of *filing_types* on or after *since_date*, based on
    the EDGAR full-index quarterly files.

    Used as a pre-filter in ``ingest_all_10k``: entities whose CIK is **not**
    in the returned set have no new filings and can be skipped entirely,
    reducing HTTP calls from O(N_entities × 3) to O(Q_quarters + N_new_filers × 2).

    The quarterly index URL format::

        https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{q}/master.zip

    The ``master.zip`` contains ``master.idx``, a pipe-delimited file::

        CIK|Company Name|Form Type|Date Filed|Filename
        --------------------------------------------------------------------------------
        320193|Apple Inc.|10-K|2024-02-02|edgar/data/320193/0000320193-24-000006.txt
    """
    index_base = get_settings().edgar_full_index_base
    filing_types_set = set(filing_types)
    matching_ciks: set[str] = set()

    # Track outcomes so we can detect when every quarter failed.
    hard_failure_count: int = 0  # network errors and bad zips
    success_count: int = 0  # quarters that responded and were parseable

    for year, quarter in _quarters_for_since(since_date):
        url = f"{index_base}/{year}/QTR{quarter}/master.zip"
        logger.info("Fetching EDGAR quarterly index %d Q%d", year, quarter)
        try:
            time.sleep(REQUEST_DELAY)
            resp = _get(url, client=client)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                logger.warning(
                    "EDGAR quarterly index %d Q%d not yet published (404), skipping",
                    year,
                    quarter,
                )
                continue  # 404 is not a hard failure — quarter just not published yet
            raise
        except httpx.HTTPError as exc:
            logger.warning("Failed to fetch EDGAR quarterly index %d Q%d: %s", year, quarter, exc)
            hard_failure_count += 1
            continue

        try:
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                with zf.open("master.idx") as fh:
                    text_fh = io.TextIOWrapper(fh, encoding="utf-8", errors="replace")
                    # Skip preamble/header lines up to and including the "---" separator
                    for line in text_fh:
                        if line.startswith("---"):
                            break
                    # Parse pipe-delimited data rows
                    for line in text_fh:
                        parts = line.rstrip("\n").split("|")
                        if len(parts) < 4:
                            continue
                        cik_raw, _company, form_type, filed_str = (
                            parts[0],
                            parts[1],
                            parts[2],
                            parts[3],
                        )
                        if form_type not in filing_types_set:
                            continue
                        try:
                            filed = date.fromisoformat(filed_str)
                        except (ValueError, TypeError):
                            continue
                        if filed < since_date:
                            continue
                        matching_ciks.add(cik_raw.strip().zfill(10))
            success_count += 1
        except (zipfile.BadZipFile, KeyError) as exc:
            logger.warning("Could not parse EDGAR quarterly index %d Q%d: %s", year, quarter, exc)
            hard_failure_count += 1
            continue

    # If every quarter we attempted to fetch produced a hard failure (network
    # error or corrupt zip), raise so the caller's fallback logic can activate
    # instead of silently treating the empty set as "no new filings".
    if hard_failure_count > 0 and success_count == 0:
        raise RuntimeError(
            f"All {hard_failure_count} EDGAR quarterly index fetches failed "
            "(network errors or corrupt archives); falling back to per-entity API calls"
        )

    logger.info(
        "EDGAR quarterly index scan complete: %d CIKs with matching filings",
        len(matching_ciks),
    )
    return matching_ciks


def ingest_all_10k(
    since_date: date,
    entity_ids: list[UUID] | None = None,
    *,
    db,
    client: httpx.Client | None = None,
    s3_client=None,
    filing_types: list[str] | None = None,
    fetch_xbrl: bool = True,
    run_id: uuid.UUID | None = None,
) -> IngestResult:
    """Ingest 10-K (and optionally other) filings for all tracked entities.

    Designed to run as an overnight Celery task.  Iterates over all Entity
    rows with a non-null ticker, resolves each ticker to a CIK, optionally
    fetches XBRL financial facts, downloads each matching filing, and writes
    an Event row.  Already-ingested filings are skipped (idempotent).

    Performance optimisation (bulk approach):

    1. Fetch ``company_tickers.json`` **once** to build a full ticker→CIK map
       (O(1) HTTP call instead of one call per entity).
    2. Download quarterly ``master.zip`` index files to pre-filter which CIKs
       have new filings (O(_MAX_INDEX_QUARTERS) calls, typically 1–2 for daily
       runs).  Entities whose CIK is absent from the index are skipped without
       any further HTTP calls.
    3. For each entity *with* new filings: fetch XBRL facts (optional),
       ``fetch_company_filings`` for the primary_document name, then download.

    This reduces HTTP calls from O(N_entities × 3) to
    O(1 + Q_quarters + N_new_filers × 2–3).

    Args:
        since_date: Only process filings filed on or after this date.
        entity_ids: Limit to these entity UUIDs.  None = all entities.
        db: SQLAlchemy Session.
        client: Optional httpx.Client for test injection.
        s3_client: Optional S3 client for test injection.
        filing_types: Form types to ingest.  Defaults to ["10-K"].
        fetch_xbrl: When True (default), fetch XBRL financial facts per entity
            and store them in ``events.raw_json["xbrl_facts"]``.

    Returns:
        IngestResult with counts of ingested / skipped / errored filings.
    """
    from sqlalchemy import select

    from cam.db.models import Entity

    if filing_types is None:
        filing_types = ["10-K"]

    result = IngestResult(run_id=run_id or uuid.uuid4())

    stmt = select(Entity).where(Entity.ticker.isnot(None))
    if entity_ids:
        stmt = stmt.where(Entity.id.in_(entity_ids))

    entities = db.execute(stmt).scalars().all()

    if not entities:
        return result

    # ------------------------------------------------------------------
    # Step 1: Resolve all tickers → CIKs with a single HTTP call.
    # ------------------------------------------------------------------
    try:
        time.sleep(REQUEST_DELAY)
        tickers_resp = _get(_EDGAR_COMPANY_TICKERS, client=client)
        tickers_data: dict = tickers_resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.error("Failed to fetch company tickers: %s", exc)
        tickers_data = {}

    ticker_to_cik: dict[str, str] = {
        str(entry.get("ticker") or "").upper(): str(entry["cik_str"]).zfill(10)
        for entry in tickers_data.values()
        if entry.get("ticker") and entry.get("cik_str")
    }

    # Build (entity, cik) pairs — no HTTP.
    entity_cik_pairs: list[tuple] = []
    for entity in entities:
        if not entity.ticker:
            continue
        cik = ticker_to_cik.get(entity.ticker.upper())
        if not cik:
            logger.warning(
                "Could not resolve CIK for ticker %s (entity %s)",
                entity.ticker,
                entity.id,
            )
            result.errors += 1
            result.error_details.append(f"No CIK for ticker {entity.ticker}")
            continue
        entity_cik_pairs.append((entity, cik))

    if not entity_cik_pairs:
        return result

    # ------------------------------------------------------------------
    # Step 2: Pre-filter via quarterly index — O(_MAX_INDEX_QUARTERS) calls.
    # Entities whose CIK is absent have no new filings; skip them entirely.
    # ------------------------------------------------------------------
    try:
        index_ciks = fetch_filings_from_index(since_date, filing_types, client=client)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Quarterly index fetch failed (%s) — falling back to per-entity API calls",
            exc,
        )
        # Fallback: assume every entity may have new filings.
        index_ciks = {cik for _, cik in entity_cik_pairs}

    # ------------------------------------------------------------------
    # Step 3: For each entity in the index, fetch metadata + download.
    # ------------------------------------------------------------------
    for entity, cik in entity_cik_pairs:
        if cik not in index_ciks:
            logger.debug(
                "Skipping entity %s (ticker %s, CIK %s): not in quarterly index",
                entity.id,
                entity.ticker,
                cik,
            )
            continue

        # Optionally fetch XBRL financial facts once per entity/CIK
        xbrl_facts: dict | None = None
        if fetch_xbrl:
            time.sleep(REQUEST_DELAY)
            xbrl_facts = fetch_xbrl_facts(cik, client=client)

        # Fetch filing metadata (provides primary_document name)
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

                with db.begin_nested():  # SAVEPOINT — isolates this filing's write
                    _upsert_filing_event(
                        db, filing, doc.object_store_path, entity.id, xbrl_facts=xbrl_facts
                    )
                result.ingested += 1

            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to ingest filing %s: %s", filing.accession_number, exc)
                failure = record_failure(
                    db,
                    source="edgar",
                    run_id=result.run_id,
                    raw_record={
                        "accession_number": filing.accession_number,
                        "cik": filing.cik,
                        "filing_type": filing.filing_type,
                        "filed_date": str(filing.filed_date) if filing.filed_date else None,
                    },
                    error_type=ERROR_DB_WRITE,
                    exc=exc,
                    raw_key=filing.accession_number,
                )
                if failure is not None:
                    result.dlq_ids.append(failure.id)
                result.errors += 1
                result.error_details.append(f"Filing {filing.accession_number}: {exc}")

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
    """Return True if an Event for this accession number already exists.

    Uses a JSON field query on ``raw_json["accession_number"]`` for an exact,
    unambiguous match that works on both SQLite and PostgreSQL.
    """
    from sqlalchemy import select

    from cam.db.models import Event

    stmt = select(Event.id).where(
        Event.source == "sec_edgar",
        Event.raw_json["accession_number"].as_string() == accession_number,
    )
    return db.execute(stmt).first() is not None


def _upsert_filing_event(
    db,
    filing: FilingMetadata,
    object_store_path: str,
    entity_id: UUID,
    *,
    xbrl_facts: dict | None = None,
) -> None:
    """Insert an Event row for the filing."""
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
            "xbrl_facts": xbrl_facts,
        },
    )
    db.add(event)
    db.commit()
