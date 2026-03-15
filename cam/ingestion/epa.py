"""
M4 — EPA Ingestion

Ingests EPA Toxic Release Inventory (TRI) annual release data and ECHO
(Enforcement and Compliance History Online) enforcement action records.

TRI self-reports vs. ECHO enforcement divergence is a key signal: when a
facility reports low releases to TRI but accumulates enforcement actions, it
suggests under-reporting or systemic non-compliance.

Data sources:
  TRI bulk CSV: https://www.epa.gov/toxics-release-inventory-tri-program/tri-data-and-tools
  ECHO API:     https://echo.epa.gov/api/swagger/ui
"""

from __future__ import annotations

import csv
import logging
import math
import tempfile
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
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

from cam.db.models import Event
from cam.entity.resolver import bulk_resolve

logger = logging.getLogger(__name__)

# Base URLs
_TRI_BULK_BASE = "https://www.epa.gov/sites/default/files/tri"
_ECHO_CASE_URL = "https://echo.epa.gov/api/case/download"

# TRI column names (the real TRI CSV uses fixed positional columns)
_TRI_FACILITY_NAME_COL = "FACILITY_NAME"
_TRI_FRS_ID_COL = "FRS_ID"
_TRI_YEAR_COL = "YEAR"
_TRI_CHEMICAL_COL = "CHEMICAL"
_TRI_TOTAL_RELEASES_COL = "TOTAL_RELEASES"
_TRI_NAICS_COL = "NAICS_CODE"
_TRI_STATE_COL = "ST"
_TRI_CITY_COL = "CITY"
_TRI_PARENT_CO_COL = "PARENT_CO_NAME"
_TRI_UNIT_COL = "UNIT_OF_MEASURE"

# Unit conversion: TRI reports in Pounds or Grams; normalise everything to Pounds.
_GRAMS_TO_LBS = Decimal("0.00220462")

# ECHO response key paths
_ECHO_CASE_LIST_KEY = "CaseList"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class IngestResult:
    """Summary of a bulk ingestion run."""

    total: int = 0
    ingested: int = 0
    skipped: int = 0
    errors: int = 0
    error_details: list[str] = field(default_factory=list)


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
    url: str,
    params: dict[str, Any] | None = None,
    *,
    client: httpx.Client | None = None,
) -> httpx.Response:
    @_make_retry_decorator()
    def _request() -> httpx.Response:
        if client is not None:
            resp = client.get(url, params=params, timeout=120)
        else:
            resp = httpx.get(url, params=params, timeout=120)
        resp.raise_for_status()
        return resp

    return _request()


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_decimal(value: str | None) -> Decimal | None:
    """Parse a numeric string, returning None for empty/invalid values."""
    cleaned = (value or "").strip().replace(",", "")
    if not cleaned:
        return None
    try:
        d = Decimal(cleaned)
        return d if d >= 0 else None
    except InvalidOperation:
        return None


def _parse_date(value: str | None) -> date | None:
    """Parse YYYY-MM-DD date strings as used by ECHO API."""
    value = (value or "").strip()
    if not value or value.lower() == "null":
        return None
    from datetime import datetime

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    logger.warning("Could not parse EPA date: %r", value)
    return None


def _normalize_to_lbs(value: Decimal | None, unit: str) -> Decimal | None:
    """Convert TRI release quantity to pounds for consistent comparison."""
    if value is None:
        return None
    if "gram" in unit.lower():
        return value * _GRAMS_TO_LBS
    return value  # already in pounds (or unknown unit — treat as pounds)


def _clean_facility_name(raw: str | None) -> str:
    """Strip trailing ' - LOCATION' suffixes from EPA facility names."""
    import re

    cleaned = (raw or "").strip()
    cleaned = re.sub(r"\s+-\s+[A-Z0-9 ]+$", "", cleaned)
    return cleaned.strip()


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _get_existing_keys(db: Session, source: str, key_field: str) -> set[str]:
    """Return all raw_json[key_field] values already persisted for source."""
    stmt = select(Event.raw_json[key_field].as_string()).where(Event.source == source)
    rows = db.execute(stmt).scalars().all()
    return {r for r in rows if r}


# ---------------------------------------------------------------------------
# TRI ingestion
# ---------------------------------------------------------------------------


def _download_tri_csv(year: int, *, client: httpx.Client | None = None) -> Path:
    """Download the TRI basic data file for a given year."""
    # Real TRI URL pattern: EPA publishes as tri_YYYY_us.zip / .csv
    url = f"{_TRI_BULK_BASE}/tri_{year}_us.csv"
    logger.info("Downloading TRI CSV for %d from %s", year, url)
    resp = _get(url, client=client)
    out_path = Path(tempfile.gettempdir()) / f"tri_{year}_us.csv"
    out_path.write_bytes(resp.content)
    return out_path


def ingest_tri(
    year: int,
    *,
    db: Session,
    csv_path: Path | None = None,
    client: httpx.Client | None = None,
) -> IngestResult:
    """Ingest TRI annual release data. Store as event_type='tri_release'.

    Parameters
    ----------
    year:     Reporting year to ingest.
    db:       SQLAlchemy session.
    csv_path: Optional pre-downloaded CSV path; downloads if not provided.
    client:   Optional httpx.Client for testing.
    """
    result = IngestResult()

    if csv_path is None:
        csv_path = _download_tri_csv(year, client=client)

    # Read CSV
    rows: list[dict[str, str]] = []
    try:
        with csv_path.open(newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rows.append(dict(row))
    except OSError as exc:
        result.errors += 1
        result.error_details.append(f"Could not open TRI CSV: {exc}")
        return result

    result.total = len(rows)

    # Idempotency: each TRI row is keyed by FRS_ID + CHEMICAL + YEAR
    existing_keys = _get_existing_keys(db, "epa_tri", "tri_key")

    to_process = []
    for row in rows:
        yr = (row.get(_TRI_YEAR_COL) or "").strip()
        if yr != str(year):
            result.skipped += 1
            continue
        frs = (row.get(_TRI_FRS_ID_COL) or "").strip()
        chem = (row.get(_TRI_CHEMICAL_COL) or "").strip()
        tri_key = f"{frs}|{chem}|{yr}"
        if tri_key and tri_key in existing_keys:
            result.skipped += 1
        else:
            row["_tri_key"] = tri_key
            to_process.append(row)

    if not to_process:
        return result

    # Prefer parent company name for entity resolution; fall back to facility name
    resolve_records = [
        {"name": _clean_facility_name(r.get(_TRI_PARENT_CO_COL) or r.get(_TRI_FACILITY_NAME_COL))}
        for r in to_process
    ]
    resolved = bulk_resolve(resolve_records, "epa_tri", db, commit=False)

    for row, res in zip(to_process, resolved):
        tri_key = row.get("_tri_key", "")
        try:
            total_releases = _parse_decimal(row.get(_TRI_TOTAL_RELEASES_COL))
            unit = (row.get(_TRI_UNIT_COL) or "Pounds").strip()
            total_releases_lbs = _normalize_to_lbs(total_releases, unit)
            event_year = int((row.get(_TRI_YEAR_COL) or "0").strip())
            event_date = date(event_year, 12, 31) if event_year > 0 else None

            raw = {k: v for k, v in row.items() if not k.startswith("_")}
            raw["tri_key"] = tri_key
            if total_releases_lbs is not None:
                raw["total_releases_lbs"] = str(total_releases_lbs)

            event = Event(
                entity_id=res.entity_id,
                source="epa_tri",
                event_type="tri_release",
                event_date=event_date,
                penalty_usd=None,  # TRI has no penalty; releases are self-reported
                description=(
                    f"{row.get(_TRI_CHEMICAL_COL, '').strip()} release: {total_releases} {unit}"
                    if total_releases is not None
                    else row.get(_TRI_CHEMICAL_COL, "").strip()
                ),
                raw_json=raw,
            )
            db.add(event)
            result.ingested += 1
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to ingest TRI record %s: %s", tri_key, exc)
            result.errors += 1
            result.error_details.append(f"tri_key={tri_key}: {exc}")

    db.commit()
    return result


# ---------------------------------------------------------------------------
# ECHO violation ingestion
# ---------------------------------------------------------------------------


def _fetch_echo_cases(
    since_date: date,
    *,
    client: httpx.Client | None = None,
) -> list[dict]:
    """Fetch enforcement cases from the ECHO API since since_date."""
    params = {
        "p_date_since": since_date.strftime("%m/%d/%Y"),
        "output": "JSON",
    }
    try:
        resp = _get(_ECHO_CASE_URL, params=params, client=client)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            logger.warning(
                "ECHO API returned 404 for %s — endpoint may be unavailable, returning empty",
                _ECHO_CASE_URL,
            )
            return []
        raise
    data = resp.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        results = data.get("Results", data)
        if isinstance(results, dict):
            return results.get(_ECHO_CASE_LIST_KEY, [])
        return []
    logger.warning("Unexpected ECHO API response type %s", type(data).__name__)
    return []


def ingest_echo_violations(
    since_date: date,
    *,
    db: Session,
    client: httpx.Client | None = None,
    cases: list[dict] | None = None,
) -> IngestResult:
    """Ingest EPA enforcement actions from ECHO.

    Parameters
    ----------
    since_date: Only fetch/process cases with action_date >= this date.
    db:         SQLAlchemy session.
    client:     Optional httpx.Client for testing.
    cases:      Optional pre-fetched list of case dicts (for testing).
    """
    result = IngestResult()

    if cases is None:
        cases = _fetch_echo_cases(since_date, client=client)

    result.total = len(cases)

    if not cases:
        return result

    # Idempotency by activity_id
    existing_ids = _get_existing_keys(db, "epa_echo", "activity_id")

    to_process = [
        c
        for c in cases
        if (c.get("activity_id") or "") not in existing_ids
        and (d := _parse_date(c.get("action_date"))) is not None
        and d >= since_date
    ]
    result.skipped = result.total - len(to_process)

    if not to_process:
        return result

    resolve_records = [{"name": _clean_facility_name(c.get("facility_name"))} for c in to_process]
    resolved = bulk_resolve(resolve_records, "epa_echo", db, commit=False)

    for case, res in zip(to_process, resolved):
        activity_id = case.get("activity_id", "")
        try:
            penalty = _parse_decimal(case.get("penalty_assessed"))
            event_date = _parse_date(case.get("action_date"))

            event = Event(
                entity_id=res.entity_id,
                source="epa_echo",
                event_type="violation",
                event_date=event_date,
                penalty_usd=penalty,
                description=case.get("description"),
                raw_json={**case, "activity_id": activity_id},
            )
            db.add(event)
            result.ingested += 1
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to ingest ECHO case %s: %s", activity_id, exc)
            result.errors += 1
            result.error_details.append(f"activity_id={activity_id}: {exc}")

    db.commit()
    return result


# ---------------------------------------------------------------------------
# TRI / enforcement divergence scoring
# ---------------------------------------------------------------------------


def compute_tri_enforcement_divergence(
    entity_id: UUID,
    year: int,
    *,
    db: Session,
) -> float | None:
    """Compare self-reported TRI releases to ECHO enforcement actions for the
    same entity and year.

    A higher score indicates a more concerning discrepancy — many enforcement
    actions relative to reported releases, suggesting under-reporting.

    Returns None when insufficient data exists (no TRI releases or no ECHO
    actions found for the entity/year combination).

    Algorithm
    ---------
    divergence = log1p(echo_penalty_total / (tri_total_releases + 1))

    This is deliberately simple and monotonic: as penalty grows relative to
    reported releases the score rises unboundedly; log dampens extreme values.
    """
    # --- TRI releases for this entity/year ---
    tri_stmt = select(Event).where(
        Event.entity_id == entity_id,
        Event.source == "epa_tri",
        Event.event_type == "tri_release",
        Event.event_date >= date(year, 1, 1),
        Event.event_date <= date(year, 12, 31),
    )
    tri_events = db.execute(tri_stmt).scalars().all()

    if not tri_events:
        return None

    # Sum up total releases across all chemicals, normalised to pounds.
    # Prefer the pre-normalised total_releases_lbs stored at ingest time;
    # fall back to TOTAL_RELEASES for events seeded without normalisation.
    tri_total: float = 0.0
    for ev in tri_events:
        raw = ev.raw_json or {}
        val = _parse_decimal(raw.get("total_releases_lbs") or raw.get(_TRI_TOTAL_RELEASES_COL))
        if val is not None:
            tri_total += float(val)

    # --- ECHO enforcement for this entity/year ---
    echo_stmt = select(Event).where(
        Event.entity_id == entity_id,
        Event.source == "epa_echo",
        Event.event_type == "violation",
        Event.event_date >= date(year, 1, 1),
        Event.event_date <= date(year, 12, 31),
    )
    echo_events = db.execute(echo_stmt).scalars().all()

    if not echo_events:
        return None

    echo_penalty_total: float = sum(
        float(ev.penalty_usd) for ev in echo_events if ev.penalty_usd is not None
    )

    # Divergence score: log1p(penalty / (releases + 1))
    return math.log1p(echo_penalty_total / (tri_total + 1))
