"""
M5 — CFPB Ingestion

Ingests CFPB consumer complaint database records and provides complaint-rate
normalisation and spike-detection analytics.

Consumer complaint velocity is a leading indicator of consumer harm before
formal regulatory action.  Raw complaint counts are stored per-event; analytics
functions normalise against total assets from EDGAR financial data (soft
dependency on M2).

Data sources:
  Complaint API: https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
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

from cam.db.models import Event
from cam.entity.resolver import bulk_resolve

logger = logging.getLogger(__name__)

_CFPB_API_URL = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
_PAGE_SIZE = 100


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
            resp = client.get(url, params=params, timeout=60)
        else:
            resp = httpx.get(url, params=params, timeout=60)
        resp.raise_for_status()
        return resp

    return _request()


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_date(value: str | None) -> date | None:
    """Parse YYYY-MM-DD complaint date strings."""
    from datetime import datetime

    value = (value or "").strip()
    if not value:
        return None
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
# API fetch helpers
# ---------------------------------------------------------------------------


def _fetch_complaints_page(
    since_date: date,
    from_offset: int = 0,
    *,
    client: httpx.Client | None = None,
) -> tuple[list[dict], int]:
    """Fetch one page of complaints from the CFPB API.

    Returns (hits_list, total_count).
    """
    params = {
        "date_received_min": since_date.strftime("%Y-%m-%d"),
        "size": _PAGE_SIZE,
        "from": from_offset,
    }
    resp = _get(_CFPB_API_URL, params=params, client=client)
    data = resp.json()
    if not isinstance(data, dict):
        logger.warning("Unexpected CFPB API response type %s", type(data).__name__)
        return [], 0
    hits_outer = data.get("hits", {})
    if not isinstance(hits_outer, dict):
        return [], 0
    hits = hits_outer.get("hits", [])
    total = hits_outer.get("total", {})
    total_count = total.get("value", 0) if isinstance(total, dict) else int(total or 0)
    return hits if isinstance(hits, list) else [], total_count


def _hits_to_complaints(hits: list[dict]) -> list[dict]:
    """Flatten CFPB API hits into normalised complaint dicts.

    Hits with a missing or blank ``_id`` are silently dropped; they cannot be
    tracked for idempotency and would be re-ingested on every run.
    """
    complaints = []
    for hit in hits:
        if not isinstance(hit, dict):
            continue
        complaint_id = str(hit.get("_id") or "").strip()
        if not complaint_id:
            logger.warning("Dropping CFPB hit with missing _id: %r", hit)
            continue
        source = hit.get("_source") or {}
        complaint = {
            "complaint_id": complaint_id,
            **{k: v for k, v in source.items()},
        }
        complaints.append(complaint)
    return complaints


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def ingest_complaints(
    since_date: date,
    *,
    db: Session,
    client: httpx.Client | None = None,
    complaints: list[dict] | None = None,
) -> IngestResult:
    """Ingest new complaints from the CFPB API.

    Parameters
    ----------
    since_date:  Only process complaints with date_received >= this date.
    db:          SQLAlchemy session.
    client:      Optional httpx.Client for testing.
    complaints:  Optional pre-fetched list of complaint dicts (for testing).
                 Each dict must have ``complaint_id`` and ``date_received`` keys.
    """
    result = IngestResult()

    if complaints is None:
        # Paginate through the full result set
        raw_complaints: list[dict] = []
        offset = 0
        while True:
            hits, total = _fetch_complaints_page(since_date, offset, client=client)
            raw_complaints.extend(_hits_to_complaints(hits))
            offset += _PAGE_SIZE
            if offset >= total or not hits:
                break
        complaints = raw_complaints

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
        try:
            event_date = _parse_date(complaint.get("date_received"))
            product = (complaint.get("product") or "").strip()
            issue = (complaint.get("issue") or "").strip()

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
            result.errors += 1
            result.error_details.append(f"complaint_id={complaint_id}: {exc}")

    db.commit()
    return result


def compute_complaint_rate(
    entity_id: UUID,
    period_months: int = 12,
    *,
    db: Session,
) -> ComplaintRate | None:
    """Return complaint rate per $1B total assets for the trailing period.

    Returns None if the entity has no financial data available for
    normalisation (requires EDGAR 10-K data from M2).

    Parameters
    ----------
    entity_id:     Entity to compute rate for.
    period_months: Trailing window in months (approximate: uses 30d/month).
    db:            SQLAlchemy session.
    """
    period_end = date.today()
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
) -> bool:
    """Return True if complaint rate has spiked in the recent half of the lookback.

    Compares the most recent ``lookback_months // 2`` months against the prior
    ``lookback_months // 2`` months.  Returns True if the recent count exceeds
    the prior count by more than ``threshold_pct`` percent.

    A prior count of zero with any recent complaints is always a spike.

    Parameters
    ----------
    entity_id:      Entity to check.
    lookback_months: Total window to analyse (split evenly into two halves).
    threshold_pct:   Percentage increase that constitutes a spike (default 50%).
    db:             SQLAlchemy session.
    """
    today = date.today()
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
