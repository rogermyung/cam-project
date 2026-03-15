"""
M12 — PE/Bankruptcy Correlator.

Measures whether PE-owned companies generate WARN Act filings and bankruptcy
events at statistically elevated rates compared to non-PE-owned peers in the
same 2-digit NAICS industry sector.

Key functions
-------------
compute_pe_warn_rate        Rate comparison for WARN Act notices.
compute_pe_bankruptcy_rate  Rate comparison for bankruptcy filings.
flag_pe_entity_for_monitoring  Mark an entity as PE-owned in the Signal table.
summarize_all_industries    Citable summary table across all NAICS sectors.

Statistical approach
--------------------
For each 2-digit NAICS code the module builds a 2×2 contingency table:

    ┌──────────────────────────┬────────────────────────────┐
    │  PE entities with ≥1     │  PE entities with 0 events │
    │  event in lookback       │  in lookback               │
    ├──────────────────────────┼────────────────────────────┤
    │  non-PE entities with ≥1 │  non-PE with 0 events      │
    └──────────────────────────┴────────────────────────────┘

A one-sided Fisher's exact test (alternative="greater") is used to compute
p-values.  The p-value is set to ``None`` when fewer than ``MIN_PE_SAMPLE``
PE entities are present in the sector, matching the PLAN.md acceptance
criterion that rate ratios are only reported for sectors with > 10 PE
entities.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from cam.db.models import Entity, Event, Signal

logger = logging.getLogger(__name__)

# Minimum number of PE-owned entities in a NAICS sector before a p-value is
# computed.  Set to 10 per PLAN.md acceptance criteria.
MIN_PE_SAMPLE: int = 10

# Event source / type constants
_WARN_SOURCE = "warn"
_WARN_EVENT_TYPE = "warn_notice"
_BANKRUPTCY_SOURCE = "pacer"
_BANKRUPTCY_EVENT_TYPE = "bankruptcy"

# ---------------------------------------------------------------------------
# Output dataclass
# ---------------------------------------------------------------------------


@dataclass
class PEComparison:
    """Rate-ratio comparison between PE-owned and non-PE-owned companies.

    All rates are expressed as *events per company per year* within the
    lookback window for the given 2-digit NAICS industry code.
    """

    industry: str  # 2-digit NAICS prefix
    pe_rate: float  # events / company / year, PE-owned
    non_pe_rate: float  # events / company / year, non-PE
    rate_ratio: float  # pe_rate / non_pe_rate; float('inf') if non_pe_rate == 0
    sample_sizes: dict[str, int] = field(default_factory=dict)
    # {"pe_count": N, "non_pe_count": M, "pe_events": X, "non_pe_events": Y}
    p_value: float | None = None  # Fisher's exact (one-sided); None if N <= MIN_PE_SAMPLE
    lookback_years: int = 5


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _since_date(lookback_years: int) -> date:
    """Return the earliest date covered by the lookback window."""
    return date.today() - timedelta(days=365 * lookback_years)


def _get_pe_entity_ids(db: Session) -> set[UUID]:
    """Return all entity IDs flagged as PE-owned via signal_type='pe_owned'."""
    stmt = select(Signal.entity_id).where(Signal.signal_type == "pe_owned")
    rows = db.execute(stmt).scalars().all()
    return {r for r in rows if r is not None}


def _get_entities_in_naics(db: Session, naics_2digit: str) -> list[Entity]:
    """Return entities whose naics_code starts with *naics_2digit*.

    Accepts either a bare 2-digit prefix (e.g. ``"52"``) or a full code
    (e.g. ``"5211"``); only the first two characters are used for matching.
    """
    prefix = naics_2digit[:2]
    stmt = select(Entity).where(Entity.naics_code.like(f"{prefix}%"))
    return list(db.execute(stmt).scalars().all())


def _entity_ids_with_events(
    db: Session,
    entity_ids: set[UUID],
    source: str,
    event_type: str,
    since: date,
) -> set[UUID]:
    """Return the subset of *entity_ids* that have ≥1 matching event since *since*.

    Uses an IN-clause over the entity_ids set; returns an empty set when the
    input is empty to avoid a zero-row IN clause.
    """
    if not entity_ids:
        return set()
    stmt = (
        select(Event.entity_id)
        .where(
            Event.entity_id.in_(entity_ids),
            Event.source == source,
            Event.event_type == event_type,
            Event.event_date.isnot(None),
            Event.event_date >= since,
        )
        .distinct()
    )
    rows = db.execute(stmt).scalars().all()
    return {r for r in rows if r is not None}


def _count_events(
    db: Session,
    entity_ids: set[UUID],
    source: str,
    event_type: str,
    since: date,
) -> int:
    """Count total events matching source+event_type since *since* for *entity_ids*."""
    if not entity_ids:
        return 0
    from sqlalchemy import func as sa_func

    stmt = select(sa_func.count()).where(
        Event.entity_id.in_(entity_ids),
        Event.source == source,
        Event.event_type == event_type,
        Event.event_date.isnot(None),
        Event.event_date >= since,
    )
    return db.execute(stmt).scalar_one() or 0


def _compute_p_value(
    pe_with: int,
    pe_without: int,
    non_pe_with: int,
    non_pe_without: int,
) -> float | None:
    """Fisher's exact test (one-sided: PE > non-PE).

    Returns None if all cells are zero (degenerate case).
    """
    try:
        from scipy.stats import fisher_exact
    except ImportError:  # pragma: no cover
        logger.error("scipy not installed; p-value computation unavailable")
        return None

    table = [[pe_with, pe_without], [non_pe_with, non_pe_without]]
    # Degenerate: all cells zero → no data, no test
    if pe_with + pe_without + non_pe_with + non_pe_without == 0:
        return None
    _, p = fisher_exact(table, alternative="greater")
    return float(p)


def _rate_ratio(pe_rate: float, non_pe_rate: float) -> float:
    """Compute rate ratio; returns float('inf') when non_pe_rate is zero."""
    if non_pe_rate == 0.0:
        return float("inf") if pe_rate > 0 else 1.0
    return pe_rate / non_pe_rate


def _compute_comparison(
    db: Session,
    naics_2digit: str,
    event_source: str,
    event_type: str,
    lookback_years: int,
) -> PEComparison:
    """Core rate-comparison computation shared by both public functions."""
    since = _since_date(lookback_years)
    pe_ids = _get_pe_entity_ids(db)
    all_entities = _get_entities_in_naics(db, naics_2digit)

    all_ids = {e.id for e in all_entities}
    sector_pe_ids = all_ids & pe_ids
    sector_non_pe_ids = all_ids - pe_ids

    pe_count = len(sector_pe_ids)
    non_pe_count = len(sector_non_pe_ids)

    pe_event_count = _count_events(db, sector_pe_ids, event_source, event_type, since)
    non_pe_event_count = _count_events(db, sector_non_pe_ids, event_source, event_type, since)

    # Rates: events / company / year
    pe_rate = pe_event_count / (pe_count * lookback_years) if pe_count > 0 else 0.0
    non_pe_rate = non_pe_event_count / (non_pe_count * lookback_years) if non_pe_count > 0 else 0.0

    # p-value only when sample is large enough (PLAN.md: "> 10 PE entities")
    p_value: float | None = None
    if pe_count > MIN_PE_SAMPLE:
        pe_ids_with = _entity_ids_with_events(db, sector_pe_ids, event_source, event_type, since)
        non_pe_ids_with = _entity_ids_with_events(
            db, sector_non_pe_ids, event_source, event_type, since
        )
        p_value = _compute_p_value(
            pe_with=len(pe_ids_with),
            pe_without=pe_count - len(pe_ids_with),
            non_pe_with=len(non_pe_ids_with),
            non_pe_without=non_pe_count - len(non_pe_ids_with),
        )

    return PEComparison(
        industry=naics_2digit[:2],
        pe_rate=round(pe_rate, 6),
        non_pe_rate=round(non_pe_rate, 6),
        rate_ratio=round(_rate_ratio(pe_rate, non_pe_rate), 4),
        sample_sizes={
            "pe_count": pe_count,
            "non_pe_count": non_pe_count,
            "pe_events": pe_event_count,
            "non_pe_events": non_pe_event_count,
        },
        p_value=p_value,
        lookback_years=lookback_years,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_pe_warn_rate(
    naics_2digit: str,
    lookback_years: int = 5,
    *,
    db: Session,
) -> PEComparison:
    """Compare WARN Act filing rates between PE-owned and non-PE-owned companies.

    Parameters
    ----------
    naics_2digit:   2-digit NAICS industry prefix (e.g. ``"62"`` for health care).
                    Only the first two characters are used.
    lookback_years: Number of years of history to include (default 5).
    db:             SQLAlchemy session.

    Returns
    -------
    :class:`PEComparison` with:
    - ``pe_rate`` / ``non_pe_rate`` — WARN events per company per year
    - ``rate_ratio`` — pe_rate / non_pe_rate (``inf`` when non-PE rate is zero)
    - ``p_value`` — Fisher's exact one-sided p-value; ``None`` if < MIN_PE_SAMPLE
    - ``sample_sizes`` — record counts for transparency
    """
    return _compute_comparison(db, naics_2digit, _WARN_SOURCE, _WARN_EVENT_TYPE, lookback_years)


def compute_pe_bankruptcy_rate(
    naics_2digit: str,
    lookback_years: int = 5,
    *,
    db: Session,
) -> PEComparison:
    """Compare bankruptcy filing rates between PE-owned and non-PE-owned companies.

    Bankruptcy events are expected to be ingested from PACER/CourtListener with
    ``source="pacer"`` and ``event_type="bankruptcy"``.  Returns a zero-rate
    comparison if no such events exist in the database.

    Parameters and return value are identical to :func:`compute_pe_warn_rate`.
    """
    return _compute_comparison(
        db, naics_2digit, _BANKRUPTCY_SOURCE, _BANKRUPTCY_EVENT_TYPE, lookback_years
    )


def flag_pe_entity_for_monitoring(
    entity_id: UUID,
    *,
    db: Session,
    evidence: str = "",
) -> None:
    """Mark an entity as PE-owned and initiate enhanced monitoring.

    Creates a ``Signal`` record with ``signal_type="pe_owned"`` if one does
    not already exist for this entity.  Idempotent: a second call for the same
    entity is a no-op (no duplicate signals created).

    Parameters
    ----------
    entity_id: UUID of the entity to flag.
    db:        SQLAlchemy session (caller must commit).
    evidence:  Optional free-text evidence string stored in the signal.
    """
    existing = db.scalars(
        select(Signal)
        .where(
            Signal.entity_id == entity_id,
            Signal.signal_type == "pe_owned",
        )
        .limit(1)
    ).first()

    if existing is not None:
        logger.debug("Entity %s already flagged as PE-owned; skipping.", entity_id)
        return

    sig = Signal(
        entity_id=entity_id,
        source="manual",
        signal_type="pe_owned",
        score=1.0,
        evidence=evidence or "Flagged for enhanced PE monitoring",
    )
    db.add(sig)
    db.flush()  # caller owns the commit boundary


def summarize_all_industries(
    event_type: str = "warn",
    lookback_years: int = 5,
    *,
    db: Session,
    min_pe_entities: int = MIN_PE_SAMPLE,
) -> list[dict[str, Any]]:
    """Compute and return a citable summary table across all NAICS sectors.

    Iterates over all distinct 2-digit NAICS prefixes present in the entities
    table and computes rate comparisons for each sector with at least
    *min_pe_entities* PE-owned companies.

    Parameters
    ----------
    event_type:       ``"warn"`` (default) or ``"bankruptcy"``.
    lookback_years:   Lookback window forwarded to the per-sector computations.
    db:               SQLAlchemy session.
    min_pe_entities:  Minimum PE entity count for a sector to be included.

    Returns
    -------
    List of dicts suitable for tabular display or CSV export, ordered by
    ``rate_ratio`` descending.  Each row contains all :class:`PEComparison`
    fields flattened alongside the ``industry_label`` field.
    """
    if event_type not in ("warn", "bankruptcy"):
        raise ValueError(f"event_type must be 'warn' or 'bankruptcy', got {event_type!r}")

    # Collect all distinct 2-digit NAICS prefixes in the entities table
    stmt = select(Entity.naics_code).where(Entity.naics_code.isnot(None)).distinct()
    codes = db.execute(stmt).scalars().all()
    prefixes: set[str] = {c[:2] for c in codes if c and len(c) >= 2}

    compute_fn = compute_pe_warn_rate if event_type == "warn" else compute_pe_bankruptcy_rate
    rows: list[dict[str, Any]] = []

    for prefix in sorted(prefixes):
        result = compute_fn(prefix, lookback_years, db=db)
        if result.sample_sizes.get("pe_count", 0) <= min_pe_entities:
            continue
        rows.append(
            {
                "industry": result.industry,
                "pe_count": result.sample_sizes["pe_count"],
                "non_pe_count": result.sample_sizes["non_pe_count"],
                "pe_events": result.sample_sizes["pe_events"],
                "non_pe_events": result.sample_sizes["non_pe_events"],
                "pe_rate": result.pe_rate,
                "non_pe_rate": result.non_pe_rate,
                "rate_ratio": result.rate_ratio,
                "p_value": result.p_value,
                "lookback_years": result.lookback_years,
                "event_type": event_type,
            }
        )

    # Sort by rate_ratio descending (inf sorts last in Python without special handling)
    rows.sort(
        key=lambda r: r["rate_ratio"] if r["rate_ratio"] != float("inf") else 1e18, reverse=True
    )
    return rows
