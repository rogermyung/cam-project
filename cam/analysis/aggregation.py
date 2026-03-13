"""
Cross-agency signal aggregation (M6).

Joins events from OSHA, EPA, and CFPB into a per-entity composite summary
and composite risk score for a given lookback window.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from cam.config import Settings
from cam.db.models import Entity, Event
from cam.ingestion.cfpb import compute_complaint_rate, detect_complaint_spike

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class AgencySignalSummary:
    """Composite per-entity signal summary across all ingested agencies."""

    entity_id: UUID
    period_start: date
    period_end: date
    osha_violation_count: int
    osha_penalty_total: float
    osha_vs_industry_benchmark: float  # ratio: entity rate / industry average
    epa_violation_count: int
    epa_penalty_total: float
    cfpb_complaint_rate: float
    cfpb_spike_detected: bool
    nlrb_complaint_count: int  # reserved for future M module; always 0 for now
    agency_overlap_count: int  # number of agencies with active signals
    composite_risk_score: float  # 0.0–1.0


# ---------------------------------------------------------------------------
# Core scoring helpers
# ---------------------------------------------------------------------------


def agency_overlap_bonus(n_agencies: int) -> float:
    """Non-linear bonus for concurrent multi-agency signals.

    1 agency : 0.0
    2 agencies: 0.3
    3+ agencies: 0.7
    """
    return {0: 0.0, 1: 0.0, 2: 0.3}.get(n_agencies, 0.7)


def _get_weights() -> tuple[float, float, float, float]:
    """Return (w_osha, w_epa, w_cfpb, w_overlap) from Settings or field defaults."""
    from cam.config import get_settings

    try:
        s = get_settings()
        return s.weight_osha_rate, s.weight_epa_rate, s.weight_cfpb_spike, s.weight_agency_overlap
    except Exception:
        # Fall back to Settings field defaults when env is not fully configured
        # (e.g. missing required DATABASE_URL in unit tests).
        fields = Settings.model_fields
        return (
            fields["weight_osha_rate"].default,
            fields["weight_epa_rate"].default,
            fields["weight_cfpb_spike"].default,
            fields["weight_agency_overlap"].default,
        )


# ---------------------------------------------------------------------------
# Industry benchmarks
# ---------------------------------------------------------------------------


def compute_industry_benchmarks(
    naics_code: str,
    period_end: date,
    *,
    db: Session,
    lookback_days: int = 365,
    source: str = "osha",
) -> dict:
    """Return industry-average violation stats for a 2-digit NAICS prefix.

    Queries all entities sharing the same 2-digit NAICS prefix and returns
    average violation count and penalty total for the lookback window.

    Parameters
    ----------
    naics_code:   NAICS code of the target entity (only first 2 digits used).
    period_end:   End of the analysis window.
    db:           SQLAlchemy session.
    lookback_days: Length of the window in days.
    source:       Event source to benchmark against (default ``"osha"``).
    """
    period_start = period_end - timedelta(days=lookback_days)
    naics_prefix = (naics_code or "")[:2]

    if not naics_prefix:
        return {
            "naics_prefix": "",
            "entity_count": 0,
            "avg_violation_count": 0.0,
            "avg_penalty_total": 0.0,
        }

    entity_ids = [
        row[0]
        for row in db.execute(
            select(Entity.id).where(Entity.naics_code.startswith(naics_prefix))
        ).fetchall()
    ]

    if not entity_ids:
        return {
            "naics_prefix": naics_prefix,
            "entity_count": 0,
            "avg_violation_count": 0.0,
            "avg_penalty_total": 0.0,
        }

    rows = db.execute(
        select(
            Event.entity_id,
            func.count(Event.id).label("violation_count"),
            func.coalesce(func.sum(Event.penalty_usd), 0).label("penalty_total"),
        )
        .where(
            Event.entity_id.in_(entity_ids),
            Event.source == source,
            Event.event_type == "violation",
            Event.event_date >= period_start,
            Event.event_date <= period_end,
        )
        .group_by(Event.entity_id)
    ).fetchall()

    counts = [r.violation_count for r in rows]
    penalties = [float(r.penalty_total) for r in rows]
    # Pad zeros for entities that had no violations in the window
    n_zero = len(entity_ids) - len(counts)
    counts.extend([0] * n_zero)
    penalties.extend([0.0] * n_zero)

    n = len(counts)
    return {
        "naics_prefix": naics_prefix,
        "entity_count": n,
        "avg_violation_count": sum(counts) / n if n else 0.0,
        "avg_penalty_total": sum(penalties) / n if n else 0.0,
    }


# ---------------------------------------------------------------------------
# Main aggregation function
# ---------------------------------------------------------------------------


def compute_agency_summary(
    entity_id: UUID,
    period_end: date,
    lookback_days: int = 365,
    *,
    db: Session,
) -> AgencySignalSummary:
    """Compute the composite agency signal summary for one entity.

    Aggregates OSHA, EPA, and CFPB signals over ``lookback_days`` ending on
    ``period_end``, computes industry benchmarks where possible, and returns
    a fully populated :class:`AgencySignalSummary` including the composite
    risk score.

    Missing data (no events, no NAICS code, no CFPB financial data) always
    defaults to 0.0 sub-scores rather than raising an error.
    """
    period_start = period_end - timedelta(days=lookback_days)

    # --- OSHA ---
    osha_row = db.execute(
        select(
            func.count(Event.id).label("count"),
            func.coalesce(func.sum(Event.penalty_usd), 0).label("penalty"),
        ).where(
            Event.entity_id == entity_id,
            Event.source == "osha",
            Event.event_type == "violation",
            Event.event_date >= period_start,
            Event.event_date <= period_end,
        )
    ).one()
    osha_violation_count = osha_row.count
    osha_penalty_total = float(osha_row.penalty)

    # --- EPA ---
    epa_row = db.execute(
        select(
            func.count(Event.id).label("count"),
            func.coalesce(func.sum(Event.penalty_usd), 0).label("penalty"),
        ).where(
            Event.entity_id == entity_id,
            Event.source == "epa_echo",
            Event.event_type == "violation",
            Event.event_date >= period_start,
            Event.event_date <= period_end,
        )
    ).one()
    epa_violation_count = epa_row.count
    epa_penalty_total = float(epa_row.penalty)

    # --- Industry benchmark ratios ---
    entity = db.get(Entity, entity_id)
    naics_code = entity.naics_code if entity else None

    def _benchmark_ratio(violation_count: int, source: str) -> float:
        """Return entity violation count / industry average, or 0.0 if unavailable."""
        if not naics_code:
            return 0.0
        bm = compute_industry_benchmarks(
            naics_code, period_end, db=db, lookback_days=lookback_days, source=source
        )
        avg = bm["avg_violation_count"]
        if avg > 0:
            return violation_count / avg
        # Any violations when industry average is zero → maximum ratio
        return 2.0 if violation_count > 0 else 0.0

    osha_vs_benchmark = _benchmark_ratio(osha_violation_count, "osha")
    epa_vs_benchmark = _benchmark_ratio(epa_violation_count, "epa_echo")

    # --- CFPB ---
    cfpb_rate_result = compute_complaint_rate(
        entity_id, period_months=max(1, lookback_days // 30), db=db, period_end=period_end
    )
    cfpb_complaint_rate = 0.0
    if cfpb_rate_result is not None and cfpb_rate_result.rate_per_billion is not None:
        cfpb_complaint_rate = float(cfpb_rate_result.rate_per_billion)

    cfpb_spike = detect_complaint_spike(entity_id, lookback_months=6, db=db, period_end=period_end)

    # Count CFPB events directly in the lookback window for overlap detection.
    # Using raw count (rather than cfpb_spike / rate) avoids dependence on
    # detect_complaint_spike's internal date.today() reference.
    cfpb_event_count = db.execute(
        select(func.count(Event.id)).where(
            Event.entity_id == entity_id,
            Event.source == "cfpb_complaint",
            Event.event_type == "complaint",
            Event.event_date >= period_start,
            Event.event_date <= period_end,
        )
    ).scalar_one()

    # --- Agency overlap ---
    active_agencies = sum(
        [
            osha_violation_count > 0,
            epa_violation_count > 0,
            cfpb_event_count > 0,
        ]
    )

    # --- Composite score ---
    w_osha, w_epa, w_cfpb, w_overlap = _get_weights()

    # Normalise ratios: entity at 2× industry average → sub-score = 1.0
    osha_sub = min(osha_vs_benchmark / 2.0, 1.0)
    epa_sub = min(epa_vs_benchmark / 2.0, 1.0)
    cfpb_sub = 1.0 if cfpb_spike else 0.0
    overlap_sub = agency_overlap_bonus(active_agencies)

    composite = w_osha * osha_sub + w_epa * epa_sub + w_cfpb * cfpb_sub + w_overlap * overlap_sub
    composite_risk_score = min(max(composite, 0.0), 1.0)

    return AgencySignalSummary(
        entity_id=entity_id,
        period_start=period_start,
        period_end=period_end,
        osha_violation_count=osha_violation_count,
        osha_penalty_total=osha_penalty_total,
        osha_vs_industry_benchmark=osha_vs_benchmark,
        epa_violation_count=epa_violation_count,
        epa_penalty_total=epa_penalty_total,
        cfpb_complaint_rate=cfpb_complaint_rate,
        cfpb_spike_detected=cfpb_spike,
        nlrb_complaint_count=0,
        agency_overlap_count=active_agencies,
        composite_risk_score=composite_risk_score,
    )
