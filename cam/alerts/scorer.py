"""
M13 — Alert Scoring Engine.

Combines signals from all upstream modules (M6–M12) into per-entity composite
alert scores, writes them to the ``alert_scores`` table, and generates
structured :class:`Alert` records whenever a score crosses a severity threshold.

Score composition
-----------------
The composite score is a weighted sum of six components drawn from the Signal
table.  Each component maps to a ``signal_type`` written by an earlier module:

===========================  ======  =========================  ======
Component key                Weight  Signal type in DB          Module
===========================  ======  =========================  ======
cross_agency_composite        0.35   cross_agency_composite      M6
risk_language_expansion       0.20   risk_language_expansion     M7
earnings_divergence           0.15   earnings_divergence         M8
proxy_escalation              0.15   proxy_escalation            M9
merger_vertical_risk          0.10   merger_vertical_risk        M10
pe_warn_flag                  0.05   pe_owned                    M12
===========================  ======  =========================  ======

Missing components default to **0.0** (graceful degradation).

Alert thresholds
----------------
watch     ≥ 0.40  Worth monitoring; no action required.
elevated  ≥ 0.65  Assign to analyst for review.
critical  ≥ 0.80  Escalate; consider regulatory referral.

An alert is generated only when an entity's alert level **increases**
(e.g. no-level → watch, watch → elevated, elevated → critical).  Repeated
daily runs at the same level produce no new alerts, satisfying the
"zero duplicate alerts for same entity/threshold in same week" criterion.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from cam.db.models import AlertScore, Entity, Signal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALERT_THRESHOLDS: dict[str, float] = {
    "watch": 0.40,
    "elevated": 0.65,
    "critical": 0.80,
}

COMPONENT_WEIGHTS: dict[str, float] = {
    "cross_agency_composite": 0.35,  # M6
    "risk_language_expansion": 0.20,  # M7
    "earnings_divergence": 0.15,  # M8
    "proxy_escalation": 0.15,  # M9
    "merger_vertical_risk": 0.10,  # M10
    "pe_warn_flag": 0.05,  # M12
}

# Maps each component key to the signal_type stored in the Signal table.
# pe_warn_flag reads from the "pe_owned" signal written by flag_pe_entity_for_monitoring().
_COMPONENT_SIGNAL_MAP: dict[str, str] = {
    "cross_agency_composite": "cross_agency_composite",
    "risk_language_expansion": "risk_language_expansion",
    "earnings_divergence": "earnings_divergence",
    "proxy_escalation": "proxy_escalation",
    "merger_vertical_risk": "merger_vertical_risk",
    "pe_warn_flag": "pe_owned",
}

# Alert level ordering for threshold-crossing comparisons.
_LEVEL_ORDER: dict[str | None, int] = {None: 0, "watch": 1, "elevated": 2, "critical": 3}

_SUGGESTED_ACTIONS: dict[str, str] = {
    "watch": (
        "Monitor for further developments; no immediate action required. "
        "Flag entity for weekly review."
    ),
    "elevated": (
        "Assign to analyst for review. Cross-check signals against public filings "
        "and regulatory disclosures."
    ),
    "critical": (
        "Escalate immediately. Consider regulatory referral and coordinate with "
        "relevant agencies listed below."
    ),
}

_REGULATORY_BODIES: dict[str, list[str]] = {
    "watch": [],
    "elevated": ["OSHA", "EPA", "CFPB"],
    "critical": ["OSHA", "EPA", "CFPB", "DOJ", "FTC"],
}

# ---------------------------------------------------------------------------
# Output dataclass
# ---------------------------------------------------------------------------


@dataclass
class Alert:
    """Structured alert record generated when an entity's level increases.

    All fields are self-contained so the alert is actionable without querying
    the database.
    """

    entity_id: UUID
    canonical_name: str
    alert_level: str  # 'watch', 'elevated', 'critical'
    score: float
    score_date: date
    prior_score: float | None
    threshold_crossed: str  # which threshold triggered this alert
    component_breakdown: dict[str, float]  # per-component scores
    top_evidence: list[str] = field(default_factory=list)  # up to 5 evidence strings
    suggested_action: str = ""
    relevant_regulatory_body: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _score_to_level(score: float) -> str | None:
    """Map a composite score to its alert level string, or None if below watch."""
    if score >= ALERT_THRESHOLDS["critical"]:
        return "critical"
    if score >= ALERT_THRESHOLDS["elevated"]:
        return "elevated"
    if score >= ALERT_THRESHOLDS["watch"]:
        return "watch"
    return None


def _level_increased(prior: str | None, new: str | None) -> bool:
    """Return True when *new* alert level is strictly higher than *prior*."""
    return _LEVEL_ORDER.get(new, 0) > _LEVEL_ORDER.get(prior, 0)


def _latest_signal_score(
    db: Session,
    entity_id: UUID,
    signal_type: str,
) -> float | None:
    """Return the most recent non-null score for the given entity/signal_type pair.

    Ordering: most recent ``signal_date`` first (NULLs last), then most recent
    ``created_at`` as a tie-breaker.  This ensures explicitly-dated signals take
    precedence over undated ones from the same ingestion batch.
    """
    stmt = (
        select(Signal.score)
        .where(
            Signal.entity_id == entity_id,
            Signal.signal_type == signal_type,
            Signal.score.isnot(None),
        )
        .order_by(Signal.signal_date.desc().nulls_last(), Signal.created_at.desc())
        .limit(1)
    )
    return db.execute(stmt).scalar_one_or_none()


def _get_component_scores(db: Session, entity_id: UUID) -> dict[str, float]:
    """Fetch the latest signal score for each component; missing → 0.0."""
    scores: dict[str, float] = {}
    for component, signal_type in _COMPONENT_SIGNAL_MAP.items():
        raw = _latest_signal_score(db, entity_id, signal_type)
        scores[component] = float(raw) if raw is not None else 0.0
    return scores


def _get_top_evidence(
    db: Session,
    entity_id: UUID,
    component_breakdown: dict[str, float],
    max_items: int = 5,
) -> list[str]:
    """Return up to *max_items* evidence strings for the highest-contributing components."""
    # Rank components by their contribution (weight × score), descending
    contributions = sorted(
        [
            (comp, COMPONENT_WEIGHTS.get(comp, 0.0) * score)
            for comp, score in component_breakdown.items()
            if score > 0.0
        ],
        key=lambda x: x[1],
        reverse=True,
    )

    evidence: list[str] = []
    for comp, _ in contributions[:max_items]:
        sig_type = _COMPONENT_SIGNAL_MAP.get(comp, comp)
        row = db.execute(
            select(Signal.evidence)
            .where(
                Signal.entity_id == entity_id,
                Signal.signal_type == sig_type,
                Signal.evidence.isnot(None),
            )
            .order_by(Signal.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        if row:
            evidence.append(row)

    return evidence[:max_items]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_entity_score(
    entity_id: UUID,
    score_date: date,
    *,
    db: Session,
) -> AlertScore:
    """Compute the composite alert score for one entity as of *score_date*.

    Reads the latest Signal record for each of the six component signal types,
    computes the weighted composite (clamped to [0.0, 1.0]), determines the
    alert level, and upserts an :class:`AlertScore` row for the entity/date.

    The session is flushed but **not committed** — the caller (typically
    :func:`run_daily_scoring`) owns the commit boundary.

    Parameters
    ----------
    entity_id:   UUID of the entity to score.
    score_date:  Date for which the score is computed.
    db:          SQLAlchemy session.

    Returns
    -------
    The :class:`AlertScore` ORM object (either newly created or updated).
    """
    component_scores = _get_component_scores(db, entity_id)
    composite = sum(COMPONENT_WEIGHTS[k] * v for k, v in component_scores.items())
    composite = min(max(composite, 0.0), 1.0)
    level = _score_to_level(composite)

    # Upsert: avoid violating the unique constraint on (entity_id, score_date)
    existing = db.scalars(
        select(AlertScore)
        .where(
            AlertScore.entity_id == entity_id,
            AlertScore.score_date == score_date,
        )
        .limit(1)
    ).first()

    if existing is not None:
        existing.composite_score = round(composite, 6)
        existing.component_scores = component_scores
        existing.alert_level = level
        db.flush()
        return existing

    alert_score = AlertScore(
        entity_id=entity_id,
        score_date=score_date,
        composite_score=round(composite, 6),
        component_scores=component_scores,
        alert_level=level,
    )
    db.add(alert_score)
    db.flush()
    return alert_score


def get_prior_score(
    entity_id: UUID,
    before_date: date,
    *,
    db: Session,
) -> AlertScore | None:
    """Return the most recent :class:`AlertScore` for *entity_id* before *before_date*.

    Used by callers to retrieve the prior score for alert-level comparison.
    Returns ``None`` if no prior score exists.
    """
    stmt = (
        select(AlertScore)
        .where(
            AlertScore.entity_id == entity_id,
            AlertScore.score_date < before_date,
        )
        .order_by(AlertScore.score_date.desc())
        .limit(1)
    )
    return db.scalars(stmt).first()


def generate_alert(
    entity_id: UUID,
    score: AlertScore,
    prior_score: AlertScore | None,
    *,
    db: Session,
) -> Alert | None:
    """Generate an :class:`Alert` if the entity's alert level has increased.

    Returns ``None`` when the new score does not cross a higher threshold than
    the prior score (including the case where both are at the same level).

    An alert is generated when:
    - The entity had no prior score and now has a watch/elevated/critical score.
    - The alert level increases (watch → elevated, elevated → critical).

    Parameters
    ----------
    entity_id:   UUID of the entity being evaluated.
    score:       The newly computed :class:`AlertScore`.
    prior_score: The most recent previous :class:`AlertScore`, or ``None``.
    db:          SQLAlchemy session (read-only; no writes performed here).

    Returns
    -------
    An :class:`Alert` dataclass ready for display or persistence, or ``None``
    if no threshold was crossed.
    """
    new_level = score.alert_level
    prior_level = prior_score.alert_level if prior_score is not None else None

    if not _level_increased(prior_level, new_level):
        return None

    entity = db.get(Entity, entity_id)
    canonical_name = entity.canonical_name if entity is not None else str(entity_id)
    component_breakdown = dict(score.component_scores or {})
    top_evidence = _get_top_evidence(db, entity_id, component_breakdown)

    return Alert(
        entity_id=entity_id,
        canonical_name=canonical_name,
        alert_level=new_level,
        score=score.composite_score,
        score_date=score.score_date,
        prior_score=prior_score.composite_score if prior_score is not None else None,
        threshold_crossed=new_level,
        component_breakdown=component_breakdown,
        top_evidence=top_evidence,
        suggested_action=_SUGGESTED_ACTIONS.get(new_level, ""),
        relevant_regulatory_body=_REGULATORY_BODIES.get(new_level, []),
    )


def run_daily_scoring(
    score_date: date | None = None,
    *,
    db: Session,
) -> list[AlertScore]:
    """Score all entities that have at least one signal in the Signal table.

    This is the scheduled entry point called by the Celery task.  It iterates
    every entity with at least one Signal record, computes the composite score,
    upserts an :class:`AlertScore` row for *score_date*, and commits once at
    the end.

    Parameters
    ----------
    score_date: Date for which scores are computed (defaults to today).
    db:         SQLAlchemy session.

    Returns
    -------
    List of :class:`AlertScore` records — one per scored entity, in the order
    they were processed.
    """
    if score_date is None:
        score_date = date.today()

    # Collect all entity IDs that have at least one non-null Signal
    entity_ids: list[Any] = list(
        db.execute(
            select(Signal.entity_id).where(Signal.entity_id.isnot(None)).distinct()
        ).scalars()
    )

    results: list[AlertScore] = []
    for eid in entity_ids:
        try:
            alert_score = compute_entity_score(eid, score_date, db=db)
            results.append(alert_score)
        except Exception:
            logger.exception("Failed to score entity %s; skipping.", eid)

    db.commit()
    logger.info("Daily scoring complete: %d entities scored for %s.", len(results), score_date)
    return results
