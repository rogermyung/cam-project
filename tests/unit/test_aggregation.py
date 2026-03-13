"""
Unit tests for M6 — Cross-Agency Aggregation (cam/analysis/aggregation.py).
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cam.analysis.aggregation import (
    AgencySignalSummary,
    agency_overlap_bonus,
    compute_agency_summary,
    compute_industry_benchmarks,
)
from cam.config import Settings
from cam.db.models import Base, Entity, Event

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

TODAY = date(2024, 6, 1)


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


def _entity(db: Session, name: str, naics_code: str | None = None) -> Entity:
    e = Entity(id=uuid.uuid4(), canonical_name=name, naics_code=naics_code)
    db.add(e)
    db.flush()
    return e


def _event(
    db: Session,
    entity_id: uuid.UUID,
    source: str,
    event_type: str,
    event_date: date,
    penalty_usd: float | None = None,
) -> Event:
    ev = Event(
        id=uuid.uuid4(),
        entity_id=entity_id,
        source=source,
        event_type=event_type,
        event_date=event_date,
        penalty_usd=Decimal(str(penalty_usd)) if penalty_usd is not None else None,
    )
    db.add(ev)
    db.flush()
    return ev


# ---------------------------------------------------------------------------
# agency_overlap_bonus
# ---------------------------------------------------------------------------


def test_overlap_bonus_zero():
    assert agency_overlap_bonus(0) == 0.0


def test_overlap_bonus_one():
    assert agency_overlap_bonus(1) == 0.0


def test_overlap_bonus_two():
    assert agency_overlap_bonus(2) == 0.3


def test_overlap_bonus_three():
    assert agency_overlap_bonus(3) == 0.7


def test_overlap_bonus_four_and_above():
    assert agency_overlap_bonus(4) == 0.7
    assert agency_overlap_bonus(10) == 0.7


# ---------------------------------------------------------------------------
# Default weights
# ---------------------------------------------------------------------------


def test_weights_sum_to_one():
    fields = Settings.model_fields
    total = (
        fields["weight_osha_rate"].default
        + fields["weight_epa_rate"].default
        + fields["weight_cfpb_spike"].default
        + fields["weight_agency_overlap"].default
    )
    assert abs(total - 1.0) < 1e-9


# ---------------------------------------------------------------------------
# compute_industry_benchmarks
# ---------------------------------------------------------------------------


def test_benchmarks_no_entities(db):
    result = compute_industry_benchmarks("33", TODAY, db=db)
    assert result["entity_count"] == 0
    assert result["avg_violation_count"] == 0.0
    assert result["avg_penalty_total"] == 0.0


def test_benchmarks_empty_naics_code(db):
    result = compute_industry_benchmarks("", TODAY, db=db)
    assert result["entity_count"] == 0


def test_benchmarks_segments_by_naics_prefix(db):
    e1 = _entity(db, "Acme Corp", naics_code="3311")
    _event(db, e1.id, "osha", "violation", date(2024, 1, 15), penalty_usd=5000)
    _event(db, e1.id, "osha", "violation", date(2024, 2, 15), penalty_usd=3000)
    # Different NAICS prefix — must NOT be included
    e2 = _entity(db, "Other Corp", naics_code="4411")
    _event(db, e2.id, "osha", "violation", date(2024, 1, 15), penalty_usd=10000)

    result = compute_industry_benchmarks("3311", TODAY, db=db)
    assert result["naics_prefix"] == "33"
    assert result["entity_count"] == 1
    assert result["avg_violation_count"] == 2.0
    assert result["avg_penalty_total"] == 8000.0


def test_benchmarks_includes_zero_violation_entities(db):
    """Entities in the NAICS group with no violations must be counted (drag the average down)."""
    e1 = _entity(db, "Dirty Corp", naics_code="3311")
    _event(db, e1.id, "osha", "violation", date(2024, 1, 15))
    _entity(db, "Clean Corp", naics_code="3322")  # same "33" prefix, no violations

    result = compute_industry_benchmarks("3311", TODAY, db=db)
    assert result["entity_count"] == 2
    assert result["avg_violation_count"] == 0.5  # (1 + 0) / 2


def test_benchmarks_excludes_out_of_window_events(db):
    e1 = _entity(db, "Corp", naics_code="3311")
    _event(db, e1.id, "osha", "violation", date(2022, 1, 1))  # > 365 days before TODAY

    result = compute_industry_benchmarks("3311", TODAY, db=db)
    assert result["avg_violation_count"] == 0.0


def test_benchmarks_source_parameter(db):
    e1 = _entity(db, "Corp", naics_code="3311")
    _event(db, e1.id, "osha", "violation", date(2024, 1, 1))
    _event(db, e1.id, "epa_echo", "violation", date(2024, 1, 2))

    osha_result = compute_industry_benchmarks("3311", TODAY, db=db, source="osha")
    epa_result = compute_industry_benchmarks("3311", TODAY, db=db, source="epa_echo")
    assert osha_result["avg_violation_count"] == 1.0
    assert epa_result["avg_violation_count"] == 1.0


# ---------------------------------------------------------------------------
# compute_agency_summary — missing / empty data
# ---------------------------------------------------------------------------


def test_summary_no_events(db):
    entity = _entity(db, "Empty Corp")
    summary = compute_agency_summary(entity.id, TODAY, db=db)

    assert isinstance(summary, AgencySignalSummary)
    assert summary.osha_violation_count == 0
    assert summary.osha_penalty_total == 0.0
    assert summary.epa_violation_count == 0
    assert summary.epa_penalty_total == 0.0
    assert summary.cfpb_complaint_rate == 0.0
    assert summary.cfpb_spike_detected is False
    assert summary.nlrb_complaint_count == 0
    assert summary.agency_overlap_count == 0
    assert summary.composite_risk_score == 0.0


def test_summary_period_fields(db):
    entity = _entity(db, "Corp")
    summary = compute_agency_summary(entity.id, TODAY, lookback_days=365, db=db)

    assert summary.period_end == TODAY
    # timedelta(days=365) from 2024-06-01 spans a leap year → 2023-06-02
    assert summary.period_start == TODAY - timedelta(days=365)
    assert summary.entity_id == entity.id


# ---------------------------------------------------------------------------
# compute_agency_summary — OSHA signals
# ---------------------------------------------------------------------------


def test_summary_osha_only(db):
    entity = _entity(db, "Dangerous Corp", naics_code="3311")
    for i in range(5):
        _event(db, entity.id, "osha", "violation", date(2024, 1, i + 1), penalty_usd=1000)

    summary = compute_agency_summary(entity.id, TODAY, db=db)

    assert summary.osha_violation_count == 5
    assert summary.osha_penalty_total == 5000.0
    assert summary.epa_violation_count == 0
    assert summary.agency_overlap_count == 1
    assert summary.composite_risk_score > 0.0


def test_summary_excludes_events_outside_window(db):
    entity = _entity(db, "Corp")
    _event(db, entity.id, "osha", "violation", date(2024, 1, 1))  # inside window
    _event(db, entity.id, "osha", "violation", date(2022, 1, 1))  # outside window

    summary = compute_agency_summary(entity.id, TODAY, lookback_days=365, db=db)
    assert summary.osha_violation_count == 1


# ---------------------------------------------------------------------------
# compute_agency_summary — EPA signals
# ---------------------------------------------------------------------------


def test_summary_epa_only(db):
    entity = _entity(db, "Polluter Corp", naics_code="3311")
    _event(db, entity.id, "epa_echo", "violation", date(2024, 3, 1), penalty_usd=50000)

    summary = compute_agency_summary(entity.id, TODAY, db=db)
    assert summary.epa_violation_count == 1
    assert summary.epa_penalty_total == 50000.0
    assert summary.osha_violation_count == 0
    assert summary.agency_overlap_count == 1


# ---------------------------------------------------------------------------
# compute_agency_summary — industry benchmark ratio
# ---------------------------------------------------------------------------


def test_summary_benchmark_ratio_above_average(db):
    """Entity with more violations than peers should have ratio > 1."""
    entity = _entity(db, "Bad Corp", naics_code="3311")
    for _ in range(10):
        _event(db, entity.id, "osha", "violation", date(2024, 1, 15))

    peer = _entity(db, "Good Corp", naics_code="3322")  # same "33" prefix
    _event(db, peer.id, "osha", "violation", date(2024, 1, 15))

    summary = compute_agency_summary(entity.id, TODAY, db=db)
    # entity has 10, peer has 1, avg = (10+1)/2 = 5.5 → ratio ≈ 1.82
    assert summary.osha_vs_industry_benchmark > 1.0


def test_summary_no_naics_benchmark_defaults_to_zero(db):
    entity = _entity(db, "Corp", naics_code=None)
    _event(db, entity.id, "osha", "violation", date(2024, 1, 1))

    summary = compute_agency_summary(entity.id, TODAY, db=db)
    assert summary.osha_vs_industry_benchmark == 0.0


# ---------------------------------------------------------------------------
# compute_agency_summary — multi-agency overlap
# ---------------------------------------------------------------------------


def test_summary_multi_agency_scores_higher_than_single(db):
    today = date(2024, 6, 1)

    multi = _entity(db, "Multi Corp", naics_code="3311")
    _event(db, multi.id, "osha", "violation", date(2024, 1, 1), penalty_usd=1000)
    _event(db, multi.id, "epa_echo", "violation", date(2024, 2, 1), penalty_usd=5000)

    single = _entity(db, "Single Corp", naics_code="3311")
    _event(db, single.id, "osha", "violation", date(2024, 1, 1), penalty_usd=1000)

    multi_summary = compute_agency_summary(multi.id, today, db=db)
    single_summary = compute_agency_summary(single.id, today, db=db)

    assert multi_summary.agency_overlap_count == 2
    assert single_summary.agency_overlap_count == 1
    assert multi_summary.composite_risk_score > single_summary.composite_risk_score


def test_summary_three_agencies_max_overlap_bonus(db):
    entity = _entity(db, "Triple Threat Corp", naics_code="3311")
    _event(db, entity.id, "osha", "violation", date(2024, 1, 1), penalty_usd=1000)
    _event(db, entity.id, "epa_echo", "violation", date(2024, 1, 2), penalty_usd=1000)
    # CFPB complaint
    _event(db, entity.id, "cfpb_complaint", "complaint", date(2024, 1, 3))

    summary = compute_agency_summary(entity.id, TODAY, db=db)
    assert summary.agency_overlap_count == 3


# ---------------------------------------------------------------------------
# compute_agency_summary — composite score properties
# ---------------------------------------------------------------------------


def test_composite_score_clamped_between_zero_and_one(db):
    entity = _entity(db, "Corp")
    summary = compute_agency_summary(entity.id, TODAY, db=db)
    assert 0.0 <= summary.composite_risk_score <= 1.0


def test_composite_score_max_inputs():
    """Verify formula with all sub-scores at maximum."""
    fields = Settings.model_fields
    w_osha = fields["weight_osha_rate"].default
    w_epa = fields["weight_epa_rate"].default
    w_cfpb = fields["weight_cfpb_spike"].default
    w_overlap = fields["weight_agency_overlap"].default
    # osha_sub=1.0, epa_sub=1.0, cfpb_sub=1.0, overlap=0.7 (3 agencies)
    expected = w_osha * 1.0 + w_epa * 1.0 + w_cfpb * 1.0 + w_overlap * 0.7
    assert abs(expected - (0.25 + 0.20 + 0.20 + 0.35 * 0.7)) < 1e-9
    assert expected <= 1.0


def test_composite_score_reproducible(db):
    """Same inputs always yield the same score."""
    entity = _entity(db, "Corp", naics_code="3311")
    _event(db, entity.id, "osha", "violation", date(2024, 1, 1), penalty_usd=5000)
    _event(db, entity.id, "epa_echo", "violation", date(2024, 2, 1), penalty_usd=2000)

    s1 = compute_agency_summary(entity.id, TODAY, db=db)
    s2 = compute_agency_summary(entity.id, TODAY, db=db)
    assert s1.composite_risk_score == s2.composite_risk_score


def test_cfpb_spike_respects_period_end(db):
    """CFPB spike detection must use period_end, not date.today(), for reproducibility."""
    entity = _entity(db, "Corp")
    historical_end = date(2024, 6, 1)

    # Seed complaints in the recent half of the lookback window (last 3 of 6 months)
    # prior half is empty → guaranteed spike
    recent_date = historical_end - timedelta(days=15)  # inside recent half
    for _ in range(5):
        _event(db, entity.id, "cfpb_complaint", "complaint", recent_date)

    summary = compute_agency_summary(entity.id, historical_end, lookback_days=365, db=db)
    # Spike should be detected relative to historical_end, regardless of today's actual date
    assert summary.cfpb_spike_detected is True

    # Calling again must produce identical result (reproducible)
    summary2 = compute_agency_summary(entity.id, historical_end, lookback_days=365, db=db)
    assert summary2.cfpb_spike_detected == summary.cfpb_spike_detected


# ---------------------------------------------------------------------------
# Integration: seed from multiple sources, verify full summary
# ---------------------------------------------------------------------------


def test_integration_full_summary(db):
    """Seed OSHA + EPA events and verify summary fields are all populated."""
    entity = _entity(db, "Acme Industries", naics_code="3311")
    peer = _entity(db, "Peer Corp", naics_code="3399")  # same "33" NAICS prefix

    # OSHA: entity has 4 violations, peer has 2 → entity is above average
    for i in range(4):
        _event(db, entity.id, "osha", "violation", date(2024, i + 1, 15), penalty_usd=2500)
    for i in range(2):
        _event(db, peer.id, "osha", "violation", date(2024, i + 1, 15), penalty_usd=1000)

    # EPA: entity has 2 violations
    _event(db, entity.id, "epa_echo", "violation", date(2024, 3, 1), penalty_usd=10000)
    _event(db, entity.id, "epa_echo", "violation", date(2024, 4, 1), penalty_usd=15000)

    summary = compute_agency_summary(entity.id, TODAY, db=db)

    assert summary.osha_violation_count == 4
    assert summary.osha_penalty_total == 10000.0
    assert summary.epa_violation_count == 2
    assert summary.epa_penalty_total == 25000.0
    assert summary.nlrb_complaint_count == 0  # not yet implemented
    assert summary.agency_overlap_count == 2
    assert summary.osha_vs_industry_benchmark > 1.0  # worse than industry average
    assert summary.composite_risk_score > 0.0
    assert 0.0 <= summary.composite_risk_score <= 1.0
