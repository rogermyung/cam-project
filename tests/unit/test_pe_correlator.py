"""
Unit tests for M12 — PE/Bankruptcy Correlator.

Covers:
- PEComparison dataclass structure
- compute_pe_warn_rate: known PE/non-PE split with known outcome rates
- compute_pe_bankruptcy_rate: same structure
- Rate ratio computation with edge cases (zero non-PE events, zero PE events)
- p-value computation via scipy.stats Fisher's exact
- Insufficient sample size returns p_value=None
- flag_pe_entity_for_monitoring: idempotency, signal created correctly
- summarize_all_industries: multi-sector summary, min_pe_entities filter
- Performance
"""

from __future__ import annotations

import time
import uuid
from datetime import date, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cam.analysis.pe_correlator import (
    MIN_PE_SAMPLE,
    PEComparison,
    _compute_p_value,
    _count_events,
    _entity_ids_with_events,
    _get_pe_entity_ids,
    compute_pe_bankruptcy_rate,
    compute_pe_warn_rate,
    flag_pe_entity_for_monitoring,
    summarize_all_industries,
)
from cam.db.models import Base, Entity, Event, Signal

# ---------------------------------------------------------------------------
# In-memory SQLite DB fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


# ---------------------------------------------------------------------------
# Helpers: synthetic dataset builders
# ---------------------------------------------------------------------------


def _make_entity(
    db: Session,
    name: str,
    naics_code: str | None = "62",
    commit: bool = False,
) -> Entity:
    """Create and add an entity to the session."""
    e = Entity(id=uuid.uuid4(), canonical_name=name, naics_code=naics_code)
    db.add(e)
    if commit:
        db.commit()
    else:
        db.flush()
    return e


def _flag_pe(db: Session, entity: Entity, commit: bool = False) -> None:
    """Flag an entity as PE-owned via a Signal record."""
    sig = Signal(
        entity_id=entity.id,
        source="manual",
        signal_type="pe_owned",
        score=1.0,
        evidence="test",
    )
    db.add(sig)
    if commit:
        db.commit()
    else:
        db.flush()


def _make_warn_event(
    db: Session,
    entity: Entity,
    event_date: date,
    commit: bool = False,
) -> Event:
    ev = Event(
        entity_id=entity.id,
        source="warn",
        event_type="warn_notice",
        event_date=event_date,
        description="Layoff notice",
    )
    db.add(ev)
    if commit:
        db.commit()
    else:
        db.flush()
    return ev


def _make_bankruptcy_event(
    db: Session,
    entity: Entity,
    event_date: date,
    commit: bool = False,
) -> Event:
    ev = Event(
        entity_id=entity.id,
        source="pacer",
        event_type="bankruptcy",
        event_date=event_date,
        description="Chapter 11 filing",
    )
    db.add(ev)
    if commit:
        db.commit()
    else:
        db.flush()
    return ev


# ---------------------------------------------------------------------------
# PEComparison dataclass
# ---------------------------------------------------------------------------


def test_pe_comparison_fields():
    pc = PEComparison(
        industry="62",
        pe_rate=0.5,
        non_pe_rate=0.1,
        rate_ratio=5.0,
        sample_sizes={"pe_count": 20, "non_pe_count": 50},
        p_value=0.01,
        lookback_years=5,
    )
    assert pc.industry == "62"
    assert pc.rate_ratio == 5.0
    assert pc.p_value == 0.01


def test_pe_comparison_default_p_value_none():
    pc = PEComparison(industry="52", pe_rate=0.0, non_pe_rate=0.0, rate_ratio=1.0)
    assert pc.p_value is None


# ---------------------------------------------------------------------------
# _get_pe_entity_ids
# ---------------------------------------------------------------------------


def test_get_pe_entity_ids_empty(db):
    assert _get_pe_entity_ids(db) == set()


def test_get_pe_entity_ids_returns_flagged(db):
    e1 = _make_entity(db, "PE Corp A", "62")
    e2 = _make_entity(db, "Public Corp B", "62")
    _flag_pe(db, e1)
    db.flush()
    result = _get_pe_entity_ids(db)
    assert e1.id in result
    assert e2.id not in result


# ---------------------------------------------------------------------------
# compute_pe_warn_rate — basic cases
# ---------------------------------------------------------------------------


def test_compute_pe_warn_rate_zero_entities(db):
    """No entities in sector → rates are 0, p_value is None."""
    result = compute_pe_warn_rate("99", db=db)
    assert isinstance(result, PEComparison)
    assert result.pe_rate == 0.0
    assert result.non_pe_rate == 0.0
    assert result.p_value is None
    assert result.industry == "99"


def test_compute_pe_warn_rate_known_ratio(db):
    """PE companies get WARN events at 2x the rate of non-PE companies.

    Setup:
    - 12 PE entities in NAICS 62, each with 2 WARN events in lookback
    - 12 non-PE entities in NAICS 62, each with 1 WARN event in lookback

    Expected rate ratio ≈ 2.0.
    """
    today = date.today()
    recent = today - timedelta(days=365)

    pe_entities = [_make_entity(db, f"PE Corp {i}", "62") for i in range(12)]
    non_pe_entities = [_make_entity(db, f"Public Corp {i}", "62") for i in range(12)]

    for e in pe_entities:
        _flag_pe(db, e)
        _make_warn_event(db, e, recent)
        _make_warn_event(db, e, recent - timedelta(days=30))

    for e in non_pe_entities:
        _make_warn_event(db, e, recent)

    db.flush()
    result = compute_pe_warn_rate("62", lookback_years=5, db=db)

    assert result.sample_sizes["pe_count"] == 12
    assert result.sample_sizes["non_pe_count"] == 12
    assert result.sample_sizes["pe_events"] == 24
    assert result.sample_sizes["non_pe_events"] == 12
    # PE: 24 events / (12 companies * 5 years) = 0.4
    # non-PE: 12 / (12 * 5) = 0.2 → ratio = 2.0
    assert abs(result.rate_ratio - 2.0) < 0.001


def test_compute_pe_warn_rate_factors_only_naics_sector(db):
    """Entities in a different NAICS sector must not affect the computation."""
    today = date.today()
    recent = today - timedelta(days=60)

    # 12 PE entities in NAICS 62 with events
    pe_entities = [_make_entity(db, f"PE Health {i}", "62") for i in range(12)]
    for e in pe_entities:
        _flag_pe(db, e)
        _make_warn_event(db, e, recent)

    # 5 PE entities in NAICS 31 (manufacturing) — should not affect sector 62 computation
    other = [_make_entity(db, f"PE Mfg {i}", "31") for i in range(5)]
    for e in other:
        _flag_pe(db, e)
        _make_warn_event(db, e, recent)
        _make_warn_event(db, e, recent)

    db.flush()
    result = compute_pe_warn_rate("62", db=db)
    assert result.sample_sizes["pe_count"] == 12


def test_compute_pe_warn_rate_excludes_old_events(db):
    """Events before the lookback window must not count."""
    today = date.today()
    old_date = today - timedelta(days=365 * 6)  # 6 years ago — outside 5-year window
    recent = today - timedelta(days=60)

    pe_entities = [_make_entity(db, f"PE Corp {i}", "52") for i in range(12)]
    for e in pe_entities:
        _flag_pe(db, e)
        _make_warn_event(db, e, old_date)  # old — should not count

    pe_entities[0]  # one recent event for entity 0
    _make_warn_event(db, pe_entities[0], recent)

    db.flush()
    result = compute_pe_warn_rate("52", lookback_years=5, db=db)
    assert result.sample_sizes["pe_events"] == 1  # only the recent one counts


# ---------------------------------------------------------------------------
# compute_pe_warn_rate — edge cases
# ---------------------------------------------------------------------------


def test_compute_pe_warn_rate_zero_non_pe_events(db):
    """Zero non-PE events → non_pe_rate == 0; rate_ratio == inf."""
    today = date.today()
    recent = today - timedelta(days=60)

    pe_entities = [_make_entity(db, f"PE Corp {i}", "56") for i in range(12)]
    [_make_entity(db, f"Public Corp {i}", "56") for i in range(12)]

    for e in pe_entities:
        _flag_pe(db, e)
        _make_warn_event(db, e, recent)
    # non-PE: no events

    db.flush()
    result = compute_pe_warn_rate("56", db=db)
    assert result.non_pe_rate == 0.0
    assert result.rate_ratio == float("inf")


def test_compute_pe_warn_rate_zero_both_rates(db):
    """Neither PE nor non-PE has events → rate_ratio == 1.0 (no observable difference)."""
    pe_entities = [_make_entity(db, f"PE Corp {i}", "23") for i in range(12)]
    [_make_entity(db, f"Public Corp {i}", "23") for i in range(12)]

    for e in pe_entities:
        _flag_pe(db, e)

    db.flush()
    result = compute_pe_warn_rate("23", db=db)
    assert result.pe_rate == 0.0
    assert result.non_pe_rate == 0.0
    assert result.rate_ratio == 1.0


def test_compute_pe_warn_rate_naics_prefix_matching(db):
    """Full NAICS code '6211' should be included in sector '62'."""
    today = date.today()
    recent = today - timedelta(days=60)

    pe_entities = [_make_entity(db, f"PE Clinic {i}", "6211") for i in range(12)]
    for e in pe_entities:
        _flag_pe(db, e)
        _make_warn_event(db, e, recent)

    db.flush()
    result = compute_pe_warn_rate("62", db=db)
    assert result.sample_sizes["pe_count"] == 12


# ---------------------------------------------------------------------------
# p-value computation
# ---------------------------------------------------------------------------


def test_p_value_small_sample_returns_none(db):
    """Fewer than MIN_PE_SAMPLE PE entities → p_value must be None."""
    today = date.today()
    recent = today - timedelta(days=60)

    # 9 PE entities (below MIN_PE_SAMPLE=10)
    pe_entities = [_make_entity(db, f"PE Corp {i}", "71") for i in range(9)]
    non_pe_entities = [_make_entity(db, f"Public Corp {i}", "71") for i in range(20)]

    for e in pe_entities:
        _flag_pe(db, e)
        _make_warn_event(db, e, recent)
    for e in non_pe_entities:
        _make_warn_event(db, e, recent)

    db.flush()
    result = compute_pe_warn_rate("71", db=db)
    assert result.p_value is None


def test_p_value_present_when_sample_sufficient(db):
    """≥ MIN_PE_SAMPLE PE entities → p_value is a float."""
    today = date.today()
    recent = today - timedelta(days=60)

    pe_entities = [_make_entity(db, f"PE Corp {i}", "72") for i in range(MIN_PE_SAMPLE)]
    [_make_entity(db, f"Public Corp {i}", "72") for i in range(20)]

    for e in pe_entities:
        _flag_pe(db, e)
        _make_warn_event(db, e, recent)
    # non-PE: no events → p-value should be very small

    db.flush()
    result = compute_pe_warn_rate("72", db=db)
    assert result.p_value is not None
    assert isinstance(result.p_value, float)
    assert 0.0 <= result.p_value <= 1.0


def test_compute_p_value_direct_known_values():
    """Direct unit test for _compute_p_value with known Fisher's exact result."""
    # Perfect separation: 10 PE all have events, 10 non-PE none do
    p = _compute_p_value(pe_with=10, pe_without=0, non_pe_with=0, non_pe_without=10)
    assert p is not None
    assert p < 0.005  # should be highly significant


def test_compute_p_value_no_difference():
    """Equal proportions → p_value should be ~0.5 (not significant)."""
    p = _compute_p_value(pe_with=5, pe_without=5, non_pe_with=5, non_pe_without=5)
    assert p is not None
    assert p > 0.1  # not significant


def test_compute_p_value_all_zero_returns_none():
    """All-zero table → p_value must be None (degenerate case)."""
    p = _compute_p_value(0, 0, 0, 0)
    assert p is None


# ---------------------------------------------------------------------------
# compute_pe_bankruptcy_rate
# ---------------------------------------------------------------------------


def test_compute_pe_bankruptcy_rate_structure(db):
    """Bankruptcy rate function returns a PEComparison with industry set."""
    result = compute_pe_bankruptcy_rate("44", db=db)
    assert isinstance(result, PEComparison)
    assert result.industry == "44"


def test_compute_pe_bankruptcy_rate_known_ratio(db):
    """PE companies go bankrupt at 3x rate of non-PE peers."""
    today = date.today()
    recent = today - timedelta(days=60)

    pe_entities = [_make_entity(db, f"PE Retail {i}", "44") for i in range(12)]
    non_pe_entities = [_make_entity(db, f"Public Retail {i}", "44") for i in range(30)]

    for e in pe_entities:
        _flag_pe(db, e)
        # 3 bankruptcy events each over 5 years → rate = 3 / 5 = 0.6
        for _ in range(3):
            _make_bankruptcy_event(db, e, recent)

    for e in non_pe_entities:
        # 1 bankruptcy event each over 5 years → rate = 1 / 5 = 0.2
        _make_bankruptcy_event(db, e, recent)

    db.flush()
    result = compute_pe_bankruptcy_rate("44", lookback_years=5, db=db)
    # PE rate = 36 / (12*5) = 0.6; non-PE rate = 30 / (30*5) = 0.2 → ratio = 3.0
    assert abs(result.rate_ratio - 3.0) < 0.001


def test_compute_pe_bankruptcy_does_not_count_warn_events(db):
    """WARN events must not affect bankruptcy rate computation."""
    today = date.today()
    recent = today - timedelta(days=60)

    pe_entities = [_make_entity(db, f"PE Corp {i}", "81") for i in range(12)]
    for e in pe_entities:
        _flag_pe(db, e)
        _make_warn_event(db, e, recent)  # WARN — not bankruptcy

    db.flush()
    result = compute_pe_bankruptcy_rate("81", db=db)
    assert result.sample_sizes["pe_events"] == 0  # no bankruptcy events


# ---------------------------------------------------------------------------
# flag_pe_entity_for_monitoring
# ---------------------------------------------------------------------------


def test_flag_pe_entity_creates_signal(db):
    entity = _make_entity(db, "Blackstone Portfolio Co", "62", commit=True)
    flag_pe_entity_for_monitoring(entity.id, db=db)
    signals = db.query(Signal).filter_by(entity_id=entity.id, signal_type="pe_owned").all()
    assert len(signals) == 1
    assert signals[0].source == "manual"
    assert signals[0].score == 1.0


def test_flag_pe_entity_idempotent(db):
    """Calling flag_pe_entity_for_monitoring twice must not create duplicate signals."""
    entity = _make_entity(db, "Blackstone Portfolio Co", "62", commit=True)
    flag_pe_entity_for_monitoring(entity.id, db=db)
    flag_pe_entity_for_monitoring(entity.id, db=db)  # second call — should be a no-op
    count = db.query(Signal).filter_by(entity_id=entity.id, signal_type="pe_owned").count()
    assert count == 1


def test_flag_pe_entity_custom_evidence(db):
    entity = _make_entity(db, "KKR Portfolio Co", "52", commit=True)
    flag_pe_entity_for_monitoring(
        entity.id, db=db, evidence="Listed in PE Stakeholder Project 2024"
    )
    sig = db.query(Signal).filter_by(entity_id=entity.id, signal_type="pe_owned").one()
    assert "PE Stakeholder Project" in sig.evidence


def test_flag_pe_entity_default_evidence(db):
    entity = _make_entity(db, "Apollo Portfolio Co", "62", commit=True)
    flag_pe_entity_for_monitoring(entity.id, db=db)
    sig = db.query(Signal).filter_by(entity_id=entity.id, signal_type="pe_owned").one()
    assert sig.evidence  # non-empty default


def test_flag_pe_entity_now_appears_in_pe_ids(db):
    """After flagging, entity should appear in _get_pe_entity_ids()."""
    entity = _make_entity(db, "New PE Corp", "62", commit=True)
    assert entity.id not in _get_pe_entity_ids(db)
    flag_pe_entity_for_monitoring(entity.id, db=db)
    assert entity.id in _get_pe_entity_ids(db)


# ---------------------------------------------------------------------------
# summarize_all_industries
# ---------------------------------------------------------------------------


def _make_sector(
    db: Session, naics: str, pe_count: int, non_pe_count: int, pe_events_each: int = 2
):
    """Create entities + PE flags + WARN events for a synthetic sector."""
    today = date.today()
    recent = today - timedelta(days=60)
    pe_entities = [_make_entity(db, f"PE {naics} Corp {i}", naics) for i in range(pe_count)]
    non_pe_entities = [
        _make_entity(db, f"Pub {naics} Corp {i}", naics) for i in range(non_pe_count)
    ]
    for e in pe_entities:
        _flag_pe(db, e)
        for _ in range(pe_events_each):
            _make_warn_event(db, e, recent)
    return pe_entities, non_pe_entities


def test_summarize_all_industries_empty_db(db):
    result = summarize_all_industries(db=db)
    assert result == []


def test_summarize_all_industries_excludes_small_pe(db):
    """Sectors with < min_pe_entities PE companies must be excluded."""
    # 5 PE entities in NAICS 72 — below default MIN_PE_SAMPLE (10)
    _make_sector(db, "72", pe_count=5, non_pe_count=20)
    db.flush()
    result = summarize_all_industries(db=db, min_pe_entities=10)
    assert all(row["pe_count"] >= 10 for row in result)


def test_summarize_all_industries_includes_large_pe(db):
    """Sectors with ≥ min_pe_entities PE companies must be included."""
    _make_sector(db, "62", pe_count=12, non_pe_count=20)
    db.flush()
    result = summarize_all_industries(db=db, min_pe_entities=10)
    assert any(row["industry"] == "62" for row in result)


def test_summarize_all_industries_sorted_by_ratio(db):
    """Rows must be sorted by rate_ratio descending."""
    # Sector 44: high ratio
    _make_sector(db, "44", pe_count=12, non_pe_count=12, pe_events_each=4)
    # Sector 62: low ratio (pe_events_each=1)
    _make_sector(db, "62", pe_count=12, non_pe_count=12, pe_events_each=1)
    db.flush()

    result = summarize_all_industries(db=db, min_pe_entities=10)
    ratios = [r["rate_ratio"] for r in result]
    assert ratios == sorted(ratios, reverse=True)


def test_summarize_all_industries_bankruptcy_mode(db):
    """event_type='bankruptcy' should use bankruptcy events, not WARN."""
    today = date.today()
    recent = today - timedelta(days=60)

    pe_entities = [_make_entity(db, f"PE Fin {i}", "52") for i in range(12)]
    for e in pe_entities:
        _flag_pe(db, e)
        _make_bankruptcy_event(db, e, recent)
        _make_warn_event(db, e, recent)  # also add WARN to confirm they're excluded

    db.flush()
    result = summarize_all_industries(event_type="bankruptcy", db=db, min_pe_entities=10)
    sector = next((r for r in result if r["industry"] == "52"), None)
    assert sector is not None
    assert sector["pe_events"] == 12  # 12 bankruptcy events, not 24


def test_summarize_all_industries_row_keys(db):
    """Each summary row must contain the expected keys."""
    _make_sector(db, "31", pe_count=12, non_pe_count=20)
    db.flush()
    result = summarize_all_industries(db=db, min_pe_entities=10)
    required_keys = {
        "industry",
        "pe_count",
        "non_pe_count",
        "pe_events",
        "non_pe_events",
        "pe_rate",
        "non_pe_rate",
        "rate_ratio",
        "p_value",
        "lookback_years",
        "event_type",
    }
    for row in result:
        assert required_keys.issubset(row.keys())


# ---------------------------------------------------------------------------
# Helper internals
# ---------------------------------------------------------------------------


def test_count_events_empty_ids(db):
    """Empty entity_ids set should return 0 without error."""
    today = date.today()
    count = _count_events(db, set(), "warn", "warn_notice", today - timedelta(days=365))
    assert count == 0


def test_entity_ids_with_events_empty_ids(db):
    """Empty entity_ids should return empty set without error."""
    today = date.today()
    result = _entity_ids_with_events(db, set(), "warn", "warn_notice", today - timedelta(days=365))
    assert result == set()


def test_entity_ids_with_events_correct_filter(db):
    """Only entities with qualifying events should be returned."""
    today = date.today()
    since = today - timedelta(days=365)
    e1 = _make_entity(db, "Corp A", "62")
    e2 = _make_entity(db, "Corp B", "62")
    _make_warn_event(db, e1, today - timedelta(days=60))  # qualifies
    # e2 has no events
    db.flush()
    result = _entity_ids_with_events(db, {e1.id, e2.id}, "warn", "warn_notice", since)
    assert e1.id in result
    assert e2.id not in result


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------


def test_compute_pe_warn_rate_performance(db):
    """compute_pe_warn_rate must complete within 500 ms for 200 entities."""
    today = date.today()
    recent = today - timedelta(days=60)

    pe_entities = [_make_entity(db, f"PE Corp {i}", "62") for i in range(100)]
    [_make_entity(db, f"Public Corp {i}", "62") for i in range(100)]
    for e in pe_entities:
        _flag_pe(db, e)
        _make_warn_event(db, e, recent)
    db.flush()

    start = time.perf_counter()
    compute_pe_warn_rate("62", db=db)
    elapsed = (time.perf_counter() - start) * 1000
    assert elapsed < 500, f"compute_pe_warn_rate took {elapsed:.1f} ms"
