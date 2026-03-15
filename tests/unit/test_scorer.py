"""
Unit tests for M13 — Alert Scoring Engine.

Covers:
- Score composition with all components present
- Score composition with missing/partial components (graceful degradation)
- Score clamping to [0.0, 1.0]
- alert_level assignment across all thresholds (None, watch, elevated, critical)
- Upsert: calling compute_entity_score twice for same entity/date updates in place
- Alert generation: only fires when level increases
- Alert generation: no alert when level unchanged or decreases
- Alert dataclass is self-contained (all fields populated)
- generate_alert returns None for no-threshold crossing
- get_prior_score: returns most recent score before date, or None
- run_daily_scoring: scores all entities with signals, commits once, skips failures
- Performance
"""

from __future__ import annotations

import time
import uuid
from datetime import date, timedelta
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cam.alerts.scorer import (
    ALERT_THRESHOLDS,
    COMPONENT_WEIGHTS,
    Alert,
    _get_component_scores,
    _get_top_evidence,
    _latest_signal_score,
    _level_increased,
    _score_to_level,
    compute_entity_score,
    generate_alert,
    get_prior_score,
    run_daily_scoring,
)
from cam.db.models import AlertScore, Base, Entity, Signal

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


# ---------------------------------------------------------------------------
# Helpers: synthetic data builders
# ---------------------------------------------------------------------------


def _make_entity(db: Session, name: str = "Test Corp") -> Entity:
    e = Entity(id=uuid.uuid4(), canonical_name=name, naics_code="62")
    db.add(e)
    db.flush()
    return e


def _make_signal(
    db: Session,
    entity: Entity,
    signal_type: str,
    score: float,
    evidence: str = "",
) -> Signal:
    sig = Signal(
        entity_id=entity.id,
        source="test",
        signal_type=signal_type,
        score=score,
        evidence=evidence or f"Evidence for {signal_type}",
    )
    db.add(sig)
    db.flush()
    return sig


def _make_alert_score(
    db: Session,
    entity: Entity,
    score_date: date,
    composite_score: float,
    alert_level: str | None = None,
    component_scores: dict | None = None,
) -> AlertScore:
    as_ = AlertScore(
        entity_id=entity.id,
        score_date=score_date,
        composite_score=composite_score,
        alert_level=alert_level or _score_to_level(composite_score),
        component_scores=component_scores or {},
    )
    db.add(as_)
    db.flush()
    return as_


# ---------------------------------------------------------------------------
# _score_to_level
# ---------------------------------------------------------------------------


def test_score_to_level_none_below_watch():
    assert _score_to_level(0.0) is None
    assert _score_to_level(0.39) is None


def test_score_to_level_watch():
    assert _score_to_level(0.40) == "watch"
    assert _score_to_level(0.64) == "watch"


def test_score_to_level_elevated():
    assert _score_to_level(0.65) == "elevated"
    assert _score_to_level(0.79) == "elevated"


def test_score_to_level_critical():
    assert _score_to_level(0.80) == "critical"
    assert _score_to_level(1.0) == "critical"


# ---------------------------------------------------------------------------
# _level_increased
# ---------------------------------------------------------------------------


def test_level_increased_none_to_watch():
    assert _level_increased(None, "watch") is True


def test_level_increased_none_to_critical():
    assert _level_increased(None, "critical") is True


def test_level_increased_watch_to_elevated():
    assert _level_increased("watch", "elevated") is True


def test_level_increased_elevated_to_critical():
    assert _level_increased("elevated", "critical") is True


def test_level_not_increased_same_level():
    assert _level_increased("watch", "watch") is False
    assert _level_increased("elevated", "elevated") is False
    assert _level_increased("critical", "critical") is False


def test_level_not_increased_none_to_none():
    assert _level_increased(None, None) is False


def test_level_not_increased_downgrade():
    assert _level_increased("elevated", "watch") is False
    assert _level_increased("critical", "elevated") is False


# ---------------------------------------------------------------------------
# _latest_signal_score
# ---------------------------------------------------------------------------


def test_latest_signal_score_returns_none_when_absent(db):
    entity = _make_entity(db)
    result = _latest_signal_score(db, entity.id, "cross_agency_composite")
    assert result is None


def test_latest_signal_score_returns_score(db):
    entity = _make_entity(db)
    _make_signal(db, entity, "cross_agency_composite", 0.75)
    result = _latest_signal_score(db, entity.id, "cross_agency_composite")
    assert result == pytest.approx(0.75)


def test_latest_signal_score_returns_most_recent(db):
    """When multiple signals exist, the one with the latest signal_date wins."""
    entity = _make_entity(db)
    older_date = date.today() - timedelta(days=30)
    newer_date = date.today() - timedelta(days=1)
    # Older signal (0.30)
    sig1 = Signal(
        entity_id=entity.id,
        source="test",
        signal_type="risk_language_expansion",
        score=0.30,
        signal_date=older_date,
    )
    # Newer signal (0.80)
    sig2 = Signal(
        entity_id=entity.id,
        source="test",
        signal_type="risk_language_expansion",
        score=0.80,
        signal_date=newer_date,
    )
    db.add_all([sig1, sig2])
    db.flush()
    result = _latest_signal_score(db, entity.id, "risk_language_expansion")
    assert result == pytest.approx(0.80)


def test_latest_signal_score_ignores_null_score(db):
    entity = _make_entity(db)
    sig = Signal(
        entity_id=entity.id,
        source="test",
        signal_type="earnings_divergence",
        score=None,
        evidence="no score",
    )
    db.add(sig)
    db.flush()
    result = _latest_signal_score(db, entity.id, "earnings_divergence")
    assert result is None


# ---------------------------------------------------------------------------
# _get_component_scores
# ---------------------------------------------------------------------------


def test_get_component_scores_all_zero_when_no_signals(db):
    entity = _make_entity(db)
    scores = _get_component_scores(db, entity.id)
    assert set(scores.keys()) == set(COMPONENT_WEIGHTS.keys())
    assert all(v == 0.0 for v in scores.values())


def test_get_component_scores_reads_pe_owned_for_pe_warn_flag(db):
    """pe_warn_flag component reads from 'pe_owned' signal_type."""
    entity = _make_entity(db)
    _make_signal(db, entity, "pe_owned", 1.0)
    scores = _get_component_scores(db, entity.id)
    assert scores["pe_warn_flag"] == pytest.approx(1.0)


def test_get_component_scores_partial_components(db):
    """Missing components default to 0.0; present ones are read correctly."""
    entity = _make_entity(db)
    _make_signal(db, entity, "cross_agency_composite", 0.6)
    _make_signal(db, entity, "risk_language_expansion", 0.4)
    scores = _get_component_scores(db, entity.id)
    assert scores["cross_agency_composite"] == pytest.approx(0.6)
    assert scores["risk_language_expansion"] == pytest.approx(0.4)
    assert scores["earnings_divergence"] == 0.0
    assert scores["proxy_escalation"] == 0.0
    assert scores["merger_vertical_risk"] == 0.0
    assert scores["pe_warn_flag"] == 0.0


# ---------------------------------------------------------------------------
# compute_entity_score
# ---------------------------------------------------------------------------


def test_compute_entity_score_zero_signals(db):
    """No signals → composite score is 0.0, alert_level is None."""
    entity = _make_entity(db, "No Signal Corp")
    result = compute_entity_score(entity.id, date.today(), db=db)
    assert isinstance(result, AlertScore)
    assert result.composite_score == 0.0
    assert result.alert_level is None


def test_compute_entity_score_full_components(db):
    """All six components at max score → composite = 1.0."""
    entity = _make_entity(db, "Max Score Corp")
    for sig_type in [
        "cross_agency_composite",
        "risk_language_expansion",
        "earnings_divergence",
        "proxy_escalation",
        "merger_vertical_risk",
        "pe_owned",  # maps to pe_warn_flag
    ]:
        _make_signal(db, entity, sig_type, 1.0)

    result = compute_entity_score(entity.id, date.today(), db=db)
    assert result.composite_score == pytest.approx(1.0)
    assert result.alert_level == "critical"


def test_compute_entity_score_watch_level(db):
    """Score that crosses watch (0.40) but not elevated (0.65)."""
    entity = _make_entity(db)
    # cross_agency_composite (weight 0.35) at score 1.2 → 0.35 * 1.0 = 0.35... needs more
    # Use cross_agency_composite=1.0 (0.35) + risk_language_expansion=0.3 (0.06) = 0.41 → watch
    _make_signal(db, entity, "cross_agency_composite", 1.0)
    _make_signal(db, entity, "risk_language_expansion", 0.30)
    result = compute_entity_score(entity.id, date.today(), db=db)
    # 0.35 * 1.0 + 0.20 * 0.30 = 0.35 + 0.06 = 0.41
    assert result.composite_score == pytest.approx(0.41, abs=0.01)
    assert result.alert_level == "watch"


def test_compute_entity_score_elevated_level(db):
    """Score that crosses elevated (0.65)."""
    entity = _make_entity(db)
    # cross_agency (0.35) + risk_lang (0.20) + earnings (0.15) = 0.70 → elevated
    _make_signal(db, entity, "cross_agency_composite", 1.0)
    _make_signal(db, entity, "risk_language_expansion", 1.0)
    _make_signal(db, entity, "earnings_divergence", 1.0)
    result = compute_entity_score(entity.id, date.today(), db=db)
    # 0.35 + 0.20 + 0.15 = 0.70
    assert result.composite_score == pytest.approx(0.70, abs=0.01)
    assert result.alert_level == "elevated"


def test_compute_entity_score_critical_level(db):
    """Score crossing critical (0.80)."""
    entity = _make_entity(db)
    _make_signal(db, entity, "cross_agency_composite", 1.0)
    _make_signal(db, entity, "risk_language_expansion", 1.0)
    _make_signal(db, entity, "earnings_divergence", 1.0)
    _make_signal(db, entity, "proxy_escalation", 1.0)
    result = compute_entity_score(entity.id, date.today(), db=db)
    # 0.35 + 0.20 + 0.15 + 0.15 = 0.85
    assert result.composite_score == pytest.approx(0.85, abs=0.01)
    assert result.alert_level == "critical"


def test_compute_entity_score_score_clamped_to_one(db):
    """Score can never exceed 1.0 even with all components at maximum."""
    entity = _make_entity(db)
    for sig_type in [
        "cross_agency_composite",
        "risk_language_expansion",
        "earnings_divergence",
        "proxy_escalation",
        "merger_vertical_risk",
        "pe_owned",
    ]:
        _make_signal(db, entity, sig_type, 2.0)  # artificially above 1.0

    result = compute_entity_score(entity.id, date.today(), db=db)
    assert result.composite_score <= 1.0


def test_compute_entity_score_score_clamped_to_zero(db):
    """Score can never go below 0.0."""
    entity = _make_entity(db)
    _make_signal(db, entity, "cross_agency_composite", -5.0)  # negative signal
    result = compute_entity_score(entity.id, date.today(), db=db)
    assert result.composite_score >= 0.0


def test_compute_entity_score_upsert_updates_existing(db):
    """Calling compute_entity_score twice for same entity/date upserts."""
    entity = _make_entity(db)
    today = date.today()

    # First call
    _make_signal(db, entity, "cross_agency_composite", 0.5)
    score1 = compute_entity_score(entity.id, today, db=db)
    first_id = score1.id

    # Upgrade the signal and re-score same date
    _make_signal(db, entity, "risk_language_expansion", 1.0)
    score2 = compute_entity_score(entity.id, today, db=db)

    assert score2.id == first_id  # same row updated
    # count AlertScores for this entity/date
    from sqlalchemy import func

    count = db.execute(
        __import__("sqlalchemy", fromlist=["select"])
        .select(func.count())
        .select_from(AlertScore)
        .where(AlertScore.entity_id == entity.id, AlertScore.score_date == today)
    ).scalar_one()
    assert count == 1


def test_compute_entity_score_component_scores_stored(db):
    """component_scores dict is stored on the AlertScore record."""
    entity = _make_entity(db)
    _make_signal(db, entity, "cross_agency_composite", 0.6)
    result = compute_entity_score(entity.id, date.today(), db=db)
    assert isinstance(result.component_scores, dict)
    assert "cross_agency_composite" in result.component_scores
    assert result.component_scores["cross_agency_composite"] == pytest.approx(0.6)


def test_compute_entity_score_graceful_degradation(db):
    """Only one component present → other components contribute 0; score still computed."""
    entity = _make_entity(db)
    _make_signal(db, entity, "merger_vertical_risk", 1.0)
    result = compute_entity_score(entity.id, date.today(), db=db)
    # merger_vertical_risk weight is 0.10 → composite = 0.10
    assert result.composite_score == pytest.approx(0.10, abs=0.001)
    assert result.component_scores["merger_vertical_risk"] == pytest.approx(1.0)
    assert result.component_scores["cross_agency_composite"] == 0.0


# ---------------------------------------------------------------------------
# get_prior_score
# ---------------------------------------------------------------------------


def test_get_prior_score_returns_none_when_no_history(db):
    entity = _make_entity(db)
    result = get_prior_score(entity.id, date.today(), db=db)
    assert result is None


def test_get_prior_score_returns_most_recent_before_date(db):
    entity = _make_entity(db)
    today = date.today()
    yesterday = today - timedelta(days=1)
    two_days_ago = today - timedelta(days=2)

    _make_alert_score(db, entity, two_days_ago, 0.3)
    _make_alert_score(db, entity, yesterday, 0.5)
    # today's score exists but should NOT be returned (before_date is today)

    result = get_prior_score(entity.id, today, db=db)
    assert result is not None
    assert result.score_date == yesterday
    assert result.composite_score == pytest.approx(0.5)


def test_get_prior_score_excludes_same_date(db):
    """Scores on or after before_date must not be returned."""
    entity = _make_entity(db)
    today = date.today()
    _make_alert_score(db, entity, today, 0.7)

    result = get_prior_score(entity.id, today, db=db)
    assert result is None


# ---------------------------------------------------------------------------
# generate_alert
# ---------------------------------------------------------------------------


def test_generate_alert_returns_none_when_no_prior_and_score_below_watch(db):
    entity = _make_entity(db)
    score = _make_alert_score(db, entity, date.today(), 0.2)  # below watch
    result = generate_alert(entity.id, score, None, db=db)
    assert result is None


def test_generate_alert_fires_when_crossing_watch_for_first_time(db):
    entity = _make_entity(db, "Watch Corp")
    score = _make_alert_score(db, entity, date.today(), 0.45, alert_level="watch")
    result = generate_alert(entity.id, score, None, db=db)
    assert result is not None
    assert isinstance(result, Alert)
    assert result.alert_level == "watch"
    assert result.prior_score is None
    assert result.threshold_crossed == "watch"


def test_generate_alert_fires_watch_to_elevated(db):
    entity = _make_entity(db, "Rising Corp")
    yesterday = date.today() - timedelta(days=1)
    prior = _make_alert_score(db, entity, yesterday, 0.45, alert_level="watch")
    score = _make_alert_score(db, entity, date.today(), 0.70, alert_level="elevated")
    result = generate_alert(entity.id, score, prior, db=db)
    assert result is not None
    assert result.alert_level == "elevated"
    assert result.prior_score == pytest.approx(0.45)


def test_generate_alert_fires_elevated_to_critical(db):
    entity = _make_entity(db, "Critical Corp")
    yesterday = date.today() - timedelta(days=1)
    prior = _make_alert_score(db, entity, yesterday, 0.70, alert_level="elevated")
    score = _make_alert_score(db, entity, date.today(), 0.85, alert_level="critical")
    result = generate_alert(entity.id, score, prior, db=db)
    assert result is not None
    assert result.alert_level == "critical"


def test_generate_alert_returns_none_same_level(db):
    """Score stays at watch → no new alert."""
    entity = _make_entity(db)
    yesterday = date.today() - timedelta(days=1)
    prior = _make_alert_score(db, entity, yesterday, 0.42, alert_level="watch")
    score = _make_alert_score(db, entity, date.today(), 0.48, alert_level="watch")
    result = generate_alert(entity.id, score, prior, db=db)
    assert result is None


def test_generate_alert_returns_none_on_downgrade(db):
    """Score drops from critical to elevated → no new alert."""
    entity = _make_entity(db)
    yesterday = date.today() - timedelta(days=1)
    prior = _make_alert_score(db, entity, yesterday, 0.82, alert_level="critical")
    score = _make_alert_score(db, entity, date.today(), 0.70, alert_level="elevated")
    result = generate_alert(entity.id, score, prior, db=db)
    assert result is None


def test_generate_alert_self_contained(db):
    """Alert record must contain all actionable fields without further DB queries."""
    entity = _make_entity(db, "Self Contained Corp")
    _make_signal(db, entity, "cross_agency_composite", 0.8, "Multiple OSHA violations")
    score = _make_alert_score(
        db,
        entity,
        date.today(),
        0.85,
        alert_level="critical",
        component_scores={"cross_agency_composite": 0.8},
    )
    alert = generate_alert(entity.id, score, None, db=db)
    assert alert is not None
    assert alert.canonical_name == "Self Contained Corp"
    assert alert.score == pytest.approx(0.85)
    assert alert.score_date == date.today()
    assert alert.suggested_action  # non-empty
    assert isinstance(alert.relevant_regulatory_body, list)
    assert isinstance(alert.component_breakdown, dict)


def test_generate_alert_critical_includes_regulatory_bodies(db):
    entity = _make_entity(db, "Regulatory Corp")
    score = _make_alert_score(db, entity, date.today(), 0.85, alert_level="critical")
    alert = generate_alert(entity.id, score, None, db=db)
    assert alert is not None
    assert "DOJ" in alert.relevant_regulatory_body
    assert "FTC" in alert.relevant_regulatory_body


def test_generate_alert_watch_has_empty_regulatory_bodies(db):
    entity = _make_entity(db, "Watch Entity")
    score = _make_alert_score(db, entity, date.today(), 0.45, alert_level="watch")
    alert = generate_alert(entity.id, score, None, db=db)
    assert alert is not None
    assert alert.relevant_regulatory_body == []


def test_generate_alert_unknown_entity_uses_id_as_name(db):
    """If entity is not in the DB, Alert.canonical_name falls back to str(entity_id)."""
    phantom_id = uuid.uuid4()
    score = AlertScore(
        entity_id=phantom_id,
        score_date=date.today(),
        composite_score=0.45,
        alert_level="watch",
        component_scores={},
    )
    db.add(score)
    db.flush()
    alert = generate_alert(phantom_id, score, None, db=db)
    assert alert is not None
    assert alert.canonical_name == str(phantom_id)


# ---------------------------------------------------------------------------
# _get_top_evidence
# ---------------------------------------------------------------------------


def test_get_top_evidence_returns_empty_for_zero_scores(db):
    entity = _make_entity(db)
    result = _get_top_evidence(db, entity.id, {"cross_agency_composite": 0.0})
    assert result == []


def test_get_top_evidence_returns_highest_contributing(db):
    entity = _make_entity(db)
    _make_signal(db, entity, "cross_agency_composite", 0.8, "OSHA violation cluster")
    _make_signal(db, entity, "risk_language_expansion", 0.9, "Risk language spike")
    breakdown = {"cross_agency_composite": 0.8, "risk_language_expansion": 0.9}
    result = _get_top_evidence(db, entity.id, breakdown)
    assert len(result) >= 1
    # risk_language (weight=0.20 * score=0.9 = 0.18) < cross_agency (0.35 * 0.8 = 0.28)
    # so cross_agency should come first
    assert "OSHA" in result[0]


def test_get_top_evidence_capped_at_max_items(db):
    entity = _make_entity(db)
    all_signals = [
        ("cross_agency_composite", 0.9, "Evidence A"),
        ("risk_language_expansion", 0.8, "Evidence B"),
        ("earnings_divergence", 0.7, "Evidence C"),
        ("proxy_escalation", 0.6, "Evidence D"),
        ("merger_vertical_risk", 0.5, "Evidence E"),
        ("pe_owned", 0.4, "Evidence F"),
    ]
    breakdown = {}
    for sig_type, score, ev in all_signals:
        _make_signal(db, entity, sig_type, score, ev)
        comp_key = "pe_warn_flag" if sig_type == "pe_owned" else sig_type
        breakdown[comp_key] = score
    result = _get_top_evidence(db, entity.id, breakdown, max_items=3)
    assert len(result) <= 3


# ---------------------------------------------------------------------------
# run_daily_scoring
# ---------------------------------------------------------------------------


def test_run_daily_scoring_empty_db(db):
    """No signals → empty list returned."""
    result = run_daily_scoring(db=db)
    assert result == []


def test_run_daily_scoring_scores_entities_with_signals(db):
    """Entities with signals are scored; entities without are skipped."""
    e1 = _make_entity(db, "Corp A")
    e2 = _make_entity(db, "Corp B")
    _make_entity(db, "Corp C")  # no signals — should not appear in results

    _make_signal(db, e1, "cross_agency_composite", 0.5)
    _make_signal(db, e2, "risk_language_expansion", 0.4)
    db.flush()

    results = run_daily_scoring(db=db)
    scored_ids = {r.entity_id for r in results}
    assert e1.id in scored_ids
    assert e2.id in scored_ids


def test_run_daily_scoring_writes_to_alert_scores(db):
    """After run_daily_scoring, AlertScore rows exist in the DB."""
    entity = _make_entity(db)
    _make_signal(db, entity, "cross_agency_composite", 0.6)
    db.flush()

    today = date.today()
    run_daily_scoring(score_date=today, db=db)

    rows = db.query(AlertScore).filter_by(entity_id=entity.id, score_date=today).all()
    assert len(rows) == 1
    assert rows[0].composite_score > 0.0


def test_run_daily_scoring_idempotent(db):
    """Running twice on same date produces exactly one AlertScore row per entity."""
    entity = _make_entity(db)
    _make_signal(db, entity, "cross_agency_composite", 0.5)
    today = date.today()

    run_daily_scoring(score_date=today, db=db)
    run_daily_scoring(score_date=today, db=db)

    count = db.query(AlertScore).filter_by(entity_id=entity.id, score_date=today).count()
    assert count == 1


def test_run_daily_scoring_uses_today_by_default(db):
    entity = _make_entity(db)
    _make_signal(db, entity, "cross_agency_composite", 0.4)

    results = run_daily_scoring(db=db)
    assert all(r.score_date == date.today() for r in results)


def test_run_daily_scoring_skips_failed_entity(db):
    """If one entity fails to score, the rest still succeed."""
    e1 = _make_entity(db, "Good Corp")
    e2 = _make_entity(db, "Bad Corp")

    _make_signal(db, e1, "cross_agency_composite", 0.5)
    _make_signal(db, e2, "cross_agency_composite", 0.6)
    db.flush()

    # Patch compute_entity_score to raise for one specific entity
    original_fn = __import__(
        "cam.alerts.scorer", fromlist=["compute_entity_score"]
    ).compute_entity_score

    call_count = [0]

    def patched(entity_id, score_date, *, db):
        call_count[0] += 1
        if entity_id == e2.id:
            raise RuntimeError("Simulated failure")
        return original_fn(entity_id, score_date, db=db)

    with patch("cam.alerts.scorer.compute_entity_score", side_effect=patched):
        results = run_daily_scoring(db=db)

    # Only e1 should succeed
    scored_ids = {r.entity_id for r in results}
    assert e1.id in scored_ids
    assert e2.id not in scored_ids


# ---------------------------------------------------------------------------
# Alert dataclass
# ---------------------------------------------------------------------------


def test_alert_dataclass_defaults():
    alert = Alert(
        entity_id=uuid.uuid4(),
        canonical_name="Test",
        alert_level="watch",
        score=0.45,
        score_date=date.today(),
        prior_score=None,
        threshold_crossed="watch",
        component_breakdown={},
    )
    assert alert.top_evidence == []
    assert alert.suggested_action == ""
    assert alert.relevant_regulatory_body == []


# ---------------------------------------------------------------------------
# COMPONENT_WEIGHTS sum to 1.0
# ---------------------------------------------------------------------------


def test_component_weights_sum_to_one():
    total = sum(COMPONENT_WEIGHTS.values())
    assert total == pytest.approx(1.0, abs=1e-9)


# ---------------------------------------------------------------------------
# ALERT_THRESHOLDS are correctly ordered
# ---------------------------------------------------------------------------


def test_alert_thresholds_ordering():
    assert ALERT_THRESHOLDS["watch"] < ALERT_THRESHOLDS["elevated"]
    assert ALERT_THRESHOLDS["elevated"] < ALERT_THRESHOLDS["critical"]


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------


def test_run_daily_scoring_performance(db):
    """run_daily_scoring must complete in < 1 s for 50 entities with 3 signals each."""
    for i in range(50):
        e = _make_entity(db, f"Perf Corp {i}")
        _make_signal(db, e, "cross_agency_composite", 0.5)
        _make_signal(db, e, "risk_language_expansion", 0.3)
        _make_signal(db, e, "pe_owned", 1.0)
    db.flush()

    start = time.perf_counter()
    run_daily_scoring(db=db)
    elapsed = (time.perf_counter() - start) * 1000
    assert elapsed < 1000, f"run_daily_scoring took {elapsed:.0f} ms for 50 entities"
