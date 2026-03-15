"""
Unit tests for M14 — Output Layer.

Covers:
- export_static_site: writes all required JSON data files (meta, alerts, entities, per-entity)
- All JSON files are valid and parseable
- meta.json has correct entity_count and alert_count
- alerts.json sorted correctly: critical → elevated → watch, then date descending
- alerts.json excludes entities with no alert level (score < watch threshold)
- entities.json contains all entities, including those with no current score
- per-entity JSON is fully self-contained (all required fields present)
- score_history included in per-entity JSON (most recent first)
- top_evidence included in per-entity JSON
- entities without any alert score get null current_score
- return value structure: {entities, alerts, files_written}
- Stale entity files are removed on re-export
- Idempotency: second export overwrites cleanly, same counts
- export_digest: includes elevated/critical alerts on or after since_date
- export_digest: excludes alerts before since_date
- export_digest: excludes watch/below-watch entities
- export_digest: returns non-empty string with correct header
- export_digest: includes signal evidence snippets per alert entity
- export_digest: sector summary appears when enough entities per NAICS
- Performance: 500 entities export in < 10 s (validates scale headroom)
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import date, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cam.db.models import AlertScore, Base, Entity, Signal
from cam.output.exporter import export_digest, export_static_site

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


def _make_entity(
    db: Session,
    name: str = "Test Corp",
    naics_code: str = "62",
    ticker: str | None = None,
) -> Entity:
    e = Entity(id=uuid.uuid4(), canonical_name=name, naics_code=naics_code, ticker=ticker)
    db.add(e)
    db.flush()
    return e


def _make_score(
    db: Session,
    entity: Entity,
    composite_score: float,
    alert_level: str | None,
    score_date: date | None = None,
    component_scores: dict | None = None,
) -> AlertScore:
    as_ = AlertScore(
        entity_id=entity.id,
        score_date=score_date or date.today(),
        composite_score=composite_score,
        alert_level=alert_level,
        component_scores=component_scores or {"cross_agency_composite": composite_score},
    )
    db.add(as_)
    db.flush()
    return as_


def _make_signal(
    db: Session,
    entity: Entity,
    signal_type: str = "cross_agency_composite",
    score: float = 0.8,
    evidence: str = "Test evidence text",
    signal_date: date | None = None,
) -> Signal:
    sig = Signal(
        entity_id=entity.id,
        source="test",
        signal_type=signal_type,
        score=score,
        evidence=evidence,
        signal_date=signal_date,
    )
    db.add(sig)
    db.flush()
    return sig


# ---------------------------------------------------------------------------
# export_static_site — file creation
# ---------------------------------------------------------------------------


def test_export_writes_required_files(db, tmp_path):
    """All required JSON data files are present after a successful export."""
    entity = _make_entity(db)
    _make_score(db, entity, 0.85, "critical")
    db.commit()

    export_static_site(tmp_path, db=db)

    # JSON data files only (no .js or HTML in the new architecture)
    assert (tmp_path / "meta.json").exists()
    assert (tmp_path / "alerts.json").exists()
    assert (tmp_path / "entities.json").exists()
    assert (tmp_path / "entities" / f"{entity.id}.json").exists()

    # Confirm no .js or .html files are written
    assert not (tmp_path / "meta.js").exists()
    assert not (tmp_path / "index.html").exists()


def test_all_json_files_are_valid(db, tmp_path):
    """Every JSON file produced must be parseable."""
    entity = _make_entity(db)
    _make_score(db, entity, 0.85, "critical")
    db.commit()

    export_static_site(tmp_path, db=db)

    for jf in [
        tmp_path / "meta.json",
        tmp_path / "alerts.json",
        tmp_path / "entities.json",
        tmp_path / "entities" / f"{entity.id}.json",
    ]:
        data = json.loads(jf.read_text())
        assert data is not None


# ---------------------------------------------------------------------------
# meta.json
# ---------------------------------------------------------------------------


def test_meta_correct_entity_count(db, tmp_path):
    e1 = _make_entity(db, "Corp A")
    e2 = _make_entity(db, "Corp B")
    _make_score(db, e1, 0.85, "critical")
    _make_score(db, e2, 0.45, "watch")
    db.commit()

    export_static_site(tmp_path, db=db)

    meta = json.loads((tmp_path / "meta.json").read_text())
    assert meta["entity_count"] == 2


def test_meta_correct_alert_count(db, tmp_path):
    """alert_count reflects entities with a non-None alert_level."""
    e1 = _make_entity(db, "Watch")
    e2 = _make_entity(db, "Critical")
    e3 = _make_entity(db, "No Level")
    _make_score(db, e1, 0.45, "watch")
    _make_score(db, e2, 0.85, "critical")
    _make_score(db, e3, 0.10, None)
    db.commit()

    export_static_site(tmp_path, db=db)

    meta = json.loads((tmp_path / "meta.json").read_text())
    assert meta["alert_count"] == 2  # watch + critical; None excluded


def test_meta_has_required_fields(db, tmp_path):
    db.commit()
    export_static_site(tmp_path, db=db)

    meta = json.loads((tmp_path / "meta.json").read_text())
    assert "exported_at" in meta
    assert "entity_count" in meta
    assert "alert_count" in meta
    assert meta["version"] == "1"


def test_meta_empty_db(db, tmp_path):
    db.commit()
    export_static_site(tmp_path, db=db)

    meta = json.loads((tmp_path / "meta.json").read_text())
    assert meta["entity_count"] == 0
    assert meta["alert_count"] == 0


# ---------------------------------------------------------------------------
# alerts.json — sort order
# ---------------------------------------------------------------------------


def test_alerts_sorted_by_level(db, tmp_path):
    """Critical appears before elevated, elevated before watch."""
    e1 = _make_entity(db, "Watch Corp")
    e2 = _make_entity(db, "Critical Corp")
    e3 = _make_entity(db, "Elevated Corp")
    _make_score(db, e1, 0.45, "watch")
    _make_score(db, e2, 0.85, "critical")
    _make_score(db, e3, 0.70, "elevated")
    db.commit()

    export_static_site(tmp_path, db=db)

    alerts = json.loads((tmp_path / "alerts.json").read_text())
    levels = [a["alert_level"] for a in alerts]
    assert levels == ["critical", "elevated", "watch"]


def test_alerts_sorted_by_date_desc_within_level(db, tmp_path):
    """Within the same alert level, more recent date appears first."""
    e1 = _make_entity(db, "Old Critical")
    e2 = _make_entity(db, "New Critical")
    _make_score(db, e1, 0.85, "critical", date.today() - timedelta(days=5))
    _make_score(db, e2, 0.82, "critical", date.today())
    db.commit()

    export_static_site(tmp_path, db=db)

    alerts = json.loads((tmp_path / "alerts.json").read_text())
    assert alerts[0]["canonical_name"] == "New Critical"
    assert alerts[1]["canonical_name"] == "Old Critical"


def test_alerts_excludes_below_watch(db, tmp_path):
    """Entities with alert_level=None are excluded from alerts.json."""
    e1 = _make_entity(db, "Below Watch")
    e2 = _make_entity(db, "Watch Corp")
    _make_score(db, e1, 0.10, None)
    _make_score(db, e2, 0.45, "watch")
    db.commit()

    export_static_site(tmp_path, db=db)

    alerts = json.loads((tmp_path / "alerts.json").read_text())
    names = [a["canonical_name"] for a in alerts]
    assert "Below Watch" not in names
    assert "Watch Corp" in names


def test_alerts_contains_expected_fields(db, tmp_path):
    """Each alert record exposes all required fields."""
    entity = _make_entity(db, "Annotated Corp", naics_code="52")
    _make_score(db, entity, 0.85, "critical")
    db.commit()

    export_static_site(tmp_path, db=db)

    alerts = json.loads((tmp_path / "alerts.json").read_text())
    assert len(alerts) == 1
    a = alerts[0]
    assert a["entity_id"] == str(entity.id)
    assert a["canonical_name"] == "Annotated Corp"
    assert a["alert_level"] == "critical"
    assert a["composite_score"] == pytest.approx(0.85)
    assert a["naics_code"] == "52"
    assert "score_date" in a
    assert "component_scores" in a


# ---------------------------------------------------------------------------
# entities.json
# ---------------------------------------------------------------------------


def test_entities_json_contains_all_entities(db, tmp_path):
    """entities.json includes every entity even if it has no score."""
    e1 = _make_entity(db, "Scored")
    e2 = _make_entity(db, "Unscored")
    _make_score(db, e1, 0.5, "watch")
    db.commit()

    export_static_site(tmp_path, db=db)

    entities = json.loads((tmp_path / "entities.json").read_text())
    ids = {e["id"] for e in entities}
    assert str(e1.id) in ids
    assert str(e2.id) in ids


def test_entities_json_fields(db, tmp_path):
    """Entity summary contains expected fields."""
    entity = _make_entity(db, "Field Check Corp", naics_code="44", ticker="FCC")
    _make_score(db, entity, 0.70, "elevated")
    db.commit()

    export_static_site(tmp_path, db=db)

    entities = json.loads((tmp_path / "entities.json").read_text())
    e = next(x for x in entities if x["id"] == str(entity.id))
    assert e["canonical_name"] == "Field Check Corp"
    assert e["naics_code"] == "44"
    assert e["ticker"] == "FCC"
    assert e["composite_score"] == pytest.approx(0.70)
    assert e["alert_level"] == "elevated"
    assert e["score_date"] is not None


def test_entities_json_unscored_entity_has_null_score(db, tmp_path):
    """An entity with no AlertScore has null composite_score and alert_level."""
    entity = _make_entity(db, "No Score Corp")
    db.commit()

    export_static_site(tmp_path, db=db)

    entities = json.loads((tmp_path / "entities.json").read_text())
    e = next(x for x in entities if x["id"] == str(entity.id))
    assert e["composite_score"] is None
    assert e["alert_level"] is None
    assert e["score_date"] is None


# ---------------------------------------------------------------------------
# Per-entity JSON (entities/{id}.json)
# ---------------------------------------------------------------------------


def test_entity_detail_self_contained(db, tmp_path):
    """Per-entity file contains all fields needed without further DB calls."""
    entity = _make_entity(db, "Self-Contained Corp", naics_code="52", ticker="SC")
    _make_score(db, entity, 0.85, "critical")
    _make_signal(db, entity, "cross_agency_composite", 0.9, "OSHA cluster in Q3")
    db.commit()

    export_static_site(tmp_path, db=db)

    detail = json.loads((tmp_path / "entities" / f"{entity.id}.json").read_text())
    assert detail["id"] == str(entity.id)
    assert detail["canonical_name"] == "Self-Contained Corp"
    assert detail["naics_code"] == "52"
    assert detail["ticker"] == "SC"
    assert detail["current_score"]["alert_level"] == "critical"
    assert detail["current_score"]["composite_score"] == pytest.approx(0.85)
    assert "component_scores" in detail["current_score"]
    assert isinstance(detail["score_history"], list)
    assert isinstance(detail["top_evidence"], list)


def test_entity_detail_no_score_has_null_current_score(db, tmp_path):
    """Entities with no AlertScore get current_score: null."""
    entity = _make_entity(db)
    db.commit()

    export_static_site(tmp_path, db=db)

    detail = json.loads((tmp_path / "entities" / f"{entity.id}.json").read_text())
    assert detail["current_score"] is None


def test_entity_detail_score_history_most_recent_first(db, tmp_path):
    """score_history is ordered most-recent first."""
    entity = _make_entity(db)
    today = date.today()
    for i in range(5):
        _make_score(db, entity, 0.4 + i * 0.05, "watch", today - timedelta(days=i))
    db.commit()

    export_static_site(tmp_path, db=db)

    detail = json.loads((tmp_path / "entities" / f"{entity.id}.json").read_text())
    history = detail["score_history"]
    assert len(history) == 5
    # Dates should be descending
    dates = [h["score_date"] for h in history]
    assert dates == sorted(dates, reverse=True)


def test_entity_detail_top_evidence_included(db, tmp_path):
    """top_evidence contains evidence from signals with a non-null score."""
    entity = _make_entity(db)
    _make_score(db, entity, 0.85, "critical")
    _make_signal(db, entity, "cross_agency_composite", 0.9, "OSHA cluster")
    _make_signal(db, entity, "risk_language_expansion", 0.7, "New risk language")
    db.commit()

    export_static_site(tmp_path, db=db)

    detail = json.loads((tmp_path / "entities" / f"{entity.id}.json").read_text())
    assert len(detail["top_evidence"]) == 2
    signal_types = {ev["signal_type"] for ev in detail["top_evidence"]}
    assert "cross_agency_composite" in signal_types


def test_entity_detail_evidence_capped_at_five(db, tmp_path):
    """At most 5 evidence items per entity."""
    entity = _make_entity(db)
    _make_score(db, entity, 0.85, "critical")
    for i in range(8):
        _make_signal(db, entity, f"sig_type_{i}", float(i) / 10, f"Evidence {i}")
    db.commit()

    export_static_site(tmp_path, db=db)

    detail = json.loads((tmp_path / "entities" / f"{entity.id}.json").read_text())
    assert len(detail["top_evidence"]) <= 5


def test_entity_detail_evidence_fields(db, tmp_path):
    """Each evidence item exposes signal_type, score, evidence, signal_date, document_url."""
    entity = _make_entity(db)
    _make_score(db, entity, 0.85, "critical")
    _make_signal(db, entity, "cross_agency_composite", 0.9, "OSHA cluster", date.today())
    db.commit()

    export_static_site(tmp_path, db=db)

    detail = json.loads((tmp_path / "entities" / f"{entity.id}.json").read_text())
    ev = detail["top_evidence"][0]
    assert "signal_type" in ev
    assert "score" in ev
    assert "evidence" in ev
    assert "signal_date" in ev
    assert "document_url" in ev


# ---------------------------------------------------------------------------
# Return value
# ---------------------------------------------------------------------------


def test_return_value_structure(db, tmp_path):
    """Return value has keys: entities, alerts, files_written."""
    entity = _make_entity(db)
    _make_score(db, entity, 0.85, "critical")
    db.commit()

    result = export_static_site(tmp_path, db=db)

    assert "entities" in result
    assert "alerts" in result
    assert "files_written" in result
    assert result["entities"] == 1
    assert result["alerts"] == 1


def test_return_value_files_written_count(db, tmp_path):
    """files_written = 3 top-level JSON + 1 per-entity JSON = 3 + N."""
    e1 = _make_entity(db, "A")
    _make_entity(db, "B")
    _make_score(db, e1, 0.85, "critical")
    db.commit()

    result = export_static_site(tmp_path, db=db)

    # 3 top-level JSON (meta + alerts + entities) + 2 entity JSON = 5
    assert result["files_written"] == 5


# ---------------------------------------------------------------------------
# Stale file cleanup
# ---------------------------------------------------------------------------


def test_stale_entity_files_removed(db, tmp_path):
    """Entity files injected from a previous run are deleted on re-export."""
    entity = _make_entity(db, "Stays")
    _make_score(db, entity, 0.85, "critical")
    db.commit()

    # First export — creates the expected entity files
    export_static_site(tmp_path, db=db)

    # Inject stale .json file simulating a previously-exported entity no longer in DB
    stale_id = uuid.uuid4()
    stale_json = tmp_path / "entities" / f"{stale_id}.json"
    stale_json.write_text("{}", encoding="utf-8")

    # Second export — must remove stale files
    export_static_site(tmp_path, db=db)

    assert not stale_json.exists(), "Stale .json file should have been removed"
    # Current entity's file must still exist
    assert (tmp_path / "entities" / f"{entity.id}.json").exists()


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_export_idempotent_same_counts(db, tmp_path):
    """Running export twice produces identical counts."""
    entity = _make_entity(db)
    _make_score(db, entity, 0.85, "critical")
    db.commit()

    r1 = export_static_site(tmp_path, db=db)
    r2 = export_static_site(tmp_path, db=db)

    assert r1["entities"] == r2["entities"]
    assert r1["alerts"] == r2["alerts"]


def test_export_idempotent_same_meta(db, tmp_path):
    """meta.json entity_count is identical on second run."""
    entity = _make_entity(db)
    _make_score(db, entity, 0.85, "critical")
    db.commit()

    export_static_site(tmp_path, db=db)
    meta1_count = json.loads((tmp_path / "meta.json").read_text())["entity_count"]

    export_static_site(tmp_path, db=db)
    meta2_count = json.loads((tmp_path / "meta.json").read_text())["entity_count"]

    assert meta1_count == meta2_count


def test_export_no_extra_entity_files(db, tmp_path):
    """Second run does not leave extra entity files when data is unchanged."""
    e1 = _make_entity(db, "Stays")
    _make_score(db, e1, 0.85, "critical")
    db.commit()

    export_static_site(tmp_path, db=db)
    first_files = set((tmp_path / "entities").iterdir())

    # Run again (data unchanged)
    export_static_site(tmp_path, db=db)
    second_files = set((tmp_path / "entities").iterdir())

    assert first_files == second_files


# ---------------------------------------------------------------------------
# export_digest
# ---------------------------------------------------------------------------


def test_digest_returns_string(db):
    result = export_digest(date.today() - timedelta(days=7), db=db)
    assert isinstance(result, str)
    assert len(result) > 0


def test_digest_has_header(db):
    result = export_digest(date.today() - timedelta(days=7), db=db)
    assert "Corporate Accountability Monitor" in result


def test_digest_includes_critical_and_elevated(db):
    """Critical and elevated entities appear in the digest body."""
    today = date.today()
    e_crit = _make_entity(db, "Critical Corp")
    e_elev = _make_entity(db, "Elevated Corp")
    _make_score(db, e_crit, 0.85, "critical", today)
    _make_score(db, e_elev, 0.70, "elevated", today)
    db.commit()

    result = export_digest(today - timedelta(days=7), db=db)

    assert "Critical Corp" in result
    assert "Elevated Corp" in result


def test_digest_excludes_watch_level(db):
    """Watch-level entities are not listed in the new-alerts section."""
    today = date.today()
    e_watch = _make_entity(db, "Watch Corp")
    _make_score(db, e_watch, 0.45, "watch", today)
    db.commit()

    result = export_digest(today - timedelta(days=7), db=db)

    # Watch Corp should not appear in the alerts section
    assert "Watch Corp" not in result


def test_digest_excludes_alerts_before_since_date(db):
    """Alerts with score_date < since_date are excluded."""
    today = date.today()
    old_date = today - timedelta(days=14)
    e_old = _make_entity(db, "Old Alert Corp")
    _make_score(db, e_old, 0.85, "critical", old_date)
    db.commit()

    # since_date is 7 days ago — old_date is 14 days ago → excluded
    result = export_digest(today - timedelta(days=7), db=db)

    assert "Old Alert Corp" not in result


def test_digest_includes_alerts_on_since_date(db):
    """Alerts on exactly since_date are included (>= comparison)."""
    since_date = date.today() - timedelta(days=7)
    entity = _make_entity(db, "On Boundary Corp")
    _make_score(db, entity, 0.85, "critical", since_date)
    db.commit()

    result = export_digest(since_date, db=db)

    assert "On Boundary Corp" in result


def test_digest_includes_evidence_snippets(db):
    """Digest alert lines include top signal evidence snippets."""
    today = date.today()
    entity = _make_entity(db, "Evidence Corp")
    _make_score(db, entity, 0.85, "critical", today)
    _make_signal(db, entity, "osha_cluster", 0.9, "Three fatality incidents in Q3")
    _make_signal(db, entity, "risk_language", 0.7, "Expanded risk disclosures in 10-K")
    db.commit()

    result = export_digest(today - timedelta(days=7), db=db)

    # Evidence snippets should appear somewhere in the digest for this entity
    assert "Evidence Corp" in result
    assert "osha_cluster" in result or "risk_language" in result


def test_digest_evidence_truncated_at_120_chars(db):
    """Evidence text is truncated to 120 characters in the digest."""
    today = date.today()
    entity = _make_entity(db, "Long Evidence Corp")
    _make_score(db, entity, 0.85, "critical", today)
    long_text = "X" * 200
    _make_signal(db, entity, "test_signal", 0.9, long_text)
    db.commit()

    result = export_digest(today - timedelta(days=7), db=db)

    # The full 200-char string should NOT appear verbatim
    assert long_text not in result
    # But the first 120 chars should be present
    assert "X" * 120 in result


def test_digest_sector_summary_appears(db):
    """Sector summary section is present."""
    result = export_digest(date.today() - timedelta(days=7), db=db)
    assert "SECTOR" in result.upper() or "TOP SECTORS" in result.upper()


def test_digest_sector_requires_min_entities(db):
    """Sectors with fewer than 3 entities are omitted from the sector summary."""
    today = date.today()
    # Only 2 entities in NAICS 99 — below the 3-entity minimum
    for i in range(2):
        e = _make_entity(db, f"Small Sector Corp {i}", naics_code="99")
        _make_score(db, e, 0.5, "watch", today)
    db.commit()

    result = export_digest(today - timedelta(days=7), db=db)

    assert "NAICS 99" not in result


def test_digest_sector_appears_when_sufficient(db):
    """A sector with >= 3 entities appears in the sector summary."""
    today = date.today()
    for i in range(4):
        e = _make_entity(db, f"Big Sector Corp {i}", naics_code="44")
        _make_score(db, e, 0.6, "elevated", today)
    db.commit()

    result = export_digest(today - timedelta(days=7), db=db)

    assert "NAICS 44" in result


def test_digest_empty_db_no_error(db):
    """Empty database produces a valid digest with zero-alert messaging."""
    result = export_digest(date.today() - timedelta(days=7), db=db)
    assert "No new critical or elevated alerts" in result


def test_digest_score_shown_in_alert_line(db):
    """Digest alert lines include the numeric score."""
    today = date.today()
    entity = _make_entity(db, "Scored Corp")
    _make_score(db, entity, 0.873, "critical", today)
    db.commit()

    result = export_digest(today - timedelta(days=7), db=db)

    assert "0.873" in result


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------


def test_export_performance_500_entities(db, tmp_path):
    """500 entities with signals export in < 10 s on any modern machine."""
    today = date.today()
    levels = ["watch", "elevated", "critical", None, "watch", None, "elevated"]
    for i in range(500):
        e = _make_entity(db, f"Perf Corp {i}", naics_code=str(10 + i % 20))
        _make_score(db, e, 0.3 + (i % 7) * 0.08, levels[i % 7], today - timedelta(days=i % 30))
        if i % 3 == 0:
            _make_signal(db, e, "cross_agency_composite", 0.5 + (i % 5) * 0.1, f"Evidence {i}")
    db.commit()

    start = time.perf_counter()
    result = export_static_site(tmp_path, db=db)
    elapsed = time.perf_counter() - start

    assert result["entities"] == 500
    assert elapsed < 10, f"Export took {elapsed:.2f}s for 500 entities (limit: 10s)"
