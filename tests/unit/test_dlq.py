"""
Tests for cam.ingestion.dlq — Dead Letter Queue.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from cam.db.models import Base, IngestFailure
from cam.ingestion.dlq import (
    ERROR_DB_WRITE,
    ERROR_ENTITY_RESOLUTION,
    export_to_csv,
    mark_resolved,
    open_failures,
    record_failure,
    replay_failures,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def engine():
    eng = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def db(engine):
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    yield session
    session.rollback()
    session.close()


def _make_failure(
    db: Session, *, source="osha", error_type=ERROR_ENTITY_RESOLUTION
) -> IngestFailure:
    run_id = uuid.uuid4()
    raw = {"estab_name": "ACME Corp", "activity_nr": "12345"}
    failure = record_failure(
        db,
        source=source,
        run_id=run_id,
        raw_record=raw,
        error_type=error_type,
        exc=ValueError("no match"),
        raw_key="12345",
    )
    db.commit()
    return failure


# ---------------------------------------------------------------------------
# record_failure
# ---------------------------------------------------------------------------


def test_record_failure_writes_row(db):
    run_id = uuid.uuid4()
    failure = record_failure(
        db,
        source="osha",
        run_id=run_id,
        raw_record={"estab_name": "Test Corp", "activity_nr": "999"},
        error_type=ERROR_ENTITY_RESOLUTION,
        exc=ValueError("no entity match"),
        raw_key="999",
    )
    db.commit()

    assert failure is not None
    assert failure.id is not None
    assert failure.source == "osha"
    assert failure.run_id == run_id
    assert failure.raw_key == "999"
    assert failure.error_type == ERROR_ENTITY_RESOLUTION
    assert "no entity match" in failure.error_msg
    assert failure.resolved_at is None
    assert failure.retry_count == 0


def test_record_failure_is_best_effort(db):
    """record_failure must not raise even if the DB session is broken."""
    bad_db = MagicMock()
    bad_db.add.side_effect = RuntimeError("session broken")

    result = record_failure(
        bad_db,
        source="osha",
        run_id=uuid.uuid4(),
        raw_record={"estab_name": "X"},
        error_type=ERROR_ENTITY_RESOLUTION,
        exc=ValueError("fail"),
    )
    assert result is None  # graceful degradation


def test_record_failure_unknown_error_type(db):
    """Unknown error_type is coerced to 'validation' with a warning."""
    run_id = uuid.uuid4()
    failure = record_failure(
        db,
        source="osha",
        run_id=run_id,
        raw_record={"x": 1},
        error_type="totally_unknown",
        exc=ValueError("x"),
    )
    db.commit()
    assert failure is not None
    assert failure.error_type == "validation"


def test_record_failure_emits_structured_log(db):
    """record_failure must emit a structured log line with required fields."""
    run_id = uuid.uuid4()
    with patch("cam.ingestion.dlq.logger") as mock_logger:
        record_failure(
            db,
            source="osha",
            run_id=run_id,
            raw_record={"estab_name": "LogTest Corp"},
            error_type=ERROR_ENTITY_RESOLUTION,
            exc=ValueError("log test"),
            raw_key="log_key_1",
        )
        db.commit()

    mock_logger.error.assert_called_once()
    call_kwargs = mock_logger.error.call_args
    extra = call_kwargs.kwargs.get("extra", {}) or (
        call_kwargs[1].get("extra", {}) if len(call_kwargs) > 1 else {}
    )
    assert extra.get("source") == "osha"
    assert extra.get("error_type") == ERROR_ENTITY_RESOLUTION
    assert str(run_id) == extra.get("run_id")


# ---------------------------------------------------------------------------
# open_failures
# ---------------------------------------------------------------------------


def test_open_failures_returns_unresolved(db):
    _make_failure(db, source="cfpb", error_type=ERROR_DB_WRITE)
    _make_failure(db, source="osha", error_type=ERROR_ENTITY_RESOLUTION)

    results = open_failures(db)
    assert len(results) >= 2


def test_open_failures_filter_by_source(db):
    _make_failure(db, source="epa_unique_source_xyz")

    results = open_failures(db, source="epa_unique_source_xyz")
    assert all(f.source == "epa_unique_source_xyz" for f in results)


def test_open_failures_filter_by_error_type(db):
    _make_failure(db, source="osha", error_type=ERROR_DB_WRITE)

    results = open_failures(db, error_type=ERROR_DB_WRITE)
    assert all(f.error_type == ERROR_DB_WRITE for f in results)


def test_open_failures_excludes_resolved(db):
    failure = _make_failure(db, source="warn")
    mark_resolved(db, [failure.id], note="test")
    db.commit()

    open_ids = {f.id for f in open_failures(db, source="warn")}
    assert failure.id not in open_ids


# ---------------------------------------------------------------------------
# mark_resolved
# ---------------------------------------------------------------------------


def test_mark_resolved_sets_resolved_at(db):
    failure = _make_failure(db)
    count = mark_resolved(db, [failure.id], note="dismissed in test")
    db.commit()

    assert count == 1
    refreshed = db.get(IngestFailure, failure.id)
    assert refreshed.resolved_at is not None
    assert "dismissed in test" in refreshed.error_msg


def test_mark_resolved_empty_list(db):
    count = mark_resolved(db, [])
    assert count == 0


def test_mark_resolved_already_resolved_not_double_counted(db):
    failure = _make_failure(db)
    mark_resolved(db, [failure.id])
    db.commit()
    count = mark_resolved(db, [failure.id])  # second call
    assert count == 0  # WHERE resolved_at IS NULL → no match


# ---------------------------------------------------------------------------
# replay_failures
# ---------------------------------------------------------------------------


def test_replay_success_marks_resolved(db):
    failure = _make_failure(db)

    def good_fn(raw_record, session):
        return True  # success

    result = replay_failures(db, [failure.id], good_fn)
    db.commit()

    assert result.attempted == 1
    assert result.succeeded == 1
    assert result.failed == 0

    refreshed = db.get(IngestFailure, failure.id)
    assert refreshed.resolved_at is not None
    assert refreshed.retry_count == 1


def test_replay_failure_increments_retry_not_resolved(db):
    failure = _make_failure(db)

    def bad_fn(raw_record, session):
        raise RuntimeError("still broken")

    result = replay_failures(db, [failure.id], bad_fn)
    db.commit()

    assert result.attempted == 1
    assert result.succeeded == 0
    assert result.failed == 1

    refreshed = db.get(IngestFailure, failure.id)
    assert refreshed.resolved_at is None
    assert refreshed.retry_count == 1


# ---------------------------------------------------------------------------
# export_to_csv
# ---------------------------------------------------------------------------


def test_export_to_csv(db, tmp_path):
    _make_failure(db, source="csv_test_source")
    out = tmp_path / "failures.csv"

    count = export_to_csv(db, out, source="csv_test_source")

    assert count >= 1
    assert out.exists()
    lines = out.read_text().splitlines()
    assert lines[0].startswith("id,source")  # header present
    assert any("csv_test_source" in line for line in lines[1:])
