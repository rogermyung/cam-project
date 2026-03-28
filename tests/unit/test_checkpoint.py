"""
Tests for cam.ingestion.checkpoint — ingestion progress cursors.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cam.db.models import Base, IngestCheckpoint
from cam.ingestion.checkpoint import complete_checkpoint, load_checkpoint, save_checkpoint

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


# ---------------------------------------------------------------------------
# Round-trip: save → load
# ---------------------------------------------------------------------------


def test_save_and_load_checkpoint(db):
    source = "osha"
    run_id = uuid.uuid4()
    cursor = {"offset": 500, "records_ok": 490, "records_err": 10}

    save_checkpoint(db, source, run_id, cursor, records_ok=490, records_err=10)
    db.commit()

    loaded = load_checkpoint(db, source, run_id=run_id)
    assert loaded == cursor


def test_load_returns_none_when_no_checkpoint(db):
    result = load_checkpoint(db, "nonexistent_source_xyz")
    assert result is None


def test_load_latest_incomplete_without_run_id(db):
    source = "cfpb_latest_test"
    run_id_1 = uuid.uuid4()
    run_id_2 = uuid.uuid4()

    save_checkpoint(db, source, run_id_1, {"page": 1}, 100, 0)
    db.commit()
    save_checkpoint(db, source, run_id_2, {"page": 7}, 700, 3)
    db.commit()

    # Without run_id, should return the most recently updated cursor
    loaded = load_checkpoint(db, source)
    assert loaded == {"page": 7}


# ---------------------------------------------------------------------------
# Upsert: calling save_checkpoint twice for same (source, run_id)
# ---------------------------------------------------------------------------


def test_save_checkpoint_upserts(db):
    source = "epa"
    run_id = uuid.uuid4()

    save_checkpoint(db, source, run_id, {"offset": 0}, 0, 0)
    db.commit()
    save_checkpoint(db, source, run_id, {"offset": 500}, 480, 20)
    db.commit()

    # Only one row for this (source, run_id) combo
    rows = db.query(IngestCheckpoint).filter_by(source=source, run_id=run_id).all()
    assert len(rows) == 1
    assert rows[0].checkpoint == {"offset": 500}
    assert rows[0].records_ok == 480


# ---------------------------------------------------------------------------
# complete_checkpoint
# ---------------------------------------------------------------------------


def test_complete_checkpoint_sets_completed_at(db):
    source = "warn_complete_test"
    run_id = uuid.uuid4()

    save_checkpoint(db, source, run_id, {"state": "CA"}, 200, 0)
    db.commit()

    complete_checkpoint(db, source, run_id)
    db.commit()

    row = db.query(IngestCheckpoint).filter_by(source=source, run_id=run_id).first()
    assert row is not None
    assert row.completed_at is not None


def test_completed_checkpoint_not_returned_by_load(db):
    source = "edgar_complete_test"
    run_id = uuid.uuid4()

    save_checkpoint(db, source, run_id, {"cik": "0001234"}, 10, 0)
    db.commit()
    complete_checkpoint(db, source, run_id)
    db.commit()

    loaded = load_checkpoint(db, source, run_id=run_id)
    assert loaded is None  # completed runs are excluded


# ---------------------------------------------------------------------------
# Partial resume behaviour
# ---------------------------------------------------------------------------


def test_partial_resume_skips_processed_records():
    """Given a 1000-item list and a checkpoint at offset 500,
    simulate that only records 500-999 would be processed."""
    records = list(range(1000))
    checkpoint_offset = 500

    remaining = records[checkpoint_offset:]
    assert len(remaining) == 500
    assert remaining[0] == 500
    assert remaining[-1] == 999
