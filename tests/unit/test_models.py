"""
Tests for SQLAlchemy models — M0 acceptance criteria:
- All tables have correct columns and constraints.
"""

import uuid
import pytest
from sqlalchemy import inspect
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cam.db.models import Base, Entity, EntityAlias, Event, Signal, AlertScore
from tests.conftest import requires_db


@pytest.fixture(scope="module")
def db_url():
    import os
    return os.environ.get(
        "DATABASE_URL", "postgresql://cam:cam@localhost:5432/cam_test"
    )


@pytest.fixture(scope="module")
def engine(db_url):
    eng = create_engine(db_url)
    Base.metadata.create_all(eng)
    yield eng
    Base.metadata.drop_all(eng)


@pytest.fixture
def session(engine):
    Session = sessionmaker(bind=engine)
    s = Session()
    yield s
    s.rollback()
    s.close()


@requires_db
def test_entity_table_exists(engine):
    inspector = inspect(engine)
    assert "entities" in inspector.get_table_names()


@requires_db
def test_entity_alias_table_exists(engine):
    inspector = inspect(engine)
    assert "entity_aliases" in inspector.get_table_names()


@requires_db
def test_events_table_exists(engine):
    inspector = inspect(engine)
    assert "events" in inspector.get_table_names()


@requires_db
def test_signals_table_exists(engine):
    inspector = inspect(engine)
    assert "signals" in inspector.get_table_names()


@requires_db
def test_alert_scores_table_exists(engine):
    inspector = inspect(engine)
    assert "alert_scores" in inspector.get_table_names()


@requires_db
def test_create_and_retrieve_entity(session):
    entity = Entity(canonical_name="Test Corp", ticker="TEST")
    session.add(entity)
    session.flush()

    fetched = session.query(Entity).filter_by(canonical_name="Test Corp").first()
    assert fetched is not None
    assert fetched.ticker == "TEST"
    assert fetched.id is not None


@requires_db
def test_entity_alias_unique_constraint(session):
    """raw_name + source must be unique."""
    import sqlalchemy.exc

    entity = Entity(canonical_name="Duplicate Corp")
    session.add(entity)
    session.flush()

    alias1 = EntityAlias(
        id=str(uuid.uuid4()),
        entity_id=entity.id,
        raw_name="Duplicate Corp",
        source="manual",
        confidence=1.0,
    )
    session.add(alias1)
    session.flush()

    alias2 = EntityAlias(
        id=str(uuid.uuid4()),
        entity_id=entity.id,
        raw_name="Duplicate Corp",
        source="manual",
        confidence=0.9,
    )
    session.add(alias2)
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        session.flush()


@requires_db
def test_alert_score_unique_constraint(session):
    """entity_id + score_date must be unique."""
    import datetime
    import sqlalchemy.exc

    entity = Entity(canonical_name="Alert Corp")
    session.add(entity)
    session.flush()

    score1 = AlertScore(
        entity_id=entity.id,
        score_date=datetime.date(2026, 1, 1),
        composite_score=0.5,
        alert_level="watch",
    )
    session.add(score1)
    session.flush()

    score2 = AlertScore(
        entity_id=entity.id,
        score_date=datetime.date(2026, 1, 1),
        composite_score=0.6,
    )
    session.add(score2)
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        session.flush()
