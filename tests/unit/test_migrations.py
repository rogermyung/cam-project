"""
Tests for Alembic migrations — M0 acceptance criteria:
- alembic upgrade head creates all tables without errors.
- All migrations are reversible (alembic downgrade -1).
"""

import os
import subprocess
import sys

import pytest

from tests.conftest import requires_db


def _run_alembic(command: list[str], db_url: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["DATABASE_URL"] = db_url
    return subprocess.run(
        [sys.executable, "-m", "alembic"] + command,
        capture_output=True,
        text=True,
        env=env,
        cwd=os.path.join(os.path.dirname(__file__), "..", ".."),
    )


@pytest.fixture(scope="module")
def db_url():
    return os.environ.get("DATABASE_URL", "postgresql://cam:cam@localhost:5432/cam_test")


@requires_db
def test_upgrade_head(db_url):
    result = _run_alembic(["upgrade", "head"], db_url)
    assert result.returncode == 0, f"alembic upgrade head failed:\n{result.stderr}"


@requires_db
def test_downgrade_is_reversible(db_url):
    _run_alembic(["upgrade", "head"], db_url)
    result = _run_alembic(["downgrade", "-1"], db_url)
    assert result.returncode == 0, f"alembic downgrade -1 failed:\n{result.stderr}"


@requires_db
def test_upgrade_after_downgrade(db_url):
    _run_alembic(["upgrade", "head"], db_url)
    _run_alembic(["downgrade", "-1"], db_url)
    result = _run_alembic(["upgrade", "head"], db_url)
    assert result.returncode == 0, f"alembic upgrade head after downgrade failed:\n{result.stderr}"


@requires_db
def test_ingest_json_columns_are_jsonb(db_url):
    """raw_json / checkpoint must be JSONB on PostgreSQL to match the ORM models.

    The ORM declares these columns as ``JSON().with_variant(JSONB, "postgresql")``;
    the migration must produce the same physical type so autogenerate stays clean
    and JSONB operators/indexing remain available.
    """
    from sqlalchemy import create_engine, inspect
    from sqlalchemy.dialects.postgresql import JSONB

    _run_alembic(["upgrade", "head"], db_url)

    engine = create_engine(db_url)
    try:
        insp = inspect(engine)
        failures = {c["name"]: c["type"] for c in insp.get_columns("ingest_failures")}
        checkpoints = {c["name"]: c["type"] for c in insp.get_columns("ingest_checkpoints")}
    finally:
        engine.dispose()

    assert isinstance(failures["raw_json"], JSONB), f"raw_json is {failures['raw_json']!r}"
    assert isinstance(checkpoints["checkpoint"], JSONB), (
        f"checkpoint is {checkpoints['checkpoint']!r}"
    )
