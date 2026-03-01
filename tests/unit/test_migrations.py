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
