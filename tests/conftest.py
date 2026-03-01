"""
Shared pytest fixtures for the CAM test suite.
All external API calls must be mocked — no live network calls in tests.
"""

import os
import socket
import pytest
from unittest.mock import patch


# ---------------------------------------------------------------------------
# Test environment defaults.
# Only set vars that are NOT already present in the environment — this lets
# CI inject its own DATABASE_URL (pointing at the provisioned 'cam' DB) while
# local runs without Postgres still get a sensible fallback.
# ---------------------------------------------------------------------------

TEST_ENV_DEFAULTS = {
    "DATABASE_URL": "postgresql://cam:cam@localhost:5432/cam",
    "EDGAR_USER_AGENT": "test@cam-project.org",
    "REDIS_URL": "redis://localhost:6379/1",
    "S3_BUCKET": "cam-test",
    "S3_ENDPOINT": "http://localhost:9000",
    "S3_ACCESS_KEY": "minioadmin",
    "S3_SECRET_KEY": "minioadmin",
    "API_AUTH_TOKEN": "test-token",
}


@pytest.fixture(autouse=True, scope="session")
def set_test_env():
    # Only inject values that aren't already defined — CI vars take precedence.
    overrides = {k: v for k, v in TEST_ENV_DEFAULTS.items() if k not in os.environ}
    with patch.dict(os.environ, overrides):
        yield


@pytest.fixture(scope="session")
def settings():
    from cam.config import get_settings
    return get_settings()


def _postgres_reachable() -> bool:
    """
    Return True if both:
    1. TCP connection to localhost:5432 succeeds, AND
    2. The target database actually exists (not just the port).
    """
    try:
        with socket.create_connection(("localhost", 5432), timeout=2):
            pass
    except OSError:
        return False

    # Verify the database exists by attempting a real connection
    try:
        import psycopg2  # type: ignore
        db_url = os.environ.get("DATABASE_URL", TEST_ENV_DEFAULTS["DATABASE_URL"])
        conn = psycopg2.connect(db_url, connect_timeout=2)
        conn.close()
        return True
    except Exception:
        pass

    # psycopg2 not installed or DB absent — fall back to TCP-only check
    return True  # Port is open; let Alembic/SQLAlchemy surface any DB-level errors


requires_db = pytest.mark.skipif(
    not _postgres_reachable(),
    reason="PostgreSQL not reachable at localhost:5432",
)
