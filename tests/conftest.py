"""
Shared pytest fixtures for the CAM test suite.
All external API calls must be mocked — no live network calls in tests.
"""

import os
import socket
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Test database URL.
# Always points at cam_test (never cam) to protect the dev/CI migration DB
# from destructive operations such as create_all, drop_all, and downgrade.
# Override with TEST_DATABASE_URL if your local setup uses a different name.
# ---------------------------------------------------------------------------
_TEST_DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://cam:cam@localhost:5432/cam_test",
)

TEST_ENV_DEFAULTS = {
    "DATABASE_URL": _TEST_DB_URL,
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
    # DATABASE_URL is always forced to the test database regardless of the
    # surrounding environment, preventing destructive test operations from
    # touching the dev or CI migration database.
    # All other vars only inject defaults for keys absent from the environment
    # so CI can control EDGAR_USER_AGENT, API_AUTH_TOKEN, etc.
    always_override = {"DATABASE_URL": _TEST_DB_URL}
    other_defaults = {
        k: v for k, v in TEST_ENV_DEFAULTS.items() if k != "DATABASE_URL" and k not in os.environ
    }
    with patch.dict(os.environ, {**always_override, **other_defaults}):
        yield


@pytest.fixture(scope="session")
def settings():
    from cam.config import get_settings

    return get_settings()


def _postgres_reachable() -> bool:
    """
    Return True if the test database (cam_test) is reachable.
    Checks TCP connectivity first, then attempts a real psycopg2 connection
    specifically to cam_test so that having only a cam DB does not cause
    destructive test operations to run against the wrong database.
    """
    try:
        with socket.create_connection(("localhost", 5432), timeout=2):
            pass
    except OSError:
        return False

    # Verify the *test* database exists — not just the port.
    try:
        import psycopg2  # type: ignore

        conn = psycopg2.connect(_TEST_DB_URL, connect_timeout=2)
        conn.close()
        return True
    except Exception:
        return False


requires_db = pytest.mark.skipif(
    not _postgres_reachable(),
    reason="PostgreSQL test database (cam_test) not reachable at localhost:5432",
)
