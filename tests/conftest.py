"""
Shared pytest fixtures for the CAM test suite.
All external API calls must be mocked — no live network calls in tests.
"""

import os
import socket
import pytest
from unittest.mock import patch


# ---------------------------------------------------------------------------
# Force test environment vars before any app code runs
# ---------------------------------------------------------------------------

TEST_ENV = {
    "DATABASE_URL": "postgresql://cam:cam@localhost:5432/cam_test",
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
    with patch.dict(os.environ, TEST_ENV):
        yield


@pytest.fixture(scope="session")
def settings():
    from cam.config import get_settings
    return get_settings()


def _postgres_reachable() -> bool:
    """Return True if a TCP connection to localhost:5432 succeeds."""
    try:
        with socket.create_connection(("localhost", 5432), timeout=2):
            return True
    except OSError:
        return False


requires_db = pytest.mark.skipif(
    not _postgres_reachable(),
    reason="PostgreSQL not reachable at localhost:5432",
)
