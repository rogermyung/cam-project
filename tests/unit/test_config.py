"""
Tests for cam/config.py — M0 acceptance criteria:
- config.py raises ValidationError if required env vars are missing.
"""

import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

# Minimum required env vars for Settings to load
_REQUIRED = {
    "DATABASE_URL": "postgresql://cam:cam@localhost:5432/cam",
    "EDGAR_USER_AGENT": "test@example.org",
}


def test_config_loads_with_required_vars():
    """Settings loads successfully when all required vars are present."""
    with patch.dict(os.environ, _REQUIRED, clear=True):
        from cam.config import Settings

        s = Settings()
        assert s.database_url == "postgresql://cam:cam@localhost:5432/cam"
        assert s.edgar_user_agent == "test@example.org"


def test_config_raises_on_missing_database_url():
    """ValidationError raised when DATABASE_URL is absent."""
    env = {k: v for k, v in _REQUIRED.items() if k != "DATABASE_URL"}
    with patch.dict(os.environ, env, clear=True):
        from cam.config import Settings

        with pytest.raises(ValidationError):
            Settings()


def test_config_raises_on_missing_edgar_user_agent():
    """ValidationError raised when EDGAR_USER_AGENT is absent."""
    env = {k: v for k, v in _REQUIRED.items() if k != "EDGAR_USER_AGENT"}
    with patch.dict(os.environ, env, clear=True):
        from cam.config import Settings

        with pytest.raises(ValidationError):
            Settings()


def test_config_api_auth_token_is_optional():
    """API_AUTH_TOKEN is optional so workers/Celery can start without it."""
    with patch.dict(os.environ, _REQUIRED, clear=True):
        from cam.config import Settings

        s = Settings()
        assert s.api_auth_token is None


def test_config_api_auth_token_reads_from_env():
    """API_AUTH_TOKEN is surfaced correctly when provided."""
    with patch.dict(os.environ, {**_REQUIRED, "API_AUTH_TOKEN": "secret"}, clear=True):
        from cam.config import Settings

        s = Settings()
        assert s.api_auth_token == "secret"


def test_config_defaults():
    """Optional settings have expected defaults."""
    with patch.dict(os.environ, _REQUIRED, clear=True):
        from cam.config import Settings

        s = Settings()
        assert s.alert_threshold_watch == pytest.approx(0.40)
        assert s.alert_threshold_elevated == pytest.approx(0.65)
        assert s.alert_threshold_critical == pytest.approx(0.80)
        assert s.entity_fuzzy_threshold == pytest.approx(0.85)
        assert s.entity_review_threshold == pytest.approx(0.65)
        assert s.redis_url == "redis://localhost:6379/0"
        assert s.api_auth_token is None


def test_config_env_var_override():
    """Threshold env vars override defaults."""
    env = {
        **_REQUIRED,
        "ALERT_THRESHOLD_WATCH": "0.30",
        "ALERT_THRESHOLD_CRITICAL": "0.90",
    }
    with patch.dict(os.environ, env, clear=True):
        from cam.config import Settings

        s = Settings()
        assert s.alert_threshold_watch == pytest.approx(0.30)
        assert s.alert_threshold_critical == pytest.approx(0.90)
