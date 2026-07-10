"""
Live smoke tests for WARN Act state sources.

These tests verify that all configured state source URLs are accessible and
return successful responses. Per-state test nodes surface which state feeds
have drifted independently.
"""

import httpx
import pytest

from cam.ingestion.warn.state_urls import STATE_CONFIGS


@pytest.mark.live
@pytest.mark.parametrize("code", sorted(STATE_CONFIGS))
def test_warn_state_url_accessible(code: str) -> None:
    """
    Verify that each WARN state source URL is accessible.

    Each state is a separate test node so that failures in individual
    state feeds are immediately visible and don't mask other states.
    """
    cfg = STATE_CONFIGS[code]
    resp = httpx.get(
        cfg.url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; CAM-smoke/1.0)"},
        follow_redirects=True,
        timeout=30,
    )
    assert resp.status_code < 400, f"State {code} failed: {cfg.url} returned {resp.status_code}"
