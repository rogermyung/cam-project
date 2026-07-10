"""Smoke tests for EPA ECHO integration."""

import pytest

from cam.config import get_settings
from tests.smoke._http import get_live


@pytest.mark.live
def test_echo_bulk_enforcement_download_reachable():
    """Verify EPA ECHO bulk enforcement ZIP endpoint is reachable and returns valid ZIP."""
    url = get_settings().echo_bulk_zip_url

    resp = get_live(
        url,
        headers={"Range": "bytes=0-3"},
        follow_redirects=True,
        timeout=60,
    )

    assert resp.status_code in (200, 206), f"Expected 200 or 206, got {resp.status_code}"
    assert resp.content[:2] == b"PK", "Response does not start with ZIP magic bytes (PK)"
