"""Smoke tests for CFPB complaints bulk download."""

import pytest

from cam.config import get_settings
from tests.smoke._http import get_live


@pytest.mark.live
def test_cfpb_bulk_reachable():
    """Check CFPB bulk download endpoint is reachable and returns ZIP data."""
    url = get_settings().cfpb_bulk_url
    resp = get_live(url, headers={"Range": "bytes=0-3"}, follow_redirects=True, timeout=60)
    assert resp.status_code in (200, 206)
    assert resp.content[:2] == b"PK"
