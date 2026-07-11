"""
Live smoke tests for WARN Act state sources.

These tests verify that all configured state source URLs are accessible and
return successful responses. Per-state test nodes surface which state feeds
have drifted independently.
"""

import pytest

from cam.ingestion.warn.state_urls import STATE_CONFIGS
from tests.smoke._http import get_live

# States with a known, already-documented outage (see the comments on their
# StateConfig in cam/ingestion/warn/state_urls.py) are xfail so the weekly
# smoke run stays a useful signal for NEW drift rather than being
# permanently red on issues already tracked. Do not add a state here just
# because it failed once — that suppresses the exact drift this suite exists
# to catch. Only add it once there's a known, documented root cause.
_KNOWN_BROKEN: dict[str, str] = {
    # CA (XLSX) and MI (Sitecore JSON API + Mozilla UA to clear the Akamai WAF)
    # were fixed in Phase 4 — both now fetch and parse live. Re-add a state here
    # only with a documented root cause; see cam/ingestion/warn/state_urls.py.
}

_PARAMS = [
    pytest.param(code, marks=pytest.mark.xfail(reason=_KNOWN_BROKEN[code], strict=False))
    if code in _KNOWN_BROKEN
    else code
    for code in sorted(STATE_CONFIGS)
]


@pytest.mark.live
@pytest.mark.parametrize("code", _PARAMS)
def test_warn_state_url_accessible(code: str) -> None:
    """
    Verify that each WARN state source URL is accessible.

    Each state is a separate test node so that failures in individual
    state feeds are immediately visible and don't mask other states.
    """
    cfg = STATE_CONFIGS[code]
    resp = get_live(
        cfg.url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; CAM-smoke/1.0)"},
        follow_redirects=True,
        timeout=30,
    )
    assert resp.status_code < 400, f"State {code} failed: {cfg.url} returned {resp.status_code}"
