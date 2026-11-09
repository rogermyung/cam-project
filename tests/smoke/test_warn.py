"""Live smoke tests for WARN state sources.

Each state is fetched AND parsed, because a page that returns HTTP 200 but no
longer contains the expected table is exactly the drift this suite exists to
catch. A reachability-only check hid three broken states for months.
"""

from __future__ import annotations

import pytest

from cam.warn import fetch, parse
from cam.warn.state_urls import STATE_CONFIGS

# States with a documented, known failure. Only add a state here with a root
# cause; an undocumented entry suppresses the signal.
_KNOWN_BROKEN: dict[str, str] = {
    "TX": "2026-11-09: page returns 200 but contains no <table>; parser never matched the live page",
    "NY": "2026-11-09: page returns 200 but parses to 0 records; parser never matched the live page",
    "PA": "2026-11-09: page returns 200 but contains no <table>; parser never matched the live page",
}

_PARAMS = [
    pytest.param(code, marks=pytest.mark.xfail(reason=_KNOWN_BROKEN[code], strict=False))
    if code in _KNOWN_BROKEN
    else code
    for code in sorted(STATE_CONFIGS)
]


@pytest.mark.live
@pytest.mark.parametrize("code", _PARAMS)
def test_warn_state_fetches_and_parses(code: str) -> None:
    cfg = STATE_CONFIGS[code]
    records = [r for r in parse(fetch(cfg.url), cfg) if r.company.strip()]
    assert records, f"{code}: fetched {cfg.url} but parsed 0 records"
