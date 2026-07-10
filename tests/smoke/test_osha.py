"""LIVE smoke tests for OSHA bulk enforcement CSV reachability."""

from datetime import date

import httpx
import pytest

from cam.ingestion.osha import _OSHA_BULK_BASE
from tests.smoke._http import get_live


@pytest.mark.live
def test_osha_bulk_csv_reachable():
    """Verify that the OSHA bulk enforcement CSV is reachable for at least one recent year.

    Tests the current year and prior year, since the current year's file may not be
    published yet. At least one must be reachable. Uses a single ranged GET per year
    (rather than HEAD-then-GET-on-exception) so a non-2xx/3xx HEAD response can't
    silently skip the GET probe.
    """
    today = date.today()
    years_to_try = [today.year, today.year - 1]
    statuses: list[int | None] = []
    urls = []

    for year in years_to_try:
        url = f"{_OSHA_BULK_BASE}/osha_{year}.csv"
        urls.append(url)
        try:
            resp = get_live(url, headers={"Range": "bytes=0-3"}, follow_redirects=True, timeout=30)
            statuses.append(resp.status_code)
        except httpx.RequestException:
            statuses.append(None)

    tried_info = ", ".join(f"{url} (status={status})" for url, status in zip(urls, statuses))
    assert any(status is not None and status < 400 for status in statuses), (
        f"No reachable OSHA CSV found. Tried: {tried_info}"
    )
