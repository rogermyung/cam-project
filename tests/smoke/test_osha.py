"""LIVE smoke tests for OSHA bulk enforcement CSV reachability."""

from datetime import date

import httpx
import pytest

from cam.ingestion.osha import _OSHA_BULK_BASE


@pytest.mark.live
def test_osha_bulk_csv_reachable():
    """Verify that the OSHA bulk enforcement CSV is reachable for at least one recent year.

    Tests the current year and prior year, since the current year's file may not be
    published yet. At least one must be reachable.
    """
    today = date.today()
    years_to_try = [today.year, today.year - 1]
    statuses = []
    urls = []

    for year in years_to_try:
        url = f"{_OSHA_BULK_BASE}/osha_{year}.csv"
        urls.append(url)
        status = None

        try:
            resp = httpx.head(url, follow_redirects=True, timeout=30)
            status = resp.status_code
        except httpx.RequestException:
            try:
                resp = httpx.get(url, headers={"Range": "bytes=0-3"}, timeout=30)
                status = resp.status_code
            except httpx.RequestException:
                pass

        statuses.append(status)

    tried_info = ", ".join([f"{url} (status={status})" for url, status in zip(urls, statuses)])
    assert any(status is not None and status < 400 for status in statuses), (
        f"No reachable OSHA CSV found. Tried: {tried_info}"
    )
