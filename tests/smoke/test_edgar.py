"""Live smoke tests for SEC EDGAR integration."""

import pytest


@pytest.mark.live
def test_company_tickers_reachable():
    """Verify SEC EDGAR company_tickers.json endpoint is reachable and well-shaped."""
    from cam.config import get_settings
    from cam.ingestion.edgar import _get

    url = get_settings().edgar_company_tickers_url
    resp = _get(url)

    assert resp.status_code == 200

    data = resp.json()
    assert isinstance(data, dict)
    assert len(data) > 1000

    sample = next(iter(data.values()))
    assert "cik_str" in sample
    assert "ticker" in sample
    assert "title" in sample
