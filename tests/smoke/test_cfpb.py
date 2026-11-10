"""Live smoke test for the CFPB bulk complaints source.

This test does not download the whole file. It streams only the first bytes
and verifies the response looks like a ZIP with a large enough content
length.
"""

from __future__ import annotations

import httpx
import pytest

from cam.cfpb import BULK_URL


@pytest.mark.live
def test_cfpb_bulk_zip_stream_looks_like_zip() -> None:
    with httpx.stream(
        "GET",
        BULK_URL,
        follow_redirects=True,
        timeout=60,
    ) as resp:
        assert resp.status_code == 200
        cl = resp.headers.get("content-length")
        if cl is not None:
            assert int(cl) > 100_000_000

        buf = bytearray()
        for chunk in resp.iter_bytes():
            buf.extend(chunk)
            if len(buf) >= 4:
                buf = buf[:4]
                break
        resp.close()

        assert bytes(buf) == b"PK\x03\x04"
