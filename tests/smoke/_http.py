"""Shared HTTP helper for tests/smoke/.

Wraps httpx with a bounded tenacity retry so a single transient blip
(timeout, connection reset) doesn't turn a weekly smoke run red — a
persistent failure (4xx/5xx, or repeated timeouts) still fails after the
retry, which is the real drift signal these tests exist to surface.
"""

from __future__ import annotations

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

_TRANSIENT_EXCEPTIONS = (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError)
_retry_transient = retry(
    retry=retry_if_exception_type(_TRANSIENT_EXCEPTIONS),
    stop=stop_after_attempt(2),
    wait=wait_fixed(2),
    reraise=True,
)


@_retry_transient
def get_live(url: str, **kwargs) -> httpx.Response:
    return httpx.get(url, **kwargs)
