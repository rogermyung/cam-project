"""
Shared TypeSafe / Jev plumbing.

Client construction and the error taxonomy, in one place so every caller
degrades the same way.  The question definitions themselves live with the
domain knowledge they encode — :mod:`cam.entity.jev_align` for entity
alignment, :mod:`cam.analysis.merger_screener` and
:mod:`cam.analysis.proxy_parser` for the screening judgments — matching how
those modules already keep their keyword tables local.

Nothing here decides policy.  Callers choose what a failure means, because
the right answer differs: entity resolution falls through to unresolved,
while the analysis modules fall back to their keyword implementations.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol

from typesafe_sdk import (
    TypeSafeAuthenticationError,
    TypeSafeError,
    TypeSafePermissionDeniedError,
)

logger = logging.getLogger(__name__)

# A rejected or unauthorised credential. Never degrade on these: they fail
# every subsequent call too, so a quiet fallback turns one misconfiguration
# into a whole run that looks like it simply found nothing. Silent empty runs
# are this project's most expensive failure mode.
FATAL_ERRORS = (TypeSafeAuthenticationError, TypeSafePermissionDeniedError)

# Everything else the service can do to us: rate limit, timeout, connection
# reset, 5xx. These are worth surviving.
TRANSIENT_ERRORS = TypeSafeError


class SystemOneClient(Protocol):
    """The one method callers need, so tests can inject a stub."""

    def system_one(self, state: Any, questions: Any, **kwargs: Any) -> Any: ...


def default_client(*, api_key: str | None = None, model: str | None = None) -> SystemOneClient:
    """Build a TypeSafe client from settings.

    Raises rather than returning a no-op stub when the key is missing: a stub
    would answer "no signal" for every question, which is indistinguishable
    from a working pipeline that genuinely found nothing.
    """
    from typesafe_sdk import TypeSafeClient

    from cam.config import get_settings

    settings = get_settings()
    key = api_key if api_key is not None else settings.typesafe_api_key
    if not key:
        raise RuntimeError(
            "TYPESAFE_API_KEY is not set; cannot use Jev. Set it in .env, or "
            "leave the relevant *_JEV_ENABLED flag unset to run without it."
        )
    return TypeSafeClient(api_key=key, model=model or settings.jev_model)


def ask(
    state: Any,
    questions: Any,
    *,
    client: SystemOneClient | None = None,
    model: str | None = None,
) -> Any:
    """Send one batched request and return the raw response.

    Every question travels in a single call: Jev ingests the state once and
    evaluates all of them in parallel, so N judgments over one document cost
    one round trip rather than N.

    Errors are not caught here — see :data:`FATAL_ERRORS` and
    :data:`TRANSIENT_ERRORS` for the split callers are expected to apply.
    """
    resolved = client if client is not None else default_client(model=model)
    kwargs: dict[str, Any] = {}
    if model is not None:
        kwargs["model"] = model
    return resolved.system_one(state=state, questions=questions, **kwargs)


def enabled(flag: str) -> bool:
    """Return whether the named boolean setting is on, defaulting to off.

    Tolerates settings being unavailable (unit tests that never touch config),
    matching the fallback convention in ``cam.alerts.scorer``.
    """
    try:
        from cam.config import get_settings

        return bool(getattr(get_settings(), flag, False))
    except Exception:  # pragma: no cover — no settings in config-free tests
        return False


__all__ = [
    "FATAL_ERRORS",
    "TRANSIENT_ERRORS",
    "SystemOneClient",
    "ask",
    "default_client",
    "enabled",
]
