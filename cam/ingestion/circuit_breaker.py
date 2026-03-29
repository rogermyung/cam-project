"""
M15 — Circuit Breaker

Prevents a failing external API from consuming the full retry budget on every
record.  Each ingestion source gets its own breaker instance from the module-
level registry.

States
------
CLOSED   Normal operation — requests pass through.
OPEN     Failing fast — calls immediately raise CircuitOpenError without
         hitting the network.  Breaker opens after ``failure_threshold``
         consecutive failures.
HALF_OPEN Testing recovery — one probe request is allowed through.  Success
         transitions back to CLOSED; failure returns to OPEN and resets the
         recovery timer.

Usage::

    from cam.ingestion.circuit_breaker import get_breaker

    breaker = get_breaker("osha")
    try:
        response = breaker.call(httpx.get, url, timeout=60)
    except CircuitOpenError:
        record_failure(..., error_type=ERROR_API_ERROR, exc=...)
        return result   # stop processing this source for this run
"""

from __future__ import annotations

import logging
import threading
import time
from enum import Enum, auto

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class CircuitOpenError(Exception):
    """Raised by CircuitBreaker.call() when the breaker is OPEN."""

    def __init__(self, source: str) -> None:
        super().__init__(
            f"Circuit breaker for '{source}' is OPEN — skipping API call. "
            f"Will retry after recovery timeout."
        )
        self.source = source


# ---------------------------------------------------------------------------
# State enum
# ---------------------------------------------------------------------------


class BreakerState(Enum):
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


# ---------------------------------------------------------------------------
# CircuitBreaker
# ---------------------------------------------------------------------------


class CircuitBreaker:
    """Thread-safe circuit breaker for a single external API source.

    Parameters
    ----------
    name:              Identifier (used in log messages).
    failure_threshold: Consecutive failures before opening the circuit.
    recovery_timeout:  Seconds to wait in OPEN state before trying HALF_OPEN.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: int = 300,
    ) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def state(self) -> BreakerState:
        with self._lock:
            return self._current_state()

    def call(self, fn, *args, **kwargs):
        """Execute *fn* respecting the circuit breaker state.

        Raises CircuitOpenError immediately when OPEN (no network call).
        Updates internal state based on success or failure.
        """
        with self._lock:
            state = self._current_state()
            if state == BreakerState.OPEN:
                raise CircuitOpenError(self.name)
            if state == BreakerState.HALF_OPEN:
                logger.info("circuit_breaker source=%s state=HALF_OPEN probing", self.name)

        try:
            result = fn(*args, **kwargs)
            self._on_success()
            return result
        except Exception:
            self._on_failure()
            raise

    def reset(self) -> None:
        """Manually reset the breaker to CLOSED (for operator use)."""
        with self._lock:
            self._state = BreakerState.CLOSED
            self._consecutive_failures = 0
            self._opened_at = None
        logger.info("circuit_breaker source=%s manually reset to CLOSED", self.name)

    # ------------------------------------------------------------------
    # Internal state machine
    # ------------------------------------------------------------------

    def _current_state(self) -> BreakerState:
        """Return effective state, transitioning OPEN→HALF_OPEN after timeout."""
        if self._state == BreakerState.OPEN:
            if self._opened_at is not None and (
                time.monotonic() - self._opened_at >= self.recovery_timeout
            ):
                self._state = BreakerState.HALF_OPEN
                logger.info(
                    "circuit_breaker source=%s OPEN→HALF_OPEN after %ds",
                    self.name,
                    self.recovery_timeout,
                )
        return self._state

    def _on_success(self) -> None:
        with self._lock:
            if self._state in (BreakerState.HALF_OPEN, BreakerState.OPEN):
                logger.info(
                    "circuit_breaker source=%s %s→CLOSED on success", self.name, self._state.name
                )
            self._state = BreakerState.CLOSED
            self._consecutive_failures = 0
            self._opened_at = None

    def _on_failure(self) -> None:
        with self._lock:
            self._consecutive_failures += 1
            if self._state == BreakerState.HALF_OPEN:
                # Probe failed — reopen immediately
                self._state = BreakerState.OPEN
                self._opened_at = time.monotonic()
                logger.warning("circuit_breaker source=%s HALF_OPEN→OPEN (probe failed)", self.name)
            elif self._consecutive_failures >= self.failure_threshold:
                self._state = BreakerState.OPEN
                self._opened_at = time.monotonic()
                logger.warning(
                    "circuit_breaker source=%s CLOSED→OPEN after %d consecutive failures",
                    self.name,
                    self._consecutive_failures,
                )


# ---------------------------------------------------------------------------
# Global registry
# ---------------------------------------------------------------------------

_registry: dict[str, CircuitBreaker] = {}
_registry_lock = threading.Lock()


def get_breaker(
    source: str,
    failure_threshold: int = 5,
    recovery_timeout: int = 300,
) -> CircuitBreaker:
    """Return the shared CircuitBreaker for *source*, creating it if needed.

    Subsequent calls with the same *source* return the same instance so that
    failure counts accumulate across function calls within a single process.
    """
    with _registry_lock:
        if source not in _registry:
            _registry[source] = CircuitBreaker(
                name=source,
                failure_threshold=failure_threshold,
                recovery_timeout=recovery_timeout,
            )
        return _registry[source]


def reset_all() -> None:
    """Reset every registered circuit breaker to CLOSED.  Useful in tests."""
    with _registry_lock:
        for breaker in _registry.values():
            breaker.reset()
        _registry.clear()
