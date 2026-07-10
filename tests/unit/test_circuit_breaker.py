"""
Tests for cam.ingestion.circuit_breaker — per-source circuit breaker.

Tests cover all three state transitions:
  CLOSED → OPEN (after failure_threshold consecutive failures)
  OPEN → HALF_OPEN (after recovery_timeout)
  HALF_OPEN → CLOSED (probe succeeds)
  HALF_OPEN → OPEN (probe fails)
"""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock

import pytest

from cam.ingestion.circuit_breaker import (
    BreakerState,
    CircuitBreaker,
    CircuitOpenError,
    get_breaker,
    reset_all,
)


@pytest.fixture(autouse=True)
def clean_registry():
    """Reset global registry between tests so state doesn't bleed."""
    reset_all()
    yield
    reset_all()


# ---------------------------------------------------------------------------
# Basic happy path
# ---------------------------------------------------------------------------


def test_closed_state_passes_calls():
    breaker = CircuitBreaker("test_ok", failure_threshold=3)
    result = breaker.call(lambda: 42)
    assert result == 42
    assert breaker.state == BreakerState.CLOSED


def test_success_after_partial_failures_stays_closed():
    breaker = CircuitBreaker("test_partial", failure_threshold=3)

    def sometimes_fail(count=[0]):
        count[0] += 1
        if count[0] <= 2:
            raise ConnectionError("transient")
        return "ok"

    # Two failures then success — should not open (threshold is 3 consecutive)
    with pytest.raises(ConnectionError):
        breaker.call(sometimes_fail)
    with pytest.raises(ConnectionError):
        breaker.call(sometimes_fail)
    result = breaker.call(sometimes_fail)
    assert result == "ok"
    assert breaker.state == BreakerState.CLOSED


# ---------------------------------------------------------------------------
# CLOSED → OPEN
# ---------------------------------------------------------------------------


def test_opens_after_threshold_consecutive_failures():
    breaker = CircuitBreaker("test_open", failure_threshold=3)

    def always_fail():
        raise ConnectionError("down")

    for _ in range(3):
        with pytest.raises(ConnectionError):
            breaker.call(always_fail)

    assert breaker.state == BreakerState.OPEN


def test_open_raises_circuit_open_error():
    breaker = CircuitBreaker("test_open_err", failure_threshold=2)

    def always_fail():
        raise RuntimeError("bang")

    for _ in range(2):
        with pytest.raises(RuntimeError):
            breaker.call(always_fail)

    assert breaker.state == BreakerState.OPEN
    with pytest.raises(CircuitOpenError) as exc_info:
        breaker.call(lambda: "should not run")
    assert exc_info.value.source == "test_open_err"


def test_open_does_not_call_fn():
    breaker = CircuitBreaker("test_no_call", failure_threshold=1)

    def always_fail():
        raise RuntimeError("bang")

    with pytest.raises(RuntimeError):
        breaker.call(always_fail)
    assert breaker.state == BreakerState.OPEN

    fn = MagicMock(return_value="x")
    with pytest.raises(CircuitOpenError):
        breaker.call(fn)
    fn.assert_not_called()  # open breaker must not invoke the function


# ---------------------------------------------------------------------------
# OPEN → HALF_OPEN (after recovery_timeout)
# ---------------------------------------------------------------------------


def test_transitions_to_half_open_after_timeout(monkeypatch):
    breaker = CircuitBreaker("test_half_open", failure_threshold=1, recovery_timeout=1)

    with pytest.raises(RuntimeError):
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("down")))

    assert breaker.state == BreakerState.OPEN

    # Capture the real monotonic value BEFORE patching to avoid recursion.
    far_future = time.monotonic() + 1000
    monkeypatch.setattr("cam.ingestion.circuit_breaker.time.monotonic", lambda: far_future)

    assert breaker.state == BreakerState.HALF_OPEN


# ---------------------------------------------------------------------------
# HALF_OPEN → CLOSED (probe succeeds)
# ---------------------------------------------------------------------------


def test_half_open_to_closed_on_success(monkeypatch):
    breaker = CircuitBreaker("test_recovery", failure_threshold=1, recovery_timeout=1)

    with pytest.raises(RuntimeError):
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("down")))

    # Fast-forward time
    start = time.monotonic()
    monkeypatch.setattr("cam.ingestion.circuit_breaker.time.monotonic", lambda: start + 2)

    assert breaker.state == BreakerState.HALF_OPEN

    # Probe succeeds → CLOSED
    result = breaker.call(lambda: "recovered")
    assert result == "recovered"
    assert breaker.state == BreakerState.CLOSED


# ---------------------------------------------------------------------------
# HALF_OPEN → OPEN (probe fails)
# ---------------------------------------------------------------------------


def test_half_open_to_open_on_probe_failure(monkeypatch):
    breaker = CircuitBreaker("test_probe_fail", failure_threshold=1, recovery_timeout=1)

    with pytest.raises(RuntimeError):
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("down")))

    start = time.monotonic()
    monkeypatch.setattr("cam.ingestion.circuit_breaker.time.monotonic", lambda: start + 2)

    assert breaker.state == BreakerState.HALF_OPEN

    # Probe fails → back to OPEN
    with pytest.raises(RuntimeError):
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("still down")))

    assert breaker.state == BreakerState.OPEN


# ---------------------------------------------------------------------------
# Global registry
# ---------------------------------------------------------------------------


def test_get_breaker_returns_same_instance():
    b1 = get_breaker("edgar")
    b2 = get_breaker("edgar")
    assert b1 is b2


def test_get_breaker_different_sources_are_independent():
    b_osha = get_breaker("osha_reg_test")
    b_epa = get_breaker("epa_reg_test")
    assert b_osha is not b_epa


def test_reset_all_clears_registry():
    get_breaker("source_a")
    get_breaker("source_b")
    reset_all()
    # After reset, get_breaker creates fresh instances
    b = get_breaker("source_a")
    assert b.state == BreakerState.CLOSED


# ---------------------------------------------------------------------------
# Manual reset
# ---------------------------------------------------------------------------


def test_manual_reset_clears_open_state():
    breaker = CircuitBreaker("test_manual_reset", failure_threshold=1)

    with pytest.raises(RuntimeError):
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("down")))

    assert breaker.state == BreakerState.OPEN
    breaker.reset()
    assert breaker.state == BreakerState.CLOSED


# ---------------------------------------------------------------------------
# Concurrency: HALF_OPEN allows only a single probe
# ---------------------------------------------------------------------------


def test_half_open_allows_only_one_concurrent_probe():
    """Under concurrency, exactly one caller runs the HALF_OPEN probe.

    WARN ingestion fetches states in parallel threads that share a breaker, so
    multiple callers can reach a HALF_OPEN breaker at once.  Only one should
    probe the recovering API; the rest must fail fast with CircuitOpenError.
    """
    # recovery_timeout=0 → the breaker moves OPEN→HALF_OPEN on the very next
    # call, so no time monkeypatching is needed.
    breaker = CircuitBreaker("test_concurrent_probe", failure_threshold=1, recovery_timeout=0)

    # Open the breaker.  (With recovery_timeout=0 the breaker is eligible to
    # move to HALF_OPEN on the next call, so we don't assert the OPEN state here
    # — merely reading it would consume the transition.)
    with pytest.raises(RuntimeError):
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("down")))

    n_threads = 8
    probe_calls = 0
    probe_calls_lock = threading.Lock()
    probe_entered = threading.Event()
    release_probe = threading.Event()

    def slow_probe():
        nonlocal probe_calls
        with probe_calls_lock:
            probe_calls += 1
        probe_entered.set()
        # Hold the probe open long enough for the other threads to attempt and
        # be rejected while this one is still in flight.
        release_probe.wait(timeout=5)
        return "recovered"

    results: list[str] = []
    results_lock = threading.Lock()
    start = threading.Barrier(n_threads)

    def worker():
        start.wait()
        try:
            breaker.call(slow_probe)
            with results_lock:
                results.append("ok")
        except CircuitOpenError:
            with results_lock:
                results.append("open")

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()

    # Wait until the single probe is in flight, then let the rejected threads
    # settle before releasing it.
    assert probe_entered.wait(timeout=5), "no probe ever started"
    # The non-probing threads reject immediately (no blocking), so give them a
    # brief window to record their results, then release the probe.
    time.sleep(0.1)
    release_probe.set()
    for t in threads:
        t.join(timeout=5)

    assert probe_calls == 1, f"expected exactly one probe, got {probe_calls}"
    assert results.count("open") == n_threads - 1
    assert results.count("ok") == 1
    # A successful probe closes the breaker.
    assert breaker.state == BreakerState.CLOSED
