"""Async-safe circuit breaker with CLOSED / OPEN / HALF_OPEN states.

State machine
-------------
CLOSED     -- requests flow through; consecutive counted failures are
              tracked.  When the count reaches ``failure_threshold`` the
              breaker trips to OPEN.
OPEN       -- requests are rejected immediately with
              :class:`~app.errors.CircuitOpenError`.  After
              ``recovery_timeout`` seconds (measured with an injectable
              clock so tests control recovery timing) the next request
              moves the breaker to HALF_OPEN.
HALF_OPEN  -- up to ``half_open_max_probes`` concurrent probe requests
              are allowed through; the rest are rejected.  A successful
              probe closes the breaker; a failed probe re-opens it and
              restarts the recovery timer.

All state transitions are serialized with an ``asyncio.Lock`` so
concurrent coroutines can never observe or write a torn state.
"""

from __future__ import annotations

import asyncio
import enum
import time
from collections.abc import Callable

from app.errors import (
    CircuitOpenError,
    GatewayError,
    RetriesExhaustedError,
    UpstreamConnectionError,
    UpstreamServerError,
    UpstreamTimeoutError,
)

#: Failures that count towards tripping the breaker (transient upstream
#: faults, including a retry block that exhausted all attempts).
DEFAULT_COUNTED_ERRORS: tuple[type[GatewayError], ...] = (
    UpstreamTimeoutError,
    UpstreamConnectionError,
    UpstreamServerError,
    RetriesExhaustedError,
)


class CircuitState(str, enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_probes: int = 1,
        clock: Callable[[], float] | None = None,
        counted_errors: tuple[type[GatewayError], ...] = DEFAULT_COUNTED_ERRORS,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_max_probes = half_open_max_probes
        self._clock = clock or time.monotonic
        self._counted_errors = counted_errors

        self._lock = asyncio.Lock()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._opened_at = 0.0
        self._half_open_in_flight = 0

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count

    async def __aenter__(self) -> "CircuitBreaker":
        async with self._lock:
            if self._state is CircuitState.OPEN:
                if self._clock() - self._opened_at >= self._recovery_timeout:
                    # Recovery window elapsed: allow probes.
                    self._state = CircuitState.HALF_OPEN
                else:
                    raise CircuitOpenError(
                        "circuit is open",
                        detail={
                            "retry_after": max(
                                0.0, self._recovery_timeout - (self._clock() - self._opened_at)
                            )
                        },
                    )
            if self._state is CircuitState.HALF_OPEN:
                if self._half_open_in_flight >= self._half_open_max_probes:
                    raise CircuitOpenError("half-open probe limit reached")
                self._half_open_in_flight += 1
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        if exc_type is None:
            await self._on_success()
        elif isinstance(exc, self._counted_errors):
            await self._on_failure()
        # Never swallow exceptions.
        return False

    async def _on_success(self) -> None:
        async with self._lock:
            if self._state is CircuitState.HALF_OPEN:
                self._half_open_in_flight = max(0, self._half_open_in_flight - 1)
                if self._half_open_in_flight == 0:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
            elif self._state is CircuitState.CLOSED:
                self._failure_count = 0

    async def _on_failure(self) -> None:
        async with self._lock:
            if self._state is CircuitState.HALF_OPEN:
                self._half_open_in_flight = max(0, self._half_open_in_flight - 1)
                self._state = CircuitState.OPEN
                self._opened_at = self._clock()
            elif self._state is CircuitState.CLOSED:
                self._failure_count += 1
                if self._failure_count >= self._failure_threshold:
                    self._state = CircuitState.OPEN
                    self._opened_at = self._clock()
