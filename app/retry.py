"""Retry with exponential backoff and jitter.

Only *transient* failures (timeout, connection error, upstream 5xx) are
retried.  Deterministic failures (upstream 4xx, circuit open, rate
limited, version/field problems) propagate immediately -- retrying them
would only add load.

When every attempt fails transiently, a
:class:`~app.errors.RetriesExhaustedError` is raised so the caller gets
one normalized conclusion instead of the raw last exception.

The sleep function is injectable so tests can run without real delays.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

from app.errors import (
    GatewayError,
    RetriesExhaustedError,
    UpstreamConnectionError,
    UpstreamServerError,
    UpstreamTimeoutError,
)

T = TypeVar("T")

#: Error categories worth retrying.
DEFAULT_RETRYABLE: tuple[type[GatewayError], ...] = (
    UpstreamTimeoutError,
    UpstreamConnectionError,
    UpstreamServerError,
)


class RetryPolicy:
    def __init__(
        self,
        *,
        max_attempts: int = 3,
        base_delay: float = 0.1,
        max_delay: float = 2.0,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        retryable: tuple[type[GatewayError, ...]] | tuple[type[GatewayError], ...] = DEFAULT_RETRYABLE,
        rng: random.Random | None = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._sleep = sleep or asyncio.sleep
        self._retryable = retryable
        self._rng = rng or random.Random()

    def _backoff(self, attempt: int) -> float:
        """Full-jitter exponential backoff for the given 1-based attempt."""
        ceiling = min(self._max_delay, self._base_delay * (2 ** (attempt - 1)))
        return self._rng.uniform(0.0, ceiling)

    async def run(self, fn: Callable[[], Awaitable[T]]) -> T:
        last_error: GatewayError | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                return await fn()
            except self._retryable as exc:
                last_error = exc
                if attempt < self._max_attempts:
                    await self._sleep(self._backoff(attempt))
        raise RetriesExhaustedError(
            f"all {self._max_attempts} attempts failed",
            last_error=last_error,
            attempts=self._max_attempts,
        )
