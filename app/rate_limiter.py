"""Async-safe token-bucket rate limiter.

Tokens are refilled lazily at ``rate_per_second`` up to a maximum of
``burst``.  ``acquire()`` is non-blocking: it either consumes a token and
returns True, or returns False so the caller can reject the request with
a normalized RATE_LIMITED error instead of queueing unboundedly.

An injectable clock keeps the limiter deterministic in tests.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable


class TokenBucketRateLimiter:
    def __init__(
        self,
        *,
        rate_per_second: float,
        burst: int,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be > 0")
        if burst < 1:
            raise ValueError("burst must be >= 1")
        self._rate = rate_per_second
        self._burst = burst
        self._clock = clock or time.monotonic
        self._tokens = float(burst)
        self._last_refill = self._clock()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        """Consume one token if available; never blocks."""
        async with self._lock:
            now = self._clock()
            elapsed = now - self._last_refill
            if elapsed > 0:
                self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
                self._last_refill = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    @property
    def available_tokens(self) -> float:
        return self._tokens
