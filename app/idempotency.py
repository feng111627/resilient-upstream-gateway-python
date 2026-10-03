"""Idempotency-key store with single-flight semantics.

Guarantees:

* The first request with a given key executes the side-effecting
  function exactly once.
* Concurrent duplicates of an in-flight key wait for the same future
  instead of triggering a second side effect (single-flight).
* Once completed, the stored result is replayed for subsequent
  duplicates until the TTL expires.
* Reusing a key with a *different* payload fingerprint raises
  :class:`~app.errors.IdempotencyConflictError` instead of silently
  returning an unrelated result.

Entries and waiters are guarded by an ``asyncio.Lock``; the store is
in-memory and process-local, which is sufficient for a single-instance
gateway (swap in Redis for multi-instance deployments).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from app.errors import IdempotencyConflictError


@dataclass
class _Entry:
    fingerprint: str
    expires_at: float
    future: asyncio.Future = field(default_factory=asyncio.Future)


def _swallow_unretrieved_exception(future: asyncio.Future) -> None:
    if not future.cancelled():
        future.exception()


class IdempotencyStore:
    def __init__(self, *, ttl: float = 3600.0, clock: Callable[[], float] | None = None) -> None:
        self._ttl = ttl
        self._clock = clock or time.monotonic
        self._entries: dict[str, _Entry] = {}
        self._lock = asyncio.Lock()

    async def execute(
        self,
        key: str,
        payload_fingerprint: str,
        fn: Callable[[], Awaitable[Any]],
    ) -> Any:
        entry, is_owner = await self._claim(key, payload_fingerprint)
        if not is_owner:
            # Duplicate of a completed or in-flight request: replay/await it.
            return await asyncio.shield(entry.future)
        # Silences "exception was never retrieved" when a failed entry has
        # no concurrent waiter; awaiting waiters still receive it normally.
        entry.future.add_done_callback(_swallow_unretrieved_exception)
        try:
            result = await fn()
        except BaseException as exc:
            entry.future.set_exception(exc)
            # Failures are not cached: a later retry with the same key
            # must be allowed to execute the side effect again.
            async with self._lock:
                if self._entries.get(key) is entry:
                    del self._entries[key]
            raise
        entry.future.set_result(result)
        return result

    async def _claim(self, key: str, fingerprint: str) -> tuple[_Entry, bool]:
        async with self._lock:
            self._evict_expired()
            existing = self._entries.get(key)
            if existing is not None:
                if existing.fingerprint != fingerprint:
                    raise IdempotencyConflictError(
                        "idempotency key reused with a different payload",
                        detail={"key": key},
                    )
                return existing, False
            entry = _Entry(fingerprint=fingerprint, expires_at=self._clock() + self._ttl)
            self._entries[key] = entry
            return entry, True

    def _evict_expired(self) -> None:
        now = self._clock()
        expired = [k for k, e in self._entries.items() if e.expires_at <= now and e.future.done()]
        for k in expired:
            del self._entries[k]

    @property
    def size(self) -> int:
        return len(self._entries)
