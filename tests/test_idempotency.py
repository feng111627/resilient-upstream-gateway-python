"""Idempotency: single-flight, replay, conflict and failure semantics."""

from __future__ import annotations

import asyncio

import pytest

from app.errors import IdempotencyConflictError, UpstreamServerError
from app.idempotency import IdempotencyStore


async def test_side_effect_runs_once_and_result_is_replayed():
    store = IdempotencyStore()
    calls = 0

    async def effect() -> dict:
        nonlocal calls
        calls += 1
        return {"tx": "tx-1"}

    first = await store.execute("key-1", "fp", effect)
    second = await store.execute("key-1", "fp", effect)
    assert first == second == {"tx": "tx-1"}
    assert calls == 1


async def test_concurrent_duplicates_share_one_execution():
    store = IdempotencyStore()
    calls = 0
    started = asyncio.Event()

    async def effect() -> str:
        nonlocal calls
        calls += 1
        started.set()
        await asyncio.sleep(0.05)  # simulate slow upstream
        return "done"

    results = await asyncio.gather(*(store.execute("key-2", "fp", effect) for _ in range(10)))
    assert results == ["done"] * 10
    assert calls == 1  # single-flight: only one real execution


async def test_same_key_different_payload_conflicts():
    store = IdempotencyStore()

    async def effect() -> str:
        return "done"

    await store.execute("key-3", "fp-A", effect)
    with pytest.raises(IdempotencyConflictError):
        await store.execute("key-3", "fp-B", effect)


async def test_failed_execution_is_not_cached_and_can_be_retried():
    store = IdempotencyStore()
    calls = 0

    async def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise UpstreamServerError("boom", upstream_status=500)
        return "recovered"

    with pytest.raises(UpstreamServerError):
        await store.execute("key-4", "fp", flaky)
    # Same key may be retried after a failure: the side effect runs again.
    assert await store.execute("key-4", "fp", flaky) == "recovered"
    assert calls == 2


async def test_different_keys_are_independent():
    store = IdempotencyStore()
    calls = 0

    async def effect() -> str:
        nonlocal calls
        calls += 1
        return f"result-{calls}"

    assert await store.execute("key-A", "fp", effect) == "result-1"
    assert await store.execute("key-B", "fp", effect) == "result-2"
    assert calls == 2
