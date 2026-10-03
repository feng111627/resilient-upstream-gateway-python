"""Retry policy: transient errors retried, deterministic ones not."""

from __future__ import annotations

import pytest

from app.errors import (
    CircuitOpenError,
    RetriesExhaustedError,
    UpstreamClientError,
    UpstreamConnectionError,
    UpstreamServerError,
    UpstreamTimeoutError,
)
from app.retry import RetryPolicy
from tests.conftest import FakeSleep


def make_policy(sleep: FakeSleep, **kwargs) -> RetryPolicy:
    defaults = dict(max_attempts=3, base_delay=0.1, max_delay=2.0, sleep=sleep)
    defaults.update(kwargs)
    return RetryPolicy(**defaults)


async def test_success_first_try(fake_sleep: FakeSleep):
    calls = 0

    async def fn() -> str:
        nonlocal calls
        calls += 1
        return "ok"

    assert await make_policy(fake_sleep).run(fn) == "ok"
    assert calls == 1
    assert fake_sleep.calls == []


@pytest.mark.parametrize(
    "error",
    [
        UpstreamTimeoutError("t"),
        UpstreamConnectionError("c"),
        UpstreamServerError("s", upstream_status=500),
    ],
)
async def test_transient_errors_are_retried_until_success(fake_sleep: FakeSleep, error):
    calls = 0

    async def fn() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise error
        return "recovered"

    assert await make_policy(fake_sleep).run(fn) == "recovered"
    assert calls == 3
    assert len(fake_sleep.calls) == 2  # slept between attempts


async def test_retries_exhausted_raises_normalized_error(fake_sleep: FakeSleep):
    async def fn() -> str:
        raise UpstreamConnectionError("still down")

    with pytest.raises(RetriesExhaustedError) as exc_info:
        await make_policy(fake_sleep, max_attempts=4).run(fn)
    assert exc_info.value.attempts == 4
    assert isinstance(exc_info.value.last_error, UpstreamConnectionError)
    assert len(fake_sleep.calls) == 3


@pytest.mark.parametrize(
    "error",
    [
        UpstreamClientError("bad request", upstream_status=400),
        CircuitOpenError("open"),
    ],
)
async def test_deterministic_errors_are_not_retried(fake_sleep: FakeSleep, error):
    calls = 0

    async def fn() -> str:
        nonlocal calls
        calls += 1
        raise error

    with pytest.raises(type(error)):
        await make_policy(fake_sleep).run(fn)
    assert calls == 1  # no retry, no sleep
    assert fake_sleep.calls == []


async def test_backoff_is_bounded_and_grows(fake_sleep: FakeSleep):
    async def fn() -> str:
        raise UpstreamTimeoutError("t")

    policy = make_policy(fake_sleep, max_attempts=5, base_delay=0.1, max_delay=0.5)
    with pytest.raises(RetriesExhaustedError):
        await policy.run(fn)
    assert len(fake_sleep.calls) == 4
    for delay in fake_sleep.calls:
        assert 0.0 <= delay <= 0.5  # full jitter within the capped ceiling
