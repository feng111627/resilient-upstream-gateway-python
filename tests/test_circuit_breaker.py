"""Circuit breaker state machine, recovery timing and concurrency safety."""

from __future__ import annotations

import asyncio

import pytest

from app.circuit_breaker import CircuitBreaker, CircuitState
from app.errors import CircuitOpenError, UpstreamConnectionError, UpstreamTimeoutError
from tests.conftest import FakeClock


def make_breaker(clock: FakeClock, **kwargs) -> CircuitBreaker:
    defaults = dict(failure_threshold=3, recovery_timeout=10.0, half_open_max_probes=1, clock=clock)
    defaults.update(kwargs)
    return CircuitBreaker(**defaults)


async def fail_once(breaker: CircuitBreaker) -> None:
    with pytest.raises(UpstreamConnectionError):
        async with breaker:
            raise UpstreamConnectionError("boom")


async def test_closed_allows_requests_and_counts_failures(fake_clock: FakeClock):
    breaker = make_breaker(fake_clock)
    async with breaker:
        pass  # success
    assert breaker.state is CircuitState.CLOSED
    await fail_once(breaker)
    assert breaker.state is CircuitState.CLOSED
    assert breaker.failure_count == 1


async def test_trips_open_after_threshold(fake_clock: FakeClock):
    breaker = make_breaker(fake_clock)
    for _ in range(3):
        await fail_once(breaker)
    assert breaker.state is CircuitState.OPEN
    with pytest.raises(CircuitOpenError):
        async with breaker:
            pass


async def test_success_resets_failure_count(fake_clock: FakeClock):
    breaker = make_breaker(fake_clock)
    await fail_once(breaker)
    await fail_once(breaker)
    async with breaker:
        pass  # success resets the consecutive-failure count
    assert breaker.failure_count == 0
    await fail_once(breaker)
    await fail_once(breaker)
    assert breaker.state is CircuitState.CLOSED  # 2 < threshold, still closed


async def test_open_until_recovery_timeout_then_half_open(fake_clock: FakeClock):
    breaker = make_breaker(fake_clock)
    for _ in range(3):
        await fail_once(breaker)
    assert breaker.state is CircuitState.OPEN

    fake_clock.advance(9.9)  # not yet
    with pytest.raises(CircuitOpenError):
        async with breaker:
            pass
    assert breaker.state is CircuitState.OPEN

    fake_clock.advance(0.2)  # now past the recovery timeout
    async with breaker:  # probe allowed in half-open
        pass
    assert breaker.state is CircuitState.CLOSED


async def test_failed_probe_reopens_and_restarts_timer(fake_clock: FakeClock):
    breaker = make_breaker(fake_clock)
    for _ in range(3):
        await fail_once(breaker)
    fake_clock.advance(10.0)
    with pytest.raises(UpstreamTimeoutError):
        async with breaker:
            raise UpstreamTimeoutError("probe failed")
    assert breaker.state is CircuitState.OPEN
    # Timer restarted: another full recovery_timeout must elapse.
    fake_clock.advance(9.9)
    with pytest.raises(CircuitOpenError):
        async with breaker:
            pass


async def test_half_open_rejects_excess_concurrent_probes(fake_clock: FakeClock):
    breaker = make_breaker(fake_clock, half_open_max_probes=1)
    for _ in range(3):
        await fail_once(breaker)
    fake_clock.advance(10.0)

    entered = asyncio.Event()
    release = asyncio.Event()

    async def probe():
        async with breaker:
            entered.set()
            await release.wait()

    task = asyncio.create_task(probe())
    await entered.wait()
    assert breaker.state is CircuitState.HALF_OPEN
    # Second concurrent probe exceeds the probe budget.
    with pytest.raises(CircuitOpenError):
        async with breaker:
            pass
    release.set()
    await task
    assert breaker.state is CircuitState.CLOSED


async def test_concurrent_failures_trip_breaker_exactly_once(fake_clock: FakeClock):
    """Many coroutines failing at once must not corrupt the counters."""
    breaker = make_breaker(fake_clock, failure_threshold=5)

    async def worker():
        with pytest.raises(UpstreamConnectionError):
            async with breaker:
                raise UpstreamConnectionError("boom")

    await asyncio.gather(*(worker() for _ in range(5)))
    assert breaker.state is CircuitState.OPEN
    assert breaker.failure_count == 5


async def test_concurrent_mixed_outcomes_leave_consistent_state(fake_clock: FakeClock):
    breaker = make_breaker(fake_clock, failure_threshold=10)

    async def ok():
        async with breaker:
            await asyncio.sleep(0)

    async def bad():
        with pytest.raises(UpstreamConnectionError):
            async with breaker:
                await asyncio.sleep(0)
                raise UpstreamConnectionError("boom")

    await asyncio.gather(*[ok() if i % 2 else bad() for i in range(50)])
    # State must be one of the legal values and counters non-negative.
    assert breaker.state in set(CircuitState)
    assert breaker.failure_count >= 0
