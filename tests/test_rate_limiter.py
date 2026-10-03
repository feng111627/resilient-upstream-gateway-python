"""Token bucket: burst allowed, then limited, refills over time, async-safe."""

from __future__ import annotations

import asyncio

from app.rate_limiter import TokenBucketRateLimiter
from tests.conftest import FakeClock


def make_limiter(clock: FakeClock, **kwargs) -> TokenBucketRateLimiter:
    defaults = dict(rate_per_second=10.0, burst=3, clock=clock)
    defaults.update(kwargs)
    return TokenBucketRateLimiter(**defaults)


async def test_burst_then_limited(fake_clock: FakeClock):
    limiter = make_limiter(fake_clock)
    results = [await limiter.acquire() for _ in range(5)]
    assert results == [True, True, True, False, False]


async def test_refill_after_time_passes(fake_clock: FakeClock):
    limiter = make_limiter(fake_clock)
    for _ in range(3):
        assert await limiter.acquire()
    assert not await limiter.acquire()
    fake_clock.advance(0.2)  # 2 tokens at 10/s
    assert await limiter.acquire()
    assert await limiter.acquire()
    assert not await limiter.acquire()


async def test_tokens_capped_at_burst(fake_clock: FakeClock):
    limiter = make_limiter(fake_clock)
    fake_clock.advance(100.0)  # ages pass: bucket must not exceed burst
    results = [await limiter.acquire() for _ in range(4)]
    assert results == [True, True, True, False]


async def test_concurrent_acquires_never_exceed_burst(fake_clock: FakeClock):
    limiter = make_limiter(fake_clock, burst=10, rate_per_second=1.0)
    results = await asyncio.gather(*(limiter.acquire() for _ in range(50)))
    assert sum(results) == 10  # exactly the burst, no token spent twice
