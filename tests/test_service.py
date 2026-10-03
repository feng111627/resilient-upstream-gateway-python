"""End-to-end orchestration: limiter -> idempotency -> breaker -> retry -> upstream."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from app.circuit_breaker import CircuitBreaker, CircuitState
from app.errors import (
    CircuitOpenError,
    IdempotencyConflictError,
    MissingFieldError,
    RateLimitedError,
    RetriesExhaustedError,
    UnsupportedVersionError,
    UpstreamClientError,
)
from app.idempotency import IdempotencyStore
from app.rate_limiter import TokenBucketRateLimiter
from app.retry import RetryPolicy
from app.service import GatewayService
from app.upstream import UpstreamClient
from tests.conftest import FakeClock, FakeSleep, MockUpstream


def make_service(
    mock_upstream: MockUpstream,
    fake_clock: FakeClock,
    fake_sleep: FakeSleep,
    **overrides,
) -> GatewayService:
    upstream = UpstreamClient(
        base_url="http://mock-upstream",
        transport=httpx.MockTransport(mock_upstream.handler),
    )
    kwargs = dict(
        upstream=upstream,
        breaker=CircuitBreaker(
            failure_threshold=3, recovery_timeout=10.0, clock=fake_clock
        ),
        limiter=TokenBucketRateLimiter(rate_per_second=100.0, burst=100, clock=fake_clock),
        retry=RetryPolicy(max_attempts=3, base_delay=0.01, sleep=fake_sleep),
        idempotency=IdempotencyStore(),
    )
    kwargs.update(overrides)
    return GatewayService(**kwargs)


def order(key: str = "key-1", **kwargs) -> dict:
    base = dict(order_id="o-1", amount=9.5, idempotency_key=key)
    base.update(kwargs)
    return base


async def test_success_v1(mock_upstream, fake_clock, fake_sleep):
    service = make_service(mock_upstream, fake_clock, fake_sleep)
    result = await service.submit_order(**order())
    assert result == {"status": "ok", "transaction_id": "tx-v1"}
    assert mock_upstream.last_payload() == {"order_id": "o-1", "amount": 9.5}


async def test_success_v2(mock_upstream, fake_clock, fake_sleep):
    mock_upstream.default = "ok_v2"
    service = make_service(mock_upstream, fake_clock, fake_sleep)
    result = await service.submit_order(**order(version="v2"))
    assert result == {"status": "ok", "transaction_id": "tx-v2"}
    assert mock_upstream.last_payload() == {
        "order": {"id": "o-1", "amount": 9.5},
        "currency": "CNY",
    }


async def test_transient_failure_retried_then_succeeds(mock_upstream, fake_clock, fake_sleep):
    mock_upstream.script = ["status_500", "timeout"]
    mock_upstream.default = "ok_v1"
    service = make_service(mock_upstream, fake_clock, fake_sleep)
    result = await service.submit_order(**order())
    assert result["transaction_id"] == "tx-v1"
    assert mock_upstream.call_count == 3  # two transient failures, then success
    assert len(fake_sleep.calls) == 2


async def test_persistent_5xx_exhausts_retries(mock_upstream, fake_clock, fake_sleep):
    mock_upstream.default = "status_500"
    service = make_service(mock_upstream, fake_clock, fake_sleep)
    with pytest.raises(RetriesExhaustedError) as exc_info:
        await service.submit_order(**order())
    assert exc_info.value.attempts == 3
    assert mock_upstream.call_count == 3


async def test_4xx_not_retried(mock_upstream, fake_clock, fake_sleep):
    mock_upstream.default = "status_422"
    service = make_service(mock_upstream, fake_clock, fake_sleep)
    with pytest.raises(UpstreamClientError):
        await service.submit_order(**order())
    assert mock_upstream.call_count == 1  # deterministic: no retry
    assert fake_sleep.calls == []


async def test_circuit_opens_and_fails_fast(mock_upstream, fake_clock, fake_sleep):
    mock_upstream.default = "status_500"
    service = make_service(mock_upstream, fake_clock, fake_sleep)
    for i in range(3):
        with pytest.raises(RetriesExhaustedError):
            await service.submit_order(**order(key=f"key-{i}"))
    assert service._breaker.state is CircuitState.OPEN
    calls_before = mock_upstream.call_count
    with pytest.raises(CircuitOpenError):
        await service.submit_order(**order(key="key-blocked"))
    assert mock_upstream.call_count == calls_before  # upstream untouched


async def test_circuit_recovers_after_timeout(mock_upstream, fake_clock, fake_sleep):
    mock_upstream.default = "status_500"
    service = make_service(mock_upstream, fake_clock, fake_sleep)
    for i in range(3):
        with pytest.raises(RetriesExhaustedError):
            await service.submit_order(**order(key=f"key-{i}"))
    fake_clock.advance(10.0)
    mock_upstream.default = "ok_v1"
    result = await service.submit_order(**order(key="key-recovered"))
    assert result["transaction_id"] == "tx-v1"
    assert service._breaker.state is CircuitState.CLOSED


async def test_rate_limited(mock_upstream, fake_clock, fake_sleep):
    limiter = TokenBucketRateLimiter(rate_per_second=1.0, burst=1, clock=fake_clock)
    service = make_service(mock_upstream, fake_clock, fake_sleep, limiter=limiter)
    await service.submit_order(**order(key="k1"))
    with pytest.raises(RateLimitedError):
        await service.submit_order(**order(key="k2"))
    assert mock_upstream.call_count == 1


async def test_idempotent_replay_hits_upstream_once(mock_upstream, fake_clock, fake_sleep):
    service = make_service(mock_upstream, fake_clock, fake_sleep)
    first = await service.submit_order(**order())
    second = await service.submit_order(**order())
    assert first == second
    assert mock_upstream.call_count == 1


async def test_concurrent_duplicate_submissions_single_side_effect(
    mock_upstream, fake_clock, fake_sleep
):
    service = make_service(mock_upstream, fake_clock, fake_sleep)
    results = await asyncio.gather(*(service.submit_order(**order()) for _ in range(8)))
    assert len({r["transaction_id"] for r in results}) == 1
    assert mock_upstream.call_count == 1


async def test_same_key_different_payload_conflicts(mock_upstream, fake_clock, fake_sleep):
    service = make_service(mock_upstream, fake_clock, fake_sleep)
    await service.submit_order(**order())
    with pytest.raises(IdempotencyConflictError):
        await service.submit_order(**order(amount=100.0))


async def test_unsupported_version_rejected_before_upstream(
    mock_upstream, fake_clock, fake_sleep
):
    service = make_service(mock_upstream, fake_clock, fake_sleep)
    with pytest.raises(UnsupportedVersionError):
        await service.submit_order(**order(version="v9"))
    assert mock_upstream.call_count == 0


async def test_missing_upstream_field_is_explicit_error(mock_upstream, fake_clock, fake_sleep):
    mock_upstream.default = "missing_field_v1"
    service = make_service(mock_upstream, fake_clock, fake_sleep)
    with pytest.raises(MissingFieldError) as exc_info:
        await service.submit_order(**order())
    assert exc_info.value.detail["field"] == "transaction_id"
