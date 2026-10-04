"""弹性客户端端到端测试：正常调用 + 各类失败 + 并发边界。

全部通过本地 MockUpstream 验证，不依赖任何外部服务。
"""

import threading

import pytest

from gateway.circuit_breaker import CircuitBreaker, CircuitState
from gateway.client import ResilientUpstreamClient
from gateway.errors import (
    CircuitOpenError,
    ErrorKind,
    RateLimitedError,
    SchemaMismatchError,
    UpstreamClientError,
    UpstreamConnectionError,
    UpstreamServerError,
    UpstreamTimeoutError,
    VersionUnsupportedError,
)
from gateway.idempotency import IdempotencyStore
from gateway.rate_limiter import TokenBucketRateLimiter
from gateway.retry import RetryPolicy
from tests.mock_upstream import MockUpstream


PAYLOAD_V1 = {"amount": 100}
PAYLOAD_V2 = {"amount": 100, "currency": "CNY"}


def make_client(upstream: MockUpstream, **overrides) -> ResilientUpstreamClient:
    params = {
        "base_url": "http://mock",
        "transport": upstream.transport,
        "retry_policy": RetryPolicy(max_attempts=3, base_delay=0.01, jitter=0.0),
        "sleep_func": lambda _: None,  # 测试里不真实等待
    }
    params.update(overrides)
    return ResilientUpstreamClient(**params)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class TestHappyPath:
    def test_normal_call_v1(self):
        up = MockUpstream()
        client = make_client(up)
        result = client.call("/pay", version="v1", payload=PAYLOAD_V1)
        assert result["status"] == "ok"
        assert result["version"] == "v1"
        assert up.calls == 1

    def test_normal_call_v2_default_version(self):
        up = MockUpstream()
        client = make_client(up)
        result = client.call("/pay", payload=PAYLOAD_V2)
        assert result["version"] == "v2"
        assert result["currency"] == "CNY"


class TestFailureClassification:
    """每类失败都要归一成可区分的错误结论，不透出底层异常。"""

    def test_timeout_is_classified_and_retried(self):
        up = MockUpstream("timeout")
        client = make_client(up)
        with pytest.raises(UpstreamTimeoutError) as exc_info:
            client.call("/pay", payload=PAYLOAD_V2)
        assert exc_info.value.kind is ErrorKind.TIMEOUT
        assert up.calls == 3  # 重试到上限

    def test_connection_failure_is_classified(self):
        up = MockUpstream("conn_error")
        client = make_client(up)
        with pytest.raises(UpstreamConnectionError) as exc_info:
            client.call("/pay", payload=PAYLOAD_V2)
        assert exc_info.value.kind is ErrorKind.CONNECTION
        assert "ConnectError" not in str(exc_info.value)  # 不透出底层异常名

    def test_upstream_4xx_is_not_retried(self):
        up = MockUpstream("bad_request")
        client = make_client(up)
        with pytest.raises(UpstreamClientError) as exc_info:
            client.call("/pay", payload=PAYLOAD_V2)
        assert exc_info.value.kind is ErrorKind.UPSTREAM_CLIENT_ERROR
        assert exc_info.value.retryable is False
        assert up.calls == 1  # 4xx 不重试

    def test_upstream_5xx_is_retried_then_raises(self):
        up = MockUpstream("server_error")
        client = make_client(up)
        with pytest.raises(UpstreamServerError) as exc_info:
            client.call("/pay", payload=PAYLOAD_V2)
        assert exc_info.value.kind is ErrorKind.UPSTREAM_SERVER_ERROR
        assert up.calls == 3

    def test_flaky_upstream_recovers_via_retry(self):
        up = MockUpstream("flaky", flaky_failures=2)
        client = make_client(up)
        result = client.call("/pay", payload=PAYLOAD_V2)
        assert result["status"] == "ok"
        assert up.calls == 3  # 2 次失败 + 第 3 次成功

    def test_schema_mismatch_gives_clear_conclusion(self):
        up = MockUpstream("missing_field")
        client = make_client(up)
        with pytest.raises(SchemaMismatchError) as exc_info:
            client.call("/pay", payload=PAYLOAD_V2)
        assert "amount" in exc_info.value.message
        assert up.calls == 1  # 结构问题不重试

    def test_unsupported_version_fails_before_any_request(self):
        up = MockUpstream()
        client = make_client(up)
        with pytest.raises(VersionUnsupportedError):
            client.call("/pay", version="v9")
        assert up.calls == 0


class TestCircuitBreakerIntegration:
    def test_circuit_opens_after_failures_and_fast_fails(self):
        up = MockUpstream("server_error")
        clock = FakeClock()
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=10.0, time_func=clock)
        client = make_client(up, circuit_breaker=cb,
                             retry_policy=RetryPolicy(max_attempts=1))
        for _ in range(2):
            with pytest.raises(UpstreamServerError):
                client.call("/pay", payload=PAYLOAD_V2)
        assert cb.state is CircuitState.OPEN
        calls_before = up.calls
        with pytest.raises(CircuitOpenError):
            client.call("/pay", payload=PAYLOAD_V2)
        assert up.calls == calls_before  # 熔断期间不打到上游

    def test_circuit_recovers_after_timeout(self):
        up = MockUpstream("flaky", flaky_failures=2)
        clock = FakeClock()
        cb = CircuitBreaker(failure_threshold=2, success_threshold=1,
                            recovery_timeout=10.0, time_func=clock)
        client = make_client(up, circuit_breaker=cb,
                             retry_policy=RetryPolicy(max_attempts=1))
        for _ in range(2):
            with pytest.raises(UpstreamServerError):
                client.call("/pay", payload=PAYLOAD_V2)
        assert cb.state is CircuitState.OPEN
        clock.now += 10.0  # 恢复时机由注入时钟控制
        result = client.call("/pay", payload=PAYLOAD_V2)  # 半开探针成功
        assert result["status"] == "ok"
        assert cb.state is CircuitState.CLOSED

    def test_concurrent_calls_dont_corrupt_circuit_state(self):
        up = MockUpstream("server_error")
        cb = CircuitBreaker(failure_threshold=5, recovery_timeout=60.0)
        client = make_client(up, circuit_breaker=cb,
                             retry_policy=RetryPolicy(max_attempts=1))
        errors = []

        def worker():
            try:
                client.call("/pay", payload=PAYLOAD_V2)
            except Exception as exc:  # noqa: BLE001 - 收集所有结论
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(30)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert cb.state is CircuitState.OPEN
        assert all(isinstance(e, (UpstreamServerError, CircuitOpenError)) for e in errors)
        assert len(errors) == 30


class TestRateLimiting:
    def test_rate_limiter_rejects_when_exhausted(self):
        up = MockUpstream()
        clock = FakeClock()
        limiter = TokenBucketRateLimiter(rate=1.0, capacity=2.0, time_func=clock)
        client = make_client(up, rate_limiter=limiter)
        client.call("/pay", payload=PAYLOAD_V2)
        client.call("/pay", payload=PAYLOAD_V2)
        with pytest.raises(RateLimitedError) as exc_info:
            client.call("/pay", payload=PAYLOAD_V2)
        assert exc_info.value.kind is ErrorKind.RATE_LIMITED
        assert up.calls == 2  # 被限流的请求没打到上游
        clock.now += 1.0     # 令牌补充后恢复
        assert client.call("/pay", payload=PAYLOAD_V2)["status"] == "ok"


class TestIdempotency:
    def test_duplicate_key_returns_same_result_without_second_call(self):
        up = MockUpstream()
        client = make_client(up)
        r1 = client.call("/pay", payload=PAYLOAD_V2, idempotency_key="order-1")
        r2 = client.call("/pay", payload=PAYLOAD_V2, idempotency_key="order-1")
        assert r1 == r2
        assert up.calls == 1  # 重复提交不产生第二次副作用

    def test_duplicate_key_replays_failure_conclusion(self):
        up = MockUpstream("bad_request")
        client = make_client(up)
        with pytest.raises(UpstreamClientError):
            client.call("/pay", payload=PAYLOAD_V2, idempotency_key="order-2")
        with pytest.raises(UpstreamClientError):
            client.call("/pay", payload=PAYLOAD_V2, idempotency_key="order-2")
        assert up.calls == 1

    def test_concurrent_duplicate_keys_cause_single_side_effect(self):
        up = MockUpstream()
        client = make_client(up, idempotency_store=IdempotencyStore())
        results, errors = [], []

        def worker():
            try:
                results.append(client.call("/pay", payload=PAYLOAD_V2, idempotency_key="order-3"))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors
        assert len(results) == 20
        assert all(r == results[0] for r in results)
        assert up.calls == 1  # 20 个并发重复提交只产生 1 次上游调用
