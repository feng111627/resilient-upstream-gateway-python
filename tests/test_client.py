"""弹性客户端端到端测试：正常调用 + 各类失败 + 并发边界。

全部通过本地 MockUpstream 验证，不依赖任何外部服务。
"""

import threading

import pytest

from gateway.circuit_breaker import CircuitBreaker, CircuitState
from gateway.client import ResilientUpstreamClient
from gateway.errors import (
    CapacityFullError,
    CircuitOpenError,
    ErrorKind,
    InvalidJsonError,
    RateLimitedError,
    RequestInvalidError,
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


def make_client(upstream: MockUpstream, **overrides) -> ResilientUpstreamClient:
    params = {
        "base_url": "http://mock",
        "transport": upstream.transport,
        "retry_policy": RetryPolicy(max_attempts=3, base_delay=0.01, jitter=0.0),
        "sleep_func": lambda _: None,  # 测试里不真实等待
    }
    params.update(overrides)
    return ResilientUpstreamClient(**params)


# 符合各版本要求的语义报文（详见 gateway/versioning.py）
V1_PAYLOAD = {"id": "order-1", "amount": 100, "callback_url": "http://cb/hook"}
V2_PAYLOAD = {"id": "order-1", "amount": 100, "currency": "CNY", "note": "午餐"}


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class TestHappyPath:
    def test_normal_call_v1(self):
        up = MockUpstream()
        client = make_client(up)
        result = client.call("/pay", version="v1", payload=V1_PAYLOAD)
        assert result["status"] == "ok"
        assert result["version"] == "v1"
        assert up.calls == 1

    def test_normal_call_v2_default_version(self):
        up = MockUpstream()
        client = make_client(up)
        result = client.call("/pay", payload=V2_PAYLOAD)
        assert result["version"] == "v2"
        assert result["currency"] == "CNY"


class TestFailureClassification:
    """每类失败都要归一成可区分的错误结论，不透出底层异常。"""

    def test_timeout_is_classified_and_retried(self):
        up = MockUpstream("timeout")
        client = make_client(up)
        with pytest.raises(UpstreamTimeoutError) as exc_info:
            client.call("/pay", payload=V2_PAYLOAD)
        assert exc_info.value.kind is ErrorKind.TIMEOUT
        assert up.calls == 3  # 重试到上限

    def test_connection_failure_is_classified(self):
        up = MockUpstream("conn_error")
        client = make_client(up)
        with pytest.raises(UpstreamConnectionError) as exc_info:
            client.call("/pay", payload=V2_PAYLOAD)
        assert exc_info.value.kind is ErrorKind.CONNECTION
        assert "ConnectError" not in str(exc_info.value)  # 不透出底层异常名

    def test_upstream_4xx_is_not_retried(self):
        up = MockUpstream("bad_request")
        client = make_client(up)
        with pytest.raises(UpstreamClientError) as exc_info:
            client.call("/pay", payload=V2_PAYLOAD)
        assert exc_info.value.kind is ErrorKind.UPSTREAM_CLIENT_ERROR
        assert exc_info.value.retryable is False
        assert up.calls == 1  # 4xx 不重试

    def test_upstream_5xx_is_retried_then_raises(self):
        up = MockUpstream("server_error")
        client = make_client(up)
        with pytest.raises(UpstreamServerError) as exc_info:
            client.call("/pay", payload=V2_PAYLOAD)
        assert exc_info.value.kind is ErrorKind.UPSTREAM_SERVER_ERROR
        assert up.calls == 3

    def test_flaky_upstream_recovers_via_retry(self):
        up = MockUpstream("flaky", flaky_failures=2)
        client = make_client(up)
        result = client.call("/pay", payload=V2_PAYLOAD)
        assert result["status"] == "ok"
        assert up.calls == 3  # 2 次失败 + 第 3 次成功

    def test_schema_mismatch_gives_clear_conclusion(self):
        up = MockUpstream("missing_field")
        client = make_client(up)
        with pytest.raises(SchemaMismatchError) as exc_info:
            client.call("/pay", payload=V2_PAYLOAD)
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
                client.call("/pay", payload=V2_PAYLOAD)
        assert cb.state is CircuitState.OPEN
        calls_before = up.calls
        with pytest.raises(CircuitOpenError):
            client.call("/pay", payload=V2_PAYLOAD)
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
                client.call("/pay", payload=V2_PAYLOAD)
        assert cb.state is CircuitState.OPEN
        clock.now += 10.0  # 恢复时机由注入时钟控制
        result = client.call("/pay", payload=V2_PAYLOAD)  # 半开探针成功
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
                client.call("/pay", payload=V2_PAYLOAD)
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
        client.call("/pay", payload=V2_PAYLOAD)
        client.call("/pay", payload=V2_PAYLOAD)
        with pytest.raises(RateLimitedError) as exc_info:
            client.call("/pay", payload=V2_PAYLOAD)
        assert exc_info.value.kind is ErrorKind.RATE_LIMITED
        assert up.calls == 2  # 被限流的请求没打到上游
        clock.now += 1.0     # 令牌补充后恢复
        assert client.call("/pay", payload=V2_PAYLOAD)["status"] == "ok"


class TestIdempotency:
    def test_duplicate_key_returns_same_result_without_second_call(self):
        up = MockUpstream()
        client = make_client(up)
        r1 = client.call("/pay", idempotency_key="order-1", payload=V2_PAYLOAD)
        r2 = client.call("/pay", idempotency_key="order-1", payload=V2_PAYLOAD)
        assert r1 == r2
        assert up.calls == 1  # 重复提交不产生第二次副作用

    def test_duplicate_key_replays_failure_conclusion(self):
        up = MockUpstream("bad_request")
        client = make_client(up)
        with pytest.raises(UpstreamClientError):
            client.call("/pay", idempotency_key="order-2", payload=V2_PAYLOAD)
        with pytest.raises(UpstreamClientError):
            client.call("/pay", idempotency_key="order-2", payload=V2_PAYLOAD)
        assert up.calls == 1

    def test_concurrent_duplicate_keys_cause_single_side_effect(self):
        up = MockUpstream()
        client = make_client(up, idempotency_store=IdempotencyStore())
        results, errors = [], []

        def worker():
            try:
                results.append(client.call("/pay", idempotency_key="order-3", payload=V2_PAYLOAD))
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


class TestBulkheadIntegration:
    """组合客户端层面的并发名额行为：快速失败、成功/失败/放弃都释放名额。"""

    def test_capacity_full_is_distinct_error(self):
        up = MockUpstream("ok")
        from gateway.bulkhead import Bulkhead

        client = make_client(up, bulkhead=Bulkhead(max_concurrent=1, max_waiters=0,
                                                    acquire_timeout=0.0))
        # 先占住唯一名额
        assert client.bulkhead.acquire()
        try:
            with pytest.raises(CapacityFullError) as exc:
                client.call("/pay", payload=V2_PAYLOAD)
            assert exc.value.kind is ErrorKind.CAPACITY_FULL
            assert exc.value.kind is not ErrorKind.TIMEOUT
            assert exc.value.kind is not ErrorKind.CIRCUIT_OPEN
        finally:
            client.bulkhead.release()
        assert up.calls == 0  # 被容量拒绝的请求没打到上游

    def test_slot_released_after_success_and_failure(self):
        from gateway.bulkhead import Bulkhead

        ok_client = make_client(MockUpstream("ok"),
                                bulkhead=Bulkhead(max_concurrent=1, max_waiters=0))
        for _ in range(3):
            assert ok_client.call("/pay", payload=V2_PAYLOAD)["status"] == "ok"
        assert ok_client.bulkhead.active == 0

        err_client = make_client(
            MockUpstream("server_error"),
            bulkhead=Bulkhead(max_concurrent=1, max_waiters=0),
            retry_policy=RetryPolicy(max_attempts=1),
        )
        for _ in range(3):
            with pytest.raises(UpstreamServerError):
                err_client.call("/pay", payload=V2_PAYLOAD)
        assert err_client.bulkhead.active == 0

    def test_blocked_slow_call_then_recovery(self):
        import threading as _t
        from gateway.bulkhead import Bulkhead

        gate = _t.Event()
        up = MockUpstream("blocking", release_gate=gate)
        client = make_client(up, bulkhead=Bulkhead(max_concurrent=1, max_waiters=0))
        done = _t.Event()

        def slow():
            client.call("/pay", payload=V2_PAYLOAD)
            done.set()

        t = _t.Thread(target=slow)
        t.start()
        import time as _time

        _time.sleep(0.05)
        assert client.bulkhead.active == 1
        with pytest.raises(CapacityFullError):
            client.call("/pay", payload=V2_PAYLOAD)
        gate.set()
        assert done.wait(1)
        t.join(1)
        assert client.bulkhead.active == 0
        # 名额归还后可以继续打
        assert client.call("/pay", payload=V2_PAYLOAD)["status"] == "ok"


class TestRequestAndResponseGuards:
    def test_invalid_request_fails_without_hitting_upstream(self):
        up = MockUpstream()
        client = make_client(up)
        with pytest.raises(RequestInvalidError) as exc:
            client.call("/pay", payload={"id": "o1", "amount": 1})  # v2 缺 currency
        assert exc.value.kind is ErrorKind.REQUEST_INVALID
        assert up.calls == 0

    def test_200_non_json_is_invalid_json(self):
        up = MockUpstream("invalid_json")
        client = make_client(up)
        with pytest.raises(InvalidJsonError) as exc:
            client.call("/pay", payload=V2_PAYLOAD)
        assert exc.value.kind is ErrorKind.INVALID_JSON
        assert "JSON" in exc.value.message
        assert up.calls == 1

    def test_malformed_5xx_non_json_is_invalid_json_not_retried(self):
        up = MockUpstream("malformed_json")
        client = make_client(
            up, retry_policy=RetryPolicy(max_attempts=3, base_delay=0.01, jitter=0.0))
        with pytest.raises(InvalidJsonError):
            client.call("/pay", payload=V2_PAYLOAD)
        assert up.calls == 1  # 无法解析的响应不按 5xx 重试
