"""多上游隔离：一个上游熔断/限流/占满名额/卡慢，不影响其他上游。"""

import threading
import time

import pytest

from gateway.circuit_breaker import CircuitState
from gateway.errors import (
    CapacityFullError,
    CircuitOpenError,
    ErrorKind,
    IdempotencyConflictError,
    UnknownUpstreamError,
    UpstreamServerError,
)
from gateway.registry import UpstreamConfig, UpstreamRegistry
from tests.mock_upstream import MockUpstream

PAYLOAD_V2 = {"amount": 100, "currency": "CNY"}


def make_registry(**upstreams: MockUpstream) -> UpstreamRegistry:
    """每个 MockUpstream 注册为同名上游，共享宽松的默认配额。"""
    registry = UpstreamRegistry()
    for name, up in upstreams.items():
        registry.register(
            UpstreamConfig(
                name=name,
                base_url=f"http://{name}",
                transport=up.transport,
                max_attempts=1,
                sleep_func=lambda _: None,
            )
        )
    return registry


class TestRegistration:
    def test_register_and_route_by_name(self):
        registry = make_registry(alpha=MockUpstream(), beta=MockUpstream())
        assert registry.names() == ["alpha", "beta"]
        assert registry.call("alpha", "/pay", payload=PAYLOAD_V2)["status"] == "ok"
        assert registry.call("beta", "/pay", payload=PAYLOAD_V2)["status"] == "ok"

    def test_duplicate_name_rejected(self):
        registry = make_registry(alpha=MockUpstream())
        with pytest.raises(ValueError, match="已登记"):
            registry.register(UpstreamConfig(name="alpha", base_url="http://x"))

    def test_unknown_upstream_gives_clear_error(self):
        registry = make_registry(alpha=MockUpstream())
        with pytest.raises(UnknownUpstreamError):
            registry.call("ghost", "/pay", payload=PAYLOAD_V2)
        with pytest.raises(UnknownUpstreamError):
            registry.health("ghost")


class TestIsolation:
    def test_circuit_of_one_upstream_does_not_affect_others(self):
        bad, good = MockUpstream("server_error"), MockUpstream()
        registry = UpstreamRegistry()
        for name, up in (("bad", bad), ("good", good)):
            registry.register(
                UpstreamConfig(
                    name=name,
                    base_url=f"http://{name}",
                    transport=up.transport,
                    max_attempts=1,
                    failure_threshold=2,
                    sleep_func=lambda _: None,
                )
            )
        for _ in range(2):
            with pytest.raises(UpstreamServerError):
                registry.call("bad", "/pay", payload=PAYLOAD_V2)
        # bad 已熔断并快速失败
        with pytest.raises(CircuitOpenError):
            registry.call("bad", "/pay", payload=PAYLOAD_V2)
        # good 完全不受影响
        assert registry.call("good", "/pay", payload=PAYLOAD_V2)["status"] == "ok"
        assert registry.health("bad")["circuit"] == CircuitState.OPEN.value
        assert registry.health("good")["circuit"] == CircuitState.CLOSED.value

    def test_rate_limit_of_one_upstream_does_not_affect_others(self):
        registry = UpstreamRegistry()
        tight, loose = MockUpstream(), MockUpstream()
        registry.register(
            UpstreamConfig(
                name="tight", base_url="http://tight", transport=tight.transport,
                rate=0.001, rate_capacity=1.0, max_attempts=1, sleep_func=lambda _: None,
            )
        )
        registry.register(
            UpstreamConfig(
                name="loose", base_url="http://loose", transport=loose.transport,
                max_attempts=1, sleep_func=lambda _: None,
            )
        )
        from gateway.errors import RateLimitedError

        registry.call("tight", "/pay", payload=PAYLOAD_V2)  # 用掉 tight 唯一令牌
        with pytest.raises(RateLimitedError):
            registry.call("tight", "/pay", payload=PAYLOAD_V2)
        # loose 的令牌桶是独立的
        assert registry.call("loose", "/pay", payload=PAYLOAD_V2)["status"] == "ok"

    def test_slow_upstream_does_not_hold_other_upstreams_slots(self):
        """慢上游占满自己的名额时，别的上游名额照旧可用。"""
        slow, fast = MockUpstream("slow", slow_delay=0.3), MockUpstream()
        registry = UpstreamRegistry()
        registry.register(
            UpstreamConfig(
                name="slow", base_url="http://slow", transport=slow.transport,
                max_in_flight=1, overflow="reject", max_attempts=1,
                sleep_func=lambda _: None,
            )
        )
        registry.register(
            UpstreamConfig(
                name="fast", base_url="http://fast", transport=fast.transport,
                max_in_flight=1, overflow="reject", max_attempts=1,
                sleep_func=lambda _: None,
            )
        )
        holder = threading.Thread(
            target=lambda: registry.call("slow", "/pay", payload=PAYLOAD_V2)
        )
        holder.start()
        time.sleep(0.05)  # slow 的唯一名额被占住
        with pytest.raises(CapacityFullError):
            registry.call("slow", "/pay", payload=PAYLOAD_V2)  # slow 自己报容量满
        # fast 的名额独立，照常成功
        assert registry.call("fast", "/pay", payload=PAYLOAD_V2)["status"] == "ok"
        holder.join()
        assert registry.health("slow")["in_flight"] == 0  # 慢请求结束后名额归还
        assert registry.health("fast")["in_flight"] == 0

    def test_per_upstream_independent_quotas(self):
        """每个上游的并发额度按各自配置生效。"""
        registry = UpstreamRegistry()
        for name, quota in (("small", 1), ("big", 5)):
            registry.register(
                UpstreamConfig(
                    name=name, base_url=f"http://{name}",
                    transport=MockUpstream().transport,
                    max_in_flight=quota, max_attempts=1, sleep_func=lambda _: None,
                )
            )
        assert registry.health("small")["max_in_flight"] == 1
        assert registry.health("big")["max_in_flight"] == 5


class TestIdempotencyAcrossUpstreams:
    def test_same_key_same_payload_different_upstream_conflicts(self):
        """同键同报文换上游：判定范围含目标上游，明确报冲突，不在新上游重复执行。"""
        alpha, beta = MockUpstream(), MockUpstream()
        registry = make_registry(alpha=alpha, beta=beta)
        registry.call("alpha", "/pay", payload=PAYLOAD_V2, idempotency_key="k-x")
        with pytest.raises(IdempotencyConflictError) as exc_info:
            registry.call("beta", "/pay", payload=PAYLOAD_V2, idempotency_key="k-x")
        assert exc_info.value.kind is ErrorKind.IDEMPOTENCY_CONFLICT
        assert alpha.calls == 1
        assert beta.calls == 0  # 冲突请求没有打到第二个上游

    def test_same_key_identical_request_same_upstream_still_reuses(self):
        """同键、上游/接口/版本/报文完全一致：照常复用，不重复产生副作用。"""
        alpha, beta = MockUpstream(), MockUpstream()
        registry = make_registry(alpha=alpha, beta=beta)
        r1 = registry.call("alpha", "/pay", payload=PAYLOAD_V2, idempotency_key="k-y")
        r2 = registry.call("alpha", "/pay", payload=PAYLOAD_V2, idempotency_key="k-y")
        assert r1 == r2
        assert alpha.calls == 1
        # 冲突不影响原键结论：换上游报过冲突后，原上游正确请求仍复用原结论
        with pytest.raises(IdempotencyConflictError):
            registry.call("beta", "/pay", payload=PAYLOAD_V2, idempotency_key="k-y")
        r3 = registry.call("alpha", "/pay", payload=PAYLOAD_V2, idempotency_key="k-y")
        assert r3 == r1
        assert alpha.calls == 1
        assert beta.calls == 0

    def test_different_keys_on_different_upstreams_are_independent(self):
        """不同幂等键互不干扰：各上游正常各自执行。"""
        alpha, beta = MockUpstream(), MockUpstream()
        registry = make_registry(alpha=alpha, beta=beta)
        r1 = registry.call("alpha", "/pay", payload=PAYLOAD_V2, idempotency_key="k-1")
        r2 = registry.call("beta", "/pay", payload=PAYLOAD_V2, idempotency_key="k-2")
        assert r1["status"] == "ok" and r2["status"] == "ok"
        assert alpha.calls == 1 and beta.calls == 1


class TestHealth:
    def test_health_all_reports_each_upstream_separately(self):
        registry = make_registry(alpha=MockUpstream(), beta=MockUpstream())
        health = registry.health_all()
        assert set(health) == {"alpha", "beta"}
        for name in ("alpha", "beta"):
            assert health[name]["circuit"] == "closed"
            assert health[name]["in_flight"] == 0
            assert health[name]["available_tokens"] > 0
