"""多上游隔离：一个上游熔断/限流/占满名额/卡慢，不影响其他上游。"""

import threading
import time

import pytest

from gateway.circuit_breaker import CircuitState
from gateway.errors import (
    CapacityFullError,
    CircuitOpenError,
    UnknownUpstreamError,
    UpstreamServerError,
)
from gateway.registry import UpstreamConfig, UpstreamRegistry
from gateway.retry import RetryPolicy
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
    def test_same_idempotency_key_on_different_upstreams_executes_twice(self):
        """同一幂等键打到两个不同上游：是两次不同的请求，互不影响、各执行一次。"""
        a, b = MockUpstream(), MockUpstream()
        registry = make_registry(alpha=a, beta=b)
        r_a = registry.call("alpha", "/pay", payload=PAYLOAD_V2, idempotency_key="dup-key")
        r_b = registry.call("beta", "/pay", payload=PAYLOAD_V2, idempotency_key="dup-key")
        assert r_a["status"] == r_b["status"] == "ok"
        assert a.calls == 1 and b.calls == 1  # 没有跨上游复用结论
        # 各自内部重复提交仍然复用
        registry.call("alpha", "/pay", payload=PAYLOAD_V2, idempotency_key="dup-key")
        registry.call("beta", "/pay", payload=PAYLOAD_V2, idempotency_key="dup-key")
        assert a.calls == 1 and b.calls == 1

    def test_shared_idempotency_store_conflicts_across_upstreams(self):
        """显式共享同一个幂等存储时（如外置 Redis 场景），指纹包含上游名，
        同键打不同上游会明确报冲突，而不是把 A 的结论丢给 B。"""
        from gateway.client import ResilientUpstreamClient
        from gateway.errors import IdempotencyConflictError
        from gateway.idempotency import IdempotencyStore

        a, b = MockUpstream(), MockUpstream()
        shared = IdempotencyStore()
        client_a = ResilientUpstreamClient(
            base_url="http://alpha", name="alpha", transport=a.transport,
            retry_policy=RetryPolicy(max_attempts=1), idempotency_store=shared,
            sleep_func=lambda _: None,
        )
        client_b = ResilientUpstreamClient(
            base_url="http://beta", name="beta", transport=b.transport,
            retry_policy=RetryPolicy(max_attempts=1), idempotency_store=shared,
            sleep_func=lambda _: None,
        )
        registry = UpstreamRegistry()
        registry.register_client("alpha", client_a)
        registry.register_client("beta", client_b)
        assert registry.call("alpha", "/pay", payload=PAYLOAD_V2,
                             idempotency_key="shared-key")["status"] == "ok"
        with pytest.raises(IdempotencyConflictError):
            registry.call("beta", "/pay", payload=PAYLOAD_V2,
                          idempotency_key="shared-key")
        assert a.calls == 1 and b.calls == 0
        # 原键在 alpha 上的结论没被弄坏
        assert registry.call("alpha", "/pay", payload=PAYLOAD_V2,
                             idempotency_key="shared-key")["status"] == "ok"
        assert a.calls == 1

    def test_same_key_switching_path_or_version_on_one_upstream_conflicts(self):
        """注册表入口上，同键换接口/换版本同样明确报冲突、不打上游。"""
        from gateway.errors import IdempotencyConflictError

        up = MockUpstream()
        registry = UpstreamRegistry()
        registry.register(
            UpstreamConfig(name="pay", base_url="http://pay", transport=up.transport,
                           max_attempts=1, sleep_func=lambda _: None))
        registry.call("pay", "/orders/create", payload=PAYLOAD_V2, idempotency_key="biz-1")
        with pytest.raises(IdempotencyConflictError):  # 换接口
            registry.call("pay", "/refund/create", payload=PAYLOAD_V2, idempotency_key="biz-1")
        with pytest.raises(IdempotencyConflictError):  # 换版本
            registry.call("pay", "/orders/create", version="v1",
                          payload={"amount": 100}, idempotency_key="biz-1")
        with pytest.raises(IdempotencyConflictError):  # 换报文
            registry.call("pay", "/orders/create",
                          payload={"amount": 200, "currency": "CNY"},
                          idempotency_key="biz-1")
        assert up.calls == 1  # 三次冲突提交都没打到上游
        # 原请求重复提交仍复用
        assert registry.call("pay", "/orders/create", payload=PAYLOAD_V2,
                             idempotency_key="biz-1")["status"] == "ok"
        assert up.calls == 1

    def test_register_externally_built_client_routes_and_reports_health(self):
        """老式构造的客户端登记进注册表：按名路由、健康状态都正常。"""
        from gateway.client import ResilientUpstreamClient

        up = MockUpstream()
        client = ResilientUpstreamClient(
            base_url="http://legacy", transport=up.transport,
            sleep_func=lambda _: None,
        )
        registry = UpstreamRegistry()
        registry.register_client("legacy", client)
        assert registry.names() == ["legacy"]
        assert registry.call("legacy", "/pay", payload=PAYLOAD_V2)["status"] == "ok"
        health = registry.health("legacy")
        assert health["circuit"] == CircuitState.CLOSED.value
        assert health["max_in_flight"] is None  # 老客户端没配舱壁，不报假数据

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


class TestHealth:
    def test_health_all_reports_each_upstream_separately(self):
        registry = make_registry(alpha=MockUpstream(), beta=MockUpstream())
        health = registry.health_all()
        assert set(health) == {"alpha", "beta"}
        for name in ("alpha", "beta"):
            assert health[name]["circuit"] == "closed"
            assert health[name]["in_flight"] == 0
            assert health[name]["available_tokens"] > 0
