"""多上游：登记多个命名上游，验证熔断/限流/并发的相互隔离、名额释放与分别健康检查。"""

import threading
import time

import pytest

from gateway.bulkhead import Bulkhead
from gateway.circuit_breaker import CircuitState
from gateway.errors import (
    CapacityFullError,
    CircuitOpenError,
    RateLimitedError,
    UpstreamNotFoundError,
    UpstreamServerError,
)
from gateway.rate_limiter import TokenBucketRateLimiter
from gateway.registry import UpstreamConfig, UpstreamRegistry
from tests.mock_upstream import MockUpstream

V2 = {"id": "o1", "amount": 10, "currency": "CNY"}


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def build_registry(scenarios: dict[str, str], **cfg_overrides) -> tuple[UpstreamRegistry, dict[str, MockUpstream]]:
    """按 {名字: 场景} 搭一个多上游登记处，每个上游注入自己的 MockTransport。"""
    mocks = {name: MockUpstream(scenario) for name, scenario in scenarios.items()}
    registry = UpstreamRegistry(transport_factory=lambda n: mocks[n].transport)
    for name in scenarios:
        cfg = UpstreamConfig(name=name, base_url=f"http://{name}", **cfg_overrides)
        registry.register(cfg)
    return registry, mocks


class TestRegistration:
    def test_register_multiple_named_upstreams(self):
        registry, _ = build_registry({"alpha": "ok", "beta": "ok"})
        assert registry.names() == ["alpha", "beta"]
        assert len(registry) == 2
        assert registry.has("alpha")
        assert "gamma" not in registry

    def test_duplicate_registration_rejected(self):
        registry, _ = build_registry({"alpha": "ok"})
        with pytest.raises(ValueError):
            registry.register(UpstreamConfig(name="alpha", base_url="http://x"))

    def test_unknown_upstream_gives_clear_error(self):
        registry, _ = build_registry({"alpha": "ok"})
        with pytest.raises(UpstreamNotFoundError) as exc:
            registry.get("ghost")
        assert "ghost" in exc.value.message
        assert registry.get("alpha") is not None

    def test_independent_configs_take_effect(self):
        mocks = {"fast": MockUpstream("ok"), "slow": MockUpstream("timeout")}
        registry = UpstreamRegistry(transport_factory=lambda n: mocks[n].transport)
        registry.register(UpstreamConfig("fast", "http://fast", timeout=1.0,
                                         max_attempts=1))
        registry.register(UpstreamConfig("slow", "http://slow", timeout=9.0,
                                         max_attempts=2, base_delay=0.01))
        assert registry.get("fast") is not registry.get("slow")


class TestCircuitIsolation:
    def test_one_upstream_opening_does_not_trip_others(self):
        registry, mocks = build_registry(
            {"bad": "server_error", "good": "ok"},
            failure_threshold=1, max_attempts=1,
        )
        with pytest.raises(UpstreamServerError):
            registry.get("bad").call("/pay", payload=V2)
        assert registry.get("bad").circuit_breaker.state is CircuitState.OPEN
        # bad 已熔断；good 必须照常放行
        result = registry.get("good").call("/pay", payload=V2)
        assert result["status"] == "ok"
        assert mocks["good"].calls == 1
        # bad 持续快速失败，good 再打几次仍正常
        for _ in range(3):
            with pytest.raises(CircuitOpenError):
                registry.get("bad").call("/pay", payload=V2)
        assert registry.get("good").call("/pay", payload=V2)["status"] == "ok"
        assert mocks["good"].calls == 2

    def test_timeout_upstream_does_not_slow_down_other(self):
        # bad 持续超时，good 的调用次数与耗时不受其拖累
        registry, mocks = build_registry(
            {"bad": "timeout", "good": "ok"}, max_attempts=1, failure_threshold=1,
        )
        with pytest.raises(Exception):
            registry.get("bad").call("/pay", payload=V2)
        start = time.monotonic()
        for _ in range(5):
            assert registry.get("good").call("/pay", payload=V2)["status"] == "ok"
        assert time.monotonic() - start < 1.0
        assert registry.get("bad").circuit_breaker.state is CircuitState.OPEN
        assert registry.get("good").circuit_breaker.state is CircuitState.CLOSED


class TestRateLimitIsolation:
    def test_quotas_are_separate_per_upstream(self):
        mocks = {"a": MockUpstream("ok"), "b": MockUpstream("ok")}
        clock = FakeClock()
        registry = UpstreamRegistry(
            transport_factory=lambda n: mocks[n].transport, time_func=clock)
        for name in ("a", "b"):
            # 每个上游各自 2 个令牌，共享同一个假时钟
            registry.register(UpstreamConfig(
                name, f"http://{name}", max_attempts=1,
                rate_limit_rate=1.0, rate_limit_capacity=2.0,
            ))
        ca, cb = registry.get("a"), registry.get("b")
        ca.call("/pay", payload=V2)
        ca.call("/pay", payload=V2)
        with pytest.raises(RateLimitedError):
            ca.call("/pay", payload=V2)
        # a 令牌耗尽不影响 b：b 的 2 个额度原封不动
        cb.call("/pay", payload=V2)
        cb.call("/pay", payload=V2)
        with pytest.raises(RateLimitedError):
            cb.call("/pay", payload=V2)
        assert mocks["a"].calls == 2 and mocks["b"].calls == 2
        clock.now += 1.0  # 各自补令牌
        assert ca.call("/pay", payload=V2)["status"] == "ok"
        assert cb.call("/pay", payload=V2)["status"] == "ok"


class TestBulkheadIsolation:
    def _registry_with_bulkheads(self, specs):
        mocks = {n: m for n, m, _ in specs}
        registry = UpstreamRegistry(transport_factory=lambda n: mocks[n].transport)
        for name, mock, bh in specs:
            client = registry.register(UpstreamConfig(
                name, f"http://{name}", max_attempts=1,
            ))
            client._bulkhead = bh
        return registry

    def test_slow_upstream_holds_only_its_own_slots(self):
        gate = threading.Event()
        slow_mock = MockUpstream("blocking", release_gate=gate)
        fast_mock = MockUpstream("ok")
        registry = self._registry_with_bulkheads([
            ("slow", slow_mock, Bulkhead(max_concurrent=1, max_waiters=0)),
            ("fast", fast_mock, Bulkhead(max_concurrent=1, max_waiters=0)),
        ])
        done = threading.Event()

        def slow_call():
            registry.get("slow").call("/pay", payload=V2)
            done.set()

        threading.Thread(target=slow_call).start()
        time.sleep(0.1)
        # slow 占满自己唯一名额并卡住
        assert registry.get("slow").bulkhead.active == 1
        with pytest.raises(CapacityFullError):
            registry.get("slow").call("/pay", payload=V2)
        # fast 的名额完全独立：slow 再卡也不影响 fast
        assert registry.get("fast").call("/pay", payload=V2)["status"] == "ok"
        assert registry.get("fast").bulkhead.active == 0
        gate.set()
        assert done.wait(1)
        assert registry.get("slow").bulkhead.active == 0  # 慢调用结束名额归还

    def test_slots_released_after_failure(self):
        mock = MockUpstream("server_error")
        registry = self._registry_with_bulkheads([
            ("x", mock, Bulkhead(max_concurrent=1, max_waiters=0)),
        ])
        client = registry.get("x")
        for _ in range(3):
            with pytest.raises(UpstreamServerError):
                client.call("/pay", payload=V2)
        assert client.bulkhead.active == 0  # 每次失败后名额都还回去了

    def test_concurrent_load_stays_within_per_upstream_capacity(self):
        gate = threading.Event()
        mock_a = MockUpstream("blocking", release_gate=gate)
        mock_b = MockUpstream("blocking", release_gate=gate)
        registry = self._registry_with_bulkheads([
            ("a", mock_a, Bulkhead(max_concurrent=2, max_waiters=0)),
            ("b", mock_b, Bulkhead(max_concurrent=3, max_waiters=0)),
        ])
        rejected = {"a": 0, "b": 0}
        lock = threading.Lock()

        def hit(name):
            try:
                registry.get(name).call("/pay", payload=V2)
            except CapacityFullError:
                with lock:
                    rejected[name] += 1

        threads = ([threading.Thread(target=hit, args=("a",)) for _ in range(8)]
                   + [threading.Thread(target=hit, args=("b",)) for _ in range(8)])
        for t in threads:
            t.start()
        time.sleep(0.2)
        assert registry.get("a").bulkhead.active == 2
        assert registry.get("b").bulkhead.active == 3
        gate.set()
        for t in threads:
            t.join(2)
        assert rejected["a"] == 6
        assert rejected["b"] == 5
        assert registry.get("a").bulkhead.active == 0
        assert registry.get("b").bulkhead.active == 0
        assert mock_a.calls == 2 and mock_b.calls == 3


class TestHealth:
    def test_health_reports_each_upstream_separately(self):
        registry, _ = build_registry({"alpha": "ok", "beta": "ok"},
                                     max_attempts=1, max_concurrent=4)
        health = registry.health()
        assert set(health) == {"alpha", "beta"}
        for name in ("alpha", "beta"):
            assert health[name]["circuit"] == "closed"
            assert health[name]["in_flight"] == 0
            assert health[name]["waiting"] == 0
            assert "available_tokens" in health[name]

    def test_health_diverges_when_one_upstream_fails(self):
        registry, _ = build_registry({"bad": "server_error", "good": "ok"},
                                     failure_threshold=1, max_attempts=1)
        with pytest.raises(UpstreamServerError):
            registry.get("bad").call("/pay", payload=V2)
        health = registry.health()
        assert health["bad"]["circuit"] == "open"
        assert health["good"]["circuit"] == "closed"  # 不是笼统的整体状态

    def test_in_flight_visible_per_upstream(self):
        gate = threading.Event()
        mock = MockUpstream("blocking", release_gate=gate)
        registry = UpstreamRegistry(transport_factory=lambda n: mock.transport)
        client = registry.register(UpstreamConfig(
            "busy", "http://busy", max_attempts=1, max_concurrent=3))
        done = threading.Event()

        def call():
            client.call("/pay", payload=V2)
            done.set()

        threads = [threading.Thread(target=call) for _ in range(3)]
        for t in threads:
            t.start()
        time.sleep(0.15)
        assert registry.health()["busy"]["in_flight"] == 3
        gate.set()
        for t in threads:
            t.join(2)
        assert registry.health()["busy"]["in_flight"] == 0
