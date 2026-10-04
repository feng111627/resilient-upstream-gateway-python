"""HTTP 层验证：统一错误响应、幂等头、版本头、多上游路由与逐上游健康检查。

上游全部用本地 Mock，不依赖任何外部服务。
"""

import threading

from fastapi.testclient import TestClient

from gateway.client import ResilientUpstreamClient
from gateway.registry import UpstreamConfig, UpstreamRegistry
from gateway.retry import RetryPolicy
from main import create_app
from tests.mock_upstream import MockUpstream

V2 = {"id": "o1", "amount": 100, "currency": "CNY"}
V1 = {"id": "o1", "amount": 100, "callback_url": "http://cb"}


def make_test_client(scenario: str = "ok", **client_overrides) -> tuple[TestClient, MockUpstream]:
    upstream = MockUpstream(scenario)
    client = ResilientUpstreamClient(
        base_url="http://mock",
        transport=upstream.transport,
        retry_policy=RetryPolicy(max_attempts=2, base_delay=0.01, jitter=0.0),
        sleep_func=lambda _: None,
        **client_overrides,
    )
    return TestClient(create_app(client), raise_server_exceptions=False), upstream


def make_multi_client(scenarios: dict[str, str]) -> tuple[TestClient, dict[str, MockUpstream]]:
    """构造挂了多个命名上游的应用，每个上游独立 MockTransport。"""
    mocks = {n: MockUpstream(s) for n, s in scenarios.items()}
    registry = UpstreamRegistry(transport_factory=lambda n: mocks[n].transport)
    for n in scenarios:
        registry.register(UpstreamConfig(
            name=n, base_url=f"http://{n}",
            max_attempts=2, base_delay=0.01, failure_threshold=2,
        ))
    app = create_app(registry=registry)
    return TestClient(app, raise_server_exceptions=False), mocks


class TestHttpGateway:
    def test_normal_call(self):
        tc, _ = make_test_client()
        resp = tc.post("/gateway/pay", json=V2)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_timeout_returns_504_with_unified_body(self):
        tc, _ = make_test_client("timeout")
        resp = tc.post("/gateway/pay", json=V2)
        assert resp.status_code == 504
        body = resp.json()
        assert body == {"error": "timeout", "message": "上游调用超时", "retryable": True}

    def test_connection_failure_returns_502(self):
        tc, _ = make_test_client("conn_error")
        resp = tc.post("/gateway/pay", json=V2)
        assert resp.status_code == 502
        assert resp.json()["error"] == "connection"

    def test_upstream_4xx_propagates_status_with_unified_body(self):
        tc, _ = make_test_client("bad_request")
        resp = tc.post("/gateway/pay", json=V2)
        assert resp.status_code == 400
        assert resp.json()["error"] == "upstream_4xx"

    def test_upstream_5xx_returns_502(self):
        tc, _ = make_test_client("server_error")
        resp = tc.post("/gateway/pay", json=V2)
        assert resp.status_code == 502
        assert resp.json()["error"] == "upstream_5xx"

    def test_idempotency_key_header_dedupes(self):
        tc, upstream = make_test_client()
        r1 = tc.post("/gateway/pay", headers={"Idempotency-Key": "k-1"}, json=V2)
        r2 = tc.post("/gateway/pay", headers={"Idempotency-Key": "k-1"}, json=V2)
        assert r1.json() == r2.json()
        assert upstream.calls == 1

    def test_version_header_selects_upstream_version(self):
        tc, _ = make_test_client()
        resp = tc.post("/gateway/pay", headers={"X-Api-Version": "v1"}, json=V1)
        assert resp.json()["version"] == "v1"
        resp = tc.post("/gateway/pay", headers={"X-Api-Version": "v9"}, json={})
        assert resp.status_code == 400
        assert resp.json()["error"] == "version_unsupported"

    def test_health_endpoint(self):
        tc, _ = make_test_client()
        resp = tc.get("/health")
        assert resp.status_code == 200
        assert resp.json()["circuit"] == "closed"


class TestNamedUpstreams:
    def test_route_to_named_upstream(self):
        tc, mocks = make_multi_client({"payment": "ok", "risk": "ok"})
        r1 = tc.post("/upstreams/payment/call/pay", json=V2)
        r2 = tc.post("/upstreams/risk/call/evaluate", json=V2)
        assert r1.status_code == 200 and r2.status_code == 200
        assert mocks["payment"].calls == 1 and mocks["risk"].calls == 1

    def test_unknown_upstream_returns_404(self):
        tc, _ = make_multi_client({"payment": "ok"})
        resp = tc.post("/upstreams/ghost/call/pay", json=V2)
        assert resp.status_code == 404
        body = resp.json()
        assert body["error"] == "upstream_not_found"
        assert "ghost" in body["message"]

    def test_one_upstream_failure_isolated_over_http(self):
        # payment 一直 5xx（每次调用含重试，累计失败很快触发熔断）；risk 全程正常
        tc, mocks = make_multi_client({"payment": "server_error", "risk": "ok"})
        seen = set()
        for _ in range(4):
            r = tc.post("/upstreams/payment/call/pay", json=V2)
            seen.add(r.json()["error"])
        assert seen <= {"upstream_5xx", "circuit_open"}
        assert "upstream_5xx" in seen
        # risk 完全不受 payment 故障/熔断影响
        assert tc.post("/upstreams/risk/call/pay", json=V2).status_code == 200
        assert tc.post("/upstreams/risk/call/pay", json=V2).status_code == 200
        assert mocks["risk"].calls == 2
        health = tc.get("/health").json()["upstreams"]
        assert health["payment"]["circuit"] == "open"
        assert health["risk"]["circuit"] == "closed"

    def test_health_lists_every_upstream_separately(self):
        tc, _ = make_multi_client({"payment": "ok", "risk": "ok"})
        resp = tc.get("/health")
        data = resp.json()
        assert set(data["upstreams"]) == {"payment", "risk"}
        assert data["upstreams"]["payment"]["circuit"] == "closed"
        assert data["upstreams"]["risk"]["circuit"] == "closed"
        assert "in_flight" in data["upstreams"]["payment"]

    def test_idempotency_conflict_returns_409(self):
        tc, upstream = make_test_client()
        tc.post("/gateway/pay", headers={"Idempotency-Key": "k-9"}, json=V2)
        changed = {**V2, "amount": 999}
        resp = tc.post("/gateway/pay", headers={"Idempotency-Key": "k-9"}, json=changed)
        assert resp.status_code == 409
        assert resp.json()["error"] == "idempotency_conflict"
        assert upstream.calls == 1

    def test_invalid_request_returns_422(self):
        tc, upstream = make_test_client()
        resp = tc.post("/gateway/pay", json={"id": "o1", "amount": 10})  # 缺 currency
        assert resp.status_code == 422
        assert resp.json()["error"] == "request_invalid"
        assert upstream.calls == 0

    def test_invalid_json_returns_502_distinct_error(self):
        tc, _ = make_test_client("invalid_json")
        resp = tc.post("/gateway/pay", json=V2)
        assert resp.status_code == 502
        assert resp.json()["error"] == "invalid_json"

    def test_version_specific_wire_is_sent(self):
        tc, upstream = make_test_client()
        tc.post("/gateway/pay", headers={"X-Api-Version": "v1"}, json=V1)
        assert upstream.last_request == {
            "order_id": "o1", "amount": 100, "callback_url": "http://cb",
        }
