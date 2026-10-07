"""HTTP 层验证：多上游路由、统一错误响应、幂等头、版本头、分上游健康状态。

上游全部用本地 Mock，不依赖外部服务。
"""

from fastapi.testclient import TestClient

from gateway.client import ResilientUpstreamClient
from gateway.registry import UpstreamConfig, UpstreamRegistry
from gateway.retry import RetryPolicy
from main import create_app
from tests.mock_upstream import MockUpstream

PAYLOAD_V2 = {"amount": 100, "currency": "CNY"}
UP = "pay"  # 测试用上游名


def make_test_client(scenario: str = "ok", **config_overrides) -> tuple[TestClient, MockUpstream]:
    upstream = MockUpstream(scenario)
    registry = UpstreamRegistry()
    registry.register(
        UpstreamConfig(
            name=UP,
            base_url="http://mock",
            transport=upstream.transport,
            max_attempts=2,
            retry_base_delay=0.01,
            retry_jitter=0.0,
            sleep_func=lambda _: None,
            **config_overrides,
        )
    )
    return TestClient(create_app(registry), raise_server_exceptions=False), upstream


class TestHttpGateway:
    def test_normal_call(self):
        tc, _ = make_test_client()
        resp = tc.post(f"/gateway/{UP}/pay", json=PAYLOAD_V2)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_unknown_upstream_returns_404(self):
        tc, _ = make_test_client()
        resp = tc.post("/gateway/ghost/pay", json=PAYLOAD_V2)
        assert resp.status_code == 404
        assert resp.json()["error"] == "unknown_upstream"

    def test_timeout_returns_504_with_unified_body(self):
        tc, _ = make_test_client("timeout")
        resp = tc.post(f"/gateway/{UP}/pay", json=PAYLOAD_V2)
        assert resp.status_code == 504
        body = resp.json()
        assert body == {"error": "timeout", "message": "上游调用超时", "retryable": True}

    def test_connection_failure_returns_502(self):
        tc, _ = make_test_client("conn_error")
        resp = tc.post(f"/gateway/{UP}/pay", json=PAYLOAD_V2)
        assert resp.status_code == 502
        assert resp.json()["error"] == "connection"

    def test_upstream_4xx_propagates_status_with_unified_body(self):
        tc, _ = make_test_client("bad_request")
        resp = tc.post(f"/gateway/{UP}/pay", json=PAYLOAD_V2)
        assert resp.status_code == 400
        assert resp.json()["error"] == "upstream_4xx"

    def test_upstream_5xx_returns_502(self):
        tc, _ = make_test_client("server_error")
        resp = tc.post(f"/gateway/{UP}/pay", json=PAYLOAD_V2)
        assert resp.status_code == 502
        assert resp.json()["error"] == "upstream_5xx"

    def test_5xx_html_error_page_is_invalid_response_not_retried(self):
        tc, upstream = make_test_client("html_5xx")
        resp = tc.post(f"/gateway/{UP}/pay", json=PAYLOAD_V2)
        assert resp.status_code == 502
        body = resp.json()
        assert body["error"] == "invalid_response"
        assert body["retryable"] is False
        assert upstream.calls == 1  # 顶着 5xx 也不重试

    def test_invalid_json_200_returns_invalid_response(self):
        tc, _ = make_test_client("invalid_json")
        resp = tc.post(f"/gateway/{UP}/pay", json=PAYLOAD_V2)
        assert resp.status_code == 502
        assert resp.json()["error"] == "invalid_response"

    def test_idempotency_key_header_dedupes(self):
        tc, upstream = make_test_client()
        r1 = tc.post(f"/gateway/{UP}/pay", json=PAYLOAD_V2, headers={"Idempotency-Key": "k-1"})
        r2 = tc.post(f"/gateway/{UP}/pay", json=PAYLOAD_V2, headers={"Idempotency-Key": "k-1"})
        assert r1.json() == r2.json()
        assert upstream.calls == 1

    def test_idempotency_key_with_changed_payload_conflicts(self):
        tc, _ = make_test_client()
        r1 = tc.post(f"/gateway/{UP}/pay", json=PAYLOAD_V2, headers={"Idempotency-Key": "k-2"})
        assert r1.status_code == 200
        r2 = tc.post(
            f"/gateway/{UP}/pay",
            json={"amount": 999, "currency": "CNY"},
            headers={"Idempotency-Key": "k-2"},
        )
        assert r2.status_code == 409
        assert r2.json()["error"] == "idempotency_conflict"

    def test_invalid_request_body_rejected_before_upstream(self):
        tc, upstream = make_test_client()
        resp = tc.post(f"/gateway/{UP}/pay", json={"amount": 100})  # v2 缺 currency
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_request"
        assert upstream.calls == 0  # 没打到上游

    def test_version_header_selects_upstream_version(self):
        tc, _ = make_test_client()
        resp = tc.post(f"/gateway/{UP}/pay", json={"amount": 100}, headers={"X-Api-Version": "v1"})
        assert resp.json()["version"] == "v1"
        resp = tc.post(f"/gateway/{UP}/pay", json=PAYLOAD_V2, headers={"X-Api-Version": "v9"})
        assert resp.status_code == 400
        assert resp.json()["error"] == "version_unsupported"

    def test_health_endpoint_reports_each_upstream(self):
        tc, _ = make_test_client()
        resp = tc.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["upstreams"][UP]["circuit"] == "closed"
        assert body["upstreams"][UP]["in_flight"] == 0

    def test_health_single_upstream(self):
        tc, _ = make_test_client()
        resp = tc.get(f"/health/{UP}")
        assert resp.status_code == 200
        assert resp.json()["circuit"] == "closed"
        resp = tc.get("/health/ghost")
        assert resp.status_code == 404


def make_legacy_client(scenario: str = "ok") -> tuple[TestClient, MockUpstream]:
    """单上游时代的构造与注入方式：直接 new ResilientUpstreamClient 传给 create_app。"""
    upstream = MockUpstream(scenario)
    client = ResilientUpstreamClient(
        base_url="http://legacy",
        transport=upstream.transport,
        retry_policy=RetryPolicy(max_attempts=2, base_delay=0.01, jitter=0.0),
        sleep_func=lambda _: None,
    )
    return TestClient(create_app(client), raise_server_exceptions=False), upstream


class TestBackwardCompatibility:
    """升级多上游后，单上游时代的调用路径与注入方式必须照常可用。"""

    def test_legacy_client_injection_proxies(self):
        tc, upstream = make_legacy_client()
        resp = tc.post("/gateway/pay", json=PAYLOAD_V2)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        assert upstream.calls == 1

    def test_legacy_client_unified_error_body_unchanged(self):
        tc, _ = make_legacy_client("timeout")
        resp = tc.post("/gateway/pay", json=PAYLOAD_V2)
        assert resp.status_code == 504
        assert resp.json() == {
            "error": "timeout", "message": "上游调用超时", "retryable": True}

    def test_legacy_headers_still_work(self):
        tc, upstream = make_legacy_client()
        resp = tc.post(
            "/gateway/pay", json={"amount": 100},
            headers={"Idempotency-Key": "leg-1", "X-Api-Version": "v1"},
        )
        assert resp.status_code == 200
        assert resp.json()["version"] == "v1"
        resp = tc.post(
            "/gateway/pay", json={"amount": 100},
            headers={"Idempotency-Key": "leg-1", "X-Api-Version": "v1"},
        )
        assert resp.status_code == 200
        assert upstream.calls == 1  # 幂等头照旧生效

    def test_legacy_multisegment_path_not_mistaken_for_upstream(self):
        """老路径多段（/gateway/orders/create）不会被误当成 上游名/接口。"""
        tc, _ = make_legacy_client()
        resp = tc.post("/gateway/orders/create", json=PAYLOAD_V2)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_legacy_health_has_top_level_circuit_and_upstreams(self):
        tc, _ = make_legacy_client()
        resp = tc.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["circuit"] == "closed"  # 老字段保留
        assert body["upstreams"]["default"]["circuit"] == "closed"  # 新视图也在

    def test_named_routing_and_legacy_path_coexist(self):
        """多上游注册表里存在 default 时：新路由按名走，老路径走 default。"""
        default_up = MockUpstream()
        pay_up = MockUpstream()
        registry = UpstreamRegistry()
        registry.register(
            UpstreamConfig(name="default", base_url="http://default",
                           transport=default_up.transport, max_attempts=1,
                           sleep_func=lambda _: None))
        registry.register(
            UpstreamConfig(name="pay", base_url="http://pay",
                           transport=pay_up.transport, max_attempts=1,
                           sleep_func=lambda _: None))
        tc = TestClient(create_app(registry), raise_server_exceptions=False)

        assert tc.post("/gateway/pay/create", json=PAYLOAD_V2).status_code == 200
        assert pay_up.calls == 1 and default_up.calls == 0  # 第一段是已登记名 -> 新路由
        assert tc.post("/gateway/refund", json=PAYLOAD_V2).status_code == 200
        assert default_up.calls == 1  # 第一段不是已登记名 -> 老路径走 default
        assert pay_up.calls == 1
