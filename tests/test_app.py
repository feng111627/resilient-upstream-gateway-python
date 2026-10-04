"""HTTP 层验证：多上游路由、统一错误响应、幂等头、版本头、分上游健康状态。

上游全部用本地 Mock，不依赖外部服务。
"""

from fastapi.testclient import TestClient

from gateway.registry import UpstreamConfig, UpstreamRegistry
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
