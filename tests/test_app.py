"""HTTP 层验证：统一错误响应、幂等头、版本头。上游用本地 Mock，不依赖外部服务。"""

from fastapi.testclient import TestClient

from gateway.client import ResilientUpstreamClient
from gateway.retry import RetryPolicy
from main import create_app
from tests.mock_upstream import MockUpstream


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


class TestHttpGateway:
    def test_normal_call(self):
        tc, _ = make_test_client()
        resp = tc.post("/gateway/pay", json={"amount": 100})
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_timeout_returns_504_with_unified_body(self):
        tc, _ = make_test_client("timeout")
        resp = tc.post("/gateway/pay")
        assert resp.status_code == 504
        body = resp.json()
        assert body == {"error": "timeout", "message": "上游调用超时", "retryable": True}

    def test_connection_failure_returns_502(self):
        tc, _ = make_test_client("conn_error")
        resp = tc.post("/gateway/pay")
        assert resp.status_code == 502
        assert resp.json()["error"] == "connection"

    def test_upstream_4xx_propagates_status_with_unified_body(self):
        tc, _ = make_test_client("bad_request")
        resp = tc.post("/gateway/pay")
        assert resp.status_code == 400
        assert resp.json()["error"] == "upstream_4xx"

    def test_upstream_5xx_returns_502(self):
        tc, _ = make_test_client("server_error")
        resp = tc.post("/gateway/pay")
        assert resp.status_code == 502
        assert resp.json()["error"] == "upstream_5xx"

    def test_idempotency_key_header_dedupes(self):
        tc, upstream = make_test_client()
        r1 = tc.post("/gateway/pay", headers={"Idempotency-Key": "k-1"})
        r2 = tc.post("/gateway/pay", headers={"Idempotency-Key": "k-1"})
        assert r1.json() == r2.json()
        assert upstream.calls == 1

    def test_version_header_selects_upstream_version(self):
        tc, _ = make_test_client()
        resp = tc.post("/gateway/pay", headers={"X-Api-Version": "v1"})
        assert resp.json()["version"] == "v1"
        resp = tc.post("/gateway/pay", headers={"X-Api-Version": "v9"})
        assert resp.status_code == 400
        assert resp.json()["error"] == "version_unsupported"

    def test_health_endpoint(self):
        tc, _ = make_test_client()
        resp = tc.get("/health")
        assert resp.status_code == 200
        assert resp.json()["circuit"] == "closed"
