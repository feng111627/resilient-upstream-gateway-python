"""HTTP surface: callers get normalized error kinds and status codes."""

from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

from app.circuit_breaker import CircuitBreaker
from app.main import create_app
from app.retry import RetryPolicy
from app.service import GatewayService
from app.upstream import UpstreamClient
from tests.conftest import FakeClock, FakeSleep, MockUpstream


@pytest.fixture
def client_and_mock(mock_upstream: MockUpstream, fake_clock: FakeClock, fake_sleep: FakeSleep):
    service = GatewayService(
        upstream=UpstreamClient(
            base_url="http://mock-upstream",
            transport=httpx.MockTransport(mock_upstream.handler),
        ),
        breaker=CircuitBreaker(failure_threshold=2, recovery_timeout=10.0, clock=fake_clock),
        retry=RetryPolicy(max_attempts=2, base_delay=0.01, sleep=fake_sleep),
    )
    app = create_app(service)
    with TestClient(app) as client:
        yield client, mock_upstream


def post_order(client, key="key-1", **body_overrides):
    body = {"order_id": "o-1", "amount": 9.5}
    body.update(body_overrides)
    return client.post("/orders", json=body, headers={"Idempotency-Key": key})


def test_success_returns_upstream_result(client_and_mock):
    client, _ = client_and_mock
    resp = post_order(client)
    assert resp.status_code == 201
    assert resp.json() == {"status": "ok", "transaction_id": "tx-v1"}


def test_timeout_surfaces_as_504_with_kind(client_and_mock):
    client, mock = client_and_mock
    mock.default = "timeout"
    resp = post_order(client)
    assert resp.status_code == 502  # retries exhausted wraps the timeout
    assert resp.json()["error"]["kind"] == "retries_exhausted"
    assert resp.json()["error"]["detail"]["last_error"] == "upstream_timeout"


def test_4xx_surfaces_as_upstream_client_error(client_and_mock):
    client, mock = client_and_mock
    mock.default = "status_422"
    resp = post_order(client)
    assert resp.status_code == 502
    body = resp.json()["error"]
    assert body["kind"] == "upstream_client_error"
    assert body["detail"]["upstream_status"] == 422


def test_circuit_open_surfaces_as_503(client_and_mock):
    client, mock = client_and_mock
    mock.default = "status_500"
    post_order(client, key="k1")
    post_order(client, key="k2")  # breaker threshold = 2 -> now open
    resp = post_order(client, key="k3")
    assert resp.status_code == 503
    assert resp.json()["error"]["kind"] == "circuit_open"


def test_idempotent_replay_returns_same_response(client_and_mock):
    client, mock = client_and_mock
    first = post_order(client)
    second = post_order(client)
    assert first.json() == second.json()
    assert mock.call_count == 1


def test_idempotency_conflict_is_409(client_and_mock):
    client, _ = client_and_mock
    post_order(client)
    resp = post_order(client, amount=100.0)
    assert resp.status_code == 409
    assert resp.json()["error"]["kind"] == "idempotency_conflict"


def test_unsupported_version_is_400(client_and_mock):
    client, _ = client_and_mock
    resp = post_order(client, version="v9")
    assert resp.status_code == 400
    assert resp.json()["error"]["kind"] == "unsupported_version"


def test_missing_idempotency_key_is_rejected(client_and_mock):
    client, _ = client_and_mock
    resp = client.post("/orders", json={"order_id": "o-1", "amount": 9.5})
    assert resp.status_code == 422  # FastAPI validation: header required


def test_health_endpoint(client_and_mock):
    client, _ = client_and_mock
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "circuit": "closed"}
