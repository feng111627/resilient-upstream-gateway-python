"""Version negotiation: both wire shapes adapt to one canonical result."""

from __future__ import annotations

import pytest

from app.errors import MissingFieldError, UnsupportedVersionError
from app.versions import UpstreamAdapter


@pytest.fixture
def adapter() -> UpstreamAdapter:
    return UpstreamAdapter()


def test_v1_request_shape(adapter: UpstreamAdapter):
    assert adapter.endpoint("v1") == "/v1/orders"
    assert adapter.build_request("v1", "o-1", 9.5) == {"order_id": "o-1", "amount": 9.5}


def test_v2_request_shape(adapter: UpstreamAdapter):
    assert adapter.endpoint("v2") == "/v2/orders"
    assert adapter.build_request("v2", "o-1", 9.5) == {
        "order": {"id": "o-1", "amount": 9.5},
        "currency": "CNY",
    }


def test_v1_response_parsed(adapter: UpstreamAdapter):
    result = adapter.parse_response("v1", {"status": "ok", "transaction_id": "tx-1"})
    assert result == {"status": "ok", "transaction_id": "tx-1"}


def test_v2_response_parsed(adapter: UpstreamAdapter):
    result = adapter.parse_response("v2", {"result": {"status": "ok", "txId": "tx-2"}})
    assert result == {"status": "ok", "transaction_id": "tx-2"}


def test_unsupported_version_rejected_explicitly(adapter: UpstreamAdapter):
    with pytest.raises(UnsupportedVersionError) as exc_info:
        adapter.build_request("v3", "o-1", 1.0)
    assert "v3" in exc_info.value.detail["supported"] or "v3" in exc_info.value.message
    assert exc_info.value.detail["supported"] == ["v1", "v2"]


@pytest.mark.parametrize(
    "version,body,missing",
    [
        ("v1", {"status": "ok"}, "transaction_id"),
        ("v1", {"transaction_id": "tx"}, "status"),
        ("v2", {"result": {"status": "ok"}}, "result.txId"),
        ("v2", {"result": {"txId": "tx"}}, "result.status"),
        ("v2", {}, "result.status"),
    ],
)
def test_missing_required_field_raises_explicit_error(
    adapter: UpstreamAdapter, version: str, body: dict, missing: str
):
    with pytest.raises(MissingFieldError) as exc_info:
        adapter.parse_response(version, body)
    assert exc_info.value.detail["field"] == missing
    assert exc_info.value.detail["version"] == version
