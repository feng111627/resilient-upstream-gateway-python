"""Upstream failure modes are normalized into distinguishable GatewayErrors."""

from __future__ import annotations

import httpx
import pytest

from app.errors import (
    ErrorKind,
    UpstreamBadResponseError,
    UpstreamClientError,
    UpstreamConnectionError,
    UpstreamServerError,
    UpstreamTimeoutError,
)
from app.upstream import UpstreamClient
from tests.conftest import MockUpstream


def make_client(mock: MockUpstream) -> UpstreamClient:
    return UpstreamClient(
        base_url="http://mock-upstream",
        transport=httpx.MockTransport(mock.handler),
    )


async def test_timeout_maps_to_timeout_error(mock_upstream: MockUpstream):
    mock_upstream.default = "timeout"
    with pytest.raises(UpstreamTimeoutError) as exc_info:
        await make_client(mock_upstream).post_json("/v1/orders", {})
    assert exc_info.value.kind is ErrorKind.TIMEOUT
    assert exc_info.value.http_status == 504


async def test_connect_error_maps_to_connection_error(mock_upstream: MockUpstream):
    mock_upstream.default = "conn_error"
    with pytest.raises(UpstreamConnectionError) as exc_info:
        await make_client(mock_upstream).post_json("/v1/orders", {})
    assert exc_info.value.kind is ErrorKind.CONNECTION


@pytest.mark.parametrize("status", [400, 404, 422])
async def test_4xx_maps_to_client_error(mock_upstream: MockUpstream, status: int):
    mock_upstream.default = f"status_{status}"
    with pytest.raises(UpstreamClientError) as exc_info:
        await make_client(mock_upstream).post_json("/v1/orders", {})
    assert exc_info.value.kind is ErrorKind.UPSTREAM_CLIENT_ERROR
    assert exc_info.value.upstream_status == status


@pytest.mark.parametrize("status", [500, 502, 503])
async def test_5xx_maps_to_server_error(mock_upstream: MockUpstream, status: int):
    mock_upstream.default = f"status_{status}"
    with pytest.raises(UpstreamServerError) as exc_info:
        await make_client(mock_upstream).post_json("/v1/orders", {})
    assert exc_info.value.kind is ErrorKind.UPSTREAM_SERVER_ERROR
    assert exc_info.value.upstream_status == status


async def test_non_json_body_maps_to_bad_response(mock_upstream: MockUpstream):
    mock_upstream.default = "bad_json"
    with pytest.raises(UpstreamBadResponseError) as exc_info:
        await make_client(mock_upstream).post_json("/v1/orders", {})
    assert exc_info.value.kind is ErrorKind.UPSTREAM_BAD_RESPONSE


async def test_raw_httpx_exception_never_escapes(mock_upstream: MockUpstream):
    """The public surface must not leak httpx exception types."""
    mock_upstream.default = "timeout"
    try:
        await make_client(mock_upstream).post_json("/v1/orders", {})
    except httpx.HTTPError:
        pytest.fail("raw httpx exception leaked across the boundary")
    except Exception:
        pass  # normalized gateway error, expected
