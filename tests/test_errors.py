"""错误归一化：底层库异常必须映射为可区分的自有错误，绝不原样透出。"""

import httpx
import pytest

from gateway.errors import (
    ErrorKind,
    GatewayError,
    UpstreamConnectionError,
    UpstreamTimeoutError,
    map_transport_error,
)


class TestTransportErrorMapping:
    def test_timeout_maps_to_timeout_error(self):
        err = map_transport_error(httpx.ReadTimeout("slow"))
        assert isinstance(err, UpstreamTimeoutError)
        assert err.kind is ErrorKind.TIMEOUT
        assert err.retryable is True

    def test_connect_timeout_is_timeout(self):
        err = map_transport_error(httpx.ConnectTimeout("slow"))
        assert err.kind is ErrorKind.TIMEOUT

    def test_connect_error_maps_to_connection_error(self):
        err = map_transport_error(httpx.ConnectError("refused"))
        assert isinstance(err, UpstreamConnectionError)
        assert err.kind is ErrorKind.CONNECTION
        assert err.retryable is True

    def test_network_error_maps_to_connection_error(self):
        err = map_transport_error(httpx.NetworkError("unreachable"))
        assert err.kind is ErrorKind.CONNECTION

    def test_unknown_exception_falls_back_to_unknown(self):
        err = map_transport_error(ValueError("weird"))
        assert type(err) is GatewayError
        assert err.kind is ErrorKind.UNKNOWN
        assert err.retryable is False

    def test_gateway_error_passes_through_mapping(self):
        original = UpstreamTimeoutError("already mapped")
        assert map_transport_error(original) is original

    def test_error_serializes_for_caller(self):
        err = map_transport_error(httpx.ConnectError("refused"))
        body = err.to_dict()
        assert body["error"] == "connection"
        assert body["retryable"] is True
        assert "ConnectError" not in body["error"]  # 不暴露底层异常类型
