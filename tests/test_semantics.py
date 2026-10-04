"""调用语义：同键换内容报冲突、请求按版本组装校验、非法响应有独立结论。"""

import pytest

from gateway.client import ResilientUpstreamClient
from gateway.errors import (
    ErrorKind,
    IdempotencyConflictError,
    InvalidRequestError,
    InvalidResponseError,
    SchemaMismatchError,
)
from gateway.retry import RetryPolicy
from gateway.versioning import build_request
from tests.mock_upstream import MockUpstream

PAYLOAD_V1 = {"amount": 100}
PAYLOAD_V2 = {"amount": 100, "currency": "CNY"}


def make_client(upstream: MockUpstream, **overrides) -> ResilientUpstreamClient:
    params = {
        "base_url": "http://mock",
        "transport": upstream.transport,
        "retry_policy": RetryPolicy(max_attempts=1),
        "sleep_func": lambda _: None,
    }
    params.update(overrides)
    return ResilientUpstreamClient(**params)


class TestIdempotencyConflict:
    def test_same_key_same_payload_reuses_conclusion(self):
        up = MockUpstream()
        client = make_client(up)
        r1 = client.call("/pay", payload=PAYLOAD_V2, idempotency_key="k-1")
        r2 = client.call("/pay", payload=PAYLOAD_V2, idempotency_key="k-1")
        assert r1 == r2
        assert up.calls == 1

    def test_same_key_different_payload_conflicts(self):
        up = MockUpstream()
        client = make_client(up)
        client.call("/pay", payload=PAYLOAD_V2, idempotency_key="k-2")
        with pytest.raises(IdempotencyConflictError) as exc_info:
            client.call("/pay", payload={"amount": 999, "currency": "CNY"},
                        idempotency_key="k-2")
        assert exc_info.value.kind is ErrorKind.IDEMPOTENCY_CONFLICT
        assert exc_info.value.retryable is False
        assert up.calls == 1  # 冲突的请求没有打到上游

    def test_same_key_different_path_or_version_also_checked_by_content(self):
        """指纹覆盖请求内容：内容不同即冲突，不默默复用。"""
        up = MockUpstream()
        client = make_client(up)
        client.call("/pay", payload=PAYLOAD_V2, idempotency_key="k-3")
        with pytest.raises(IdempotencyConflictError):
            client.call("/pay", payload={"amount": 100, "currency": "USD"},
                        idempotency_key="k-3")

    def test_conflict_does_not_poison_original_entry(self):
        """冲突失败后，原键的正确内容重复提交仍能复用原结论。"""
        up = MockUpstream()
        client = make_client(up)
        r1 = client.call("/pay", payload=PAYLOAD_V2, idempotency_key="k-4")
        with pytest.raises(IdempotencyConflictError):
            client.call("/pay", payload={"amount": 1, "currency": "CNY"},
                        idempotency_key="k-4")
        r2 = client.call("/pay", payload=PAYLOAD_V2, idempotency_key="k-4")
        assert r2 == r1
        assert up.calls == 1


class TestRequestVersionAdaptation:
    def test_build_request_v1_shape(self):
        assert build_request("v1", {"amount": 5, "memo": "hi"}) == {"amount": 5, "memo": "hi"}

    def test_build_request_v2_requires_currency(self):
        with pytest.raises(InvalidRequestError) as exc_info:
            build_request("v2", {"amount": 5})
        assert "currency" in exc_info.value.message
        assert exc_info.value.kind is ErrorKind.INVALID_REQUEST

    def test_build_request_rejects_unsupported_fields(self):
        with pytest.raises(InvalidRequestError) as exc_info:
            build_request("v1", {"amount": 5, "currency": "CNY"})  # v1 不支持 currency
        assert "currency" in exc_info.value.message

    def test_build_request_strips_nothing_allowed_and_keeps_optional(self):
        body = build_request("v2", {"amount": 5, "currency": "CNY", "memo": "m"})
        assert body == {"amount": 5, "currency": "CNY", "memo": "m"}

    def test_client_rejects_invalid_request_before_hitting_upstream(self):
        up = MockUpstream()
        client = make_client(up)
        with pytest.raises(InvalidRequestError):
            client.call("/pay", payload={"amount": 100})  # 默认 v2 缺 currency
        assert up.calls == 0
        with pytest.raises(InvalidRequestError):
            client.call("/pay", version="v1", payload=PAYLOAD_V2)  # v1 不认 currency
        assert up.calls == 0

    def test_client_sends_version_shaped_body(self):
        """v1 请求按 v1 组装；v2 请求按 v2 组装，各自通过各自校验。"""
        up = MockUpstream()
        client = make_client(up)
        assert client.call("/pay", version="v1", payload=PAYLOAD_V1)["version"] == "v1"
        assert client.call("/pay", version="v2", payload=PAYLOAD_V2)["version"] == "v2"


class TestInvalidResponse:
    def test_non_json_body_is_invalid_response_not_timeout_or_5xx(self):
        up = MockUpstream("invalid_json")
        client = make_client(up)
        with pytest.raises(InvalidResponseError) as exc_info:
            client.call("/pay", payload=PAYLOAD_V2)
        assert exc_info.value.kind is ErrorKind.INVALID_RESPONSE
        # 与超时/4xx/5xx/结构缺失都是不同类别
        assert exc_info.value.kind not in (
            ErrorKind.TIMEOUT,
            ErrorKind.UPSTREAM_CLIENT_ERROR,
            ErrorKind.UPSTREAM_SERVER_ERROR,
            ErrorKind.SCHEMA_MISMATCH,
        )
        assert up.calls == 1  # 解析问题不重试

    def test_json_but_not_object_is_invalid_response(self):
        up = MockUpstream("not_object")
        client = make_client(up)
        with pytest.raises(InvalidResponseError):
            client.call("/pay", payload=PAYLOAD_V2)

    def test_missing_field_stays_schema_mismatch(self):
        """结构对得上但缺字段：仍是 schema_mismatch，与非法 JSON 区分开。"""
        up = MockUpstream("missing_field")
        client = make_client(up)
        with pytest.raises(SchemaMismatchError) as exc_info:
            client.call("/pay", payload=PAYLOAD_V2)
        assert exc_info.value.kind is ErrorKind.SCHEMA_MISMATCH

    def test_invalid_response_does_not_leak_parser_exception(self):
        up = MockUpstream("invalid_json")
        client = make_client(up)
        with pytest.raises(InvalidResponseError) as exc_info:
            client.call("/pay", payload=PAYLOAD_V2)
        # 不把 JSONDecodeError 等解析层异常名甩给调用方
        assert "JSONDecodeError" not in str(exc_info.value)
        assert exc_info.value.__cause__ is None
