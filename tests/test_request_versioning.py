"""版本相关：请求按版本组装并校验（缺字段/不支持字段明确拒绝），响应非法 JSON 单独分类。"""

import pytest

from gateway.client import ResilientUpstreamClient
from gateway.errors import (
    ErrorKind,
    InvalidJsonError,
    RequestInvalidError,
    VersionUnsupportedError,
)
from gateway.retry import RetryPolicy
from gateway.versioning import prepare_request
from tests.mock_upstream import MockUpstream

V2_PAYLOAD = {"id": "order-1", "amount": 100, "currency": "CNY", "note": "hi"}
V1_PAYLOAD = {"id": "order-1", "amount": 100, "callback_url": "http://cb"}


def make_client(upstream: MockUpstream, **kw) -> ResilientUpstreamClient:
    params = {
        "base_url": "http://mock",
        "transport": upstream.transport,
        "retry_policy": RetryPolicy(max_attempts=1),
        "sleep_func": lambda _: None,
    }
    params.update(kw)
    return ResilientUpstreamClient(**params)


class TestRequestAssembly:
    def test_v1_wire_is_flat_with_legacy_field_names(self):
        wire = prepare_request("v1", V1_PAYLOAD)
        assert wire == {
            "order_id": "order-1",
            "amount": 100,
            "callback_url": "http://cb",
        }

    def test_v2_wire_is_nested_payment_envelope(self):
        wire = prepare_request("v2", V2_PAYLOAD)
        assert wire == {
            "payment": {"id": "order-1", "amount": 100, "currency": "CNY"},
            "note": "hi",
        }

    def test_client_sends_assembled_wire_to_upstream(self):
        up = MockUpstream()
        client = make_client(up)
        client.call("/pay", version="v2", payload=V2_PAYLOAD)
        assert up.last_request == {
            "payment": {"id": "order-1", "amount": 100, "currency": "CNY"},
            "note": "hi",
        }
        client.call("/pay", version="v1", payload=V1_PAYLOAD)
        assert up.last_request == {
            "order_id": "order-1", "amount": 100, "callback_url": "http://cb",
        }


class TestRequestValidation:
    def test_missing_required_field_rejected_before_request(self):
        up = MockUpstream()
        client = make_client(up)
        with pytest.raises(RequestInvalidError) as exc:
            # v2 必填 currency
            client.call("/pay", payload={"id": "o1", "amount": 10})
        assert exc.value.kind is ErrorKind.REQUEST_INVALID
        assert "currency" in exc.value.message
        assert up.calls == 0  # 不合规请求绝不发出

    def test_null_required_field_rejected(self):
        up = MockUpstream()
        client = make_client(up)
        with pytest.raises(RequestInvalidError):
            client.call("/pay", payload={"id": None, "amount": 10, "currency": "CNY"})
        assert up.calls == 0

    def test_field_unsupported_by_version_rejected(self):
        up = MockUpstream()
        client = make_client(up)
        with pytest.raises(RequestInvalidError) as exc:
            # v1 没有 currency 概念；带上不支持字段必须明确拒绝而非静默丢弃
            client.call(
                "/pay", version="v1",
                payload={"id": "o1", "amount": 10,
                         "callback_url": "http://cb", "currency": "USD"},
            )
        assert "currency" in exc.value.message
        assert "不支持" in exc.value.message
        assert up.calls == 0

    def test_v2_rejects_v1_only_field(self):
        up = MockUpstream()
        client = make_client(up)
        with pytest.raises(RequestInvalidError) as exc:
            client.call(
                "/pay",
                payload={"id": "o1", "amount": 10, "currency": "CNY",
                         "callback_url": "http://cb"},
            )
        assert "callback_url" in exc.value.message

    def test_v1_requires_callback_url(self):
        up = MockUpstream()
        client = make_client(up)
        with pytest.raises(RequestInvalidError) as exc:
            client.call("/pay", version="v1",
                        payload={"id": "o1", "amount": 10})
        assert "callback_url" in exc.value.message
        assert up.calls == 0

    def test_non_object_body_rejected(self):
        with pytest.raises(RequestInvalidError):
            prepare_request("v2", ["not", "an", "object"])  # type: ignore[arg-type]

    def test_unknown_version_cannot_prepare(self):
        with pytest.raises(VersionUnsupportedError):
            prepare_request("v9", {})


class TestInvalidJsonResponse:
    def test_200_with_html_body_is_invalid_json_not_5xx(self):
        up = MockUpstream("invalid_json")
        client = make_client(up)
        with pytest.raises(InvalidJsonError) as exc:
            client.call("/pay", payload=V2_PAYLOAD)
        assert exc.value.kind is ErrorKind.INVALID_JSON
        assert exc.value.retryable is False
        assert "JSONDecodeError" not in exc.value.message  # 不甩解析层原始异常名
        assert up.calls == 1  # 解析问题不重试

    def test_malformed_json_distinct_from_timeout_and_http_errors(self):
        up = MockUpstream("malformed_json")
        client = make_client(
            up,
            retry_policy=RetryPolicy(max_attempts=3, base_delay=0.01, jitter=0.0),
        )
        with pytest.raises(InvalidJsonError):
            client.call("/pay", payload=V2_PAYLOAD)
        # 502 状态码但响应体不是 JSON：优先给"非法响应"的独立结论，
        # 而不是笼统按 5xx 重试
        assert up.calls == 1

    def test_invalid_json_kind_distinct_values(self):
        kinds = {
            ErrorKind.TIMEOUT.value, ErrorKind.INVALID_JSON.value,
            ErrorKind.SCHEMA_MISMATCH.value, ErrorKind.UPSTREAM_SERVER_ERROR.value,
            ErrorKind.CAPACITY_FULL.value,
        }
        assert len(kinds) == 5  # 五类结论互不相同，调用方可分别处理
