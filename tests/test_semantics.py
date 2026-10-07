"""调用语义：同键换请求报冲突、请求按版本组装校验、非法响应有独立结论。"""

import threading

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

    def test_same_key_same_payload_different_path_conflicts(self):
        """同键同报文但换了目标接口：两次不同的调用，必须报冲突。"""
        up = MockUpstream()
        client = make_client(up)
        client.call("/pay", payload=PAYLOAD_V2, idempotency_key="k-path")
        with pytest.raises(IdempotencyConflictError) as exc_info:
            client.call("/refund", payload=PAYLOAD_V2, idempotency_key="k-path")
        assert exc_info.value.kind is ErrorKind.IDEMPOTENCY_CONFLICT
        assert up.calls == 1  # 冲突请求没有打到上游

    def test_same_key_same_payload_different_version_conflicts(self):
        """同键同报文但换了接口版本：同样明确报冲突。"""
        up = MockUpstream()
        client = make_client(up)
        client.call("/pay", version="v2", payload=PAYLOAD_V2, idempotency_key="k-ver")
        with pytest.raises(IdempotencyConflictError):
            client.call("/pay", version="v1", payload={"amount": 100},
                        idempotency_key="k-ver")
        assert up.calls == 1

    def test_same_key_identical_request_still_reuses(self):
        """同键、上游/接口/版本/报文完全一致：照常复用旧结论。"""
        up = MockUpstream()
        client = make_client(up)
        r1 = client.call("/pay", version="v2", payload=PAYLOAD_V2, idempotency_key="k-same")
        r2 = client.call("/pay", version="v2", payload=PAYLOAD_V2, idempotency_key="k-same")
        assert r1 == r2
        assert up.calls == 1

    def test_concurrent_mixed_requests_stay_consistent(self):
        """并发同时打进来：同请求的都拿到同一结论，换内容的全报冲突，不乱。"""
        up = MockUpstream()
        client = make_client(up)
        results, conflicts = [], []

        def worker(changed: bool):
            payload = {"amount": 999, "currency": "CNY"} if changed else PAYLOAD_V2
            try:
                results.append(
                    client.call("/pay", payload=payload, idempotency_key="k-conc")
                )
            except IdempotencyConflictError:
                conflicts.append(1)

        threads = [threading.Thread(target=worker, args=(i % 2 == 0,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(results) + len(conflicts) == 20
        assert conflicts  # 换了报文的那些明确报了冲突
        assert all(r == results[0] for r in results)  # 同请求的结论一致
        assert up.calls == 1  # 只有 owner 真正打了一次上游

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

    def test_5xx_with_non_json_body_is_invalid_response_not_retried(self):
        """5xx 状态码 + HTML 错误页：不再当上游 5xx 重试，给独立的解析层结论。"""
        up = MockUpstream("server_error_html")
        client = make_client(up, retry_policy=RetryPolicy(max_attempts=3))
        with pytest.raises(InvalidResponseError) as exc_info:
            client.call("/pay", payload=PAYLOAD_V2)
        assert exc_info.value.kind is ErrorKind.INVALID_RESPONSE
        assert exc_info.value.kind is not ErrorKind.UPSTREAM_SERVER_ERROR
        assert exc_info.value.retryable is False
        assert up.calls == 1  # 非法响应不重试

    def test_5xx_with_valid_json_body_still_retried_as_server_error(self):
        """5xx 且报文是合法 JSON：仍按上游 5xx 处理并重试，不受新规则影响。"""
        from gateway.errors import UpstreamServerError

        up = MockUpstream("server_error")
        client = make_client(up, retry_policy=RetryPolicy(max_attempts=3))
        with pytest.raises(UpstreamServerError):
            client.call("/pay", payload=PAYLOAD_V2)
        assert up.calls == 3

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
