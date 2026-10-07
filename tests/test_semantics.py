"""调用语义：同键换内容报冲突、请求按版本组装校验、非法响应有独立结论。"""

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

    def test_same_key_different_path_conflicts(self):
        """同一幂等键换了目标接口（path）：明确冲突，不复用旧结论、不打上游。"""
        up = MockUpstream()
        client = make_client(up)
        client.call("/pay", payload=PAYLOAD_V2, idempotency_key="k-path")
        with pytest.raises(IdempotencyConflictError) as exc_info:
            client.call("/refund", payload=PAYLOAD_V2, idempotency_key="k-path")
        assert exc_info.value.kind is ErrorKind.IDEMPOTENCY_CONFLICT
        assert up.calls == 1  # 换接口的重复键没有打到上游

    def test_same_key_different_version_conflicts(self):
        """同一幂等键换了接口版本：明确冲突，哪怕两份报文各自都合法。"""
        up = MockUpstream()
        client = make_client(up)
        client.call("/pay", version="v1", payload=PAYLOAD_V1, idempotency_key="k-ver")
        with pytest.raises(IdempotencyConflictError) as exc_info:
            client.call("/pay", version="v2", payload=PAYLOAD_V2, idempotency_key="k-ver")
        assert exc_info.value.kind is ErrorKind.IDEMPOTENCY_CONFLICT
        assert up.calls == 1

    def test_same_key_default_version_equals_explicit_same_request(self):
        """未指定版本与显式指定默认版本是同一份请求，正常复用，不误报冲突。"""
        up = MockUpstream()
        client = make_client(up)
        r1 = client.call("/pay", payload=PAYLOAD_V2, idempotency_key="k-def")
        r2 = client.call("/pay", version="v2", payload=PAYLOAD_V2, idempotency_key="k-def")
        assert r1 == r2
        assert up.calls == 1

    def test_concurrent_same_key_with_different_request_conflicts_and_original_survives(
        self,
    ):
        """owner 执行期间，不同请求并发撞同一键：等 owner 结论后报冲突，
        原键的正确结论不受损坏，之后照样复用。"""
        up = MockUpstream("slow", slow_delay=0.3)
        client = make_client(up)
        outcomes: list[tuple[str, object]] = []
        owner_result: dict = {}

        def owner():
            try:
                owner_result["value"] = client.call(
                    "/pay", payload=PAYLOAD_V2, idempotency_key="k-conc")
                outcomes.append(("owner-ok", None))
            except Exception as exc:  # noqa: BLE001
                outcomes.append(("owner-err", exc))

        def interloper():
            try:
                client.call("/refund", payload=PAYLOAD_V2, idempotency_key="k-conc")
                outcomes.append(("intruder-ok", None))
            except IdempotencyConflictError as exc:
                outcomes.append(("intruder-conflict", exc))
            except Exception as exc:  # noqa: BLE001
                outcomes.append(("intruder-other", exc))

        t_owner = threading.Thread(target=owner)
        t_intruder = threading.Thread(target=interloper)
        t_owner.start()
        threading.Event().wait(0.05)  # 确保 owner 先成为 owner
        t_intruder.start()
        t_owner.join()
        t_intruder.join()

        tags = [tag for tag, _ in outcomes]
        assert "owner-ok" in tags
        assert "intruder-conflict" in tags
        assert up.calls == 1  # 冲突请求没有产生第二次上游调用
        # 报过冲突之后，原键的正确结论仍可正常复用
        again = client.call("/pay", payload=PAYLOAD_V2, idempotency_key="k-conc")
        assert again == owner_result["value"]
        assert up.calls == 1

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

    def test_5xx_with_html_body_is_invalid_response_and_not_retried(self):
        """顶着 5xx 状态码的 HTML 错误页：独立结论 invalid_response，绝不按 5xx 重试。"""
        up = MockUpstream("html_5xx")
        client = ResilientUpstreamClient(
            base_url="http://mock",
            transport=up.transport,
            retry_policy=RetryPolicy(max_attempts=3, base_delay=0.01, jitter=0.0),
            sleep_func=lambda _: None,
        )
        with pytest.raises(InvalidResponseError) as exc_info:
            client.call("/pay", payload=PAYLOAD_V2)
        assert exc_info.value.kind is ErrorKind.INVALID_RESPONSE
        assert exc_info.value.kind is not ErrorKind.UPSTREAM_SERVER_ERROR
        assert exc_info.value.retryable is False
        assert up.calls == 1  # 只打一次，不重试
        assert "500" in exc_info.value.message  # 消息里保留 HTTP 状态线索

    def test_5xx_html_does_not_leak_parser_exception(self):
        up = MockUpstream("html_5xx")
        client = make_client(up)
        with pytest.raises(InvalidResponseError) as exc_info:
            client.call("/pay", payload=PAYLOAD_V2)
        assert "JSONDecodeError" not in str(exc_info.value)
        assert exc_info.value.__cause__ is None

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
