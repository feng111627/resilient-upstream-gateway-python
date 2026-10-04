"""本地模拟上游：基于 httpx.MockTransport，不依赖任何外部服务。

支持场景：
- ok             正常返回对应版本的完整响应
- timeout        抛出 httpx.ReadTimeout（模拟上游超时）
- conn_error     抛出 httpx.ConnectError（模拟连接失败）
- bad_request    返回 400
- server_error   返回 500
- missing_field  返回 200 但缺少必填字段
- invalid_json   返回 200 但响应体不是合法 JSON
- malformed_json 返回 502 且响应体不是合法 JSON（错误响应也无法解析）
- flaky          前 flaky_failures 次返回 500，之后恢复正常
- blocking       在 release_gate 被 set 之前一直阻塞（模拟慢上游，用于并发/名额测试）

last_request 记录最近一次收到的线上报文，用于验证请求按版本组装是否正确。
"""

from __future__ import annotations

import threading

import httpx


class MockUpstream:
    def __init__(
        self,
        scenario: str = "ok",
        *,
        flaky_failures: int = 0,
        release_gate: threading.Event | None = None,
    ) -> None:
        self.scenario = scenario
        self.flaky_failures = flaky_failures
        self.release_gate = release_gate
        self.calls = 0
        self.last_request: dict | None = None
        self.received: list[dict] = []
        self._lock = threading.Lock()

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        with self._lock:
            self.calls += 1
            call_no = self.calls

        body = self._parse_body(request)
        with self._lock:
            self.last_request = body
            self.received.append(body)

        if self.scenario == "timeout":
            raise httpx.ReadTimeout("simulated read timeout", request=request)
        if self.scenario == "conn_error":
            raise httpx.ConnectError("simulated connect error", request=request)
        if self.scenario == "bad_request":
            return httpx.Response(400, json={"upstream_error": "bad request"})
        if self.scenario == "server_error":
            return httpx.Response(500, json={"upstream_error": "boom"})
        if self.scenario == "missing_field":
            return httpx.Response(200, json={"id": "tx-1", "status": "ok"})  # 缺 amount 等
        if self.scenario == "invalid_json":
            return httpx.Response(
                200, content=b"<html>not json</html>",
                headers={"content-type": "text/html"},
            )
        if self.scenario == "malformed_json":
            return httpx.Response(
                502, content=b"oops, not json",
                headers={"content-type": "text/plain"},
            )
        if self.scenario == "blocking":
            if self.release_gate is not None:
                self.release_gate.wait()
            return self._ok(request)
        if self.scenario == "flaky":
            if call_no <= self.flaky_failures:
                return httpx.Response(500, json={"upstream_error": "boom"})
            return self._ok(request)
        return self._ok(request)

    @staticmethod
    def _parse_body(request: httpx.Request) -> dict | None:
        try:
            import json

            return json.loads(request.content or b"{}")
        except ValueError:
            return None

    @staticmethod
    def _ok(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/v1/"):
            return httpx.Response(200, json={"id": "tx-1", "status": "ok", "amount": 100})
        return httpx.Response(
            200,
            json={"id": "tx-1", "status": "ok", "amount": 100, "currency": "CNY"},
        )
