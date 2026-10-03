"""本地模拟上游：基于 httpx.MockTransport，不依赖任何外部服务。

支持场景：
- ok             正常返回对应版本的完整响应
- timeout        抛出 httpx.ReadTimeout（模拟上游超时）
- conn_error     抛出 httpx.ConnectError（模拟连接失败）
- bad_request    返回 400
- server_error   返回 500
- missing_field  返回 200 但缺少必填字段
- flaky          前 flaky_failures 次返回 500，之后恢复正常
"""

from __future__ import annotations

import threading

import httpx


class MockUpstream:
    def __init__(self, scenario: str = "ok", *, flaky_failures: int = 0) -> None:
        self.scenario = scenario
        self.flaky_failures = flaky_failures
        self.calls = 0
        self._lock = threading.Lock()

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        with self._lock:
            self.calls += 1
            call_no = self.calls

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
        if self.scenario == "flaky":
            if call_no <= self.flaky_failures:
                return httpx.Response(500, json={"upstream_error": "boom"})
            return self._ok(request)
        return self._ok(request)

    @staticmethod
    def _ok(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/v1/"):
            return httpx.Response(200, json={"id": "tx-1", "status": "ok", "amount": 100})
        return httpx.Response(
            200,
            json={"id": "tx-1", "status": "ok", "amount": 100, "currency": "CNY"},
        )
