"""弹性上游网关 HTTP 入口。

- POST /gateway/{path}：代理调用上游，自动做错误归一化、限流、熔断、重试、
  幂等（Idempotency-Key 头）、版本协商（X-Api-Version 头）。
- 所有失败都返回统一错误体：{"error": <类别>, "message": ..., "retryable": ...}，
  绝不把底层库异常原样透给调用方。
"""

from __future__ import annotations

import os

import uvicorn
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

from gateway.client import ResilientUpstreamClient
from gateway.errors import ErrorKind, GatewayError

# 错误类别 -> 对调用方的 HTTP 状态码
_STATUS_BY_KIND = {
    ErrorKind.TIMEOUT: 504,
    ErrorKind.CONNECTION: 502,
    ErrorKind.UPSTREAM_CLIENT_ERROR: 400,
    ErrorKind.UPSTREAM_SERVER_ERROR: 502,
    ErrorKind.CIRCUIT_OPEN: 503,
    ErrorKind.RATE_LIMITED: 429,
    ErrorKind.VERSION_UNSUPPORTED: 400,
    ErrorKind.SCHEMA_MISMATCH: 502,
    ErrorKind.UNKNOWN: 500,
}


def create_app(client: ResilientUpstreamClient | None = None) -> FastAPI:
    app = FastAPI(title="resilient-upstream-gateway")
    app.state.upstream_client = client or ResilientUpstreamClient(
        base_url=os.environ.get("UPSTREAM_BASE_URL", "http://127.0.0.1:9000"),
    )

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_: Request, exc: GatewayError) -> JSONResponse:
        status = _STATUS_BY_KIND.get(exc.kind, 500)
        if isinstance(exc, GatewayError) and getattr(exc, "status_code", None) and exc.kind is ErrorKind.UPSTREAM_CLIENT_ERROR:
            status = exc.status_code  # 上游 4xx 原状态码更有信息量
        return JSONResponse(status_code=status, content=exc.to_dict())

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "circuit": app.state.upstream_client.circuit_breaker.state.value}

    @app.post("/gateway/{path:path}")
    def proxy(
        path: str,
        payload: dict | None = None,
        idempotency_key: str | None = Header(default=None),
        x_api_version: str | None = Header(default=None),
    ) -> dict:
        return app.state.upstream_client.call(
            f"/{path}",
            idempotency_key=idempotency_key,
            version=x_api_version,
            payload=payload,
        )

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
