"""弹性上游网关 HTTP 入口（支持多个命名上游）。

- POST /upstreams/{name}/call/{path}：调用指定名字的上游，自动做错误归一化、
  限流、熔断、并发隔离、重试、幂等（Idempotency-Key 头）、
  版本协商与请求适配（X-Api-Version 头）。
- POST /gateway/{path}：向后兼容的单上游入口，走默认上游（名为 "default"）。
- GET /health：逐上游健康状态（熔断态/在飞数/排队数/令牌余量），不是笼统整体状态。
- 所有失败都返回统一错误体：{"error": <类别>, "message": ..., "retryable": ...}，
  绝不把底层库异常原样透给调用方。

多上游配置通过环境变量 GATEWAY_UPSTREAMS（JSON）声明，例如：
  [{"name": "payment", "base_url": "http://10.0.0.1:9000",
    "timeout": 3.0, "max_concurrent": 16, "rate_limit_rate": 100.0},
   {"name": "risk", "base_url": "http://10.0.0.2:9000",
    "failure_threshold": 3, "max_concurrent": 8}]
未设置时退化为单个 default 上游（地址取 UPSTREAM_BASE_URL，向后兼容）。
"""

from __future__ import annotations

import json
import os

import uvicorn
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

from gateway.errors import ErrorKind, GatewayError
from gateway.registry import UpstreamConfig, UpstreamRegistry

# 错误类别 -> 对调用方的 HTTP 状态码
_STATUS_BY_KIND = {
    ErrorKind.TIMEOUT: 504,
    ErrorKind.CONNECTION: 502,
    ErrorKind.UPSTREAM_CLIENT_ERROR: 400,
    ErrorKind.UPSTREAM_SERVER_ERROR: 502,
    ErrorKind.CIRCUIT_OPEN: 503,
    ErrorKind.RATE_LIMITED: 429,
    ErrorKind.CAPACITY_FULL: 503,          # 太忙：和熔断同为 503，但 error 类别不同
    ErrorKind.IDEMPOTENCY_CONFLICT: 409,
    ErrorKind.VERSION_UNSUPPORTED: 400,
    ErrorKind.REQUEST_INVALID: 422,
    ErrorKind.INVALID_JSON: 502,
    ErrorKind.SCHEMA_MISMATCH: 502,
    ErrorKind.UPSTREAM_NOT_FOUND: 404,
    ErrorKind.UNKNOWN: 500,
}

DEFAULT_UPSTREAM_NAME = "default"


def build_registry_from_env() -> UpstreamRegistry:
    """从环境变量构建多上游登记处；未配置则退化为单个 default 上游。"""
    registry = UpstreamRegistry()
    raw = os.environ.get("GATEWAY_UPSTREAMS")
    if raw:
        for item in json.loads(raw):
            name = item.pop("name")
            registry.register(UpstreamConfig.from_dict(name, item))
    else:
        registry.register(UpstreamConfig(
            name=DEFAULT_UPSTREAM_NAME,
            base_url=os.environ.get("UPSTREAM_BASE_URL", "http://127.0.0.1:9000"),
        ))
    return registry


def create_app(client=None, *, registry: UpstreamRegistry | None = None) -> FastAPI:
    """构造应用。

    - registry：直接注入多上游登记处（测试/多上游场景用）。
    - client：向后兼容，注入单个已构造的客户端时包成只有 default 的登记处。
    """
    app = FastAPI(title="resilient-upstream-gateway")

    if registry is not None:
        app.state.registry = registry
    elif client is not None:
        app.state.registry = _registry_with_legacy_client(client)
    else:
        app.state.registry = build_registry_from_env()

    # 兼容旧代码直接访问 app.state.upstream_client
    app.state.upstream_client = (
        app.state.registry.get(DEFAULT_UPSTREAM_NAME)
        if DEFAULT_UPSTREAM_NAME in app.state.registry
        else None
    )

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_: Request, exc: GatewayError) -> JSONResponse:
        status = _STATUS_BY_KIND.get(exc.kind, 500)
        # 上游 4xx 原状态码更有信息量
        if exc.kind is ErrorKind.UPSTREAM_CLIENT_ERROR and getattr(exc, "status_code", None):
            status = exc.status_code
        return JSONResponse(status_code=status, content=exc.to_dict())

    @app.get("/health")
    def health() -> dict:
        registry = app.state.registry
        upstreams = registry.health()
        # 顶层 circuit 向后兼容：优先 default，没有则取任一上游
        legacy_circuit = None
        if DEFAULT_UPSTREAM_NAME in upstreams:
            legacy_circuit = upstreams[DEFAULT_UPSTREAM_NAME]["circuit"]
        elif upstreams:
            legacy_circuit = next(iter(upstreams.values()))["circuit"]
        return {
            "status": "ok",
            "circuit": legacy_circuit,
            "upstreams": upstreams,  # 逐上游状态，不是笼统整体状态
        }

    @app.post("/upstreams/{name}/call/{path:path}")
    def call_named_upstream(
        name: str,
        path: str,
        payload: dict | None = None,
        idempotency_key: str | None = Header(default=None),
        x_api_version: str | None = Header(default=None),
    ) -> dict:
        upstream = app.state.registry.get(name)  # 未登记 -> upstream_not_found 404
        return upstream.call(
            f"/{path}",
            idempotency_key=idempotency_key,
            version=x_api_version,
            payload=payload,
        )

    @app.post("/gateway/{path:path}")
    def proxy(
        path: str,
        payload: dict | None = None,
        idempotency_key: str | None = Header(default=None),
        x_api_version: str | None = Header(default=None),
    ) -> dict:
        # 向后兼容的单上游入口：始终走 default 上游
        upstream = app.state.registry.get(DEFAULT_UPSTREAM_NAME)
        return upstream.call(
            f"/{path}",
            idempotency_key=idempotency_key,
            version=x_api_version,
            payload=payload,
        )

    return app


def _registry_with_legacy_client(client) -> UpstreamRegistry:
    registry = UpstreamRegistry()
    registry.attach(DEFAULT_UPSTREAM_NAME, client)
    return registry


app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
