"""弹性上游网关 HTTP 入口（多上游版，兼容单上游老用法）。

路由：
- POST /gateway/{upstream}/{path}：按名字路由到对应上游（多上游新用法）。
- POST /gateway/{path}：单上游时代的老路径，路由到名为 "default" 的上游，
  升级后老调用照常可用（按名字的路由是在旁边加的，没有顶掉老路径）。
- GET /health：每个上游分别的状态（熔断/在飞数/剩余令牌），不给笼统整体结论。
- GET /health/{upstream}：单个上游的状态。

每次调用自动做错误归一化、限流、并发名额、熔断、重试、
幂等（Idempotency-Key 头）、版本协商（X-Api-Version 头）。
所有失败都返回统一错误体：{"error": <类别>, "message": ..., "retryable": ...}，
绝不把底层库异常原样透给调用方。

上游配置：环境变量 UPSTREAMS 为 JSON，例如
  {"pay": {"base_url": "http://127.0.0.1:9001", "max_in_flight": 10},
   "risk": {"base_url": "http://127.0.0.1:9002", "overflow": "wait", "max_wait": 0.5}}
未设置 UPSTREAMS 时回退到单上游：UPSTREAM_BASE_URL（默认 http://127.0.0.1:9000），
名字为 "default"。

兼容说明：
- create_app() 既接受 UpstreamRegistry（新），也接受直接构造的
  ResilientUpstreamClient（老用法），后者会被登记为 "default" 上游。
- 多段路径 /gateway/a/b 优先按「名字路由」解析（a 为上游名）；
  单段路径 /gateway/pay 走老路径，打到 "default" 上游。
"""

from __future__ import annotations

import json
import os

import uvicorn
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

from gateway.client import ResilientUpstreamClient
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
    ErrorKind.CAPACITY_FULL: 503,
    ErrorKind.UNKNOWN_UPSTREAM: 404,
    ErrorKind.IDEMPOTENCY_CONFLICT: 409,
    ErrorKind.VERSION_UNSUPPORTED: 400,
    ErrorKind.INVALID_REQUEST: 400,
    ErrorKind.SCHEMA_MISMATCH: 502,
    ErrorKind.INVALID_RESPONSE: 502,
    ErrorKind.UNKNOWN: 500,
}

DEFAULT_UPSTREAM = "default"


def registry_from_env() -> UpstreamRegistry:
    """从环境变量构建注册表：UPSTREAMS(JSON) 优先，缺省回退单上游。"""
    registry = UpstreamRegistry()
    raw = os.environ.get("UPSTREAMS")
    if raw:
        configs = json.loads(raw)
        for name, cfg in configs.items():
            registry.register(UpstreamConfig(name=name, **cfg))
        return registry
    registry.register(
        UpstreamConfig(
            name=DEFAULT_UPSTREAM,
            base_url=os.environ.get("UPSTREAM_BASE_URL", "http://127.0.0.1:9000"),
        )
    )
    return registry


def create_app(
    registry: UpstreamRegistry | ResilientUpstreamClient | None = None,
) -> FastAPI:
    """构建应用。

    - 不传：从环境变量构建注册表（UPSTREAMS / UPSTREAM_BASE_URL）；
    - 传 UpstreamRegistry：多上游新用法；
    - 传 ResilientUpstreamClient：单上游时代的老用法，客户端被登记为
      "default" 上游，老路径 /gateway/{path} 照常可用。
    """
    if registry is None:
        registry = registry_from_env()
    elif isinstance(registry, ResilientUpstreamClient):
        client = registry
        registry = UpstreamRegistry()
        registry.register_client(DEFAULT_UPSTREAM, client)

    app = FastAPI(title="resilient-upstream-gateway")
    app.state.registry = registry

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_: Request, exc: GatewayError) -> JSONResponse:
        status = _STATUS_BY_KIND.get(exc.kind, 500)
        if getattr(exc, "status_code", None) and exc.kind is ErrorKind.UPSTREAM_CLIENT_ERROR:
            status = exc.status_code  # 上游 4xx 原状态码更有信息量
        return JSONResponse(status_code=status, content=exc.to_dict())

    @app.get("/health")
    def health_all() -> dict:
        return {"status": "ok", "upstreams": registry.health_all()}

    @app.get("/health/{upstream}")
    def health_one(upstream: str) -> dict:
        return registry.health(upstream)

    @app.post("/gateway/{upstream}/{path:path}")
    def proxy_named(
        upstream: str,
        path: str,
        payload: dict | None = None,
        idempotency_key: str | None = Header(default=None),
        x_api_version: str | None = Header(default=None),
    ) -> dict:
        return registry.call(
            upstream,
            f"/{path}",
            idempotency_key=idempotency_key,
            version=x_api_version,
            payload=payload,
        )

    @app.post("/gateway/{path:path}")
    def proxy_default(
        path: str,
        payload: dict | None = None,
        idempotency_key: str | None = Header(default=None),
        x_api_version: str | None = Header(default=None),
    ) -> dict:
        # 单上游时代的老路径：打到 "default" 上游，升级后老调用照常可用
        return registry.call(
            DEFAULT_UPSTREAM,
            f"/{path}",
            idempotency_key=idempotency_key,
            version=x_api_version,
            payload=payload,
        )

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
