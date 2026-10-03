"""FastAPI application exposing the gateway endpoints.

The app normalizes every :class:`~app.errors.GatewayError` into a JSON
error body whose ``error.kind`` tells the caller exactly which failure
occurred; the HTTP status comes from the error itself.
"""

from __future__ import annotations

import os

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.errors import GatewayError
from app.service import GatewayService
from app.upstream import UpstreamClient


class OrderRequest(BaseModel):
    order_id: str = Field(min_length=1)
    amount: float = Field(gt=0)
    version: str = "v1"


def create_app(service: GatewayService | None = None) -> FastAPI:
    app = FastAPI(title="resilient-upstream-gateway")

    if service is None:
        service = GatewayService(
            upstream=UpstreamClient(
                base_url=os.environ.get("UPSTREAM_BASE_URL", "http://127.0.0.1:9999"),
                timeout=float(os.environ.get("UPSTREAM_TIMEOUT", "5")),
            )
        )
    app.state.service = service

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_request: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_payload())

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok", "circuit": service._breaker.state.value}

    @app.post("/orders", status_code=201)
    async def submit_order(
        body: OrderRequest,
        idempotency_key: str = Header(alias="Idempotency-Key"),
    ) -> dict:
        return await service.submit_order(
            order_id=body.order_id,
            amount=body.amount,
            idempotency_key=idempotency_key,
            version=body.version,
        )

    return app


app = create_app()
