"""HTTP client wrapper that normalizes httpx failures into GatewayError.

No httpx (or asyncio) exception ever crosses this boundary:

===============================  ==================================
httpx / transport condition      normalized to
===============================  ==================================
``httpx.TimeoutException``       :class:`UpstreamTimeoutError`
``httpx.ConnectError`` and other :class:`UpstreamConnectionError`
``httpx.TransportError``
HTTP 4xx from upstream           :class:`UpstreamClientError`
HTTP 5xx from upstream           :class:`UpstreamServerError`
non-JSON / non-object body       :class:`UpstreamBadResponseError`
===============================  ==================================
"""

from __future__ import annotations

from typing import Any

import httpx

from app.errors import (
    UpstreamBadResponseError,
    UpstreamClientError,
    UpstreamConnectionError,
    UpstreamServerError,
    UpstreamTimeoutError,
)


class UpstreamClient:
    def __init__(self, *, base_url: str, timeout: float = 5.0, transport: Any = None) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=httpx.Timeout(timeout),
            transport=transport,
        )

    async def post_json(self, path: str, payload: dict) -> dict[str, Any]:
        try:
            response = await self._client.post(path, json=payload)
        except httpx.TimeoutException as exc:
            raise UpstreamTimeoutError(f"upstream call timed out: {exc.__class__.__name__}") from exc
        except httpx.TransportError as exc:
            # ConnectError, ConnectTimeout (already caught above), ReadError, ...
            raise UpstreamConnectionError(f"cannot reach upstream: {exc.__class__.__name__}") from exc

        status = response.status_code
        if 400 <= status < 500:
            raise UpstreamClientError(
                f"upstream rejected the request with {status}",
                upstream_status=status,
                detail={"upstream_status": status, "upstream_body": response.text[:500]},
            )
        if status >= 500:
            raise UpstreamServerError(
                f"upstream failed with {status}",
                upstream_status=status,
                detail={"upstream_status": status},
            )
        try:
            body = response.json()
        except ValueError as exc:
            raise UpstreamBadResponseError("upstream returned a non-JSON body") from exc
        if not isinstance(body, dict):
            raise UpstreamBadResponseError("upstream returned a non-object JSON body")
        return body

    async def aclose(self) -> None:
        await self._client.aclose()
