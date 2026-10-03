"""Gateway service orchestration.

Request pipeline for ``submit_order``:

1. **Rate limit** -- token bucket; rejection is immediate (RATE_LIMITED).
2. **Idempotency** -- the idempotency key plus a fingerprint of the
   logical payload guarantee the upstream side effect happens at most
   once; duplicates replay or await the original result.
3. **Circuit breaker** -- one logical call counts as one success/failure;
   when open, calls fail fast (CIRCUIT_OPEN) without touching upstream.
4. **Retry** -- transient failures inside the breaker are retried with
   backoff; exhaustion yields RETRIES_EXHAUSTED.
5. **Version adaptation** -- request/response shapes are translated per
   upstream version; missing fields raise MISSING_FIELD.

Every collaborator is injectable so tests can use a mock upstream
transport, a fake clock and a no-op sleep.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from app.circuit_breaker import CircuitBreaker
from app.errors import RateLimitedError
from app.idempotency import IdempotencyStore
from app.rate_limiter import TokenBucketRateLimiter
from app.retry import RetryPolicy
from app.upstream import UpstreamClient
from app.versions import UpstreamAdapter


class GatewayService:
    def __init__(
        self,
        *,
        upstream: UpstreamClient,
        adapter: UpstreamAdapter | None = None,
        breaker: CircuitBreaker | None = None,
        limiter: TokenBucketRateLimiter | None = None,
        retry: RetryPolicy | None = None,
        idempotency: IdempotencyStore | None = None,
    ) -> None:
        self._upstream = upstream
        self._adapter = adapter or UpstreamAdapter()
        self._breaker = breaker or CircuitBreaker()
        self._limiter = limiter or TokenBucketRateLimiter(rate_per_second=50.0, burst=100)
        self._retry = retry or RetryPolicy()
        self._idempotency = idempotency or IdempotencyStore()

    @staticmethod
    def _fingerprint(version: str, order_id: str, amount: float) -> str:
        canonical = json.dumps(
            {"version": version, "order_id": order_id, "amount": amount},
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()

    async def submit_order(
        self,
        *,
        order_id: str,
        amount: float,
        idempotency_key: str,
        version: str = "v1",
    ) -> dict[str, Any]:
        if not await self._limiter.acquire():
            raise RateLimitedError("gateway rate limit exceeded")

        # Validate the version before claiming the idempotency key so a
        # bad version never poisons the store.
        self._adapter.check_version(version)
        fingerprint = self._fingerprint(version, order_id, amount)

        async def call_upstream() -> dict[str, Any]:
            async with self._breaker:
                body = await self._retry.run(
                    lambda: self._upstream.post_json(
                        self._adapter.endpoint(version),
                        self._adapter.build_request(version, order_id, amount),
                    )
                )
            return self._adapter.parse_response(version, body)

        return await self._idempotency.execute(idempotency_key, fingerprint, call_upstream)
