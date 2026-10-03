"""弹性上游客户端：组合错误归一化、限流、熔断、重试、幂等、版本兼容。

单次 call() 的执行顺序：
1. 版本协商（不支持的版本直接失败，不发请求）。
2. 幂等键登记（重复提交直接复用首个请求的结论）。
3. 重试循环（每次尝试前）：
   a. 限流检查 —— 没令牌快速失败，不打到上游；
   b. 熔断检查 —— 熔断打开快速失败；
   c. 发起调用，传输异常归一化为 GatewayError；
   d. 5xx/传输错误记为熔断失败并按退避重试；4xx 直接失败不计入熔断；
   e. 成功后校验响应 schema，字段缺失给明确错误。
"""

from __future__ import annotations

import time
from typing import Callable

import httpx

from .circuit_breaker import CircuitBreaker
from .errors import (
    CircuitOpenError,
    GatewayError,
    RateLimitedError,
    UpstreamClientError,
    UpstreamServerError,
    map_transport_error,
)
from .idempotency import IdempotencyStore
from .rate_limiter import TokenBucketRateLimiter
from .retry import RetryPolicy
from .versioning import negotiate_version, validate_response


class ResilientUpstreamClient:
    def __init__(
        self,
        *,
        base_url: str,
        timeout: float = 5.0,
        retry_policy: RetryPolicy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        rate_limiter: TokenBucketRateLimiter | None = None,
        idempotency_store: IdempotencyStore | None = None,
        transport: httpx.BaseTransport | None = None,
        sleep_func: Callable[[float], None] = time.sleep,
    ) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout, transport=transport)
        self._retry = retry_policy or RetryPolicy()
        self._circuit = circuit_breaker or CircuitBreaker()
        self._rate_limiter = rate_limiter or TokenBucketRateLimiter(rate=50.0, capacity=100.0)
        self._idempotency = idempotency_store or IdempotencyStore()
        self._sleep = sleep_func

    def call(
        self,
        path: str,
        *,
        idempotency_key: str | None = None,
        version: str | None = None,
        payload: dict | None = None,
    ) -> dict:
        negotiated = negotiate_version(version)

        if idempotency_key is None:
            return self._execute(path, version=negotiated, payload=payload)

        claim = self._idempotency.begin(idempotency_key)
        if not claim.is_owner:
            if claim.error is not None:
                raise claim.error
            return claim.result
        try:
            result = self._execute(path, version=negotiated, payload=payload)
        except GatewayError as exc:
            self._idempotency.fail(idempotency_key, exc)
            raise
        self._idempotency.complete(idempotency_key, result)
        return result

    def _execute(self, path: str, *, version: str, payload: dict | None) -> dict:
        last_error: GatewayError | None = None
        for attempt in range(1, self._retry.max_attempts + 1):
            if not self._rate_limiter.allow():
                raise RateLimitedError("本地限流：请求过多，请稍后重试")
            if not self._circuit.allow_request():
                raise CircuitOpenError("熔断器打开：上游故障中，已快速失败")

            try:
                response = self._client.post(f"/{version}{path}", json=payload or {})
            except Exception as exc:  # 底层异常绝不原样透出
                error = map_transport_error(exc)
                self._circuit.record_failure()
                last_error = error
                if error.retryable and attempt < self._retry.max_attempts:
                    self._sleep(self._retry.delay_for_attempt(attempt))
                    continue
                raise error from None

            if response.status_code >= 500:
                error = UpstreamServerError(
                    f"上游服务错误: HTTP {response.status_code}",
                    status_code=response.status_code,
                )
                self._circuit.record_failure()
                last_error = error
                if attempt < self._retry.max_attempts:
                    self._sleep(self._retry.delay_for_attempt(attempt))
                    continue
                raise error from None

            if response.status_code >= 400:
                # 4xx 是请求方问题：不计入熔断、不重试
                raise UpstreamClientError(
                    f"上游拒绝请求: HTTP {response.status_code}",
                    status_code=response.status_code,
                    detail=_safe_body(response),
                ) from None

            self._circuit.record_success()
            return validate_response(version, _safe_json(response))

        assert last_error is not None  # 理论上不可达
        raise last_error

    @property
    def circuit_breaker(self) -> CircuitBreaker:
        return self._circuit

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "ResilientUpstreamClient":
        return self

    def __exit__(self, *args) -> None:
        self.close()


def _safe_json(response: httpx.Response):
    try:
        return response.json()
    except ValueError:
        from .errors import SchemaMismatchError

        raise SchemaMismatchError("上游响应不是合法 JSON") from None


def _safe_body(response: httpx.Response) -> object:
    try:
        return response.json()
    except ValueError:
        return response.text[:500]
