"""弹性上游客户端：组合错误归一化、限流、熔断、重试、幂等、版本兼容。

单次 call() 的执行顺序：
1. 版本协商（不支持的版本直接失败，不发请求）。
2. 幂等键登记（重复提交直接复用首个请求的结论；同键换内容报冲突）。
3. 请求报文按所对接版本组装校验（字段缺失/不支持直接失败，不发请求）。
4. 并发名额获取（该上游在飞请求超限：按配置等待或直接报“容量满”；
   无论成功/失败/放弃，名额都在 finally 中归还）。
5. 重试循环（每次尝试前）：
   a. 限流检查 —— 没令牌快速失败，不打到上游；
   b. 熔断检查 —— 熔断打开快速失败；
   c. 发起调用，传输异常归一化为 GatewayError；
   d. 响应体不是合法 JSON：无论状态码都报 INVALID_RESPONSE，独立结论、不重试；
   e. 5xx/传输错误记为熔断失败并按退避重试；4xx 直接失败不计入熔断；
   f. 成功响应再校验结构：顶层不是对象报 INVALID_RESPONSE，
      字段缺失报 SCHEMA_MISMATCH，都与超时/4xx/5xx 分得开。
"""

from __future__ import annotations

import json
import time
from typing import Callable

import httpx

from .circuit_breaker import CircuitBreaker
from .concurrency import ConcurrencyLimiter
from .errors import (
    CircuitOpenError,
    GatewayError,
    InvalidResponseError,
    RateLimitedError,
    UpstreamClientError,
    UpstreamServerError,
    map_transport_error,
)
from .idempotency import IdempotencyStore
from .rate_limiter import TokenBucketRateLimiter
from .retry import RetryPolicy
from .versioning import build_request, negotiate_version, validate_response


class ResilientUpstreamClient:
    def __init__(
        self,
        *,
        base_url: str,
        timeout: float = 5.0,
        retry_policy: RetryPolicy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        rate_limiter: TokenBucketRateLimiter | None = None,
        concurrency_limiter: ConcurrencyLimiter | None = None,
        idempotency_store: IdempotencyStore | None = None,
        transport: httpx.BaseTransport | None = None,
        sleep_func: Callable[[float], None] = time.sleep,
        upstream_name: str | None = None,
    ) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout, transport=transport)
        self._upstream_name = upstream_name
        # 注意不能用 `x or Default()`：IdempotencyStore 实现了 __len__，
        # 空存储是 falsy，会被 or 误换成新实例，共享存储就接不上了
        self._retry = retry_policy if retry_policy is not None else RetryPolicy()
        self._circuit = circuit_breaker if circuit_breaker is not None else CircuitBreaker()
        self._rate_limiter = (
            rate_limiter
            if rate_limiter is not None
            else TokenBucketRateLimiter(rate=50.0, capacity=100.0)
        )
        self._concurrency = concurrency_limiter  # None 表示不限制在飞数量
        self._idempotency = (
            idempotency_store if idempotency_store is not None else IdempotencyStore()
        )
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

        # 请求指纹覆盖整个请求：上游/接口/版本/报文，任何一项变了都明确报冲突
        fingerprint = _request_fingerprint(self._upstream_name, path, negotiated, payload)
        claim = self._idempotency.begin(idempotency_key, fingerprint=fingerprint)
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
        # 请求报文先按版本组装校验：字段缺失/不支持直接失败，不发请求
        body = build_request(version, payload)
        if self._concurrency is not None:
            # 名额在整个调用期间持有（含重试），任何退出路径都归还
            with self._concurrency.slot():
                return self._attempts(path, version=version, body=body)
        return self._attempts(path, version=version, body=body)

    def _attempts(self, path: str, *, version: str, body: dict) -> dict:
        last_error: GatewayError | None = None
        for attempt in range(1, self._retry.max_attempts + 1):
            if not self._rate_limiter.allow():
                raise RateLimitedError("本地限流：请求过多，请稍后重试")
            if not self._circuit.allow_request():
                raise CircuitOpenError("熔断器打开：上游故障中，已快速失败")

            try:
                response = self._client.post(f"/{version}{path}", json=body)
            except Exception as exc:  # 底层异常绝不原样透出
                error = map_transport_error(exc)
                self._circuit.record_failure()
                last_error = error
                if error.retryable and attempt < self._retry.max_attempts:
                    self._sleep(self._retry.delay_for_attempt(attempt))
                    continue
                raise error from None

            # 无论状态码是什么，响应体不是合法 JSON 都属于解析层的独立结论：
            # 不按 4xx/5xx 归类、不重试、不透出解析层原始异常
            parsed = _safe_json(response)

            if response.status_code >= 500:
                error = UpstreamServerError(
                    f"上游服务错误: HTTP {response.status_code}",
                    status_code=response.status_code,
                    detail=parsed,
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
                    detail=parsed,
                ) from None

            if not isinstance(parsed, dict):
                raise InvalidResponseError(
                    f"上游响应顶层不是 JSON 对象: {type(parsed).__name__}"
                ) from None
            result = validate_response(version, parsed)
            self._circuit.record_success()
            return result

        assert last_error is not None  # 理论上不可达
        raise last_error

    @property
    def base_url(self) -> str:
        return str(self._client.base_url)

    @property
    def circuit_breaker(self) -> CircuitBreaker:
        return self._circuit

    @property
    def rate_limiter(self) -> TokenBucketRateLimiter:
        return self._rate_limiter

    @property
    def concurrency_limiter(self) -> ConcurrencyLimiter | None:
        return self._concurrency

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "ResilientUpstreamClient":
        return self

    def __exit__(self, *args) -> None:
        self.close()


def _request_fingerprint(
    upstream_name: str | None, path: str, version: str, payload: dict | None
) -> str:
    """整个请求的指纹：打到哪个上游、哪个接口、走哪个版本、报文内容全算进去。

    同一幂等键只要其中任何一项变了，就是两次不同的调用，必须明确报冲突，
    绝不把上一次的结论原样复用回去。
    """
    return json.dumps(
        {
            "upstream": upstream_name,
            "path": path,
            "version": version,
            "payload": payload or {},
        },
        sort_keys=True,
        default=str,
    )


def _safe_json(response: httpx.Response):
    try:
        return response.json()
    except ValueError:
        raise InvalidResponseError("上游响应不是合法 JSON") from None
