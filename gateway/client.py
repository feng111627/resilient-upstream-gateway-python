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
   d. 先解析响应体：压根不是合法 JSON、或顶层不是 JSON 对象，
      一律给 INVALID_RESPONSE 独立结论（不重试、不计入熔断、
      不透出解析层原始异常），哪怕它顶着 5xx 状态码也不再按 5xx 重试；
   e. 5xx/传输错误记为熔断失败并按退避重试；4xx 直接失败不计入熔断；
   f. 成功后按版本校验响应字段，字段缺失报 SCHEMA_MISMATCH，
      与“压根不是合法 JSON”的 INVALID_RESPONSE 分得开。
"""

from __future__ import annotations

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
from .idempotency import IdempotencyStore, request_fingerprint
from .rate_limiter import TokenBucketRateLimiter
from .retry import RetryPolicy
from .versioning import build_request, negotiate_version, validate_response


class ResilientUpstreamClient:
    def __init__(
        self,
        *,
        base_url: str,
        name: str | None = None,
        timeout: float = 5.0,
        retry_policy: RetryPolicy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        rate_limiter: TokenBucketRateLimiter | None = None,
        concurrency_limiter: ConcurrencyLimiter | None = None,
        idempotency_store: IdempotencyStore | None = None,
        transport: httpx.BaseTransport | None = None,
        sleep_func: Callable[[float], None] = time.sleep,
    ) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout, transport=transport)
        self._name = name
        self._retry = retry_policy or RetryPolicy()
        self._circuit = circuit_breaker or CircuitBreaker()
        self._rate_limiter = rate_limiter or TokenBucketRateLimiter(rate=50.0, capacity=100.0)
        self._concurrency = concurrency_limiter  # None 表示不限制在飞数量
        # 注意：IdempotencyStore 实现了 __len__，空存储是 falsy，
        # 这里必须显式判 None，不能用 `or`，否则外部注入的空存储会被偷偷换掉
        self._idempotency = (
            idempotency_store if idempotency_store is not None else IdempotencyStore()
        )
        self._sleep = sleep_func

    @property
    def name(self) -> str | None:
        return self._name

    def bind_name(self, name: str) -> None:
        """由注册表回填上游名（幂等指纹需要覆盖“打到哪个上游”）。

        客户端只能绑定一次；试图绑成另一个名字说明它被重复登记，明确报错。
        """
        if self._name is not None and self._name != name:
            raise ValueError(
                f"该客户端已绑定上游名 {self._name!r}，不能再登记为 {name!r}"
            )
        self._name = name

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

        # 整个请求身份的指纹：上游/接口/版本/报文任一不同都算不同请求，
        # 同一幂等键换了请求要明确报冲突，不能复用旧结论
        fingerprint = request_fingerprint(
            upstream=self._name, path=path, version=negotiated, payload=payload
        )
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

            # 先解析响应体：不是合法 JSON / 顶层不是对象，给独立结论。
            # 不看状态码——顶着 5xx 的 HTML 错误页同样不许按 5xx 反复重试。
            parsed = _parse_json_object(response)

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

            self._circuit.record_success()
            return validate_response(version, parsed)

        assert last_error is not None  # 理论上不可达
        raise last_error

    @property
    def circuit_breaker(self) -> CircuitBreaker:
        return self._circuit

    @property
    def rate_limiter(self) -> TokenBucketRateLimiter:
        return self._rate_limiter

    @property
    def concurrency_limiter(self) -> ConcurrencyLimiter | None:
        return self._concurrency

    @property
    def base_url(self) -> str:
        return str(self._client.base_url)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "ResilientUpstreamClient":
        return self

    def __exit__(self, *args) -> None:
        self.close()


def _parse_json_object(response: httpx.Response) -> dict:
    """把响应体解析成 JSON 对象。

    - 压根不是合法 JSON（如 HTML 错误页）：InvalidResponseError；
    - 是合法 JSON 但顶层不是对象（如数组/字符串）：InvalidResponseError；
    - 解析层原始异常（json.JSONDecodeError 等）绝不透给调用方。
    """
    try:
        parsed = response.json()
    except ValueError:
        raise InvalidResponseError(
            f"上游响应不是合法 JSON（HTTP {response.status_code}）"
        ) from None
    if not isinstance(parsed, dict):
        raise InvalidResponseError(
            f"上游响应顶层不是 JSON 对象: {type(parsed).__name__}"
        ) from None
    return parsed
