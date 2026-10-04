"""弹性上游客户端：组合错误归一化、限流、熔断、并发隔离、重试、幂等、版本兼容。

单次 call() 的执行顺序：
1. 版本协商（不支持的版本直接失败，不发请求）。
2. 按版本组装并校验请求报文（缺字段/不支持字段直接失败，不发请求）。
3. 幂等键登记（携带请求指纹；同键换内容直接报冲突；重复提交复用首个结论）。
4. 获取该上游的并发名额（隔离舱）：满了按配置等待或快速失败，结论与超时/熔断不同。
5. 重试循环（每次尝试前）：
   a. 限流检查 —— 没令牌快速失败，不打到上游；
   b. 熔断检查 —— 熔断打开快速失败；
   c. 发起调用，传输异常归一化为 GatewayError；
   d. 5xx/传输错误记为熔断失败并按退避重试；4xx 直接失败不计入熔断；
   e. 成功后：响应不是合法 JSON 给 INVALID_JSON；结构对不上给 SCHEMA_MISMATCH。
6. 无论成功失败还是中途放弃，并发名额都在 finally 中归还。
"""

from __future__ import annotations

import json
import time
from typing import Callable

import httpx

from .bulkhead import Bulkhead
from .circuit_breaker import CircuitBreaker
from .errors import (
    CapacityFullError,
    CircuitOpenError,
    GatewayError,
    InvalidJsonError,
    RateLimitedError,
    UpstreamClientError,
    UpstreamServerError,
    map_transport_error,
)
from .idempotency import IdempotencyStore
from .rate_limiter import TokenBucketRateLimiter
from .retry import RetryPolicy
from .versioning import negotiate_version, prepare_request, validate_response


class ResilientUpstreamClient:
    def __init__(
        self,
        *,
        base_url: str,
        timeout: float = 5.0,
        retry_policy: RetryPolicy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        rate_limiter: TokenBucketRateLimiter | None = None,
        bulkhead: Bulkhead | None = None,
        idempotency_store: IdempotencyStore | None = None,
        transport: httpx.BaseTransport | None = None,
        sleep_func: Callable[[float], None] = time.sleep,
    ) -> None:
        self._base_url = base_url
        self._client = httpx.Client(base_url=base_url, timeout=timeout, transport=transport)
        self._retry = retry_policy or RetryPolicy()
        self._circuit = circuit_breaker or CircuitBreaker()
        self._rate_limiter = rate_limiter or TokenBucketRateLimiter(rate=50.0, capacity=100.0)
        self._bulkhead = bulkhead or Bulkhead(max_concurrent=20, max_waiters=50,
                                              acquire_timeout=5.0)
        self._idempotency = idempotency_store or IdempotencyStore()
        self._sleep = sleep_func

    @property
    def base_url(self) -> str:
        return self._base_url

    def call(
        self,
        path: str,
        *,
        idempotency_key: str | None = None,
        version: str | None = None,
        payload: dict | None = None,
    ) -> dict:
        negotiated = negotiate_version(version)
        wire = prepare_request(negotiated, payload)  # 不合法请求在发网前就拦下

        if idempotency_key is None:
            return self._execute(path, version=negotiated, wire=wire)

        fingerprint = _fingerprint(path, negotiated, payload)
        claim = self._idempotency.begin(idempotency_key, fingerprint)
        if not claim.is_owner:
            if claim.error is not None:
                raise claim.error
            return claim.result
        try:
            result = self._execute(path, version=negotiated, wire=wire)
        except GatewayError as exc:
            self._idempotency.fail(idempotency_key, exc)
            raise
        self._idempotency.complete(idempotency_key, result)
        return result

    def _execute(self, path: str, *, version: str, wire: dict) -> dict:
        # 占该上游自己的并发名额；排队满/等超时给 capacity_full，与超时、熔断区分
        if not self._bulkhead.acquire():
            raise CapacityFullError(
                f"上游繁忙：并发名额已用尽（上限 {self._bulkhead.capacity}），请稍后重试"
            )
        try:
            return self._retry_loop(path, version=version, wire=wire)
        finally:
            self._bulkhead.release()  # 成功、失败、放弃都必须归还

    def _retry_loop(self, path: str, *, version: str, wire: dict) -> dict:
        last_error: GatewayError | None = None
        for attempt in range(1, self._retry.max_attempts + 1):
            if not self._rate_limiter.allow():
                raise RateLimitedError("本地限流：请求过多，请稍后重试")
            if not self._circuit.allow_request():
                raise CircuitOpenError("熔断器打开：上游故障中，已快速失败")

            try:
                response = self._client.post(f"/{version}{path}", json=wire)
            except Exception as exc:  # 底层异常绝不原样透出
                error = map_transport_error(exc)
                self._circuit.record_failure()
                last_error = error
                if error.retryable and attempt < self._retry.max_attempts:
                    self._sleep(self._retry.delay_for_attempt(attempt))
                    continue
                raise error from None

            # 先看响应体是不是合法 JSON：即使是 5xx，只要体无法解析，
            # 结论就是"非法响应"（invalid_json）而不是笼统的 5xx，且不重试
            if not _is_json_response(response):
                raise InvalidJsonError(
                    f"上游返回了无法解析的内容（HTTP {response.status_code}），不是合法 JSON",
                    detail={"status_code": response.status_code,
                            "content_type": response.headers.get("content-type"),
                            "body_preview": response.text[:200]},
                ) from None

            if response.status_code >= 500:
                error = UpstreamServerError(
                    f"上游服务错误: HTTP {response.status_code}",
                    status_code=response.status_code,
                    detail=_safe_body(response),
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

    @property
    def rate_limiter(self) -> TokenBucketRateLimiter:
        return self._rate_limiter

    @property
    def bulkhead(self) -> Bulkhead:
        return self._bulkhead

    def health(self) -> dict:
        """该上游自己的健康状态（供登记处汇总，不暴露笼统整体状态）。"""
        return {
            "circuit": self._circuit.state.value,
            "in_flight": self._bulkhead.active,
            "waiting": self._bulkhead.waiting,
            "available_tokens": round(self._rate_limiter.available_tokens, 3),
        }

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "ResilientUpstreamClient":
        return self

    def __exit__(self, *args) -> None:
        self.close()


def _fingerprint(path: str, version: str, payload: dict | None) -> str:
    """请求内容指纹：路径 + 版本 + 确定性序列化的语义报文。"""
    canonical = json.dumps(payload or {}, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=False)
    return f"{version}|{path}|{canonical}"


def _is_json_response(response: httpx.Response) -> bool:
    """判断响应体是否为合法 JSON（不抛解析层异常）。"""
    try:
        response.json()
    except ValueError:
        return False
    return True


def _safe_json(response: httpx.Response):
    try:
        return response.json()
    except ValueError:
        # 上游有响应（甚至 HTTP 200）但内容不是合法 JSON：单独结论，
        # 与超时、4xx/5xx、结构对不上都分开，且不抛出解析层原始异常
        snippet = response.text[:200]
        raise InvalidJsonError(
            "上游返回的内容不是合法 JSON，无法解析",
            detail={"content_type": response.headers.get("content-type"),
                    "body_preview": snippet},
        ) from None


def _safe_body(response: httpx.Response) -> object:
    try:
        return response.json()
    except ValueError:
        return response.text[:500]
