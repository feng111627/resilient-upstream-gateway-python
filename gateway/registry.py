"""多上游登记处：登记多个带名字的上游，各自独立配置、独立健康状态、互不影响。

隔离靠"每个上游各自一套组件实例"实现：熔断器、限流器、并发隔离舱、幂等存储
都是按上游独立创建的。因此 A 上游持续超时/被熔断/占满并发，只会消耗 A 自己
的额度，绝不影响 B 的放行、令牌或并发名额。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import httpx

from .bulkhead import Bulkhead
from .circuit_breaker import CircuitBreaker
from .client import ResilientUpstreamClient
from .errors import UpstreamNotFoundError
from .idempotency import IdempotencyStore
from .rate_limiter import TokenBucketRateLimiter
from .retry import RetryPolicy


@dataclass(frozen=True)
class UpstreamConfig:
    """单个命名上游的配置：地址、超时与重试/熔断/限流/并发额度。"""

    name: str
    base_url: str
    timeout: float = 5.0
    # 重试
    max_attempts: int = 3
    base_delay: float = 0.1
    max_delay: float = 2.0
    # 熔断
    failure_threshold: int = 5
    success_threshold: int = 2
    recovery_timeout: float = 30.0
    # 限流（令牌桶）
    rate_limit_rate: float = 50.0
    rate_limit_capacity: float = 100.0
    # 并发隔离舱
    max_concurrent: int = 20
    max_waiters: int = 50          # -1 表示排队不限；0 表示满了立即拒绝
    acquire_timeout: float | None = 5.0

    @staticmethod
    def from_dict(name: str, raw: dict) -> "UpstreamConfig":
        """从配置字典构造，忽略未知键，方便外部配置演进。"""
        known = {f for f in UpstreamConfig.__dataclass_fields__ if f != "name"}
        unknown = set(raw) - known
        if unknown:
            raise ValueError(f"上游 {name!r} 配置含未知字段: {', '.join(sorted(unknown))}")
        return UpstreamConfig(name=name, **raw)


@dataclass
class _Bound:
    """一个已登记上游的运行时绑定。"""

    config: UpstreamConfig
    client: ResilientUpstreamClient


class UpstreamRegistry:
    def __init__(
        self,
        *,
        transport_factory: Callable[[str], httpx.BaseTransport | None] | None = None,
        time_func: Callable[[], float] | None = None,
    ) -> None:
        # transport_factory(name) 便于测试注入 MockTransport；生产环境返回 None 走真实网络
        self._transport_factory = transport_factory or (lambda _name: None)
        self._time = time_func
        self._bounds: dict[str, _Bound] = {}

    def register(self, config: UpstreamConfig) -> ResilientUpstreamClient:
        """登记一个命名上游；同名重复登记直接报错，避免静默覆盖配置。"""
        if not config.name:
            raise ValueError("上游名字不能为空")
        if config.name in self._bounds:
            raise ValueError(f"上游 {config.name!r} 已登记，不能重复登记")

        retry = RetryPolicy(
            max_attempts=config.max_attempts,
            base_delay=config.base_delay,
            max_delay=config.max_delay,
        )
        breaker = CircuitBreaker(
            failure_threshold=config.failure_threshold,
            success_threshold=config.success_threshold,
            recovery_timeout=config.recovery_timeout,
            time_func=self._time,
        )
        limiter = TokenBucketRateLimiter(
            rate=config.rate_limit_rate,
            capacity=config.rate_limit_capacity,
            time_func=self._time,
        )
        bulkhead = Bulkhead(
            max_concurrent=config.max_concurrent,
            max_waiters=config.max_waiters,
            acquire_timeout=config.acquire_timeout,
        )
        client = ResilientUpstreamClient(
            base_url=config.base_url,
            timeout=config.timeout,
            retry_policy=retry,
            circuit_breaker=breaker,
            rate_limiter=limiter,
            bulkhead=bulkhead,
            idempotency_store=IdempotencyStore(),  # 每个上游独立的幂等空间
            transport=self._transport_factory(config.name),
        )
        self._bounds[config.name] = _Bound(config=config, client=client)
        return client

    def attach(self, name: str, client: ResilientUpstreamClient) -> None:
        """直接绑定一个已构造的客户端（主要用于测试注入与向后兼容包装）。"""
        if not name:
            raise ValueError("上游名字不能为空")
        if name in self._bounds:
            raise ValueError(f"上游 {name!r} 已登记，不能重复登记")
        self._bounds[name] = _Bound(
            config=UpstreamConfig(name=name, base_url=client.base_url),
            client=client,
        )

    def get(self, name: str) -> ResilientUpstreamClient:
        """按名字取上游客户端；未登记给明确结论而不是 None/KeyError。"""
        bound = self._bounds.get(name)
        if bound is None:
            raise UpstreamNotFoundError(
                f"未登记的上游 {name!r}，已登记: {', '.join(sorted(self._bounds)) or '(空)'}",
                detail={"upstream": name, "known": sorted(self._bounds)},
            )
        return bound.client

    def has(self, name: str) -> bool:
        return name in self._bounds

    def names(self) -> list[str]:
        """所有已登记上游的名字。"""
        return sorted(self._bounds)

    def health(self) -> dict:
        """逐上游健康状态：熔断态、在飞数、排队数、令牌余量，各自独立。"""
        return {
            name: {
                "base_url": bound.config.base_url,
                **bound.client.health(),
            }
            for name, bound in sorted(self._bounds.items())
        }

    def close(self) -> None:
        for bound in self._bounds.values():
            bound.client.close()

    def __contains__(self, name: object) -> bool:
        return name in self._bounds

    def __len__(self) -> int:
        return len(self._bounds)
