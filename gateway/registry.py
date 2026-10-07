"""多上游注册表：同时登记多个带名字的上游，互相隔离。

隔离原则：
- 每个上游拥有自己独立的一套组件：httpx 客户端（独立地址/超时）、
  重试策略、熔断器、限流器、并发名额限制器。
- 某个上游挂了/一直超时，只会熔断、限流、占满它自己的名额，
  不会拖累其他上游。
- health() 分别给出每个上游的当前状态（熔断状态、在飞数、剩余令牌），
  而不是一个笼统的整体状态。

幂等是调用方层面的语义，注册表内所有上游共享同一个幂等存储：
指纹里含上游名/接口/版本/报文，同一幂等键换任何一项（包括换上游）
都明确报冲突，绝不默默复用旧结论或重复产生副作用。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable

import httpx

from .circuit_breaker import CircuitBreaker
from .client import ResilientUpstreamClient
from .concurrency import ConcurrencyLimiter
from .errors import UnknownUpstreamError
from .idempotency import IdempotencyStore
from .rate_limiter import TokenBucketRateLimiter
from .retry import RetryPolicy


@dataclass(frozen=True)
class UpstreamConfig:
    """单个上游的完整配置。所有额度都是该上游独享的。"""

    name: str
    base_url: str
    timeout: float = 5.0
    # 重试
    max_attempts: int = 3
    retry_base_delay: float = 0.1
    retry_max_delay: float = 2.0
    retry_jitter: float = 0.1
    # 熔断
    failure_threshold: int = 5
    success_threshold: int = 2
    recovery_timeout: float = 30.0
    # 限流（令牌桶）
    rate: float = 50.0
    rate_capacity: float = 100.0
    # 并发名额（舱壁）
    max_in_flight: int = 20
    overflow: str = "reject"       # "reject" 立刻报容量满 / "wait" 等 max_wait 秒
    max_wait: float = 0.0
    # 测试注入点（不放进构造的客户端签名里，由注册表透传）
    transport: httpx.BaseTransport | None = field(default=None, compare=False)
    sleep_func: Callable[[float], None] = field(default=time.sleep, compare=False)


class _UpstreamEntry:
    """一个上游 = 一个客户端 + 它独享的各组件（健康状态从这里读）。"""

    def __init__(
        self,
        *,
        name: str,
        base_url: str,
        client: ResilientUpstreamClient,
        circuit: CircuitBreaker,
        rate_limiter: TokenBucketRateLimiter,
        concurrency: ConcurrencyLimiter | None,
    ) -> None:
        self.name = name
        self.base_url = base_url
        self.client = client
        self.circuit = circuit
        self.rate_limiter = rate_limiter
        self.concurrency = concurrency

    @classmethod
    def from_config(
        cls, config: UpstreamConfig, idempotency_store: IdempotencyStore
    ) -> "_UpstreamEntry":
        """按配置新建一整套该上游独享的组件（幂等存储由注册表共享）。"""
        circuit = CircuitBreaker(
            failure_threshold=config.failure_threshold,
            success_threshold=config.success_threshold,
            recovery_timeout=config.recovery_timeout,
        )
        rate_limiter = TokenBucketRateLimiter(
            rate=config.rate, capacity=config.rate_capacity
        )
        concurrency = ConcurrencyLimiter(
            max_in_flight=config.max_in_flight,
            overflow=config.overflow,
            max_wait=config.max_wait,
        )
        client = ResilientUpstreamClient(
            base_url=config.base_url,
            timeout=config.timeout,
            retry_policy=RetryPolicy(
                max_attempts=config.max_attempts,
                base_delay=config.retry_base_delay,
                max_delay=config.retry_max_delay,
                jitter=config.retry_jitter,
            ),
            circuit_breaker=circuit,
            rate_limiter=rate_limiter,
            concurrency_limiter=concurrency,
            idempotency_store=idempotency_store,
            transport=config.transport,
            sleep_func=config.sleep_func,
            upstream_name=config.name,
        )
        return cls(
            name=config.name,
            base_url=config.base_url,
            client=client,
            circuit=circuit,
            rate_limiter=rate_limiter,
            concurrency=concurrency,
        )

    @classmethod
    def from_client(cls, name: str, client: ResilientUpstreamClient) -> "_UpstreamEntry":
        """包装外部构造好的客户端（兼容单上游时代的注入方式）。

        组件直接复用客户端内部已有的那一套，健康状态照常可读。
        """
        return cls(
            name=name,
            base_url=str(client.base_url),
            client=client,
            circuit=client.circuit_breaker,
            rate_limiter=client.rate_limiter,
            concurrency=client.concurrency_limiter,
        )

    def health(self) -> dict:
        return {
            "name": self.name,
            "base_url": self.base_url,
            "circuit": self.circuit.state.value,
            "in_flight": self.concurrency.in_flight if self.concurrency else 0,
            "max_in_flight": self.concurrency.max_in_flight if self.concurrency else None,
            "available_tokens": round(self.rate_limiter.available_tokens, 3),
        }


class UpstreamRegistry:
    """带名字的上游集合。调用按名字路由，状态按名字隔离。"""

    def __init__(self, *, idempotency_store: IdempotencyStore | None = None) -> None:
        self._lock = threading.Lock()
        self._upstreams: dict[str, _UpstreamEntry] = {}
        # 注册表级共享幂等存储：同一幂等键跨上游也受指纹约束（换上游即冲突）。
        # 不能用 `or`：空存储因 __len__ 是 falsy，会被误换成新实例
        self._idempotency = (
            idempotency_store if idempotency_store is not None else IdempotencyStore()
        )

    def register(self, config: UpstreamConfig) -> None:
        if not config.name:
            raise ValueError("上游名称不能为空")
        with self._lock:
            if config.name in self._upstreams:
                raise ValueError(f"上游 {config.name!r} 已登记")
            self._upstreams[config.name] = _UpstreamEntry.from_config(
                config, self._idempotency
            )

    def register_client(self, name: str, client: ResilientUpstreamClient) -> None:
        """把外部构造好的客户端登记为具名上游（兼容单上游时代的注入方式）。"""
        if not name:
            raise ValueError("上游名称不能为空")
        with self._lock:
            if name in self._upstreams:
                raise ValueError(f"上游 {name!r} 已登记")
            self._upstreams[name] = _UpstreamEntry.from_client(name, client)

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self._upstreams)

    def _entry(self, name: str) -> _UpstreamEntry:
        with self._lock:
            entry = self._upstreams.get(name)
        if entry is None:
            raise UnknownUpstreamError(
                f"未登记的上游 {name!r}，已登记: {', '.join(self.names()) or '(无)'}"
            )
        return entry

    def call(
        self,
        name: str,
        path: str,
        *,
        idempotency_key: str | None = None,
        version: str | None = None,
        payload: dict | None = None,
    ) -> dict:
        return self._entry(name).client.call(
            path,
            idempotency_key=idempotency_key,
            version=version,
            payload=payload,
        )

    def health(self, name: str) -> dict:
        """单个上游的当前状态。"""
        return self._entry(name).health()

    def health_all(self) -> dict:
        """每个上游分别的状态，不给笼统的整体结论。"""
        with self._lock:
            entries = [self._upstreams[n] for n in sorted(self._upstreams)]
        return {e.name: e.health() for e in entries}

    def close(self) -> None:
        with self._lock:
            entries = list(self._upstreams.values())
        for e in entries:
            e.client.close()
