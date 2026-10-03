"""弹性上游客户端：组合错误归一化、限流、熔断、重试、幂等、版本兼容。"""

from __future__ import annotations


class ResilientUpstreamClient:
    """占位骨架。"""

    def __init__(self, *, base_url: str, **kwargs) -> None:
        self._base_url = base_url

    def call(self, path: str, *, idempotency_key: str | None = None,
             version: str | None = None, payload: dict | None = None) -> dict:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError
