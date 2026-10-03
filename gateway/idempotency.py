"""幂等键支持：同一幂等键的重复提交返回首个结果，不产生重复副作用。"""

from __future__ import annotations

import threading


class IdempotencyStore:
    """占位骨架。"""

    def __init__(self, *, ttl: float = 300.0, time_func=None) -> None:
        self._lock = threading.Lock()

    def begin(self, key: str):
        raise NotImplementedError

    def complete(self, key: str, result) -> None:
        raise NotImplementedError

    def fail(self, key: str, exc: BaseException) -> None:
        raise NotImplementedError
