"""限流器：令牌桶，线程安全。"""

from __future__ import annotations

import threading


class TokenBucketRateLimiter:
    """占位骨架。"""

    def __init__(self, *, rate: float, capacity: float, time_func=None) -> None:
        self._lock = threading.Lock()

    def allow(self) -> bool:
        raise NotImplementedError
