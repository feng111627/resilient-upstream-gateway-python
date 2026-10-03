"""限流器：令牌桶算法，线程安全，时钟可注入。"""

from __future__ import annotations

import threading
import time
from typing import Callable


class TokenBucketRateLimiter:
    """令牌桶：以 rate 个/秒补充令牌，最多囤积 capacity 个。

    allow() 有令牌则取走一个并放行，否则拒绝（调用方映射为限流错误）。
    """

    def __init__(
        self,
        *,
        rate: float,
        capacity: float,
        time_func: Callable[[], float] | None = None,
    ) -> None:
        if rate <= 0 or capacity <= 0:
            raise ValueError("rate 与 capacity 必须为正数")
        self._rate = rate
        self._capacity = capacity
        self._tokens = capacity
        self._time = time_func or time.monotonic
        self._last_refill = self._time()
        self._lock = threading.Lock()

    def allow(self) -> bool:
        with self._lock:
            now = self._time()
            elapsed = now - self._last_refill
            if elapsed > 0:
                self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
                self._last_refill = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    @property
    def available_tokens(self) -> float:
        with self._lock:
            return self._tokens
