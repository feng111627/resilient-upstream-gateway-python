"""重试策略：指数退避 + 抖动，只重试可重试的错误。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay: float = 0.1
    max_delay: float = 2.0
    jitter: float = 0.1

    def delay_for_attempt(self, attempt: int) -> float:
        raise NotImplementedError
