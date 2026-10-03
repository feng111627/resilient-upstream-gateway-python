"""重试策略：指数退避 + 抖动，只重试被标记为可重试的错误。

退避公式：delay = min(max_delay, base_delay * 2^(attempt-1)) * (1 ± jitter)
配合熔断器使用，避免把故障中的上游打死。
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Callable


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3            # 总尝试次数（含首次）
    base_delay: float = 0.1          # 首次重试的基础等待秒数
    max_delay: float = 2.0           # 单次等待上限
    jitter: float = 0.1              # 抖动比例（±10%）
    random_func: Callable[[], float] = field(default=random.random, compare=False)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts 必须 >= 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("延迟不能为负")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter 必须在 [0, 1]")

    def delay_for_attempt(self, attempt: int) -> float:
        """第 attempt 次尝试失败后、下一次尝试前的等待秒数（attempt 从 1 开始）。"""
        if attempt < 1:
            raise ValueError("attempt 从 1 开始")
        delay = min(self.max_delay, self.base_delay * (2 ** (attempt - 1)))
        if self.jitter:
            spread = delay * self.jitter
            delay += (self.random_func() * 2 - 1) * spread
        return max(0.0, delay)
