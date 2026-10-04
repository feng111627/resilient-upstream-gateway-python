"""并发名额限制（舱壁）：限制单个上游同时在飞的请求数量。

语义：
- 每个上游各自持有独立的限制器，慢上游只会占满自己的名额，
  不会挤占其他上游的并发额度。
- 两种溢出策略（overflow）：
  - "reject"：名额满立刻失败，调用方映射为 CapacityFullError（容量满/太忙），
    与超时、熔断是不同类别；
  - "wait"：在 max_wait 秒内等待名额释放，超时仍未拿到名额才失败。
- 名额通过 acquire() 获取、release() 归还；无论调用成功、失败还是被放弃，
  调用方都必须在 finally 中归还（提供 slot() 上下文管理器保证这一点）。
- 线程安全：状态变更在锁内完成，等待用 Condition，避免忙轮询。
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Callable, Iterator

from .errors import CapacityFullError


class ConcurrencyLimiter:
    def __init__(
        self,
        *,
        max_in_flight: int,
        overflow: str = "reject",
        max_wait: float = 0.0,
        time_func: Callable[[], float] | None = None,
    ) -> None:
        if max_in_flight < 1:
            raise ValueError("max_in_flight 必须 >= 1")
        if overflow not in ("reject", "wait"):
            raise ValueError("overflow 只能是 'reject' 或 'wait'")
        if overflow == "wait" and max_wait <= 0:
            raise ValueError("overflow='wait' 时 max_wait 必须为正数")
        self._max = max_in_flight
        self._overflow = overflow
        self._max_wait = max_wait
        self._time = time_func or time.monotonic
        self._cond = threading.Condition()
        self._in_flight = 0

    @property
    def in_flight(self) -> int:
        with self._cond:
            return self._in_flight

    @property
    def max_in_flight(self) -> int:
        return self._max

    def acquire(self) -> None:
        """获取一个并发名额；拿不到按溢出策略处理（等待或直接失败）。"""
        deadline = self._time() + self._max_wait
        with self._cond:
            if self._in_flight < self._max:
                self._in_flight += 1
                return
            if self._overflow == "reject":
                raise CapacityFullError(
                    f"该上游并发名额已满（{self._max} 个在飞），请稍后重试"
                )
            # wait 模式：等名额释放，最多等 max_wait 秒
            while True:
                remaining = deadline - self._time()
                if remaining <= 0:
                    raise CapacityFullError(
                        f"等待 {self._max_wait}s 后该上游并发名额仍满"
                        f"（{self._max} 个在飞），请稍后重试"
                    )
                self._cond.wait(timeout=remaining)
                if self._in_flight < self._max:
                    self._in_flight += 1
                    return

    def release(self) -> None:
        """归还一个并发名额。无论成功/失败/放弃都必须调用。"""
        with self._cond:
            if self._in_flight <= 0:
                raise RuntimeError("并发名额归还次数多于获取次数")
            self._in_flight -= 1
            self._cond.notify()

    @contextmanager
    def slot(self) -> Iterator[None]:
        """上下文管理器：保证任何退出路径（含异常）都归还名额。"""
        self.acquire()
        try:
            yield
        finally:
            self.release()
