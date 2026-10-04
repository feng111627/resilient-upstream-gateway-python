"""并发隔离舱：限制每个上游同时在飞的请求数，超出按配置等待或快速失败。

每个上游持有独立的 Bulkhead 实例：A 上游占满自己的名额，既不会占用 B 的名额，
也不会让 B 的请求排队。等待队列长度有上限（max_waiters），避免请求无限堆积
拖垮内存；名额无论成功、失败还是调用中途被放弃，都必须在 finally 中归还。
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Callable, Iterator

from .errors import CapacityFullError


class Bulkhead:
    def __init__(
        self,
        *,
        max_concurrent: int,
        max_waiters: int = 0,
        acquire_timeout: float | None = None,
        time_func: Callable[[], float] | None = None,
    ) -> None:
        if max_concurrent < 1:
            raise ValueError("max_concurrent 必须 >= 1")
        if max_waiters < -1:
            raise ValueError("max_waiters 必须 >= -1（-1 表示不限制排队数）")
        if acquire_timeout is not None and acquire_timeout < 0:
            raise ValueError("acquire_timeout 不能为负")
        self._max = max_concurrent
        self._max_waiters = max_waiters  # 0 = 满了立刻拒绝；-1 = 排队不限
        self._default_timeout = acquire_timeout
        self._time = time_func or time.monotonic

        self._cond = threading.Condition()
        self._active = 0
        self._waiting = 0

    def acquire(self, timeout: float | None = -1.0) -> bool:  # type: ignore[assignment]
        """获取一个并发名额。

        - 有名额：立即占用并返回 True；
        - 名额满且等待队列未满：阻塞等待，超时返回 False；
        - 名额满且等待队列也满：立即返回 False（快速失败，不堆积）。

        timeout 缺省（哨兵 -1.0）使用构造时的 acquire_timeout；
        传 None 表示一直等；传 0 表示只做一次非阻塞尝试。
        """
        if timeout == -1.0:
            timeout = self._default_timeout

        with self._cond:
            if self._active < self._max:
                self._active += 1
                return True

            # 名额已满：先看等待队列还收不收
            if self._max_waiters >= 0 and self._waiting >= self._max_waiters:
                return False

            self._waiting += 1
            try:
                if timeout is None:
                    while self._active >= self._max:
                        self._cond.wait()
                elif timeout <= 0:
                    return False
                else:
                    deadline = self._time() + timeout
                    while self._active >= self._max:
                        remaining = deadline - self._time()
                        if remaining <= 0:
                            return False
                        # 用条件变量真实等待；被唤醒后重新检查谓词，防虚假唤醒
                        self._cond.wait(timeout=max(0.0, remaining))
                self._active += 1
                return True
            finally:
                self._waiting -= 1

    def release(self) -> None:
        """归还一个名额并唤醒一个等待者。多还不会把计数减成负数。"""
        with self._cond:
            if self._active > 0:
                self._active -= 1
            self._cond.notify_all()

    @contextmanager
    def slot(self, timeout: float | None = -1.0) -> Iterator[None]:
        """上下文管理器：拿不到名额抛 CapacityFullError；退出时必定归还。"""
        if not self.acquire(timeout):
            raise CapacityFullError(
                f"上游繁忙：并发名额已用尽（上限 {self._max}），请稍后重试"
            )
        try:
            yield
        finally:
            self.release()

    @property
    def active(self) -> int:
        """当前在飞请求数。"""
        with self._cond:
            return self._active

    @property
    def waiting(self) -> int:
        """当前排队等待名额的请求数。"""
        with self._cond:
            return self._waiting

    @property
    def capacity(self) -> int:
        return self._max
