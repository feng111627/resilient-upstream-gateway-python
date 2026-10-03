"""熔断器：关闭/打开/半开三态，线程安全，恢复时机可控。

状态切换规则：
- CLOSED：正常放行；连续失败次数达到 failure_threshold 后切到 OPEN。
- OPEN：快速失败（拒绝请求）；自打开起经过 recovery_timeout 后，
  下一个请求到来时切到 HALF_OPEN 并放行一个探针。
- HALF_OPEN：同一时刻只允许一个探针请求（其余拒绝）；
  探针连续成功 success_threshold 次后切回 CLOSED；任何一次失败立即回到 OPEN。

并发安全：所有状态读写都在同一把锁内完成，状态迁移是原子的。
恢复时机可控：time_func 可注入，测试中用假时钟精确控制 recovery_timeout。
"""

from __future__ import annotations

import enum
import threading
import time
from typing import Callable


class CircuitState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        recovery_timeout: float = 30.0,
        time_func: Callable[[], float] | None = None,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold 必须 >= 1")
        if success_threshold < 1:
            raise ValueError("success_threshold 必须 >= 1")
        self._failure_threshold = failure_threshold
        self._success_threshold = success_threshold
        self._recovery_timeout = recovery_timeout
        self._time = time_func or time.monotonic

        self._lock = threading.Lock()
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._half_open_successes = 0
        self._opened_at: float | None = None
        self._half_open_probe_in_flight = False

    @property
    def state(self) -> CircuitState:
        with self._lock:
            self._maybe_transition_to_half_open()
            return self._state

    def allow_request(self) -> bool:
        """判断当前请求是否放行。HALF_OPEN 下只放行一个探针。"""
        with self._lock:
            self._maybe_transition_to_half_open()
            if self._state is CircuitState.CLOSED:
                return True
            if self._state is CircuitState.OPEN:
                return False
            # HALF_OPEN：探针互斥
            if self._half_open_probe_in_flight:
                return False
            self._half_open_probe_in_flight = True
            return True

    def record_success(self) -> None:
        with self._lock:
            if self._state is CircuitState.HALF_OPEN:
                self._half_open_probe_in_flight = False
                self._half_open_successes += 1
                if self._half_open_successes >= self._success_threshold:
                    self._transition_to_closed()
            else:
                self._consecutive_failures = 0

    def record_failure(self) -> None:
        with self._lock:
            if self._state is CircuitState.HALF_OPEN:
                self._half_open_probe_in_flight = False
                self._transition_to_open()
                return
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._failure_threshold:
                self._transition_to_open()

    # ---- 内部状态迁移（调用时必须已持有锁） ----

    def _maybe_transition_to_half_open(self) -> None:
        if (
            self._state is CircuitState.OPEN
            and self._opened_at is not None
            and self._time() - self._opened_at >= self._recovery_timeout
        ):
            self._state = CircuitState.HALF_OPEN
            self._half_open_successes = 0
            self._half_open_probe_in_flight = False

    def _transition_to_open(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = self._time()
        self._consecutive_failures = 0
        self._half_open_successes = 0
        self._half_open_probe_in_flight = False

    def _transition_to_closed(self) -> None:
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._half_open_successes = 0
        self._opened_at = None
        self._half_open_probe_in_flight = False
