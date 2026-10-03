"""熔断器：关闭/打开/半开三态，线程安全，恢复时机可控。"""

from __future__ import annotations

import enum
import threading


class CircuitState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """占位骨架：接口定义，实现后续填充。"""

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        recovery_timeout: float = 30.0,
        time_func=None,
    ) -> None:
        self._lock = threading.Lock()
        self._state = CircuitState.CLOSED

    @property
    def state(self) -> CircuitState:
        raise NotImplementedError

    def allow_request(self) -> bool:
        raise NotImplementedError

    def record_success(self) -> None:
        raise NotImplementedError

    def record_failure(self) -> None:
        raise NotImplementedError
