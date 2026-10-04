"""幂等键支持：同一幂等键的重复提交返回首个请求的结论，不产生重复副作用。

语义：
- 首个携带某幂等键的请求成为 owner，真正执行上游调用。
- 执行期间到达的重复请求阻塞等待 owner 的结论，然后拿到相同结果/错误。
- owner 完成后结论在 TTL 内被缓存，之后的重复提交直接返回缓存结论。
- 结论（成功或失败）都会被缓存，保证重复提交得到一致的明确结论。
- 调用方可提供请求指纹（fingerprint）：同一幂等键换了请求内容时
  明确抛 IdempotencyConflictError，绝不默默复用旧结论。

线程安全：所有状态变更在锁内完成，等待使用 Event，避免惊群与竞态。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

from .errors import IdempotencyConflictError


@dataclass(frozen=True)
class Claim:
    """begin() 的返回：要么你是 owner 去执行，要么直接拿到已有结论。"""

    is_owner: bool
    result: Any = None
    error: BaseException | None = None


class _Entry:
    __slots__ = ("done", "event", "result", "error", "expires_at", "fingerprint")

    def __init__(self, expires_at: float, fingerprint: str | None) -> None:
        self.done = False
        self.event = threading.Event()
        self.result: Any = None
        self.error: BaseException | None = None
        self.expires_at = expires_at
        self.fingerprint = fingerprint


class IdempotencyStore:
    def __init__(self, *, ttl: float = 300.0, time_func: Callable[[], float] | None = None) -> None:
        if ttl <= 0:
            raise ValueError("ttl 必须为正数")
        self._ttl = ttl
        self._time = time_func or time.monotonic
        self._lock = threading.Lock()
        self._entries: dict[str, _Entry] = {}

    def begin(self, key: str, *, fingerprint: str | None = None) -> Claim:
        """登记幂等键。owner 负责执行并调用 complete/fail；非 owner 等待结论。

        fingerprint 是请求内容的指纹（如规范化 JSON 的哈希）。同一键的后续
        提交若指纹不同，说明换了请求内容，明确抛 IdempotencyConflictError，
        而不是把旧结论复用回去。
        """
        if not key:
            raise ValueError("幂等键不能为空")
        while True:
            with self._lock:
                self._purge_expired()
                entry = self._entries.get(key)
                if entry is None:
                    entry = _Entry(self._time() + self._ttl, fingerprint)
                    self._entries[key] = entry
                    return Claim(is_owner=True)
                self._check_fingerprint(key, entry, fingerprint)
                if entry.done:
                    return Claim(is_owner=False, result=entry.result, error=entry.error)
                event = entry.event
            # 在锁外等待 owner 完成，避免持锁阻塞
            event.wait()

    @staticmethod
    def _check_fingerprint(key: str, entry: _Entry, fingerprint: str | None) -> None:
        if (
            fingerprint is not None
            and entry.fingerprint is not None
            and entry.fingerprint != fingerprint
        ):
            raise IdempotencyConflictError(
                f"幂等键 {key!r} 已绑定不同的请求内容，拒绝复用旧结论"
            )

    def complete(self, key: str, result: Any) -> None:
        self._finish(key, result=result, error=None)

    def fail(self, key: str, exc: BaseException) -> None:
        self._finish(key, result=None, error=exc)

    def _finish(self, key: str, *, result: Any, error: BaseException | None) -> None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None or entry.done:
                return
            entry.done = True
            entry.result = result
            entry.error = error
            entry.expires_at = self._time() + self._ttl  # 从完成时刻重新计 TTL
            entry.event.set()

    def _purge_expired(self) -> None:
        now = self._time()
        expired = [k for k, e in self._entries.items() if e.done and e.expires_at <= now]
        for k in expired:
            del self._entries[k]

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)
