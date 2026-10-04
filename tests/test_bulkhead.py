"""并发隔离舱单测：名额获取/释放、排队、队列上限、超时、异常路径不泄漏名额。"""

import threading
import time

import pytest

from gateway.bulkhead import Bulkhead
from gateway.errors import CapacityFullError, ErrorKind


class TestQuota:
    def test_slots_deplete_then_fast_fail(self):
        b = Bulkhead(max_concurrent=2, max_waiters=0)
        assert b.acquire() is True
        assert b.acquire() is True
        assert b.active == 2
        assert b.acquire(timeout=0) is False  # 满额立即拒绝，不堆积
        assert b.waiting == 0

    def test_released_slot_is_reusable(self):
        b = Bulkhead(max_concurrent=1, max_waiters=0)
        assert b.acquire()
        b.release()
        assert b.active == 0
        assert b.acquire(timeout=0) is True

    def test_extra_release_does_not_go_negative(self):
        b = Bulkhead(max_concurrent=1)
        b.acquire()
        b.release()
        b.release()  # 多还一次不能把计数减穿
        assert b.active == 0
        assert b.acquire(timeout=0) is True


class TestWaiting:
    def test_waiter_gets_slot_after_release(self):
        b = Bulkhead(max_concurrent=1, max_waiters=-1)
        b.acquire()
        acquired: list[bool] = []
        done = threading.Event()

        def waiter():
            acquired.append(b.acquire(timeout=2))
            done.set()

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.05)
        assert b.waiting == 1
        b.release()
        assert done.wait(1)
        assert acquired == [True]
        assert b.active == 1
        t.join()

    def test_wait_times_out_and_returns_false_without_leak(self):
        b = Bulkhead(max_concurrent=1, max_waiters=-1)
        b.acquire()
        assert b.acquire(timeout=0.05) is False
        assert b.waiting == 0       # 超时退出后等待计数正确回收
        assert b.active == 1       # 在飞名额没被动过

    def test_waiter_queue_cap_rejects_immediately(self):
        b = Bulkhead(max_concurrent=1, max_waiters=1)
        b.acquire()
        parked = threading.Event()

        def first_waiter():
            parked.set()
            b.acquire(timeout=None)

        t = threading.Thread(target=first_waiter)
        t.start()
        assert parked.wait(1)
        time.sleep(0.05)
        assert b.waiting == 1
        # 队列只有 1 个位置：第三个请求不等，立刻失败，内存里不会无限堆积
        assert b.acquire(timeout=5) is False
        b.release()
        t.join(1)

    def test_waiters_cannot_exceed_max_concurrent_under_contention(self):
        b = Bulkhead(max_concurrent=3, max_waiters=-1)
        for _ in range(3):
            b.acquire()
        in_flight_seen: list[int] = []
        stop = threading.Event()

        def worker():
            if b.acquire(timeout=2):
                in_flight_seen.append(b.active)
                time.sleep(0.02)
                b.release()
            stop.set()

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        time.sleep(0.05)
        for _ in range(3):
            b.release()
        for t in threads:
            t.join(3)
        assert in_flight_seen, "至少应有等待者拿到名额"
        assert max(in_flight_seen) <= 3  # 在飞数绝不超过该上游配额
        assert b.active == 0


class TestSlotContext:
    def test_slot_raises_capacity_full_when_full(self):
        b = Bulkhead(max_concurrent=1, max_waiters=0)
        b.acquire()
        with pytest.raises(CapacityFullError) as exc_info:
            with b.slot(timeout=0):
                pass
        assert exc_info.value.kind is ErrorKind.CAPACITY_FULL
        assert exc_info.value.retryable is True  # 语义上稍后可重试

    def test_slot_released_on_success_and_error(self):
        b = Bulkhead(max_concurrent=1)
        with b.slot():
            assert b.active == 1
        assert b.active == 0
        with pytest.raises(RuntimeError):
            with b.slot():
                raise RuntimeError("调用中途失败")
        assert b.active == 0  # 失败路径名额也必须归还

    def test_slot_released_on_cancel(self):
        # 模拟调用方中途放弃（GeneratorExit 场景）：名额仍要归还
        b = Bulkhead(max_concurrent=1)

        def abandon():
            with b.slot():
                return  # 正常提前退出，with 会关闭

        abandon()
        assert b.active == 0


class TestValidation:
    def test_bad_config_rejected(self):
        with pytest.raises(ValueError):
            Bulkhead(max_concurrent=0)
        with pytest.raises(ValueError):
            Bulkhead(max_concurrent=1, max_waiters=-2)
        with pytest.raises(ValueError):
            Bulkhead(max_concurrent=1, acquire_timeout=-1)
