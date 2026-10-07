"""并发名额（舱壁）：超限给明确的“容量满”结论；名额在任何结局下都正确归还。"""

import threading
import time

import pytest

from gateway.client import ResilientUpstreamClient
from gateway.concurrency import ConcurrencyLimiter
from gateway.errors import CapacityFullError, ErrorKind, UpstreamServerError
from gateway.retry import RetryPolicy
from tests.mock_upstream import MockUpstream

PAYLOAD_V2 = {"amount": 100, "currency": "CNY"}


def make_client(upstream: MockUpstream, limiter: ConcurrencyLimiter) -> ResilientUpstreamClient:
    return ResilientUpstreamClient(
        base_url="http://mock",
        transport=upstream.transport,
        retry_policy=RetryPolicy(max_attempts=1),
        concurrency_limiter=limiter,
        sleep_func=lambda _: None,
    )


class TestRejectMode:
    def test_overflow_rejects_immediately_with_distinct_kind(self):
        """名额满立刻报 capacity_full，与超时/熔断是可区分的类别。"""
        up = MockUpstream("slow", slow_delay=0.3)
        limiter = ConcurrencyLimiter(max_in_flight=1, overflow="reject")
        client = make_client(up, limiter)

        holder = threading.Thread(target=lambda: client.call("/pay", payload=PAYLOAD_V2))
        holder.start()
        time.sleep(0.05)  # 等 holder 占住名额
        with pytest.raises(CapacityFullError) as exc_info:
            client.call("/pay", payload=PAYLOAD_V2)
        assert exc_info.value.kind is ErrorKind.CAPACITY_FULL
        assert exc_info.value.kind is not ErrorKind.TIMEOUT
        assert exc_info.value.kind is not ErrorKind.CIRCUIT_OPEN
        holder.join()

    def test_slot_released_after_success(self):
        up = MockUpstream()
        limiter = ConcurrencyLimiter(max_in_flight=1, overflow="reject")
        client = make_client(up, limiter)
        client.call("/pay", payload=PAYLOAD_V2)
        assert limiter.in_flight == 0  # 名额已归还，后续调用不受阻
        client.call("/pay", payload=PAYLOAD_V2)
        assert limiter.in_flight == 0

    def test_slot_released_after_failure(self):
        up = MockUpstream("server_error")
        limiter = ConcurrencyLimiter(max_in_flight=1, overflow="reject")
        client = make_client(up, limiter)
        with pytest.raises(UpstreamServerError):
            client.call("/pay", payload=PAYLOAD_V2)
        assert limiter.in_flight == 0  # 失败也归还名额

    def test_slot_released_when_call_abandoned_midway(self):
        """调用中途被其他环节（如限流）放弃，名额同样归还。"""
        from gateway.rate_limiter import TokenBucketRateLimiter

        up = MockUpstream()
        clock_val = [0.0]
        limiter = ConcurrencyLimiter(max_in_flight=2, overflow="reject")
        client = ResilientUpstreamClient(
            base_url="http://mock",
            transport=up.transport,
            retry_policy=RetryPolicy(max_attempts=1),
            concurrency_limiter=limiter,
            rate_limiter=TokenBucketRateLimiter(
                rate=0.001, capacity=1.0, time_func=lambda: clock_val[0]
            ),
            sleep_func=lambda _: None,
        )
        client.call("/pay", payload=PAYLOAD_V2)  # 用掉唯一令牌
        from gateway.errors import RateLimitedError

        with pytest.raises(RateLimitedError):
            client.call("/pay", payload=PAYLOAD_V2)  # 占用名额后被限流放弃
        assert limiter.in_flight == 0

    @pytest.mark.parametrize("scenario,error_name", [
        ("invalid_json", "InvalidResponseError"),
        ("html_5xx", "InvalidResponseError"),
        ("missing_field", "SchemaMismatchError"),
        ("bad_request", "UpstreamClientError"),
        ("timeout", "UpstreamTimeoutError"),
    ])
    def test_slot_released_after_each_failure_outcome(self, scenario, error_name):
        """成功之外的各种结局——非法 JSON、结构缺失、4xx、超时——名额都必须归还。"""
        import gateway.errors as errors

        up = MockUpstream(scenario)
        limiter = ConcurrencyLimiter(max_in_flight=1, overflow="reject")
        client = make_client(up, limiter)
        error_cls = getattr(errors, error_name)
        with pytest.raises(error_cls):
            client.call("/pay", payload=PAYLOAD_V2)
        assert limiter.in_flight == 0  # 失败后名额归还
        # 名额立即可被下一次请求复用（无泄漏、无多还）
        ok_up = MockUpstream()
        follow_up = ResilientUpstreamClient(
            base_url="http://mock", transport=ok_up.transport,
            retry_policy=RetryPolicy(max_attempts=1),
            concurrency_limiter=limiter,  # 复用同一个 limiter 验证名额可用
            sleep_func=lambda _: None,
        )
        assert follow_up.call("/pay", payload=PAYLOAD_V2)["status"] == "ok"
        assert limiter.in_flight == 0

    def test_slot_released_for_waiter_that_gives_up(self):
        """wait 模式下等待者超时放弃，持名额的请求结束后计数仍归零、不多还。"""
        up = MockUpstream("slow", slow_delay=0.3)
        limiter = ConcurrencyLimiter(max_in_flight=1, overflow="wait", max_wait=0.05)
        client = make_client(up, limiter)
        holder = threading.Thread(target=lambda: client.call("/pay", payload=PAYLOAD_V2))
        holder.start()
        time.sleep(0.05)
        with pytest.raises(CapacityFullError):
            client.call("/pay", payload=PAYLOAD_V2)  # 等待超时放弃，没拿到名额
        assert limiter.in_flight == 1  # 持有者还在飞
        holder.join()
        assert limiter.in_flight == 0  # 持有者结束后精确归零

    def test_no_unbounded_pileup(self):
        """30 个并发打 2 个名额：放行的有限，其余立刻拿到明确结论，不堆积。"""
        up = MockUpstream("slow", slow_delay=0.1)
        limiter = ConcurrencyLimiter(max_in_flight=2, overflow="reject")
        client = make_client(up, limiter)
        results, rejected = [], []

        def worker():
            try:
                results.append(client.call("/pay", payload=PAYLOAD_V2))
            except CapacityFullError:
                rejected.append(1)

        threads = [threading.Thread(target=worker) for _ in range(30)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(results) + len(rejected) == 30
        assert len(rejected) > 0
        assert limiter.in_flight == 0  # 全部归还，无泄漏


class TestWaitMode:
    def test_waiter_blocks_until_slot_freed_then_succeeds(self):
        up = MockUpstream("slow", slow_delay=0.15)
        limiter = ConcurrencyLimiter(max_in_flight=1, overflow="wait", max_wait=2.0)
        client = make_client(up, limiter)
        outcomes = []

        def worker(tag):
            try:
                outcomes.append((tag, client.call("/pay", payload=PAYLOAD_V2)["status"]))
            except CapacityFullError:
                outcomes.append((tag, "capacity_full"))

        t1 = threading.Thread(target=worker, args=("first",))
        t1.start()
        time.sleep(0.05)
        t2 = threading.Thread(target=worker, args=("second",))
        t2.start()
        t1.join()
        t2.join()
        assert sorted(outcomes) == [("first", "ok"), ("second", "ok")]  # 等到了名额

    def test_waiter_times_out_with_capacity_full(self):
        up = MockUpstream("slow", slow_delay=0.5)
        limiter = ConcurrencyLimiter(max_in_flight=1, overflow="wait", max_wait=0.1)
        client = make_client(up, limiter)
        holder = threading.Thread(target=lambda: client.call("/pay", payload=PAYLOAD_V2))
        holder.start()
        time.sleep(0.05)
        started = time.monotonic()
        with pytest.raises(CapacityFullError):
            client.call("/pay", payload=PAYLOAD_V2)
        assert time.monotonic() - started < 0.5  # 按 max_wait 放弃，没跟着慢上游卡死
        holder.join()
        assert limiter.in_flight == 0
