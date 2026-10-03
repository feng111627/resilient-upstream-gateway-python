"""熔断器：三态切换规则与并发安全。"""

import threading

from gateway.circuit_breaker import CircuitBreaker, CircuitState


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_breaker(**overrides) -> tuple[CircuitBreaker, FakeClock]:
    clock = FakeClock()
    params = {"failure_threshold": 3, "success_threshold": 2, "recovery_timeout": 10.0}
    params.update(overrides)
    return CircuitBreaker(time_func=clock, **params), clock


class TestStateTransitions:
    def test_closed_until_threshold_reached(self):
        cb, _ = make_breaker()
        cb.record_failure()
        cb.record_failure()
        assert cb.state is CircuitState.CLOSED
        cb.record_failure()
        assert cb.state is CircuitState.OPEN

    def test_success_resets_failure_count(self):
        cb, _ = make_breaker()
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        cb.record_failure()
        cb.record_failure()
        assert cb.state is CircuitState.CLOSED

    def test_open_rejects_requests(self):
        cb, _ = make_breaker()
        for _ in range(3):
            cb.record_failure()
        assert cb.allow_request() is False

    def test_open_transitions_to_half_open_after_recovery_timeout(self):
        cb, clock = make_breaker()
        for _ in range(3):
            cb.record_failure()
        clock.advance(9.9)
        assert cb.state is CircuitState.OPEN
        clock.advance(0.2)
        assert cb.state is CircuitState.HALF_OPEN

    def test_half_open_allows_single_probe_only(self):
        cb, clock = make_breaker()
        for _ in range(3):
            cb.record_failure()
        clock.advance(10)
        assert cb.allow_request() is True   # 探针
        assert cb.allow_request() is False  # 同时只允许一个探针

    def test_half_open_closes_after_enough_successes(self):
        cb, clock = make_breaker()
        for _ in range(3):
            cb.record_failure()
        clock.advance(10)
        assert cb.allow_request()
        cb.record_success()
        assert cb.state is CircuitState.HALF_OPEN
        assert cb.allow_request()
        cb.record_success()
        assert cb.state is CircuitState.CLOSED

    def test_half_open_failure_reopens_immediately(self):
        cb, clock = make_breaker()
        for _ in range(3):
            cb.record_failure()
        clock.advance(10)
        assert cb.allow_request()
        cb.record_failure()
        assert cb.state is CircuitState.OPEN
        # 重新计时：再过 recovery_timeout 才能半开
        clock.advance(9.9)
        assert cb.state is CircuitState.OPEN
        clock.advance(0.2)
        assert cb.state is CircuitState.HALF_OPEN


class TestConcurrency:
    def test_concurrent_failures_never_exceed_one_open_transition(self):
        cb, _ = make_breaker(failure_threshold=10)
        threads = [threading.Thread(target=cb.record_failure) for _ in range(200)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert cb.state is CircuitState.OPEN

    def test_half_open_admits_exactly_one_probe_under_contention(self):
        cb, clock = make_breaker(failure_threshold=1)
        cb.record_failure()
        clock.advance(10)
        admitted = []
        threads = [
            threading.Thread(target=lambda: cb.allow_request() and admitted.append(1))
            for _ in range(50)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(admitted) == 1
