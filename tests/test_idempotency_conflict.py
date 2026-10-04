"""幂等键冲突：同一幂等键换了请求内容，必须明确报冲突，不能默默复用旧结论。"""

import threading

import pytest

from gateway.client import ResilientUpstreamClient
from gateway.errors import ErrorKind, IdempotencyConflictError
from gateway.idempotency import IdempotencyStore
from gateway.retry import RetryPolicy
from tests.mock_upstream import MockUpstream

V2_PAYLOAD = {"id": "order-1", "amount": 100, "currency": "CNY"}
V2_PAYLOAD_DIFFERENT = {"id": "order-1", "amount": 200, "currency": "CNY"}
V1_PAYLOAD = {"id": "order-1", "amount": 100, "callback_url": "http://cb"}


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def make_client(upstream: MockUpstream, **kw) -> ResilientUpstreamClient:
    params = {
        "base_url": "http://mock",
        "transport": upstream.transport,
        "retry_policy": RetryPolicy(max_attempts=1),
        "sleep_func": lambda _: None,
        "idempotency_store": IdempotencyStore(time_func=FakeClock()),
    }
    params.update(kw)
    return ResilientUpstreamClient(**params)


class TestStoreLevelConflict:
    def test_same_key_different_fingerprint_after_done_conflicts(self):
        store = IdempotencyStore(time_func=FakeClock())
        claim = store.begin("k", fingerprint="fp-a")
        assert claim.is_owner
        store.complete("k", {"ok": True})

        with pytest.raises(IdempotencyConflictError) as exc:
            store.begin("k", fingerprint="fp-b")
        assert exc.value.kind is ErrorKind.IDEMPOTENCY_CONFLICT
        assert exc.value.retryable is False

    def test_same_key_same_fingerprint_replays(self):
        store = IdempotencyStore(time_func=FakeClock())
        first = store.begin("k", fingerprint="fp-a")
        assert first.is_owner
        store.complete("k", {"ok": True})
        again = store.begin("k", fingerprint="fp-a")
        assert not again.is_owner
        assert again.result == {"ok": True}

    def test_conflict_against_in_flight_owner(self):
        store = IdempotencyStore(time_func=FakeClock())
        owner_started = threading.Event()
        block = threading.Event()

        def owner():
            c = store.begin("k", fingerprint="fp-a")
            assert c.is_owner
            owner_started.set()
            block.wait()
            store.complete("k", 1)

        t = threading.Thread(target=owner)
        t.start()
        assert owner_started.wait(1)
        # owner 还在执行：同键不同内容同样立即冲突，而不是排队等结果
        with pytest.raises(IdempotencyConflictError):
            store.begin("k", fingerprint="fp-b")
        block.set()
        t.join(1)

    def test_expired_entry_allows_new_content(self):
        clock = FakeClock()
        store = IdempotencyStore(ttl=10.0, time_func=clock)
        store.begin("k", fingerprint="fp-a")
        store.complete("k", 1)
        clock.now += 11.0  # TTL 过后，键与新内容可重新绑定
        claim = store.begin("k", fingerprint="fp-b")
        assert claim.is_owner


class TestClientLevelConflict:
    def test_same_key_different_payload_conflicts_and_no_second_call(self):
        up = MockUpstream()
        client = make_client(up)
        r1 = client.call("/pay", idempotency_key="dup-1", payload=V2_PAYLOAD)
        assert r1["status"] == "ok"
        with pytest.raises(IdempotencyConflictError) as exc:
            client.call("/pay", idempotency_key="dup-1", payload=V2_PAYLOAD_DIFFERENT)
        assert exc.value.kind is ErrorKind.IDEMPOTENCY_CONFLICT
        assert up.calls == 1  # 冲突请求没有打到上游，旧结论也没被复用
        # 原内容仍可安全重放
        again = client.call("/pay", idempotency_key="dup-1", payload=V2_PAYLOAD)
        assert again == r1
        assert up.calls == 1

    def test_same_key_different_version_conflicts(self):
        up = MockUpstream()
        client = make_client(up)
        client.call("/pay", idempotency_key="dup-2", version="v2", payload=V2_PAYLOAD)
        # 同键、同语义字段，但换了版本 -> 请求会被组装成不同报文，也算换内容
        with pytest.raises(IdempotencyConflictError):
            client.call("/pay", idempotency_key="dup-2", version="v1", payload=V1_PAYLOAD)

    def test_same_key_different_path_conflicts(self):
        up = MockUpstream()
        client = make_client(up)
        client.call("/refund", idempotency_key="dup-3", payload=V2_PAYLOAD)
        with pytest.raises(IdempotencyConflictError):
            client.call("/pay", idempotency_key="dup-3", payload=V2_PAYLOAD)

    def test_conflict_separate_by_idempotency_space(self):
        # 同一个上游 client 内，不同键互不影响
        up = MockUpstream()
        client = make_client(up)
        client.call("/pay", idempotency_key="a", payload=V2_PAYLOAD)
        client.call("/pay", idempotency_key="b", payload=V2_PAYLOAD_DIFFERENT)
        assert up.calls == 2
