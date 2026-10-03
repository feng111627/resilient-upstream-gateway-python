"""Shared fixtures: a scripted in-process mock upstream.

The mock upstream never touches the network.  It is driven by a queue of
*behaviours* (or a single default behaviour) and records every request so
tests can assert exactly how many times the gateway called upstream.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field

import httpx
import pytest


@dataclass
class MockUpstream:
    """Scriptable upstream behaviour for httpx.MockTransport."""

    default: str = "ok_v1"
    script: list[str] = field(default_factory=list)
    requests: list[httpx.Request] = field(default_factory=list)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        behaviour = self.script.pop(0) if self.script else self.default
        return self._respond(behaviour, request)

    def _respond(self, behaviour: str, request: httpx.Request) -> httpx.Response:
        if behaviour == "timeout":
            raise httpx.ReadTimeout("simulated read timeout", request=request)
        if behaviour == "conn_error":
            raise httpx.ConnectError("simulated connection refused", request=request)
        if behaviour == "bad_json":
            return httpx.Response(200, content=b"<html>not json</html>")
        if behaviour == "missing_field_v1":
            return httpx.Response(200, json={"status": "ok"})  # no transaction_id
        if behaviour == "missing_field_v2":
            return httpx.Response(200, json={"result": {"status": "ok"}})  # no txId
        if behaviour.startswith("status_"):
            code = int(behaviour.removeprefix("status_"))
            return httpx.Response(code, json={"upstream_error": behaviour})
        if behaviour == "ok_v1":
            return httpx.Response(200, json={"status": "ok", "transaction_id": "tx-v1"})
        if behaviour == "ok_v2":
            return httpx.Response(200, json={"result": {"status": "ok", "txId": "tx-v2"}})
        raise AssertionError(f"unknown behaviour: {behaviour}")

    @property
    def call_count(self) -> int:
        return len(self.requests)

    def last_payload(self) -> dict:
        return json.loads(self.requests[-1].content)


@pytest.fixture
def mock_upstream() -> MockUpstream:
    return MockUpstream()


@pytest.fixture
def transport_factory(mock_upstream: MockUpstream) -> Callable[[], httpx.MockTransport]:
    return lambda: httpx.MockTransport(mock_upstream.handler)


class FakeClock:
    """Manually advanced clock for breaker/limiter tests."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def fake_clock() -> FakeClock:
    return FakeClock()


class FakeSleep:
    """Records sleep durations instead of actually sleeping."""

    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


@pytest.fixture
def fake_sleep() -> FakeSleep:
    return FakeSleep()
