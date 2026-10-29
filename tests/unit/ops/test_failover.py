"""Failover state: SQS window enter/expire, Valkey-down mode with probe and backoff, local cap."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Literal, Protocol

import pytest

from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.errors import ConfigError
from taskiq_llm_queue.failover import FailoverSettings, FailoverState
from taskiq_llm_queue.metrics_names import FAILOVER_ACTIVE


class FailoverPolicy(Protocol):
    """Local copy of the client's failover hook (routing_hooks.py), checked structurally."""

    def path_override(self, model: str) -> Literal["direct"] | None: ...
    def report_sqs_failure(self, model: str, exc: BaseException) -> None: ...
    def report_redis_failure(self, exc: BaseException) -> None: ...
    def redis_down(self) -> bool: ...


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class Gauges:
    def __init__(self) -> None:
        self.values: list[tuple[float, dict[str, str]]] = []

    def incr(self, name: str, value: float = 1.0, labels: Mapping[str, str] | None = None) -> None:
        raise AssertionError("unexpected incr")

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        raise AssertionError("unexpected observe")

    def gauge(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        assert name == FAILOVER_ACTIVE
        self.values.append((value, dict(labels or {})))


def test_sqs_window_enters_and_expires_with_injected_clock() -> None:
    clock, gauges = Clock(), Gauges()
    state: FailoverPolicy = FailoverState(metrics=gauges, clock=clock)
    assert state.path_override("gpt20b") is None
    state.report_sqs_failure("gpt20b@apac", TimeoutError())
    assert state.path_override("gpt20b") == "direct"
    assert state.path_override("embed") is None  # per model
    clock.now += 299.9
    assert state.path_override("gpt20b@primary") == "direct"
    clock.now += 0.2
    assert state.path_override("gpt20b") is None
    assert gauges.values == [
        (1.0, {"model": "gpt20b", "kind": "sqs"}),
        (0.0, {"model": "gpt20b", "kind": "sqs"}),
    ]


def test_repeat_failure_extends_window_without_new_alarm() -> None:
    clock, gauges = Clock(), Gauges()
    state = FailoverState(FailoverSettings(sqs_failover_secs=10), metrics=gauges, clock=clock)
    state.report_sqs_failure("m", OSError())
    clock.now += 8
    state.report_sqs_failure("m", OSError())
    clock.now += 8
    assert state.sqs_failover_active("m")
    assert len(gauges.values) == 1


def test_redis_down_without_probe_uses_backoff_window() -> None:
    clock, gauges = Clock(), Gauges()
    settings = FailoverSettings(redis_retry_min_secs=1, redis_retry_max_secs=4)
    state = FailoverState(settings, metrics=gauges, clock=clock)
    assert not state.redis_down()
    state.report_redis_failure(ConnectionError())
    assert state.redis_down()
    assert state.path_override("any") == "direct"
    clock.now += 1.01
    assert not state.redis_down()  # window over: the caller may try Valkey again
    state.report_redis_failure(ConnectionError())  # still failing -> backoff doubles
    clock.now += 1.5
    assert state.redis_down()
    for _ in range(5):
        state.report_redis_failure(ConnectionError())
    clock.now += 4.01
    assert not state.redis_down()  # capped at redis_retry_max_secs
    state.report_redis_ok()
    assert gauges.values == [(1.0, {"kind": "redis"}), (0.0, {"kind": "redis"})]


async def test_redis_down_with_probe_stays_down_until_probe_succeeds() -> None:
    healthy = asyncio.Event()
    probes = 0

    async def probe() -> None:
        nonlocal probes
        probes += 1
        if not healthy.is_set():
            raise ConnectionError("still down")

    settings = FailoverSettings(redis_retry_min_secs=0.01, redis_retry_max_secs=0.02)
    state = FailoverState(settings, probe=probe)
    state.report_redis_failure(ConnectionError())
    await asyncio.sleep(0.1)
    assert state.redis_down()  # probe running, failing
    assert probes >= 2
    healthy.set()
    for _ in range(100):
        if not state.redis_down():
            break
        await asyncio.sleep(0.01)
    assert not state.redis_down()
    assert state.path_override("m") is None
    await state.aclose()


async def test_probe_redis_once_and_aclose_idempotent() -> None:
    async def ok() -> bool:
        return True

    state = FailoverState(probe=ok, clock=Clock())
    assert await FailoverState().probe_redis() is False  # no probe configured
    state.report_redis_failure(ConnectionError())
    assert await state.probe_redis() is True
    assert not state.redis_down()
    await state.aclose()
    await state.aclose()


def test_local_cap_and_limiter() -> None:
    state = FailoverState(FailoverSettings(local_cap_fallback=7, live_pods=4))
    assert state.local_cap("m", "t") == 7  # nothing learned yet
    state.note_limit("m", "t", 100)
    state.note_limit("m", "t", None)  # ignored
    assert state.local_cap("m", "t") == 25
    limiter = state.local_limiter("m", "t")
    assert limiter.cap == 25
    assert state.local_limiter("m", "t") is limiter
    assert limiter.try_acquire("m", "t", Lane.RT) is not None
    state.report_redis_failure(OSError())
    state.report_redis_ok()
    assert state.local_limiter("m", "t") is not limiter  # rebuilt per outage
    unknown_pods = FailoverState(FailoverSettings(local_cap_fallback=3))
    unknown_pods.note_limit("m", "t", 1000)
    assert unknown_pods.local_cap("m", "t") == 3


def test_settings_from_env_and_validation() -> None:
    s = FailoverSettings.from_env({"LLMQ_SQS_FAILOVER_SECS": "60", "LLMQ_LOCAL_LIMIT_CAP": "5"})
    assert (s.sqs_failover_secs, s.local_cap_fallback) == (60.0, 5)
    assert FailoverSettings.from_env({}).sqs_failover_secs == 300.0
    with pytest.raises(ConfigError, match=r"LLMQ_SQS_FAILOVER_SECS.*LLMQ_LOCAL_LIMIT_CAP"):
        FailoverSettings.from_env({"LLMQ_SQS_FAILOVER_SECS": "x", "LLMQ_LOCAL_LIMIT_CAP": "y"})
    with pytest.raises(ConfigError):
        FailoverSettings(sqs_failover_secs=0)
