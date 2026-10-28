"""Circuit breaker Lua on real Valkey: closed -> open -> half-open -> closed / open again."""

from __future__ import annotations

import asyncio

import pytest
import redis.asyncio

from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.reliability import BreakerSettings, BreakerState, CircuitBreaker

pytestmark = pytest.mark.integration

M, T = "m", "primary"


def make(valkey: redis.asyncio.Redis, keys: KeySpace) -> CircuitBreaker:
    return CircuitBreaker(
        valkey, keys, BreakerSettings(threshold=3, window_secs=0.5, cooldown_secs=0.3)
    )


async def fail(cb: CircuitBreaker, n: int) -> BreakerState:
    state = BreakerState.CLOSED
    for _ in range(n):
        state = await cb.record(M, T, ok=False)
    return state


async def test_opens_after_threshold(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    cb = make(valkey, keys)
    assert await cb.state(M, T) is BreakerState.CLOSED
    assert await fail(cb, 2) is BreakerState.CLOSED
    assert await cb.allow(M, T)
    assert await fail(cb, 1) is BreakerState.OPEN
    assert not await cb.allow(M, T)
    assert await cb.record(M, T, ok=True) is BreakerState.OPEN, "late success keeps it open"


async def test_window_resets_count(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    cb = make(valkey, keys)
    await fail(cb, 2)
    await asyncio.sleep(0.6)
    assert await fail(cb, 2) is BreakerState.CLOSED


async def test_half_open_probe_then_close(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    cb = make(valkey, keys)
    await fail(cb, 3)
    await asyncio.sleep(0.35)
    assert await cb.state(M, T) is BreakerState.HALF_OPEN, "peek reports half-open"
    assert await cb.allow(M, T), "one probe goes through"
    assert not await cb.allow(M, T), "only one probe per cooldown"
    assert await cb.record(M, T, ok=True) is BreakerState.CLOSED
    assert await cb.allow(M, T)


async def test_half_open_failure_reopens(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    cb = make(valkey, keys)
    await fail(cb, 3)
    await asyncio.sleep(0.35)
    assert await cb.allow(M, T)
    assert await cb.record(M, T, ok=False) is BreakerState.OPEN
    assert not await cb.allow(M, T)


async def test_targets_are_independent(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    cb = make(valkey, keys)
    await fail(cb, 3)
    assert await cb.allow(M, "apac")
