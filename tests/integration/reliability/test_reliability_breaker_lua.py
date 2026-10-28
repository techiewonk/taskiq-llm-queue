"""Circuit breaker Lua on real Valkey: closed -> open -> half-open -> closed / open again."""

from __future__ import annotations

import asyncio

import pytest
import redis.asyncio

from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.reliability import BreakerSettings, BreakerState, CircuitBreaker

pytestmark = pytest.mark.integration

M, T = "m", "primary"


def make(valkey: redis.asyncio.Redis, keys: KeySpace, probes: int = 1) -> CircuitBreaker:
    settings = BreakerSettings(
        threshold=3, window_secs=0.5, cooldown_secs=0.3, half_open_probes=probes
    )
    return CircuitBreaker(valkey, keys, settings)


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


async def test_half_open_allows_configured_probes(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """Breaker review: half-open let only one probe per cooldown cluster-wide; now configurable."""
    cb = make(valkey, keys, probes=3)
    await fail(cb, 3)
    await asyncio.sleep(0.35)
    assert [await cb.allow(M, T) for _ in range(4)] == [True, True, True, False]
    assert await cb.record(M, T, ok=True) is BreakerState.CLOSED


async def test_neutral_outcome_frees_probe_without_counting(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """Breaker review: an uncounted outcome (e.g. a throttle) held the probe slot until cooldown."""
    cb = make(valkey, keys)
    assert await cb.record_neutral(M, T) is BreakerState.CLOSED
    await fail(cb, 3)
    assert await cb.record_neutral(M, T) is BreakerState.OPEN
    await asyncio.sleep(0.35)
    assert await cb.allow(M, T)
    assert not await cb.allow(M, T)
    assert await cb.record_neutral(M, T) is BreakerState.HALF_OPEN
    assert await cb.allow(M, T), "the probe slot came back"


async def test_lost_probe_frees_after_cooldown(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    cb = make(valkey, keys, probes=2)
    await fail(cb, 3)
    await asyncio.sleep(0.35)
    assert await cb.allow(M, T)
    assert await cb.allow(M, T)
    assert not await cb.allow(M, T)
    await asyncio.sleep(0.35)
    assert await cb.allow(M, T), "probes that never reported free after cooldown"
