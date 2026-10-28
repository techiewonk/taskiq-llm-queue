"""Adaptive limiter Lua on real Valkey: start values, ramp, AIMD, pause, lane share, leases."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
import redis
import redis.asyncio
from redis.asyncio.client import Pipeline

from taskiq_llm_queue.config import Lane
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.protocols import Lease, Limiter, ReceiveGate
from taskiq_llm_queue.reliability import AdaptiveLimiter, LimiterSettings, SyncAdaptiveLimiter, _lua

pytestmark = pytest.mark.integration

M, T = "m", "primary"


def make(valkey: redis.asyncio.Redis, keys: KeySpace, **kw: object) -> AdaptiveLimiter:
    base: dict[str, object] = {
        "initial": {M: 4},
        "min_limit": 2,
        "max_limit": 50,
        "ramp_window": 3,
        "increase_every": 2,
        "pause_secs": 0.15,
        "poll_secs": 0.01,
    }
    base.update(kw)
    return AdaptiveLimiter(valkey, keys, LimiterSettings(**base))  # type: ignore[arg-type]


async def take(lim: AdaptiveLimiter, n: int, lane: Lane = Lane.RT) -> list[Lease]:
    leases = [await lim.try_acquire(M, T, lane) for _ in range(n)]
    assert all(leases), "expected every acquire to succeed"
    return [lease for lease in leases if lease is not None]


async def succeed(lim: AdaptiveLimiter, n: int, lane: Lane = Lane.RT) -> None:
    for _ in range(n):
        lease = await lim.try_acquire(M, T, lane)
        assert lease is not None
        await lim.release(lease, throttled=False)


async def release_all(lim: AdaptiveLimiter, leases: list[Lease]) -> None:
    for lease in leases:
        await lim.release(lease, throttled=False)


async def throttle(lim: AdaptiveLimiter) -> None:
    lease = await lim.try_acquire(M, T, Lane.RT)
    assert lease is not None
    await lim.release(lease, throttled=True)


async def limit_of(lim: AdaptiveLimiter) -> int | None:
    return (await lim.state(M, T)).limit


async def test_implements_limiter_protocol(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    lim = make(valkey, keys)
    assert isinstance(lim, Limiter)
    assert isinstance(lim.gate(M, T), ReceiveGate)


async def test_starts_at_initial_and_stored_value_wins(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """Baseline start per model; a learned limit already in Valkey beats the configured start."""
    lim = make(valkey, keys)
    assert await limit_of(lim) is None
    await succeed(lim, 1)
    assert await limit_of(lim) == 4
    await valkey.hset(keys.limiter("other", T), mapping={"limit": 9, "ramp": 0})
    lease = await lim.try_acquire("other", T, Lane.RT)
    assert lease is not None
    assert lim.last_limits[("other", T)] == 9


async def test_stored_value_is_clamped_to_min_max(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    lim = make(valkey, keys)
    await valkey.hset(keys.limiter(M, T), mapping={"limit": 100000, "ramp": 0})
    await succeed(lim, 1)
    assert await limit_of(lim) == 50
    await valkey.hset(keys.limiter(M, T), "limit", 0)
    await succeed(lim, 1)
    assert await limit_of(lim) == 2


async def test_ramp_doubles_per_window_until_cap(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """Ramp doubles per window of busy successes (inflight >= 0.9 x limit), up to `ramp_cap`."""
    lim = make(valkey, keys, ramp_cap=20)
    held = await take(lim, 3)
    await succeed(lim, 3)  # each at inflight 4 of 4
    assert await limit_of(lim) == 8
    held += await take(lim, 4)
    await succeed(lim, 3)  # inflight 8 of 8
    assert await limit_of(lim) == 16
    held += await take(lim, 8)
    await succeed(lim, 3)  # inflight 16 of 16, peak cap 32
    assert await limit_of(lim) == 20  # DS12: ramp capped
    held += await take(lim, 4)
    await succeed(lim, 3)
    assert await limit_of(lim) == 20
    await release_all(lim, held)


async def test_ramp_never_exceeds_max(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    lim = make(valkey, keys, initial={M: 30})
    held = await take(lim, 29)
    await succeed(lim, 3)
    assert await limit_of(lim) == 50
    await release_all(lim, held)


async def test_idle_traffic_does_not_ramp(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    """Finding 4 (DS12): sequential calls at concurrency 1 ramped 20 -> 2000; limit must hold."""
    lim = make(valkey, keys, initial={M: 20}, max_limit=2000)
    for _ in range(100):
        lease = await lim.acquire(M, T, Lane.RT, timeout_secs=1)
        assert lease is not None
        await lim.release(lease, throttled=False)
    assert await limit_of(lim) == 20


async def test_growth_capped_at_twice_peak(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    """Finding 4 (DS12): ramp stops at 2 x the measured peak inflight, not at `max_limit`."""
    lim = make(valkey, keys, initial={M: 4}, utilisation=0.5)
    held = await take(lim, 2)
    await succeed(lim, 3)  # busy at inflight 3 >= 2; peak 3
    assert await limit_of(lim) == 6, "doubling to 8 would exceed 2 x peak"
    await release_all(lim, held)


async def test_bg_only_traffic_can_grow(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    """Finding 4: utilisation is measured against the lane's share, so a saturated bg grows."""
    lim = make(valkey, keys, initial={M: 10})
    held = await take(lim, 7, Lane.BG)
    await succeed(lim, 3, Lane.BG)  # inflight 8 of the 8 bg slots
    assert await limit_of(lim) == 16  # min(2 x 10, 2 x peak 8)
    await release_all(lim, held)


async def test_refused_acquire_writes_nothing(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    """Finding 7: a refused acquire did HSET + ZREMRANGEBYSCORE on every 50 ms poll (hot key)."""
    lim = make(valkey, keys, initial={M: 2})
    held = await take(lim, 2)
    await valkey.zadd(keys.leases(M, T), {"expired": 1.0})
    async with valkey.pipeline(transaction=True) as pipe:
        await pipe.watch(keys.limiter(M, T), keys.leases(M, T))
        for _ in range(5):
            assert await lim.try_acquire(M, T, Lane.RT) is None
        pipe.multi()  # type: ignore[no-untyped-call]
        pipe.ping()
        await pipe.execute()  # WatchError if any refused call wrote either key
    assert await valkey.zscore(keys.leases(M, T), "expired") == 1.0
    await release_all(lim, held)
    assert await lim.try_acquire(M, T, Lane.RT) is not None
    assert await valkey.zscore(keys.leases(M, T), "expired") is None, "pruned on grant"


async def test_acquire_sleeps_out_the_pause(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    """Finding 7: a paused acquire polled every 50 ms; it now sleeps the pause remaining."""
    delays: list[float] = []

    async def sleep(secs: float) -> None:
        delays.append(secs)
        await asyncio.sleep(secs)

    lim = AdaptiveLimiter(
        valkey, keys, LimiterSettings(initial={M: 4}, pause_secs=0.3, poll_secs=0.01), sleep=sleep
    )
    await throttle(lim)
    assert await lim.acquire(M, T, Lane.RT, timeout_secs=5) is not None
    assert delays, "acquire must have waited"
    assert delays[0] >= 0.2
    assert len(delays) <= 2


async def test_acquire_backoff_is_bounded(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    """Finding 7: waiting at the limit backs off with jitter instead of a fixed 50 ms poll."""
    delays: list[float] = []

    async def sleep(secs: float) -> None:
        delays.append(secs)
        await asyncio.sleep(secs)

    settings = LimiterSettings(initial={M: 2}, min_limit=2, poll_secs=0.01, max_poll_secs=0.08)
    lim = AdaptiveLimiter(valkey, keys, settings, sleep=sleep)
    held = await take(lim, 2)
    assert await lim.acquire(M, T, Lane.RT, timeout_secs=0.5) is None
    assert len(delays) < 0.5 / 0.01 / 2, "backoff must grow beyond poll_secs"
    assert max(delays) <= 0.08
    await release_all(lim, held)


async def test_acquire_should_abort_stops_waiting(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """Finding 7: a cancelled caller kept waiting for a slot until its timeout."""
    lim = make(valkey, keys, initial={M: 2})
    held = await take(lim, 2)
    calls = 0

    async def cancelled() -> bool:
        nonlocal calls
        calls += 1
        return calls >= 2

    started = time.monotonic()
    assert await lim.acquire(M, T, Lane.RT, timeout_secs=10, should_abort=cancelled) is None
    assert time.monotonic() - started < 1
    assert calls == 2
    await release_all(lim, held)


async def test_renew_does_not_revive_expired_lease(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """DS11: an expired lease's slot may already be reused, so renew must not bring it back."""
    lim = make(valkey, keys, initial={M: 2}, lease_ttl_secs=0.1)
    lease = await lim.try_acquire(M, T, Lane.RT)
    assert lease is not None
    await asyncio.sleep(0.2)
    await lim.renew([lease])
    assert (await lim.state(M, T)).inflight == 0


async def test_throttle_ends_ramp_decreases_once_per_second(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    lim = make(valkey, keys, initial={M: 10})
    await throttle(lim)
    assert await limit_of(lim) == 7
    assert await valkey.hget(keys.limiter(M, T), "ramp") == b"0"
    await asyncio.sleep(0.2)  # let the pause end; still inside the same second
    await throttle(lim)
    assert await limit_of(lim) == 7, "second throttle within 1 s must not shrink again"
    await asyncio.sleep(1.05)
    await throttle(lim)
    assert await limit_of(lim) == 4  # floor(7 * 0.7)
    await asyncio.sleep(0.2)
    await succeed(lim, 6)
    assert await limit_of(lim) == 4, "no ramp after the first throttle"


async def test_decrease_stops_at_min(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    lim = make(valkey, keys, initial={M: 2})
    await throttle(lim)
    assert await limit_of(lim) == 2


async def test_additive_increase_only_near_full(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """DS12: +1 per `increase_every` successes only when inflight >= 0.9 x limit."""
    lim = make(valkey, keys, initial={M: 10})
    await throttle(lim)  # limit 7, ramp off
    await asyncio.sleep(0.2)
    await succeed(lim, 10)  # inflight 1 of 7: idle, no drift
    assert await limit_of(lim) == 7
    held = await take(lim, 6)
    await succeed(lim, 2)  # each runs at inflight 7 >= 6.3
    assert await limit_of(lim) == 8
    await succeed(lim, 4)  # inflight 7 < 7.2 at limit 8
    assert await limit_of(lim) == 8
    for lease in held:
        await lim.release(lease, throttled=False)


async def test_pause_closes_gate_and_blocks_acquire(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    lim = make(valkey, keys, pause_secs=0.3)
    gate = lim.gate(M, T)
    assert gate.is_open()
    await throttle(lim)
    assert not gate.is_open()
    assert await lim.acquire(M, T, Lane.RT, timeout_secs=0.05) is None
    other_pod = make(valkey, keys, pause_secs=0.3)
    other_gate = other_pod.gate(M, T)
    assert other_gate.is_open(), "a pod that has not read Valkey yet still has its cached view"
    started = time.monotonic()
    await other_gate.wait_open()
    assert time.monotonic() - started > 0.1, "wait_open must see the pause set by another pod"
    assert other_gate.is_open()
    await gate.wait_open()
    assert await lim.try_acquire(M, T, Lane.RT) is not None


async def test_lane_share(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    """bg/bulk may only use limit x bg_share; rt may use the whole limit."""
    lim = make(valkey, keys, initial={M: 10})
    await take(lim, 8, Lane.BG)
    assert await lim.try_acquire(M, T, Lane.BULK) is None
    assert await lim.try_acquire(M, T, Lane.BG) is None
    await take(lim, 2, Lane.RT)
    assert await lim.try_acquire(M, T, Lane.RT) is None
    assert (await lim.state(M, T)).inflight == 10


async def test_expired_leases_are_repaired(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    """DS11: a crashed holder's lease frees its slot once it expires; no sweep needed."""
    lim = make(valkey, keys, initial={M: 2}, lease_ttl_secs=0.2)
    await take(lim, 2)
    assert await lim.try_acquire(M, T, Lane.RT) is None
    await asyncio.sleep(0.3)
    assert (await lim.state(M, T)).inflight == 0
    assert await lim.try_acquire(M, T, Lane.RT) is not None


async def test_renew_keeps_lease_alive(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    lim = make(valkey, keys, initial={M: 2}, lease_ttl_secs=0.3)
    lease = await lim.try_acquire(M, T, Lane.RT)
    assert lease is not None
    await asyncio.sleep(0.2)
    [renewed] = await lim.renew([lease])
    assert renewed.expires_at > lease.expires_at
    await asyncio.sleep(0.2)
    assert (await lim.state(M, T)).inflight == 1


async def test_acquire_waits_for_a_free_slot(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    lim = make(valkey, keys, initial={M: 2})
    held = await take(lim, 2)

    async def free_later() -> None:
        await asyncio.sleep(0.1)
        await lim.release(held[0], throttled=False)

    task = asyncio.create_task(free_later())
    assert await lim.acquire(M, T, Lane.RT, timeout_secs=2.0) is not None
    await task
    started = time.monotonic()
    assert await lim.acquire(M, T, Lane.RT, timeout_secs=0.1) is None
    assert time.monotonic() - started >= 0.09


async def test_double_release_is_harmless(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    lim = make(valkey, keys)
    lease = await lim.try_acquire(M, T, Lane.RT)
    assert lease is not None
    await lim.release(lease, throttled=False)
    await lim.release(lease, throttled=False)
    assert (await lim.state(M, T)).inflight == 0


async def test_sync_limiter_shares_state(
    valkey: redis.asyncio.Redis, sync_valkey: redis.Redis, keys: KeySpace
) -> None:
    """DS18 sync path: same Lua, same keys, so sync and async callers share one limit."""
    settings = LimiterSettings(initial={M: 2}, min_limit=2, poll_secs=0.01)
    sync = SyncAdaptiveLimiter(sync_valkey, keys, settings)
    lim = AdaptiveLimiter(valkey, keys, settings)
    held = await take(lim, 1)
    lease = sync.acquire(M, T, Lane.RT, timeout_secs=0)
    assert lease is not None
    assert sync.acquire(M, T, Lane.RT, timeout_secs=0.05) is None
    assert sync.acquire(M, T, Lane.RT, timeout_secs=10, should_abort=lambda: True) is None
    sync.release(lease, throttled=True)
    assert (await lim.state(M, T)).paused_secs > 0
    await lim.release(held[0], throttled=False)


async def test_states_and_renew_take_one_round_trip(
    valkey: redis.asyncio.Redis, keys: KeySpace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Item 6: the receive budget reads, and the heartbeat renews, every target at once."""
    lim = make(valkey, keys, initial={M: 4}, lease_ttl_secs=5)
    held = [await lim.try_acquire(M, t, Lane.RT) for t in ("a", "b", "b")]
    leases = [lease for lease in held if lease is not None]
    assert len(leases) == 3
    executes = 0
    original = Pipeline.execute

    async def counting(pipe: Pipeline, raise_on_error: bool = True) -> Any:
        nonlocal executes
        executes += 1
        return await original(pipe, raise_on_error)

    monkeypatch.setattr(Pipeline, "execute", counting)
    states = await lim.states(M, ["a", "b", "c"])
    assert [(s.limit, s.inflight) for s in states] == [(4, 1), (4, 2), (None, 0)]
    renewed = await lim.renew(leases)
    assert executes == 2
    assert [r.token for r in renewed] == [lease.token for lease in leases]
    assert all(r.expires_at >= lease.expires_at for r, lease in zip(renewed, leases, strict=True))
    monkeypatch.undo()
    assert [await lim.state(M, t) for t in ("a", "b", "c")] == states


async def test_run_many_reloads_an_unknown_script(valkey: redis.asyncio.Redis) -> None:
    """A NOSCRIPT reply (server restarted or flushed) re-runs that script alone, loading it."""
    script = valkey.register_script("return ARGV[1]")
    script.sha = "0" * 40
    known = valkey.register_script("return 'k'")
    await known([], [])
    assert await _lua.run_many(valkey, [(known, [], []), (script, [], ["x"])]) == [b"k", b"x"]
