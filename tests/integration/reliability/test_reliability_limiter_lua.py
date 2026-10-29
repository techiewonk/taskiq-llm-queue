"""Adaptive limiter Lua on real Valkey: start values, ramp, AIMD, pause, lane share, leases."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
import redis
import redis.asyncio
from redis.asyncio.client import Pipeline
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from taskiq_llm_queue.broker.queue import SQSQueue
from taskiq_llm_queue.config import Lane, ModelConfig
from taskiq_llm_queue.keys import KeySpace
from taskiq_llm_queue.lifecycle import receive_budget
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


async def throttle(lim: AdaptiveLimiter, n: int = 2) -> None:
    """`n` throttles in a row: two cut (min_throttles, ratio 1); three also pause (95 % bound)."""
    for _ in range(n):
        lease = await lim.acquire(M, T, Lane.RT, timeout_secs=2)  # waits out a pause
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
    await throttle(lim, 3)  # 3 of 3: a storm by the 95 % bound
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
    lim = make(valkey, keys, initial={M: 4})  # start 4: futile cuts may go down to it
    await valkey.hset(keys.limiter(M, T), mapping={"limit": 10, "ramp": 1})
    await throttle(lim)
    assert await limit_of(lim) == 7
    assert await valkey.hget(keys.limiter(M, T), "ramp") == b"0"
    await asyncio.sleep(0.2)  # let the pause end; still inside the same second
    await throttle(lim)
    assert await limit_of(lim) == 7, "second throttle within 1 s must not shrink again"
    await asyncio.sleep(1.05)
    await throttle(lim, 1)  # the last window's throttles still weigh in: a storm, not noise
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
    await throttle(lim, 3)  # 3 of 3: a storm by the 95 % bound
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
    for _ in range(2):  # 3 throttles in a row: a storm by the 95 % bound, so it pauses
        lease = sync.acquire(M, T, Lane.RT, timeout_secs=0)
        assert lease is not None
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


async def test_sporadic_throttles_neither_cut_nor_pause(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """Bug LIMITER_COLLAPSE (S35): every 429 cut x0.7 and paused 2 s, so 2 % noise pinned 3-5.

    Two throttles among 60 successes in one window (3 % < loss_tolerance) change nothing.
    """
    lim = make(valkey, keys, initial={M: 10}, pause_secs=2.0)
    await valkey.hset(keys.limiter(M, T), mapping={"limit": 10, "ramp": 0})
    for i in range(62):
        lease = await lim.try_acquire(M, T, Lane.RT)
        assert lease is not None
        await lim.release(lease, throttled=i in (30, 61))
    state = await lim.state(M, T)
    assert state.limit == 10
    assert state.paused_secs == 0
    assert lim.gate(M, T).is_open()


async def test_moderate_loss_cuts_without_pausing(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """Bug LIMITER_COLLAPSE: only a throttle storm (>= pause_ratio) pauses every pod's gate.

    A 1-in-4 throttle rate cuts the limit once (clustered) but leaves the target unpaused.
    """
    lim = make(valkey, keys, initial={M: 10}, pause_secs=2.0)
    await valkey.hset(keys.limiter(M, T), mapping={"limit": 10, "ramp": 0})
    for i in range(8):
        lease = await lim.try_acquire(M, T, Lane.RT)
        assert lease is not None
        await lim.release(lease, throttled=i % 4 == 3)
    state = await lim.state(M, T)
    assert state.limit == 7, "two throttles at 25 % of the window cut once"
    assert state.paused_secs == 0


async def test_one_cut_per_round_trip(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    """Bug LIMITER_COLLAPSE: throttles of calls sent before a cut must not cut again.

    A call started before the cut and throttled after the window is ignored; a new one counts.
    """
    lim = make(valkey, keys, initial={M: 4})
    await valkey.hset(keys.limiter(M, T), mapping={"limit": 10, "ramp": 0})
    old = await lim.try_acquire(M, T, Lane.RT)
    assert old is not None
    await throttle(lim)
    assert await limit_of(lim) == 7
    await asyncio.sleep(1.05)
    await lim.release(old, throttled=True)
    assert await limit_of(lim) == 7, "sent under the old limit: no second cut"
    await throttle(lim, 1)
    assert await limit_of(lim) == 4


async def test_growth_scales_with_the_limit(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    """Bug LIMITER_COLLAPSE: +1 per 20 busy successes left a cut limit of 4 stuck for minutes.

    Growth is +1 per min(increase_every, limit / 2) busy successes: every 2 at limit 4.
    """
    lim = make(valkey, keys, initial={M: 4}, increase_every=20)
    await valkey.hset(keys.limiter(M, T), mapping={"limit": 4, "ramp": 0})
    held = await take(lim, 3)
    await succeed(lim, 2)  # inflight 4 of 4
    assert await limit_of(lim) == 5
    await release_all(lim, held)


async def test_growth_settles_below_the_last_cut(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """Bug S07: the limit saw-toothed 44-64 around a capacity of 50 instead of settling.

    From settle_ratio x w_max (the limit last cut from) growth is one step per `probe_secs`;
    below it growth is normal, and passing w_max / settle_ratio forgets w_max.
    """
    lim = make(valkey, keys, initial={M: 8}, increase_every=2, probe_secs=0.5, max_limit=100)
    await valkey.hset(keys.limiter(M, T), mapping={"limit": 8, "ramp": 0, "w_max": 10})
    held = await take(lim, 7)
    await succeed(lim, 2)  # below 9 = 0.9 x 10: normal growth
    assert await limit_of(lim) == 9
    held += await take(lim, 1)
    await succeed(lim, 20)  # at the plateau: wait out probe_secs since the last step
    assert await limit_of(lim) == 9
    for expected in (10, 11):
        await asyncio.sleep(0.55)
        await succeed(lim, 2)
        assert await limit_of(lim) == expected
        held += await take(lim, 1)
        await succeed(lim, 10)
        assert await limit_of(lim) == expected, "one probe step per probe_secs"
    await asyncio.sleep(0.55)
    await succeed(lim, 2)
    assert await limit_of(lim) == 12
    held += await take(lim, 1)
    await succeed(lim, 6)  # at 12 >= 10 / 0.9 the old ceiling is forgotten
    assert await valkey.hget(keys.limiter(M, T), "w_max") == b"0"
    assert (await limit_of(lim) or 0) >= 13, "normal growth again, no probe wait"
    await release_all(lim, held)


async def test_failed_release_does_not_leak_its_lease(
    valkey: redis.asyncio.Redis, keys: KeySpace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug: a release that failed (pool exhausted) left its lease holding a slot for 240 s.

    The lease is remembered and dropped by this pod's next acquire.
    """
    lim = make(valkey, keys, initial={M: 2})
    lease = await lim.try_acquire(M, T, Lane.RT)
    assert lease is not None

    async def broken(*_: object) -> None:
        raise RedisConnectionError("pool exhausted")

    monkeypatch.setattr(lim, "_release", broken)
    with pytest.raises(RedisConnectionError):
        await lim.release(lease, throttled=False)
    monkeypatch.undo()
    assert (await lim.state(M, T)).inflight == 1
    again = await lim.try_acquire(M, T, Lane.RT)
    assert again is not None
    assert (await lim.state(M, T)).inflight == 1, "the orphan was dropped first"
    await lim.release(again, throttled=False)


async def test_lost_acquire_reply_does_not_leak_a_lease(
    valkey: redis.asyncio.Redis, keys: KeySpace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug: an acquire whose reply was lost after the script ran left an unknown live lease."""
    lim = make(valkey, keys, initial={M: 2})
    real = lim._acquire

    async def ran_then_failed(keys_: Any, args: Any) -> Any:
        await real(keys_, args)
        raise RedisTimeoutError("reply lost")

    monkeypatch.setattr(lim, "_acquire", ran_then_failed)
    with pytest.raises(RedisTimeoutError):
        await lim.try_acquire(M, T, Lane.RT)
    monkeypatch.undo()
    assert (await lim.state(M, T)).inflight == 1
    lease = await lim.try_acquire(M, T, Lane.RT)
    assert lease is not None
    assert (await lim.state(M, T)).inflight == 1
    await lim.release(lease, throttled=False)


async def test_throttle_while_probing_cuts_back_to_the_plateau(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """Bug S07: every cut was x0.7, so a limit at its known ceiling dipped 30 % each cycle.

    A storm met while probing near w_max cuts x settle_ratio; a second storm soon after
    (capacity really dropped) cuts x decrease_factor again.
    """
    lim = make(valkey, keys, initial={M: 4}, max_limit=100)
    await valkey.hset(keys.limiter(M, T), mapping={"limit": 20, "ramp": 0, "w_max": 21})
    await throttle(lim)
    assert await limit_of(lim) == 18  # floor(20 x 0.9), not 14
    await asyncio.sleep(1.05)
    await throttle(lim, 1)
    assert await limit_of(lim) == 12  # floor(18 x 0.7): within probe_secs of the last cut


async def test_probing_needs_every_slot_in_use(valkey: redis.asyncio.Redis, keys: KeySpace) -> None:
    """Bug S07: probing grew on 0.9 x limit in use, so an unbound limit crept past capacity.

    Above the plateau a success grows the limit only when all allowed slots are in use.
    """
    lim = make(valkey, keys, initial={M: 9}, increase_every=2, probe_secs=0.0, max_limit=100)
    await valkey.hset(keys.limiter(M, T), mapping={"limit": 9, "ramp": 0, "w_max": 10})
    held = await take(lim, 7)
    await succeed(lim, 6)  # inflight 8 of 9: busy, not full
    assert await limit_of(lim) == 9
    held += await take(lim, 1)
    await succeed(lim, 2)  # inflight 9 of 9
    assert await limit_of(lim) == 10
    await release_all(lim, held)


async def test_futile_cuts_stop_at_the_start_value(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """Bug S35b: a 30 % random 429 storm kept cutting every second, far below the start value.

    When the throttle ratio does not fall after a cut, the throttles are not ours: the limit
    is cut no lower than the start value; a cut that did lower the ratio may go below it.
    """
    lim = make(valkey, keys, initial={M: 8}, max_limit=100)
    await valkey.hset(keys.limiter(M, T), mapping={"limit": 10, "ramp": 0})
    await throttle(lim)
    assert await limit_of(lim) == 7
    await asyncio.sleep(1.05)
    await throttle(lim, 1)  # ratio still 1.0: futile, and 7 is already below the start
    assert await limit_of(lim) == 7
    await asyncio.sleep(2.1)  # past both windows: a fresh, lower ratio
    for i in range(10):
        lease = await lim.try_acquire(M, T, Lane.RT)
        assert lease is not None
        await lim.release(lease, throttled=i in (4, 9))  # 20 % < 0.6 x 100 %: the cut helped
    assert await limit_of(lim) == 4


async def test_model_cap_holds_under_busy_successes(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """`max_concurrency` 10: 1,000 busy successes at full use never lift the limit past 10.

    The start (40) is clamped to the cap, ramp and additive growth stop there, and an 11th
    concurrent acquire is refused; other models keep the global `max_limit`.
    """
    lim = make(valkey, keys, initial={M: 40}, max_by_model={M: 10}, max_limit=2000)
    held = await take(lim, 9)
    for _ in range(1000):  # every success at inflight 10 of 10: as busy as it gets
        await succeed(lim, 1)
        assert await limit_of(lim) == 10
    held += await take(lim, 1)
    assert await lim.try_acquire(M, T, Lane.RT) is None
    assert (await lim.state(M, T)).inflight == 10
    await release_all(lim, held)
    assert lim.settings.max_for("other") == 2000


async def test_model_cap_cuts_below_and_regrows_to_cap(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """Adaptive behaviour stays below the cap: a throttle storm cuts, busy successes grow back."""
    lim = make(valkey, keys, initial={M: 10}, max_by_model={M: 10}, pause_secs=0, probe_secs=0)
    await succeed(lim, 1)
    await throttle(lim)
    cut = await limit_of(lim)
    assert cut is not None
    assert cut < 10
    for _ in range(200):
        held = await take(lim, max(0, (await limit_of(lim) or 1) - 1))
        await succeed(lim, 1)
        await release_all(lim, held)
    assert await limit_of(lim) == 10


async def test_lowered_cap_clamps_learned_limit(
    valkey: redis.asyncio.Redis, sync_valkey: redis.Redis, keys: KeySpace
) -> None:
    """A limit learned before the cap existed is read capped and rewritten on the next call.

    The sync twin (direct path) sends the same bounds, so it sees the same cap.
    """
    await valkey.hset(keys.limiter(M, T), mapping={"limit": 40, "ramp": 0})
    lim = make(valkey, keys, max_by_model={M: 6})
    assert (await lim.state(M, T)).limit == 6  # PEEK capped in Python: receive budgets
    sync = SyncAdaptiveLimiter(sync_valkey, keys, lim.settings)
    leases = [sync.acquire(M, T, Lane.RT, timeout_secs=0) for _ in range(7)]
    assert [lease is not None for lease in leases] == [True] * 6 + [False]
    assert int(await valkey.hget(keys.limiter(M, T), "limit") or 0) == 6
    for lease in leases:
        if lease is not None:
            sync.release(lease, throttled=False)
    assert await limit_of(lim) == 6


async def test_receive_budget_nets_out_local_waiters(
    valkey: redis.asyncio.Redis, keys: KeySpace
) -> None:
    """A full bucket plus a waiting caller gives a 0 budget, so the backlog stays in SQS.

    S53 measured 44 messages held out of SQS by 3 pods with a bucket of 10 before waiters were
    subtracted (headroom alone let each pod receive what its own waiters would take).
    """
    lim = make(valkey, keys, initial={M: 2}, max_by_model={M: 2}, min_limit=1)
    model = ModelConfig(provider="bedrock", model_id="x", targets={T: {}})
    budget = receive_budget(M, model, lim, {"q": Lane.RT}, None)
    queue = SQSQueue(name="q")
    held = await take(lim, 1)
    assert await budget(queue) == 1
    held += await take(lim, 1)
    waiter = asyncio.create_task(lim.acquire(M, T, Lane.RT, timeout_secs=5))
    for _ in range(500):  # until the waiter has been refused once and sleeps
        if lim.waiting(M, T):
            break
        await asyncio.sleep(0.01)
    assert await budget(queue) == 0
    await lim.release(held.pop(), throttled=False)
    assert await budget(queue) == 0, "the freed slot is the waiter's, not a new receive's"
    lease = await waiter
    assert lease is not None
    assert lim.waiting(M, T) == 0
    await release_all(lim, [*held, lease])
