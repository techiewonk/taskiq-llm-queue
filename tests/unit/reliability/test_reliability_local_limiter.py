"""Pure-logic limiter pieces: settings, headroom, pause gate, local (Valkey-down) limiter."""

from __future__ import annotations

import threading

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from taskiq_llm_queue.config import Lane, ModelConfig, QueueConfig
from taskiq_llm_queue.protocols import Limiter, ReceiveGate
from taskiq_llm_queue.reliability import (
    LimiterSettings,
    LimiterState,
    LocalLimiter,
    PauseGate,
    local_cap,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now

    async def sleep(self, secs: float) -> None:
        self.now += secs

    def sync_sleep(self, secs: float) -> None:
        self.now += secs


def test_settings_validate_at_boot() -> None:
    with pytest.raises(ValueError, match="min_limit"):
        LimiterSettings(min_limit=5, max_limit=2)
    with pytest.raises(ValueError, match="decrease_factor"):
        LimiterSettings(decrease_factor=1.5)
    with pytest.raises(ValueError, match="positive"):
        LimiterSettings(ramp_window=0)


def test_settings_from_config_and_lane_share() -> None:
    config = QueueConfig(
        models={"a": ModelConfig(provider="bedrock", model_id="x", limiter_initial=7)}
    )
    settings = LimiterSettings.from_config(config, bg_share=0.5)
    assert settings.start_for("a") == 7
    assert settings.start_for("missing") == settings.default_initial
    assert settings.share_for(Lane.RT) == 1.0
    assert settings.share_for(Lane.BULK) == 0.5


def test_headroom() -> None:
    assert LimiterState(10, 3, 0.0).headroom() == 7
    assert LimiterState(10, 7, 0.0).headroom(0.8) == 1
    assert LimiterState(10, 9, 0.0).headroom(0.8) == 0
    assert LimiterState(10, 0, 1.0).headroom() == 0
    assert LimiterState(None, 0, 0.0).headroom() == 1


@pytest.mark.parametrize(
    ("limit", "pods", "fallback", "expected"),
    [(200, 15, 10, 13), (None, 15, 10, 10), (200, 0, 10, 10), (3, 15, 10, 1), (None, 1, 0, 1)],
)
def test_local_cap(limit: int | None, pods: int, fallback: int, expected: int) -> None:
    """DS18: per-pod cap = last known limit / live pods, else the configured fallback."""
    assert local_cap(limit, pods, fallback) == expected


async def test_pause_gate_closes_and_reopens() -> None:
    clock = FakeClock()
    refreshes = 0

    async def refresh() -> None:
        nonlocal refreshes
        refreshes += 1

    gate = PauseGate(refresh, clock=clock, sleep=clock.sleep)
    assert isinstance(gate, ReceiveGate)
    gate.close_for(2.0)
    assert not gate.is_open()
    await gate.wait_open()
    assert gate.is_open()
    assert clock.now >= 102.0
    assert refreshes >= 2


async def test_pause_gate_stays_closed_while_valkey_down() -> None:
    """DS18: no Valkey means no receiving; the gate retries until a refresh succeeds."""
    clock = FakeClock()
    calls = 0

    async def refresh() -> None:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RedisConnectionError("down")

    gate = PauseGate(refresh, clock=clock, sleep=clock.sleep, retry_secs=1.0)
    await gate.wait_open()
    assert calls == 3
    assert clock.now >= 102.0


def make_local(cap: int = 5) -> tuple[LocalLimiter, FakeClock]:
    clock = FakeClock()
    lim = LocalLimiter(
        cap, pause_secs=2.0, clock=clock, sleep=clock.sleep, sync_sleep=clock.sync_sleep
    )
    return lim, clock


async def test_local_limiter_cap_and_lane_share() -> None:
    lim, _ = make_local(5)
    assert isinstance(lim, Limiter)
    bg = [await lim.acquire("m", "t", Lane.BG, 0) for _ in range(4)]
    assert all(bg)
    assert await lim.acquire("m", "t", Lane.BULK, 0) is None
    assert await lim.acquire("m", "t", Lane.RT, 0) is not None
    assert await lim.acquire("m", "t", Lane.RT, 0) is None
    assert lim.inflight("m", "t") == 5
    assert await lim.acquire("m", "other", Lane.RT, 0) is not None


async def test_local_limiter_waits_until_timeout() -> None:
    lim, clock = make_local(1)
    lease = await lim.acquire("m", "t", Lane.RT, 0)
    assert lease is not None
    assert await lim.acquire("m", "t", Lane.RT, 0.5) is None
    assert clock.now >= 100.5
    await lim.release(lease, throttled=False)
    assert lim.inflight("m", "t") == 0


async def test_local_limiter_throttle_pauses() -> None:
    """A throttle storm (3 of 3 calls in the window) pauses the model target."""
    lim, clock = make_local(3)
    for _ in range(3):
        lease = await lim.acquire("m", "t", Lane.RT, 0)
        assert lease is not None
        await lim.release(lease, throttled=True)
    assert not lim.gate("m", "t").is_open()
    assert await lim.acquire("m", "t", Lane.RT, 0) is None
    assert await lim.acquire("m", "t", Lane.RT, 3.0) is not None
    assert clock.now >= 102.0


async def test_local_limiter_lone_throttle_does_not_pause() -> None:
    """Bug LIMITER_COLLAPSE (S35): every 429 closed the gate 2 s; noise must not pause it.

    One throttle among successes, or two throttles in different windows, leave the gate open.
    """
    lim, clock = make_local(3)
    for throttled in (False, False, True, False, False):
        lease = await lim.acquire("m", "t", Lane.RT, 0)
        assert lease is not None
        await lim.release(lease, throttled=throttled)
    assert lim.gate("m", "t").is_open()
    clock.now += 1.5
    lease = await lim.acquire("m", "t", Lane.RT, 0)
    assert lease is not None
    await lim.release(lease, throttled=True)
    assert lim.gate("m", "t").is_open(), "the earlier throttle is in a past window"


def test_local_limiter_sync_from_threads() -> None:
    lim = LocalLimiter(3)
    got: list[bool] = []
    lock = threading.Lock()

    def worker() -> None:
        lease = lim.acquire_sync("m", "t", Lane.RT, 0)
        with lock:
            got.append(lease is not None)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert got.count(True) == 3
    assert lim.inflight("m", "t") == 3


def test_local_limiter_rejects_zero_cap() -> None:
    with pytest.raises(ValueError, match="cap"):
        LocalLimiter(0)


def test_storm_needs_evidence_not_a_lucky_window() -> None:
    """Bug S35b: under 30 % random 429s, "2 of 3 throttled" paused a model ~22 % of windows and
    fewer outcomes paused it more; it sat paused 50-70 % of the time. The 95 % lower bound
    pauses 3 of 3 and 6 of 6, not 2 of 2, 2 of 3 or 5 of 6."""
    from taskiq_llm_queue.reliability.local_limiter import storm_lower_bound

    pauses = {
        (t, n): storm_lower_bound(t, n) >= 0.5 for t, n in [(2, 2), (3, 3), (2, 3), (5, 6), (6, 6)]
    }
    assert pauses == {(2, 2): False, (3, 3): True, (2, 3): False, (5, 6): False, (6, 6): True}
